package stream

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"
)

type controlledReader struct {
	mu          sync.Mutex
	started     chan int64
	blocked     map[int64]chan struct{}
	blockFirst  map[int64]chan struct{}
	reads       map[int64]int
	invalidated int
}

type blockingSummaryWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingSummaryWriter) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("stream summary")) {
		w.once.Do(func() { close(w.started) })
		<-w.release
	}
	return len(data), nil
}

func newControlledReader() *controlledReader {
	return &controlledReader{
		started:    make(chan int64, 128),
		blocked:    make(map[int64]chan struct{}),
		blockFirst: make(map[int64]chan struct{}),
		reads:      make(map[int64]int),
	}
}

func (r *controlledReader) ReadStreamRange(ctx context.Context, _ string, offset, length int64) ([]byte, error) {
	r.mu.Lock()
	r.reads[offset]++
	readCount := r.reads[offset]
	block := r.blocked[offset]
	if readCount == 1 && r.blockFirst[offset] != nil {
		block = r.blockFirst[offset]
	}
	r.mu.Unlock()
	select {
	case r.started <- offset:
	default:
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	data := make([]byte, length)
	for index := range data {
		data[index] = byte(offset + int64(index))
	}
	return data, nil
}

func (r *controlledReader) InvalidateDownloadLink(string) {
	r.mu.Lock()
	r.invalidated++
	r.mu.Unlock()
}

func testManager(t *testing.T, reader Reader, mutate func(*Config)) *Manager {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ChunkSize = 4
	cfg.ForwardGap = 4
	cfg.SeekThreshold = 8
	cfg.Workers = 4
	cfg.SessionWorkers = 3
	cfg.LowWatermark = 8
	cfg.TargetBuffer = 16
	cfg.BackBuffer = 4
	cfg.MemoryCache = 64
	cfg.DiskCache = 0
	cfg.Hedge = false
	if mutate != nil {
		mutate(&cfg)
	}
	manager, err := NewManager(reader, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func testHandle(t *testing.T, manager *Manager, file File) *Handle {
	t.Helper()
	handle := manager.Open(file)
	if handle == nil {
		t.Fatal("stream handle was not opened")
	}
	t.Cleanup(handle.Release)
	return handle
}

func TestDefaultConfigUsesOneMiBChunksAndLegacyReadThresholds(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ChunkSize != 1<<20 || cfg.ForwardGap != 8<<20 || cfg.SeekThreshold != 16<<20 {
		t.Fatalf("unexpected stream defaults: %#v", cfg)
	}
	if cfg.HedgeMinDelay != 300*time.Millisecond || cfg.HedgeMaxDelay != 800*time.Millisecond {
		t.Fatalf("unexpected hedge defaults: %#v", cfg)
	}
}

func TestOneMiBChunksUseTargetBufferForForwardSeekThreshold(t *testing.T) {
	manager := &Manager{cfg: DefaultConfig()}
	file := File{FSID: "1", Size: 1 << 30, MTM: 1}
	newStreamingHandle := func() *Handle {
		return &Handle{
			file:         file,
			lastOffset:   0,
			lastLength:   196608,
			forwardReads: 2,
			mode:         modeStream,
		}
	}

	near := manager.observeHandleLocked(newStreamingHandle(), 8<<20, 196608)
	if near.event == readEventSeek {
		t.Fatal("an eight MiB forward gap was incorrectly classified as a seek")
	}
	buffered := manager.observeHandleLocked(newStreamingHandle(), 64<<20, 196608)
	if buffered.event == readEventSeek {
		t.Fatal("a forward gap inside the target buffer was incorrectly classified as a seek")
	}

	far := manager.observeHandleLocked(newStreamingHandle(), 257<<20, 196608)
	if far.event != readEventSeek {
		t.Fatal("a forward gap beyond the target buffer was not classified as a seek")
	}
}

func TestLargeFileHeadProbeDoesNotStartStreaming(t *testing.T) {
	manager := &Manager{cfg: DefaultConfig()}
	handle := &Handle{
		file:       File{FSID: "1", Size: 1 << 30, MTM: 1},
		lastOffset: -1,
		mode:       modeProbe,
	}
	for _, offset := range []int64{0, 196608, 2 << 20, 7 << 20} {
		if observed := manager.observeHandleLocked(handle, offset, 196608); observed.mode == modeStream {
			t.Fatalf("head probe at offset %d unexpectedly entered streaming mode", offset)
		}
	}
	observed := manager.observeHandleLocked(handle, 8<<20, 196608)
	if observed.mode == modeStream {
		t.Fatal("first read outside the head probe edge unexpectedly entered streaming mode")
	}
	observed = manager.observeHandleLocked(handle, 8<<20+196608, 196608)
	if observed.mode != modeStream {
		t.Fatal("sequential reads after the head probe did not enter streaming mode")
	}
}

func TestScheduledWindowReadDoesNotResetStreamEpoch(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 4096, MTM: 1}
	handle := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	handle.mu.Lock()
	handle.lastOffset = 0
	handle.lastLength = 2
	handle.forwardReads = 2
	handle.mode = modeStream
	handle.mu.Unlock()
	manager.mu.Lock()
	s := manager.sessions[version]
	s.activeHandle = handle.id
	s.scheduleSet = true
	s.scheduleEpoch = s.epoch
	s.scheduledFloor = 100
	s.scheduledEnd = 200
	manager.mu.Unlock()

	if _, err := handle.ReadAt(context.Background(), 150*manager.cfg.ChunkSize, 2); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	epoch := s.epoch
	manager.mu.Unlock()
	if epoch != 0 {
		t.Fatalf("scheduled-window read unexpectedly reset stream epoch: %d", epoch)
	}
}

func TestIndependentHandleSeekDoesNotResetStreamingSession(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 128, MTM: 1}
	playback := testHandle(t, manager, file)
	probe := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	manager.mu.Lock()
	s := manager.sessions[version]
	s.activeHandle = playback.id
	s.scheduleSet = true
	s.scheduleEpoch = s.epoch
	s.scheduledFloor = 0
	s.scheduledEnd = 5
	manager.mu.Unlock()
	probe.mu.Lock()
	probe.lastOffset = 0
	probe.lastLength = 2
	probe.forwardReads = 2
	probe.mode = modeStream
	probe.mu.Unlock()

	if _, err := probe.ReadAt(context.Background(), 64, 2); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	epoch := s.epoch
	active := s.activeHandle
	manager.mu.Unlock()
	if epoch != 0 || active != playback.id {
		t.Fatalf("independent handle changed playback session: epoch=%d active=%d playback=%d", epoch, active, playback.id)
	}
}

func TestStreamingReadSchedulesAheadBeforeForegroundWait(t *testing.T) {
	reader := newControlledReader()
	release := make(chan struct{})
	reader.blocked[0] = release
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	handle.mu.Lock()
	handle.lastOffset = -1
	handle.forwardReads = 2
	handle.mode = modeStream
	handle.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := handle.ReadAt(context.Background(), 0, 2)
		done <- err
	}()

	seenAhead := false
	deadline := time.After(time.Second)
	for !seenAhead {
		select {
		case offset := <-reader.started:
			if offset > 0 {
				seenAhead = true
			}
		case <-deadline:
			t.Fatal("foreground read did not schedule an ahead chunk")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestChunkVersionIncludesChunkSize(t *testing.T) {
	file := File{FSID: "1", Size: 64 << 20, MTM: 1}
	if chunkVersion(file, 1<<20) == chunkVersion(file, 8<<20) {
		t.Fatal("different stream chunk sizes shared one cache namespace")
	}
}

func TestManagerSharesInflightChunkAcrossHandles(t *testing.T) {
	reader := newControlledReader()
	release := make(chan struct{})
	reader.blocked[0] = release
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 64, MTM: 1}
	first := testHandle(t, manager, file)
	second := testHandle(t, manager, file)
	done := make(chan error, 2)
	go func() { _, err := first.ReadAt(context.Background(), 0, 2); done <- err }()
	<-reader.started
	go func() { _, err := second.ReadAt(context.Background(), 0, 2); done <- err }()
	select {
	case offset := <-reader.started:
		if offset == 0 {
			t.Fatalf("duplicate download started before the shared chunk completed: offset=%d", offset)
		}
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	for index := 0; index < 2; index++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	reader.mu.Lock()
	reads := reader.reads[0]
	reader.mu.Unlock()
	if reads != 1 {
		t.Fatalf("shared chunk downloaded %d times", reads)
	}
}

func TestProbeHandleDoesNotDisturbStreamingHandle(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 128, MTM: 1}
	playback := testHandle(t, manager, file)
	probe := testHandle(t, manager, file)
	if _, err := playback.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := playback.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	before := manager.sessions[chunkVersion(file, manager.cfg.ChunkSize)].epoch
	active := manager.sessions[chunkVersion(file, manager.cfg.ChunkSize)].activeHandle
	manager.mu.Unlock()
	if _, err := probe.ReadAt(context.Background(), 126, 2); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	after := manager.sessions[chunkVersion(file, manager.cfg.ChunkSize)].epoch
	stillActive := manager.sessions[chunkVersion(file, manager.cfg.ChunkSize)].activeHandle
	manager.mu.Unlock()
	if after != before || stillActive != active {
		t.Fatalf("probe changed playback session: epoch %d -> %d active=%d -> %d", before, after, active, stillActive)
	}
	if _, err := playback.ReadAt(context.Background(), 6, 2); err != nil {
		t.Fatal(err)
	}
}

func TestManagerForegroundDoesNotWaitForSlowAheadChunk(t *testing.T) {
	reader := newControlledReader()
	slow := make(chan struct{})
	reader.blocked[8] = slow
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 64, MTM: 1}
	handle := testHandle(t, manager, file)
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case offset := <-reader.started:
			if offset == 8 {
				goto slowStarted
			}
		case <-deadline:
			t.Fatal("slow ahead chunk did not start")
		}
	}

slowStarted:
	start := time.Now()
	if _, err := handle.ReadAt(context.Background(), 4, 2); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("foreground waited for slow ahead chunk: %s", elapsed)
	}
	close(slow)
}

func TestManagerKeepsWorkersFilledAroundSlowChunk(t *testing.T) {
	reader := newControlledReader()
	slow := make(chan struct{})
	reader.blocked[8] = slow
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	wanted := map[int64]bool{8: false, 12: false, 16: false, 20: false}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		all := true
		for _, seen := range wanted {
			all = all && seen
		}
		if all {
			break
		}
		select {
		case offset := <-reader.started:
			if _, ok := wanted[offset]; ok {
				wanted[offset] = true
			}
		case <-timer.C:
			t.Fatalf("workers did not continue past slow chunk: %#v", wanted)
		}
	}
	close(slow)
}

func TestSingleStreamingSessionCanUseAllWorkers(t *testing.T) {
	reader := newControlledReader()
	releases := make([]chan struct{}, 4)
	for index := range releases {
		releases[index] = make(chan struct{})
		reader.blocked[int64((index+2)*4)] = releases[index]
	}
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.Workers = 4
		cfg.SessionWorkers = 2
	})
	file := File{FSID: "1", Size: 64, MTM: 1}
	handle := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	manager.store.putMemory(chunkKey{version: version, index: 0}, []byte{0, 1, 2, 3})
	manager.store.putMemory(chunkKey{version: version, index: 1}, []byte{4, 5, 6, 7})
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}

	started := map[int64]bool{}
	deadline := time.After(time.Second)
	for len(started) < 4 {
		select {
		case offset := <-reader.started:
			if offset == 8 || offset == 12 || offset == 16 || offset == 20 {
				started[offset] = true
			}
		case <-deadline:
			t.Fatalf("single session used fewer than all workers: %#v", started)
		}
	}
	for _, release := range releases {
		close(release)
	}
}

func TestMultipleStreamingSessionsUseReservedWorkerLimit(t *testing.T) {
	manager := &Manager{
		cfg: Config{Workers: 8, SessionWorkers: 6},
		sessions: map[string]*session{
			"first":  {refs: 1, activeHandle: 1},
			"second": {refs: 1, activeHandle: 2},
		},
	}
	if got := manager.sessionWorkerLimitLocked(manager.sessions["first"]); got != 6 {
		t.Fatalf("unexpected competing-session worker limit: %d", got)
	}
}

func TestScheduleBufferAdvancesIncrementally(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	manager.store.putMemory(chunkKey{version: version, index: 0}, []byte{0, 1, 2, 3})
	manager.store.putMemory(chunkKey{version: version, index: 1}, []byte{4, 5, 6, 7})
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}

	manager.mu.Lock()
	s := manager.sessions[version]
	firstNear := s.scheduledNear
	firstEnd := s.scheduledEnd
	epoch := s.epoch
	manager.mu.Unlock()
	manager.scheduleBuffer(file, 10, epoch)
	manager.mu.Lock()
	secondNear := s.scheduledNear
	secondEnd := s.scheduledEnd
	manager.mu.Unlock()
	if secondNear <= firstNear || secondEnd <= firstEnd {
		t.Fatalf("schedule frontier did not advance: near %d -> %d end %d -> %d", firstNear, secondNear, firstEnd, secondEnd)
	}
}

func TestSequentialStreamReadDoesNotPromoteConsumedMemoryChunk(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.MemoryCache = 12
		cfg.TargetBuffer = 8
		cfg.LowWatermark = 4
		cfg.BackBuffer = 0
	})
	file := File{FSID: "1", Size: 64, MTM: 1}
	handle := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	handle.mu.Lock()
	handle.lastOffset = 0
	handle.lastLength = 3
	handle.forwardReads = 2
	handle.mode = modeStream
	handle.mu.Unlock()
	manager.mu.Lock()
	manager.sessions[version].activeHandle = handle.id
	manager.mu.Unlock()
	for index := int64(0); index < 3; index++ {
		manager.store.putMemory(chunkKey{version: version, index: index}, []byte{0, 1, 2, 3})
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	manager.store.putMemory(chunkKey{version: version, index: 3}, []byte{12, 13, 14, 15})
	if _, ok := manager.store.get(chunkKey{version: version, index: 0}); ok {
		t.Fatal("sequentially consumed chunk remained promoted in memory")
	}
	if _, ok := manager.store.get(chunkKey{version: version, index: 2}); !ok {
		t.Fatal("future chunk was evicted before consumed chunk")
	}
}

func TestScheduleBufferEvictsChunksBehindBackBuffer(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.TargetBuffer = 16
		cfg.BackBuffer = 4
		cfg.MemoryCache = 64
	})
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	manager.mu.Lock()
	s := manager.sessions[version]
	s.activeHandle = handle.id
	epoch := s.epoch
	manager.mu.Unlock()
	for index := int64(0); index < 8; index++ {
		manager.store.putMemory(chunkKey{version: version, index: index}, []byte{0, 1, 2, 3})
	}
	manager.scheduleBuffer(file, 20, epoch)
	if _, ok := manager.store.get(chunkKey{version: version, index: 2}); ok {
		t.Fatal("chunk behind back buffer was retained")
	}
	if _, ok := manager.store.get(chunkKey{version: version, index: 4}); !ok {
		t.Fatal("chunk inside back buffer was removed")
	}
}

func TestManagerSeekCancelsOldAheadTask(t *testing.T) {
	reader := newControlledReader()
	slow := make(chan struct{})
	reader.blocked[8] = slow
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 64, 2); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	epoch := manager.sessions[chunkVersion(file, manager.cfg.ChunkSize)].epoch
	manager.mu.Unlock()
	if epoch == 0 {
		t.Fatal("seek did not advance epoch")
	}
	close(slow)
}

func TestManagerHedgesSlowForegroundChunkOnlyOnce(t *testing.T) {
	reader := newControlledReader()
	first := make(chan struct{})
	reader.blockFirst[0] = first
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.Hedge = true
		cfg.HedgeMinDelay = 20 * time.Millisecond
		cfg.HedgeMaxDelay = 100 * time.Millisecond
	})
	file := File{FSID: "1", Size: 64, MTM: 1}
	firstHandle := testHandle(t, manager, file)
	secondHandle := testHandle(t, manager, file)
	done := make(chan error, 2)
	go func() { _, err := firstHandle.ReadAt(context.Background(), 0, 2); done <- err }()
	go func() { _, err := secondHandle.ReadAt(context.Background(), 0, 2); done <- err }()
	for index := 0; index < 2; index++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("hedge did not rescue foreground chunk")
		}
	}
	close(first)
	if manager.Stats().Hedges != 1 {
		t.Fatalf("unexpected hedge count: %#v", manager.Stats())
	}
	reader.mu.Lock()
	reads := reader.reads[0]
	reader.mu.Unlock()
	if reads != 2 {
		t.Fatalf("expected one original and one hedge request, got %d", reads)
	}
}

func TestManagerInvalidatesLinkAfterConsecutiveSlowForegroundChunks(t *testing.T) {
	reader := newControlledReader()
	first := make(chan struct{})
	second := make(chan struct{})
	third := make(chan struct{})
	reader.blockFirst[0] = first
	reader.blockFirst[4] = second
	reader.blockFirst[8] = third
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.Hedge = true
		cfg.HedgeMinDelay = time.Millisecond
		cfg.HedgeMaxDelay = 50 * time.Millisecond
	})
	file := File{FSID: "1", Size: 64, MTM: 1}
	handle := testHandle(t, manager, file)
	for _, offset := range []int64{0, 4, 8} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if _, err := handle.ReadAt(ctx, offset, 2); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	close(first)
	close(second)
	close(third)
	reader.mu.Lock()
	invalidated := reader.invalidated
	reader.mu.Unlock()
	if invalidated != 1 {
		t.Fatalf("expected one dlink invalidation, got %d", invalidated)
	}
}

func TestManagerReportsContiguousBufferAhead(t *testing.T) {
	reader := newControlledReader()
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 64, MTM: 1}
	handle := testHandle(t, manager, file)
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for manager.BufferAhead(file, 6) < 6 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := manager.BufferAhead(file, 6); got < 6 {
		t.Fatalf("unexpected buffer ahead: %d", got)
	}
}

func TestSummaryLoggingDoesNotBlockForegroundRead(t *testing.T) {
	writer := &blockingSummaryWriter{started: make(chan struct{}), release: make(chan struct{})}
	previousWriter := log.Writer()
	log.SetOutput(writer)
	defer log.SetOutput(previousWriter)
	defer close(writer.release)

	reader := newControlledReader()
	manager := testManager(t, reader, nil)
	file := File{FSID: "1", Size: 64, MTM: 1}
	handle := testHandle(t, manager, file)
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := handle.ReadAt(context.Background(), 3, 3)
		done <- err
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("summary logger did not start")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("foreground read waited for summary logging")
	}
}
