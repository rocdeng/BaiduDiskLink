package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stuckReader：offset 0 的前 stuckReads 次请求一直挂起，之后正常返回。
type stuckReader struct {
	mu          sync.Mutex
	reads       int
	zeroReads   int
	stuckReads  int
	invalidated int
	err         error
}

func (r *stuckReader) ReadStreamRange(ctx context.Context, _ string, offset, length int64) ([]byte, error) {
	r.mu.Lock()
	r.reads++
	if offset == 0 {
		r.zeroReads++
	}
	count := r.zeroReads
	err := r.err
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if offset == 0 && count <= r.stuckReads {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return make([]byte, length), nil
}

func (r *stuckReader) InvalidateDownloadLink(string) {
	r.mu.Lock()
	r.invalidated++
	r.mu.Unlock()
}

func TestForegroundSecondHedgeSwitchesNodeWhenFirstHedgeAlsoStalls(t *testing.T) {
	reader := &stuckReader{stuckReads: 2}
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.Hedge = true
		cfg.HedgeMinDelay = 10 * time.Millisecond
		cfg.HedgeMaxDelay = 20 * time.Millisecond
	})
	handle := testHandle(t, manager, File{FSID: "1", Size: 64, MTM: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := handle.ReadAt(ctx, 0, 2); err != nil {
		t.Fatalf("second hedge should rescue the read: %v", err)
	}
	if got := manager.Stats().Hedges; got != 2 {
		t.Fatalf("expected two hedges, got %d", got)
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.invalidated == 0 {
		t.Fatal("second hedge should invalidate the download link to switch CDN node")
	}
}

func TestStreamDownloadDoesNotRetryUnsatisfiableRange(t *testing.T) {
	reader := &stuckReader{err: errors.New("download range failed: status=416 body=")}
	manager := testManager(t, reader, nil)
	handle := testHandle(t, manager, File{FSID: "1", Size: 4, MTM: 1})
	if _, err := handle.ReadAt(context.Background(), 0, 2); err == nil {
		t.Fatal("expected range error")
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.reads != 1 {
		t.Fatalf("416 must not be retried, got %d reads", reader.reads)
	}
}

type flakyHoleReader struct {
	mu           sync.Mutex
	failOffset   int64
	failAttempts int
	attempts     map[int64]int
}

func (r *flakyHoleReader) ReadStreamRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	r.mu.Lock()
	if r.attempts == nil {
		r.attempts = make(map[int64]int)
	}
	r.attempts[offset]++
	count := r.attempts[offset]
	r.mu.Unlock()
	if offset == r.failOffset && count <= r.failAttempts {
		return nil, errors.New("download range stalled: idle timeout")
	}
	return make([]byte, length), nil
}

func TestScheduleBufferRepairsFailedNearWindowHole(t *testing.T) {
	oldCooldown := chunkRepairCooldown
	oldBackoff := streamRetryBackoff
	chunkRepairCooldown = 15 * time.Millisecond
	streamRetryBackoff = time.Millisecond
	defer func() {
		chunkRepairCooldown = oldCooldown
		streamRetryBackoff = oldBackoff
	}()

	// offset=12 (chunk 3) 前两次失败（耗尽 download 内部重试），第三次恢复正常。
	reader := &flakyHoleReader{failOffset: 12, failAttempts: 2}
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.ChunkSize = 4
		cfg.TargetBuffer = 24
		cfg.LowWatermark = 8
		cfg.BackBuffer = 4
		cfg.MemoryCache = 64
	})
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	// 进入流模式并触发首次预取（chunk 3 会失败 2 次后出队）。
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reader.mu.Lock()
		n := reader.attempts[12]
		reader.mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 等冷却结束，再推进播放位置（不直接读 chunk 3），验证 scheduleBuffer 主动补发空洞。
	time.Sleep(25 * time.Millisecond)
	if _, err := handle.ReadAt(context.Background(), 6, 2); err != nil {
		t.Fatal(err)
	}

	key := chunkKey{version: chunkVersion(file, manager.cfg.ChunkSize), index: 3}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := manager.store.length(key); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := manager.store.length(key); !ok {
		t.Fatal("failed near-window chunk was not repaired by scheduleBuffer")
	}
	stats := manager.Stats()
	if stats.Repairs == 0 {
		t.Fatalf("expected repair counter > 0, got %+v", stats)
	}
	if stats.SlowAborts == 0 {
		t.Fatalf("expected slow abort counter > 0, got %+v", stats)
	}
}

func TestTaskHeapOrdersSameFileByNearestIndex(t *testing.T) {
	var queue taskHeap
	far := &task{key: chunkKey{version: "v1", index: 50}, priority: priorityNear, seq: 1}
	near := &task{key: chunkKey{version: "v1", index: 5}, priority: priorityNear, seq: 2}
	otherFile := &task{key: chunkKey{version: "v2", index: 1}, priority: priorityNear, seq: 3}

	queue.Push(far)
	queue.Push(near)
	queue.Push(otherFile)
	if !queue.Less(1, 0) {
		t.Fatal("nearer chunk index for same file should have higher priority than farther chunk")
	}
	if !queue.Less(0, 2) {
		t.Fatal("different files with same priority should fall back to FIFO seq order")
	}
}

func TestSmallBackwardJitterDoesNotDemoteStreamMode(t *testing.T) {
	manager := &Manager{cfg: DefaultConfig()}
	handle := &Handle{
		file:         File{FSID: "1", Size: 1 << 30, MTM: 1},
		lastOffset:   20 << 20,
		lastLength:   1 << 20,
		forwardReads: 5,
		mode:         modeStream,
	}
	// 并发预读乱序：比 expected (21 MiB) 回退 1.5 MiB，在 4 MiB 容忍区间内。
	obs := manager.observeHandleLocked(handle, 19<<20+512<<10, 512<<10)
	if obs.mode != modeStream || obs.event != readEventNone || !obs.streamRead {
		t.Fatalf("small backward jitter should stay in stream mode: %+v", obs)
	}
	if handle.lastOffset != 20<<20 {
		t.Fatalf("backward jitter should not roll back high-water lastOffset, got %d", handle.lastOffset)
	}

	// 真正的大幅回跳（回退 10 MiB > 4 MiB）仍应识别为 seek。
	seekObs := manager.observeHandleLocked(handle, 10<<20, 512<<10)
	if seekObs.event != readEventSeek || handle.mode != modeProbe {
		t.Fatalf("large backward jump should still trigger seek: obs=%+v mode=%v", seekObs, handle.mode)
	}
}

func TestLowWatermarkDrainProactivelyInvalidatesDlink(t *testing.T) {
	oldCheck := lowWatermarkCheckInterval
	oldRefresh := lowWatermarkRefreshCooldown
	lowWatermarkCheckInterval = 5 * time.Millisecond
	lowWatermarkRefreshCooldown = 20 * time.Millisecond
	defer func() {
		lowWatermarkCheckInterval = oldCheck
		lowWatermarkRefreshCooldown = oldRefresh
	}()

	reader := newControlledReader()
	// 阻塞 chunk 4..10，让后台 inflight > 0 且缓冲无法继续填充。
	blocker := make(chan struct{})
	defer close(blocker)
	for i := int64(4); i <= 10; i++ {
		reader.blocked[i*4] = blocker
	}
	manager := testManager(t, reader, func(cfg *Config) {
		cfg.ChunkSize = 4
		cfg.LowWatermark = 16
		cfg.TargetBuffer = 32
		cfg.BackBuffer = 4
		cfg.MemoryCache = 64
	})
	file := File{FSID: "1", Size: 128, MTM: 1}
	handle := testHandle(t, manager, file)
	version := chunkVersion(file, manager.cfg.ChunkSize)
	for i := int64(0); i < 4; i++ {
		manager.store.putMemory(chunkKey{version: version, index: i}, []byte{0, 1, 2, 3})
	}
	// 进入流模式并把 cursor 向前推进，使 bufferAhead 在低水位之下持续下降。
	if _, err := handle.ReadAt(context.Background(), 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ReadAt(context.Background(), 3, 3); err != nil {
		t.Fatal(err)
	}
	// 等待后台 worker 占用 inflight。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		inflight := manager.sessions[version].inflight
		manager.mu.Unlock()
		if inflight > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 连续三次推进 cursor，bufferAhead 递减（10 -> 8 -> 6 < LowWatermark=16）。
	for _, off := range []int64{6, 8, 10} {
		time.Sleep(10 * time.Millisecond)
		if _, err := handle.ReadAt(context.Background(), off, 2); err != nil {
			t.Fatal(err)
		}
	}
	reader.mu.Lock()
	invalidated := reader.invalidated
	reader.mu.Unlock()
	if invalidated == 0 {
		t.Fatal("expected proactive dlink invalidation when buffer drains below low watermark")
	}
}

func TestMemoryCacheClampsOversizedBufferWindow(t *testing.T) {
	manager := testManager(t, newControlledReader(), func(cfg *Config) {
		cfg.ChunkSize = 4
		cfg.MemoryCache = 20
		cfg.TargetBuffer = 32
		cfg.BackBuffer = 8
		cfg.LowWatermark = 24
	})
	if manager.cfg.TargetBuffer+manager.cfg.BackBuffer > manager.cfg.MemoryCache {
		t.Fatalf("buffer window (%d+%d) should be clamped within memory cache (%d)", manager.cfg.TargetBuffer, manager.cfg.BackBuffer, manager.cfg.MemoryCache)
	}
	if manager.cfg.LowWatermark > manager.cfg.TargetBuffer {
		t.Fatalf("low watermark (%d) should not exceed clamped target buffer (%d)", manager.cfg.LowWatermark, manager.cfg.TargetBuffer)
	}
}
