package stream

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Reader interface {
	ReadStreamRange(ctx context.Context, fsid string, offset, length int64) ([]byte, error)
}

type Config struct {
	ChunkSize      int64
	ForwardGap     int64
	SeekThreshold  int64
	Workers        int
	LowWatermark   int64
	TargetBuffer   int64
	BackBuffer     int64
	MemoryCache    int64
	DiskCache      int64
	CachePath      string
	Hedge          bool
	HedgeMinDelay  time.Duration
	HedgeMaxDelay  time.Duration
	SessionWorkers int
}

func DefaultConfig() Config {
	return Config{
		ChunkSize:      1 << 20,
		ForwardGap:     8 << 20,
		SeekThreshold:  16 << 20,
		Workers:        8,
		LowWatermark:   128 << 20,
		TargetBuffer:   256 << 20,
		BackBuffer:     32 << 20,
		MemoryCache:    320 << 20,
		DiskCache:      2 << 30,
		Hedge:          true,
		HedgeMinDelay:  300 * time.Millisecond,
		HedgeMaxDelay:  800 * time.Millisecond,
		SessionWorkers: 6,
	}
}

type Manager struct {
	reader Reader
	cfg    Config
	store  *chunkStore

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	diskWG sync.WaitGroup

	mu          sync.Mutex
	cond        *sync.Cond
	queue       taskHeap
	tasks       map[chunkKey]*task
	sessions    map[string]*session
	closed      bool
	sequence    int64
	nextHandle  uint64
	diskWrites  chan cacheEntry
	summaries   chan summaryRequest
	latencies   []time.Duration
	latencyNext int

	downloaded atomic.Int64
	retries    atomic.Int64
	hedges     atomic.Int64
}

type session struct {
	file           File
	epoch          int64
	activeHandle   uint64
	streamAnchor   int64
	cursor         int64
	refs           int
	inflight       int
	slowStreak     int
	lastAccess     time.Time
	lastSummary    time.Time
	scheduleSet    bool
	scheduleEpoch  int64
	scheduledFloor int64
	scheduledNear  int64
	scheduledEnd   int64
	hedges         int64
	dlinkRefreshes int64
}

type summaryRequest struct {
	file   File
	cursor int64
	epoch  int64
}

type Handle struct {
	manager      *Manager
	file         File
	id           uint64
	mu           sync.Mutex
	lastOffset   int64
	lastLength   int64
	forwardReads int
	mode         accessMode
	tailProbe    bool
	epoch        int64
	released     bool
}

type accessMode uint8

const (
	modeProbe accessMode = iota
	modeStream
)

type priority uint8

const (
	priorityForeground priority = iota
	priorityNear
	priorityAhead
	priorityBack
)

type task struct {
	key         chunkKey
	file        File
	offset      int64
	length      int64
	priority    priority
	epoch       int64
	seq         int64
	index       int
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	data        []byte
	err         error
	started     bool
	finished    bool
	foreground  bool
	hedged      bool
	hedgeCancel context.CancelFunc
	startedAt   time.Time
}

type taskHeap []*task

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	return h[i].seq < h[j].seq
}
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *taskHeap) Push(value any) {
	task := value.(*task)
	task.index = len(*h)
	*h = append(*h, task)
}
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	task := old[n-1]
	task.index = -1
	*h = old[:n-1]
	return task
}

func NewManager(reader Reader, cfg Config) (*Manager, error) {
	if reader == nil {
		return nil, errors.New("stream reader is required")
	}
	defaults := DefaultConfig()
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaults.ChunkSize
	}
	if cfg.ForwardGap <= 0 {
		cfg.ForwardGap = defaults.ForwardGap
	}
	if cfg.SeekThreshold < cfg.ForwardGap {
		cfg.SeekThreshold = defaults.SeekThreshold
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaults.Workers
	}
	if cfg.LowWatermark <= 0 {
		cfg.LowWatermark = defaults.LowWatermark
	}
	if cfg.TargetBuffer < cfg.LowWatermark {
		cfg.TargetBuffer = defaults.TargetBuffer
	}
	if cfg.BackBuffer <= 0 {
		cfg.BackBuffer = defaults.BackBuffer
	}
	if cfg.MemoryCache < 0 || cfg.DiskCache < 0 {
		return nil, errors.New("stream cache limits must be non-negative")
	}
	if cfg.HedgeMinDelay <= 0 {
		cfg.HedgeMinDelay = defaults.HedgeMinDelay
	}
	if cfg.HedgeMaxDelay < cfg.HedgeMinDelay {
		cfg.HedgeMaxDelay = defaults.HedgeMaxDelay
	}
	if cfg.SessionWorkers <= 0 || cfg.SessionWorkers > cfg.Workers {
		cfg.SessionWorkers = cfg.Workers - 2
		if cfg.SessionWorkers <= 0 {
			cfg.SessionWorkers = cfg.Workers
		}
	}
	store, err := newChunkStore(cfg.MemoryCache, cfg.CachePath, cfg.DiskCache)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		reader:     reader,
		cfg:        cfg,
		store:      store,
		ctx:        ctx,
		cancel:     cancel,
		tasks:      make(map[chunkKey]*task),
		sessions:   make(map[string]*session),
		diskWrites: make(chan cacheEntry, cfg.Workers*8),
		summaries:  make(chan summaryRequest, 1),
	}
	m.cond = sync.NewCond(&m.mu)
	heap.Init(&m.queue)
	for index := 0; index < cfg.Workers; index++ {
		m.wg.Add(1)
		go m.worker()
	}
	m.diskWG.Add(1)
	go m.diskWriter()
	m.wg.Add(1)
	go m.summaryWorker()
	return m, nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	for _, task := range m.tasks {
		if task.cancel != nil {
			task.cancel()
		}
		if task.hedgeCancel != nil {
			task.hedgeCancel()
		}
	}
	m.cancel()
	m.cond.Broadcast()
	m.mu.Unlock()
	m.wg.Wait()
	close(m.diskWrites)
	m.diskWG.Wait()
	return nil
}

func (m *Manager) Open(file File) *Handle {
	if m == nil || file.FSID == "" {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	s := m.sessionLocked(file)
	s.refs++
	s.lastAccess = time.Now()
	m.nextHandle++
	handle := &Handle{manager: m, file: file, id: m.nextHandle, lastOffset: -1, mode: modeProbe}
	m.mu.Unlock()
	return handle
}

func (h *Handle) Release() {
	if h == nil || h.manager == nil {
		return
	}
	h.mu.Lock()
	if h.released {
		h.mu.Unlock()
		return
	}
	h.released = true
	m := h.manager
	file := h.file
	id := h.id
	h.mu.Unlock()

	m.mu.Lock()
	version := chunkVersion(file, m.cfg.ChunkSize)
	if s := m.sessions[version]; s != nil {
		if s.refs > 0 {
			s.refs--
		}
		if s.activeHandle == id {
			s.epoch++
			s.activeHandle = 0
			m.cancelSessionTasksLocked(version, s.epoch)
		}
		s.lastAccess = time.Now()
		m.cleanupSessionLocked(version, s)
	}
	m.mu.Unlock()
}

func (h *Handle) ReadAt(ctx context.Context, offset, length int64) ([]byte, error) {
	if h == nil || h.manager == nil {
		return nil, errors.New("stream handle is nil")
	}
	return h.manager.readAt(ctx, h, offset, length)
}

func (m *Manager) readAt(ctx context.Context, handle *Handle, offset, length int64) ([]byte, error) {
	if m == nil || handle == nil {
		return nil, errors.New("stream manager is nil")
	}
	file := handle.file
	if file.FSID == "" {
		return nil, errors.New("fsid is required")
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("offset and length must be non-negative")
	}
	if length == 0 || file.Size > 0 && offset >= file.Size {
		return []byte{}, nil
	}
	if file.Size > 0 && offset+length > file.Size {
		length = file.Size - offset
	}

	epoch, mode, streamRead, event, logEvent, err := m.prepareRead(handle, offset, length)
	if err != nil {
		return nil, err
	}
	if logEvent && event == readEventSeek {
		log.Printf("stream seek fsid=%q epoch=%d offset=%d", file.FSID, epoch, offset)
	} else if logEvent && event == readEventStart {
		log.Printf("stream start fsid=%q epoch=%d offset=%d", file.FSID, epoch, offset)
	}
	if mode == modeStream && streamRead {
		m.scheduleBuffer(file, offset+length, epoch)
	} else if mode == modeProbe && m.isProbeEdge(file, offset) {
		m.scheduleProbe(file, offset, epoch)
	}

	first := offset / m.cfg.ChunkSize
	last := (offset + length - 1) / m.cfg.ChunkSize
	chunks := make([][]byte, last-first+1)
	touchCache := mode != modeStream || !streamRead
	for index := first; index <= last; index++ {
		data, err := m.waitChunk(ctx, file, index, epoch, priorityForeground, touchCache)
		if err != nil {
			return nil, err
		}
		chunks[index-first] = data
	}
	result := make([]byte, length)
	written := int64(0)
	for index, data := range chunks {
		chunkIndex := first + int64(index)
		chunkOffset := chunkIndex * m.cfg.ChunkSize
		start := int64(0)
		if offset > chunkOffset {
			start = offset - chunkOffset
		}
		available := int64(len(data)) - start
		need := length - written
		if available > need {
			available = need
		}
		if available <= 0 {
			return nil, io.ErrUnexpectedEOF
		}
		copy(result[written:written+available], data[start:start+available])
		written += available
	}
	if written != length {
		return nil, io.ErrUnexpectedEOF
	}
	if mode == modeStream && streamRead {
		m.scheduleBuffer(file, offset+length, epoch)
		m.queueSummary(file, offset+length, epoch)
	}
	return result, nil
}

func (m *Manager) isProbeEdge(file File, offset int64) bool {
	if file.Size <= 0 || offset < 0 {
		return false
	}
	const edge = int64(8 << 20)
	return offset < edge || offset >= file.Size-edge
}

func (m *Manager) scheduleProbe(file File, offset, epoch int64) {
	if m == nil || m.cfg.ChunkSize <= 0 || file.Size <= 0 || offset < 0 || offset >= file.Size {
		return
	}
	const prefetchSize = int64(16 << 20)
	endOffset := offset + prefetchSize
	if endOffset > file.Size {
		endOffset = file.Size
	}
	start := offset / m.cfg.ChunkSize
	end := (endOffset - 1) / m.cfg.ChunkSize
	for index := start; index <= end; index++ {
		m.ensureBackgroundTask(file, index, epoch, priorityNear)
	}
}

func (m *Manager) queueSummary(file File, cursor, epoch int64) {
	request := summaryRequest{file: file, cursor: cursor, epoch: epoch}
	select {
	case m.summaries <- request:
	default:
	}
}

func (m *Manager) summaryWorker() {
	defer m.wg.Done()
	for {
		select {
		case request := <-m.summaries:
			m.maybeLogSummary(request.file, request.cursor, request.epoch)
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) maybeLogSummary(file File, cursor, epoch int64) {
	now := time.Now()
	m.mu.Lock()
	s := m.sessions[chunkVersion(file, m.cfg.ChunkSize)]
	if s == nil || now.Sub(s.lastSummary) < 5*time.Second {
		m.mu.Unlock()
		return
	}
	s.lastSummary = now
	inflight := s.inflight
	workerLimit := m.sessionWorkerLimitLocked(s)
	sessionHedges := s.hedges
	dlinkRefreshes := s.dlinkRefreshes
	m.mu.Unlock()

	bufferAhead := m.BufferAhead(file, cursor)
	stats := m.Stats()
	memoryBytes, diskBytes := m.store.usage()
	log.Printf("stream summary fsid=%q epoch=%d buffer_ahead=%d buffer_low=%t low_watermark=%d inflight=%d worker_limit=%d cache_memory=%d cache_disk=%d downloaded=%d retries=%d hedges=%d session_hedges=%d dlink_refreshes=%d", file.FSID, epoch, bufferAhead, bufferAhead < m.cfg.LowWatermark, m.cfg.LowWatermark, inflight, workerLimit, memoryBytes, diskBytes, stats.Downloaded, stats.Retries, stats.Hedges, sessionHedges, dlinkRefreshes)
}

func (m *Manager) sessionLocked(file File) *session {
	version := chunkVersion(file, m.cfg.ChunkSize)
	s := m.sessions[version]
	if s == nil {
		s = &session{file: file, lastAccess: time.Now()}
		m.sessions[version] = s
	}
	return s
}

type readEvent uint8

const (
	readEventNone readEvent = iota
	readEventStart
	readEventSeek
)

type observation struct {
	mode       accessMode
	event      readEvent
	streamRead bool
}

func (m *Manager) prepareRead(handle *Handle, offset, length int64) (int64, accessMode, bool, readEvent, bool, error) {
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.released {
		return 0, modeProbe, false, readEventNone, false, errors.New("stream handle is released")
	}
	observed := m.observeHandleLocked(handle, offset, length)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, modeProbe, false, readEventNone, false, errors.New("stream manager is closed")
	}
	s := m.sessionLocked(handle.file)
	version := chunkVersion(handle.file, m.cfg.ChunkSize)
	event := observed.event
	previousEpoch := s.epoch
	previousActiveHandle := s.activeHandle
	withinWindow := m.inScheduledWindowLocked(s, offset)
	independentRead := false
	if withinWindow && event == readEventSeek {
		event = readEventNone
	}
	if event == readEventSeek {
		if s.activeHandle != 0 && s.activeHandle != handle.id {
			// A separate metadata/range handle must not cancel the active playback window.
			event = readEventNone
			independentRead = true
		} else {
			s.epoch++
			m.cancelSessionTasksLocked(version, s.epoch)
			s.activeHandle = handle.id
			s.streamAnchor = offset
		}
	} else if event == readEventStart {
		if s.activeHandle != 0 && s.activeHandle != handle.id && !withinWindow {
			s.epoch++
			m.cancelSessionTasksLocked(version, s.epoch)
			s.activeHandle = handle.id
			s.streamAnchor = offset
		} else if s.activeHandle == 0 {
			s.activeHandle = handle.id
			s.streamAnchor = offset
		}
	} else if observed.mode == modeStream && s.activeHandle == 0 {
		s.activeHandle = handle.id
		s.streamAnchor = offset
		event = readEventStart
	}
	handle.epoch = s.epoch
	s.lastAccess = time.Now()
	streamRead := !independentRead && observed.streamRead && (s.activeHandle == handle.id || withinWindow)
	if streamRead {
		if end := offset + length; end > s.cursor {
			s.cursor = end
		}
	}
	logEvent := s.epoch != previousEpoch || (event == readEventStart && previousActiveHandle == 0 && s.activeHandle == handle.id)
	return s.epoch, observed.mode, streamRead, event, logEvent, nil
}

func (m *Manager) inScheduledWindowLocked(s *session, offset int64) bool {
	if m == nil || s == nil || !s.scheduleSet || s.scheduleEpoch != s.epoch || m.cfg.ChunkSize <= 0 || offset < 0 {
		return false
	}
	index := offset / m.cfg.ChunkSize
	return index >= s.scheduledFloor && index <= s.scheduledEnd
}

func (m *Manager) observeHandleLocked(handle *Handle, offset, length int64) observation {
	probeThreshold := int64(128 << 10)
	probeEdge := int64(1 << 20)
	headProbeEdge := int64(0)
	if handle.file.Size >= 256<<20 {
		headProbeEdge = 8 << 20
	}
	if handle.file.Size < probeEdge*4 {
		probeThreshold = m.cfg.ChunkSize / 2
		if probeThreshold <= 0 {
			probeThreshold = 1
		}
		probeEdge = m.cfg.ChunkSize
	}
	tailProbe := handle.file.Size > 0 && length <= probeThreshold && offset >= handle.file.Size-probeEdge
	if handle.lastOffset < 0 && tailProbe {
		handle.tailProbe = true
		handle.lastOffset = offset
		handle.lastLength = length
		return observation{mode: modeProbe}
	}
	if handle.tailProbe {
		if tailProbe {
			handle.lastOffset = offset
			handle.lastLength = length
			return observation{mode: modeProbe}
		}
		handle.tailProbe = false
		handle.lastOffset = -1
		handle.lastLength = 0
		handle.forwardReads = 0
		handle.mode = modeProbe
	}
	if headProbeEdge > 0 && handle.mode != modeStream && !tailProbe && offset < headProbeEdge {
		handle.lastOffset = offset
		handle.lastLength = length
		handle.forwardReads = 0
		return observation{mode: modeProbe}
	}

	event := readEventNone
	if handle.lastOffset >= 0 {
		expected := handle.lastOffset + handle.lastLength
		distance := offset - expected
		edgeProbe := length <= probeThreshold && (offset < probeEdge || tailProbe)
		probeJump := edgeProbe && offset != handle.lastOffset && (distance < 0 || distance > m.cfg.SeekThreshold)
		if probeJump {
			return observation{mode: handle.mode}
		}
		forwardSeekThreshold := m.cfg.SeekThreshold
		if m.cfg.TargetBuffer > forwardSeekThreshold {
			forwardSeekThreshold = m.cfg.TargetBuffer
		}
		seek := offset != handle.lastOffset && handle.mode == modeStream && (distance < 0 || distance > forwardSeekThreshold)
		if seek {
			handle.forwardReads = 1
			handle.mode = modeProbe
			event = readEventSeek
		} else if offset == handle.lastOffset {
			// Concurrent duplicate reads on one handle do not change its access mode.
		} else if distance >= 0 && distance <= m.cfg.ForwardGap {
			handle.forwardReads++
		} else {
			handle.forwardReads = 1
		}
	} else {
		handle.forwardReads = 1
	}
	if handle.forwardReads >= 2 && handle.mode != modeStream {
		handle.mode = modeStream
		event = readEventStart
	}
	handle.lastOffset = offset
	handle.lastLength = length
	return observation{mode: handle.mode, event: event, streamRead: handle.mode == modeStream}
}

func (m *Manager) cleanupSessionLocked(version string, s *session) {
	if s == nil || s.refs > 0 || s.inflight > 0 {
		return
	}
	for _, task := range m.tasks {
		if task.key.version == version && !task.finished {
			return
		}
	}
	delete(m.sessions, version)
}

func (m *Manager) cancelSessionTasksLocked(version string, currentEpoch int64) {
	for _, task := range m.tasks {
		if task.key.version == version && task.epoch < currentEpoch && task.priority != priorityForeground && task.cancel != nil {
			task.cancel()
		}
	}
}

func (m *Manager) scheduleBuffer(file File, cursor, epoch int64) {
	if cursor >= file.Size && file.Size > 0 {
		return
	}
	start := cursor / m.cfg.ChunkSize
	endOffset := cursor + m.cfg.TargetBuffer
	if file.Size > 0 && endOffset > file.Size {
		endOffset = file.Size
	}
	if endOffset <= cursor {
		return
	}
	end := (endOffset - 1) / m.cfg.ChunkSize
	nearEnd := (cursor + 64<<20) / m.cfg.ChunkSize
	if nearEnd > end {
		nearEnd = end
	}
	floorOffset := cursor - m.cfg.BackBuffer
	if floorOffset < 0 {
		floorOffset = 0
	}
	floor := floorOffset / m.cfg.ChunkSize

	version := chunkVersion(file, m.cfg.ChunkSize)
	m.mu.Lock()
	s := m.sessions[version]
	if s == nil || s.epoch != epoch || s.activeHandle == 0 {
		m.mu.Unlock()
		return
	}
	reset := !s.scheduleSet || s.scheduleEpoch != epoch
	previousFloor := s.scheduledFloor
	previousNear := s.scheduledNear
	previousEnd := s.scheduledEnd
	if reset {
		s.scheduleSet = true
		s.scheduleEpoch = epoch
		s.scheduledFloor = floor
		s.scheduledNear = nearEnd
		s.scheduledEnd = end
	} else {
		if floor > s.scheduledFloor {
			s.scheduledFloor = floor
		}
		if nearEnd > s.scheduledNear {
			s.scheduledNear = nearEnd
		}
		if end > s.scheduledEnd {
			s.scheduledEnd = end
		}
	}
	m.mu.Unlock()
	if reset {
		m.store.pruneMemoryWindow(version, floor, end)
	} else if floor > previousFloor {
		m.store.removeMemoryRange(version, previousFloor, floor)
	}

	if reset {
		for index := start; index <= nearEnd; index++ {
			m.ensureBackgroundTask(file, index, epoch, priorityNear)
		}
		for index := nearEnd + 1; index <= end; index++ {
			m.ensureBackgroundTask(file, index, epoch, priorityAhead)
		}
	} else {
		promoteStart := previousNear + 1
		if promoteStart < start {
			promoteStart = start
		}
		promoteEnd := nearEnd
		if promoteEnd > previousEnd {
			promoteEnd = previousEnd
		}
		for index := promoteStart; index <= promoteEnd; index++ {
			m.ensureBackgroundTask(file, index, epoch, priorityNear)
		}
		for index := previousEnd + 1; index <= end; index++ {
			priority := priorityAhead
			if index <= nearEnd {
				priority = priorityNear
			}
			m.ensureBackgroundTask(file, index, epoch, priority)
		}
	}
	if !reset || m.cfg.BackBuffer <= 0 || cursor <= 0 {
		return
	}
	backStart := floor
	backEnd := (cursor - 1) / m.cfg.ChunkSize
	for index := backStart; index <= backEnd; index++ {
		m.ensureBackgroundTask(file, index, epoch, priorityBack)
	}
}

func (m *Manager) ensureBackgroundTask(file File, index, epoch int64, priority priority) {
	key := chunkKey{version: chunkVersion(file, m.cfg.ChunkSize), index: index}
	if _, ok := m.store.length(key); ok {
		return
	}
	m.ensureTask(file, index, epoch, priority)
}

func (m *Manager) waitChunk(ctx context.Context, file File, index, epoch int64, priority priority, touchCache bool) ([]byte, error) {
	key := chunkKey{version: chunkVersion(file, m.cfg.ChunkSize), index: index}
	var data []byte
	var ok bool
	if touchCache {
		data, ok = m.store.get(key)
	} else {
		data, ok = m.store.getWithoutTouch(key)
	}
	if ok {
		return data, nil
	}
	task := m.ensureTask(file, index, epoch, priority)
	if task == nil {
		return nil, errors.New("stream manager is closed")
	}
	if priority == priorityForeground {
		m.markForeground(task)
	}
	if priority == priorityForeground && m.cfg.Hedge {
		return m.waitForegroundWithHedge(ctx, task)
	}
	select {
	case <-task.done:
		return task.data, task.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Manager) markForeground(task *task) {
	if task == nil {
		return
	}
	m.mu.Lock()
	if !task.finished {
		task.foreground = true
	}
	m.mu.Unlock()
}

func (m *Manager) waitForegroundWithHedge(ctx context.Context, task *task) ([]byte, error) {
	timer := time.NewTimer(m.hedgeDelay())
	defer timer.Stop()
	select {
	case <-task.done:
		return task.data, task.err
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	m.startHedge(task)
	select {
	case <-task.done:
		return task.data, task.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Manager) hedgeDelay() time.Duration {
	m.mu.Lock()
	samples := append([]time.Duration(nil), m.latencies...)
	m.mu.Unlock()
	if len(samples) == 0 {
		return m.cfg.HedgeMinDelay
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p95 := samples[(len(samples)*95-1)/100]
	delay := p95 + p95/2
	if delay < m.cfg.HedgeMinDelay {
		return m.cfg.HedgeMinDelay
	}
	if delay > m.cfg.HedgeMaxDelay {
		return m.cfg.HedgeMaxDelay
	}
	return delay
}

type downloadLinkInvalidator interface {
	InvalidateDownloadLink(fsid string)
}

func (m *Manager) startHedge(task *task) {
	m.mu.Lock()
	if m.closed || task.finished || task.hedged || task.ctx.Err() != nil {
		m.mu.Unlock()
		return
	}
	task.hedged = true
	hedgeCtx, hedgeCancel := context.WithCancel(m.ctx)
	task.hedgeCancel = hedgeCancel
	m.hedges.Add(1)
	invalidate := false
	if s := m.sessions[task.key.version]; s != nil {
		s.hedges++
		s.slowStreak++
		if s.slowStreak >= 3 {
			s.slowStreak = 0
			s.dlinkRefreshes++
			invalidate = true
		}
	}
	m.wg.Add(1)
	m.mu.Unlock()

	if invalidate {
		if invalidator, ok := m.reader.(downloadLinkInvalidator); ok {
			invalidator.InvalidateDownloadLink(task.file.FSID)
			log.Printf("stream dlink invalidated fsid=%q reason=consecutive-slow-foreground", task.file.FSID)
		}
	}
	go func() {
		defer m.wg.Done()
		defer hedgeCancel()
		data, err := m.reader.ReadStreamRange(hedgeCtx, task.file.FSID, task.offset, task.length)
		if err != nil || int64(len(data)) != task.length {
			return
		}
		m.publishHedge(task, data)
	}()
}

func (m *Manager) publishHedge(task *task, data []byte) bool {
	m.store.putMemory(task.key, data)
	m.mu.Lock()
	if task.finished {
		m.mu.Unlock()
		return false
	}
	task.finished = true
	task.data = data
	task.hedgeCancel = nil
	if task.cancel != nil {
		task.cancel()
	}
	if m.tasks[task.key] == task {
		delete(m.tasks, task.key)
	}
	close(task.done)
	m.mu.Unlock()
	m.downloaded.Add(int64(len(data)))
	m.queueDiskWrite(task.key, data)
	return true
}

func (m *Manager) ensureTask(file File, index, epoch int64, priority priority) *task {
	key := chunkKey{version: chunkVersion(file, m.cfg.ChunkSize), index: index}
	if data, ok := m.store.get(key); ok {
		return &task{key: key, file: file, epoch: epoch, priority: priority, done: closedChannel(), data: data, finished: true}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if existing := m.tasks[key]; existing != nil && !existing.finished && existing.ctx.Err() == nil {
		if priority < existing.priority && !existing.started {
			existing.priority = priority
			heap.Fix(&m.queue, existing.index)
		}
		return existing
	}
	offset := index * m.cfg.ChunkSize
	length := m.cfg.ChunkSize
	if file.Size > 0 && offset+length > file.Size {
		length = file.Size - offset
	}
	if length <= 0 {
		return &task{key: key, file: file, epoch: epoch, priority: priority, done: closedChannel(), finished: true}
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.sequence++
	task := &task{key: key, file: file, offset: offset, length: length, priority: priority, epoch: epoch, seq: m.sequence, index: -1, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	m.tasks[key] = task
	heap.Push(&m.queue, task)
	m.cond.Signal()
	return task
}

func closedChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		task := m.nextTask()
		if task == nil {
			return
		}
		data, err := m.download(task)
		m.finishTask(task, data, err)
	}
}

func (m *Manager) nextTask() *task {
	m.mu.Lock()
	defer m.mu.Unlock()
	for !m.closed {
		for m.queue.Len() == 0 && !m.closed {
			m.cond.Wait()
		}
		if m.closed {
			return nil
		}
		var skipped []*task
		for m.queue.Len() > 0 {
			task := heap.Pop(&m.queue).(*task)
			if task.finished {
				continue
			}
			if task.ctx.Err() != nil {
				m.discardCanceledTaskLocked(task)
				continue
			}
			s := m.sessions[task.key.version]
			if s != nil && task.priority != priorityForeground && s.inflight >= m.sessionWorkerLimitLocked(s) {
				skipped = append(skipped, task)
				continue
			}
			task.started = true
			task.startedAt = time.Now()
			if s != nil {
				s.inflight++
			}
			for _, item := range skipped {
				heap.Push(&m.queue, item)
			}
			return task
		}
		for _, item := range skipped {
			heap.Push(&m.queue, item)
		}
		m.cond.Wait()
	}
	return nil
}

func (m *Manager) sessionWorkerLimitLocked(target *session) int {
	if target == nil {
		return m.cfg.Workers
	}
	activeSessions := 0
	for _, current := range m.sessions {
		if current != nil && current.refs > 0 && current.activeHandle != 0 {
			activeSessions++
			if activeSessions > 1 {
				return m.cfg.SessionWorkers
			}
		}
	}
	return m.cfg.Workers
}

func (m *Manager) discardCanceledTaskLocked(task *task) {
	if task == nil || task.finished {
		return
	}
	task.finished = true
	task.err = task.ctx.Err()
	if m.tasks[task.key] == task {
		delete(m.tasks, task.key)
	}
	close(task.done)
	m.cleanupSessionLocked(task.key.version, m.sessions[task.key.version])
}

func (m *Manager) download(task *task) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		data, err := m.reader.ReadStreamRange(task.ctx, task.file.FSID, task.offset, task.length)
		if err == nil && int64(len(data)) == task.length {
			m.downloaded.Add(int64(len(data)))
			return data, nil
		}
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		lastErr = err
		if task.ctx.Err() != nil {
			return nil, task.ctx.Err()
		}
		m.retries.Add(1)
	}
	return nil, fmt.Errorf("stream chunk offset=%d length=%d: %w", task.offset, task.length, lastErr)
}

func (m *Manager) finishTask(task *task, data []byte, err error) {
	if err == nil {
		m.store.putMemory(task.key, data)
	}
	m.mu.Lock()
	if s := m.sessions[task.key.version]; s != nil && task.started && s.inflight > 0 {
		s.inflight--
	}
	if task.finished {
		m.cleanupSessionLocked(task.key.version, m.sessions[task.key.version])
		m.mu.Unlock()
		m.cond.Broadcast()
		return
	}
	if task.hedgeCancel != nil {
		task.hedgeCancel()
		task.hedgeCancel = nil
	}
	task.finished = true
	task.data = data
	task.err = err
	if m.tasks[task.key] == task {
		delete(m.tasks, task.key)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("chunk failed fsid=%q offset=%d: %v", task.file.FSID, task.offset, err)
	}
	if err == nil && !task.startedAt.IsZero() {
		m.recordLatencyLocked(time.Since(task.startedAt))
	}
	if task.foreground && !task.hedged {
		if s := m.sessions[task.key.version]; s != nil {
			s.slowStreak = 0
		}
	}
	close(task.done)
	m.cleanupSessionLocked(task.key.version, m.sessions[task.key.version])
	m.mu.Unlock()
	m.cond.Broadcast()
	if err == nil {
		m.queueDiskWrite(task.key, data)
	}
}

func (m *Manager) recordLatencyLocked(latency time.Duration) {
	const maxSamples = 32
	if latency <= 0 {
		return
	}
	if len(m.latencies) < maxSamples {
		m.latencies = append(m.latencies, latency)
		return
	}
	m.latencies[m.latencyNext%maxSamples] = latency
	m.latencyNext++
}

func (m *Manager) queueDiskWrite(key chunkKey, data []byte) {
	if m.store.diskPath == "" || m.store.diskLimit <= 0 || len(data) == 0 {
		return
	}
	select {
	case m.diskWrites <- cacheEntry{key: key, data: data}:
	default:
		log.Printf("stream disk cache queue full version=%q chunk=%d", key.version, key.index)
	}
}

func (m *Manager) diskWriter() {
	defer m.diskWG.Done()
	for entry := range m.diskWrites {
		if err := m.store.putDisk(entry.key, entry.data); err != nil {
			log.Printf("stream disk cache write failed version=%q chunk=%d: %v", entry.key.version, entry.key.index, err)
		}
	}
}

type Stats struct {
	Downloaded int64
	Retries    int64
	Hedges     int64
}

func (m *Manager) Stats() Stats {
	if m == nil {
		return Stats{}
	}
	return Stats{Downloaded: m.downloaded.Load(), Retries: m.retries.Load(), Hedges: m.hedges.Load()}
}

func (m *Manager) LowWatermark() int64 {
	if m == nil {
		return 0
	}
	return m.cfg.LowWatermark
}

func (m *Manager) BufferAhead(file File, cursor int64) int64 {
	if m == nil || cursor < 0 {
		return 0
	}
	startIndex := cursor / m.cfg.ChunkSize
	within := cursor % m.cfg.ChunkSize
	var ready int64
	for index := startIndex; ; index++ {
		chunkLength, ok := m.store.length(chunkKey{version: chunkVersion(file, m.cfg.ChunkSize), index: index})
		if !ok {
			break
		}
		length := chunkLength
		if index == startIndex {
			if within >= length {
				break
			}
			length -= within
		}
		ready += length
		if chunkLength < m.cfg.ChunkSize {
			break
		}
	}
	return ready
}
