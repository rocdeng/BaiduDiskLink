package remote

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"baidudisklink/internal/baidu"
)

import "context"

type Reader struct {
	mu            sync.RWMutex
	client        baidu.Client
	links         map[string]cachedLink
	cached        map[cacheKey]cachedRead
	cacheByFSID   map[string]map[cacheKey]struct{}
	cacheOrder    []cacheKey
	cacheBytes    int64
	cacheLimit    int64
	inflight      map[cacheKey]*inflightRead
	exactInflight map[exactRangeKey]*exactInflightRead
	prefetchSlots chan struct{}
	concurrency   int
	chunkSize     int64
}

type cacheKey struct {
	fsid   string
	offset int64
}

type exactRangeKey struct {
	fsid   string
	offset int64
	length int64
}

type cachedLink struct {
	link      baidu.DownloadLink
	expiresAt time.Time
}

type cachedRead struct {
	fsid   string
	offset int64
	data   []byte
}

type inflightRead struct {
	done chan struct{}
	data []byte
	err  error
}

type exactInflightRead struct {
	done      chan struct{}
	data      []byte
	err       error
	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	waiters   int
	completed bool
}

const prefetchBytes = 8 << 20
const maxCachedWindows = 8
const maxBackgroundPrefetches = 4
const maxBackgroundChunkConcurrency = 2

func NewReader(client baidu.Client) *Reader {
	return &Reader{
		client:        client,
		links:         make(map[string]cachedLink),
		cached:        make(map[cacheKey]cachedRead),
		cacheByFSID:   make(map[string]map[cacheKey]struct{}),
		inflight:      make(map[cacheKey]*inflightRead),
		exactInflight: make(map[exactRangeKey]*exactInflightRead),
		prefetchSlots: make(chan struct{}, maxBackgroundPrefetches),
		cacheLimit:    64 << 20,
		concurrency:   1,
		chunkSize:     4 << 20,
	}
}

func (r *Reader) SetDownloadOptions(concurrency int, chunkSize int64) {
	if r == nil {
		return
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if chunkSize <= 0 {
		chunkSize = 4 << 20
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.concurrency = concurrency
	r.chunkSize = chunkSize
	r.clearReadCacheLocked()
}

func (r *Reader) SetClient(client baidu.Client) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = client
	r.links = make(map[string]cachedLink)
	r.clearReadCacheLocked()
}

func (r *Reader) List(path string) ([]baidu.RemoteEntry, error) {
	client := r.currentClient()
	if r == nil || client == nil {
		return nil, nil
	}
	return client.List(path)
}

func (r *Reader) Delete(paths []string) error {
	client := r.currentClient()
	if r == nil || client == nil {
		return nil
	}
	return client.Delete(paths)
}

func (r *Reader) ReadRange(ctx context.Context, fsid string, offset, length int64) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("length must be non-negative")
	}
	if fsid == "" {
		return nil, errors.New("fsid is required")
	}
	client := r.currentClient()
	if r == nil || client == nil {
		return make([]byte, length), nil
	}
	if _, err := r.downloadLink(fsid, client); err != nil {
		return nil, err
	}
	if data, ok := r.readCached(fsid, offset, length); ok {
		return data, nil
	}
	if inflight := r.joinExactInflight(exactRangeKey{fsid: fsid, offset: offset, length: length}); inflight != nil {
		data, err := r.waitExactInflight(ctx, inflight)
		if err == nil || ctx.Err() != nil || !errors.Is(err, context.Canceled) {
			return data, err
		}
	}
	data, err := r.readWithOptions(ctx, client, fsid, offset, length)
	if err != nil || int64(len(data)) >= length {
		return data, err
	}
	missingOffset := offset + int64(len(data))
	missingLength := length - int64(len(data))
	missing, err := r.ReadExactRange(ctx, fsid, missingOffset, missingLength)
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(data)+len(missing))
	copy(result, data)
	copy(result[len(data):], missing)
	if int64(len(result)) != length {
		return nil, fmt.Errorf("read range at %d returned %d of %d bytes: %w", offset, len(result), length, io.ErrUnexpectedEOF)
	}
	return result, nil
}

func (r *Reader) Prefetch(ctx context.Context, fsid string, offset, length int64) error {
	if r == nil || length <= 0 {
		return nil
	}
	if fsid == "" {
		return errors.New("fsid is required")
	}
	if offset < 0 {
		return errors.New("offset must be non-negative")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	client := r.currentClient()
	if client == nil {
		return nil
	}
	if _, err := r.downloadLink(fsid, client); err != nil {
		return err
	}
	key := exactRangeKey{fsid: fsid, offset: offset, length: length}
	for attempt := 0; attempt < 2; attempt++ {
		if _, ok := r.readCached(fsid, offset, length); ok {
			return nil
		}
		inflight, taskCtx, owner := r.beginExactInflight(key)
		if owner {
			go r.runQueuedExactPrefetch(taskCtx, client, key, inflight)
		}
		_, err := r.waitExactInflight(ctx, inflight)
		if err == nil || ctx.Err() != nil || !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return context.Canceled
}

func (r *Reader) runQueuedExactPrefetch(ctx context.Context, client baidu.Client, key exactRangeKey, inflight *exactInflightRead) {
	select {
	case r.prefetchSlots <- struct{}{}:
		if !r.startExactTask(ctx, client, key, inflight, maxBackgroundChunkConcurrency, func() { <-r.prefetchSlots }) {
			<-r.prefetchSlots
		}
	case <-ctx.Done():
		inflight.startOnce.Do(func() {
			r.finishExactInflight(key, inflight, nil, ctx.Err())
		})
	}
}

func (r *Reader) startExactTask(ctx context.Context, client baidu.Client, key exactRangeKey, inflight *exactInflightRead, concurrencyLimit int, onDone func()) bool {
	started := false
	inflight.startOnce.Do(func() {
		started = true
		go func() {
			if onDone != nil {
				defer onDone()
			}
			data, err := r.fetchExactRange(ctx, client, key.fsid, key.offset, key.length, concurrencyLimit)
			r.finishExactInflight(key, inflight, data, err)
		}()
	})
	return started
}

func (r *Reader) ReadExactRange(ctx context.Context, fsid string, offset, length int64) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("length must be non-negative")
	}
	if fsid == "" {
		return nil, errors.New("fsid is required")
	}
	client := r.currentClient()
	if r == nil || client == nil {
		return make([]byte, length), nil
	}
	if _, err := r.downloadLink(fsid, client); err != nil {
		return nil, err
	}
	if data, ok := r.readCached(fsid, offset, length); ok {
		return data, nil
	}
	key := exactRangeKey{fsid: fsid, offset: offset, length: length}
	for attempt := 0; attempt < 2; attempt++ {
		inflight, taskCtx, _ := r.beginExactInflight(key)
		r.startExactTask(taskCtx, client, key, inflight, 0, nil)
		data, err := r.waitExactInflight(ctx, inflight)
		if err == nil || ctx.Err() != nil || !errors.Is(err, context.Canceled) {
			return data, err
		}
	}
	return nil, context.Canceled
}

func (r *Reader) fetchExactRange(ctx context.Context, client baidu.Client, fsid string, offset, length int64, concurrencyLimit int) ([]byte, error) {
	r.mu.RLock()
	concurrency := r.concurrency
	chunkSize := r.chunkSize
	r.mu.RUnlock()
	if concurrencyLimit > 0 && concurrency > concurrencyLimit {
		concurrency = concurrencyLimit
	}
	if concurrency > 1 && length > chunkSize {
		data, err := r.readConcurrent(ctx, client, fsid, offset, length, chunkSize, concurrency)
		if err == nil {
			r.storeCached(fsid, offset, data)
		}
		return data, err
	}
	data := make([]byte, length)
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		n, err := client.ReadRange(ctx, fsid, offset, data)
		if err == nil && n != len(data) {
			err = fmt.Errorf("read exact range at %d returned %d of %d bytes: %w", offset, n, len(data), io.ErrUnexpectedEOF)
		}
		if err == nil {
			data = data[:n]
			r.storeCached(fsid, offset, data)
			return data, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		_ = client.RefreshAuth()
	}
	return nil, lastErr
}

func (r *Reader) joinExactInflight(key exactRangeKey) *exactInflightRead {
	r.mu.Lock()
	defer r.mu.Unlock()
	inflight := r.exactInflight[key]
	if inflight != nil {
		inflight.waiters++
	}
	return inflight
}

func (r *Reader) beginExactInflight(key exactRangeKey) (*exactInflightRead, context.Context, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inflight := r.exactInflight[key]; inflight != nil {
		inflight.waiters++
		return inflight, inflight.ctx, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	inflight := &exactInflightRead{done: make(chan struct{}), ctx: ctx, cancel: cancel, waiters: 1}
	r.exactInflight[key] = inflight
	return inflight, ctx, true
}

func (r *Reader) finishExactInflight(key exactRangeKey, inflight *exactInflightRead, data []byte, err error) {
	r.mu.Lock()
	if r.exactInflight[key] == inflight {
		delete(r.exactInflight, key)
	}
	inflight.data = data
	inflight.err = err
	inflight.completed = true
	inflight.cancel = nil
	close(inflight.done)
	r.mu.Unlock()
}

func (r *Reader) waitExactInflight(ctx context.Context, inflight *exactInflightRead) ([]byte, error) {
	defer r.releaseExactWaiter(inflight)
	select {
	case <-inflight.done:
		return inflight.data, inflight.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Reader) releaseExactWaiter(inflight *exactInflightRead) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inflight.waiters > 0 {
		inflight.waiters--
	}
	if inflight.waiters == 0 && !inflight.completed && inflight.cancel != nil {
		inflight.cancel()
	}
}

func (r *Reader) readCached(fsid string, offset, length int64) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r == nil {
		return nil, false
	}
	return r.readCachedLocked(fsid, offset, length)
}

func (r *Reader) storeCached(fsid string, offset int64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r == nil {
		return
	}
	r.storeCachedLocked(fsid, offset, data)
}

func (r *Reader) readCachedLocked(fsid string, offset, length int64) ([]byte, bool) {
	bucket := r.cacheByFSID[fsid]
	for key := range bucket {
		cached, ok := r.cached[key]
		if !ok || len(cached.data) == 0 || offset < cached.offset {
			continue
		}
		end := offset + length
		cachedEnd := cached.offset + int64(len(cached.data))
		if end > cachedEnd {
			continue
		}
		for index, ordered := range r.cacheOrder {
			if ordered == key && index != len(r.cacheOrder)-1 {
				copy(r.cacheOrder[index:], r.cacheOrder[index+1:])
				r.cacheOrder[len(r.cacheOrder)-1] = key
				break
			}
		}
		start := offset - cached.offset
		return cached.data[start : start+length], true
	}
	return nil, false
}

func (r *Reader) ReadCachedWindow(fsid string, offset, length int64) ([]byte, bool) {
	if length <= 0 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readCachedLocked(fsid, offset, length)
}

func (r *Reader) removeCachedKeyLocked(key cacheKey) {
	cached, ok := r.cached[key]
	if !ok {
		return
	}
	r.cacheBytes -= int64(len(cached.data))
	delete(r.cached, key)
	if bucket := r.cacheByFSID[key.fsid]; bucket != nil {
		delete(bucket, key)
		if len(bucket) == 0 {
			delete(r.cacheByFSID, key.fsid)
		}
	}
}

func (r *Reader) storeCachedLocked(fsid string, offset int64, data []byte) {
	if len(data) == 0 || int64(len(data)) > r.cacheLimit {
		return
	}
	key := cacheKey{fsid: fsid, offset: offset}
	if _, ok := r.cached[key]; ok {
		r.removeCachedKeyLocked(key)
		for index, existing := range r.cacheOrder {
			if existing == key {
				r.cacheOrder = append(r.cacheOrder[:index], r.cacheOrder[index+1:]...)
				break
			}
		}
	}
	r.cached[key] = cachedRead{fsid: fsid, offset: offset, data: data}
	bucket := r.cacheByFSID[fsid]
	if bucket == nil {
		bucket = make(map[cacheKey]struct{})
		r.cacheByFSID[fsid] = bucket
	}
	bucket[key] = struct{}{}
	r.cacheOrder = append(r.cacheOrder, key)
	r.cacheBytes += int64(len(data))
	for r.cacheBytes > r.cacheLimit && len(r.cacheOrder) > 0 {
		oldest := r.cacheOrder[0]
		r.cacheOrder = r.cacheOrder[1:]
		r.removeCachedKeyLocked(oldest)
	}
}

func (r *Reader) clearReadCacheLocked() {
	r.cached = make(map[cacheKey]cachedRead)
	r.cacheByFSID = make(map[string]map[cacheKey]struct{})
	r.cacheOrder = nil
	r.cacheBytes = 0
	r.inflight = make(map[cacheKey]*inflightRead)
	r.exactInflight = make(map[exactRangeKey]*exactInflightRead)
}

func (r *Reader) readWithOptions(ctx context.Context, client baidu.Client, fsid string, offset, length int64) ([]byte, error) {
	r.mu.RLock()
	concurrency := r.concurrency
	chunkSize := r.chunkSize
	r.mu.RUnlock()
	if concurrency <= 1 {
		return r.readSequential(ctx, client, fsid, offset, length, chunkSize)
	}
	return r.readConcurrent(ctx, client, fsid, offset, length, chunkSize, concurrency)
}

func (r *Reader) readSequential(ctx context.Context, client baidu.Client, fsid string, offset, length, chunkSize int64) ([]byte, error) {
	fetchLength := length
	if fetchLength < prefetchBytes {
		fetchLength = prefetchBytes
	}
	if fetchLength < chunkSize {
		fetchLength = chunkSize
	}
	fetchOffset := alignOffset(offset, fetchLength)
	if data, ok := r.readCached(fsid, offset, length); ok {
		return data, nil
	}
	if data, err, waited := r.waitInflight(ctx, fsid, fetchOffset, offset, length); waited {
		return data, err
	}
	inflight, owner := r.beginInflight(fsid, fetchOffset)
	if !owner {
		select {
		case <-inflight.done:
			if inflight.err != nil {
				return nil, inflight.err
			}
			return sliceForWindow(inflight.data, fetchOffset, offset, length), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	data := make([]byte, fetchLength)
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		n, err := client.ReadRange(ctx, fsid, fetchOffset, data)
		if err == nil {
			data = data[:n]
			r.finishInflight(fsid, fetchOffset, inflight, data, nil)
			r.storeCached(fsid, fetchOffset, data)
			return sliceForWindow(data, fetchOffset, offset, length), nil
		}
		lastErr = err
		if ctx.Err() != nil {
			r.finishInflight(fsid, fetchOffset, inflight, nil, ctx.Err())
			return nil, ctx.Err()
		}
		_ = client.RefreshAuth()
	}
	r.finishInflight(fsid, fetchOffset, inflight, nil, lastErr)
	return nil, lastErr
}

func alignOffset(offset, size int64) int64 {
	if size <= 0 || offset <= 0 {
		return 0
	}
	return offset / size * size
}

func sliceForWindow(data []byte, windowOffset, offset, length int64) []byte {
	start := offset - windowOffset
	if start < 0 {
		start = 0
	}
	if start >= int64(len(data)) {
		return []byte{}
	}
	end := start + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return data[start:end]
}
func (r *Reader) waitInflight(ctx context.Context, fsid string, windowOffset, offset, length int64) ([]byte, error, bool) {
	key := cacheKey{fsid: fsid, offset: windowOffset}
	r.mu.RLock()
	inflight := r.inflight[key]
	r.mu.RUnlock()
	if inflight == nil {
		return nil, nil, false
	}
	select {
	case <-inflight.done:
		if inflight.err != nil {
			return nil, inflight.err, true
		}
		return sliceForWindow(inflight.data, windowOffset, offset, length), nil, true
	case <-ctx.Done():
		return nil, ctx.Err(), true
	}
}

func (r *Reader) beginInflight(fsid string, windowOffset int64) (*inflightRead, bool) {
	key := cacheKey{fsid: fsid, offset: windowOffset}
	r.mu.Lock()
	defer r.mu.Unlock()
	if inflight := r.inflight[key]; inflight != nil {
		return inflight, false
	}
	inflight := &inflightRead{done: make(chan struct{})}
	r.inflight[key] = inflight
	return inflight, true
}

func (r *Reader) finishInflight(fsid string, windowOffset int64, inflight *inflightRead, data []byte, err error) {
	key := cacheKey{fsid: fsid, offset: windowOffset}
	r.mu.Lock()
	if r.inflight[key] == inflight {
		delete(r.inflight, key)
	}
	inflight.data = data
	inflight.err = err
	close(inflight.done)
	r.mu.Unlock()
}

func (r *Reader) readConcurrent(ctx context.Context, client baidu.Client, fsid string, offset, length, chunkSize int64, concurrency int) ([]byte, error) {
	if length <= 0 {
		return []byte{}, nil
	}
	if chunkSize <= 0 {
		chunkSize = 4 << 20
	}
	total := int((length + chunkSize - 1) / chunkSize)
	if total <= 1 {
		return r.readSequential(ctx, client, fsid, offset, length, chunkSize)
	}
	if concurrency > total {
		concurrency = total
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	buf := make([]byte, length)
	jobs := make(chan int)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case index, ok := <-jobs:
				if !ok {
					return
				}
				start := offset + int64(index)*chunkSize
				begin := int64(index) * chunkSize
				end := begin + chunkSize
				if end > length {
					end = length
				}
				dst := buf[begin:end]
				var err error
				for attempt := 0; attempt < 2; attempt++ {
					var n int
					n, err = client.ReadRange(ctx, fsid, start, dst)
					if err == nil && n != len(dst) {
						err = fmt.Errorf("read chunk at %d returned %d of %d bytes: %w", start, n, len(dst), io.ErrUnexpectedEOF)
					}
					if err == nil {
						break
					}
					if ctx.Err() != nil {
						break
					}
					_ = client.RefreshAuth()
				}
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
					return
				}
			}
		}
	}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go worker()
	}
	for index := 0; index < total; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			index = total
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.storeCached(fsid, offset, buf)
	return buf, nil
}

func (r *Reader) downloadLink(fsid string, client baidu.Client) (baidu.DownloadLink, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cached, ok := r.links[fsid]; ok && time.Now().Before(cached.expiresAt) {
		return cached.link, nil
	}
	link, err := client.GetDownloadLink(fsid)
	if err != nil {
		return baidu.DownloadLink{}, err
	}
	expiresAt := link.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = time.Now().Add(10 * time.Minute)
	}
	r.links[fsid] = cachedLink{link: link, expiresAt: expiresAt}
	return link, nil
}

func (r *Reader) currentClient() baidu.Client {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client
}
