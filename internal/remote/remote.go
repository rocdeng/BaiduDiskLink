package remote

import (
	"errors"
	"sync"
	"time"

	"baidudisklink/internal/baidu"
)

type Reader struct {
	mu          sync.RWMutex
	client      baidu.Client
	links       map[string]cachedLink
	cached      map[cacheKey]cachedRead
	cacheOrder  []cacheKey
	inflight    map[cacheKey]*inflightRead
	concurrency int
	chunkSize   int64
}

type cacheKey struct {
	fsid   string
	offset int64
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

const prefetchBytes = 8 << 20
const maxCachedWindows = 8

func NewReader(client baidu.Client) *Reader {
	return &Reader{
		client:      client,
		links:       make(map[string]cachedLink),
		cached:      make(map[cacheKey]cachedRead),
		inflight:    make(map[cacheKey]*inflightRead),
		concurrency: 1,
		chunkSize:   4 << 20,
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

func (r *Reader) ReadRange(fsid string, offset, length int64) ([]byte, error) {
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
	return r.readWithOptions(client, fsid, offset, length)
}

func (r *Reader) ReadExactRange(fsid string, offset, length int64) ([]byte, error) {
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
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		data, err := client.ReadRange(fsid, offset, length)
		if err == nil {
			r.storeCached(fsid, offset, data)
			return data, nil
		}
		lastErr = err
		_ = client.RefreshAuth()
	}
	return nil, lastErr
}

func (r *Reader) readCached(fsid string, offset, length int64) ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
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
	for _, cached := range r.cached {
		if len(cached.data) == 0 || cached.fsid != fsid {
			continue
		}
		if offset < cached.offset {
			continue
		}
		end := offset + length
		cachedEnd := cached.offset + int64(len(cached.data))
		if end > cachedEnd {
			continue
		}
		start := offset - cached.offset
		return append([]byte(nil), cached.data[start:start+length]...), true
	}
	return nil, false
}

func (r *Reader) ReadCachedWindow(fsid string, offset, length int64) ([]byte, bool) {
	if length <= 0 {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.readCachedLocked(fsid, offset, length)
}

func (r *Reader) storeCachedLocked(fsid string, offset int64, data []byte) {
	key := cacheKey{fsid: fsid, offset: offset}
	if _, ok := r.cached[key]; !ok {
		r.cacheOrder = append(r.cacheOrder, key)
	}
	r.cached[key] = cachedRead{
		fsid:   fsid,
		offset: offset,
		data:   append([]byte(nil), data...),
	}
	for len(r.cacheOrder) > maxCachedWindows {
		oldest := r.cacheOrder[0]
		r.cacheOrder = r.cacheOrder[1:]
		delete(r.cached, oldest)
	}
}

func (r *Reader) clearReadCacheLocked() {
	r.cached = make(map[cacheKey]cachedRead)
	r.cacheOrder = nil
	r.inflight = make(map[cacheKey]*inflightRead)
}

func sliceForRead(data []byte, offset, length int64) []byte {
	if length >= int64(len(data)) {
		return data
	}
	return append([]byte(nil), data[:length]...)
}

func (r *Reader) readWithOptions(client baidu.Client, fsid string, offset, length int64) ([]byte, error) {
	r.mu.RLock()
	concurrency := r.concurrency
	chunkSize := r.chunkSize
	r.mu.RUnlock()
	if concurrency <= 1 {
		return r.readSequential(client, fsid, offset, length, chunkSize)
	}
	return r.readConcurrent(client, fsid, offset, length, chunkSize, concurrency)
}

func (r *Reader) readSequential(client baidu.Client, fsid string, offset, length, chunkSize int64) ([]byte, error) {
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
	if data, err, waited := r.waitInflight(fsid, fetchOffset, offset, length); waited {
		return data, err
	}
	inflight, owner := r.beginInflight(fsid, fetchOffset)
	if !owner {
		<-inflight.done
		if inflight.err != nil {
			return nil, inflight.err
		}
		return sliceForWindow(inflight.data, fetchOffset, offset, length), nil
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		data, err := client.ReadRange(fsid, fetchOffset, fetchLength)
		if err == nil {
			r.finishInflight(fsid, fetchOffset, inflight, data, nil)
			r.storeCached(fsid, fetchOffset, data)
			return sliceForWindow(data, fetchOffset, offset, length), nil
		}
		lastErr = err
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
	return append([]byte(nil), data[start:end]...)
}

func (r *Reader) waitInflight(fsid string, windowOffset, offset, length int64) ([]byte, error, bool) {
	key := cacheKey{fsid: fsid, offset: windowOffset}
	r.mu.RLock()
	inflight := r.inflight[key]
	r.mu.RUnlock()
	if inflight == nil {
		return nil, nil, false
	}
	<-inflight.done
	if inflight.err != nil {
		return nil, inflight.err, true
	}
	return sliceForWindow(inflight.data, windowOffset, offset, length), nil, true
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
	inflight.data = append([]byte(nil), data...)
	inflight.err = err
	close(inflight.done)
	r.mu.Unlock()
}

func (r *Reader) readConcurrent(client baidu.Client, fsid string, offset, length, chunkSize int64, concurrency int) ([]byte, error) {
	if length <= 0 {
		return []byte{}, nil
	}
	if chunkSize <= 0 {
		chunkSize = 4 << 20
	}
	total := int((length + chunkSize - 1) / chunkSize)
	if total <= 1 {
		return r.readSequential(client, fsid, offset, length, chunkSize)
	}
	if concurrency > total {
		concurrency = total
	}
	buf := make([]byte, length)
	type result struct {
		index int
		data  []byte
		err   error
	}
	jobs := make(chan int, total)
	results := make(chan result, total)
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				start := offset + int64(index)*chunkSize
				want := chunkSize
				if remain := length - int64(index)*chunkSize; remain < want {
					want = remain
				}
				var data []byte
				var err error
				for attempt := 0; attempt < 2; attempt++ {
					data, err = client.ReadRange(fsid, start, want)
					if err == nil {
						break
					}
					_ = client.RefreshAuth()
				}
				results <- result{index: index, data: data, err: err}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		start := int64(res.index) * chunkSize
		copy(buf[start:start+int64(len(res.data))], res.data)
	}
	if firstErr != nil {
		return nil, firstErr
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
