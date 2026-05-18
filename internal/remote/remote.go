package remote

import (
	"errors"
	"sync"
	"time"

	"baidudisklink/internal/baidu"
)

type Reader struct {
	mu     sync.RWMutex
	client baidu.Client
	links  map[string]cachedLink
	cached cachedRead
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

func NewReader(client baidu.Client) *Reader {
	return &Reader{
		client: client,
		links:  make(map[string]cachedLink),
	}
}

func (r *Reader) SetClient(client baidu.Client) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = client
	r.links = make(map[string]cachedLink)
	r.cached = cachedRead{}
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
	fetchLength := length
	if fetchLength < minPrefetchBytes {
		fetchLength = minPrefetchBytes
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		data, err := client.ReadRange(fsid, offset, fetchLength)
		if err == nil {
			r.storeCached(fsid, offset, data)
			return sliceForRead(data, offset, length), nil
		}
		lastErr = err
		_ = client.RefreshAuth()
	}
	return nil, lastErr
}

const minPrefetchBytes int64 = 1024 * 1024

func (r *Reader) readCached(fsid string, offset, length int64) ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r == nil || len(r.cached.data) == 0 || r.cached.fsid != fsid {
		return nil, false
	}
	if offset < r.cached.offset {
		return nil, false
	}
	end := offset + length
	cachedEnd := r.cached.offset + int64(len(r.cached.data))
	if end > cachedEnd {
		return nil, false
	}
	start := offset - r.cached.offset
	return append([]byte(nil), r.cached.data[start:start+length]...), true
}

func (r *Reader) storeCached(fsid string, offset int64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r == nil {
		return
	}
	r.cached = cachedRead{
		fsid:   fsid,
		offset: offset,
		data:   append([]byte(nil), data...),
	}
}

func sliceForRead(data []byte, offset, length int64) []byte {
	if length >= int64(len(data)) {
		return data
	}
	return append([]byte(nil), data[:length]...)
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
