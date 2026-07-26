package remote

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"baidudisklink/internal/baidu"
)

import "context"

type stubClient struct {
	mu            sync.Mutex
	downloadCalls int
	readCalls     int
	readStarted   chan struct{}
	readRelease   chan struct{}
	failRead      bool
	shortRead     bool
	invalidated   int
}

func (s *stubClient) List(path string) ([]baidu.RemoteEntry, error) { return nil, nil }
func (s *stubClient) Stat(path string) (baidu.RemoteEntry, error)   { return baidu.RemoteEntry{}, nil }
func (s *stubClient) GetDownloadLink(fsid string) (baidu.DownloadLink, error) {
	s.downloadCalls++
	return baidu.DownloadLink{URL: "https://example.invalid/" + fsid, ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (s *stubClient) ReadRange(ctx context.Context, fsid string, offset int64, dst []byte) (int, error) {
	s.mu.Lock()
	s.readCalls++
	if s.readStarted != nil {
		select {
		case s.readStarted <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
	if s.readRelease != nil {
		select {
		case <-s.readRelease:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if s.failRead {
		return 0, errors.New("transient read failure")
	}
	if s.shortRead && len(dst) > 0 {
		return len(dst) - 1, nil
	}
	return len(dst), nil
}
func (s *stubClient) Delete(paths []string) error { return nil }
func (s *stubClient) RefreshAuth() error          { return nil }
func (s *stubClient) InvalidateDownloadLink(string) {
	s.mu.Lock()
	s.invalidated++
	s.mu.Unlock()
}

func sameBackingArray(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

func TestSliceForWindowSharesBackingArray(t *testing.T) {
	window := []byte("abcdefgh")
	got := sliceForWindow(window, 0, 0, 4)
	if !sameBackingArray(got, window) {
		t.Fatal("window slice copied backing storage")
	}
}

func TestFinishInflightPublishesSameWindow(t *testing.T) {
	r := NewReader(&stubClient{})
	inflight, owner := r.beginInflight("1", 0)
	if !owner {
		t.Fatal("expected inflight ownership")
	}
	window := []byte("abcdefgh")
	r.finishInflight("1", 0, inflight, window, nil)
	if !sameBackingArray(inflight.data, window) {
		t.Fatal("inflight publication copied backing storage")
	}
}

func TestReadCacheMaintainsFSIDIndex(t *testing.T) {
	r := NewReader(&stubClient{})
	r.cacheLimit = 6
	r.storeCached("one", 0, []byte("1111"))
	r.storeCached("two", 0, []byte("2222"))
	if len(r.cacheByFSID["one"]) != 0 {
		t.Fatalf("evicted FSID bucket remains: %#v", r.cacheByFSID)
	}
	if len(r.cacheByFSID["two"]) != 1 {
		t.Fatalf("active FSID bucket missing: %#v", r.cacheByFSID)
	}
	r.clearReadCacheLocked()
	if len(r.cacheByFSID) != 0 {
		t.Fatalf("clear left stale FSID index: %#v", r.cacheByFSID)
	}
}

func TestPrefetchPopulatesCache(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	if err := r.Prefetch(context.Background(), "1", 0, 4); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	before := client.readCalls
	client.mu.Unlock()
	if _, err := r.ReadExactRange(context.Background(), "1", 0, 4); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	after := client.readCalls
	client.mu.Unlock()
	if after != before {
		t.Fatalf("foreground read missed prefetched cache: before=%d after=%d", before, after)
	}
}

func TestPrefetchUsesConfiguredDownloadConcurrency(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	r.SetDownloadOptions(2, 4)
	if err := r.Prefetch(context.Background(), "1", 0, 8); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 2 {
		t.Fatalf("expected two concurrent chunks, got %d backend reads", readCalls)
	}
}

func TestPrefetchPublishesFastChunkBeforeSlowSibling(t *testing.T) {
	client := newChunkBlockingClient(4)
	r := NewReader(client)
	r.SetDownloadOptions(2, 4)

	prefetchDone := make(chan error, 1)
	go func() {
		prefetchDone <- r.Prefetch(context.Background(), "1", 0, 8)
	}()
	client.waitStarted(t, 0)
	client.waitStarted(t, 4)

	readDone := make(chan error, 1)
	go func() {
		_, err := r.ReadRange(context.Background(), "1", 0, 4)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreground read waited for an unrelated slow prefetch chunk")
	}

	close(client.release)
	if err := <-prefetchDone; err != nil {
		t.Fatal(err)
	}
	if got := client.readCount(0); got != 1 {
		t.Fatalf("completed chunk was downloaded %d times", got)
	}
}

func TestPrefetchKeepsSingleConnectionRangeExact(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	offset := int64((8 << 20) - 2)
	if err := r.Prefetch(context.Background(), "1", offset, 4); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	before := client.readCalls
	client.mu.Unlock()
	if _, err := r.ReadExactRange(context.Background(), "1", offset, 4); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	after := client.readCalls
	client.mu.Unlock()
	if after != before {
		t.Fatalf("exact foreground read missed unaligned prefetch: before=%d after=%d", before, after)
	}
}

func TestReadCacheEvictsByByteBudgetAndPromotesHits(t *testing.T) {
	r := NewReader(&stubClient{})
	r.cacheLimit = 10
	r.storeCached("1", 0, []byte("123456"))
	r.storeCached("2", 0, []byte("abcdef"))
	if _, ok := r.readCached("1", 0, 1); ok {
		t.Fatal("oldest window should be evicted by byte budget")
	}
	if got, ok := r.readCached("2", 0, 3); !ok || string(got) != "abc" {
		t.Fatalf("newest window missing: %q ok=%v", got, ok)
	}
	r.storeCached("3", 0, []byte("XYZ"))
	if _, ok := r.readCached("2", 0, 1); !ok {
		t.Fatal("cache hit should promote window")
	}
	r.storeCached("4", 0, []byte("78901234"))
	if _, ok := r.readCached("3", 0, 1); ok {
		t.Fatal("least recently used window should be evicted")
	}
}

func TestReadCacheConcurrentHits(t *testing.T) {
	r := NewReader(&stubClient{})
	r.storeCached("1", 0, []byte("12345678"))
	r.storeCached("2", 0, []byte("abcdefgh"))

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if data, ok := r.readCached("1", 0, 4); !ok || string(data) != "1234" {
					t.Errorf("cache miss or corrupt data: %q ok=%v", data, ok)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestReadCacheSkipsOversizedWindow(t *testing.T) {
	r := NewReader(&stubClient{})
	r.cacheLimit = 4
	r.storeCached("1", 0, []byte("12345"))
	if _, ok := r.readCached("1", 0, 1); ok {
		t.Fatal("oversized window must not be cached")
	}
	if r.cacheBytes != 0 {
		t.Fatalf("unexpected cache bytes: %d", r.cacheBytes)
	}
}

func TestReadRangeUsesRequestedLength(t *testing.T) {
	r := NewReader(nil)
	got, err := r.ReadRange(context.Background(), "fsid-1", 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 bytes, got %d", len(got))
	}
}

func TestReadRangeRejectsNegativeLength(t *testing.T) {
	r := NewReader(nil)
	_, err := r.ReadRange(context.Background(), "fsid-1", 10, -1)
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestReadConcurrentRejectsShortChunk(t *testing.T) {
	client := &stubClient{shortRead: true}
	r := NewReader(client)
	r.SetDownloadOptions(2, 4)

	if _, err := r.ReadRange(context.Background(), "fsid-1", 0, 8); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected short read error, got %v", err)
	}
}

func TestReadExactRangeDoesNotPrefetch(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	got, err := r.ReadExactRange(context.Background(), "fsid-1", 64<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4<<20 {
		t.Fatalf("expected exact length, got %d", len(got))
	}
	if client.readCalls != 1 {
		t.Fatalf("expected one backend read, got %d", client.readCalls)
	}
}

func TestReadExactRangeRejectsShortRead(t *testing.T) {
	client := &stubClient{shortRead: true}
	r := NewReader(client)
	if _, err := r.ReadExactRange(context.Background(), "fsid-1", 1024, 4096); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected short exact read error, got %v", err)
	}
}

func TestReadRangeCompletesRequestCrossingAlignedWindow(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	offset := int64((8 << 20) - (64 << 10))
	got, err := r.ReadRange(context.Background(), "fsid-1", offset, 192<<10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 192<<10 {
		t.Fatalf("expected complete cross-window read, got %d bytes", len(got))
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 2 {
		t.Fatalf("expected aligned read plus exact remainder, got %d backend reads", readCalls)
	}
}

func TestReadRangeCachesDownloadLinkAndRetries(t *testing.T) {
	client := &stubClient{failRead: true}
	r := NewReader(client)
	_, err := r.ReadRange(context.Background(), "fsid-1", 0, 8)
	if err == nil {
		t.Fatal("expected error from stub client")
	}
	if client.downloadCalls != 1 {
		t.Fatalf("expected one download link request, got %d", client.downloadCalls)
	}
	if client.readCalls != 2 {
		t.Fatalf("expected two read attempts, got %d", client.readCalls)
	}
}

func TestReadStreamRangeBypassesReaderCache(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	r.storeCached("fsid-1", 0, []byte("cached"))
	data, err := r.ReadStreamRange(context.Background(), "fsid-1", 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if len(data) != 6 || readCalls != 1 {
		t.Fatalf("stream range len=%d backend_reads=%d", len(data), readCalls)
	}
}

func TestInvalidateDownloadLinkClearsBothReaderAndClientCaches(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	if _, err := r.downloadLink("fsid-1", client); err != nil {
		t.Fatal(err)
	}
	r.InvalidateDownloadLink("fsid-1")
	r.mu.RLock()
	_, cached := r.links["fsid-1"]
	r.mu.RUnlock()
	client.mu.Lock()
	invalidated := client.invalidated
	client.mu.Unlock()
	if cached || invalidated != 1 {
		t.Fatalf("invalidate cached=%v client_calls=%d", cached, invalidated)
	}
}

func TestClassifyDownloadError(t *testing.T) {
	tests := []struct {
		err  error
		want DownloadErrorKind
	}{
		{fmt.Errorf("download range failed: status=403"), DownloadErrorAuth},
		{fmt.Errorf("download range failed: status=416"), DownloadErrorRange},
		{fmt.Errorf("connection reset by peer"), DownloadErrorTransport},
		{io.ErrUnexpectedEOF, DownloadErrorEOF},
	}
	for _, test := range tests {
		if got := ClassifyDownloadError(test.err); got != test.want {
			t.Fatalf("classify %v: got %d want %d", test.err, got, test.want)
		}
	}
}

func TestReadRangePrefetchesAndReusesCachedWindow(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	got, err := r.ReadRange(context.Background(), "fsid-1", 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(got))
	}
	got, err = r.ReadRange(context.Background(), "fsid-1", 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", len(got))
	}
	if client.readCalls != 1 {
		t.Fatalf("expected one backend read due to prefetch cache, got %d", client.readCalls)
	}
}

func TestReadRangeAlignsPrefetchWindow(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	got, err := r.ReadRange(context.Background(), "fsid-1", 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", len(got))
	}
	got, err = r.ReadRange(context.Background(), "fsid-1", 2048, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", len(got))
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 1 {
		t.Fatalf("expected aligned prefetch cache to reuse one backend read, got %d", readCalls)
	}
}

func TestReadRangeCoalescesInflightWindow(t *testing.T) {
	client := &stubClient{
		readStarted: make(chan struct{}, 2),
		readRelease: make(chan struct{}),
	}
	r := NewReader(client)
	firstDone := make(chan error, 1)
	go func() {
		_, err := r.ReadRange(context.Background(), "fsid-1", 0, 1024)
		firstDone <- err
	}()
	<-client.readStarted
	secondDone := make(chan error, 1)
	go func() {
		_, err := r.ReadRange(context.Background(), "fsid-1", 2048, 1024)
		secondDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	close(client.readRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 1 {
		t.Fatalf("expected concurrent reads in one window to share one backend read, got %d", readCalls)
	}
}

func TestReadRangeCancelsInflightWaiter(t *testing.T) {
	client := &stubClient{
		readStarted: make(chan struct{}, 1),
		readRelease: make(chan struct{}),
	}
	r := NewReader(client)
	ownerDone := make(chan error, 1)
	go func() {
		_, err := r.ReadRange(context.Background(), "fsid-1", 0, 1024)
		ownerDone <- err
	}()
	<-client.readStarted

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := r.ReadRange(ctx, "fsid-1", 2048, 1024)
		waiterDone <- err
	}()
	cancel()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled waiter, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("inflight waiter ignored cancellation")
	}
	close(client.readRelease)
	if err := <-ownerDone; err != nil {
		t.Fatal(err)
	}
}

type chunkBlockingClient struct {
	mu          sync.Mutex
	blockOffset int64
	started     chan int64
	release     chan struct{}
	reads       map[int64]int
}

func newChunkBlockingClient(blockOffset int64) *chunkBlockingClient {
	return &chunkBlockingClient{
		blockOffset: blockOffset,
		started:     make(chan int64, 8),
		release:     make(chan struct{}),
		reads:       make(map[int64]int),
	}
}

func (c *chunkBlockingClient) List(string) ([]baidu.RemoteEntry, error) { return nil, nil }
func (c *chunkBlockingClient) Stat(string) (baidu.RemoteEntry, error) {
	return baidu.RemoteEntry{}, nil
}
func (c *chunkBlockingClient) GetDownloadLink(string) (baidu.DownloadLink, error) {
	return baidu.DownloadLink{URL: "https://example.invalid", ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (c *chunkBlockingClient) Delete([]string) error { return nil }
func (c *chunkBlockingClient) RefreshAuth() error    { return nil }
func (c *chunkBlockingClient) ReadRange(ctx context.Context, _ string, offset int64, dst []byte) (int, error) {
	c.mu.Lock()
	c.reads[offset]++
	c.mu.Unlock()
	select {
	case c.started <- offset:
	default:
	}
	if offset == c.blockOffset {
		select {
		case <-c.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	for i := range dst {
		dst[i] = byte(offset + int64(i))
	}
	return len(dst), nil
}

func (c *chunkBlockingClient) waitStarted(t *testing.T, want int64) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case got := <-c.started:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for chunk %d", want)
		}
	}
}

func (c *chunkBlockingClient) readCount(offset int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads[offset]
}
