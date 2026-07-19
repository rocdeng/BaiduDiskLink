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
	activeReads   int
	maxActive     int
	readOffsets   map[int64]int
	readStarted   chan struct{}
	offsetStarted chan int64
	readRelease   chan struct{}
	failRead      bool
	shortRead     bool
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
	if s.readOffsets == nil {
		s.readOffsets = make(map[int64]int)
	}
	s.readOffsets[offset]++
	s.activeReads++
	if s.activeReads > s.maxActive {
		s.maxActive = s.activeReads
	}
	if s.readStarted != nil {
		select {
		case s.readStarted <- struct{}{}:
		default:
		}
	}
	if s.offsetStarted != nil {
		select {
		case s.offsetStarted <- offset:
		default:
		}
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeReads--
		s.mu.Unlock()
	}()
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

func TestPrefetchFetchesOneExactWindow(t *testing.T) {
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
		t.Fatalf("expected exact window to use configured chunk concurrency, got %d backend reads", readCalls)
	}
}

func TestReadExactRangeUsesConfiguredChunkConcurrency(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	r.SetDownloadOptions(2, 4)
	data, err := r.ReadExactRange(context.Background(), "1", 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 8 {
		t.Fatalf("unexpected data length %d", len(data))
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 2 {
		t.Fatalf("expected two concurrent chunks, got %d backend reads", readCalls)
	}
}

func TestPrefetchCoalescesSameRangeAcrossHandles(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 2), readRelease: make(chan struct{})}
	r := NewReader(client)
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- r.Prefetch(context.Background(), "1", 8<<20, 8<<20) }()
	<-client.readStarted
	go func() { second <- r.Prefetch(context.Background(), "1", 8<<20, 8<<20) }()
	time.Sleep(20 * time.Millisecond)
	close(client.readRelease)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 1 {
		t.Fatalf("expected one shared backend read, got %d", readCalls)
	}
}

func TestForegroundReadSharesBackgroundExactRange(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 2), readRelease: make(chan struct{})}
	r := NewReader(client)
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- r.Prefetch(context.Background(), "1", 8<<20, 8<<20) }()
	<-client.readStarted
	readDone := make(chan error, 1)
	go func() {
		data, err := r.ReadExactRange(context.Background(), "1", 8<<20, 8<<20)
		if err == nil && len(data) != 8<<20 {
			err = fmt.Errorf("unexpected data length %d", len(data))
		}
		readDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	close(client.readRelease)
	if err := <-prefetchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 1 {
		t.Fatalf("expected foreground read to share background request, got %d backend reads", readCalls)
	}
}

func TestForegroundReadRangeSharesBackgroundWindow(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 2), readRelease: make(chan struct{})}
	r := NewReader(client)
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- r.Prefetch(context.Background(), "1", 8<<20, 8<<20) }()
	<-client.readStarted
	readDone := make(chan error, 1)
	go func() {
		data, err := r.ReadRange(context.Background(), "1", 8<<20, 8<<20)
		if err == nil && len(data) != 8<<20 {
			err = fmt.Errorf("unexpected data length %d", len(data))
		}
		readDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	close(client.readRelease)
	if err := <-prefetchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 1 {
		t.Fatalf("expected foreground range to share background request, got %d backend reads", readCalls)
	}
}

func TestForegroundReadRangeRecoversFromCanceledBackgroundWindow(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 2), readRelease: make(chan struct{})}
	r := NewReader(client)
	prefetchCtx, cancelPrefetch := context.WithCancel(context.Background())
	prefetchDone := make(chan error, 1)
	go func() { prefetchDone <- r.Prefetch(prefetchCtx, "1", 8<<20, 8<<20) }()
	<-client.readStarted
	cancelPrefetch()
	if err := <-prefetchDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled background prefetch, got %v", err)
	}
	readDone := make(chan error, 1)
	go func() {
		data, err := r.ReadRange(context.Background(), "1", 8<<20, 8<<20)
		if err == nil && len(data) != 8<<20 {
			err = fmt.Errorf("unexpected data length %d", len(data))
		}
		readDone <- err
	}()
	close(client.readRelease)
	err := <-readDone
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls < 1 || readCalls > 2 {
		t.Fatalf("expected canceled request to complete or retry once, got %d backend reads", readCalls)
	}
}

func TestBackgroundPrefetchHasGlobalConcurrencyLimit(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 6), readRelease: make(chan struct{})}
	r := NewReader(client)
	done := make(chan error, 5)
	for i := 0; i < 5; i++ {
		off := int64(i) * (8 << 20)
		go func() { done <- r.Prefetch(context.Background(), "1", off, 8<<20) }()
	}
	for i := 0; i < maxBackgroundPrefetches; i++ {
		<-client.readStarted
	}
	select {
	case <-client.readStarted:
		t.Fatal("fifth background request started before a global slot was released")
	case <-time.After(100 * time.Millisecond):
	}
	close(client.readRelease)
	for i := 0; i < 5; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestBackgroundPrefetchLimitsChunkConcurrencyPerWindow(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 8), readRelease: make(chan struct{})}
	r := NewReader(client)
	r.SetDownloadOptions(4, 2)
	done := make(chan error, 1)
	go func() { done <- r.Prefetch(context.Background(), "1", 0, 8) }()
	<-client.readStarted
	<-client.readStarted
	select {
	case <-client.readStarted:
		t.Fatal("background window used more than two chunk requests")
	case <-time.After(100 * time.Millisecond):
	}
	close(client.readRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	maxActive := client.maxActive
	client.mu.Unlock()
	if maxActive != 2 {
		t.Fatalf("expected two active background chunks, got %d", maxActive)
	}
}

func TestForegroundReadBypassesQueuedBackgroundPrefetch(t *testing.T) {
	client := &stubClient{offsetStarted: make(chan int64, 6), readRelease: make(chan struct{})}
	r := NewReader(client)
	done := make(chan error, 5)
	for i := 0; i < 5; i++ {
		off := int64(i) * (8 << 20)
		go func() { done <- r.Prefetch(context.Background(), "1", off, 8<<20) }()
	}
	started := map[int64]bool{}
	for len(started) < maxBackgroundPrefetches {
		select {
		case off := <-client.offsetStarted:
			started[off] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the two background slots")
		}
	}
	var queuedOffset int64 = -1
	for _, off := range []int64{0, 8 << 20, 16 << 20, 24 << 20, 32 << 20} {
		if !started[off] {
			queuedOffset = off
			break
		}
	}
	if queuedOffset < 0 {
		t.Fatalf("expected one queued range, started=%v", started)
	}
	foregroundDone := make(chan error, 1)
	go func() {
		_, err := r.ReadExactRange(context.Background(), "1", queuedOffset, 8<<20)
		foregroundDone <- err
	}()
	select {
	case off := <-client.offsetStarted:
		if off != queuedOffset {
			t.Fatalf("foreground started unexpected offset %d, want %d", off, queuedOffset)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("foreground read waited behind the background slot queue")
	}
	close(client.readRelease)
	if err := <-foregroundDone; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	readCalls := client.readCalls
	readOffsets := make(map[int64]int, len(client.readOffsets))
	for off, count := range client.readOffsets {
		readOffsets[off] = count
	}
	client.mu.Unlock()
	if readCalls != 5 {
		t.Fatalf("expected each exact range to download once, got %d backend reads: %v", readCalls, readOffsets)
	}
}

func TestCanceledPrefetchCallerStillPublishesForOtherHandles(t *testing.T) {
	client := &stubClient{readStarted: make(chan struct{}, 1), readRelease: make(chan struct{})}
	r := NewReader(client)
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- r.Prefetch(ownerCtx, "1", 8<<20, 8<<20) }()
	<-client.readStarted
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- r.Prefetch(context.Background(), "1", 8<<20, 8<<20) }()
	waitForExactWaiters(t, r, exactRangeKey{fsid: "1", offset: 8 << 20, length: 8 << 20}, 2)
	cancelOwner()
	close(client.readRelease)
	if err := <-ownerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled caller to return promptly, got %v", err)
	}
	if err := <-waiterDone; err != nil {
		t.Fatalf("other handle lost shared prefetch: %v", err)
	}
	client.mu.Lock()
	readCalls := client.readCalls
	client.mu.Unlock()
	if readCalls != 1 {
		t.Fatalf("expected one shared backend read, got %d", readCalls)
	}
}

func waitForExactWaiters(t *testing.T, r *Reader, key exactRangeKey, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		inflight := r.exactInflight[key]
		got := 0
		if inflight != nil {
			got = inflight.waiters
		}
		r.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d exact-range waiters", want)
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
