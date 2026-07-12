package remote

import (
	"errors"
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
	return len(dst), nil
}
func (s *stubClient) Delete(paths []string) error { return nil }
func (s *stubClient) RefreshAuth() error          { return nil }

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
