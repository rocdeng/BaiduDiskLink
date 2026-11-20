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
