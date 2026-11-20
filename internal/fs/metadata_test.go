package fs

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	goFs "github.com/hanwen/go-fuse/v2/fs"

	"baidudisklink/internal/baidu"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
)

// slowListClient：List 阻塞到 release 关闭，并统计调用次数。
type slowListClient struct {
	listErrorClient
	calls   atomic.Int64
	release chan struct{}
	entries []baidu.RemoteEntry
}

func (c *slowListClient) List(string) ([]baidu.RemoteEntry, error) {
	c.calls.Add(1)
	<-c.release
	return c.entries, nil
}

func seedLoadedDir(t *testing.T, dbStore *store.Store, expiresAt int64) store.Entry {
	t.Helper()
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	dir := store.Entry{FSID: "1", Parent: "0", Path: "/movies", Name: "movies", IsDir: true, ExpiresAt: expiresAt}
	if err := dbStore.UpsertEntry(dir); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "2", Parent: "1", Path: "/movies/a.mkv", Name: "a.mkv", Size: 10}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestStaleDirectoryServesCachedChildrenWithoutBlocking(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := seedLoadedDir(t, dbStore, time.Now().Add(-time.Minute).Unix())
	client := &slowListClient{release: make(chan struct{})}
	defer close(client.release)
	fs := NewFilesystem(dbStore, remote.NewReader(client), nil, "/")
	node := &entryNode{Filesystem: fs, entry: dir}

	done := make(chan syscall.Errno, 1)
	go func() {
		_, errno := node.Readdir(context.Background())
		done <- errno
	}()
	select {
	case errno := <-done:
		if errno != 0 {
			t.Fatalf("unexpected errno %v", errno)
		}
	case <-time.After(time.Second):
		t.Fatal("stale directory read blocked on remote List")
	}
	deadline := time.Now().Add(time.Second)
	for client.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if client.calls.Load() != 1 {
		t.Fatalf("expected one background refresh, got %d", client.calls.Load())
	}
}

func TestConcurrentLoadsOfSameDirectoryAreCoalesced(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := seedLoadedDir(t, dbStore, 0)
	client := &slowListClient{release: make(chan struct{})}
	fs := NewFilesystem(dbStore, remote.NewReader(client), nil, "/")
	node := &entryNode{Filesystem: fs, entry: dir}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = node.Readdir(context.Background())
		}()
	}
	deadline := time.Now().Add(time.Second)
	for client.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(client.release)
	wg.Wait()
	if client.calls.Load() != 1 {
		t.Fatalf("expected a single List call, got %d", client.calls.Load())
	}
}

func TestParentRefreshPreservesLoadedChildDirectoryExpiry(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Now().Add(time.Hour).Unix()
	seedLoadedDir(t, dbStore, expiresAt)
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{Entries: map[string][]baidu.RemoteEntry{
		"/": {{FSID: "1", ServerName: "movies", Path: "/movies", IsDir: true}},
	}}), nil, "/")
	if err := fs.refreshRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.GetByPath("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ExpiresAt != expiresAt {
		t.Fatalf("child directory freshness lost after parent refresh: %#v", got)
	}
}

func TestLookupFindsChildByIndexedQuery(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := seedLoadedDir(t, dbStore, time.Now().Add(time.Hour).Unix())
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{}), nil, "/")
	node := &entryNode{Filesystem: fs, entry: dir}
	if _, errno := node.Lookup(context.Background(), "missing.mkv", nil); errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %v", errno)
	}
	child, err := dbStore.GetChild("1", "a.mkv")
	if err != nil || child == nil || child.Path != "/movies/a.mkv" {
		t.Fatalf("unexpected child: %#v err=%v", child, err)
	}
}

func TestStableAttrIsDerivedFromMetadata(t *testing.T) {
	file := store.Entry{FSID: "12345", Size: 100, MTM: 7}
	first := stableAttrFor(file)
	if first != stableAttrFor(file) {
		t.Fatal("same metadata must yield the same inode identity")
	}
	if first.Ino != 12347 {
		t.Fatalf("expected inode derived from fsid, got %d", first.Ino)
	}
	changed := file
	changed.Size = 200
	if stableAttrFor(changed) == first {
		t.Fatal("changed content must yield a new inode generation")
	}
	if attr := stableAttrFor(store.Entry{FSID: "not-numeric"}); attr.Ino <= 1 || attr.Reserved() {
		t.Fatalf("hashed inode must not be reserved: %#v", attr)
	}
	var _ goFs.StableAttr = first
}

func TestNegativeCacheIsInitializedForConcurrentUse(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{}), nil, "/")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fs.markMissing("/x")
		}()
	}
	wg.Wait()
	if !fs.negative.IsMissing("/x") {
		t.Fatal("expected path to be marked missing")
	}
}

func TestDirModeGrantsWriteOnlyWhenDeleteEnabled(t *testing.T) {
	grouped := NewFilesystem(nil, nil, []uint32{101}, "/")
	if got := grouped.dirMode(); got != 0750 {
		t.Fatalf("grouped read-only dir mode = %o", got)
	}
	grouped.SetDeleteEnabled(true)
	if got := grouped.dirMode(); got != 0770 {
		t.Fatalf("grouped delete-enabled dir mode = %o", got)
	}
	open := NewFilesystem(nil, nil, nil, "/")
	if got := open.dirMode(); got != 0755 {
		t.Fatalf("default dir mode = %o", got)
	}
}
