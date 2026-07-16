package fs

import (
	"context"
	"database/sql"
	"sync"
	"syscall"
	"testing"
	"time"

	"baidudisklink/internal/baidu"
	"baidudisklink/internal/cache"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
	"github.com/hanwen/go-fuse/v2/fuse"
	_ "modernc.org/sqlite"
)

func TestJoinPath(t *testing.T) {
	if got := JoinPath("/", "movies"); got != "/movies" {
		t.Fatalf("unexpected path: %q", got)
	}
	if got := JoinPath("/movies", "test.mkv"); got != "/movies/test.mkv" {
		t.Fatalf("unexpected path: %q", got)
	}
}

func TestDirEntriesAreSorted(t *testing.T) {
	children := []store.Entry{
		{Name: "b.mkv"},
		{Name: "a.mkv"},
	}
	got := dirEntries(children)
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Name != "a.mkv" || got[1].Name != "b.mkv" {
		t.Fatalf("unexpected ordering: %#v", got)
	}
}

func TestReaderReturnsRequestedLength(t *testing.T) {
	r := remote.NewReader(nil)
	data, err := r.ReadRange(context.Background(), "fsid-1", 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(data))
	}
}

func TestEntryReadPassesThroughRequestedLength(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	entry := store.Entry{
		FSID:  "1",
		Path:  "/movie.mkv",
		Name:  "movie.mkv",
		Size:  5,
		IsDir: false,
	}
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{}), nil, "/")
	node := &entryNode{Filesystem: fs, entry: entry}
	got, errno := node.Read(context.Background(), nil, make([]byte, 16), 0)
	if errno != 0 {
		t.Fatalf("expected read to succeed, got %v", errno)
	}
	data, status := got.Bytes(nil)
	if status != 0 {
		t.Fatalf("expected read bytes to succeed, got %v", status)
	}
	if len(data) != 16 {
		t.Fatalf("expected read to pass through requested length, got %d", len(data))
	}
}

func TestEntryReadUsesCachedWindowWhenAvailable(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	entry := store.Entry{
		FSID:  "1",
		Path:  "/movie.mkv",
		Name:  "movie.mkv",
		Size:  16,
		IsDir: false,
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/": {{FSID: "1", ServerName: "movie.mkv", Path: "/movie.mkv", Size: 16}},
		},
	})
	remoteReader.SetDownloadOptions(1, 16)
	if _, err := remoteReader.ReadRange(context.Background(), "1", 0, 16); err != nil {
		t.Fatal(err)
	}
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	node := &entryNode{Filesystem: fs, entry: entry}
	got, errno := node.Read(context.Background(), nil, make([]byte, 8), 4)
	if errno != 0 {
		t.Fatalf("expected read to succeed, got %v", errno)
	}
	data, status := got.Bytes(nil)
	if status != 0 {
		t.Fatalf("expected read bytes to succeed, got %v", status)
	}
	if len(data) != 8 {
		t.Fatalf("expected cached read to return requested length, got %d", len(data))
	}
}

func TestEntryFileHandleReusesReadWindow(t *testing.T) {
	client := &countingReadClient{}
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 16, lastRead: -1}
	entry := store.Entry{
		FSID: "1",
		Path: "/movie.mkv",
		Size: 64,
	}
	first, err := handle.read(context.Background(), remoteReader, entry, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	second, err := handle.read(context.Background(), remoteReader, entry, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 8 || len(second) != 8 {
		t.Fatalf("unexpected read lengths: %d %d", len(first), len(second))
	}
	if client.reads != 1 {
		t.Fatalf("expected one remote read for same handle window, got %d", client.reads)
	}
}

func TestEntryFileHandlePrefetchesAndReusesNextWindow(t *testing.T) {
	client := newPrefetchReadClient(-1)
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 16 << 20, lastRead: -1}
	entry := store.Entry{FSID: "1", Path: "/movie.mkv", Size: 64 << 20}

	if _, err := handle.read(context.Background(), remoteReader, entry, 0, 192<<10); err != nil {
		t.Fatal(err)
	}
	client.waitForOffset(t, 0)
	if _, err := handle.read(context.Background(), remoteReader, entry, (4<<20)+(192<<10), 192<<10); err != nil {
		t.Fatal(err)
	}
	client.waitForOffset(t, 16<<20)
	if _, err := handle.read(context.Background(), remoteReader, entry, 16<<20, 192<<10); err != nil {
		t.Fatal(err)
	}
	if reads := client.readCount(); reads != 2 {
		t.Fatalf("expected prefetched boundary to avoid another backend read, got %d", reads)
	}
	if handle.lastStrategy != "next-window-prefetch" {
		t.Fatalf("unexpected boundary strategy %q", handle.lastStrategy)
	}
}

func TestEntryFileHandleCombinesReadAcrossPrefetchedBoundary(t *testing.T) {
	client := newPrefetchReadClient(-1)
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 16 << 20, lastRead: -1}
	entry := store.Entry{FSID: "1", Path: "/movie.mkv", Size: 64 << 20}

	if _, err := handle.read(context.Background(), remoteReader, entry, 0, 192<<10); err != nil {
		t.Fatal(err)
	}
	client.waitForOffset(t, 0)
	if _, err := handle.read(context.Background(), remoteReader, entry, (4<<20)+(192<<10), 192<<10); err != nil {
		t.Fatal(err)
	}
	client.waitForOffset(t, 16<<20)
	got, err := handle.read(context.Background(), remoteReader, entry, (16<<20)-(64<<10), 192<<10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 192<<10 {
		t.Fatalf("expected complete cross-window read, got %d bytes", len(got))
	}
	if reads := client.readCount(); reads != 2 {
		t.Fatalf("expected no duplicate boundary download, got %d backend reads", reads)
	}
	if handle.lastStrategy != "prefetched-boundary" {
		t.Fatalf("unexpected boundary strategy %q", handle.lastStrategy)
	}
}

func TestEntryFileHandleCancelsPrefetchOnSeek(t *testing.T) {
	client := newPrefetchReadClient(16 << 20)
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 16 << 20, lastRead: -1}
	entry := store.Entry{FSID: "1", Path: "/movie.mkv", Size: 64 << 20}

	if _, err := handle.read(context.Background(), remoteReader, entry, 0, 192<<10); err != nil {
		t.Fatal(err)
	}
	client.waitForOffset(t, 0)
	if _, err := handle.read(context.Background(), remoteReader, entry, (4<<20)+(192<<10), 192<<10); err != nil {
		t.Fatal(err)
	}
	client.waitForOffset(t, 16<<20)
	if _, err := handle.read(context.Background(), remoteReader, entry, 48<<20, 192<<10); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("prefetch was not canceled after seek")
	}
}

func TestEntryFileHandleUsesSmallerWindowAfterLargeSeek(t *testing.T) {
	client := &countingReadClient{}
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 16 << 20, lastRead: -1}
	entry := store.Entry{
		FSID: "1",
		Path: "/movie.mkv",
		Size: 128 << 20,
	}
	if _, err := handle.read(context.Background(), remoteReader, entry, 0, 4<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.read(context.Background(), remoteReader, entry, 80<<20, 4<<20); err != nil {
		t.Fatal(err)
	}
	if client.reads != 2 {
		t.Fatalf("expected second remote read after seek, got %d", client.reads)
	}
	if client.lengths[1] != 8<<20 {
		t.Fatalf("expected smaller seek window, got %d", client.lengths[1])
	}
}

func TestEntryFileHandleUsesSmallerWindowForInitialHighOffsetRead(t *testing.T) {
	client := &countingReadClient{}
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 16 << 20, lastRead: -1}
	entry := store.Entry{
		FSID: "1",
		Path: "/movie.mkv",
		Size: 128 << 20,
	}
	if _, err := handle.read(context.Background(), remoteReader, entry, 80<<20, 4<<20); err != nil {
		t.Fatal(err)
	}
	if client.reads != 1 {
		t.Fatalf("expected one remote read, got %d", client.reads)
	}
	if client.lengths[0] != 8<<20 {
		t.Fatalf("expected smaller initial seek window, got %d", client.lengths[0])
	}
}

func TestEntryFileHandleRefetchesWhenCachedWindowCannotSatisfyRead(t *testing.T) {
	client := &countingReadClient{}
	remoteReader := remote.NewReader(client)
	handle := &entryFileHandle{windowSize: 32, lastRead: -1}
	entry := store.Entry{
		FSID: "1",
		Path: "/movie.mkv",
		Size: 256,
	}
	if got, err := handle.read(context.Background(), remoteReader, entry, 64, 8); err != nil {
		t.Fatal(err)
	} else if len(got) != 8 {
		t.Fatalf("expected first read length 8, got %d", len(got))
	}
	got, err := handle.read(context.Background(), remoteReader, entry, 72, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 16 {
		t.Fatalf("expected full second read, got %d", len(got))
	}
	if client.reads != 2 {
		t.Fatalf("expected cache tail miss to refetch, got %d reads", client.reads)
	}
}

func TestRefreshRootLoadsRemoteEntriesIntoStore(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/": {{FSID: "1", ServerName: "movies", Path: "/movies", IsDir: true}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.refreshRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "movies" {
		t.Fatalf("unexpected refresh result: %#v", got)
	}
}

func TestRefreshRootUsesConfiguredRemoteRootAsLocalRoot(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/Videos": {{FSID: "2", ServerName: "TV", Path: "/Videos/TV", IsDir: true}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/Videos")
	if err := fs.refreshRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/Videos")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "TV" || got[0].Path != "/Videos/TV" {
		t.Fatalf("unexpected configured root result: %#v", got)
	}
	localRoot, err := dbStore.GetByPath("/Videos")
	if err != nil {
		t.Fatal(err)
	}
	if localRoot == nil || !localRoot.IsDir {
		t.Fatalf("expected configured remote root to be stored as local root, got %#v", localRoot)
	}
}

func TestRefreshRootDoesNotMarkChildDirectoryContentsFresh(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/Videos": {{FSID: "2", ServerName: "Movie", Path: "/Videos/Movie", IsDir: true}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/Videos")
	if err := fs.refreshRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	movie, err := dbStore.GetByPath("/Videos/Movie")
	if err != nil {
		t.Fatal(err)
	}
	if movie == nil || !movie.IsDir {
		t.Fatalf("expected Movie directory, got %#v", movie)
	}
	if movie.ExpiresAt != 0 {
		t.Fatalf("expected child directory contents to remain stale, got expires_at=%d", movie.ExpiresAt)
	}
	if !fs.shouldRefreshDir("/Videos/Movie", nil) {
		t.Fatal("expected child directory to refresh on demand")
	}
}

func TestRefreshDirDoesNotMarkNestedDirectoryContentsFresh(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/Videos/Movie": {{FSID: "3", ServerName: "New Folder", Path: "/Videos/Movie/New Folder", IsDir: true}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/Videos")
	if err := fs.refreshDir(context.Background(), "/Videos/Movie", "2"); err != nil {
		t.Fatal(err)
	}
	child, err := dbStore.GetByPath("/Videos/Movie/New Folder")
	if err != nil {
		t.Fatal(err)
	}
	if child == nil || !child.IsDir {
		t.Fatalf("expected nested directory, got %#v", child)
	}
	if child.ExpiresAt != 0 {
		t.Fatalf("expected nested directory contents to remain stale, got expires_at=%d", child.ExpiresAt)
	}
}

func TestRefreshDirLoadsNestedEntries(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "1",
		Parent: "0",
		Path:   "/movies",
		Name:   "movies",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/movies": {{FSID: "2", ServerName: "test.mkv", Path: "/movies/test.mkv", Size: 9, IsDir: false}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.refreshDir(context.Background(), "/movies", "1"); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "test.mkv" {
		t.Fatalf("unexpected nested refresh result: %#v", got)
	}
}

func TestRefreshDirPreservesExistingDirectoryMTime(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "1",
		Parent: "0",
		Path:   "/movies",
		Name:   "movies",
		IsDir:  true,
		MTM:    1710000000,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/movies": {{FSID: "2", ServerName: "test.mkv", Path: "/movies/test.mkv", Size: 9, IsDir: false}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.refreshDir(context.Background(), "/movies", "1"); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.GetByPath("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.MTM != 1710000000 {
		t.Fatalf("expected directory mtime to be preserved, got %#v", got)
	}
}

func TestRefreshDirLoadsNewFileIntoExistingDirectory(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "1",
		Parent: "0",
		Path:   "/Videos",
		Name:   "Videos",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "2",
		Parent: "1",
		Path:   "/Videos/TV",
		Name:   "TV",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/Videos/TV": {{FSID: "3", ServerName: "new.mkv", Path: "/Videos/TV/new.mkv", Size: 10, IsDir: false}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.refreshDir(context.Background(), "/Videos/TV", "2"); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/Videos/TV")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "new.mkv" {
		t.Fatalf("expected new file to appear in existing directory, got %#v", got)
	}
}

func TestRefreshDirReplacesExistingChildren(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "1",
		Parent: "0",
		Path:   "/movies",
		Name:   "movies",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "2",
		Parent: "1",
		Path:   "/movies/old.mkv",
		Name:   "old.mkv",
		Size:   9,
		IsDir:  false,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/movies": {{FSID: "3", ServerName: "new.mkv", Path: "/movies/new.mkv", Size: 10, IsDir: false}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.refreshDir(context.Background(), "/movies", "1"); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "new.mkv" {
		t.Fatalf("expected refreshed children to replace old ones, got %#v", got)
	}
}

func TestRefreshDirPreservesParentRelation(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "1",
		Parent: "0",
		Path:   "/Videos",
		Name:   "Videos",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/Videos": {{FSID: "2", ServerName: "Movie", Path: "/Videos/Movie", IsDir: true}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.refreshDir(context.Background(), "/Videos", "1"); err != nil {
		t.Fatal(err)
	}
	rootChildren, err := dbStore.ListChildren("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(rootChildren) != 1 || rootChildren[0].Name != "Videos" {
		t.Fatalf("expected Videos to remain under root, got %#v", rootChildren)
	}
}

func TestRefreshAllRefreshesKnownDirectoryTree(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "1",
		Parent: "0",
		Path:   "/Videos",
		Name:   "Videos",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "2",
		Parent: "1",
		Path:   "/Videos/TV",
		Name:   "TV",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/":          {{FSID: "1", ServerName: "Videos", Path: "/Videos", IsDir: true}},
			"/Videos":    {{FSID: "2", ServerName: "TV", Path: "/Videos/TV", IsDir: true}},
			"/Videos/TV": {{FSID: "3", ServerName: "new.mkv", Path: "/Videos/TV/new.mkv", Size: 10, IsDir: false}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if err := fs.RefreshAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/Videos/TV")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "new.mkv" {
		t.Fatalf("expected deep refresh to load new file, got %#v", got)
	}
}

func TestRefreshAllSkipsConcurrentRefresh(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/")
	if !fs.tryRefreshToken() {
		t.Fatal("expected refresh token to be available")
	}
	defer fs.releaseRefreshToken()
	if err := fs.RefreshAll(context.Background()); err != nil {
		t.Fatalf("expected concurrent refresh to be skipped without error, got %v", err)
	}
}

func TestRefreshRootOnlyDoesNotRefreshKnownDirectoryTree(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{
		FSID:   "2",
		Parent: "1",
		Path:   "/Videos/TV",
		Name:   "TV",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	remoteReader := remote.NewReader(&baidu.StaticClient{
		Entries: map[string][]baidu.RemoteEntry{
			"/Videos":    {{FSID: "2", ServerName: "TV", Path: "/Videos/TV", IsDir: true}},
			"/Videos/TV": {{FSID: "3", ServerName: "new.mkv", Path: "/Videos/TV/new.mkv", Size: 10, IsDir: false}},
		},
	})
	fs := NewFilesystem(dbStore, remoteReader, nil, "/Videos")
	if err := fs.RefreshRootOnly(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := dbStore.ListChildren("/Videos/TV")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected root-only refresh to skip child directories, got %#v", got)
	}
}

func TestLookupUsesNegativeCacheForMissingEntries(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{}), nil, "/")
	fs.negative = cache.NewNegativeCache(30 * time.Second)
	fs.markMissing("/missing")
	if _, errno := fs.Lookup(context.Background(), "missing", nil); errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %v", errno)
	}
	if !fs.negative.IsMissing("/missing") {
		t.Fatal("expected missing entry to be cached")
	}
}

func TestFillEntryOutUsesMtimeFromMetadata(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{}), nil, "/")
	var out fuse.EntryOut
	fs.fillEntryOut(store.Entry{Size: 1024, MTM: 1710000000}, syscall.S_IFREG, &out)
	if got := int64(out.Mtime); got != 1710000000 {
		t.Fatalf("expected mtime from metadata, got %d", got)
	}
}

func TestUnlinkRejectsWhenDeleteDisabled(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "1", Parent: "0", Path: "/Videos", Name: "Videos", IsDir: true}); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "2", Parent: "1", Path: "/Videos/test.mkv", Name: "test.mkv"}); err != nil {
		t.Fatal(err)
	}
	client := &deleteRecordingClient{}
	fs := NewFilesystem(dbStore, remote.NewReader(client), nil, "/Videos")
	if errno := fs.Unlink(context.Background(), "test.mkv"); errno != syscall.EROFS {
		t.Fatalf("expected EROFS, got %v", errno)
	}
	if len(client.deleted) != 0 {
		t.Fatalf("delete should not be called, got %#v", client.deleted)
	}
}

func TestUnlinkDeletesRemoteFileAndMetadata(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "1", Parent: "0", Path: "/Videos", Name: "Videos", IsDir: true}); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "2", Parent: "1", Path: "/Videos/test.mkv", Name: "test.mkv"}); err != nil {
		t.Fatal(err)
	}
	client := &deleteRecordingClient{}
	fs := NewFilesystem(dbStore, remote.NewReader(client), nil, "/Videos")
	fs.SetDeleteEnabled(true)
	if errno := fs.Unlink(context.Background(), "test.mkv"); errno != 0 {
		t.Fatalf("expected delete to succeed, got %v", errno)
	}
	if len(client.deleted) != 1 || client.deleted[0] != "/Videos/test.mkv" {
		t.Fatalf("unexpected remote delete paths: %#v", client.deleted)
	}
	got, err := dbStore.GetByPath("/Videos/test.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected metadata removed, got %#v", got)
	}
}

func TestRmdirRejectsFile(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "1", Parent: "0", Path: "/Videos", Name: "Videos", IsDir: true}); err != nil {
		t.Fatal(err)
	}
	if err := dbStore.UpsertEntry(store.Entry{FSID: "2", Parent: "1", Path: "/Videos/test.mkv", Name: "test.mkv"}); err != nil {
		t.Fatal(err)
	}
	fs := NewFilesystem(dbStore, remote.NewReader(&deleteRecordingClient{}), nil, "/Videos")
	fs.SetDeleteEnabled(true)
	if errno := fs.Rmdir(context.Background(), "test.mkv"); errno != syscall.ENOTDIR {
		t.Fatalf("expected ENOTDIR, got %v", errno)
	}
}

func TestRmdirDeletesRemoteDirectoryAndMetadataSubtree(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	entries := []store.Entry{
		{FSID: "1", Parent: "0", Path: "/Videos", Name: "Videos", IsDir: true},
		{FSID: "2", Parent: "1", Path: "/Videos/Movie", Name: "Movie", IsDir: true},
		{FSID: "3", Parent: "2", Path: "/Videos/Movie/test.mkv", Name: "test.mkv"},
	}
	for _, entry := range entries {
		if err := dbStore.UpsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	client := &deleteRecordingClient{}
	fs := NewFilesystem(dbStore, remote.NewReader(client), nil, "/Videos")
	fs.SetDeleteEnabled(true)
	if errno := fs.Rmdir(context.Background(), "Movie"); errno != 0 {
		t.Fatalf("expected directory delete to succeed, got %v", errno)
	}
	if len(client.deleted) != 1 || client.deleted[0] != "/Videos/Movie" {
		t.Fatalf("unexpected remote delete paths: %#v", client.deleted)
	}
	if got, err := dbStore.GetByPath("/Videos/Movie"); err != nil || got != nil {
		t.Fatalf("expected directory metadata removed, got %#v err=%v", got, err)
	}
	if got, err := dbStore.GetByPath("/Videos/Movie/test.mkv"); err != nil || got != nil {
		t.Fatalf("expected child metadata removed, got %#v err=%v", got, err)
	}
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type countingReadClient struct {
	reads   int
	offsets []int64
	lengths []int64
}

type prefetchReadClient struct {
	mu          sync.Mutex
	reads       int
	started     chan int64
	blockOffset int64
	canceled    chan struct{}
	cancelOnce  sync.Once
}

func newPrefetchReadClient(blockOffset int64) *prefetchReadClient {
	return &prefetchReadClient{
		started:     make(chan int64, 16),
		blockOffset: blockOffset,
		canceled:    make(chan struct{}),
	}
}

func (c *prefetchReadClient) List(string) ([]baidu.RemoteEntry, error) {
	return nil, nil
}

func (c *prefetchReadClient) Stat(string) (baidu.RemoteEntry, error) {
	return baidu.RemoteEntry{}, nil
}

func (c *prefetchReadClient) GetDownloadLink(string) (baidu.DownloadLink, error) {
	return baidu.DownloadLink{}, nil
}

func (c *prefetchReadClient) Delete([]string) error {
	return nil
}

func (c *prefetchReadClient) ReadRange(ctx context.Context, _ string, offset int64, dst []byte) (int, error) {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
	select {
	case c.started <- offset:
	default:
	}
	if offset == c.blockOffset {
		<-ctx.Done()
		c.cancelOnce.Do(func() { close(c.canceled) })
		return 0, ctx.Err()
	}
	for i := range dst {
		dst[i] = 'x'
	}
	return len(dst), nil
}

func (c *prefetchReadClient) RefreshAuth() error {
	return nil
}

func (c *prefetchReadClient) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func (c *prefetchReadClient) waitForOffset(t *testing.T, want int64) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case got := <-c.started:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for backend offset %d", want)
		}
	}
}

func (c *countingReadClient) List(path string) ([]baidu.RemoteEntry, error) {
	return nil, nil
}

func (c *countingReadClient) Stat(path string) (baidu.RemoteEntry, error) {
	return baidu.RemoteEntry{}, nil
}

func (c *countingReadClient) GetDownloadLink(fsid string) (baidu.DownloadLink, error) {
	return baidu.DownloadLink{}, nil
}

func (c *countingReadClient) Delete(paths []string) error {
	return nil
}

func (c *countingReadClient) ReadRange(_ context.Context, _ string, offset int64, dst []byte) (int, error) {
	c.reads++
	c.offsets = append(c.offsets, offset)
	c.lengths = append(c.lengths, int64(len(dst)))
	for i := range dst {
		dst[i] = 'x'
	}
	return len(dst), nil
}

func (c *countingReadClient) RefreshAuth() error {
	return nil
}

type deleteRecordingClient struct {
	deleted []string
}

func (c *deleteRecordingClient) List(path string) ([]baidu.RemoteEntry, error) {
	return nil, nil
}

func (c *deleteRecordingClient) Stat(path string) (baidu.RemoteEntry, error) {
	return baidu.RemoteEntry{}, nil
}

func (c *deleteRecordingClient) GetDownloadLink(fsid string) (baidu.DownloadLink, error) {
	return baidu.DownloadLink{}, nil
}

func (c *deleteRecordingClient) Delete(paths []string) error {
	c.deleted = append(c.deleted, paths...)
	return nil
}

func (c *deleteRecordingClient) ReadRange(_ context.Context, fsid string, offset int64, dst []byte) (int, error) {
	return len(dst), nil
}

func (c *deleteRecordingClient) RefreshAuth() error {
	return nil
}
