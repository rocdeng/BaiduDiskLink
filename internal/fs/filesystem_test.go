package fs

import (
	"context"
	"database/sql"
	"strings"
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
	data, err := r.ReadRange("fsid-1", 0, 8)
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
	if _, err := remoteReader.ReadRange("1", 0, 16); err != nil {
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
	lengths []int64
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

func (c *countingReadClient) ReadRange(fsid string, offset, length int64) ([]byte, error) {
	c.reads++
	c.lengths = append(c.lengths, length)
	return []byte(strings.Repeat("x", int(length))), nil
}

func (c *countingReadClient) RefreshAuth() error {
	return nil
}
