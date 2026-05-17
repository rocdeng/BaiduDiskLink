package fs

import (
	"context"
	"database/sql"
	"syscall"
	"testing"
	"time"

	"baidudisklink/internal/baidu"
	"baidudisklink/internal/cache"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
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
	fs := NewFilesystem(dbStore, remoteReader, 0)
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
	fs := NewFilesystem(dbStore, remoteReader, 0)
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
	fs := NewFilesystem(dbStore, remoteReader, 0)
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
	fs := NewFilesystem(dbStore, remoteReader, 0)
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

func TestLookupUsesNegativeCacheForMissingEntries(t *testing.T) {
	dbStore, err := store.Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	fs := NewFilesystem(dbStore, remote.NewReader(&baidu.StaticClient{}), 0)
	fs.negative = cache.NewNegativeCache(30 * time.Second)
	fs.markMissing("/missing")
	if _, errno := fs.Lookup(context.Background(), "missing", nil); errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %v", errno)
	}
	if !fs.negative.IsMissing("/missing") {
		t.Fatal("expected missing entry to be cached")
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
