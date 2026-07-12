package store

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestReplaceChildrenRollsBackOnInsertFailure(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertEntry(Entry{FSID: "old", Parent: "p", Path: "/old", Name: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`create trigger reject_bad before insert on entries when new.fsid = 'bad' begin select raise(abort, 'bad entry'); end`); err != nil {
		t.Fatal(err)
	}
	err = st.ReplaceChildren("p", []Entry{{FSID: "bad", Parent: "p", Path: "/bad", Name: "bad"}})
	if err == nil {
		t.Fatal("expected insert failure")
	}
	children, err := st.GetChildrenByParent("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].FSID != "old" {
		t.Fatalf("rollback lost old children: %#v", children)
	}
}

func TestUpsertEntriesRollsBackOnFailure(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`create trigger reject_bad before insert on entries when new.fsid = 'bad' begin select raise(abort, 'bad entry'); end`); err != nil {
		t.Fatal(err)
	}
	err = st.UpsertEntries([]Entry{{FSID: "good", Parent: "0", Path: "/good", Name: "good"}, {FSID: "bad", Parent: "0", Path: "/bad", Name: "bad"}})
	if err == nil {
		t.Fatal("expected batch failure")
	}
	entry, err := st.GetByPath("/good")
	if err != nil {
		t.Fatal(err)
	}
	if entry != nil {
		t.Fatalf("partial batch write remained: %#v", entry)
	}
}

func TestUpsertEntriesEmptyBatchIsNoOp(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertEntries(nil); err != nil {
		t.Fatalf("empty batch must remain a no-op: %v", err)
	}
}

func TestUpsertFromRemoteFillsOnlyEmptyParent(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertFromRemote("fallback", []Entry{{FSID: "1", Path: "/one", Name: "one"}, {FSID: "2", Parent: "explicit", Path: "/two", Name: "two"}}); err != nil {
		t.Fatal(err)
	}
	one, _ := st.GetByPath("/one")
	two, _ := st.GetByPath("/two")
	if one.Parent != "fallback" || two.Parent != "explicit" {
		t.Fatalf("parents one=%q two=%q", one.Parent, two.Parent)
	}
}

func TestUpsertAndListEntries(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}

	want := Entry{
		FSID:   "123",
		Parent: "0",
		Path:   "/movies",
		Name:   "movies",
		IsDir:  true,
	}
	if err := s.UpsertEntry(want); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListChildren("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no children yet, got %d", len(got))
	}
}

func TestUpsertChildAndListEntries(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureRoot(); err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertEntry(Entry{
		FSID:   "123",
		Parent: "0",
		Path:   "/movies",
		Name:   "movies",
		IsDir:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertEntry(Entry{
		FSID:   "124",
		Parent: "123",
		Path:   "/movies/test.mkv",
		Name:   "test.mkv",
		Size:   100,
		IsDir:  false,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListChildren("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 child, got %d", len(got))
	}
	if got[0].Name != "test.mkv" {
		t.Fatalf("unexpected child: %#v", got[0])
	}
}

func TestListChildrenReturnsSortedResults(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureRoot(); err != nil {
		t.Fatal(err)
	}

	entries := []Entry{
		{FSID: "1", Parent: "0", Path: "/movies", Name: "movies", IsDir: true},
		{FSID: "2", Parent: "1", Path: "/movies/b.mkv", Name: "b.mkv", Size: 1},
		{FSID: "3", Parent: "1", Path: "/movies/a.mkv", Name: "a.mkv", Size: 1},
	}
	for _, entry := range entries {
		if err := s.UpsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ListChildren("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 children, got %d", len(got))
	}
	if got[0].Name != "a.mkv" || got[1].Name != "b.mkv" {
		t.Fatalf("unexpected order: %#v", got)
	}
}

func TestRootChildrenAreListedFromParentZero(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureRoot(); err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertEntry(Entry{FSID: "1", Parent: "0", Path: "/movies", Name: "movies", IsDir: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertEntry(Entry{FSID: "2", Parent: "0", Path: "/tv", Name: "tv", IsDir: true}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListChildren("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 root children, got %d", len(got))
	}
	if got[0].Name != "movies" || got[1].Name != "tv" {
		t.Fatalf("unexpected root listing: %#v", got)
	}
}

func TestExpirePathClearsExpiry(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpirePath("/"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetByPath("/")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ExpiresAt != 0 {
		t.Fatalf("expected root to expire, got %#v", got)
	}
}

func TestDeletePathRemovesEntrySubtree(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertEntry(Entry{FSID: "1", Parent: "0", Path: "/Videos", Name: "Videos", IsDir: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertEntry(Entry{FSID: "2", Parent: "1", Path: "/Videos/Movie", Name: "Movie", IsDir: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertEntry(Entry{FSID: "3", Parent: "2", Path: "/Videos/Movie/a.mkv", Name: "a.mkv"}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePath("/Videos/Movie"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetByPath("/Videos/Movie"); err != nil || got != nil {
		t.Fatalf("expected directory removed, got %#v err=%v", got, err)
	}
	if got, err := s.GetByPath("/Videos/Movie/a.mkv"); err != nil || got != nil {
		t.Fatalf("expected child removed, got %#v err=%v", got, err)
	}
	if got, err := s.GetByPath("/Videos"); err != nil || got == nil {
		t.Fatalf("expected parent preserved, got %#v err=%v", got, err)
	}
}
