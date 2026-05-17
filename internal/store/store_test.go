package store

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

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
