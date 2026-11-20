package store

import (
	"database/sql"
	"testing"
)

func openMemoryStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// :memory: 每个连接是独立数据库，测试中固定单连接。
	db.SetMaxOpenConns(1)
	st, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestReplaceChildrenRemovesSubtreeOfDeletedDirectory(t *testing.T) {
	st := openMemoryStore(t)
	for _, entry := range []Entry{
		{FSID: "1", Parent: "0", Path: "/old", Name: "old", IsDir: true},
		{FSID: "2", Parent: "1", Path: "/old/sub", Name: "sub", IsDir: true},
		{FSID: "3", Parent: "2", Path: "/old/sub/a.mkv", Name: "a.mkv"},
		{FSID: "4", Parent: "0", Path: "/keep", Name: "keep", IsDir: true},
		{FSID: "5", Parent: "4", Path: "/keep/b.mkv", Name: "b.mkv"},
	} {
		if err := st.UpsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.ReplaceChildren("0", []Entry{{FSID: "4", Parent: "0", Path: "/keep", Name: "keep", IsDir: true}}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/old/sub", "/old/sub/a.mkv"} {
		if got, err := st.GetByPath(path); err != nil || got != nil {
			t.Fatalf("expected orphan %s to be removed, got %#v err=%v", path, got, err)
		}
	}
	if got, err := st.GetByPath("/keep/b.mkv"); err != nil || got == nil {
		t.Fatalf("kept directory lost its children: %#v err=%v", got, err)
	}
}

func TestDeletePathTreatsWildcardCharactersLiterally(t *testing.T) {
	st := openMemoryStore(t)
	for _, entry := range []Entry{
		{FSID: "1", Parent: "0", Path: "/a_", Name: "a_", IsDir: true},
		{FSID: "2", Parent: "1", Path: "/a_/x.mkv", Name: "x.mkv"},
		{FSID: "3", Parent: "0", Path: "/ab", Name: "ab", IsDir: true},
		{FSID: "4", Parent: "3", Path: "/ab/y.mkv", Name: "y.mkv"},
	} {
		if err := st.UpsertEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.DeletePath("/a_"); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.GetByPath("/a_/x.mkv"); got != nil {
		t.Fatal("expected /a_ subtree to be deleted")
	}
	if got, _ := st.GetByPath("/ab/y.mkv"); got == nil {
		t.Fatal("sibling matched by LIKE wildcard was deleted")
	}
}
