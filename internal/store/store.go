package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Entry struct {
	ID         int64
	FSID       string
	Parent     string
	Path       string
	Name       string
	Size       int64
	IsDir      bool
	MTM        int64
	MD5        string
	LastSyncAt int64
	ExpiresAt  int64
	Negative   bool
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

func Open(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("db is required")
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) UpsertEntry(e Entry) error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.db == nil {
		return errors.New("db is required")
	}
	_, err := s.db.Exec(`
		insert into entries
		(fsid, parent_fsid, path, name, size, is_dir, mtime, md5, last_sync_at, expires_at, negative)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(path) do update set
			fsid=excluded.fsid,
			parent_fsid=excluded.parent_fsid,
			name=excluded.name,
			size=excluded.size,
			is_dir=excluded.is_dir,
			mtime=excluded.mtime,
			md5=excluded.md5,
			last_sync_at=excluded.last_sync_at,
			expires_at=excluded.expires_at,
			negative=excluded.negative
	`, e.FSID, e.Parent, e.Path, e.Name, e.Size, boolToInt(e.IsDir), e.MTM, e.MD5, e.LastSyncAt, e.ExpiresAt, boolToInt(e.Negative))
	return err
}

func (s *Store) ListChildren(path string) ([]Entry, error) {
	if s == nil {
		return nil, errors.New("store is nil")
	}
	if s.db == nil {
		return nil, errors.New("db is required")
	}
	parentID := "0"
	if path != "/" {
		parent, err := s.GetByPath(path)
		if err != nil {
			return nil, err
		}
		if parent == nil {
			return nil, nil
		}
		parentID = parent.FSID
	}
	rows, err := s.db.Query(`
		select id, fsid, parent_fsid, path, name, size, is_dir, mtime, md5, last_sync_at, expires_at, negative
		from entries
		where parent_fsid = ?
		order by name asc
	`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		var e Entry
		var isDir int
		var negative int
		if err := rows.Scan(&e.ID, &e.FSID, &e.Parent, &e.Path, &e.Name, &e.Size, &isDir, &e.MTM, &e.MD5, &e.LastSyncAt, &e.ExpiresAt, &negative); err != nil {
			return nil, err
		}
		e.IsDir = isDir != 0
		e.Negative = negative != 0
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) GetByPath(path string) (*Entry, error) {
	if s == nil {
		return nil, errors.New("store is nil")
	}
	if s.db == nil {
		return nil, errors.New("db is required")
	}
	row := s.db.QueryRow(`
		select id, fsid, parent_fsid, path, name, size, is_dir, mtime, md5, last_sync_at, expires_at, negative
		from entries
		where path = ?
	`, path)
	var e Entry
	var isDir int
	var negative int
	if err := row.Scan(&e.ID, &e.FSID, &e.Parent, &e.Path, &e.Name, &e.Size, &isDir, &e.MTM, &e.MD5, &e.LastSyncAt, &e.ExpiresAt, &negative); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	e.IsDir = isDir != 0
	e.Negative = negative != 0
	return &e, nil
}

func (s *Store) EnsureRoot() error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.db == nil {
		return errors.New("db is required")
	}
	_, err := s.db.Exec(`
		insert into entries (fsid, parent_fsid, path, name, is_dir, last_sync_at, expires_at)
		values ('0', '', '/', '', 1, ?, ?)
		on conflict(path) do nothing
	`, time.Now().Unix(), time.Now().Add(24*time.Hour).Unix())
	return err
}

func (s *Store) UpsertEntries(entries []Entry) error {
	for _, entry := range entries {
		if err := s.UpsertEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetChildrenByParent(parentID string) ([]Entry, error) {
	if s == nil {
		return nil, errors.New("store is nil")
	}
	if s.db == nil {
		return nil, errors.New("db is required")
	}
	rows, err := s.db.Query(`
		select id, fsid, parent_fsid, path, name, size, is_dir, mtime, md5, last_sync_at, expires_at, negative
		from entries
		where parent_fsid = ?
		order by name asc
	`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		var isDir int
		var negative int
		if err := rows.Scan(&e.ID, &e.FSID, &e.Parent, &e.Path, &e.Name, &e.Size, &isDir, &e.MTM, &e.MD5, &e.LastSyncAt, &e.ExpiresAt, &negative); err != nil {
			return nil, err
		}
		e.IsDir = isDir != 0
		e.Negative = negative != 0
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) UpsertFromRemote(parent string, entries []Entry) error {
	for _, entry := range entries {
		if entry.Parent == "" {
			entry.Parent = parent
		}
		if err := s.UpsertEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReplaceChildren(parentID string, entries []Entry) error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.db == nil {
		return errors.New("db is required")
	}
	if _, err := s.db.Exec(`delete from entries where parent_fsid = ?`, parentID); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	values := make([]string, 0, len(entries))
	args := make([]any, 0, len(entries)*11)
	for _, entry := range entries {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args,
			entry.FSID,
			entry.Parent,
			entry.Path,
			entry.Name,
			entry.Size,
			boolToInt(entry.IsDir),
			entry.MTM,
			entry.MD5,
			entry.LastSyncAt,
			entry.ExpiresAt,
			boolToInt(entry.Negative),
		)
	}
	query := fmt.Sprintf(`
		insert into entries
		(fsid, parent_fsid, path, name, size, is_dir, mtime, md5, last_sync_at, expires_at, negative)
		values %s
		on conflict(path) do update set
			fsid=excluded.fsid,
			parent_fsid=excluded.parent_fsid,
			name=excluded.name,
			size=excluded.size,
			is_dir=excluded.is_dir,
			mtime=excluded.mtime,
			md5=excluded.md5,
			last_sync_at=excluded.last_sync_at,
			expires_at=excluded.expires_at,
			negative=excluded.negative
	`, strings.Join(values, ","))
	_, err := s.db.Exec(query, args...)
	return err
}

func (s *Store) ExpirePath(path string) error {
	if s == nil {
		return errors.New("store is nil")
	}
	if s.db == nil {
		return errors.New("db is required")
	}
	_, err := s.db.Exec(`update entries set expires_at = 0 where path = ?`, path)
	return err
}

func (s *Store) migrate() error {
	if s.db == nil {
		return errors.New("db is required")
	}
	_, err := s.db.Exec(`
		create table if not exists entries (
			id integer primary key autoincrement,
			fsid text not null,
			parent_fsid text not null,
			path text not null unique,
			name text not null,
			size integer not null default 0,
			is_dir integer not null default 0,
			mtime integer not null default 0,
			md5 text not null default '',
			last_sync_at integer not null default 0,
			expires_at integer not null default 0,
			negative integer not null default 0
		);
		create index if not exists idx_entries_parent_fsid on entries(parent_fsid);
	`)
	return err
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func DebugString(e Entry) string {
	return fmt.Sprintf("%s (%s)", e.Path, e.Name)
}

func Sorted(entries []Entry) []Entry {
	out := append([]Entry(nil), entries...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}
