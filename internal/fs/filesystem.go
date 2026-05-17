package fs

import (
	"context"
	"errors"
	"log"
	"path"
	"sort"
	"syscall"
	"time"

	goFs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"baidudisklink/internal/cache"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
)

type Filesystem struct {
	goFs.Inode
	store    *store.Store
	remote   *remote.Reader
	negative *cache.NegativeCache
	ttl      time.Duration
	gids     map[uint32]struct{}
}

func NewFilesystem(st *store.Store, r *remote.Reader, gids []uint32) *Filesystem {
	out := make(map[uint32]struct{}, len(gids))
	for _, gid := range gids {
		out[gid] = struct{}{}
	}
	return &Filesystem{store: st, remote: r, ttl: time.Minute, gids: out}
}

func (f *Filesystem) OnAdd(ctx context.Context) {
	if f == nil {
		return
	}
	f.populate(ctx)
}

func (f *Filesystem) Getattr(ctx context.Context, fh goFs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | f.dirMode()
	out.Gid = f.firstGID()
	return 0
}

func (f *Filesystem) Opendir(ctx context.Context) syscall.Errno {
	return 0
}

func (f *Filesystem) Readdir(ctx context.Context) (goFs.DirStream, syscall.Errno) {
	if f == nil || f.store == nil {
		return goFs.NewListDirStream(nil), 0
	}
	children, err := f.store.ListChildren("/")
	if err != nil {
		return nil, syscall.EIO
	}
	if f.shouldRefreshRoot(children) {
		if err := f.refreshRoot(ctx); err != nil {
			return nil, syscall.EIO
		}
		children, err = f.store.ListChildren("/")
		if err != nil {
			return nil, syscall.EIO
		}
	}
	return goFs.NewListDirStream(dirEntries(children)), 0
}

func (f *Filesystem) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*goFs.Inode, syscall.Errno) {
	if f == nil || f.store == nil {
		return nil, syscall.ENOENT
	}
	if f.negative != nil && f.negative.IsMissing("/"+name) {
		return nil, syscall.ENOENT
	}
	children, err := f.store.ListChildren("/")
	if err != nil {
		return nil, syscall.EIO
	}
	if f.shouldRefreshRoot(children) {
		if err := f.refreshRoot(ctx); err != nil {
			f.markMissing("/" + name)
			return nil, syscall.ENOENT
		}
		children, err = f.store.ListChildren("/")
		if err != nil {
			return nil, syscall.EIO
		}
	}
	for _, child := range children {
		if child.Name != name {
			continue
		}
		return f.newEntryInode(ctx, child, out), 0
	}
	f.markMissing("/" + name)
	return nil, syscall.ENOENT
}

func (f *Filesystem) refreshRoot(ctx context.Context) error {
	if f == nil || f.store == nil || f.remote == nil {
		return nil
	}
	entries, err := f.remote.List("/")
	if err != nil {
		return err
	}
	log.Printf("refresh root loaded %d entries", len(entries))
	if len(entries) == 0 {
		f.markMissing("/")
		return nil
	}
	mapped := make([]store.Entry, 0, len(entries))
	for _, entry := range entries {
		mapped = append(mapped, store.Entry{
			FSID:      entry.FSID,
			Parent:    "0",
			Path:      entry.Path,
			Name:      entry.ServerName,
			Size:      entry.Size,
			IsDir:     entry.IsDir,
			MTM:       entry.ServerMTime,
			MD5:       entry.MD5,
			ExpiresAt: time.Now().Add(f.ttl).Unix(),
			LastSyncAt: time.Now().Unix(),
		})
	}
	if err := f.store.ReplaceChildren("0", mapped); err != nil {
		return err
	}
	return f.store.UpsertEntry(store.Entry{
		FSID:       "0",
		Parent:     "",
		Path:       "/",
		Name:       "",
		IsDir:      true,
		LastSyncAt:  time.Now().Unix(),
		ExpiresAt:   time.Now().Add(f.ttl).Unix(),
	})
}

func (f *Filesystem) populate(ctx context.Context) {
	if f == nil || f.store == nil {
		return
	}
	entries, _ := f.store.ListChildren("/")
	for _, e := range entries {
		f.addEntry(ctx, e)
	}
}

func (f *Filesystem) addEntry(ctx context.Context, e store.Entry) {
	child := f.newEntryInode(ctx, e, nil)
	if child == nil {
		return
	}
	f.AddChild(e.Name, child, true)
}

func (f *Filesystem) newEntryInode(ctx context.Context, e store.Entry, out *fuse.EntryOut) *goFs.Inode {
	if f == nil {
		return nil
	}
	stable := goFs.StableAttr{}
	mode := uint32(syscall.S_IFREG)
	if e.IsDir {
		mode = syscall.S_IFDIR
	}
	stable.Mode = mode
	node := &entryNode{
		Filesystem: f,
		entry:      e,
	}
	if out != nil {
		perm := f.fileMode()
		if e.IsDir {
			perm = f.dirMode()
		}
		out.Mode = mode | perm
		out.Gid = f.firstGID()
		out.Size = uint64(e.Size)
	}
	return f.NewPersistentInode(ctx, node, stable)
}

func dirEntries(children []store.Entry) []fuse.DirEntry {
	list := make([]fuse.DirEntry, 0, len(children))
	for _, child := range children {
		mode := uint32(syscall.S_IFREG)
		if child.IsDir {
			mode = syscall.S_IFDIR
		}
		list = append(list, fuse.DirEntry{Name: child.Name, Mode: mode})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	return list
}

type entryNode struct {
	goFs.Inode
	Filesystem *Filesystem
	entry      store.Entry
}

var _ = (goFs.NodeGetattrer)((*entryNode)(nil))
var _ = (goFs.NodeLookuper)((*entryNode)(nil))
var _ = (goFs.NodeReaddirer)((*entryNode)(nil))
var _ = (goFs.NodeReader)((*entryNode)(nil))

func (n *entryNode) Getattr(ctx context.Context, fh goFs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.entry.IsDir {
		out.Mode = syscall.S_IFDIR | n.Filesystem.dirMode()
		out.Gid = n.Filesystem.firstGID()
		return 0
	}
	out.Mode = syscall.S_IFREG | n.Filesystem.fileMode()
	out.Gid = n.Filesystem.firstGID()
	out.Size = uint64(n.entry.Size)
	return 0
}

func (n *entryNode) Readdir(ctx context.Context) (goFs.DirStream, syscall.Errno) {
	if n == nil || n.Filesystem == nil || n.Filesystem.store == nil {
		return goFs.NewListDirStream(nil), 0
	}
	children, err := n.Filesystem.store.ListChildren(n.entry.Path)
	if err != nil {
		return nil, syscall.EIO
	}
	if n.Filesystem.shouldRefreshDir(n.entry.Path, children) && n.Filesystem.remote != nil && n.entry.IsDir {
		if err := n.Filesystem.refreshDir(ctx, n.entry.Path, n.entry.FSID); err != nil {
			return nil, syscall.EIO
		}
		children, err = n.Filesystem.store.ListChildren(n.entry.Path)
		if err != nil {
			return nil, syscall.EIO
		}
	}
	return goFs.NewListDirStream(dirEntries(children)), 0
}

func (n *entryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*goFs.Inode, syscall.Errno) {
	if n == nil || n.Filesystem == nil || n.Filesystem.store == nil {
		return nil, syscall.ENOENT
	}
	children, err := n.Filesystem.store.ListChildren(n.entry.Path)
	if err != nil {
		return nil, syscall.EIO
	}
	if n.Filesystem.shouldRefreshDir(n.entry.Path, children) && n.Filesystem.remote != nil && n.entry.IsDir {
		if err := n.Filesystem.refreshDir(ctx, n.entry.Path, n.entry.FSID); err != nil {
			return nil, syscall.ENOENT
		}
		children, err = n.Filesystem.store.ListChildren(n.entry.Path)
		if err != nil {
			return nil, syscall.EIO
		}
	}
	for _, child := range children {
		if child.Name != name {
			continue
		}
		mode := uint32(syscall.S_IFREG)
		if child.IsDir {
			mode = syscall.S_IFDIR
		}
		out.Mode = mode | 0755
		out.Size = uint64(child.Size)
		inode := n.NewPersistentInode(ctx, &entryNode{
			Filesystem: n.Filesystem,
			entry:      child,
		}, goFs.StableAttr{Mode: mode})
		return inode, 0
	}
	return nil, syscall.ENOENT
}

func (f *Filesystem) refreshDir(ctx context.Context, dirPath string, fsid string) error {
	if f == nil || f.store == nil || f.remote == nil {
		return nil
	}
	entries, err := f.remote.List(dirPath)
	if err != nil {
		return err
	}
	log.Printf("refresh dir %q loaded %d entries", dirPath, len(entries))
	if len(entries) == 0 {
		f.markMissing(dirPath)
		return nil
	}
	mapped := make([]store.Entry, 0, len(entries))
	for _, entry := range entries {
		mapped = append(mapped, store.Entry{
			FSID:       entry.FSID,
			Parent:     fsid,
			Path:       entry.Path,
			Name:       entry.ServerName,
			Size:       entry.Size,
			IsDir:      entry.IsDir,
			MTM:        entry.ServerMTime,
			MD5:        entry.MD5,
			ExpiresAt:   time.Now().Add(f.ttl).Unix(),
			LastSyncAt:  time.Now().Unix(),
		})
	}
	if err := f.store.ReplaceChildren(fsid, mapped); err != nil {
		return err
	}
	parent := "0"
	if existing, err := f.store.GetByPath(dirPath); err == nil && existing != nil && existing.Parent != "" {
		parent = existing.Parent
	}
	return f.store.UpsertEntry(store.Entry{
		FSID:       fsid,
		Parent:     parent,
		Path:       dirPath,
		Name:       path.Base(dirPath),
		IsDir:      true,
		LastSyncAt: time.Now().Unix(),
		ExpiresAt:  time.Now().Add(f.ttl).Unix(),
	})
}

func (f *Filesystem) markMissing(path string) {
	if f == nil {
		return
	}
	if f.negative == nil {
		f.negative = cache.NewNegativeCache(30 * time.Second)
	}
	f.negative.MarkMissing(path)
}

func (n *entryNode) Read(ctx context.Context, fh goFs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if n == nil || n.Filesystem == nil || n.Filesystem.remote == nil {
		return fuse.ReadResultData(nil), 0
	}
	data, err := n.Filesystem.remote.ReadRange(n.entry.FSID, off, int64(len(dest)))
	if err != nil {
		log.Printf("fuse read failed path=%q fsid=%q offset=%d length=%d: %v", n.entry.Path, n.entry.FSID, off, len(dest), err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(data), 0
}

func (n *entryNode) Opendir(ctx context.Context) syscall.Errno {
	return 0
}

func (n *entryNode) Open(ctx context.Context, openFlags uint32) (fh goFs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.entry.IsDir {
		return nil, 0, syscall.EISDIR
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *Filesystem) shouldRefreshRoot(children []store.Entry) bool {
	return f.shouldRefresh("/", children)
}

func (f *Filesystem) shouldRefreshDir(path string, children []store.Entry) bool {
	return f.shouldRefresh(path, children)
}

func (f *Filesystem) shouldRefresh(path string, children []store.Entry) bool {
	if f == nil || f.remote == nil {
		return false
	}
	if f.ttl <= 0 {
		return true
	}
	if len(children) == 0 {
		return true
	}
	if path == "" {
		path = "/"
	}
	current, err := f.store.GetByPath(path)
	if err != nil || current == nil {
		return true
	}
	if current.ExpiresAt == 0 {
		return true
	}
	if time.Unix(current.ExpiresAt, 0).Before(time.Now()) {
		return true
	}
	return false
}

type MountOptions struct {
	AllowOther bool
	GIDs       []uint32
}

func Mount(mountPath string, root *Filesystem, opts MountOptions) (*fuse.Server, error) {
	if mountPath == "" {
		return nil, errors.New("mount path is required")
	}
	if root == nil {
		return nil, errors.New("root filesystem is required")
	}
	mountOpts := &goFs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: opts.AllowOther,
		},
	}
	if len(opts.GIDs) > 0 {
		mountOpts.GID = opts.GIDs[0]
	}
	return goFs.Mount(mountPath, root, mountOpts)
}

func JoinPath(parent, name string) string {
	if parent == "" || parent == "/" {
		return path.Join("/", name)
	}
	return path.Join(parent, name)
}

func (f *Filesystem) dirMode() uint32 {
	if f != nil && len(f.gids) > 0 {
		return 0750
	}
	return 0755
}

func (f *Filesystem) fileMode() uint32 {
	if f != nil && len(f.gids) > 0 {
		return 0640
	}
	return 0644
}

func (f *Filesystem) firstGID() uint32 {
	if f == nil {
		return 0
	}
	for gid := range f.gids {
		return gid
	}
	return 0
}
