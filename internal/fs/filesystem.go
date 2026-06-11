package fs

import (
	"context"
	"errors"
	"log"
	"path"
	"sort"
	"strings"
	"sync"
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
	store        *store.Store
	remote       *remote.Reader
	negative     *cache.NegativeCache
	ttl          time.Duration
	primaryGID   uint32
	gids         map[uint32]struct{}
	rootPath     string
	refreshMu    chan struct{}
	traceReads   bool
	enableDelete bool
}

const fuseReadWindowSize int64 = 16 << 20

func NewFilesystem(st *store.Store, r *remote.Reader, gids []uint32, rootPath string) *Filesystem {
	out := make(map[uint32]struct{}, len(gids))
	var primary uint32
	for _, gid := range gids {
		out[gid] = struct{}{}
		if primary == 0 {
			primary = gid
		}
	}
	if rootPath == "" {
		rootPath = "/"
	}
	return &Filesystem{store: st, remote: r, ttl: time.Minute, primaryGID: primary, gids: out, rootPath: rootPath, refreshMu: make(chan struct{}, 1)}
}

func (f *Filesystem) SetTraceReads(enabled bool) {
	if f == nil {
		return
	}
	f.traceReads = enabled
}

func (f *Filesystem) SetDeleteEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.enableDelete = enabled
}

func (f *Filesystem) OnAdd(ctx context.Context) {
	if f == nil {
		return
	}
	f.populate(ctx)
	if err := f.RefreshAll(ctx); err != nil {
		log.Printf("refresh all on mount failed: %v", err)
	}
}

func (f *Filesystem) tryRefreshToken() bool {
	if f == nil || f.refreshMu == nil {
		return false
	}
	select {
	case f.refreshMu <- struct{}{}:
		return true
	default:
		return false
	}
}

func (f *Filesystem) releaseRefreshToken() {
	if f == nil || f.refreshMu == nil {
		return
	}
	select {
	case <-f.refreshMu:
	default:
	}
}

func (f *Filesystem) Getattr(ctx context.Context, fh goFs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | f.dirMode()
	out.Gid = f.primaryGID
	out.Mtime = uint64(time.Now().Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	out.Size = 0
	return 0
}

func (f *Filesystem) Opendir(ctx context.Context) syscall.Errno {
	return 0
}

func (f *Filesystem) Unlink(ctx context.Context, name string) syscall.Errno {
	return f.deleteChild(ctx, f.rootPath, name, false)
}

func (f *Filesystem) Rmdir(ctx context.Context, name string) syscall.Errno {
	return f.deleteChild(ctx, f.rootPath, name, true)
}

func (f *Filesystem) Readdir(ctx context.Context) (goFs.DirStream, syscall.Errno) {
	if f == nil || f.store == nil {
		return goFs.NewListDirStream(nil), 0
	}
	children, err := f.store.ListChildren(f.rootPath)
	if err != nil {
		return nil, syscall.EIO
	}
	if f.shouldRefreshRoot(children) {
		if err := f.refreshRoot(ctx); err != nil {
			return nil, syscall.EIO
		}
		children, err = f.store.ListChildren(f.rootPath)
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
	if f.negative != nil && f.negative.IsMissing(JoinPath(f.rootPath, name)) {
		return nil, syscall.ENOENT
	}
	children, err := f.store.ListChildren(f.rootPath)
	if err != nil {
		return nil, syscall.EIO
	}
	if f.shouldRefreshRoot(children) {
		if err := f.refreshRoot(ctx); err != nil {
			f.markMissing(JoinPath(f.rootPath, name))
			return nil, syscall.ENOENT
		}
		children, err = f.store.ListChildren(f.rootPath)
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
	f.markMissing(JoinPath(f.rootPath, name))
	return nil, syscall.ENOENT
}

func (f *Filesystem) refreshRoot(ctx context.Context) error {
	if f == nil || f.store == nil || f.remote == nil {
		return nil
	}
	entries, err := f.remote.List(f.rootPath)
	if err != nil {
		return err
	}
	log.Printf("refresh root %q loaded %d entries", f.rootPath, len(entries))
	if len(entries) == 0 {
		f.markMissing(f.rootPath)
		return nil
	}
	mapped := make([]store.Entry, 0, len(entries))
	for _, entry := range entries {
		relPath := trimRootPrefix(f.rootPath, entry.Path)
		if relPath == "" {
			relPath = entry.ServerName
		}
		if relPath != "" && !strings.HasPrefix(relPath, "/") {
			relPath = "/" + relPath
		}
		mapped = append(mapped, store.Entry{
			FSID:       entry.FSID,
			Parent:     "0",
			Path:       path.Join(f.rootPath, relPath),
			Name:       entry.ServerName,
			Size:       entry.Size,
			IsDir:      entry.IsDir,
			MTM:        entry.ServerMTime,
			MD5:        entry.MD5,
			ExpiresAt:  f.childExpiresAt(entry.IsDir),
			LastSyncAt: time.Now().Unix(),
		})
	}
	if err := f.store.ReplaceChildren("0", mapped); err != nil {
		return err
	}
	mtm := f.existingMTime(f.rootPath)
	return f.store.UpsertEntry(store.Entry{
		FSID:       "0",
		Parent:     "",
		Path:       f.rootPath,
		Name:       "",
		IsDir:      true,
		MTM:        mtm,
		LastSyncAt: time.Now().Unix(),
		ExpiresAt:  time.Now().Add(f.ttl).Unix(),
	})
}

func (f *Filesystem) RefreshAll(ctx context.Context) error {
	if f == nil || f.store == nil || f.remote == nil {
		return nil
	}
	if !f.tryRefreshToken() {
		return nil
	}
	defer f.releaseRefreshToken()
	if err := f.refreshRoot(ctx); err != nil {
		return err
	}
	visited := make(map[string]struct{})
	return f.refreshKnownDirectories(ctx, f.rootPath, visited)
}

func (f *Filesystem) RefreshRootOnly(ctx context.Context) error {
	if f == nil || f.store == nil || f.remote == nil {
		return nil
	}
	if !f.tryRefreshToken() {
		return nil
	}
	defer f.releaseRefreshToken()
	return f.refreshRoot(ctx)
}

func (f *Filesystem) refreshKnownDirectories(ctx context.Context, current string, visited map[string]struct{}) error {
	if f == nil || f.store == nil || visited == nil {
		return nil
	}
	if _, ok := visited[current]; ok {
		return nil
	}
	visited[current] = struct{}{}
	children, err := f.store.ListChildren(current)
	if err != nil {
		return err
	}
	for _, child := range children {
		if !child.IsDir {
			continue
		}
		if f.remote != nil {
			if err := f.refreshDir(ctx, child.Path, child.FSID); err != nil {
				return err
			}
		}
		if err := f.refreshKnownDirectories(ctx, child.Path, visited); err != nil {
			return err
		}
	}
	return nil
}

func (f *Filesystem) populate(ctx context.Context) {
	if f == nil || f.store == nil {
		return
	}
	entries, _ := f.store.ListChildren(f.rootPath)
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
		f.fillEntryOut(e, mode, out)
	}
	return f.NewPersistentInode(ctx, node, stable)
}

func (f *Filesystem) fillEntryOut(e store.Entry, mode uint32, out *fuse.EntryOut) {
	if f == nil || out == nil {
		return
	}
	perm := f.fileMode()
	if e.IsDir {
		perm = f.dirMode()
	}
	out.Mode = mode | perm
	out.Gid = f.primaryGID
	t := inodeTime(e.MTM)
	out.Mtime = uint64(t.Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	out.Size = uint64(e.Size)
	if e.IsDir {
		out.Size = 0
		out.Blocks = 0
		return
	}
	out.Blocks = (uint64(e.Size) + 511) / 512
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
var _ = (goFs.NodeUnlinker)((*entryNode)(nil))
var _ = (goFs.NodeRmdirer)((*entryNode)(nil))

func (n *entryNode) Getattr(ctx context.Context, fh goFs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.entry.IsDir {
		out.Mode = syscall.S_IFDIR | n.Filesystem.dirMode()
		out.Gid = n.Filesystem.primaryGID
		t := inodeTime(n.entry.MTM)
		out.Mtime = uint64(t.Unix())
		out.Atime = out.Mtime
		out.Ctime = out.Mtime
		out.Size = 0
		out.Blocks = 0
		return 0
	}
	out.Mode = syscall.S_IFREG | n.Filesystem.fileMode()
	out.Gid = n.Filesystem.primaryGID
	t := inodeTime(n.entry.MTM)
	out.Mtime = uint64(t.Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	out.Size = uint64(n.entry.Size)
	out.Blocks = (uint64(n.entry.Size) + 511) / 512
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
		n.Filesystem.fillEntryOut(child, mode, out)
		inode := n.NewPersistentInode(ctx, &entryNode{
			Filesystem: n.Filesystem,
			entry:      child,
		}, goFs.StableAttr{Mode: mode})
		return inode, 0
	}
	return nil, syscall.ENOENT
}

func (n *entryNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.deleteChild(ctx, name, false)
}

func (n *entryNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.deleteChild(ctx, name, true)
}

func (n *entryNode) deleteChild(ctx context.Context, name string, wantDir bool) syscall.Errno {
	if n == nil || n.Filesystem == nil || n.Filesystem.store == nil || n.Filesystem.remote == nil {
		return syscall.EIO
	}
	return n.Filesystem.deleteChild(ctx, n.entry.Path, name, wantDir)
}

func (f *Filesystem) deleteChild(ctx context.Context, parentPath string, name string, wantDir bool) syscall.Errno {
	if f == nil || f.store == nil || f.remote == nil {
		return syscall.EIO
	}
	if !f.enableDelete {
		return syscall.EROFS
	}
	children, err := f.store.ListChildren(parentPath)
	if err != nil {
		return syscall.EIO
	}
	var target *store.Entry
	for _, child := range children {
		if child.Name == name {
			copy := child
			target = &copy
			break
		}
	}
	if target == nil {
		return syscall.ENOENT
	}
	if wantDir && !target.IsDir {
		return syscall.ENOTDIR
	}
	if !wantDir && target.IsDir {
		return syscall.EISDIR
	}
	if target.Path == "" || target.Path == f.rootPath || target.Path == "/" {
		return syscall.EPERM
	}
	if err := f.remote.Delete([]string{target.Path}); err != nil {
		log.Printf("fuse delete failed path=%q fsid=%q: %v", target.Path, target.FSID, err)
		return syscall.EIO
	}
	if err := f.store.DeletePath(target.Path); err != nil {
		log.Printf("delete metadata failed path=%q fsid=%q: %v", target.Path, target.FSID, err)
		return syscall.EIO
	}
	log.Printf("fuse delete path=%q fsid=%q", target.Path, target.FSID)
	return 0
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
			ExpiresAt:  f.childExpiresAt(entry.IsDir),
			LastSyncAt: time.Now().Unix(),
		})
	}
	if err := f.store.ReplaceChildren(fsid, mapped); err != nil {
		return err
	}
	parent := "0"
	if existing, err := f.store.GetByPath(dirPath); err == nil && existing != nil && existing.Parent != "" {
		parent = existing.Parent
	}
	mtm := f.existingMTime(dirPath)
	return f.store.UpsertEntry(store.Entry{
		FSID:       fsid,
		Parent:     parent,
		Path:       dirPath,
		Name:       path.Base(dirPath),
		IsDir:      true,
		MTM:        mtm,
		LastSyncAt: time.Now().Unix(),
		ExpiresAt:  time.Now().Add(f.ttl).Unix(),
	})
}

func (f *Filesystem) childExpiresAt(isDir bool) int64 {
	if isDir {
		return 0
	}
	return time.Now().Add(f.ttl).Unix()
}

func (f *Filesystem) existingMTime(dirPath string) int64 {
	if f == nil || f.store == nil {
		return 0
	}
	existing, err := f.store.GetByPath(dirPath)
	if err != nil || existing == nil {
		return 0
	}
	return existing.MTM
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
	if n.entry.Size > 0 && off >= n.entry.Size {
		return fuse.ReadResultData(nil), 0
	}
	if handle, ok := fh.(*entryFileHandle); ok {
		data, err := handle.read(ctx, n.Filesystem.remote, n.entry, off, int64(len(dest)))
		if err != nil {
			log.Printf("fuse read failed path=%q fsid=%q offset=%d length=%d: %v", n.entry.Path, n.entry.FSID, off, len(dest), err)
			return nil, syscall.EIO
		}
		n.traceRead(off, len(dest), len(data), handle.lastStrategy)
		return fuse.ReadResultData(data), 0
	}
	if n.entry.Size > 0 && len(dest) > 0 {
		if data, ok := n.Filesystem.remote.ReadCachedWindow(n.entry.FSID, off, int64(len(dest))); ok {
			n.traceRead(off, len(dest), len(data), "global-cache")
			return fuse.ReadResultData(data), 0
		}
	}
	data, err := n.Filesystem.remote.ReadRange(n.entry.FSID, off, int64(len(dest)))
	if err != nil {
		log.Printf("fuse read failed path=%q fsid=%q offset=%d length=%d: %v", n.entry.Path, n.entry.FSID, off, len(dest), err)
		return nil, syscall.EIO
	}
	n.traceRead(off, len(dest), len(data), "node-range")
	return fuse.ReadResultData(data), 0
}

func (n *entryNode) traceRead(off int64, requested, returned int, strategy string) {
	if n == nil || n.Filesystem == nil || !n.Filesystem.traceReads {
		return
	}
	log.Printf("fuse read path=%q fsid=%q offset=%d requested=%d returned=%d strategy=%s", n.entry.Path, n.entry.FSID, off, requested, returned, strategy)
}

func (n *entryNode) Opendir(ctx context.Context) syscall.Errno {
	return 0
}

func (n *entryNode) Open(ctx context.Context, openFlags uint32) (fh goFs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.entry.IsDir {
		return nil, 0, syscall.EISDIR
	}
	return &entryFileHandle{windowSize: fuseReadWindowSize, lastRead: -1}, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *Filesystem) shouldRefreshRoot(children []store.Entry) bool {
	return f.shouldRefresh(f.rootPath, children)
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
	if path == "" {
		path = f.rootPath
	}
	current, err := f.store.GetByPath(path)
	if err != nil || current == nil {
		return true
	}
	if current.ExpiresAt == 0 {
		return true
	}
	if time.Until(time.Unix(current.ExpiresAt, 0)) <= 0 {
		return true
	}
	return false
}

func trimRootPrefix(rootPath, fullPath string) string {
	if rootPath == "" || rootPath == "/" {
		return fullPath
	}
	if fullPath == rootPath {
		return ""
	}
	if strings.HasPrefix(fullPath, rootPath+"/") {
		return strings.TrimPrefix(fullPath, rootPath)
	}
	return fullPath
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
	timeout := time.Second
	maxIO := 1 << 20
	mountOpts := &goFs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:   opts.AllowOther,
			MaxWrite:     maxIO,
			MaxReadAhead: maxIO,
		},
		AttrTimeout:  &timeout,
		EntryTimeout: &timeout,
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

func inodeTime(mtm int64) time.Time {
	if mtm > 0 {
		return time.Unix(mtm, 0)
	}
	return time.Now()
}

type entryFileHandle struct {
	mu           sync.Mutex
	window       []byte
	windowOff    int64
	windowSize   int64
	lastRead     int64
	lastStrategy string
}

func (h *entryFileHandle) read(ctx context.Context, remote *remote.Reader, entry store.Entry, off, length int64) ([]byte, error) {
	if h == nil || remote == nil {
		return nil, nil
	}
	if length <= 0 {
		return []byte{}, nil
	}
	if entry.Size > 0 && off >= entry.Size {
		return []byte{}, nil
	}
	if h.windowSize <= 0 {
		h.windowSize = fuseReadWindowSize
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	firstHighOffsetRead := h.lastRead < 0 && off >= h.windowSize
	jump := firstHighOffsetRead || (h.lastRead >= 0 && off > h.lastRead+int64(len(h.window)) && off-h.lastRead > h.windowSize)
	h.lastRead = off
	if len(h.window) > 0 {
		if data, ok := h.sliceWindow(off, length); ok {
			h.lastStrategy = "handle-cache"
			return data, nil
		}
	}
	fetchLen := h.windowSize
	if jump {
		fetchLen = min64(length*2, h.windowSize/2)
		if fetchLen < length {
			fetchLen = length
		}
	}
	fetchOff := off
	if !jump {
		fetchOff = (off / h.windowSize) * h.windowSize
	}
	if fetchLen < length {
		fetchLen = length
	}
	if entry.Size > 0 && fetchOff+fetchLen > entry.Size {
		fetchLen = entry.Size - fetchOff
	}
	var data []byte
	var err error
	if jump {
		data, err = remote.ReadExactRange(entry.FSID, fetchOff, fetchLen)
		h.lastStrategy = "seek-exact"
	} else {
		data, err = remote.ReadRange(entry.FSID, fetchOff, fetchLen)
		h.lastStrategy = "window-prefetch"
	}
	if err != nil {
		return nil, err
	}
	h.windowOff = fetchOff
	h.window = append(h.window[:0], data...)
	return h.sliceWindowOrEmpty(off, length), nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (h *entryFileHandle) sliceWindowOrEmpty(off, length int64) []byte {
	data, ok := h.sliceWindow(off, length)
	if !ok {
		return []byte{}
	}
	return data
}

func (h *entryFileHandle) sliceWindow(off, length int64) ([]byte, bool) {
	if h == nil || len(h.window) == 0 || length <= 0 {
		return nil, false
	}
	windowEnd := h.windowOff + int64(len(h.window))
	if off < h.windowOff || off >= windowEnd {
		return nil, false
	}
	start := off - h.windowOff
	end := start + length
	if end > int64(len(h.window)) {
		end = int64(len(h.window))
	}
	if end <= start {
		return nil, false
	}
	return append([]byte(nil), h.window[start:end]...), true
}
