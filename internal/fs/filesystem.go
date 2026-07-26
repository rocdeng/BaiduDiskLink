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
	"baidudisklink/internal/stream"
)

type Filesystem struct {
	goFs.Inode
	store        *store.Store
	remote       *remote.Reader
	stream       *stream.Manager
	negative     *cache.NegativeCache
	ttl          time.Duration
	primaryGID   uint32
	gids         map[uint32]struct{}
	rootPath     string
	refreshMu    chan struct{}
	traceReads   bool
	enableDelete bool
	readDiagMu   sync.Mutex
	readDiag     map[string]*readDiagnosticWindow
}

const (
	slowFuseReadThreshold  = 300 * time.Millisecond
	readDiagnosticInterval = 5 * time.Second
)

type readDiagnosticWindow struct {
	startedAt      time.Time
	path           string
	fsid           string
	strategy       string
	slowReads      int
	canceledReads  int
	requestedBytes int64
	returnedBytes  int64
	maxElapsed     time.Duration
	maxOffset      int64
}

func NewFilesystem(st *store.Store, r *remote.Reader, gids []uint32, rootPath string) *Filesystem {
	return NewFilesystemWithStream(st, r, nil, gids, rootPath)
}

func NewFilesystemWithStream(st *store.Store, r *remote.Reader, streamManager *stream.Manager, gids []uint32, rootPath string) *Filesystem {
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
	return &Filesystem{store: st, remote: r, stream: streamManager, ttl: time.Minute, primaryGID: primary, gids: out, rootPath: rootPath, refreshMu: make(chan struct{}, 1), readDiag: make(map[string]*readDiagnosticWindow)}
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

func (f *Filesystem) recordSlowRead(path, fsid string, offset int64, requested, returned int, elapsed time.Duration, strategy string) {
	f.recordReadDiagnostic(time.Now(), path, fsid, offset, requested, returned, elapsed, strategy, true)
}

func (f *Filesystem) recordCanceledRead(path, fsid string, offset int64, requested int) {
	f.recordReadDiagnostic(time.Now(), path, fsid, offset, requested, 0, 0, "", false)
}

func (f *Filesystem) recordReadDiagnostic(now time.Time, path, fsid string, offset int64, requested, returned int, elapsed time.Duration, strategy string, slow bool) {
	if f == nil || fsid == "" {
		return
	}
	f.readDiagMu.Lock()
	if f.readDiag == nil {
		f.readDiag = make(map[string]*readDiagnosticWindow)
	}
	window := f.readDiag[fsid]
	if window == nil {
		window = &readDiagnosticWindow{startedAt: now, path: path, fsid: fsid}
		f.readDiag[fsid] = window
	}
	var flushed *readDiagnosticWindow
	if now.Sub(window.startedAt) >= readDiagnosticInterval {
		copy := *window
		flushed = &copy
		*window = readDiagnosticWindow{startedAt: now, path: path, fsid: fsid}
	}
	if path != "" {
		window.path = path
	}
	if strategy != "" {
		window.strategy = strategy
	}
	if slow {
		window.slowReads++
		window.requestedBytes += int64(requested)
		window.returnedBytes += int64(returned)
		if elapsed > window.maxElapsed {
			window.maxElapsed = elapsed
			window.maxOffset = offset
		}
	} else {
		window.canceledReads++
		window.requestedBytes += int64(requested)
		window.maxOffset = offset
	}
	f.readDiagMu.Unlock()
	if flushed != nil {
		logReadDiagnostic(*flushed)
	}
}

func logReadDiagnostic(window readDiagnosticWindow) {
	if window.slowReads == 0 && window.canceledReads == 0 {
		return
	}
	log.Printf("fuse read summary path=%q fsid=%q window=%s slow_reads=%d canceled_reads=%d requested=%d returned=%d max_elapsed=%s max_offset=%d strategy=%s", window.path, window.fsid, readDiagnosticInterval, window.slowReads, window.canceledReads, window.requestedBytes, window.returnedBytes, window.maxElapsed, window.maxOffset, window.strategy)
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
		startedAt := time.Now()
		var data []byte
		var err error
		if streamHandle := handle.stream(); streamHandle != nil {
			data, err = streamHandle.ReadAt(ctx, off, int64(len(dest)))
			handle.setStrategy("stream-manager")
		} else {
			data, err = n.Filesystem.remote.ReadRange(ctx, n.entry.FSID, off, int64(len(dest)))
			handle.setStrategy("remote-range")
		}
		elapsed := time.Since(startedAt)
		strategy := handle.strategy()
		if elapsed >= slowFuseReadThreshold {
			n.Filesystem.recordSlowRead(n.entry.Path, n.entry.FSID, off, len(dest), len(data), elapsed, strategy)
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				n.Filesystem.recordCanceledRead(n.entry.Path, n.entry.FSID, off, len(dest))
			} else {
				log.Printf("fuse read failed path=%q fsid=%q offset=%d length=%d: %v", n.entry.Path, n.entry.FSID, off, len(dest), err)
			}
			return nil, syscall.EIO
		}
		n.traceRead(off, len(dest), len(data), strategy)
		return fuse.ReadResultData(data), 0
	}
	if n.entry.Size > 0 && len(dest) > 0 {
		if data, ok := n.Filesystem.remote.ReadCachedWindow(n.entry.FSID, off, int64(len(dest))); ok {
			n.traceRead(off, len(dest), len(data), "global-cache")
			return fuse.ReadResultData(data), 0
		}
	}
	data, err := n.Filesystem.remote.ReadRange(ctx, n.entry.FSID, off, int64(len(dest)))
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
	if !shouldTraceRead(off, strategy, requested, returned) {
		return
	}
	log.Printf("fuse read path=%q fsid=%q offset=%d requested=%d returned=%d strategy=%s", n.entry.Path, n.entry.FSID, off, requested, returned, strategy)
}

func shouldTraceRead(off int64, strategy string, requested, returned int) bool {
	if returned != requested {
		return true
	}
	return strategy == "stream-event"
}

func (n *entryNode) Opendir(ctx context.Context) syscall.Errno {
	return 0
}

func (n *entryNode) Open(ctx context.Context, openFlags uint32) (fh goFs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.entry.IsDir {
		return nil, 0, syscall.EISDIR
	}
	if n.Filesystem != nil && n.Filesystem.stream != nil {
		file := streamFile(n.entry)
		streamHandle := n.Filesystem.stream.Open(file)
		if streamHandle == nil {
			return nil, 0, syscall.EIO
		}
		return &entryFileHandle{streamHandle: streamHandle}, fuse.FOPEN_KEEP_CACHE, 0
	}
	return &entryFileHandle{}, fuse.FOPEN_KEEP_CACHE, 0
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
	lastStrategy string
	streamHandle *stream.Handle
}

func (h *entryFileHandle) Release(_ context.Context) syscall.Errno {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	streamHandle := h.streamHandle
	h.streamHandle = nil
	h.mu.Unlock()
	if streamHandle != nil {
		streamHandle.Release()
	}
	return 0
}

func (h *entryFileHandle) setStrategy(strategy string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.lastStrategy = strategy
	h.mu.Unlock()
}

func (h *entryFileHandle) stream() *stream.Handle {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.streamHandle
}

func (h *entryFileHandle) strategy() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastStrategy
}

func streamFile(entry store.Entry) stream.File {
	return stream.File{FSID: entry.FSID, Size: entry.Size, MTM: entry.MTM}
}
