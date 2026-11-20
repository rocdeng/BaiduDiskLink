package fs

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"math"
	"path"
	"sort"
	"strconv"
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
	startedAt    time.Time
	// flights 按目录路径合并并发刷新：同一目录同一时刻只调用一次百度 List。
	flightMu sync.Mutex
	flights  map[string]*refreshFlight
}

type refreshFlight struct {
	done chan struct{}
	err  error
}

// DefaultMetadataTTL：目录内容缓存的新鲜期。过期后先返回旧数据并在后台刷新，不阻塞 FUSE 调用。
const DefaultMetadataTTL = 5 * time.Minute

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
	return &Filesystem{
		store:      st,
		remote:     r,
		stream:     streamManager,
		negative:   cache.NewNegativeCache(30 * time.Second),
		ttl:        DefaultMetadataTTL,
		primaryGID: primary,
		gids:       out,
		rootPath:   rootPath,
		refreshMu:  make(chan struct{}, 1),
		readDiag:   make(map[string]*readDiagnosticWindow),
		startedAt:  time.Now(),
		flights:    make(map[string]*refreshFlight),
	}
}

// SetMetadataTTL 设置目录缓存新鲜期；必须在挂载前调用。
func (f *Filesystem) SetMetadataTTL(ttl time.Duration) {
	if f == nil || ttl <= 0 {
		return
	}
	f.ttl = ttl
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
	if f == nil || !f.traceReads {
		return
	}
	f.recordReadDiagnostic(time.Now(), path, fsid, offset, requested, returned, elapsed, strategy, true)
}

func (f *Filesystem) recordCanceledRead(path, fsid string, offset int64, requested int) {
	if f == nil || !f.traceReads {
		return
	}
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
	// 挂载前 app 已同步刷新根目录；其余已知目录放到后台，避免拖慢挂载。
	go func() {
		if err := f.RefreshAll(context.Background()); err != nil {
			log.Printf("refresh all on mount failed: %v", err)
		}
	}()
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
	// 固定为启动时间，避免每次 stat 都“变化”触发媒体库实时监控误报。
	out.Mtime = uint64(f.startedAt.Unix())
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
	if err := f.ensureDirLoaded(f.rootPath, "0", true); err != nil {
		return nil, syscall.EIO
	}
	children, err := f.store.ListChildren(f.rootPath)
	if err != nil {
		return nil, syscall.EIO
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
	if err := f.ensureDirLoaded(f.rootPath, "0", true); err != nil {
		return nil, syscall.EIO
	}
	child, err := f.store.GetChild("0", name)
	if err != nil {
		return nil, syscall.EIO
	}
	if child == nil {
		f.markMissing(JoinPath(f.rootPath, name))
		return nil, syscall.ENOENT
	}
	return f.lookupInode(ctx, &f.Inode, *child, out), 0
}

type dirFreshness uint8

const (
	dirFresh dirFreshness = iota
	// dirStale：内容加载过但已过新鲜期，可先返回旧数据。
	dirStale
	// dirMissing：从未加载或被强制过期，必须同步加载。
	dirMissing
)

func (f *Filesystem) freshness(dirPath string) (dirFreshness, error) {
	if f == nil || f.remote == nil {
		return dirFresh, nil
	}
	if f.ttl <= 0 {
		return dirMissing, nil
	}
	current, err := f.store.GetByPath(dirPath)
	if err != nil {
		return dirMissing, err
	}
	if current == nil || current.ExpiresAt == 0 {
		return dirMissing, nil
	}
	if time.Now().Unix() >= current.ExpiresAt {
		return dirStale, nil
	}
	return dirFresh, nil
}

// ensureDirLoaded 实现 stale-while-revalidate：
// 未加载过的目录同步拉取；已过期的目录立即返回旧数据，后台异步刷新。
func (f *Filesystem) ensureDirLoaded(dirPath, fsid string, isRoot bool) error {
	state, err := f.freshness(dirPath)
	if err != nil {
		return err
	}
	refresh := func() error {
		if isRoot {
			return f.refreshRoot(context.Background())
		}
		return f.refreshDir(context.Background(), dirPath, fsid)
	}
	switch state {
	case dirMissing:
		return f.refreshCoalesced(dirPath, refresh, true)
	case dirStale:
		go func() {
			if err := f.refreshCoalesced(dirPath, refresh, false); err != nil {
				log.Printf("background refresh %q failed: %v", dirPath, err)
			}
		}()
	}
	return nil
}

// refreshCoalesced 合并同一目录的并发刷新。wait=false 时若已有刷新在进行则直接返回。
func (f *Filesystem) refreshCoalesced(key string, refresh func() error, wait bool) error {
	f.flightMu.Lock()
	if flight := f.flights[key]; flight != nil {
		f.flightMu.Unlock()
		if !wait {
			return nil
		}
		<-flight.done
		return flight.err
	}
	if f.flights == nil {
		f.flights = make(map[string]*refreshFlight)
	}
	flight := &refreshFlight{done: make(chan struct{})}
	f.flights[key] = flight
	f.flightMu.Unlock()

	flight.err = refresh()

	f.flightMu.Lock()
	delete(f.flights, key)
	f.flightMu.Unlock()
	close(flight.done)
	return flight.err
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
	f.preserveChildDirExpiry("0", mapped)
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
			refresh := func() error { return f.refreshDir(ctx, child.Path, child.FSID) }
			if err := f.refreshCoalesced(child.Path, refresh, true); err != nil {
				// 单个目录失败不影响其余目录的后台刷新。
				log.Printf("refresh dir %q failed: %v", child.Path, err)
				continue
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
	stable := stableAttrFor(e)
	node := &entryNode{
		Filesystem: f,
		entry:      e,
	}
	if out != nil {
		f.fillEntryOut(e, stable.Mode, out)
	}
	return f.NewPersistentInode(ctx, node, stable)
}

// lookupInode 为 Lookup 返回子节点：
//   - inode 号由 fsid 推导、Gen 由 size/mtime 推导，同一未变化文件始终对应同一个 inode，
//     go-fuse 会自动复用已存在的节点，内核页缓存（FOPEN_KEEP_CACHE）因此能命中；
//   - 使用非持久 inode，内核 FORGET 后即可回收，避免反复扫库导致 inode 无限增长；
//   - 文件内容变化（Gen 变化）时，解除旧节点的持久标记，让其可被回收。
func (f *Filesystem) lookupInode(ctx context.Context, parent *goFs.Inode, e store.Entry, out *fuse.EntryOut) *goFs.Inode {
	stable := stableAttrFor(e)
	f.fillEntryOut(e, stable.Mode, out)
	if existing := parent.GetChild(e.Name); existing != nil {
		if existing.StableAttr() == stable {
			return existing
		}
		existing.ForgetPersistent()
	}
	return parent.NewInode(ctx, &entryNode{Filesystem: f, entry: e}, stable)
}

// stableAttrFor 根据元数据生成稳定的 inode 标识。
func stableAttrFor(e store.Entry) goFs.StableAttr {
	mode := uint32(syscall.S_IFREG)
	if e.IsDir {
		mode = syscall.S_IFDIR
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(e.FSID))
	ino := hash.Sum64()
	if parsed, err := strconv.ParseUint(e.FSID, 10, 64); err == nil && parsed < math.MaxUint64-2 {
		ino = parsed + 2
	}
	// 0、1（根）与 ^0 是 go-fuse 保留值。
	if ino <= 1 || ino == math.MaxUint64 {
		ino = 2
	}
	gen := uint64(e.Size)*1_000_003 ^ uint64(e.MTM)
	if e.IsDir {
		// 目录的 mtime 会随刷新保留，不参与 Gen，避免目录节点频繁失效。
		gen = 0
	}
	return goFs.StableAttr{Mode: mode, Ino: ino, Gen: gen}
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
	if n.entry.IsDir {
		if err := n.Filesystem.ensureDirLoaded(n.entry.Path, n.entry.FSID, false); err != nil {
			return nil, syscall.EIO
		}
	}
	children, err := n.Filesystem.store.ListChildren(n.entry.Path)
	if err != nil {
		return nil, syscall.EIO
	}
	return goFs.NewListDirStream(dirEntries(children)), 0
}

func (n *entryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*goFs.Inode, syscall.Errno) {
	if n == nil || n.Filesystem == nil || n.Filesystem.store == nil {
		return nil, syscall.ENOENT
	}
	childPath := JoinPath(n.entry.Path, name)
	if n.Filesystem.negative != nil && n.Filesystem.negative.IsMissing(childPath) {
		return nil, syscall.ENOENT
	}
	if !n.entry.IsDir {
		return nil, syscall.ENOTDIR
	}
	if err := n.Filesystem.ensureDirLoaded(n.entry.Path, n.entry.FSID, false); err != nil {
		return nil, syscall.EIO
	}
	child, err := n.Filesystem.store.GetChild(n.entry.FSID, name)
	if err != nil {
		return nil, syscall.EIO
	}
	if child == nil {
		n.Filesystem.markMissing(childPath)
		return nil, syscall.ENOENT
	}
	return n.Filesystem.lookupInode(ctx, &n.Inode, *child, out), 0
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
	f.markMissing(target.Path)
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
	f.preserveChildDirExpiry(fsid, mapped)
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

// preserveChildDirExpiry：父目录刷新会重写子目录行；沿用子目录原有的 expires_at，
// 否则每次父目录刷新都会把所有已加载的子目录打回“未加载”，迫使下一次 Lookup 同步调用百度 API。
func (f *Filesystem) preserveChildDirExpiry(parentFSID string, mapped []store.Entry) {
	existing, err := f.store.GetChildrenByParent(parentFSID)
	if err != nil || len(existing) == 0 {
		return
	}
	expiry := make(map[string]int64, len(existing))
	for _, entry := range existing {
		if entry.IsDir && entry.ExpiresAt > 0 {
			expiry[entry.Path+"\x00"+entry.FSID] = entry.ExpiresAt
		}
	}
	for index := range mapped {
		if !mapped[index].IsDir {
			continue
		}
		if expiresAt, ok := expiry[mapped[index].Path+"\x00"+mapped[index].FSID]; ok {
			mapped[index].ExpiresAt = expiresAt
		}
	}
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
				if ctx.Err() != nil {
					// 内核因调用进程收到信号而中断了请求：返回 EINTR 让内核/调用方重试，
					// 返回 EIO 会让预读页被标记为错误，播放器随即读到 I/O error。
					return nil, syscall.EINTR
				}
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
	// DefaultPermissions 让内核按 mode/uid/gid 检查访问权限（opt-in）。
	// 开启后只有 root 和 FUSE_GROUP_NAME 组成员能读文件；默认关闭以保持旧版行为。
	DefaultPermissions bool
}

func Mount(mountPath string, root *Filesystem, opts MountOptions) (*fuse.Server, error) {
	if mountPath == "" {
		return nil, errors.New("mount path is required")
	}
	if root == nil {
		return nil, errors.New("root filesystem is required")
	}
	// 视频库元数据很少变化，适当延长内核缓存，减少播放过程中的 Lookup/Getattr 往返。
	timeout := 10 * time.Second
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
	if opts.DefaultPermissions {
		mountOpts.MountOptions.Options = append(mountOpts.MountOptions.Options, "default_permissions")
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
		if f.enableDelete {
			// 内核 default_permissions 要求父目录可写才允许 unlink/rmdir。
			return 0770
		}
		return 0750
	}
	if f != nil && f.enableDelete {
		return 0777
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
