//go:build linux

package fs

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	iofs "io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"baidudisklink/internal/baidu"
	"baidudisklink/internal/remote"
	"baidudisklink/internal/store"
	"baidudisklink/internal/stream"
)

// mutableClient：目录内容可在运行中变化（模拟网盘文件被替换），读取返回全零数据。
type mutableClient struct {
	baidu.StaticClient
	mu      sync.Mutex
	version int64
}

func (c *mutableClient) List(path string) ([]baidu.RemoteEntry, error) {
	c.mu.Lock()
	version := c.version
	c.mu.Unlock()
	switch path {
	case "/Videos":
		return []baidu.RemoteEntry{{FSID: "10", ServerName: "BDTV", Path: "/Videos/BDTV", IsDir: true}}, nil
	case "/Videos/BDTV":
		return []baidu.RemoteEntry{{FSID: "11", ServerName: "流人", Path: "/Videos/BDTV/流人", IsDir: true}}, nil
	case "/Videos/BDTV/流人":
		var out []baidu.RemoteEntry
		for i := 0; i < 20; i++ {
			out = append(out, baidu.RemoteEntry{
				FSID:        fmt.Sprintf("%d", 100+i),
				ServerName:  fmt.Sprintf("Slow.Horses.S06E%02d.mkv", i),
				Path:        fmt.Sprintf("/Videos/BDTV/流人/Slow.Horses.S06E%02d.mkv", i),
				Size:        64 << 20,
				ServerMTime: 1000,
			})
		}
		// 只有这些文件的元数据持续变化（触发新的 inode Gen）；它们只被 stat/遍历，不被读取。
		// 正在读取的文件大小持续变化时内核会返回 EIO，旧版本同样如此，不在本测试范围内。
		for i := 0; i < 5; i++ {
			out = append(out, baidu.RemoteEntry{
				FSID:        fmt.Sprintf("%d", 200+i),
				ServerName:  fmt.Sprintf("changing-%d.mkv", i),
				Path:        fmt.Sprintf("/Videos/BDTV/流人/changing-%d.mkv", i),
				Size:        (1 << 20) + version,
				ServerMTime: 1000 + version,
			})
		}
		return out, nil
	}
	return nil, nil
}

// TestFUSEMountSurvivesConcurrentLookupsAndMetadataChanges 在真实内核上挂载，
// 并发执行遍历、stat、读取，同时让文件元数据变化并强制内核回收 inode，
// 用于验证稳定 inode / 非持久 inode 改动不会导致 go-fuse panic（表现为 Transport endpoint is not connected）。
func TestFUSEMountSurvivesConcurrentLookupsAndMetadataChanges(t *testing.T) {
	if os.Getenv("BAIDUDISKLINK_FUSE_IT") == "" {
		t.Skip("set BAIDUDISKLINK_FUSE_IT=1 to run the real FUSE integration test")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse not available")
	}
	// 与生产一致：文件数据库 + WAL + busy_timeout（:memory: 每个连接是独立空库，不适合并发）。
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "meta.db")+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	dbStore, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer dbStore.Close()
	if err := dbStore.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	client := &mutableClient{}
	reader := remote.NewReader(client)
	streamCfg := stream.DefaultConfig()
	streamCfg.DiskCache = 0
	streamCfg.MemoryCache = 64 << 20
	streamCfg.TargetBuffer = 16 << 20
	streamCfg.LowWatermark = 8 << 20
	manager, err := stream.NewManager(reader, streamCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	filesystem := NewFilesystemWithStream(dbStore, reader, manager, []uint32{0}, "/Videos")
	filesystem.SetMetadataTTL(time.Second)
	if err := filesystem.RefreshRootOnly(context.Background()); err != nil {
		t.Fatal(err)
	}
	mountPath := t.TempDir()
	server, err := Mount(mountPath, filesystem, MountOptions{AllowOther: true, GIDs: []uint32{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Unmount() }()
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}

	duration := 20 * time.Second
	if value := os.Getenv("BAIDUDISKLINK_FUSE_IT_SECONDS"); value != "" {
		if parsed, err := time.ParseDuration(value + "s"); err == nil {
			duration = parsed
		}
	}
	deadline := time.Now().Add(duration)
	var ops, failures atomic.Int64
	var firstErr atomic.Value
	record := func(err error) {
		failures.Add(1)
		firstErr.CompareAndSwap(nil, err.Error())
	}
	var wg sync.WaitGroup
	dir := filepath.Join(mountPath, "BDTV", "流人")

	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			buf := make([]byte, 256<<10)
			for time.Now().Before(deadline) {
				ops.Add(1)
				switch rng.Intn(3) {
				case 0:
					_ = filepath.WalkDir(mountPath, func(_ string, _ iofs.DirEntry, err error) error {
						if err != nil {
							record(err)
						}
						return nil
					})
				case 1:
					name := filepath.Join(dir, fmt.Sprintf("changing-%d.mkv", rng.Intn(5)))
					if rng.Intn(2) == 0 {
						name = filepath.Join(dir, fmt.Sprintf("Slow.Horses.S06E%02d.mkv", rng.Intn(20)))
					}
					if _, err := os.Stat(name); err != nil {
						record(err)
					}
				case 2:
					name := filepath.Join(dir, fmt.Sprintf("Slow.Horses.S06E%02d.mkv", rng.Intn(20)))
					file, err := os.Open(name)
					if err != nil {
						record(err)
						continue
					}
					for i := 0; i < 8; i++ {
						if _, err := file.ReadAt(buf, rng.Int63n(60<<20)); err != nil && err != io.EOF {
							record(err)
							break
						}
					}
					_ = file.Close()
				}
			}
		}(int64(worker))
	}
	// 元数据变化（触发新的 inode Gen）；可选强制内核回收 dentry/inode（触发 FORGET）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			client.mu.Lock()
			client.version++
			client.mu.Unlock()
			// drop_caches 会让内核中断进行中的读请求，旧版本同样会出现零星 EIO，默认不开启。
			if os.Getenv("BAIDUDISKLINK_FUSE_IT_DROP_CACHES") != "" {
				_ = os.WriteFile("/proc/sys/vm/drop_caches", []byte("2"), 0o644)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()
	wg.Wait()

	t.Logf("ops=%d failures=%d first=%v", ops.Load(), failures.Load(), firstErr.Load())
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("mount is no longer usable: %v", err)
	}
	if failures.Load() > 0 {
		t.Fatalf("%d filesystem operations failed, first: %v", failures.Load(), firstErr.Load())
	}
}
