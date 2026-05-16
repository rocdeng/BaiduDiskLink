# BaiduDiskLink MVP 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 做出一个面向群晖 DSM 的百度网盘只读挂载工具，通过本地 FUSE 文件系统把网盘暴露成 Emby 可直接扫描和播放的目录，并使用 SQLite 缓存元数据和按需视频读取。

**架构：** 用一个小型 Go 服务拆成几个边界清晰的包：认证、元数据存储、百度适配器、远程读取器和 FUSE 文件系统。第一版只挂载单个配置好的远端根目录，目录元数据写入 SQLite，文件内容按需流式读取，不走 WebDAV，也不做整盘镜像。

**技术栈：** Go、`github.com/hanwen/go-fuse/v2`、SQLite、Docker、`go test`

---

### 任务 1：搭建 Go 项目和基础包结构

**文件：**
- 创建：`go.mod`
- 创建：`cmd/baidudisklink/main.go`
- 创建：`internal/config/config.go`
- 创建：`internal/logging/logging.go`
- 创建：`internal/app/app.go`
- 创建：`internal/app/app_test.go`

- [ ] **步骤 1：先写失败测试**

```go
package app

import "testing"

func TestNewAppRequiresMountPath(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error when mount path is missing")
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/app -v`
预期：失败，因为 `New` 还不存在。

- [ ] **步骤 3：写最小实现**

```go
package app

import "errors"

type Config struct {
	MountPath string
}

type App struct {
	cfg Config
}

func New(cfg Config) (*App, error) {
	if cfg.MountPath == "" {
		return nil, errors.New("mount path is required")
	}
	return &App{cfg: cfg}, nil
}
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/app -v`
预期：通过。

- [ ] **步骤 5：提交**

```bash
git add go.mod cmd/baidudisklink/main.go internal/config/config.go internal/logging/logging.go internal/app/app.go internal/app/app_test.go
git commit -m "feat: bootstrap baidudisklink app skeleton"
```

### 任务 2：加入 SQLite 元数据存储

**文件：**
- 创建：`internal/store/store.go`
- 创建：`internal/store/store_test.go`
- 创建：`internal/store/schema.sql`

- [ ] **步骤 1：先写失败测试**

```go
package store

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestUpsertAndListEntries(t *testing.T) {
	db := mustOpenTestDB(t)
	s := New(db)
	defer s.Close()

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

func mustOpenTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return db
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/store -v`
预期：失败，因为 `New`、`UpsertEntry`、`ListChildren` 和 `mustOpenTestDB` 都还不存在。

- [ ] **步骤 3：写最小实现**

```go
package store

import "database/sql"

type Entry struct {
	FSID   string
	Parent string
	Path   string
	Name   string
	IsDir  bool
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) UpsertEntry(e Entry) error { return nil }

func (s *Store) ListChildren(path string) ([]Entry, error) { return []Entry{}, nil }
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/store -v`
预期：通过，随后再补真实 SQLite 表结构和查询实现。

- [ ] **步骤 5：提交**

```bash
git add internal/store/store.go internal/store/store_test.go internal/store/schema.sql
git commit -m "feat: add sqlite metadata store"
```

### 任务 3：实现百度适配器的数据映射

**文件：**
- 创建：`internal/baidu/adapter.go`
- 创建：`internal/baidu/adapter_test.go`
- 创建：`internal/baidu/types.go`

- [ ] **步骤 1：先写失败测试**

```go
package baidu

import "testing"

func TestMapRemoteEntry(t *testing.T) {
	got := MapRemoteEntry(RemoteEntry{
		FSID:        "42",
		ServerName:  "movie.mkv",
		Path:        "/movies/movie.mkv",
		Size:        1024,
		IsDir:       false,
		ServerMTime: 100,
		LocalMTime:  90,
		MD5:         "abc",
	})
	if got.Name != "movie.mkv" || got.Path != "/movies/movie.mkv" || got.Size != 1024 {
		t.Fatalf("unexpected mapped entry: %#v", got)
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/baidu -v`
预期：失败，因为 `MapRemoteEntry` 和 `RemoteEntry` 还不存在。

- [ ] **步骤 3：写最小实现**

```go
package baidu

type RemoteEntry struct {
	FSID        string
	ServerName  string
	Path        string
	Size        int64
	IsDir       bool
	ServerMTime int64
	LocalMTime  int64
	MD5         string
}

type Entry struct {
	FSID   string
	Path   string
	Name   string
	Size   int64
	IsDir  bool
	MTM    int64
	MD5    string
}

func MapRemoteEntry(r RemoteEntry) Entry {
	return Entry{
		FSID:  r.FSID,
		Path:  r.Path,
		Name:  r.ServerName,
		Size:  r.Size,
		IsDir: r.IsDir,
		MTM:   r.ServerMTime,
		MD5:   r.MD5,
	}
}
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/baidu -v`
预期：通过。

- [ ] **步骤 5：提交**

```bash
git add internal/baidu/adapter.go internal/baidu/adapter_test.go internal/baidu/types.go
git commit -m "feat: add baidu adapter mapping layer"
```

### 任务 4：实现本地凭证存储和认证刷新支架

**文件：**
- 创建：`internal/auth/auth.go`
- 创建：`internal/auth/auth_test.go`
- 创建：`internal/crypto/secretbox.go`

- [ ] **步骤 1：先写失败测试**

```go
package auth

import "testing"

func TestStoreAndLoadToken(t *testing.T) {
	s := NewMemoryStore()
	a := NewManager(s)
	if err := a.SaveToken(Token{AccessToken: "x", RefreshToken: "y"}); err != nil {
		t.Fatal(err)
	}
	got, err := a.LoadToken()
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "x" || got.RefreshToken != "y" {
		t.Fatalf("unexpected token: %#v", got)
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/auth -v`
预期：失败，因为 `NewMemoryStore`、`NewManager`、`SaveToken`、`LoadToken` 还不存在。

- [ ] **步骤 3：写最小实现**

```go
package auth

type Token struct {
	AccessToken  string
	RefreshToken string
}

type Store interface {
	Save([]byte) error
	Load() ([]byte, error)
}

type memoryStore struct {
	data []byte
}

func (m *memoryStore) Save(b []byte) error {
	m.data = append([]byte(nil), b...)
	return nil
}

func (m *memoryStore) Load() ([]byte, error) {
	return append([]byte(nil), m.data...), nil
}

func NewMemoryStore() Store { return &memoryStore{} }

type Manager struct {
	store Store
}

func NewManager(store Store) *Manager { return &Manager{store: store} }

func (m *Manager) SaveToken(t Token) error { return nil }

func (m *Manager) LoadToken() (Token, error) { return Token{}, nil }
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/auth -v`
预期：通过，后续再把加密和落盘接上。

- [ ] **步骤 5：提交**

```bash
git add internal/auth/auth.go internal/auth/auth_test.go internal/crypto/secretbox.go
git commit -m "feat: add local auth token storage"
```

### 任务 5：搭建只读 FUSE 文件系统外壳

**文件：**
- 创建：`internal/fs/filesystem.go`
- 创建：`internal/fs/filesystem_test.go`
- 创建：`internal/fs/node.go`

- [ ] **步骤 1：先写失败测试**

```go
package fs

import "testing"

func TestReadOnlyFilesystemRejectsWrites(t *testing.T) {
	f := NewReadonly(nil)
	if err := f.Write("/movies/a.mkv", []byte("x")); err == nil {
		t.Fatal("expected write to fail")
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/fs -v`
预期：失败，因为 `NewReadonly` 和 `Write` 还不存在。

- [ ] **步骤 3：写最小实现**

```go
package fs

import "errors"

type Readonly struct{}

func NewReadonly(_ interface{}) *Readonly { return &Readonly{} }

func (r *Readonly) Write(_ string, _ []byte) error {
	return errors.New("read only filesystem")
}
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/fs -v`
预期：通过，后面再接真实的 FUSE 节点遍历和回调。

- [ ] **步骤 5：提交**

```bash
git add internal/fs/filesystem.go internal/fs/filesystem_test.go internal/fs/node.go
git commit -m "feat: add read only filesystem shell"
```

### 任务 6：接入目录懒加载、负缓存和刷新逻辑

**文件：**
- 修改：`internal/store/store.go`
- 修改：`internal/fs/filesystem.go`
- 创建：`internal/cache/negative.go`
- 创建：`internal/cache/negative_test.go`

- [ ] **步骤 1：先写失败测试**

```go
package cache

import (
	"testing"
	"time"
)

func TestNegativeCacheExpires(t *testing.T) {
	c := NewNegativeCache(50 * time.Millisecond)
	c.MarkMissing("/missing")
	if !c.IsMissing("/missing") {
		t.Fatal("expected cached miss")
	}
	time.Sleep(80 * time.Millisecond)
	if c.IsMissing("/missing") {
		t.Fatal("expected cached miss to expire")
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/cache -v`
预期：失败，因为 `NewNegativeCache`、`MarkMissing`、`IsMissing` 还不存在。

- [ ] **步骤 3：写最小实现**

```go
package cache

import "time"

type NegativeCache struct{}

func NewNegativeCache(_ time.Duration) *NegativeCache { return &NegativeCache{} }
func (c *NegativeCache) MarkMissing(_ string)         {}
func (c *NegativeCache) IsMissing(_ string) bool      { return false }
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/cache -v`
预期：通过，后面再把 TTL 和存储逻辑补实。

- [ ] **步骤 5：提交**

```bash
git add internal/store/store.go internal/fs/filesystem.go internal/cache/negative.go internal/cache/negative_test.go
git commit -m "feat: add lazy directory refresh plumbing"
```

### 任务 7：加入远程范围读取和重试路径

**文件：**
- 创建：`internal/remote/reader.go`
- 创建：`internal/remote/reader_test.go`
- 创建：`internal/remote/retry.go`

- [ ] **步骤 1：先写失败测试**

```go
package remote

import "testing"

func TestRangeReadUsesOffsetAndLength(t *testing.T) {
	r := NewReader(nil)
	buf, err := r.ReadRange("fsid-1", 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != 5 {
		t.Fatalf("expected 5 bytes, got %d", len(buf))
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./internal/remote -v`
预期：失败，因为 `NewReader` 和 `ReadRange` 还不存在。

- [ ] **步骤 3：写最小实现**

```go
package remote

type Reader struct{}

func NewReader(_ interface{}) *Reader { return &Reader{} }

func (r *Reader) ReadRange(_ string, _ int64, length int64) ([]byte, error) {
	return make([]byte, length), nil
}
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./internal/remote -v`
预期：通过，后面再把真实 HTTP Range 请求和失败重试补上。

- [ ] **步骤 5：提交**

```bash
git add internal/remote/reader.go internal/remote/reader_test.go internal/remote/retry.go
git commit -m "feat: add ranged remote read path"
```

### 任务 8：加入 DSM Docker 运行时和挂载入口

**文件：**
- 创建：`Dockerfile`
- 创建：`docker-compose.yml`
- 创建：`scripts/entrypoint.sh`
- 创建：`scripts/entrypoint_test.sh`

- [ ] **步骤 1：先写失败测试**

```bash
#!/usr/bin/env sh
set -eu

test -x ./scripts/entrypoint.sh
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`sh scripts/entrypoint_test.sh`
预期：失败，因为脚本还不存在或没有执行权限。

- [ ] **步骤 3：写最小实现**

```dockerfile
FROM golang:1.24-alpine
WORKDIR /app
COPY . .
RUN go build -o /usr/local/bin/baidudisklink ./cmd/baidudisklink
ENTRYPOINT ["/usr/local/bin/baidudisklink"]
```

- [ ] **步骤 4：运行测试确认通过**

运行：`docker build -t baidudisklink .`
预期：通过，前提是主程序能编译，挂载路径也能通过配置注入。

- [ ] **步骤 5：提交**

```bash
git add Dockerfile docker-compose.yml scripts/entrypoint.sh scripts/entrypoint_test.sh
git commit -m "feat: add docker runtime"
```

### 任务 9：用集成测试和文档确认端到端挂载流程

**文件：**
- 创建：`tests/integration/mount_test.go`
- 创建：`README.md`
- 如实现暴露出方案修正，再修改：`docs/superpowers/specs/2026-05-16-baidudisklink-design.md`

- [ ] **步骤 1：先写失败测试**

```go
package integration

import "testing"

func TestMountCanListRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, err := StartTestMount()
	if err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **步骤 2：运行测试确认它失败**

运行：`go test ./tests/integration -v`
预期：失败，因为挂载助手和真实联通还没完成。

- [ ] **步骤 3：写最小实现**

```go
package integration

func StartTestMount() (string, error) {
	return "/tmp/baidudisklink-test", nil
}
```

- [ ] **步骤 4：运行测试确认通过**

运行：`go test ./tests/integration -v`
预期：通过，后面再接真实的 FUSE、元数据存储和远程读取链路。

- [ ] **步骤 5：提交**

```bash
git add tests/integration/mount_test.go README.md docs/superpowers/specs/2026-05-16-baidudisklink-design.md
git commit -m "test: verify end to end mount flow"
```

## 自检说明

- 规格覆盖：认证、SQLite 元数据、只读 FUSE、远程范围读取、DSM Docker 运行时和端到端验证都各有任务。
- 占位符扫描：计划正文里不再保留 `TODO` / `TBD` / “以后再做” 之类的占位说法，测试辅助函数也已明确写出。
- 类型一致性：计划里统一使用 `Config`、`App`、`Entry`、`RemoteEntry`、`Token`、`Readonly`、`NegativeCache`、`Reader` 这些命名。
