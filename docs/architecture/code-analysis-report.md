# BaiduDiskLink 代码分析报告

> 生成日期：2026-07-27
> 分析范围：全仓库 Go 源码（约 11,350 行，含测试），不含历史设计文档（docs/specs/architecture/）

---

## 一、项目定位

BaiduDiskLink 是一个用 Go 编写的 **百度网盘 FUSE 挂载工具**，核心目标是面向群晖 DSM + Emby 媒体库场景，实现 **高码率视频的流畅流式播放**。

与 WebDAV 方案不同，它直接将百度网盘目录以本地文件系统形式暴露给宿主机（通过 FUSE + Docker `rshared` 挂载传播），让 Emby、Infuse 等播放器可以像读本地文件一样直接读网盘内容。

| 维度 | 说明 |
|---|---|
| 语言 / 运行时 | Go 1.24+，纯静态二进制（CGO-free，使用 `modernc.org/sqlite`） |
| 核心依赖 | `go-fuse/v2`、`modernc.org/sqlite` |
| 部署形态 | Docker 容器，`privileged + SYS_ADMIN + /dev/fuse` |
| 默认网盘根目录 | `/Videos`（可通过 `BAIDUDISKLINK_REMOTE_ROOT_PATH` 配置） |
| 挂载读写性 | 默认只读；开启 `BAIDUDISKLINK_ENABLE_DELETE=1` 后允许删除（无写入/上传/重命名） |
| 主要设计目标 | 100 Mbps 码率视频流畅播放、seek 快速响应、token 自动续期、元数据缓存 |

---

## 二、目录结构总览

```
BaiduDiskLink/
├── cmd/baidudisklink/main.go        # 入口，6 个子命令分发
├── internal/
│   ├── app/            # 应用编排（组件组装、启动流程、bench 系列、playback）
│   ├── auth/           # OAuth 流程、token 持久化
│   ├── baidu/          # 百度网盘 REST API 客户端
│   ├── cache/          # negative lookup 内存缓存
│   ├── config/         # 环境变量解析
│   ├── fs/             # FUSE 文件系统实现（go-fuse/v2）
│   ├── logging/        # 日志（默认 logger 的薄封装）
│   ├── remote/         # 远程 Range 读取层（并发分块、inflight 合并、LRU 窗口缓存）
│   ├── store/          # SQLite 元数据持久化层
│   └── stream/         # 高码率流式播放引擎（chunk 调度、hedge、内存+磁盘 LRU）
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── scripts/            # dsm-verify / smoke / verify 脚本
└── docs/specs/...      # 历史设计文档（本次未纳入代码分析）
```

各包代码行数（仅 `.go` 生产代码，不含测试）：

| 包 | 生产代码行数 |
|---|---|
| `internal/stream` | 1,566（manager.go 1187 + store.go 379） |
| `internal/fs` | 896 |
| `internal/remote` | 677 |
| `internal/baidu` | 731（client.go 556 + adapter.go + types.go） |
| `internal/app` | 1,554（app.go 450 + bench.go 478 + download_bench.go 418 + playback.go 205） |
| `internal/store` | 351 |
| `internal/auth` | 308 |
| `internal/config` | 115 |
| `internal/cache` | 37 |

**核心复杂度集中在 `internal/stream/manager.go`（1187 行）**，它实现了类视频播放器的自适应预读缓冲引擎。

---

## 三、入口与命令

`cmd/baidudisklink/main.go` 根据 `os.Args[1]` 分发 6 种运行模式：

| 子命令 | 用途 | 关键参数 |
|---|---|---|
| _（无参数）_ | 正常挂载模式：OAuth → 挂载 FUSE → 阻塞直到卸载 | 通过环境变量配置 |
| `bench-download` | 直链下载基准，不经 API/FUSE，直接对 dlink 做 N 连接 Range 下载测速 | `-url` `-cookie` `-bytes`(256MB) `-connections`(8) `-chunk-size`(1MB) `-http-version`(auto/1.1/2) `-retries` |
| `bench` | 远程文件基准，走完整 API 流程获取 dlink 后 Range 读 | `-path` `-bytes`(200MB) `-concurrency` `-chunk-size` |
| `bench-fuse` | 本地挂载点基准，直接 `os.Open` + `ReadFull` 读 FUSE 路径（不经项目代码） | `-path`(mnt/test.zip) `-bytes`(200MB) |
| `bench-stream` | 流式播放基准，按指定码率模拟顺序播放并定期 seek，输出卡顿/缓冲/延迟统计 | `-path` `-bitrate`(Mbps) `-duration` `-seek-interval` `-disk-cache` |
| `playback` | HTTP Range 代理服务器，方便浏览器/播放器绕开 FUSE 直接流式播放 | `-path` `-listen`(127.0.0.1:8787) |

---

## 四、分层架构

```
┌──────────────────────────────────────────────────────────────┐
│  用户进程（Emby / VLC / cp / ls …）                           │
└──────────────────────────────────────────────────────────────┘
                           │ FUSE Read / Readdir / Lookup（go-fuse/v2）
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  internal/fs  —— FUSE 文件系统节点                            │
│  · Getattr / Lookup / Readdir / Read / Unlink / Rmdir        │
│  · Negative cache (30s TTL)                                  │
│  · 目录刷新（按需 TTL + 每分钟定时）                           │
│  · 慢读 5 秒窗口聚合日志                                       │
└──────────────────────────────────────────────────────────────┘
          │ 有 stream handle         │ 无 stream handle
          ▼                          ▼
┌──────────────────────┐    ┌─────────────────────────────────┐
│  internal/stream     │    │  internal/remote                │
│  · 访问模式探测       │    │  · dlink 缓存（独立）            │
│   (probe/stream)     │    │  · 窗口 LRU 缓存 (64MB)         │
│  · chunk 调度 (1MB)  │    │  · inflight 请求合并             │
│  · 4 级优先级堆       │    │  · concurrency 分块并发         │
│  · hedge 竞速         │    │  · 错误分类 + auth 自动刷新     │
│  · 内存+磁盘 LRU     │    └─────────────────────────────────┘
│  · epoch/seek 失效   │                │
└──────────────────────┘                │
          │ ReadStreamRange             │ ReadRange / ReadExactRange
          └──────────────┬──────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  internal/baidu  —— 百度网盘 API 客户端                       │
│  · List / Stat / Delete / GetDownloadLink / ReadRange        │
│  · 独立 metadataClient / downloadClient（连接池/超时分离）    │
│  · dlink 内存缓存 10min；HEAD 解析 Location 拿 CDN URL        │
│  · RefreshAuth()  自动刷 token，回调 onTokenUpdate 持久化     │
└──────────────────────────────────────────────────────────────┘
                         │ HTTPS（HTTP/2）
                         ▼
┌──────────────────────────────────────────────────────────────┐
│  百度网盘开放 API + CDN（dlink 重定向后的最终下载节点）        │
└──────────────────────────────────────────────────────────────┘

横切层：
  internal/store  —— SQLite 元数据缓存（entries 表，WAL 模式）
  internal/cache  —— NegativeCache（内存 TTL map）
  internal/auth   —— OAuth HTTP 回调服务器 + FileStore (token.json)
  internal/config —— 所有环境变量（BAIDUDISKLINK_*）
```

### 为什么要分 stream 与 remote 两层？

- **`stream.Manager`** 面向"视频播放"场景：小块（1MB）、持续前向缓冲、hedge 抗尾延迟、内存/磁盘二级缓存、seek 时 epoch 失效。它只要求底层 reader 提供一个"给定 offset/length 返回字节"的能力（`ReadStreamRange`），不复用 remote 层的 inflight 合并和 LRU 窗口缓存，避免上层调度与下层缓存相互干扰。
- **`remote.Reader`** 面向"通用 Range 读取"场景：大块（8MB）、并发分块、inflight 合并、一次性读缓存。FUSE 在没经过 stream handle（例如非视频的小文件随机读、bench 命令）时直接走它。

---

## 五、各模块详细分析

### 5.1 `internal/baidu`：百度网盘 API 客户端

**Client 接口**（`types.go`）：

```go
type Client interface {
    List(path string) ([]RemoteEntry, error)
    Stat(path string) (RemoteEntry, error)
    Delete(paths []string) error
    GetDownloadLink(fsid string) (DownloadLink, error)
    ReadRange(ctx context.Context, fsid string, offset int64, dst []byte) (int, error)
    RefreshAuth() error
}
```

**API 端点**：

| 操作 | 端点 | 方法 | 备注 |
|---|---|---|---|
| 列目录 | `/rest/2.0/xpan/file?method=list` | GET | limit=200，用 `start`/`next_mark` 翻页 |
| 文件元数据/dlink | `/rest/2.0/xpan/multimedia?method=filemetas&dlink=1` | GET | 同时用于 Stat 和 GetDownloadLink |
| 删除 | `/rest/2.0/xpan/file?method=filemanager&opera=delete` | POST | body 为 `filelist=[paths]` + `async=0` |
| 刷新 token | `https://openapi.baidu.com/oauth/2.0/token` | GET | `grant_type=refresh_token` |

**HTTP 客户端分离**（`client.go:79-110`）：

- `metadataClient`：30s 超时，`MaxConnsPerHost=2`，禁用压缩；用于 list/stat/dlink。
- `downloadClient`：75s 超时，`ResponseHeaderTimeout=30s`，`MaxConnsPerHost=max(concurrency+8, 12)`，禁用压缩；用于 Range 下载。
- 两者都 `ForceAttemptHTTP2: true`。

**dlink 获取流程**（`GetDownloadLink`，`client.go:218-252`）：

1. 先查 `c.links[fsid]` 内存缓存（10 分钟有效期）。
2. 调用 `filemetas` API 拿 `dlink` 字段。
3. 对 dlink 发 **HEAD** 请求（带 `access_token` query 参数），`CheckRedirect` 返回 `ErrUseLastResponse` 阻止自动跳转，从 `Location` 响应头中拿到最终 CDN URL。
4. 缓存 10 分钟，返回给上层。

`InvalidateDownloadLink(fsid)` 供 stream 层在连续 hedge 触发时主动失效。

**Range 下载**（`ReadRange`，`client.go:330-413`）：

1. 包一层 60s 超时 context。
2. 构造 `Range: bytes={offset}-{end}`，UA 设为 `pan.baidu.com`。
3. 要求返回 `206 Partial Content`，否则读取 512B body 报错。
4. 严格解析 `Content-Range`（`parseContentRange`），验证 start/end/total 三者一致，且返回长度与请求长度匹配（多读 1 字节检测超长）。
5. 超时/取消时调用 `c.downloadClient.CloseIdleConnections()`，防止坏连接留在池里。

**Token 刷新**（`RefreshAuth`，`client.go:445-493`）：

1. 加锁后用 refresh_token 请求 token endpoint。
2. 更新 `accessToken`，若响应带了新 refresh_token 也一并更新。
3. **清空 dlink 缓存**（因为 access_token 变了，旧 dlink 鉴权会失败）。
4. 触发 `onTokenUpdate` 回调——由 `app.bindRemoteClient` 注入，将新 token 写回 `auth.FileStore`。

所有对外请求在取 token 时都通过 `c.token()`（加锁读），保证并发安全。

### 5.2 `internal/fs`：FUSE 文件系统

基于 `github.com/hanwen/go-fuse/v2`。核心类型：

- `Filesystem`：根 inode，嵌入 `goFs.Inode`；持有 `*store.Store` / `*remote.Reader` / `*stream.Manager` / `*cache.NegativeCache`。
- `entryNode`：每个文件/目录的 inode，嵌入 `goFs.Inode`，持有 `store.Entry`。
- `entryFileHandle`：打开的文件句柄，包装 `*stream.Handle`。

**编译期接口断言**覆盖：`NodeGetattrer`、`NodeLookuper`、`NodeReaddirer`、`NodeReader`、`NodeUnlinker`、`NodeRmdirer`。

**挂载选项**（`Mount()`）：

| 选项 | 值 |
|---|---|
| `AllowOther` | true |
| `MaxWrite` / `MaxReadAhead` | 1 MB |
| `AttrTimeout` / `EntryTimeout` | 1 s |
| `GID` | 若配置了 `FUSE_GROUP_NAME` 则设置，便于 Emby 组权限访问 |

**Lookup 流程**：

1. 先查 negative cache（30s TTL），命中直接返回 `ENOENT`。
2. 从 store 列父目录 children。
3. `shouldRefreshDir()` 判断：没记录 / `expires_at==0` / 已过期 → 调 `refreshDir()` 远程刷新。
4. 在 children 中按 name 匹配，找到则创建 persistent inode 返回。

**Read 核心路径**（`filesystem.go:661-708`）：

- 若打开时有 stream handle：`streamHandle.ReadAt()`，策略标记 `"stream-manager"`。
- 否则走 `remote.ReadRange()`，策略标记 `"remote-range"`。
- 无 handle 的 fallback：先查 `remote.ReadCachedWindow()`（全局读缓存），再走 Range 下载。
- 每次读记录耗时，≥300ms 的写入慢读诊断窗口；`context.Canceled` 记为 canceled，其他错误返回 `EIO`。

**删除**（`deleteChild`，`filesystem.go:542-583`）：

- 必须 `enableDelete` 开启，否则 `EROFS`。
- 先 `remote.Delete()` 调百度 API，成功后 `store.DeletePath()` 删除 SQLite 记录及子树。
- 禁止删根路径。

**目录刷新**：

- `refreshDir(dirPath, fsid)`：调 `remote.List` 拿条目 → 映射成 `store.Entry`（文件 TTL 1 分钟，目录 `expires_at=0` 即立即可刷新）→ `store.ReplaceChildren` 事务性替换。
- `RefreshRootOnly()` / `RefreshAll()`：前者只刷根，后者递归刷所有已知子目录。
- `refreshMu` 是容量 1 的 channel（token bucket），同一时刻只允许一个刷新 goroutine。

**慢读聚合日志**：每个 fsid 维护一个 5 秒窗口的 `readDiagnosticWindow`，聚合 slow_reads / canceled_reads / requested / returned / max_elapsed / max_offset，窗口结束时输出一条 `fuse read summary` 日志。

**Trace**：`SetTraceReads(true)` 时打印所有"返回字节数 ≠ 请求字节数"或"stream-event"策略的读（调试用，默认关）。

### 5.3 `internal/stream`：高码率流式播放引擎

这是整个项目最复杂的组件。

#### 核心数据模型

| 概念 | 说明 |
|---|---|
| `File` | `{FSID, Size, MTM}` 唯一标识一个远程文件 |
| `chunkKey` | `{version, index}`，其中 `version = "{fsid}-{size}-{mtm}-chunk-{chunkSize}"`；文件修改后 version 变化自动失效旧缓存 |
| chunk 大小 | 默认 1 MiB（`ChunkSize`） |
| `task` | 单个 chunk 的下载任务，带优先级、epoch、hedge 状态 |
| `session` | 每个文件的播放会话，跟踪 cursor、active handle、epoch、调度窗口 |
| `Handle` | 打开的文件句柄，跟踪访问模式（probe vs stream） |
| 优先级 | `priorityForeground(0) > priorityNear(1) > priorityAhead(2) > priorityBack(3)` |

#### 访问模式检测（`observeHandleLocked`）

Handle 初始为 **modeProbe**（播放器先读头尾探测元数据），满足以下条件之一切换到 **modeStream**（开始预读）：

- 连续 2 次前向读（`forwardReads >= 2`）且间距 ≤ `ForwardGap`（8 MiB）。
- 探测读识别：≤128 KiB 且在文件末尾 1 MiB 内（尾探测）；大文件（≥256 MiB）前 8 MiB 内（头探测），都不算 stream。

Seek 检测：stream 模式下回退或跳跃超过 `SeekThreshold`（16 MiB，与 TargetBuffer 取大值）→ 切回 probe，触发 `readEventSeek`。

#### Epoch 机制

每个 session 维护 `epoch` 计数器：

- Seek 时 `epoch++`，取消该 session 所有旧 epoch 的后台任务（`cancelSessionTasksLocked`）。
- 不同 handle 间互不干扰：若已有 activeHandle，新 handle seek 时如果不是 active handle 自身，视为 independent read，不取消。
- 前台任务（`priorityForeground`）不被取消。

#### 缓冲调度窗口（`scheduleBuffer`）

当 cursor 推进时，按距 cursor 的距离划区：

| 区域 | 范围 | 优先级 |
|---|---|---|
| Near | cursor ～ cursor+64 MiB | `priorityNear` |
| Ahead | cursor+64 MiB ～ cursor+TargetBuffer (256 MiB) | `priorityAhead` |
| Back | cursor-BackBuffer (32 MiB) ～ cursor | `priorityBack`（仅 reset 时调度） |

cursor 前进时 floor 随之移动，`removeMemoryRange` 把 floor 之前的 chunk 从内存缓存中淘汰。seek reset 时调用 `pruneMemoryWindow` 清除窗口外所有内存 chunk。

#### Worker 池

- 固定 `cfg.Workers` 个 goroutine（默认 8）。
- 任务队列是 **最小堆** `taskHeap`：按 priority 升序，同 priority 按 seq 升序保证 FIFO。
- 通过 `sync.Cond` 等待任务（`nextTask()`）。
- **Session worker 限制**：多 session 并存时，每个 session 的后台任务（非 foreground）并发上限为 `SessionWorkers`（默认 `Workers-2=6`），防止多文件切换互相抢占。前台任务不受限。

#### Hedge / 竞速读（抗尾延迟）

前台同步等待 chunk 时，tail latency 容忍机制：

1. 先等 `hedgeDelay()`：取最近 32 个 chunk 下载延迟的 P95 × 1.5，clamp 到 `[HedgeMinDelay=300ms, HedgeMaxDelay=800ms]`。
2. 若原任务在 delay 内未完成，启动 hedge goroutine **并发请求同一块**。
3. 连续 3 次 hedge（`slowStreak >= 3`）判定 dlink 可能劣化，调 `InvalidateDownloadLink(fsid)` 让下次拿新 URL。
4. 先到的写入缓存，后到的取消。
5. `hedges atomic.Int64` 全局计数 + session 级别计数，bench-stream 可以看到。

#### 重试与统计

- `download()` 对失败 chunk 重试 1 次（共 2 次尝试），`retries atomic.Int64` 计数。
- `summaryWorker`：每 5 秒输出一条 `stream summary` 日志（buffer_ahead、low_watermark、inflight、缓存占用、downloaded/retries/hedges）。
- 32 格环形缓冲区记录最近 chunk 延迟，用于 hedge delay 计算。
- `BufferAhead(file, cursor)`：从 cursor 起统计连续已缓存 chunk 的总字节数，供 FUSE/bench 判断缓冲水位。

#### chunk 两级缓存（`store.go`）

| 层级 | 实现 | 容量（默认） | 备注 |
|---|---|---|---|
| 内存 LRU | `map[chunkKey]*list.Element` + `*list.List` | 320 MiB | 命中 `MoveToBack`，超限从 Front 淘汰 |
| 磁盘 LRU | 每 chunk 一个文件 `{version}-{index}.chunk` | 2 GiB | 写走临时 `.part` + rename 原子化；启动时清理残留 `.part`，扫描 mtime 重建 LRU |

- 磁盘写入异步：`diskWrites` channel（缓冲 `Workers*8`），单独 `diskWriter()` goroutine 消费，不阻塞读取路径。
- 磁盘读命中时把数据 **回填内存** `s.putMemory(key, data)`。
- 单独的 `diskMu sync.Mutex` 保护磁盘操作，不阻塞内存访问。
- key 版本化：`chunkVersion(file, chunkSize)` 包含 size/mtm/chunkSize，参数或文件变更后旧缓存自动失效。

### 5.4 `internal/remote`：远程读取层

`remote.Reader` 是比 stream 更底层的通用读取器。

```go
type Reader struct {
    client      baidu.Client           // 可动态替换（SetClient）
    links       map[string]cachedLink  // dlink 缓存（与 baidu 层独立双重缓存）
    cached      map[cacheKey]cachedRead // 窗口读缓存
    cacheByFSID map[string]map[cacheKey]struct{} // 按 fsid 分组索引
    cacheOrder  []cacheKey             // LRU 顺序
    cacheBytes  int64
    cacheLimit  int64                  // 默认 64 MiB
    inflight    map[cacheKey]*inflightRead
    concurrency int                    // 默认 1
    chunkSize   int64                  // 默认 8 MiB
}
```

#### 两条读取路径

- `ReadRange` / `ReadExactRange`：完整的"缓存 → inflight 合并 → 并发/顺序 → 回填缓存"流程，FUSE 无 stream handle 时与 bench 命令使用。
- `ReadStreamRange`：直接透传 `client.ReadRange`（带 auth 重试），不走 inflight 也不写缓存——专门给 stream.Manager 用，避免两层缓存互相干扰。

#### Range 拆分与并发（`readConcurrent` / `readSequential`）

- `readConcurrent()`：按 `chunkSize`（8 MiB）切分，启 `concurrency` 个 worker goroutine，通过 jobs channel 分发到 chunk index；首个错误通过 `context.Cancel` 终止其他 worker；共享一个 `[]byte` buffer，按顺序拼接结果。
- `readSequential()`：单连接，但会将 offset 对齐到 chunkSize 边界，最小读取 `prefetchBytes`（8 MiB）做预取。

#### Inflight 合并

相同 `(fsid, windowOffset)` 的并发请求会合并：

- 第一个请求 `beginInflight()` 创建 owner + done channel。
- 后续请求 `waitInflight()` 等 channel。
- 完成时 `finishInflight()` 写入 data/err，close channel，清 map。

#### 错误分类与重试

`ClassifyDownloadError(err)`：

| 类别 | 判断 | 处理 |
|---|---|---|
| `DownloadErrorAuth` | 401/403/unauthorized/forbidden | 调 `client.RefreshAuth()` 后重试 |
| `DownloadErrorRange` | 416/content range | 不重试 |
| `DownloadErrorTransport` | timeout/connection reset/context | 重试 |
| `DownloadErrorEOF` | UnexpectedEOF | 重试 |

`ReadExactRange()` 最多重试 1 次。Auth 错误会先刷新 token 再重试。

### 5.5 `internal/store`：SQLite 元数据持久化

**唯一的表** `entries`：

```
(id, fsid, parent_fsid, path UNIQUE, name, size, is_dir,
 mtime, md5, last_sync_at, expires_at, negative)
```

索引：`idx_entries_parent_fsid ON parent_fsid`。

关键方法：

| 方法 | 作用 |
|---|---|
| `UpsertEntry` / `UpsertEntries` | 单条/批量 upsert（后者在事务里 Prepare 一次） |
| `ReplaceChildren(parentID, entries)` | 事务中先删后插，用于目录刷新的原子替换 |
| `ListChildren(path)` | 先按 path 查 entry，再按 `parent_fsid` 列子项 |
| `GetByPath(path)` | 按唯一路径查询 |
| `DeletePath(path)` | 删除 `path = ? OR path LIKE ?/%` 的整棵子树 |
| `ExpirePath(path)` | 设 `expires_at=0` 强制下次访问刷新 |
| `EnsureRoot()` | 保证 `/` 存在（`fsid='0'`） |

DSN 由 `app.sqliteDSN()` 拼出，启用 `busy_timeout(5000)` 和 `journal_mode(WAL)`。

### 5.6 `internal/cache`：Negative Lookup 缓存

极简组件（37 行）：

- `NegativeCache`：`map[string]time.Time` + `sync.Mutex`。
- `MarkMissing(path)` 标记不存在；`IsMissing(path)` 检查 TTL（在 fs 层硬编码 30s），过期自动删除。
- 作用：避免对 ENOENT 路径反复打远程 API。

### 5.7 `internal/auth`：OAuth 流程

三个文件：

| 文件 | 内容 |
|---|---|
| `auth.go` | `Token`、`Manager`、`OAuthConfig`、`BuildAuthorizeURL` |
| `oauth.go` | `OAuthServer`（HTTP 回调）、`ExchangeToken`、`SaveOAuthTokenFromCallback` |
| `file_store.go` | `FileStore`（JSON 文件持久化，0600 权限） |

OAuth 授权码流程：

1. `BuildAuthorizeURL(cfg)` 拼出 `https://openapi.baidu.com/oauth/2.0/authorize?response_type=code&client_id=...&redirect_uri=...&scope=basic,netdisk&state=...`，打印到 stdout。
2. `OAuthServer.Start(addr)` 在 `0.0.0.0:8765` 起 HTTP 服务，注册 `/callback` handler。
3. 用户浏览器授权后百度 302 到 `redirect_uri?code=...`。
4. `handleCallback` 取 code 写入容量 1 的非阻塞 `result` channel，浏览器页显示 "authorization received"。
5. `Wait()` 阻塞等 code。
6. `ExchangeToken(cfg, code, client)` 用 code 换 access_token + refresh_token（GET，响应兼容 JSON/XML 两种格式——早期/某些情况下百度返回 XML）。
7. `Manager.SaveToken()` → `FileStore.Save()` 写 0600 权限的 `token.json`。

**Token 自动续期持久化**：`app.bindRemoteClient` 注入 `onTokenUpdate` 回调到 APIClient，当 `RefreshAuth()` 拿到新 token 时回调 `auth.SaveToken()`，把新 token 写回磁盘。

### 5.8 `internal/config`：环境变量配置

所有配置走环境变量（除 bench 命令用 flag 临时覆盖）。完整清单与默认值：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAIDUDISKLINK_MOUNT_PATH` | _(必填)_ | FUSE 挂载点 |
| `BAIDUDISKLINK_REMOTE_ROOT_PATH` | `/Videos` | 网盘根目录 |
| `BAIDUDISKLINK_TOKEN_PATH` | _(必填)_ | token.json 路径 |
| `BAIDUDISKLINK_META_DB_PATH` | _(必填)_ | SQLite 元数据 DB |
| `BAIDUDISKLINK_FUSE_GROUP_NAME` | _(空)_ | 允许访问 FUSE 的组（gid 或组名，逗号分隔） |
| `BAIDUDISKLINK_FUSE_TRACE_READS` | false | 追踪每次 FUSE 读（调试） |
| `BAIDUDISKLINK_ENABLE_DELETE` | false | 允许删除 |
| `BAIDUDISKLINK_DOWNLOAD_CONCURRENCY` | 1 | 直读并发连接 |
| `BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE` | 8 MiB | 直读分块 |
| `BAIDUDISKLINK_STREAM_CHUNK_SIZE` | 1 MiB | stream chunk |
| `BAIDUDISKLINK_STREAM_WORKERS` | 8 | worker 池大小 |
| `BAIDUDISKLINK_STREAM_LOW_WATERMARK` | 128 MiB | 缓冲低水位 |
| `BAIDUDISKLINK_STREAM_TARGET_BUFFER` | 256 MiB | 前向目标缓冲 |
| `BAIDUDISKLINK_STREAM_BACK_BUFFER` | 32 MiB | 后向保留 |
| `BAIDUDISKLINK_STREAM_MEMORY_CACHE` | 320 MiB | 内存 LRU 上限 |
| `BAIDUDISKLINK_STREAM_DISK_CACHE` | 2 GiB | 磁盘 LRU 上限（docker-compose 默认覆盖为 0 即关闭） |
| `BAIDUDISKLINK_STREAM_CACHE_PATH` | _(空)_ | 磁盘缓存目录 |
| `BAIDUDISKLINK_STREAM_HEDGE` | true | 启用 hedge 竞速 |
| `BAIDUDISKLINK_CLIENT_ID` | _(必填)_ | 百度 App Key |
| `BAIDUDISKLINK_CLIENT_SECRET` | _(必填)_ | 百度 Secret Key |
| `BAIDUDISKLINK_REDIRECT_URI` | _(必填)_ | OAuth 回调 |
| `BAIDUDISKLINK_OAUTH_LISTEN_ADDR` | _(从 redirect_uri 推断)_ | OAuth HTTP 监听地址 |
| `BAIDUDISKLINK_OAUTH_SCOPE` | `basic,netdisk` | OAuth scope |
| `BAIDUDISKLINK_OAUTH_STATE` | `baidudisklink` | OAuth state |
| `BAIDUDISKLINK_AUTHORIZE_BASE_URL` | 百度 authorize URL | |
| `BAIDUDISKLINK_TOKEN_BASE_URL` | 百度 token URL | |
| `BAIDUDISKLINK_API_BASE_URL` | `https://pan.baidu.com` | |

布尔值用自定义的 `parseBool`（接受 `1/true/yes/on`）。

### 5.9 `internal/app`：应用编排

**`app.New(cfg)` 组件初始化顺序**（`app.go:101-208`）：

1. 校验必填项（MountPath / TokenPath / MetaDBPath / ClientID / RedirectURI）。
2. `os.MkdirAll` 挂载点、DB 目录、Token 目录。
3. `sql.Open("sqlite", sqliteDSN())`（WAL + busy_timeout=5000）。
4. `store.Open(db)` 自动 migrate schema → `EnsureRoot()` → `ExpirePath("/")` + `ExpirePath(RemoteRootPath)`。
5. 解析 `FuseGroupName` → GIDs（支持名字和数字，逗号分隔）。
6. 创建 `auth.FileStore` + `auth.Manager`。
7. 创建 `remote.Reader`（初始用 `baidu.StaticClient` 桩），`SetDownloadOptions`。
8. 创建 `stream.Manager`（`SessionWorkers = Workers - 2`）。
9. 创建 `auth.OAuthServer`。
10. 定义 `clientFactory` 闭包：按 token 创建 `baidu.APIClient`，注入 `onTokenUpdate` 回调持久化 token。
11. 定义 `mountFunc` 闭包：`fs.Mount(mountPath, root, {AllowOther:true, GIDs})`。

**`app.Run()` 启动流程**：

```
尝试 bindRemoteClient（从 token.json 加载 token → clientFactory）
   ↓ 成功
remoteHealthCheck（列 RemoteRootPath 验证 token 可用性）
   ↓ 成功                          ↓ 失败（无 token 或 token 失效）
mountAndWait()                   打印 OAuth 授权 URL 到 stdout
                                 Start OAuth HTTP 服务器
                                 Wait() 等用户回调拿 code
                                 saveOAuthToken（Exchange + Save）
                                 bindRemoteClient
                                 mountAndWait()
```

**`mountAndWait()`**：

1. `fs.NewFilesystemWithStream(...)`。
2. `SetTraceReads` / `SetDeleteEnabled`。
3. `RefreshRootOnly()` 预加载根目录。
4. `mountFunc()` 挂载 FUSE。
5. `startRefreshLoop(stop)`：每分钟 `RefreshRootOnly()`。
6. `server.Wait()` 阻塞直到卸载；defer `server.Unmount()` 保证清理。

> 注意：项目**没有显式处理 SIGINT/SIGTERM**，依赖 go-fuse 的 `server.Wait()` 内部拦截信号并触发 Unmount。

**依赖注入设计**：`clientFactory`、`mountFunc`、`oauthFlow` 接口都设计为可替换，`StaticClient` 作为初始桩 client，token 加载后换成真实 APIClient——这也是所有测试能在不启真服务的情况下跑完的原因。

**bench-stream 关键统计指标**（`bench.go:179-347`）：

按 `-bitrate` Mbps 计算每 200ms 步长要读的字节数；读耗时超步长记为 stall；`-seek-interval` 每隔 N 秒向前跳 64 MiB。统计输出包括：Startup、BufferReady、BufferMin/Max、BufferLowCount/ZeroCount、Read P50/P95/P99/Max、Stalls/WarmupStalls/SteadyStalls、Stall 总时长/P95/Max、首/末次 stall 时间、Seek 次数/P95、远程下载字节/重试/hedge、吞吐（MiB/s、Mbps）。验收阈值：`steady_stalls=0`、`buffer_zero_count=0`、`seek_p95<3s`。

**playback HTTP Range 代理**（`playback.go`）：

- 起 HTTP 服务（默认 127.0.0.1:8787），解析 `Range: bytes=start-end` 与 `bytes=-suffix`。
- 返回 `206 Partial Content` + `Content-Range`，循环 4 MiB chunk 读 + Write。
- 用于绕开 FUSE 直接用浏览器/播放器播。

---

## 六、并发模型

| 组件 | 并发机制 |
|---|---|
| FUSE 层 | go-fuse 为每个请求起 goroutine，天然并发 |
| stream.Manager | 固定 8 worker + 1 diskWriter + 1 summaryWorker，通过 `sync.Cond` + heap 调度 |
| stream epoch | `cancelSessionTasksLocked` 在 seek 时取消旧任务；前台任务保留 |
| remote.Reader | `readConcurrent` 每个 Range 临时起 N worker，用 `context.Cancel` 错误传播；inflight map 去重 |
| baidu.APIClient | `sync.Mutex` 保护 token 与 dlink 缓存；HTTP 传输层连接池（`MaxConnsPerHost`）控 TCP 并发 |
| store | SQLite WAL + busy_timeout=5000ms，批量 upsert 走事务 + Prepare |
| Token 刷新 | APIClient 内加锁串行，onTokenUpdate 回调持久化 |

共享状态的保护手段覆盖 `sync.Mutex` / `sync.RWMutex` / `sync.Cond` / channel / `atomic.Int64`，分工清晰。

---

## 七、Docker 部署

### Dockerfile（多阶段构建）

- **build**：`golang:1.25.11-alpine`，`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build` 出纯静态二进制。
- **runtime**：`alpine:3.20`，装 `ca-certificates fuse3 bash tzdata`，二进制放到 `/usr/local/bin/baidudisklink`。
- VOLUME `/data`（token.json + meta.db + 可选 stream-cache）。
- 默认 ENV：`MOUNT_PATH=/mnt/baidu`、`TOKEN_PATH=/data/token.json`、`META_DB_PATH=/data/meta.db`、`OAUTH_LISTEN_ADDR=0.0.0.0:8765`。

### docker-compose.yml 关键点

| 配置 | 值 | 作用 |
|---|---|---|
| `privileged` + `cap_add: SYS_ADMIN` + `devices: /dev/fuse:/dev/fuse` | — | FUSE 挂载所需能力 |
| `./data:/data` | — | 持久化 token/meta.db/stream-cache |
| bind mount `${HOST_MOUNT_PATH}` → `/mnt/baidu` | **`propagation: rshared`** | **关键**：让容器内 FUSE 挂载事件双向传播到宿主机，使 DSM/Emby 能直接访问 |
| `8765:8765` | — | OAuth 回调端口 |
| `STREAM_DISK_CACHE` | compose 默认 `0`（关闭） | 覆盖 Go 默认 2 GiB |
| `TZ` | `Asia/Shanghai` | 日志时区 |

**`rshared` 是整个部署方案的核心技巧**：没有它，容器里 FUSE 挂载只在容器的 mount namespace 可见，宿主机看到的是空目录。

### 三层路径容易混淆

| 类型 | 示例 | 配置项 |
|---|---|---|
| 百度网盘目录 | `/Videos` | `BAIDUDISKLINK_REMOTE_ROOT_PATH` |
| 容器内挂载点 | `/mnt/baidu` | `BAIDUDISKLINK_MOUNT_PATH`（Dockerfile 固定） |
| DSM 宿主机目录 | `/volume2/baidu_videos` | `BAIDUDISKLINK_HOST_MOUNT_PATH`（Emby 媒体库路径） |

若远端入口是 `/Videos`，挂载根直接展示 `/Videos` 下的内容（不会多一层 `Videos`）。

---

## 八、测试策略

主要测试文件：

| 文件 | 行数 | 覆盖内容 |
|---|---|---|
| `internal/fs/filesystem_test.go` | 909 | Lookup/Readdir/Read/Delete/刷新/negative cache |
| `internal/app/app_test.go` | 755 | App 启动、OAuth 集成、Remote 绑定、bench、playback HTTP Range |
| `internal/stream/manager_test.go` | 713 | chunk 调度、hedge、seek、缓存窗口、buffer ahead、多 session worker 限幅 |
| `internal/remote/remote_test.go` | 571 | 并发读、inflight 合并、缓存、重试、Range 对齐 |
| `internal/baidu/client_test.go` | 456 | List/Stat/Delete/ReadRange/RefreshAuth，HTTP mock |
| `internal/store/store_test.go` | 296 | SQLite CRUD、ReplaceChildren、DeletePath 子树删除、ExpirePath |
| `internal/app/download_bench_test.go` | 265 | Range 验证、Content-Range 解析、重试、重定向 |
| `internal/stream/store_test.go` | 195 | 内存 LRU、磁盘读写、LRU 淘汰、窗口修剪 |
| `internal/auth/*_test.go` | ~280 | Token 序列化、AuthorizeURL、ExchangeToken、OAuthServer 回调 |

**测试替身手段**：

- `baidu.StaticClient`（`adapter.go`）：内置 `Entries map[string][]RemoteEntry` 的内存桩，大量被 fs/app/stream 测试使用。
- stream manager 测试用"函数类型实现 Reader 接口"的 fake reader，可以注入可控延迟和错误。
- HTTP 层测试用 `httptest.Server`。
- app 测试用 `fakeMountServer` 替代真实 FUSE 挂载（`waitFn`/`unmountFn` 可注入）。
- SQLite 层用临时文件或内存 DB。

**常用命令**（Makefile）：

| 命令 | 作用 |
|---|---|
| `make test` | `go test ./...` |
| `make verify` | `scripts/verify.sh`（Docker 配置、部署说明、smoke、脚本语法） |
| `make check` | test + verify |
| `make dsm-verify` | 在 DSM 上验收（容器状态、/dev/fuse、token/DB 落盘、FUSE 挂载表、列目录、读 1 字节） |
| `make build` | 交叉编译 linux/amd64 静态二进制 |

---

## 九、端到端读取路径总结

```
用户进程发起 read(fd, offset, size)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ go-fuse 内核模块 → VFS → Filesystem.Read()                  │
│   · 慢读计时、cancel 计数、trace                              │
│   · 有 stream handle? ──yes──▶ stream.Handle.ReadAt()       │
│   └──no──▶ remote.ReadRange()                               │
└─────────────────────────────────────────────────────────────┘

[stream 路径]
   observeHandleLocked 判定 probe/stream
   prepareRead 处理 epoch（seek 时 epoch++，取消旧任务）
   scheduleBuffer 按 Near/Ahead/Back 优先级把 chunk 压入 heap
   waitChunk:
      chunkStore.get() → 内存 LRU 命中？返回
                    └──▶ 磁盘 LRU 命中？回填内存，返回
                    └──▶ ensureTask: 若 chunk 未排队则压入 heap
                         waitForeground:
                            ├─ 等 hedgeDelay（P95×1.5，clamp 300-800ms）
                            ├─ 仍未完成 → 启 hedge goroutine 并发抢
                            └─ 先到者写缓存，取消另一
   worker 池: sync.Cond 唤醒 → 取最小堆任务 → Reader.ReadStreamRange
                                                              │
                                                              ▼
                                    baidu.APIClient.ReadRange (见下)

[remote 路径（无 stream handle）]
   查 inflight map: 是否同 (fsid, windowOffset) 已在飞
      是 → waitInflight() 等 owner
      否 → beginInflight()
   concurrency>1? readConcurrent: readSequential
     readConcurrent: N worker 按 8 MiB chunk 分块，context.Cancel 错误传播
     readSequential: 单连接，offset 对齐到 chunkSize，最小读 8 MiB 预取
   回填 64 MiB LRU 窗口缓存
                                                              │
                                                              ▼
                                    baidu.APIClient.ReadRange (见下)

[baidu.APIClient.ReadRange]
   GetDownloadLink(fsid):
      · 查 links map（10 min TTL）
      · 否则 filemetas API 拿 dlink
      · HEAD dlink（带 access_token）→ Location → CDN URL
      · 缓存
   构造 Range: bytes=offset-end，UA=pan.baidu.com
   60s 超时 context，downloadClient.Do
   必须 206；严格 parseContentRange 验证
   精确读满 want 字节（多读 1 字节检测超长）
   超时/取消 → CloseIdleConnections
        │
        ▼
   HTTPS → 百度 CDN（HTTP/2 可复用连接）
```

---

## 十、关键设计权衡与亮点

| 设计点 | 选择 | 权衡 |
|---|---|---|
| stream chunk 大小 1 MiB（vs remote 8 MiB） | 更小 | 降低 CDN 大 Range 慢尾延迟，代价是更多 HTTP 请求；hedge 机制对冲 |
| 内存缓存 320 MiB + 磁盘 2 GiB | 二级 LRU | 覆盖完整前向窗口（256 MiB）+ 后向缓冲（32 MiB）后仍有余量；磁盘写异步不阻塞读 |
| Hedge 竞速 | 基于最近 32 次 P95 自适应 | 连续 3 次 hedge 自动失效 dlink，应对 CDN 链接劣化 |
| 访问模式探测（probe/stream） | 头部 8 MiB / 尾部 1 MiB 小读不算 stream | 避免播放器读元数据时触发大量无谓预读 |
| dlink 双重缓存（baidu 层 + remote 层） | 两层独立 TTL | 简单但有冗余；InvalidateDownloadLink 同时清两层 |
| stream 走 `ReadStreamRange` 绕过 remote 层缓存 | 两层不互相干扰 | 放弃 remote 层 inflight 合并，但 stream 自己有任务堆 + epoch 管理，职责更清晰 |
| SQLite 元数据 + WAL | 持久化缓存 | 重启不用重列目录；busy_timeout=5s 抗并发写 |
| `rshared` 挂载传播 | 容器内挂载→宿主机可见 | 避免在宿主机装 Go/FUSE，部署只依赖 Docker |
| Token 自动刷 + 回调持久化 | 永不过期（只要 refresh_token 有效） | onTokenUpdate 把新 token 写回 0600 文件 |
| go-fuse `AttrTimeout/EntryTimeout=1s` | 1 秒内核属性缓存 | 减 API 压力同时保持一定实时性 |
| 没有 os.Signal 处理 | 依赖 go-fuse Wait() 拦截 SIGINT/SIGTERM | 简洁；SIGKILL 可能留残留挂载（README 给出 `umount -l` 清理方法） |

---

## 十一、潜在改进点（仅观察，非推荐）

以下是在代码阅读中观察到的点，是否需要改进取决于作者目标：

1. **stream 与 remote 双重 dlink 缓存**：`baidu.APIClient.links` 和 `remote.Reader.links` 各存一份，过期检查独立。实际命中的永远是 baidu 层（remote 的 `downloadLink()` 会先调 client.GetDownloadLink），remote 层的那份在大多数路径上读不到，但失效时要清两层。可以考虑去掉 remote 层的 dlink 缓存。
2. **HTTP 客户端分离的连接数**：`NewDownloadHTTPClient(workers)` 用 `StreamWorkers`（默认 8）来定 `MaxConnsPerHost`，但实际 stream worker 一次只发一个 Range、理论上峰值并发 ≈ workers + hedges；当用户用 bench-download 开到 8+ 连接时，新建的 APIClient 用的是 metadataClient 单 client（download_bench 自己构造 http.Client，不走 APIClient，所以没问题）。现状在 stream 路径下是合理的。
3. **docker-compose 默认关闭磁盘缓存**（`STREAM_DISK_CACHE: 0`），而 Go 代码默认 2 GiB。这是刻意为之（DSM 磁盘 I/O 可能抖动）但容易让读代码的人困惑。README 有说明。
4. **`RefreshAll()`** 递归刷新所有子目录——当前启动流程只用 `RefreshRootOnly()`，`RefreshAll` 在代码里存在但未被主流程调用（app_test 有用），属于保留接口。
5. **删除无回收站**：`enableDelete` 调百度 `opera=delete` 直接删除，没有二次确认或软删除——README 已经强调风险。
6. **没有显式信号处理**：依赖 go-fuse Wait 拦截；如果以 PID 1 在容器里跑，docker stop 的 SIGTERM 能被 go-fuse 处理；极端情况下（SIGKILL）需要 `umount -l` 清残留挂载点，README 已列。

---

## 十二、一句话总结

> BaiduDiskLink 是一个用 Go 写的百度网盘 FUSE 挂载工具，核心创新在 `internal/stream` 的"类播放器"自适应预读引擎：1 MiB 小块 + 8 worker 优先级堆调度 + 基于 P95 延迟的 hedge 竞速 + 320 MiB 内存/2 GiB 磁盘二级 LRU + epoch seek 失效，配合 dlink 自动解析与 refresh_token 持久化，再用 Docker `rshared` 挂载传播把整个网盘以本地目录形式暴露给 DSM/Emby，实现 100 Mbps 级别高码率视频的流式播放。
