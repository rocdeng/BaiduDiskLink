# BaiduDiskLink TODO / 改进建议

> 生成日期：2026-07-27
> 基于对全仓 Go 源码（约 11,350 行）的审阅。按优先级 P0（bug/正确性）→ P1（性能/体验）→ P2（清理/可维护性）排序。
> 每条都给出具体文件位置、问题、建议改法、验收标准。

---

## P0 — Bug / 正确性问题

### 1. Negative cache 只在根 Lookup 生效，子目录根本没查
- **位置**：`internal/fs/filesystem.go`
  - 根节点 `Filesystem.Lookup` (line 228)：`if f.negative != nil && f.negative.IsMissing(JoinPath(f.rootPath, name))`
  - 子目录 `entryNode.Lookup` (line 492-525)：**无任何 negative cache 检查**
- **问题**：用户在 `/Videos/SubDir/` 下反复 Lookup 一个不存在的文件时，每一次都会走 `store.ListChildren` + `shouldRefreshDir` 判断，虽然目录 TTL 内不会打远程 API，但 negative cache 的设计意图（快速 ENOENT，避免 store 扫描）只在根目录生效，越深的目录越享受不到。这对 Emby 扫库时探测字幕文件、伴生文件等场景会产生无意义的 store 查询压力。
- **建议**：在 `entryNode.Lookup` 开头加一行 `if n.Filesystem.negative != nil && n.Filesystem.negative.IsMissing(JoinPath(n.entry.Path, name)) { return nil, syscall.ENOENT }`；`deleteChild` 成功后也要 `MarkMissing`。同步在 Readdir 路径下不存在的文件场景考虑是否也需要标记。
- **验收**：新增测试：连续两次对 `/Videos/SubDir/nope` Lookup，验证第二次命中 negative cache 而不走 store.ListChildren；删除文件后立即 Lookup 命中 negative cache。

### 2. `refreshDir` / `refreshRoot` 把空目录自身错误地 markMissing
- **位置**：
  - `internal/fs/filesystem.go:264-266`（`refreshRoot`）：`if len(entries) == 0 { f.markMissing(f.rootPath); return nil }`
  - `internal/fs/filesystem.go:594-596`（`refreshDir`）：`if len(entries) == 0 { f.markMissing(dirPath); return nil }`
- **问题**：
  - `markMissing(path)` 语义是"这个路径不存在，下次 Lookup 直接 ENOENT"。但这里的 `dirPath`/`f.rootPath` 本身是**已存在的目录**，只是当前**为空**。把存在的路径标记为 missing 语义错误。
  - 实际上由于 negative cache key 是具体路径，根 Lookup 检查的是 `JoinPath(root, name)` 而不是 root 本身，所以目前没引发直接故障（属于"错进错出"的负负得正）。但这是埋雷：一旦未来有人让 Lookup 也检查目录本身是否 missing，或者 negative cache 扩展到更多地方，根目录和空目录就会被误判为不存在。
- **建议**：删除这两处 `f.markMissing(...)` 调用。空目录就是空目录，`store.ReplaceChildren(fsid, [])` 之后下次列目录返回空列表即可，不需要 negative 标记。
- **验收**：删除两行后跑 `go test ./internal/fs/...`；新增测试：刷新空目录后 Lookup 目录内不存在的子项，首次返回 ENOENT 并正确标记子项路径为 missing（而非父目录）。

### 3. chunkStore 在磁盘 chunk 损坏/读失败时未删除残留文件 → 磁盘泄漏
- **位置**：`internal/stream/store.go:119-124`（`getWithTouch`）
  ```go
  data, err := os.ReadFile(path)
  if err != nil {
      s.forgetDisk(key, path)   // 仅从内存索引移除
      return nil, false
  }
  ```
  `forgetDisk` 只调 `removeDiskLocked`（从 map 和 LRU 移除），**没有 `os.Remove(path)`**。
- **问题**：磁盘 chunk 文件被部分写入、磁盘损坏或用户手改后，读失败时内存索引被清，但文件留在磁盘上，既不被 `trimDiskLocked` 清理（已不在 diskEntries 里），也不被下次命中（diskReady 已无 key），直到目录被手动清理。磁盘缓存上限 2 GiB 长期跑下来会有孤立文件积累。
- **建议**：在 `forgetDisk` 里加一行 `os.Remove(path)`；或改名为 `evictDisk` 并让所有调用方都负责删文件。为了不让锁里做 I/O，可在锁外异步删除：
  ```go
  func (s *chunkStore) forgetDisk(key chunkKey, path string) {
      s.diskMu.Lock()
      s.removeDiskLocked(key, path)
      s.diskMu.Unlock()
      _ = os.Remove(path) // best-effort, ignore ErrNotExist
  }
  ```
- **验收**：写测试：构造一个含无效字节的 `.chunk` 文件放入 cachePath，`get(key)` 返回 false 后验证该文件已从磁盘删除。

### 4. `ReadStreamRange` 在 auth 失败后只刷 token 不重试，浪费一次 RTT
- **位置**：`internal/remote/remote.go:278-301`
  ```go
  if err != nil {
      if ClassifyDownloadError(err) == DownloadErrorAuth {
          _ = client.RefreshAuth()   // 刷了 token 但直接 return err
      }
      return nil, err
  }
  ```
  对比 `ReadExactRange`（line 257-275）用 for 循环重试 2 次，auth 错误刷新后第二次直接成功。
- **问题**：stream 路径外层 `download()`（manager.go:1045）虽然有 `for attempt := 0; attempt < 2; attempt++` 兜底，但第一次必失败、第二次才成功，等于白白浪费一次网络 RTT（百度 CDN → 客户端）+ 一次完整的 chunk 等待时间。hedge 竞速和前台读延迟都会被这次无谓的失败拖慢一次。
- **建议**：把 auth 刷新后的重试在 `ReadStreamRange` 内部完成，和 `ReadExactRange` 对齐：
  ```go
  for attempt := 0; attempt < 2; attempt++ {
      n, err := client.ReadRange(ctx, fsid, offset, data)
      if err == nil && n != len(data) { err = ... }
      if err == nil { return data[:n], nil }
      if ctx.Err() != nil { return nil, ctx.Err() }
      if ClassifyDownloadError(err) == DownloadErrorAuth { _ = client.RefreshAuth() }
  }
  return nil, lastErr
  ```
  如果不想重试 transport 错误（外层 `download` 已做），可以只在 auth 错误时 continue，其他 break。
- **验收**：写测试用 `httptest.Server` 第一次返回 401、第二次返回 206，验证 `ReadStreamRange` 一次调用直接成功（不依赖外层重试）。

---

## P1 — 性能 / 体验改进

### 5. 非流式 read path（remote.Reader）缓存命中查找是 O(n) 全 bucket 扫描 + O(n) LRU touch
- **位置**：`internal/remote/remote.go:321-344`（`readCachedLocked`）
  - 遍历 `bucket`（`map[cacheKey]struct{}`）找 offset 落在的缓存窗口——bucket 里最多有 `cacheLimit/chunkSize = 64MB/8MB = 8` 个条目，常数小但代码是 O(n) 遍历 map。
  - LRU touch 用 `cacheOrder` slice 做线性查找 + 切片位移（`copy(r.cacheOrder[index:], r.cacheOrder[index+1:])`），每次命中都 O(n)。
- **建议**：用 `container/list`（像 stream/store.go 那样）+ `map[cacheKey]*list.Element` 结构，命中时 O(1) `MoveToBack`。或者直接在 cacheKey 上加 fsid 索引用有序结构。当前规模下性能影响不大（最多 8 项），但和 stream 的实现风格不统一，维护两套 LRU 容易出 bug。
- **验收**：重构后 `go test ./internal/remote/... -count=1` 通过，benchmark 显示 LRU touch 延迟不随缓存项数线性增长。

### 6. Stream.Handle 没有 inode 级缓存，每次 open() 都新建 Handle 并可能丢 session
- **位置**：`internal/fs/filesystem.go:731-744`（`entryNode.Open`）：每次 Open 都 `n.Filesystem.stream.Open(file)` 新建 Handle。
- **问题**：正常播放时 Emby 一个 fd 读完，没问题。但某些场景（IINA、VLC 反复探测、拖动时先 open→读一点→close→再 open）会导致 `Handle.Release()` 把 `s.refs` 减到 0、`cleanupSessionLocked` 把 session 删掉。下次 open 同一个文件，session 重建，之前预热的缓冲（内存+磁盘）虽然 chunkStore 里还在（keyed by chunkVersion，与 session 无关），但 `scheduledFloor/End/cursor` 等调度状态全丢，可能短时间内重复调度。
- **建议**：在 inode 上维护"最近活跃 Handle"的引用（release 时延迟几秒再清），或者让 Release 只减 ref 不立即清 session（session 自己带 idle TTL）。简化做法：`cleanupSessionLocked` 保留 session 一段时间（比如 30s）再清，允许短时间内 reopen 复用 cursor/schedule 状态。
- **验收**：新增测试模拟 open→读→release→立刻 open→读同一文件，验证第二次 open 的第一次 Read 不需要重新调度首块（命中已缓存数据）。

### 7. 没有显式 SIGINT/SIGTERM 处理，依赖 go-fuse Wait 内部拦截
- **位置**：`internal/app/app.go` —— `Run()` 调用 `server.Wait()` 阻塞，没有 `signal.Notify` 上下文。
- **问题**：go-fuse v2 的 `server.Wait()` 内部确实监听 SIGINT/SIGTERM 并调 Unmount，但：
  1. 作为容器 PID 1 运行时，dockerd 发送 SIGTERM 后如果 go-fuse 没在 10s 默认超时内完成，会被 SIGKILL 强杀，可能留下 FUSE 挂载点"transport endpoint is not connected"残留（README 已经给了 `umount -l` 的人工修复方案）。
  2. defer 里的 `server.Unmount()` 能跑，但 SQLite 关闭、diskWriter flush（`m.diskWG.Wait()` 在 `Close()` 里，`Close()` 没被调用）等清理没有保证。
- **建议**：
  1. 在 `mountAndWait` 里 `signal.NotifyContext` 捕获 SIGINT/SIGTERM，触发 `stream.Manager.Close()`（等 worker 空跑、diskWriter flush）+ `store.Close()`（关 SQLite），再调 `server.Unmount()`/`server.Wait()`。
  2. 给整个 shutdown 一个超时（比如 15s），超时后强制退出。
- **验收**：`docker stop` 容器，验证退出码 0、`/proc/mounts` 无残留挂载、下次启动 data/meta.db 不出现 `database is locked` 或 WAL 文件堆积。

### 8. docker-compose 默认关磁盘缓存（`STREAM_DISK_CACHE=0`）但 Go 代码默认 2 GiB，缺日志提示
- **位置**：`docker-compose.yml:35` vs `internal/config/config.go:55` 默认值。
- **问题**：README 已经说明，但用户直接用 Go 代码跑二进制（不走 docker-compose）会默认开 2 GiB 磁盘缓存到空的 `STREAM_CACHE_PATH`——空路径时 newChunkStore 会把 diskLimit 当作 0 处理（store.go:66 `if diskPath != "" && diskLimit > 0`），所以实际不会写盘，但这层判断用户看不到。启动日志里没有打印当前磁盘缓存是否开启、路径是什么、上限多大。
- **建议**：启动时打印一行 log，比如 `stream cache memory=320MiB disk=2GiB path=/data/stream-cache` 或 `stream cache disk=disabled`，让用户一眼看清。
- **验收**：docker-compose up 日志里能看到 cache 配置。

### 9. Stream summary channel 容量为 1 且丢消息静默，诊断信号会被吞
- **位置**：`internal/stream/manager.go:244`（`summaries: make(chan summaryRequest, 1)`）+ `queueSummary` line 441-447（`select { case m.summaries <- request: default: }`）。
- **问题**：节流靠 `s.lastSummary` 的 5s 时间窗判断，channel 满了直接 default 丢——但如果 summaryWorker 正忙、同一秒内连续 2 次 queueSummary，第二次被丢后要等下一个 5s 周期才会再触发，日志出现明显卡顿间隙。极端情况下（持续小步读），可能很长时间都看不到 `stream summary` 日志，让排查问题的人以为引擎卡死。
- **建议**：要么把 channel 容量调大（8 足够），要么 queueSummary 改为"替换最新值"（用 `select { case m.summaries <- request: default: drain then send }`），保证 5s 窗口内至少有一次 summary 能被消费。
- **验收**：压测持续 60s 高频小步读，日志里 `stream summary` 间隔 ≤ 10s。

---

## P2 — 清理 / 可维护性

### 10. `app.bindRemoteClient` 里重复定义 `clientFactory` 是死代码
- **位置**：`internal/app/app.go:412-418`
  ```go
  if a.clientFactory == nil {
      a.clientFactory = func(token auth.Token) baidu.Client {
          return baidu.NewAPIClientWithHTTPClients(..., nil)  // 注意：这里传的 onTokenUpdate 是 nil！
      }
  }
  ```
  `app.New` 已经在 line 191-193 给 `a.clientFactory` 赋过值（且 onTokenUpdate 不为 nil），所以这个 nil 分支永远走不到。
- **问题**：如果未来某人改了 New 让 clientFactory 可选，这里的 fallback 闭包**丢失了 onTokenUpdate 回调**，新 token 不会持久化——这是个隐患。
- **建议**：直接删除 `if a.clientFactory == nil { ... }` 整块，需要时让 New 负责构造。
- **验收**：删除后 `go build ./... && go test ./internal/app/...` 通过。

### 11. `Filesystem.RefreshAll` 方法未被主流程调用（死代码）
- **位置**：`internal/fs/filesystem.go` 中 `RefreshAll` 方法（递归刷新所有已知子目录）。主流程只调 `RefreshRootOnly` + 目录访问时的按需刷新。
- **建议**：要么删掉，要么加个 `BAIDUDISKLINK_REFRESH_ALL=1` 环境变量让用户可选（定期全量刷新能让 Emby 扫库更快看到新文件，但百度 API 调用次数会涨）。
- **验收**：删除则跑 `go vet`；保留则补一个启动开关。

### 12. 缺乏结构化指标 / Prometheus 导出
- **问题**：当前只有日志输出（`fuse read summary`、`stream summary`），想做长期监控（buffer_ahead 趋势、hedge 频率、下载 P95、auth 刷新次数）只能靠日志聚合。
- **建议**：用 `expvar` 或 Prometheus client_golang 暴露：
  - `baidudisklink_stream_buffer_ahead_bytes`
  - `baidudisklink_stream_downloaded_bytes_total`
  - `baidudisklink_stream_retries_total` / `hedges_total` / `dlink_refreshes_total`
  - `baidudisklink_stream_inflight_tasks`
  - `baidudisklink_cache_memory_bytes` / `cache_disk_bytes`
  - `baidudisklink_fuse_slow_reads_total`
  默认监听 `127.0.0.1:9xxx` 的 `/metrics`，不影响生产。
- **验收**：容器内 curl 能拿到 Prometheus 文本格式指标。

### 13. Token 文件没有原子写入
- **位置**：`internal/auth/file_store.go`
- **问题**：`Save(data)` 直接 `os.WriteFile`，进程崩溃在写一半时会留下截断的 token.json，下次启动无法恢复，必须重新 OAuth。
- **建议**：写临时文件 + rename（和 stream/store.go 里 putDisk 同款模式）：
  ```go
  tmp := path + ".tmp"
  os.WriteFile(tmp, data, 0600)
  os.Rename(tmp, path)
  ```
- **验收**：压测 1000 次 Save，中途 kill -9，重启 token.json 始终可用。

### 14. 单元测试覆盖可以补强的几个点
- `internal/stream/manager.go` hedge 竞速在 `slowStreak >= 3` 时会 InvalidateDownloadLink——现有测试是否覆盖连续慢读场景？补一个验证 dlink 失效回调被触发的测试。
- `internal/fs` 目录刷新时网络失败，store 旧数据应保留（目前 refreshDir 失败返回 EIO，已有的 children 不动，但没测试明确保证）。
- `internal/baidu` 的 `resolveDownloadURL` HEAD 跟随逻辑：没测试 302 → 302 多级重定向、4xx/5xx 错误体。

### 15. 部署相关小改进
- **Healthcheck**：Dockerfile 没加 `HEALTHCHECK` 指令，可以加一个检查 FUSE 挂载点是否能 stat 根目录的脚本，让 `docker-compose ps` 能区分"活着但没授权完"和"正常工作"。
- **Graceful OAuth shutdown**：OAuth server 现在在 `Run()` defer 里 Shutdown，但 Wait() 拿到 code 后到 mountAndWait 之间还有一段时间 Shutdown 了也不影响；这是 OK 的，但 `Shutdown` 用 `context.Background()` 没超时，极端情况下 HTTP server 卡死会阻塞整个退出——换成 5s timeout context。

---

## 优先级执行建议

| 阶段 | 包含项 | 理由 |
|---|---|---|
| 先修 P0 | #1 #2 #3 #4 | 都是实打实的 bug，修了立刻改善正确性/延迟 |
| 再做 P1 | #6 #7 优先，#5 #8 #9 按节奏 | Handle 复用和优雅退出影响真实用户体验 |
| 最后 P2 | #10 #13 立刻能改，#11 #12 #14 #15 规划进下个版本 | 死代码和健壮性，不影响当前功能 |

修完 P0 后跑一次 `bench-stream --bitrate 100 --duration 10m --seek-interval 60s`，对比修复前后 `steady_stalls`、`read_p95`、`hedges` 三个指标（auth 失效重试那项修复在 token 过期场景下应该能观察到 read_p95 下降）。
