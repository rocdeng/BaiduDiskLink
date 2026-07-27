# BaiduDiskLink 改进建议评审（todo_glm）

> 生成日期：2026-07-27
> 这是针对 `todo.md` 的独立第二意见：逐条表态是否认可、补充理由，并追加通读代码后发现的新问题。
> 评审基于对全仓 Go 源码的二次通读（重点核对 `internal/auth`、`internal/remote`、`internal/stream`、`internal/fs`、`internal/store` 的关键路径）。

---

## 一、对 todo.md 15 条的逐条表态

| # | todo.md 原条目 | 我的判断 | 理由 / 补充 |
|---|---|---|---|
| 1 | Negative cache 只在根 Lookup 生效 | ✅ 认可 | 已核实 `filesystem.go:492-525` 的 `entryNode.Lookup` 确实无 `IsMissing` 检查，只有根 `Filesystem.Lookup:228` 有。**修复注意**：子节点查 negative cache 时 key 必须用 `JoinPath(n.entry.Path, name)`（完整子路径），不能只用 name；并且 `deleteChild` 成功后要 `MarkMissing` 才能闭环。todo.md 漏了 deleteChild 这环。 |
| 2 | refreshDir 空目录 markMissing 自身 | ✅ 认可 | 已核实 `filesystem.go:264-266` 和 `594-596`。`markMissing` 语义是"路径不存在"，把存在的空目录标成 missing 是错的。目前因 negative cache 只在根用、且根 Lookup 查的是 `JoinPath(root,name)` 不匹配 root 本身，所以**目前无害但埋雷**。删两行即可。 |
| 3 | chunkStore.forgetDisk 不删损坏文件 | ✅ 强烈认可 | 已核实 `store.go:119-124` + `forgetDisk:199-203` + `removeDiskLocked:359-367`。`removeDiskLocked` 只删 map/LRU 记录，**无 `os.Remove`**。`trimDiskLocked` 扫的是 `diskEntries`，孤立文件永不被清——**真实磁盘泄漏**。这是 todo.md 里最实在的一条。锁外删文件避免 I/O 阻塞 diskMu。 |
| 4 | ReadStreamRange auth 不重试 | 🟡 部分认可 | 已核实 `remote.go:278-301`。功能上不出错（外层 `download()` attempt<2 兜底），只是多一次 RTT。**应降级为 P1 性能，不是 P0 正确性**。另注：`ReadExactRange` auth 刷新后直接 continue 进第二次迭代，无 sleep，正常 OK。 |
| 5 | remote LRU O(n) touch | 🟡 部分认可 | 已核实 `remote.go:321-344`。规模上限 `64MB/8MB=8` 项，O(n) 实际开销可忽略。风格和 stream 层不统一是真问题。**应降级为 P2 清理**，不是 P1 性能。 |
| 6 | Stream.Handle 无 inode 缓存 | 🟡 部分认可 | 已核实 `filesystem.go:731-744` 每次 Open 新建 Handle。补充修正：`cleanupSessionLocked`（manager.go:652-662）条件是 `refs>0 || inflight>0` 都为 false 且无未完成 task，**不是 release 立刻清**——要等 inflight 归零。但 reopen 丢 cursor/schedule 状态是真问题。延迟清 session 的建议合理。 |
| 7 | 无 SIGINT/SIGTERM 处理 | ✅ 认可 | 已核实 `app.go:264-342` 无 `signal.Notify`。**补充关键点**：go-fuse `server.Wait()` 内部用 signal.Notify 拦截，但**只在 Wait 已阻塞时生效**；启动阶段（OAuth 流程、mountAndWait 之前）收到信号无人管。Go runtime 默认不注册 SIGTERM handler，PID 1 场景下 `docker stop` 发 SIGTERM 会直接终止进程、不优雅 Unmount。用 `signal.NotifyContext` 包住整个 Run 是正解。 |
| 8 | 磁盘缓存默认值不一致 + 不打印 | ✅ 认可 | 已核实 `docker-compose.yml:35` 默认 0、`config.go:55` 默认 2GiB。`newChunkStore`（store.go:66）在 `diskPath==""` 或 `diskLimit<=0` 时不启磁盘缓存——但用户直接跑二进制且设了 `STREAM_CACHE_PATH` 却没设 `STREAM_DISK_CACHE` 时会默认开 2GiB。启动 log 一行解决。 |
| 9 | stream summary channel 容量 1 丢消息 | 🟡 部分认可 | 已核实 `manager.go:244,441-447`。`summaryWorker` 单 goroutine 处理 `maybeLogSummary`（几行 log，很快），正常不堆积；只有 log 后端阻塞时才丢。**应降级为 P2**。todo.md 的"drain then send"建议会引入并发竞争，不如直接把容量调到 8。 |
| 10 | bindRemoteClient 死代码 clientFactory | ✅ 强烈认可 | 已核实 `app.go:412-418` 的 nil 分支永不触发（New 时已赋值），且 fallback 闭包**丢了 onTokenUpdate 回调**——隐患真实。直接删整块。 |
| 11 | RefreshAll 死代码 | ✅ 认可 | 主流程只调 `RefreshRootOnly`。删或加开关二选一。 |
| 12 | 缺 Prometheus 指标 | ✅ 认可 | 但对个人项目可能过重，建议先用 `expvar` 起步（标准库零依赖），暴露 `Stats()` 已有的 downloaded/retries/hedges + buffer_ahead + cache bytes。 |
| 13 | Token 非原子写 | ✅ 认可，**建议升 P1** | 已核实 `file_store.go:16-21` 直接 `os.WriteFile`。崩溃留截断文件 → 下次启动必须重新 OAuth。改 tmp+rename 三行代码，收益高。todo.md 标 P2 偏低，**应升 P1**。 |
| 14 | 测试盲点 | ✅ 认可 | 补一条：`hedge goroutine 用 m.ctx 不跟 task.ctx 取消`（见下文新增 D），目前没测试覆盖"前台读取消后 hedge 仍在跑"的场景。 |
| 15 | Dockerfile HEALTHCHECK / OAuth Shutdown 超时 | ✅ 认可 | 已核实 `oauth.go:72-77` Shutdown 用 `context.Background()` 无超时。改成 5s timeout context。 |

### 小结

todo.md 的 15 条**全部站得住脚**，没有错误判断。需要调整的是 3 处优先级：

| 条目 | todo.md 优先级 | 建议调整 | 原因 |
|---|---|---|---|
| #4 ReadStreamRange auth 不重试 | P0 | → P1 | 功能不出错，仅性能 |
| #5 remote LRU O(n) touch | P1 | → P2 | 规模 8 项，开销可忽略 |
| #9 summary channel 丢消息 | P1 | → P2 | 正常不堆积 |
| #13 Token 非原子写 | P2 | → P1 | 影响真实可用性，三行改完 |

---

## 二、todo.md 漏掉、我新追加的可改进点

通读代码时又发现 4 条 todo.md 没收的：

### 新增 A（P1，性能/资源）：remote.Reader.links 与 baidu.APIClient.links 是冗余的双重 dlink 缓存
- **位置**：`internal/remote/remote.go:652-668`（`downloadLink`）调 `client.GetDownloadLink(fsid)`，而后者（`baidu/client.go:218-252`）内部已有自己的 `c.links[fsid]` 缓存（10 分钟 TTL）。
- **问题**：remote 层把 client 返回的结果**再缓存一份**在 `r.links`。实际命中的永远是 client 层，remote 层那份是纯冗余。但 `InvalidateDownloadLink`（remote.go:138-149）要清两层。维护两套缓存既浪费内存又容易不一致。
- **建议**：删掉 `r.links`，`downloadLink` 直接 `return client.GetDownloadLink(fsid)`；`InvalidateDownloadLink` 只调 `client.InvalidateDownloadLink`。
- **验收**：`go test ./internal/remote/...` 通过；验证 hedge 触发 InvalidateDownloadLink 后下次下载重新拿 dlink。

### 新增 B（P1，安全）：OAuth 回调不校验 state 参数 → CSRF 风险
- **位置**：`internal/auth/oauth.go:79-90`（`handleCallback`）只取 `code`，**不校验** `r.URL.Query().Get("state") == s.cfg.State`。
- **问题**：OAuth 2.0 标准要求校验 state 防 CSRF。虽然监听在 `0.0.0.0:8765`（docker-compose 还映射到主机），本地攻击面看似小，但：
  1. 端口对容器所在网络开放，同网段恶意设备/容器可访问。
  2. 若用户在浏览器授权时被诱导访问 `http://DSM-IP:8765/callback?code=ATTACKER_CODE`，攻击者可注入自己控制的 code（攻击者需先有合法 code，但仍是协议违反，能让授权流程被劫持到错误账号）。
- **建议**：`handleCallback` 开头加 `if r.URL.Query().Get("state") != s.cfg.State { http.Error(w, "state mismatch", http.StatusBadRequest); return }`。cfg.State 默认 `baidudisklink`，建议同时支持启动时随机生成 state。
- **验收**：测试用错误 state 返回 400；正确 state 正常返回 200。

### 新增 C（P1，性能/资源）：hedge goroutine 用 m.ctx 而非 task.ctx，前台取消后 hedge 仍空跑
- **位置**：`internal/stream/manager.go:865` `hedgeCtx, hedgeCancel := context.WithCancel(m.ctx)`
- **问题**：hedge 任务挂在 manager 全局 ctx 上，**不跟随 task.ctx（前台请求 ctx）**。如果前台读已因 `ctx.Done()` 返回（`waitForegroundWithHedge:823,831`），hedge goroutine 仍会跑完整个 chunk 下载（浪费带宽+连接池槽位），完成后 `publishHedge` 因 `task.finished` 返回 false 不写缓存——纯浪费。
- **建议**：`hedgeCtx, hedgeCancel := context.WithCancel(task.ctx)`，让 hedge 跟随前台请求生命周期。前台 ctx 取消时 hedge 自动取消。
- **验收**：测试模拟前台 ctx 取消，验证 hedge goroutine 在 `wg.Wait()` 时不阻塞、不继续下载。

### 新增 D（P0，正确性）：StreamMemoryCache < TargetBuffer + BackBuffer 时无启动校验 → 缓冲自挤
- **位置**：`internal/stream/manager.go:189-257`（`NewManager`）只校验 `TargetBuffer >= LowWatermark`、`MemoryCache >= 0`，**不校验 `MemoryCache >= TargetBuffer + BackBuffer`**。
- **问题**：README 明确说"内存预算覆盖完整前向窗口和后向保留，连续播放不会因为预取挤出即将消费的数据"。但当用户配置 `STREAM_MEMORY_CACHE=128MiB` + 默认 `TargetBuffer=256MiB + BackBuffer=32MiB` 时，内存缓存装不下整个调度窗口，LRU 淘汰会挤掉即将消费的 chunk，退化为反复重新下载。代码不强制校验，用户踩坑无任何提示。
- **建议**：`NewManager` 里加 `if cfg.MemoryCache > 0 && cfg.MemoryCache < cfg.TargetBuffer + cfg.BackBuffer { return nil, errors.New("stream memory cache must cover target buffer + back buffer") }`（或降级为 log.Printf warn 并自动放大 MemoryCache）。
- **验收**：配错时启动失败并有清晰错误信息；或 warn 后自动调整并打印实际值。

---

## 三、调整后的优先级总表

| 优先级 | 条目 | 来源 | 一句话 |
|---|---|---|---|
| **P0** | #3 | todo.md | chunkStore 损坏 chunk 不删文件 → 磁盘泄漏 |
| **P0** | #1+#2 | todo.md | negative cache 只在根用 + 空目录错标 missing（一起修） |
| **P0** | 新增 D | 本评审 | MemoryCache < TargetBuffer+BackBuffer 无校验 → 缓冲自挤 |
| **P0** | #10 | todo.md | bindRemoteClient 死代码且 fallback 丢 onTokenUpdate |
| **P1** | #7 | todo.md | 无 SIGINT/SIGTERM 处理，PID 1 下 docker stop 不优雅 |
| **P1** | #13↑ | todo.md(升) | Token 非原子写，崩溃留截断文件 |
| **P1** | 新增 A | 本评审 | remote/baidu 双重 dlink 缓存冗余 |
| **P1** | 新增 B | 本评审 | OAuth 不校验 state → CSRF |
| **P1** | 新增 C | 本评审 | hedge 用 m.ctx 不跟 task.ctx，前台取消后空跑 |
| **P1** | #4↓ | todo.md(降) | ReadStreamRange auth 失败不重试，多一次 RTT |
| **P1** | #6 | todo.md | Handle 无 inode 缓存，reopen 丢 session 状态 |
| **P1** | #8 | todo.md | 磁盘缓存默认值不一致，启动不打印 |
| **P2** | #5↓ | todo.md(降) | remote LRU O(n) touch，规模 8 项可忽略 |
| **P2** | #9↓ | todo.md(降) | summary channel 容量 1，正常不堆积 |
| **P2** | #11 | todo.md | RefreshAll 死代码 |
| **P2** | #12 | todo.md | 缺指标，先 expvar 起步 |
| **P2** | #14 | todo.md | 测试盲点 |
| **P2** | #15 | todo.md | Dockerfile HEALTHCHECK / OAuth Shutdown 超时 |

---

## 四、最终结论

老公的 todo.md **整体质量很高**，15 条全部成立，没有误判。我的调整主要是：

1. **3 条优先级下调**（#4 P0→P1、#5 P1→P2、#9 P1→P2）：功能都不出错，只是性能/可观测性，不至于 P0/P1。
2. **1 条优先级上调**（#13 P2→P1）：Token 原子写三行改完，收益直接。
3. **追加 4 条新发现**：双重 dlink 缓存（A）、OAuth state 不校验（B）、hedge ctx 跟错（C）、MemoryCache 校验缺失（D）。其中 D 是 P0（会让用户配置错误时静默退化）。

建议的修复顺序：**先修 4 条 P0（#3、#1+#2、新增 D、#10）**，都是几十行内的小改且立竿见影；再做 6 条 P1 里和退出/认证相关的（#7、#13、新增 B、新增 C）；其余按节奏推进。

修完 P0 后跑 `bench-stream --bitrate 100 --duration 10m --seek-interval 60s`，对比 `steady_stalls`、`read_p95`、`hedges` 三个指标——新增 D 修复后配置正确的用户应该看到 `steady_stalls=0` 更稳定。
