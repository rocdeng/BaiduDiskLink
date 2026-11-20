# 停止播放后内存不释放 — 优化方案（待实施）

> 背景：停止播放后 RSS 仍有 400+ MB。此文档只记录方案，不落地代码。
> 状态：工作区存在未提交的并发改动（滑动空闲回收 + `osReclaimWorker`），方案需在其上叠加。

## 1. 根因

| 阶段 | 现状 |
| --- | --- |
| 播放中 | 流式引擎把已下载分块放进内存热缓存，默认预算 320 MiB（`StreamMemoryCache`） |
| 停止播放 | FUSE `Release` → 会话引用归零，`cleanupSessionLocked` 只删会话，**不释放内存缓存** |
| 停止后 | 内存缓存仍持有已缓冲分块；滑动空闲回收默认 `IdleReclaim=10min` 后才触发 |
| 结果 | RSS 停留约 400 MB（320 MiB 缓存 + Go 运行时），最长 10 分钟不回落 |

关键点：Go 运行时即使缓存分块变为垃圾，也不会主动把内存归还 OS，必须显式 `debug.FreeOSMemory()`（并发实现已放入 `osReclaimWorker`，只是触发时机太晚——等 10 分钟空闲）。

## 2. 方案

在"会话结束 = 停止播放"的时点**立即**回收该文件的内存缓存，并复用现有 `freeOS` 通道 + `osReclaimWorker` 把内存归还 OS。

### 改动点

| 文件 | 改动 |
| --- | --- |
| `internal/stream/store.go` | 新增 `purgeVersionMemory(version string) int64`：删除该 version 全部内存分块，返回释放字节数 |
| `internal/stream/manager.go` | `cleanupSessionLocked` 在 `delete(m.sessions, version)` 后调用 `purgeVersionMemory`；释放量 ≥ 阈值（建议 8 MiB）时向 `freeOS` 发 `reclaimRequest{streamBytes: ...}` |

### 实现细节

- 只在"最后一个句柄释放、无 inflight、无未完成任务"时触发（`cleanupSessionLocked` 现有守卫已保证），播放暂停（句柄未关闭）不会误回收。
- `m.closed` 时跳过，避免关闭流程做无用功。
- `freeOS` 通道缓冲为 1，天然合并并发回收请求，避免 GC 风暴；GC + `debug.FreeOSMemory()` 在 `osReclaimWorker`（已存在）中执行，不持 `m.mu`，不会阻塞前台。
- **不删并发实现的滑动空闲回收**：它是兜底，且负责清理 `remote.Reader` 的 64 MiB 读缓存（`ClearReadCache()`）。
- 磁盘缓存默认 2 GiB 已承接数据，purge 内存不影响重开文件（回读走磁盘）；`diskWrites` 队列中的切片被引用，不会提前释放。

## 3. 测试

| 用例 | 断言 |
| --- | --- |
| `store_test.go`: `TestChunkStorePurgesVersionMemory` | 同版本块全被删、跨版本块保留、`usage()` 归零 |
| `manager_test.go`: `TestLastHandleReleasePurgesSessionMemory` | 释放最后一个句柄后 `store.get` 失效 |
| `manager_test.go`: `TestActiveSessionMemorySurvivesSiblingHandleRelease` | 双句柄释放其一，共享内存缓存保留 |

现有测试兼容性：
- `TestManagerReclaimsMemoryAfterTenMinuteIdleWindow`：句柄全程打开（refs>0），走空闲回收路径，不受影响。
- `TestManagerOpenCancelsPendingIdleReclaim` 已被并发改动替换为 `TestManagerRecentActivityDelaysIdleReclaim`，无冲突。

## 4. 验证

```bash
go build ./...
go test -race ./internal/stream/... ./internal/remote/...
go vet ./...
```

实机：播放 → 停止后观察日志 `stream idle memory reclaimed ...` 即时出现，`ps -o rss` 回落。

## 5. 可选增强

- 把 `Config.IdleReclaim` 暴露为环境变量（当前只走默认 10 分钟，`internal/app/app.go` 未透传 `IdleReclaim`），可调至 60s 作为第二道保险。
- `minFreeOSReclaim` 阈值（8 MiB）避免小探测会话频繁触发 GC。
