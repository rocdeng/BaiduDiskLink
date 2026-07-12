# BaiduDiskLink P1 自适应窗口与异步预读设计

日期：2026-07-12
状态：已批准

## 目标

在 P0 已完成的严格 Range 读取、64 MiB 缓存预算、全局下载并发和 context 取消基础上，优化 FUSE/Emby 的连续播放路径：

1. 首次读取和 seek 使用较小窗口，降低起播及拖动恢复延迟。
2. 确认连续播放后逐步扩大读取窗口，提高高 RTT 网络下的持续吞吐。
3. 在播放器消费当前窗口期间预取下一窗口，使网络读取与播放消费重叠。
4. 保持预取任务、内存和下载连接受 P0 资源限制约束。

## 非目标

- 不增加磁盘内容缓存。
- 不增加新的环境变量或运行配置。
- 不改变 playback HTTP 代理的读取策略。
- 不修改默认下载并发、chunk 大小或 64 MiB 缓存预算。
- 不实现跨文件、跨句柄或基于时间的全局预测。
- 不提高元数据刷新频率。

## 方案

采用 FUSE 文件句柄级状态机。每个 `entryFileHandle` 只观察自己的读取序列，因此多个播放器读取同一 FSID 时不会相互污染。`remote.Reader` 只提供受资源约束的同步读取与显式预取能力，不自行推断访问模式。

## 窗口状态机

固定窗口阶梯：

```go
var sequentialWindowSizes = [...]int64{
    4 << 20,
    8 << 20,
    16 << 20,
    32 << 20,
}
```

### 首次读取

- 从 4 MiB 开始。
- 若 FUSE 单次请求大于 4 MiB，则窗口至少覆盖本次请求。
- 窗口不得超过文件剩余大小。

### 连续读取

读取满足以下任一条件即视为连续：

- 请求 offset 位于当前窗口内，且不小于上次返回区间起点；
- 请求 offset 等于上次返回区间末尾。

每完成一次跨越新的顺序区间，窗口上升一级：4 → 8 → 16 → 32 MiB。命中同一小范围的重复读取不得反复升级。

### seek

以下情况视为 seek：

- offset 小于上次返回区间起点；
- offset 大于当前窗口末尾；
- offset 与上次返回区间末尾之间存在空洞。

seek 窗口为：

```text
max(request length × 2, 1 MiB)，上限 4 MiB
```

并重置顺序级别到 4 MiB。seek 后的新连续序列重新逐级增长。

## 预读触发

当前读取结束位置达到当前窗口的 50% 时，尝试预读下一窗口：

```text
consumedEnd - windowOffset >= windowLength / 2
```

下一窗口：

- offset 等于当前窗口实际数据末尾；
- 长度等于当前自适应窗口大小；
- 在文件尾部截断；
- 长度为零时不启动。

每个句柄最多一个预读任务。若相同 `(fsid, offset, length)` 已在预读，不重复创建。

## 生命周期与取消

`entryFileHandle` 增加：

```go
prefetchCancel context.CancelFunc
prefetchDone   chan struct{}
prefetchOff    int64
prefetchLen    int64
closed         bool
```

规则：

- 新 seek 在同步读取前取消旧预读。
- 新预读替换旧预读前先取消旧任务。
- `Release` 取消预读并等待 goroutine 退出。
- 预读 context 不直接继承单次 FUSE Read context；它由句柄生命周期 context 管理，避免前台 Read 返回后自动取消。
- 预读任务结束时仅在仍是当前任务的情况下清理状态，防止旧 goroutine 覆盖新任务。
- 句柄关闭后不得启动新预读。

## Remote Reader 预读接口

新增显式接口：

```go
func (r *Reader) Prefetch(ctx context.Context, fsid string, offset, length int64) error
```

行为：

1. 参数无效、长度为零或 context 已取消时直接返回。
2. 若完整范围已经在缓存中，直接返回，不产生 HTTP 请求。
3. 使用现有 inflight 合并相同窗口请求。
4. 使用现有下载并发、连接池和 Range 校验。
5. 成功后发布到 64 MiB 全局缓存。
6. 失败不改变前台已返回的数据。

前台读取到预读窗口时，直接从全局缓存命中。若预读仍在进行，复用现有 inflight 合并等待，不能再发起重复下载。

## 前台优先级

P0 当前下载并发默认是 1。预读不能长期占用唯一令牌导致前台读取饥饿：

- 预读只在当前窗口消费过半后启动，此时播放器仍有约半窗口数据可消费。
- 新 seek 立即取消旧预读，释放连接和下载令牌。
- 前台请求与相同预读窗口合并，而不是排队重复下载。
- 本次不实现通用优先级队列；实测若仍出现抢占，再作为独立优化处理。

## 错误处理

- 预读失败不让成功的前台 FUSE Read 返回错误。
- context 取消属于正常结束，不记录错误日志。
- 其他预读错误只在启用 `BAIDUDISKLINK_FUSE_TRACE_READS` 时记录，避免网络波动刷日志。
- 前台读取失败仍按现有行为映射为 `EIO`。
- 预读失败不得写入缓存或留下 inflight 状态。

## 并发安全

- 句柄窗口、自适应级别、上次范围和预读状态全部受 `entryFileHandle.mu` 保护。
- 网络调用不持有句柄互斥锁。
- 启动预读时在锁内登记任务身份，goroutine 在锁外下载。
- 完成回调重新加锁，并核对任务身份后清理。
- `Release` 在锁内取出 cancel/done，解锁后取消并等待，避免死锁。

## 测试

### 自适应窗口

- 首次读取使用 4 MiB。
- 连续跨区间读取按 4→8→16→32 MiB 增长。
- 重复读取同一区间不升级。
- 窗口不超过 32 MiB。
- FUSE 请求本身大于窗口时仍完整覆盖。
- 文件尾部窗口正确截断。

### seek

- 向前、向后和跨空洞读取均重置到 1–4 MiB seek 窗口。
- seek 取消正在运行的预读。
- seek 后连续读取重新增长。

### 预读

- 消费不足 50% 不触发。
- 达到 50% 只启动一个任务。
- 预读命中后下一次前台读取不新增百度请求。
- 前台读取等待相同 inflight，而不重复下载。
- 文件尾部不启动零长度预读。
- 预读失败不影响前台返回。

### 生命周期

- `Release` 取消阻塞预读并等待退出。
- Release 后不得创建新任务。
- `go test -race` 无数据竞争。

## 验证

自动验证：

```bash
/usr/local/go/bin/go test -race ./internal/remote ./internal/fs ./internal/app
make check
```

DSM 实测使用与 P0 相同的文件和样本，记录：

- 首次起播时间；
- seek 后恢复时间；
- 连续吞吐；
- 百度 Range 请求数；
- 容器峰值 RSS；
- 多次 seek 后是否残留下载连接或 goroutine。

真实百度、DSM 和 Emby 的性能结论不能由单元测试代替。
