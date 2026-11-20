# 100 Mbps 高码率流式引擎设计

## 1. 背景

BaiduDiskLink 当前通过百度官方开放接口取得 `dlink`，再用 HTTP Range 为 FUSE 提供文件内容。实机直链测试证明百度 CDN 的 `8 MiB` 和 `32 MiB` Range 存在明显慢尾，冷状态吞吐可能降至 `6.06 MiB/s` 和 `4.63 MiB/s`；`1 MiB` Range 两轮均超过 100 Mbps 播放要求。因此流式引擎使用独立 `1 MiB` 调度块，同时保持原有 Seek 判定范围。

现场数据仍表明高码率播放缺少足够的抗抖动能力：

| 测试 | 结果 |
| --- | ---: |
| 256 MiB，单并发 | 10.57 MiB/s |
| 256 MiB，2 并发 | 27.73 MiB/s |
| 256 MiB，4 并发 | 33.21 MiB/s |
| 256 MiB，8 并发 | 44.42 MiB/s |
| FUSE 冷读样本 | 19.66 MiB/s |

这说明百度 CDN 总带宽足够，但单连接和单分块延迟波动明显。继续扩大 FUSE 句柄窗口不能彻底解决问题，因为当前句柄同时承担访问模式判断、缓存、预取和 Seek 生命周期，无法形成稳定、持续、有优先级的下载流水线。

## 2. 目标

| 项目 | 目标 |
| --- | --- |
| 稳定播放 | 70 Mbps 视频连续播放 30 分钟无新增加载 |
| 理论上限 | 100 Mbps 视频模拟播放 30 分钟零停顿 |
| 启动 | 冷启动首个前台块 P95 小于 3 秒 |
| Seek | 随机 Seek 后恢复 P95 小于 3 秒 |
| 抗抖动 | 单个 Range 卡住或延迟 8 秒时不耗尽已建立缓冲 |
| 内存 | 热缓存默认不超过 320 MiB |
| 磁盘 | 临时分块缓存默认不超过 2 GiB |
| 可维护性 | FUSE 只负责文件语义，流式调度集中在独立包 |

100 Mbps 等于约 `11.92 MiB/s`。默认缓冲目标为 `256 MiB`，理论上可覆盖约 21.5 秒网络抖动；低水位为 `128 MiB`，可覆盖约 10.7 秒。

## 3. 非目标

- 不缓存完整视频作为默认行为。
- 不引入 aria2c、WebDAV 或额外播放代理作为核心依赖。
- 不修改目录刷新、OAuth、删除和元数据缓存语义。
- 不承诺百度 CDN 长时间低于 100 Mbps 消费速度时仍能无限播放。
- 不在本次重构中实现上传或其他可写操作。

## 4. 总体架构

```text
Emby / FUSE
     |
     | Read(offset, length)
     v
internal/stream.Manager
     |
     +-- Session: 共享 epoch、活跃句柄、缓冲水位
     +-- Handle: 独立播放游标和访问模式
     +-- Scheduler: 前台优先队列、持续 Worker、取消和竞速
     +-- ChunkStore: 内存热缓存 + 本地磁盘缓存
     +-- Metrics: 下载速度、块延迟、缓冲水位、停顿
     |
     v
internal/remote.Reader
     |
     | 精确 Range 传输、dlink 和错误分类
     v
Baidu CDN
```

职责边界：

| 模块 | 负责 | 不负责 |
| --- | --- | --- |
| `internal/fs` | Open/Read/Release、errno、文件大小边界 | 分块队列、缓存淘汰、慢块策略 |
| `internal/stream` | 会话、优先级、缓冲水位、块生命周期、缓存 | 百度 API 和 FUSE inode |
| `internal/remote` | 精确 Range 下载、重试入口、dlink 刷新 | 推断播放模式和前向窗口 |
| `internal/baidu` | 官方 API、HTTP、token/dlink | 播放调度 |

## 5. 分块模型

默认分块固定为 `1 MiB`。每个块由 `(FSID, 文件版本, chunkSize, chunkIndex)` 唯一标识。块大小属于缓存命名空间的一部分，修改配置后不会错误复用旧块。

```text
missing -> queued -> downloading -> ready
                       |              |
                       v              v
                    retry          evicted
                       |
                       v
                    failed
```

块对象只发布不可变数据。多个 FUSE 句柄或 Emby 探测请求读取同一块时，共享缓存和 inflight，不重复下载。

文件版本优先使用 `FSID + Size + MTM`；当元数据版本变化时，旧缓存自然失效。

## 6. 会话模型

`Manager` 按 FSID 维护共享 `Session`，但访问游标不放在 Session 中。每次 FUSE `Open` 都创建独立的 `stream.Handle`；多个句柄共享块缓存、inflight 和调度队列，分别保存自己的 Probe/Stream/Seek 状态。

Session 记录：

- 当前 epoch，Seek 时递增。
- 当前活跃 Handle 的前台游标。
- 当前块、后向保留范围和前向目标范围。
- 活跃句柄数和最后访问时间。
- 缓冲水位、消费速度和下载速度滑动窗口。

Handle 记录：

- 最近一次 offset/length 和连续前向读取次数。
- 当前访问模式、尾部 Probe 状态和所属 epoch。
- Release 状态；Release 只减少 Session 引用，不会重置其他句柄的游标。

尾部小范围探测只在自己的 Handle 内生效。即使 Emby 同时打开播放句柄和 MKV 尾部探测句柄，也不会把播放 Session 错误地 Seek 到文件尾部。

访问模式：

| 模式 | 判定 | 行为 |
| --- | --- | --- |
| Probe | 孤立的小范围头部或尾部读取 | 精确满足，不建立 256 MiB 队列 |
| Stream | 连续两次向前读取 | 建立持续前向缓冲 |
| Seek | 大跨度跳转或方向变化 | 新 epoch，取消旧后台任务并从新位置重建 |

尾部 MKV 探测不会替换正在播放位置的 Stream epoch。

## 7. 调度器

默认全局 8 个下载 Worker。Worker 完成一块后立即领取下一块，不等待同批其他块。

首次进入 Stream 模式时建立完整前向窗口；之后游标前进只追加新进入窗口尾部的块，并提升新进入 64 MiB 近端区块的优先级，不在每次 FUSE Read 后重新扫描整个 256 MiB 窗口。

任务优先级：

| 优先级 | 范围 |
| --- | --- |
| P0 | 当前 FUSE Read 覆盖的块 |
| P1 | 当前游标后 64 MiB |
| P2 | 64–256 MiB 前向缓冲 |
| P3 | 后向 32 MiB、其他会话及低优先级探测 |

约束：

- P0 可以提升已排队块的优先级。
- 单个活跃播放 Session 可以使用全部 8 个 Worker，避免保留槽位在单片播放时闲置。出现第二个活跃播放 Session 后，每个 Session 恢复最多 6 个 Worker 的公平配额；前台 Seek 和探测仍可提升优先级。
- 每个 Session 最多保留一个前向 epoch；旧 epoch 的 queued 任务直接丢弃，downloading 任务取消。
- 当前块完成即唤醒前台，不等待其余块。

## 8. 缓冲控制

默认值：

| 参数 | 默认值 |
| --- | ---: |
| 块大小 | 1 MiB |
| Worker 数 | 8 |
| 前向低水位 | 128 MiB |
| 前向目标水位 | 256 MiB |
| 后向保留 | 32 MiB |
| 内存热缓存 | 320 MiB |
| 磁盘缓存 | 2 GiB |

调度器根据 ready 块连续区间计算 `bufferAhead`。低于低水位时优先补充当前 Session；达到目标水位后停止继续扩张，避免无边界下载。

## 9. 两级缓存

### 9.1 内存缓存

- 保存离游标最近的 ready 块。
- 默认预算 320 MiB，覆盖 256 MiB 前向目标、32 MiB 后向保留和调度余量。
- 顺序播放命中内存块时不提升已经消费块的 LRU 位置，使新进入窗口尾部的块优先淘汰最旧历史块，而不是挤出尚未消费的前向块；Probe 和 Seek 命中仍按普通 LRU 提升。
- Stream Session 同时按文件位置维护内存窗口：保留游标前 32 MiB 到游标后 256 MiB，游标前进时增量移除刚离开后向窗口的块。LRU 仅处理多 Session 竞争和窗口外兜底，不决定单 Session 的连续播放块。
- 按字节预算做 LRU 淘汰；当前前台块由 Handle 直接等待并发布，更大的连续范围由磁盘缓存承接，不依赖把 256 MiB 全部固定在内存中。
- 查询磁盘块是否存在时只读取文件元数据，不会为了计算 buffer 或补队列把块数据重新读入内存。

### 9.2 磁盘缓存

默认目录：

```text
/data/stream-cache
```

要求：

- 不位于 FUSE 挂载点内。
- 下载到 `.part`，完整校验长度后原子改名为 `.chunk`。
- 文件名包含 FSID、Size、MTM 和 chunkIndex。
- 默认预算 2 GiB，按 LRU 清理。
- 启动时扫描一次已有块建立内存索引；运行中写入、命中和淘汰均维护该索引，不对每个块重新扫描缓存目录。
- 块发布依赖关闭文件后的原子改名，不对每个 1 MiB 临时文件单独执行 `fsync`，也不通过修改文件时间记录命中。
- 服务启动时删除无法识别的残留 `.part`，但不把缓存视为服务端真相。
- 流式引擎使用独立的精确 Range 入口，绕过 `remote.Reader` 的旧窗口数据缓存，避免同一块在两级内存缓存中重复占用。

## 10. 慢块竞速

只对可能导致播放停顿的块启用受限竞速：

- P0 块等待超过动态阈值时，在 HTTP 连接池预算内发起一个相同 Range 的竞速请求。
- 阈值取最近块延迟 P95 的 1.5 倍，并限制在 1.5–4 秒。
- 每块最多竞速一次。
- 先完成者发布数据，另一请求立即取消。
- 缓冲高于低水位时不对后台块竞速。

连续三个 P0 块触发竞速时，流式引擎同时失效 Reader 和 Baidu APIClient 的当前 dlink；后续重试会重新解析 CDN URL。普通成功块会清零连续慢块计数，避免偶发抖动导致频繁调用元数据接口。

## 11. 错误处理

| 错误 | 行为 |
| --- | --- |
| 超时、连接重置、短读 | 新连接重试，保留任务优先级 |
| 401/403 | 刷新 token 和 dlink 后重试 |
| 416 | 刷新文件元数据，确认文件版本 |
| 连续慢连接 | 失效当前 dlink，重新解析 CDN URL |
| EOF | 仅文件真实末尾接受 |
| 磁盘缓存写失败 | 降级为内存缓存，不阻断前台 |

禁止把所有下载错误都解释为认证过期。

## 12. FUSE 接入

`entryFileHandle` 收缩为：

- 持有一个 `stream.Handle` 和最后一次诊断策略。
- `Read` 调用 `Handle.ReadAt(ctx, off, length)`。
- `Release` 释放独立 Handle 和 Session 引用。
- 保留现有读取错误到 `EIO` 的映射。

Seek 判断、预取 goroutine、窗口切片和缓存状态全部从 FUSE 句柄移出；FUSE 不再维护旧的 32 MiB 窗口预取引擎。

## 13. 可观测性

正常运行不打印每个 FUSE Read，只输出关键事件：

```text
stream start
stream seek
buffer low
chunk hedge
chunk retry
chunk failed
stream summary
```

聚合摘要在独立后台协程中计算和输出；即使 DSM 的 Docker 日志写入暂时阻塞，也不能延迟前台 FUSE Read。

聚合摘要字段：

```text
fsid, epoch, buffer_ahead, download_10s, consume_10s,
workers, inflight, worker_limit, cache_memory, cache_disk, hedges, retries, stalls
```

## 14. 配置

新增配置均有保守默认值：

```dotenv
BAIDUDISKLINK_STREAM_CHUNK_SIZE=1048576
BAIDUDISKLINK_STREAM_WORKERS=8
BAIDUDISKLINK_STREAM_LOW_WATERMARK=134217728
BAIDUDISKLINK_STREAM_TARGET_BUFFER=268435456
BAIDUDISKLINK_STREAM_BACK_BUFFER=33554432
BAIDUDISKLINK_STREAM_MEMORY_CACHE=335544320
BAIDUDISKLINK_STREAM_DISK_CACHE=2147483648
BAIDUDISKLINK_STREAM_CACHE_PATH=/data/stream-cache
BAIDUDISKLINK_STREAM_HEDGE=1
```

原有 `BAIDUDISKLINK_DOWNLOAD_CONCURRENCY` 和 `BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE` 继续用于旧 `bench`；FUSE 播放独立使用 `STREAM_WORKERS` 和 `STREAM_CHUNK_SIZE`。

## 15. 测试与验收

### 自动化测试

- 块状态机和优先级提升。
- Worker 完成即补位，不等待慢块。
- 同块多读共享 inflight。
- P0 块先于 P1/P2 返回。
- Seek epoch 取消旧任务。
- Probe 不破坏 Stream 会话。
- 内存和磁盘缓存预算、LRU、原子发布。
- 慢块竞速只发布一次。
- 短读、超时、401/403、416 和 EOF 分类。
- `go test -race ./...`、`go vet ./...`、Linux amd64 Docker 构建。

### 模拟验收

新增 `bench-stream`：

```bash
baidudisklink bench-stream \
  --path '/Videos/test.mkv' \
  --bitrate 100 \
  --duration 30m \
  --seek-interval 60s
```

输出中的 `startup` 单独统计冷启动首个消费步的等待时间，`stalls` 只统计起播后的持续播放停顿，避免把正常起播等待误报为播放卡顿。

### 百度直链下载隔离测试

为了区分百度 CDN、HTTP 协议复用和 FUSE/流式缓存问题，增加独立的 `bench-download`。该命令在读取应用配置前分流，只接受百度直链和可选 Cookie，不触发 OAuth、目录刷新、FUSE 挂载、`remote.Reader` 或本地缓存。

它先发起一个 `Range: bytes=0-0` 请求获取文件大小，随后由固定 Worker 数直接下载目标字节数。每个请求都校验 `206`、`Content-Range` 和响应长度，并记录实际 TCP 连接数、协议、Range 请求数和重试数。`--http-version auto` 可以观察默认协商结果，`--http-version 1.1` 用于和 aria2c 的多物理连接模式对照。

```bash
baidudisklink bench-download \
  --url "$BAIDUDISKLINK_BENCH_URL" \
  --cookie "$BAIDUDISKLINK_BENCH_COOKIE" \
  --bytes 268435456 \
  --connections 8 \
  --chunk-size 1048576 \
  --http-version 1.1 \
  --retries 0
```

直链和 Cookie 不进入日志、测试或仓库；生产环境优先使用 `BAIDUDISKLINK_BENCH_URL` 与 `BAIDUDISKLINK_BENCH_COOKIE` 环境变量传递。

通过条件：

| 场景 | 标准 |
| --- | --- |
| 100 Mbps 模拟播放 30 分钟 | 建立缓冲后零停顿 |
| 注入一个 8 秒慢块 | 缓冲不耗尽 |
| 单连接永久卡住 | 竞速请求接管 |
| 每分钟随机 Seek | P95 小于 3 秒 |
| 两句柄同文件 | 不重复下载 |
| 内存/磁盘预算 | 不超过配置上限 |

### 实机验收

- DSM 上使用 Emby Direct Play。
- 70 Mbps 样本连续播放至少 30 分钟。
- 100 Mbps 样本连续播放至少 30 分钟。
- 各执行 5 次冷启动和 10 次大跨度 Seek。
- 同时记录 `bench`、`bench-fuse` 和 `stream summary`。

## 16. 迁移与回滚

- 新引擎先在独立分支和测试镜像验证。
- FUSE 对外路径和 Emby 媒体库路径不变。
- 合并前删除旧句柄预取实现，避免双引擎长期共存。
- 出现回归时可直接切回正式镜像标签，不修改元数据数据库。
