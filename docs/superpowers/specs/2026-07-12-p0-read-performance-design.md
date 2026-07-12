# BaiduDiskLink P0 读取性能与资源控制设计

日期：2026-07-12
状态：已批准

## 目标

在不改变现有部署配置和默认下载并发的前提下，完成四项 P0 优化：

1. 减少百度 Range 下载、远程缓存和 FUSE 句柄之间的大块重复复制。
2. 将全局读取缓存限制为 64 MiB 字节预算。
3. 为元数据请求和文件下载配置可控的 HTTP Transport 与超时。
4. 将请求取消贯穿到 HTTP 下载，并让并发分块在首个错误后快速停止。

## 非目标

- 不实现磁盘内容缓存。
- 不实现自适应预读窗口或后台预取；这些属于 P1。
- 不提高默认 `BAIDUDISKLINK_DOWNLOAD_CONCURRENCY=1`。
- 不改变 OAuth、目录刷新、删除开关或挂载路径语义。
- 不引入第三方依赖。

## 总体架构

读取链路保持现有分层：

```text
FUSE / playback HTTP request
        ↓ context.Context
internal/remote.Reader
        ↓ caller-owned destination slices
internal/baidu.Client
        ↓ http.NewRequestWithContext + bounded body read
Baidu dlink/CDN
```

数据窗口只在 `remote.Reader` 分配一次。顺序读取直接填充该窗口；并发读取的 worker 填充互不重叠的子切片。成功窗口以不可变对象进入 64 MiB LRU。FUSE 文件句柄引用该不可变窗口，不再复制出第二份 16 MiB 缓冲。

## 接口设计

### 百度客户端

`baidu.Client` 的范围读取从返回新切片改为填充目标缓冲区：

```go
ReadRange(ctx context.Context, fsid string, offset int64, dst []byte) (int, error)
```

契约：

- `dst` 为空时返回 `(0, nil)`。
- 使用 `http.NewRequestWithContext`，请求取消直接终止正文读取。
- 设置 `Range: bytes=<offset>-<offset+len(dst)-1>`。
- Range 请求只接受合法的 `206 Partial Content`。
- 校验 `Content-Range` 起点与请求 offset 一致；总长度未知时允许 `*`。
- 通过 `io.LimitReader(resp.Body, int64(len(dst))+1)` 读取，拒绝超过目标长度的响应。
- 到达文件 EOF 时允许短读；其他意外短读返回 `io.ErrUnexpectedEOF`。
- 返回值 `n` 是已经写入 `dst[:n]` 的字节数。

所有静态客户端、测试替身和调用者同步切换，不保留旧接口适配层。

### Remote Reader

公开读取入口增加上下文：

```go
ReadRange(ctx context.Context, fsid string, offset, length int64) ([]byte, error)
ReadExactRange(ctx context.Context, fsid string, offset, length int64) ([]byte, error)
```

`remote.Reader` 内部引入不可变窗口：

```go
type readWindow struct {
    fsid   string
    offset int64
    data   []byte
}
```

缓存命中返回窗口内的只读子切片。调用方不得修改。FUSE 和 playback 仅消费数据，不写入返回值，满足该约束。

### FUSE 文件句柄

`entryFileHandle` 保存最近窗口的引用和范围元数据，不再执行：

```go
h.window = append(h.window[:0], data...)
```

返回 FUSE 前不再为句柄缓存命中额外复制；数据生命周期由句柄引用或全局缓存引用保证。窗口一旦发布即不可修改。

## 缓存预算

全局读取缓存采用 LRU，硬上限为 64 MiB：

```go
const defaultReadCacheBytes int64 = 64 << 20
```

规则：

- 预算按 `len(window.data)` 累计，不按缓存项数量。
- 新窗口超过 64 MiB 时不进入缓存，但本次读取仍正常返回。
- 加入窗口前，从最久未使用项开始驱逐，直到满足预算。
- 缓存命中提升为最近使用。
- 同一 `(fsid, offset)` 被替换时先扣除旧窗口大小。
- `SetClient` 和下载参数变化继续清空缓存，同时把已用字节归零。
- inflight 请求不计入缓存预算，但计入在途内存限制。

## 全局并发与在途内存

`Reader` 维护进程级下载令牌，不允许每次读取独立放大并发。令牌容量等于配置的下载并发，默认 1。

在途字节上限采用与缓存预算相同的 64 MiB。每个新分配窗口在下载前申请对应字节；完成、失败或取消后释放。单次读取若超过 64 MiB，则独占在途预算，避免永久阻塞；同一时间不允许第二个窗口开始下载。

并发分块行为：

- 为一次读取创建派生 context。
- worker 开始 HTTP 请求前获取全局下载令牌。
- worker 直接写入最终窗口的互不重叠子切片。
- 首个不可恢复错误记录后调用 `cancel()`。
- 未开始任务不再发起；在途请求由 context 中止。
- 等所有 worker 退出后返回首个错误。
- 失败窗口不得写入缓存。

认证刷新只在非 context 取消错误上尝试一次。多个失败 worker 不得并发刷新 token；沿用或扩展单次刷新互斥。

## HTTP Transport

应用创建共享的元数据客户端和下载客户端，避免依赖默认 Transport：

### 元数据/OAuth 客户端

- 总请求超时：30 秒。
- `DialContext` 超时：10 秒。
- TLS 握手超时：10 秒。
- 响应头超时：15 秒。
- 空闲连接超时：90 秒。

### 下载客户端

- 不使用覆盖完整正文读取的 `http.Client.Timeout`。
- `DialContext` 超时：10 秒。
- TLS 握手超时：10 秒。
- 响应头超时：30 秒。
- 空闲连接超时：90 秒。
- `MaxConnsPerHost` 与 `MaxIdleConnsPerHost` 至少等于配置下载并发，最低为 1。
- 禁用透明响应压缩，确保 Range 字节和正文长度可验证。

`APIClient` 可继续持有一个客户端，但构造函数必须获得配置好的 Transport；测试注入自定义 `RoundTripper` 的能力保留。

## 错误处理

- context 取消和 deadline 错误原样返回，不刷新 token。
- 非 206 Range 响应返回包含状态码的错误，错误正文最多读取 512 字节。
- Content-Range 不匹配、响应超长、非 EOF 短读均不得缓存。
- 并发下载返回首个业务错误；取消产生的次生错误不覆盖它。
- FUSE 继续把读取失败映射为 `EIO`；playback 继续记录失败并结束响应。

## 兼容性

- 环境变量名称和默认值不变。
- 默认下载并发仍为 1，默认 chunk 仍为 4 MiB。
- `bench`、`bench-fuse`、FUSE 和 playback 的用户接口不变。
- Go 内部接口执行干净切换，不保留 deprecated 方法或兼容包装。

## 测试设计

### `internal/baidu`

- Range 请求使用传入 context。
- 合法 206 响应直接填充目标缓冲区。
- 200 响应被拒绝，避免整文件读入。
- Content-Range 起点不匹配被拒绝。
- 超长正文被拒绝且不会写越界。
- context 取消终止阻塞下载。
- 专用 Transport 的连接池和超时参数符合设计值。

### `internal/remote`

- 缓存按 64 MiB 字节预算驱逐，而不是按项数。
- 命中会更新 LRU 顺序。
- 超预算单窗口不缓存。
- 读取数据只分配一个窗口，worker 写入不同子区间。
- 首个分块错误取消其他阻塞分块。
- 多次并发读取共享全局下载并发上限。
- 在途字节上限阻止并发窗口超过预算。
- context 取消不触发认证刷新。

### `internal/fs` 与 `internal/app`

- 文件句柄复用同一窗口且不再构造句柄副本。
- seek 精确读与连续窗口读保持现有行为。
- playback 客户端取消可传递到 remote。
- 现有 Range、HEAD、bench 行为保持通过。

## 验证

实现完成后执行：

```bash
/usr/local/go/bin/go test ./internal/baidu ./internal/remote ./internal/fs ./internal/app
make check
```

真实 DSM 上再使用同一文件、相同样本大小对比：

```bash
docker-compose exec baidudisklink baidudisklink bench --path /Videos/<file>
docker-compose exec baidudisklink baidudisklink bench-fuse --path /mnt/baidu/<file>
```

同时记录容器峰值 RSS、起播时间、连续吞吐和 seek 恢复时间。真实百度、DSM、Emby 验证不能由单元测试替代。
