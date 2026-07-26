# baidudisklink-architecture

## Page 1: 01 系统上下文

### Components (6)

- 用户 / 管理员 [Person] 配置 OAuth、挂载目录并进行运维测试
- 媒体消费者 [Software System: Emby / 网易爆米花 / Infuse] 扫描媒体库、读取文件、播放与 Seek
- BaiduDiskLink [Software System: Go + FUSE 3] 把百度网盘目录映射为 DSM 本地只读文件系统
- 百度开放平台 [Software System: OAuth 2.0 / Netdisk API] 授权、Token 刷新、目录与文件元数据
- 百度 CDN [Software System: HTTP Range] 提供视频文件分块内容
- 群晖 DSM [Software System: Docker / FUSE / bind mount] 运行容器并向宿主机传播挂载点

### Relations (5)

- 用户 / 管理员 [Person] 配置 OAuth、挂载目录并进行运维测试 —配置、授权、诊断命令→ BaiduDiskLink [Software System: Go + FUSE 3] 把百度网盘目录映射为 DSM 本地只读文件系统
- 媒体消费者 [Software System: Emby / 网易爆米花 / Infuse] 扫描媒体库、读取文件、播放与 Seek —访问 /volume2/baidu_videos→ 群晖 DSM [Software System: Docker / FUSE / bind mount] 运行容器并向宿主机传播挂载点
- 群晖 DSM [Software System: Docker / FUSE / bind mount] 运行容器并向宿主机传播挂载点 —/dev/fuse + rshared 挂载传播→ BaiduDiskLink [Software System: Go + FUSE 3] 把百度网盘目录映射为 DSM 本地只读文件系统
- BaiduDiskLink [Software System: Go + FUSE 3] 把百度网盘目录映射为 DSM 本地只读文件系统 —OAuth、List、Stat、dlink→ 百度开放平台 [Software System: OAuth 2.0 / Netdisk API] 授权、Token 刷新、目录与文件元数据
- BaiduDiskLink [Software System: Go + FUSE 3] 把百度网盘目录映射为 DSM 本地只读文件系统 —并发 HTTP Range 下载→ 百度 CDN [Software System: HTTP Range] 提供视频文件分块内容

## Page 2: 02 容器与部署

### Components (9)

- CLI 入口 [Container: cmd/baidudisklink] mount、bench、bench-fuse、bench-stream、bench-download、playback
- 应用编排 [Container: internal/app] 配置校验、依赖组装、OAuth 流程、挂载与周期刷新
- FUSE 挂载 [Container: go-fuse/v2 + FUSE 3] 容器内挂载 /mnt/baidu，向 DSM 传播
- 持久化数据卷 [Container: /data] token.json、meta.db、stream-cache _data store_
- DSM 宿主机挂载点 [Container: bind propagation: rshared] /volume2/baidu_videos，供 Emby 与播放器读取
- OAuth 回调端口 [Container: HTTP :8765] 接收百度授权码回调
- Docker Compose / Container Manager [Software System] privileged、SYS_ADMIN、/dev/fuse、环境变量与卷映射
- 应用编排 [Container: internal/app] 配置校验、依赖组装、OAuth 流程、挂载与周期刷新
- 媒体消费者 [Software System: Emby / 网易爆米花 / Infuse] 通过 DSM 宿主机挂载点读取视频

### Relations (7)

- Docker Compose / Container Manager [Software System] privileged、SYS_ADMIN、/dev/fuse、环境变量与卷映射 —启动容器进程→ CLI 入口 [Container: cmd/baidudisklink] mount、bench、bench-fuse、bench-stream、bench-download、playback
- CLI 入口 [Container: cmd/baidudisklink] mount、bench、bench-fuse、bench-stream、bench-download、playback —加载环境变量并执行命令→ 应用编排 [Container: internal/app] 配置校验、依赖组装、OAuth 流程、挂载与周期刷新
- 应用编排 [Container: internal/app] 配置校验、依赖组装、OAuth 流程、挂载与周期刷新 —Mount / Wait / Unmount→ FUSE 挂载 [Container: go-fuse/v2 + FUSE 3] 容器内挂载 /mnt/baidu，向 DSM 传播
- 应用编排 [Container: internal/app] 配置校验、依赖组装、OAuth 流程、挂载与周期刷新 —读写凭证、元数据和缓存→ 持久化数据卷 [Container: /data] token.json、meta.db、stream-cache
- 应用编排 [Container: internal/app] 配置校验、依赖组装、OAuth 流程、挂载与周期刷新 —未授权时启动本地回调→ OAuth 回调端口 [Container: HTTP :8765] 接收百度授权码回调
- FUSE 挂载 [Container: go-fuse/v2 + FUSE 3] 容器内挂载 /mnt/baidu，向 DSM 传播 —rshared 挂载传播→ DSM 宿主机挂载点 [Container: bind propagation: rshared] /volume2/baidu_videos，供 Emby 与播放器读取
- DSM 宿主机挂载点 [Container: bind propagation: rshared] /volume2/baidu_videos，供 Emby 与播放器读取 —本地文件路径读取→ 媒体消费者 [Software System: Emby / 网易爆米花 / Infuse] 通过 DSM 宿主机挂载点读取视频

## Page 3: 03 应用组件

### Components (13)

- 配置加载 [Component: internal/config] 解析 BAIDUDISKLINK_* 环境变量及默认值
- 认证管理 [Component: internal/auth] OAuth URL、回调换 Token、文件凭证存储与刷新
- 元数据仓库 [Component: internal/store + SQLite] 目录、文件属性、缓存过期与刷新
- 负缓存 [Component: internal/cache] 短期缓存不存在路径，降低重复远端请求
- FUSE 文件系统 [Component: internal/fs] Lookup、Getattr、Readdir、Open、Read、Release、可选删除
- 高码率流式引擎 [Component: internal/stream] Session、Handle、优先队列、Worker、缓冲水位、Hedge
- 两级分块缓存 [Container: Memory LRU + Disk LRU] 默认 320 MiB 内存 + 2 GiB 磁盘，缓存键含文件版本与块大小 _data store_
- 远端读取层 [Component: internal/remote] 精确 Range、旧 bench 并发读取、dlink 缓存和错误分类
- 百度 API 客户端 [Component: internal/baidu] List、Stat、Delete、GetDownloadLink、ReadRange、RefreshAuth
- 可观测性 [Component: internal/logging + 聚合日志] stream summary、FUSE 慢读摘要、benchmark 指标
- 诊断与基准工具 [Component: internal/app] bench-download、bench、bench-fuse、bench-stream、playback
- 应用编排 [Container: internal/app] 组装认证、存储、远端、流式引擎和 FUSE
- 百度开放平台与 CDN [Software System: OAuth / Netdisk API / HTTP Range] 认证、元数据、dlink 与文件分块内容

### Relations (10)

- 配置加载 [Component: internal/config] 解析 BAIDUDISKLINK_* 环境变量及默认值 —构造运行配置→ 应用编排 [Container: internal/app] 组装认证、存储、远端、流式引擎和 FUSE
- 应用编排 [Container: internal/app] 组装认证、存储、远端、流式引擎和 FUSE —加载或获取 Token→ 认证管理 [Component: internal/auth] OAuth URL、回调换 Token、文件凭证存储与刷新
- 应用编排 [Container: internal/app] 组装认证、存储、远端、流式引擎和 FUSE —打开 meta.db→ 元数据仓库 [Component: internal/store + SQLite] 目录、文件属性、缓存过期与刷新
- 应用编排 [Container: internal/app] 组装认证、存储、远端、流式引擎和 FUSE —创建并挂载文件系统→ FUSE 文件系统 [Component: internal/fs] Lookup、Getattr、Readdir、Open、Read、Release、可选删除
- FUSE 文件系统 [Component: internal/fs] Lookup、Getattr、Readdir、Open、Read、Release、可选删除 —目录和属性查询→ 元数据仓库 [Component: internal/store + SQLite] 目录、文件属性、缓存过期与刷新
- FUSE 文件系统 [Component: internal/fs] Lookup、Getattr、Readdir、Open、Read、Release、可选删除 —文件 ReadAt / Release→ 高码率流式引擎 [Component: internal/stream] Session、Handle、优先队列、Worker、缓冲水位、Hedge
- 高码率流式引擎 [Component: internal/stream] Session、Handle、优先队列、Worker、缓冲水位、Hedge —查询、发布、淘汰分块→ 两级分块缓存 [Container: Memory LRU + Disk LRU] 默认 320 MiB 内存 + 2 GiB 磁盘，缓存键含文件版本与块大小
- 高码率流式引擎 [Component: internal/stream] Session、Handle、优先队列、Worker、缓冲水位、Hedge —P0-P3 Range 任务→ 远端读取层 [Component: internal/remote] 精确 Range、旧 bench 并发读取、dlink 缓存和错误分类
- 远端读取层 [Component: internal/remote] 精确 Range、旧 bench 并发读取、dlink 缓存和错误分类 —下载与元数据接口→ 百度 API 客户端 [Component: internal/baidu] List、Stat、Delete、GetDownloadLink、ReadRange、RefreshAuth
- 高码率流式引擎 [Component: internal/stream] Session、Handle、优先队列、Worker、缓冲水位、Hedge —缓冲、重试、Hedge 摘要→ 可观测性 [Component: internal/logging + 聚合日志] stream summary、FUSE 慢读摘要、benchmark 指标

## Page 4: 04 播放读取链路

### Components (11)

- 播放器 / Emby 读取 [Software System: open + read + seek] 通过 DSM 本地路径发起小块、顺序或跳转读取
- FUSE 语义层 [Component: internal/fs] 路径映射、权限、文件大小边界和 errno
- 独立 Stream Handle [Component: Probe / Stream / Seek] 每次 Open 独立游标；多个句柄共享 Session 与缓存
- 优先级调度器 [Component: P0 / P1 / P2 / P3] 前台块、近端 64 MiB、目标 256 MiB、后向 32 MiB
- 下载 Worker 池 [Component: 默认 8 Workers] 逐块领取任务，不等待同批慢块；支持取消和公平配额
- 慢块竞速与重试 [Component: 动态 P95 阈值] P0 慢块最多竞速一次，连续慢块刷新 dlink
- 内存热缓存 [Container: 默认 320 MiB] 覆盖前向窗口、后向保留与调度余量 _data store_
- 磁盘临时缓存 [Container: 默认 2 GiB / 可关闭] .part 完整后原子改名为 .chunk，按 LRU 清理 _data store_
- 精确 Range 传输 [Component: internal/remote] 按 1 MiB 调度块调用底层 ReadRange
- dlink 与 HTTP 客户端 [Component: internal/baidu] 官方 API 获取直链，下载连接池执行 Range
- 百度 CDN 节点 [Software System: HTTP/1.1 Range] 返回目标字节范围

### Relations (14)

- 播放器 / Emby 读取 [Software System: open + read + seek] 通过 DSM 本地路径发起小块、顺序或跳转读取 —Read(offset, length)→ FUSE 语义层 [Component: internal/fs] 路径映射、权限、文件大小边界和 errno
- FUSE 语义层 [Component: internal/fs] 路径映射、权限、文件大小边界和 errno —Handle.ReadAt→ 独立 Stream Handle [Component: Probe / Stream / Seek] 每次 Open 独立游标；多个句柄共享 Session 与缓存
- 独立 Stream Handle [Component: Probe / Stream / Seek] 每次 Open 独立游标；多个句柄共享 Session 与缓存 —先查热缓存→ 内存热缓存 [Container: 默认 320 MiB] 覆盖前向窗口、后向保留与调度余量
- 独立 Stream Handle [Component: Probe / Stream / Seek] 每次 Open 独立游标；多个句柄共享 Session 与缓存 —识别模式并创建 epoch→ 优先级调度器 [Component: P0 / P1 / P2 / P3] 前台块、近端 64 MiB、目标 256 MiB、后向 32 MiB
- 优先级调度器 [Component: P0 / P1 / P2 / P3] 前台块、近端 64 MiB、目标 256 MiB、后向 32 MiB —按优先级持续派发→ 下载 Worker 池 [Component: 默认 8 Workers] 逐块领取任务，不等待同批慢块；支持取消和公平配额
- 下载 Worker 池 [Component: 默认 8 Workers] 逐块领取任务，不等待同批慢块；支持取消和公平配额 —前台慢块触发保护→ 慢块竞速与重试 [Component: 动态 P95 阈值] P0 慢块最多竞速一次，连续慢块刷新 dlink
- 下载 Worker 池 [Component: 默认 8 Workers] 逐块领取任务，不等待同批慢块；支持取消和公平配额 —下载缺失块→ 精确 Range 传输 [Component: internal/remote] 按 1 MiB 调度块调用底层 ReadRange
- 慢块竞速与重试 [Component: 动态 P95 阈值] P0 慢块最多竞速一次，连续慢块刷新 dlink —竞速请求 / 重试→ 精确 Range 传输 [Component: internal/remote] 按 1 MiB 调度块调用底层 ReadRange
- 精确 Range 传输 [Component: internal/remote] 按 1 MiB 调度块调用底层 ReadRange —ReadRange(fsid, offset)→ dlink 与 HTTP 客户端 [Component: internal/baidu] 官方 API 获取直链，下载连接池执行 Range
- 精确 Range 传输 [Component: internal/remote] 按 1 MiB 调度块调用底层 ReadRange —发布不可变分块→ 内存热缓存 [Container: 默认 320 MiB] 覆盖前向窗口、后向保留与调度余量
- 精确 Range 传输 [Component: internal/remote] 按 1 MiB 调度块调用底层 ReadRange —异步落盘→ 磁盘临时缓存 [Container: 默认 2 GiB / 可关闭] .part 完整后原子改名为 .chunk，按 LRU 清理
- 内存热缓存 [Container: 默认 320 MiB] 覆盖前向窗口、后向保留与调度余量 —唤醒前台读取→ 独立 Stream Handle [Component: Probe / Stream / Seek] 每次 Open 独立游标；多个句柄共享 Session 与缓存
- 独立 Stream Handle [Component: Probe / Stream / Seek] 每次 Open 独立游标；多个句柄共享 Session 与缓存 —返回请求字节→ FUSE 语义层 [Component: internal/fs] 路径映射、权限、文件大小边界和 errno
- FUSE 语义层 [Component: internal/fs] 路径映射、权限、文件大小边界和 errno —本地文件数据→ 播放器 / Emby 读取 [Software System: open + read + seek] 通过 DSM 本地路径发起小块、顺序或跳转读取

## Page 5: 05 元数据与认证链路

### Components (9)

- 管理员 [Person] 首次授权或凭证失效后重新授权
- 本地 OAuth Server [Component: internal/auth :8765] 生成授权 URL 并接收 code
- 百度授权页 / Token API [Software System: OAuth 2.0] 用户登录授权并返回 access_token / refresh_token
- Token 文件 [Container: /data/token.json] 本地保存凭证，远端不经第三方中转 _data store_
- 百度元数据客户端 [Component: internal/baidu] 使用 Token 调用 List、Stat、dlink 和删除接口
- SQLite 元数据缓存 [Container: /data/meta.db (WAL)] 目录树、FSID、大小、MTM、过期状态 _data store_
- FUSE 目录操作 [Component: Lookup / Readdir / Getattr] 优先查询 SQLite，缺失或过期时拉取远端
- 刷新机制 [Component: 懒刷新 + 每分钟根目录刷新] 序列化远端刷新并更新负缓存
- 百度网盘官方 API [Software System: Netdisk API] 目录列表、文件元数据和 dlink 接口

### Relations (11)

- 管理员 [Person] 首次授权或凭证失效后重新授权 —打开授权 URL→ 本地 OAuth Server [Component: internal/auth :8765] 生成授权 URL 并接收 code
- 本地 OAuth Server [Component: internal/auth :8765] 生成授权 URL 并接收 code —authorize / token exchange→ 百度授权页 / Token API [Software System: OAuth 2.0] 用户登录授权并返回 access_token / refresh_token
- 百度授权页 / Token API [Software System: OAuth 2.0] 用户登录授权并返回 access_token / refresh_token —callback code + Token→ 本地 OAuth Server [Component: internal/auth :8765] 生成授权 URL 并接收 code
- 本地 OAuth Server [Component: internal/auth :8765] 生成授权 URL 并接收 code —保存凭证→ Token 文件 [Container: /data/token.json] 本地保存凭证，远端不经第三方中转
- Token 文件 [Container: /data/token.json] 本地保存凭证，远端不经第三方中转 —启动时加载 Token→ 百度元数据客户端 [Component: internal/baidu] 使用 Token 调用 List、Stat、dlink 和删除接口
- 百度元数据客户端 [Component: internal/baidu] 使用 Token 调用 List、Stat、dlink 和删除接口 —刷新 Token 后回写→ Token 文件 [Container: /data/token.json] 本地保存凭证，远端不经第三方中转
- FUSE 目录操作 [Component: Lookup / Readdir / Getattr] 优先查询 SQLite，缺失或过期时拉取远端 —读取目录和属性→ SQLite 元数据缓存 [Container: /data/meta.db (WAL)] 目录树、FSID、大小、MTM、过期状态
- FUSE 目录操作 [Component: Lookup / Readdir / Getattr] 优先查询 SQLite，缺失或过期时拉取远端 —缓存缺失或过期→ 刷新机制 [Component: 懒刷新 + 每分钟根目录刷新] 序列化远端刷新并更新负缓存
- 刷新机制 [Component: 懒刷新 + 每分钟根目录刷新] 序列化远端刷新并更新负缓存 —List / Stat→ 百度元数据客户端 [Component: internal/baidu] 使用 Token 调用 List、Stat、dlink 和删除接口
- 百度元数据客户端 [Component: internal/baidu] 使用 Token 调用 List、Stat、dlink 和删除接口 —官方网盘 API→ 百度网盘官方 API [Software System: Netdisk API] 目录列表、文件元数据和 dlink 接口
- SQLite 元数据缓存 [Container: /data/meta.db (WAL)] 目录树、FSID、大小、MTM、过期状态 —返回本地目录视图→ FUSE 目录操作 [Component: Lookup / Readdir / Getattr] 优先查询 SQLite，缺失或过期时拉取远端
