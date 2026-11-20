# BaiduDiskLink 完整架构图

本目录保存 BaiduDiskLink 当前实现的多页 C4 架构图。

| 文件 | 说明 |
| --- | --- |
| `baidudisklink-architecture.drawio` | 可直接用 draw.io 打开的主架构图，共 5 页，支持页面间下钻 |
| `baidudisklink-c4.json` | 生成架构图的结构化 C4 源数据，便于后续维护和重新布局 |

架构图页面：

1. 系统上下文：用户、播放器、群晖 DSM、百度开放平台与 CDN。
2. 容器与部署：Docker Compose、CLI、应用编排、FUSE、数据卷和宿主机挂载点。
3. 应用组件：配置、认证、SQLite 元数据、FUSE、流式引擎、缓存、远端读取和百度客户端。
4. 播放读取链路：Probe/Stream/Seek、优先队列、Worker、Hedge、两级缓存和 HTTP Range。
5. 元数据与认证链路：OAuth、Token、目录刷新和 SQLite 本地视图。

图中带下钻链接的节点可以跳转到对应的下一层页面。
