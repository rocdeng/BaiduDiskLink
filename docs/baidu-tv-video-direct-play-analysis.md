# 百度网盘 TV 版视频直连播放分析

本文基于反编译后的 APK 代码做静态分析，目标是说明：

1. 它是怎么拿到视频播放信息的。
2. 它为什么能直接播放百度网盘里的视频原文件。
3. 这条链路和普通网盘浏览/下载有什么区别。

## 结论先说

这套 TV 版并不是先把视频完整下载到本地再播，而是：

1. 先通过账号凭证请求“视频媒体信息”。
2. 服务端返回一个 `VideoMediaInfo`。
3. 其中包含关键字段 `dlink`。
4. 播放器直接拿 `dlink` 作为原始播放地址。
5. 后续播放过程中再通过 HTTP `Range` 分段读取视频流。

也就是说，它的核心不是“本地缓存整个文件”，而是“先拿直链，再按需读流”。

## 关键入口

### 1. 业务入口：`getVideoInfo`

入口在 `NetdiskSource$getVideoInfo$1`。

它会拿到一个 `VideoFile`，然后取出 `serverPath`，交给 `VideoService` 去查视频信息。

证据：

- `smali_classes4/com/baidu/netdisk/tv/core/model/NetdiskSource$getVideoInfo$1.smali`
- 这里有日志：`getVideoInfo-----start getVideoMediaInfo, serverPath: ...`

### 2. 服务入口：`VideoApi.getVideoMediaInfo`

证据在 `com/baidu/netdisk/tv/core/repository/job/_/_.smali`：

- 类标记是 `VideoApi`
- 方法名是 `getVideoMediaInfo`
- 参数只有两个：
  - `url`
  - `taskId`

代码里先构造 `Evidence` 对应的认证信息，再发起请求。

### 3. 请求执行：`GetVideoMediaInfoJob`

证据在 `com/baidu/netdisk/tv/core/repository/job/_.smali`：

- 日志名：`GetVideoMediaInfoJob`
- 它调用 `VideoApi.aR(url, taskId)`
- 成功后会解析成 `GetVideoMediaInfoResponse`

### 4. 响应解析：拿 `dlink`

证据在 `com/baidu/netdisk/tv/core/repository/job/__/_.smali`：

- 把网络响应内容反序列化成 `GetVideoMediaInfoResponse`
- 取 `response.videoMediaInfo`
- 再取 `videoMediaInfo.dlink`

代码里明确写了：

- `GetVideoMediaInfoParser parse`
- `content : ...`
- `videoMediaInfo.dlink`

### 5. 最终播放使用 `dlink`

证据在 `com/baidu/netdisk/tv/core/viewmodel/__.smali`：

- 日志：`meta接口获取到dlink，设置给playUrl?.originPlayUrl`

这说明播放器最终把 `dlink` 填进了播放 URL。

## 推断出的接口语义

从代码能直接确认的是：

- 它有一个“视频媒体信息”接口。
- 入参是视频资源 URL 和任务 ID。
- 返回体里有 `VideoMediaInfo`。
- 其中的 `dlink` 是播放核心。

从命名和链路看，这个接口的语义更像：

- “给我某个网盘视频文件的媒体信息”
- “返回可播放的原始下载/播放直链”

我没有在当前反编译结果里直接看到完整的 REST 路径字符串，所以**接口路径本身不在这份文档里做硬断言**。  
但它的行为已经足够明确：这就是百度 TV 版用于视频原文件直连播放的媒体信息接口。

## 为什么它能直连播放

原因其实很简单：

### 1. 它拿到的是 `dlink`

`dlink` 不是普通网页分享页，也不是一个中间跳转页，而是可直接用于 HTTP 拉取的播放地址。

### 2. 播放器支持分段读取

从代码里的 `Range` 痕迹可以看出，它会对媒体流做分段请求，而不是一次性整文件下载。

这就允许：

- 播放器边播边取
- 快进时跳到指定 offset
- 不需要完整落盘

### 3. 服务端返回的媒体信息里已经带播放属性

`VideoMediaInfo` 里除了 `dlink`，还有：

- `playModel`
- `resolution`
- `formatName`
- `duration`
- `size`
- `md5`
- `adTime`
- `adLTime`
- `thirdToken`

这说明它返回的是“播放器所需的媒体元数据包”，不是单纯文件列表。

## 为什么不是普通下载接口

普通下载一般只关心：

- 文件名
- 文件大小
- 文件 ID
- 下载地址

而 TV 版这里更像一个“播放器专用接口”：

- 先查媒体信息
- 再拿 `dlink`
- 再直接播放

它把“可播放性”放在服务端判断好了，所以 TV 端可以做得很像本地播放器。

## 和我们项目的对应关系

这份分析对我们当前项目有两个直接价值：

| 百度 TV 版做法 | 我们项目的对应思路 |
|---|---|
| 先拿媒体信息 | 先拿 `dlink` / 元数据 |
| 播放器直接用直链 | FUSE / bench 直接走远端读 |
| HTTP `Range` 分段读 | `ReadRange()` 分块拉取 |
| 只缓存必要元数据 | SQLite 缓存目录和媒体信息 |

## 需要注意的点

1. 这份分析是基于反编译代码，不是官方接口文档。
2. 当前能完全确认的是“请求链路”和“数据流向”，不是完整接口 URL 字符串。
3. 但对于理解“为什么能直连播放”已经足够。

## 关键证据索引

| 文件 | 证据 |
|---|---|
| `smali_classes4/com/baidu/netdisk/tv/core/model/NetdiskSource$getVideoInfo$1.smali` | `getVideoInfo-----start getVideoMediaInfo, serverPath:` |
| `smali_classes4/com/baidu/netdisk/tv/core/repository/_.smali` | `getVideoMediaInfo` 入口 |
| `smali_classes4/com/baidu/netdisk/tv/core/repository/job/_.smali` | `GetVideoMediaInfoJob`，调用 `VideoApi.aR(url, taskId)` |
| `smali_classes4/com/baidu/netdisk/tv/core/repository/job/__/_.smali` | JSON 解析，取 `videoMediaInfo.dlink` |
| `smali_classes4/com/baidu/netdisk/tv/core/viewmodel/__.smali` | `meta接口获取到dlink，设置给playUrl?.originPlayUrl` |

