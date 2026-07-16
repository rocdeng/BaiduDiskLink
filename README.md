# BaiduDiskLink

BaiduDiskLink 是一个把百度网盘目录只读挂载成本地目录的工具，主要面向群晖 DSM + Emby 媒体库场景。

它的目标不是再提供一个 WebDAV 服务，而是让 Emby 直接看到一个本地路径，例如：

```text
/volume2/baidu_videos
```

你可以把百度网盘里的 `/Videos` 映射成 DSM 上的共享目录，Emby 直接读取这个共享目录即可。

## 适用场景

- 你希望在 DSM 上直接挂载百度网盘目录
- 你主要浏览和播放媒体文件，不需要写入百度网盘
- 你希望 Emby 看到的是本地文件夹，而不是 WebDAV 再二次映射
- 你有百度开放平台应用，可以配置 OAuth 回调地址

当前实现是只读挂载，不支持上传、删除、重命名。

如果明确开启删除开关，BaiduDiskLink 可以通过 FUSE 删除百度网盘中的文件或目录；默认仍然禁止删除。

## 技术简介

BaiduDiskLink 由 Go 编写，核心链路如下：

1. 通过百度开放平台 OAuth 获取并保存 `access_token` / `refresh_token`
2. 使用百度网盘官方接口列目录、取文件元数据和 `dlink`
3. 通过 `go-fuse` 在容器内挂载 FUSE 文件系统
4. 通过 Docker bind mount + `rshared` 把容器内挂载传播回 DSM 宿主机目录
5. 用 SQLite 缓存目录元数据，减少重复列目录
6. 对文件读取做 `Range` 读取、直链缓存、预读窗口和 FUSE read-ahead 优化

主要组件：

- Go 1.24
- Docker / docker-compose
- FUSE 3
- SQLite
- 百度网盘官方开放接口

## 目录映射关系

默认配置下有三层路径，容易混淆：

| 类型 | 示例 | 说明 |
| --- | --- | --- |
| 百度网盘目录 | `/Videos` | `BAIDUDISKLINK_REMOTE_ROOT_PATH` 指向的远端入口 |
| 容器内挂载点 | `/mnt/baidu` | 程序实际挂载 FUSE 的位置 |
| DSM 宿主机目录 | `/volume2/baidu_videos` | Emby 应该添加的媒体库路径 |

如果远端入口是 `/Videos`，本地挂载根目录会直接展示 `/Videos` 下的内容，不会再多一层 `Videos`。

## 配置

复制模板：

```bash
cp .env.example .env
```

最重要的配置项如下：

```dotenv
# 百度开放平台 App Key，也就是 Client ID / API Key，不是 Client Secret
BAIDUDISKLINK_CLIENT_ID=你的百度开放平台 App Key
BAIDUDISKLINK_CLIENT_SECRET=你的百度开放平台 Client Secret

# 必须和百度开放平台后台配置的授权回调地址完全一致
BAIDUDISKLINK_REDIRECT_URI=http://DSM-IP:8765/callback

# 给 DSM/Emby 看的宿主机挂载目录。这里是挂载点，不是源码目录。
# 强烈建议使用 DSM 共享目录，例如 /volume2/baidu_videos。
BAIDUDISKLINK_HOST_MOUNT_PATH=/volume2/baidu_videos

# 百度网盘里的入口目录。默认 /Videos。
BAIDUDISKLINK_REMOTE_ROOT_PATH=/Videos

# 可选：允许指定 DSM 组读取挂载内容。推荐填数字 GID，多个用逗号分隔。
BAIDUDISKLINK_FUSE_GROUP_NAME=101

# 可选：百度直链读取并发数。默认 1。
BAIDUDISKLINK_DOWNLOAD_CONCURRENCY=1

# 可选：每个分块读取大小。默认 4MiB。
BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE=4194304

# 可选：允许通过挂载目录删除百度网盘文件/目录。默认关闭。
BAIDUDISKLINK_ENABLE_DELETE=

# 可选：容器时区。默认 Asia/Shanghai，使日志显示 GMT+8 时间。
TZ=Asia/Shanghai
```

这两个参数建议先只在 `bench` 里试，确认效果稳定后再用于日常挂载。

默认容器内路径由 `docker-compose.yml` 固定为：

```text
BAIDUDISKLINK_MOUNT_PATH=/mnt/baidu
BAIDUDISKLINK_TOKEN_PATH=/data/token.json
BAIDUDISKLINK_META_DB_PATH=/data/meta.db
BAIDUDISKLINK_OAUTH_LISTEN_ADDR=0.0.0.0:8765
```

## DSM 部署

### 1. 准备目录

建议源码目录和最终挂载目录分开。挂载目录推荐用 DSM 共享目录，方便 Emby 套件权限识别。

```bash
cd /volume2/backup
git clone https://github.com/rocdeng/BaiduDiskLink.git
cd BaiduDiskLink

cp .env.example .env
mkdir -p data
mkdir -p /volume2/baidu_videos
```

然后在 DSM 控制面板里确认 `/volume2/baidu_videos` 对 Emby 服务用户有读取权限。

### 2. 配置百度开放平台

在百度开放平台应用后台配置授权回调地址，例如：

```text
http://DSM-IP:8765/callback
```

这个地址必须和 `.env` 里的 `BAIDUDISKLINK_REDIRECT_URI` 完全一致。百度后台修改回调地址后可能不会马上生效，遇到 `redirect_uri_mismatch` 可以等一段时间再试。

### 3. 编辑 `.env`

至少填写：

```dotenv
BAIDUDISKLINK_CLIENT_ID=你的百度开放平台 App Key
BAIDUDISKLINK_CLIENT_SECRET=你的百度开放平台 Client Secret
BAIDUDISKLINK_REDIRECT_URI=http://DSM-IP:8765/callback
BAIDUDISKLINK_HOST_MOUNT_PATH=/volume2/baidu_videos
BAIDUDISKLINK_REMOTE_ROOT_PATH=/Videos
```

如果要让 Emby 通过组权限访问，可以再填：

```dotenv
BAIDUDISKLINK_FUSE_GROUP_NAME=101
```

DSM 上可以通过下面命令查看组 GID：

```bash
cat /etc/group
```

### 4. 构建并启动

DSM 上通常是 `docker-compose`，不是 `docker compose`：

```bash
docker-compose build
docker-compose up -d
```

以后拉了新代码或新增命令后，也要重新 build：

```bash
docker-compose build
docker-compose up -d
```

### 5. 完成授权

查看日志：

```bash
docker-compose logs -f baidudisklink
```

首次启动会打印百度授权 URL。打开这个 URL，登录并授权。成功后 token 会保存到：

```text
./data/token.json
```

授权成功且挂载完成后，DSM 宿主机目录应该能看到百度网盘内容：

```bash
ls -la /volume2/baidu_videos
```

Emby 里添加媒体库时，使用宿主机路径：

```text
/volume2/baidu_videos
```

### 6. 直连播放代理

如果你想绕开 FUSE 播放大文件，可以直接启动本地播放代理：

```bash
docker-compose exec baidudisklink baidudisklink playback --path /Videos/test.zip --listen 127.0.0.1:8787
```

它会在本机起一个支持 `Range` 的 HTTP 服务，播放器可以把它当成本地视频源使用。这个入口是给播放链路做直连验证的，不需要再经过 WebDAV 或二次映射。

## 验证和测试

### DSM 验收

如果 DSM 有 `make`：

```bash
make dsm-verify
```

如果没有 `make`：

```bash
bash scripts/dsm-verify.sh
```

验收脚本会检查：

- 容器是否运行
- `/dev/fuse` 是否可见
- token 和 SQLite 元数据是否落盘
- FUSE 挂载表是否包含 `/mnt/baidu`
- 挂载目录是否能列出文件
- 是否能读取挂载文件的 1 字节

文件读取探针默认超时是 `20s`，可以调整：

```bash
BAIDUDISKLINK_VERIFY_READ_TIMEOUT=60s bash scripts/dsm-verify.sh
```

### 性能对比

`bench` 测百度官方接口 + `dlink` 的直读速度：

```bash
docker-compose exec baidudisklink baidudisklink bench --path /Videos/test.zip
```

默认读取 200MiB，适合更稳定地观察真实吞吐。

可以临时覆盖并发和分块大小：

```bash
docker-compose exec baidudisklink baidudisklink bench --path /Videos/test.zip --concurrency 4 --chunk-size 4194304
```

如果你想通过 `.env` 全局生效，也可以直接改：

```dotenv
BAIDUDISKLINK_DOWNLOAD_CONCURRENCY=4
BAIDUDISKLINK_DOWNLOAD_CHUNK_SIZE=4194304
```

`bench-fuse` 测 FUSE 挂载后的实际读取速度：

```bash
docker-compose exec baidudisklink baidudisklink bench-fuse --path /mnt/baidu/test.zip
```

如果需要诊断 Emby 拖动进度时是否真的触发了 FUSE 读取，可以临时打开读跟踪：

```dotenv
BAIDUDISKLINK_FUSE_TRACE_READS=1
```

然后重新构建并启动，再观察日志：

```bash
docker-compose logs -f baidudisklink
```

日志会打印每次 FUSE 读的文件、offset、请求长度、返回长度和读取策略。这个开关只建议排查问题时打开，正常使用时保持为空。

### 删除文件和目录

默认情况下挂载目录仍然是防误删的只读模式。确实需要删除百度网盘里的文件或目录时，在 `.env` 中开启：

```dotenv
BAIDUDISKLINK_ENABLE_DELETE=1
```

然后重建容器让环境变量生效：

```bash
docker-compose up -d --force-recreate
```

开启后，从 DSM/Emby/SMB 对挂载目录执行删除，会调用百度网盘删除接口，并同步清理本地 SQLite 元数据缓存。删除是真实作用到百度网盘的操作，建议只在确认需要时开启。

如果主容器没有运行，也可以用 `run --rm`，但主容器运行时更推荐 `exec`，避免新容器同时打开同一个 SQLite 元数据库。

```bash
docker-compose run --rm baidudisklink bench --path /Videos/test.zip
docker-compose run --rm baidudisklink bench-fuse --path /mnt/baidu/test.zip
```

### 开发测试

本地开发常用命令：

```bash
make test
make verify
make check
```

`make check` 会顺序执行单元测试和配置校验，日常改动优先跑 `make check`。`make verify` 会检查 Docker 配置、关键部署说明、入口 smoke 和脚本语法。

## 元数据刷新

启动后会先刷新 `BAIDUDISKLINK_REMOTE_ROOT_PATH` 指向的目录。之后每 1 分钟刷新已知目录树；手工浏览目录时也会按需刷新。

如果百度网盘里新增、删除或重命名文件，通常会在刷新后反映到本地挂载目录。文件和目录时间会优先使用百度网盘返回的修改时间，并缓存到 SQLite。

## Emby 权限建议

DSM 上普通 Linux 权限和 DSM 共享目录权限不是一回事。最稳的做法是：

1. 新建一个独立 DSM 共享目录作为挂载出口，例如 `/volume2/baidu_videos`
2. 在 DSM 控制面板里把这个共享目录的读取权限给 Emby 服务用户
3. 让 `BAIDUDISKLINK_HOST_MOUNT_PATH` 指向这个共享目录
4. 如果还需要 Linux 组权限，设置 `BAIDUDISKLINK_FUSE_GROUP_NAME`

不要把源码目录直接当成最终挂载目录，也不要把挂载点放进一个没有 DSM 共享目录语义的普通路径里。这样即使 `ls -la` 看起来权限没问题，DSM 套件用户也可能访问不到。

## 常见问题

### `docker compose up --build` 提示 unknown flag

DSM 上可能只有 `docker-compose`：

```bash
docker-compose build
docker-compose up -d
```

### `redirect_uri_mismatch`

确认百度开放平台后台的回调地址和 `.env` 里的 `BAIDUDISKLINK_REDIRECT_URI` 完全一致，包括协议、域名、端口和路径。百度后台修改回调地址后可能需要等待一段时间才生效。

### `is not a shared mount`

说明宿主机挂载点不在 shared 挂载树下。可以检查：

```bash
mount | grep ' on /volume2 '
```

通常需要让对应 volume 支持 shared 传播：

```bash
mount --make-rshared /volume2
```

如果不想改整个 volume，可以把项目和挂载出口放到 DSM 已经适配好的 Docker 目录或共享目录里。原则是：`BAIDUDISKLINK_HOST_MOUNT_PATH` 所在的宿主目录必须允许 mount propagation，容器内 FUSE 挂载才能回传给宿主机。

### `transport endpoint is not connected`

这是残留的 FUSE 挂载点。先停容器，再卸载宿主挂载目录：

```bash
docker-compose down
umount /volume2/baidu_videos
```

如果普通卸载失败：

```bash
umount -l /volume2/baidu_videos
```

清掉残留挂载后再重新启动容器。

### `database is locked (SQLITE_BUSY)`

通常是多个容器实例同时打开 `data/meta.db`。优先使用 `docker-compose exec` 在主容器里运行 `bench` / `bench-fuse`，不要频繁用 `run --rm` 起新容器。

当前程序已经给 SQLite 配置了 busy timeout 和 WAL，但仍建议避免多个实例同时操作同一份 `data`。

### 挂载目录为空

按顺序检查：

1. OAuth 是否完成，`./data/token.json` 是否存在
2. 容器日志是否有百度接口错误
3. `BAIDUDISKLINK_REMOTE_ROOT_PATH` 是否写对，例如 `/Videos`
4. 远端目录里是否确实有文件
5. 是否执行了 `docker-compose build` 使用最新镜像

### Emby 看不到目录

优先确认宿主挂载路径是 DSM 共享目录，并且 Emby 服务用户有该共享目录的读取权限。仅靠 Linux 的 `root administrators` 权限显示并不一定足够。

## 安全说明

- `data/token.json` 包含百度 OAuth token，不要提交到 Git，也不要公开分享
- `.env` 包含 App Key 和 Client Secret，不要公开分享
- 挂载内容只读，程序不会写入或删除百度网盘文件
- 建议只把需要给 Emby 使用的目录作为 `BAIDUDISKLINK_REMOTE_ROOT_PATH`，例如 `/Videos`
