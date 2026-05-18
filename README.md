# BaiduDiskLink

百度网盘只读挂载工具，面向 DSM 和 Emby 场景。

## 开发

```bash
make test
make verify
make check
```

`make verify` 会额外检查构建产物、Docker 配置和关键部署说明。
它还会做一次入口 smoke，逐项确认缺少关键配置时程序会快速失败并报错，并对脚本做 `bash -n` 语法检查。
`make check` 会顺序执行 `make test` 和 `make verify`。
日常改动优先跑 `make check`。

## 环境变量

- `BAIDUDISKLINK_MOUNT_PATH`
- `BAIDUDISKLINK_TOKEN_PATH`
- `BAIDUDISKLINK_META_DB_PATH`
- `BAIDUDISKLINK_CLIENT_ID`
- `BAIDUDISKLINK_CLIENT_SECRET`
- `BAIDUDISKLINK_REDIRECT_URI`
- `BAIDUDISKLINK_FUSE_GROUP_NAME`
- `BAIDUDISKLINK_OAUTH_SCOPE`
- `BAIDUDISKLINK_OAUTH_STATE`
- `BAIDUDISKLINK_OAUTH_LISTEN_ADDR`
- `BAIDUDISKLINK_AUTHORIZE_BASE_URL`
- `BAIDUDISKLINK_TOKEN_BASE_URL`
- `BAIDUDISKLINK_API_BASE_URL`

## Docker

### DSM 部署步骤

不要把 `make dsm-verify` 当成安装入口。它是部署、授权、挂载成功之后的验收脚本。首次部署建议按下面顺序来：

1. 将整个项目目录拷贝到 DSM，推荐放在带 shared 传播属性的目录，例如：

   ```bash
   /volume1/@docker/BaiduDiskLink
   ```

2. 进入项目目录，复制环境变量模板：

   ```bash
   cd /volume1/@docker/BaiduDiskLink
   cp .env.example .env
   mkdir -p data
   # 推荐新建一个 DSM 共享目录作为挂载出口，例如 /volume1/video/BaiduDiskLink
   mkdir -p /volume1/video/BaiduDiskLink
   ```

3. 编辑 `.env`，至少填写下面三项：

   ```dotenv
   # 百度开放平台 app key（Client ID / API Key），不是 Client Secret
   BAIDUDISKLINK_CLIENT_ID=你的百度开放平台 App Key
   BAIDUDISKLINK_CLIENT_SECRET=你的百度开放平台 Client Secret
   BAIDUDISKLINK_REDIRECT_URI=http://DSM-IP:8765/callback
   # 如果要给多个 DSM 组读，逗号分隔。优先推荐直接填数字 GID。
   # 例如：1024,1030
   BAIDUDISKLINK_FUSE_GROUP_NAME=1024,1030
   # 给 DSM/Emby 看的宿主机挂载目录。这里是挂载点，不是源码目录。
   # 默认 ./mnt 表示项目根目录下的 mnt 目录。
   # 推荐新建一个 DSM 共享目录作为挂载出口，不要和项目源码目录混在一起。
   BAIDUDISKLINK_HOST_MOUNT_PATH=/volume1/video/BaiduDiskLink
   ```

   这个变量必须通过 `docker-compose.yml` 传进容器才会生效，已经在仓库里配好。

4. 在百度开放平台应用后台，把授权回调地址配置成同一个地址：

   ```text
   http://DSM-IP:8765/callback
   ```

5. 构建并启动容器。DSM 上这套环境通常使用 `docker-compose`，不是 `docker compose`：

   ```bash
   docker-compose build
   docker-compose up -d
   ```

6. 首次启动时查看容器日志，打开日志里打印的百度授权 URL，按页面提示登录并授权。授权成功后，程序会通过本地回调拿到 token，并保存到：

   ```text
   ./data/token.json
   ```

   查看日志命令：

   ```bash
   docker-compose logs -f baidudisklink
   ```

7. 授权完成且挂载目录能看到百度网盘文件后，在 Emby 里添加 DSM 宿主机上的挂载目录：

   ```text
   /volume1/video/BaiduDiskLink
   ```

8. 最后再运行 DSM 验收脚本：

   ```bash
   make dsm-verify
   ```

如果 `make dsm-verify` 失败，优先看输出里的 `DSM verification failed checks:` 和最后一个 `checking:` 项。常见原因包括容器未运行、OAuth 未完成、`token.json` 不存在、`/dev/fuse` 权限不足、挂载传播未生效，或者百度网盘目录里没有可读取的文件。

如果启动时提示 `is not a shared mount`，说明 DSM 宿主机上的挂载点还不是 shared。可以先检查：

```bash
findmnt -o TARGET,PROPAGATION /volume1 /volume1/docker
```

通常需要把宿主挂载点改成 shared，然后再启动容器：

```bash
mount --make-rshared /volume1
```

如果你的 DSM 环境不接受对 `/volume1` 直接改 shared，可以改为对更小的实际挂载点处理，但原则一样：`./mnt` 所在的宿主目录必须在 shared 挂载树下，容器内的 FUSE 挂载才能回传给 DSM 和 Emby。

如果重建容器时提示 `transport endpoint is not connected`，通常是宿主机上的挂载目录残留了一个断开的 FUSE 挂载。先把容器停掉，再卸载这个目录：

```bash
docker-compose down
umount /volume1/video/BaiduDiskLink
```

如果普通卸载失败，再试惰性卸载：

```bash
umount -l /volume1/video/BaiduDiskLink
```

清掉残留挂载后再重新启动容器即可。
如果之后还要重新 `build`，先确认 `mnt` 已经卸载干净，否则 Docker 在打包上下文时会再次碰到这个目录。

### 快速启动

```bash
cp .env.example .env
# 编辑 .env，填入百度开放平台应用信息和 DSM 回调地址
docker-compose build
docker-compose up -d
```

容器默认开启 `/dev/fuse`，并暴露 OAuth 回调端口 `8765`。容器内挂载点是 `/mnt/baidu`，元数据与 token 保存在 `/data`。
`BAIDUDISKLINK_HOST_MOUNT_PATH` 是宿主机侧的挂载点路径，不是源码目录；默认值 `./mnt` 代表项目根目录下的 `mnt/`。它会采用 `rshared` 传播。DSM 上强烈建议新建一个普通共享目录，例如 `/volume1/video/BaiduDiskLink`，专门拿来做挂载出口，不要放在 `/volume1/@docker` 下面给 Emby 读，也不要和项目源码目录混用。你这次遇到的情况也说明了这一点：非共享目录即使 Linux 权限看着没问题，DSM 套件用户也可能还是访问不到。

DSM 场景建议：

1. 容器开启 `privileged` 并挂载 `/dev/fuse`
2. 将百度开放平台回调地址配置为 `http://<DSM-IP>:8765/callback`
3. 如果不显式设置 `BAIDUDISKLINK_OAUTH_LISTEN_ADDR`，程序会根据回调地址自动监听 `0.0.0.0:8765`
4. 挂载路径留给 Emby 直接读取即可，不再经过 WebDAV

DSM 实机验收：

```bash
make dsm-verify
```

不设 `BAIDUDISKLINK_CONTAINER` 时默认检查名为 `baidudisklink` 的容器。
`make dsm-verify` 会展开并执行 `scripts/dsm-verify.sh`。
脚本依赖 DSM 上可用的 `docker`、`find`、`dd` 和 `timeout` 命令。
脚本开头会列出宿主前提、容器前提和期望结果，方便先核对环境再看探针。

如果 DSM 上没有 `make`，可以直接执行：

```bash
bash scripts/dsm-verify.sh
```

这条检查会确认容器运行、`/dev/fuse` 可见、挂载目录存在、token 与元数据数据库已经落盘，列出挂载目录中的前几项内容，确认目录里至少有一个文件，然后读取第一个挂载文件的 1 字节来触发按需读取路径。
如果失败，脚本退出时也会打印 `DSM verification summary` 和失败项列表，再看输出里最后一个 `checking:` 项即可定位卡在哪一步。
文件读取探针默认超时是 `20s`，可通过 `BAIDUDISKLINK_VERIFY_READ_TIMEOUT` 调整。

目录会在挂载后先做一次全量刷新，之后每 1 分钟后台再刷新一轮已知目录树；你手工浏览目录时也会按需再刷新一次。百度网盘里对目录做重命名、删除或新增后，通常最晚 1 分钟左右会在本地反映出来。

### 给 Emby 开放读取

如果你希望 DSM 上的 Emby 服务直接读取挂载目录，不想手工逐个加 ACL，推荐把 `BAIDUDISKLINK_FUSE_GROUP_NAME` 设成对应的 DSM 组 GID，多个就用逗号分隔，例如 `1024,1030`。

这会让 FUSE 挂载对该组可见，同时保持网盘内容只读。实际效果是：

1. Emby 只要属于这些 GID 对应的组之一，就能浏览媒体库
2. 其他普通用户不会因为这个挂载自动获得访问权
3. 这仍然不是“完全无权限配置”，DSM 侧至少要保证 Emby 进程属于这个组

如果你已经有一个现成的 Emby 账号/服务用户，通常只要把它加入 `embysvr` 组即可，不需要给每个媒体目录单独补权限。

### 为什么推荐共享目录

在 DSM 上，普通 Linux 权限和 DSM 共享目录权限不是一回事。实际部署里，最稳的做法是：

1. 先新建一个独立共享目录，专门作为挂载出口
2. 把这个共享目录的权限给到 `embysvr` 或对应服务用户
3. 让 `BAIDUDISKLINK_HOST_MOUNT_PATH` 指向这个共享目录

这样 Emby 和 DSM 自己的权限模型都能正常识别，少踩很多坑。不要把源码目录直接当成最终挂载目录，也不要把挂载点挂进一个没有共享语义的普通路径里。
