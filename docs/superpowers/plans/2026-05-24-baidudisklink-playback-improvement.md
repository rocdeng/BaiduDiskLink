# BaiduDiskLink 播放链路改进计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 在保留现有 `dlink + FUSE` 挂载能力的前提下，优先做出一条更稳定的“播放直连层”，让 Emby / 播放器在大 MKV、快进、连续拖动时尽量绕开 FUSE 的抖动；随后再补齐 FUSE 对 DSM / SMB 场景的兼容性。

**总体思路：**

1. 播放路径优先走直连层，不把所有播放都压在 FUSE 文件系统上。
2. FUSE 继续承担“浏览 / 扫库 / 目录展示”职责。
3. 元数据仍然走 SQLite 缓存，`dlink` 与视频媒体信息要提前可用。
4. 先把播放体验做稳，再去修 FUSE 的 SMB 兼容和文件系统语义。

**技术栈：** Go、现有 `internal/baidu` / `internal/remote` / `internal/fs` / `internal/store`、SQLite、Docker、`go test`

---

## 第一阶段：播放直连层

### 任务 1：把播放路径和 FUSE 路径拆开

**目标：**

- 播放时不要默认从 FUSE 读文件。
- 优先使用视频媒体信息中的 `dlink`。
- 保留 FUSE 作为目录浏览和媒体库扫描入口。

**涉及文件：**

- 修改：`internal/app/app.go`
- 修改：`internal/remote/remote.go`
- 修改：`internal/fs/filesystem.go`
- 修改：`internal/config/config.go`
- 修改：`cmd/baidudisklink/main.go`
- 修改：`README.md`

- [ ] **步骤 1：先补测试**

建议先写一组测试，覆盖以下行为：

```go
func TestPlaybackPathCanBypassFuse(t *testing.T)
func TestVideoMediaInfoProvidesDlink(t *testing.T)
func TestDirectPlayUsesRangeReads(t *testing.T)
```

测试重点：

- 直播放路径可以不依赖挂载目录。
- 能从媒体信息里拿到 `dlink`。
- 播放数据读取仍然走 `Range`。

- [ ] **步骤 2：定义播放直连入口**

建议在 app 层增加一个明确的播放入口，语义上类似：

```go
GetVideoMediaInfo(remotePath string) (VideoMediaInfo, error)
```

它负责：

- 定位远端视频文件
- 获取媒体信息
- 返回 `dlink` 和播放所需元数据

- [ ] **步骤 3：增加直连播放输出**

优先让播放器或外部客户端可以直接拿到：

- `dlink`
- `duration`
- `size`
- `resolution`
- `formatName`

如果后续要接 HTTP 代理层，这里也要保留一个稳定的响应结构。

- [ ] **步骤 4：运行测试**

运行：

```bash
go test ./internal/app ./internal/remote ./internal/fs ./cmd/baidudisklink
```

- [ ] **步骤 5：提交**

```bash
git add internal/app/app.go internal/remote/remote.go internal/fs/filesystem.go internal/config/config.go cmd/baidudisklink/main.go README.md
git commit -m "feat: add direct playback path"
```

### 任务 2：把播放链路做成可测的直连模式

**目标：**

- 用直连层做真实播放验证。
- 不再只依赖 `bench` 和 FUSE 读取速度。

**建议新增能力：**

- 一个播放测速命令或播放探测命令
- 一个直接读取 `dlink` 的 HTTP Range 验证
- 一个可复用的播放代理上下文

- [ ] **步骤 1：补测试**

建议验证：

- 播放链路拿到的 `dlink` 可直接 `Range` 读取
- 快进时不会把 FUSE 读路径卡死
- 连续读大文件时有稳定的窗口缓存命中

- [ ] **步骤 2：实现最小播放代理层**

如果后续要做更完整的体验，可以加一层本地 HTTP 服务：

- 入参：视频文件标识或远端路径
- 输出：支持 `Range` 的视频流响应

这样播放器读的是本地 HTTP，而不是 SMB/FUSE。

- [ ] **步骤 3：验证大 MKV**

重点测：

- 起播时间
- 拖动响应
- 连续快进
- 卡顿恢复

---

## 第二阶段：FUSE 兼容性和 SMB 适配

### 任务 3：补齐 FUSE 文件系统语义

**目标：**

- 让 DSM 本机、SMB 挂载、文件管理器都能更稳地打开目录。

**建议方向：**

- `Access`
- `Statfs`
- `Flush`
- `Release`
- `Fsync`
- `Lock`
- `xattr`
- 打开/关闭句柄语义
- 更完整的权限和 ACL 映射

- [ ] **步骤 1：写兼容性测试**

建议先补：

- 普通目录浏览
- SMB 访问目录
- 打开文件
- 读取文件属性

- [ ] **步骤 2：补实现**

先从最容易影响 SMB 的部分开始，不要一次性重写整个文件系统。

- [ ] **步骤 3：回归测试**

验证：

- DSM 本机可见
- SMB 可打开
- 文件管理器可浏览
- Emby 可扫描

### 任务 4：优化 FUSE 读缓存和预读策略

**目标：**

- 继续降低 FUSE 的抖动。
- 让顺序播放、探测、跳读都更平稳。

**建议方向：**

- 按文件句柄做更细粒度缓存
- 进一步减少重复 `Range` 请求
- 根据播放器行为做头部预读
- 分析 P95/P99 延迟，而不是只看平均速度

---

## 验收标准

| 阶段 | 验收标准 |
|---|---|
| 播放直连层 | 能直接拿到 `dlink`，播放链路不必依赖 FUSE |
| 大文件播放 | 大 MKV 快进、拖动、连续播放明显更稳 |
| FUSE 浏览层 | DSM 中继续可见，扫描和浏览不受影响 |
| SMB 兼容性 | 群晖共享目录通过 SMB 打开不再受限 |

## 备注

- 当前优先级明确：**先做播放直连层**。
- FUSE 兼容性改进放后面做。
- 这份计划会随着播放直连方案的实现再细化。

