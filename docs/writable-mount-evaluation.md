# FUSE 挂载目录可写方案评估

> 生成日期：2026-07-27
> 评估对象：BaiduDiskLink 当前只读 + 可选删除的 FUSE 挂载，扩展为完整可写（上传/创建目录/重命名/移动/截断）的可行性与方案选择。

---

## 一、现状基线

### 1.1 当前能力边界

| FUSE 操作 | 实现情况 | 底层调用 |
|---|---|---|
| `Lookup` / `Readdir` / `Getattr` | ✅ 只读 | `baidu.List` / `baidu.Stat` |
| `Read` / `Open` | ✅ 只读 | `stream.Manager` 或 `remote.ReadRange` |
| `Unlink` / `Rmdir` | ⚠️ 开关型（`enableDelete`） | `baidu.Delete`（`opera=delete`） |
| `Write` / `Create` | ❌ 未实现 | — |
| `Mkdir` | ❌ 未实现 | — |
| `Rename` | ❌ 未实现 | — |
| `Setattr`（truncate/chmod/time） | ❌ 未实现 | — |
| `Fsync` | ❌ 未实现 | — |

### 1.2 百度网盘开放 API 写入能力（基于已知接口约束）

百度网盘开放平台提供以下写入相关接口（均为 `xpan` REST 风格，本项目已封装了 `List/Stat/Delete/ReadRange`，新增写入接口是对称扩展）：

| 操作 | 接口 | 关键约束 |
|---|---|---|
| **分片上传** | `precreate` → `superfile2`（循环）→ `create` | 三步式：预创建 → 分片上传 → 创建文件 |
| 分片大小 | — | **固定 4 MiB**（`block_list` 里每个分片 MD5），与现有 stream 的 1 MiB chunk 不兼容 |
| 秒传 | `precreate` 返回 `return_type=2` | 依赖全文件 MD5 + size，命中则跳过上传 |
| 创建目录 | `filemanager?opera=create_dir` 或 `file?method=create_dir` | 父目录必须存在 |
| 移动/重命名 | `filemanager?opera=move` | `filelist` 指定源+目标路径 |
| 复制 | `filemanager?opera=copy` | 异步，返回 taskid |
| 删除 | `filemanager?opera=delete` | ✅ 已实现 |

### 1.3 本项目架构契合度

**已具备、可复用：**
- `baidu.Client` 接口 + `APIClient` 封装：新增 `Upload/Mkdir/Move/Rename` 方法是对称扩展，`getJSON`/`doJSON`/token 处理可复用。
- `store.Entry` schema 已有 `path/name/size/is_dir/mtime/md5`，写入后更新 SQLite 元数据即可（`UpsertEntry` 已是 upsert）。
- `remote.Reader` 的 dlink 缓存、`InvalidateDownloadLink` 机制：写入/截断后失效旧 dlink。
- `Filesystem.deleteChild` 已建立的"远程 API → 本地元数据同步 → negative cache"闭环模式，可推广到所有写操作。

**缺失、需新建：**
- 写入接口的 FUSE handler（`NodeWriter` 等 7 个接口，go-fuse/v2 v2.8.0 全部支持）。
- 4 MiB 分片上传管线（precreate/superfile2/create），与现有 1 MiB stream chunk 是两套独立逻辑。
- 写入缓冲与 flush 语义（FUSE `Write` 是写内核缓冲，`Fsync`/`Release` 时刷到远端）。
- 冲突检测与并发写协调。

---

## 二、核心难点分析

### 2.1 百度上传是"分片整文件"模型，与 FUSE"流式追加写"语义不匹配 ⭐ 最关键

这是整个方案最根本的矛盾。

**FUSE 写语义**：应用 `open(O_WRONLY) → write(offset=0, 4KB) → write(offset=4096, 4KB) → ... → fsync/close`，每次 `write` 是随机偏移的增量提交，内核 Page Cache 累积。

**百度上传语义**：
1. `precreate`：提交**全文件** size + **全文件** MD5 + 各 4 MiB 分片 MD5 列表 → 返回 uploadid。
2. `superfile2`：按分片顺序上传，每个分片 4 MiB，带 `partseq`。
3. `create`：提交 uploadid + block_list 完成文件创建。

矛盾点：

| 维度 | FUSE | 百度 |
|---|---|---|
| 何时知道文件大小 | 不确定（write 到任意 offset，可能 sparse） | precreate **必须**提前知道 |
| 何时知道 MD5 | 写完才能算 | precreate **必须**提前知道全文件 MD5 + 分片 MD5 |
| 写入顺序 | 任意 offset（可回写、可 sparse） | 分片**严格顺序**上传 |
| 中间可读 | 可（已写部分对后续 read 可见） | 上传完成前文件不存在 |
| 截断 | 可 truncate 到任意长度 | 不支持，需重新上传 |

**结论**：FUSE 的 write 不能"边写边传百度"，必须在本地攒成完整文件（或至少攒到能算 MD5 的程度），在 `Fsync`/`Release` 时一次性走 precreate→superfile2→create。

### 2.2 本地暂存空间与 DSM 磁盘压力

边写边攒意味着需要本地缓冲区：

- **方案 A：完整落盘缓冲**。`Write` 写入本地临时文件（如 `/data/wbuf/{fsid-or-uuid}.tmp`），`Release` 时算 MD5 + 切 4 MiB 分片 + 上传 + 删临时文件。
  - 优点：实现简单，MD5/size 在 Release 时自然确定，任意 offset 写都能支持。
  - 缺点：**需要等于写入文件大小的本地磁盘空间**。DSM 上挂载目录如果用户用 Emby 刮削元数据写小文件（KB 级）无压力；但如果用户往挂载点 cp 一个 10 GB 文件，`/data` 要吃 10 GB。`/data` 通常在 DSM 系统盘，空间敏感。
- **方案 B：内存缓冲 + 阈值落盘**。小于阈值（如 64 MiB）走内存，超过落盘。复杂度高，收益有限。

**推荐 A**，并加配置项 `BAIDUDISKLINK_WRITE_BUFFER_DIR`（默认 `/data/wbuf`）+ `BAIDUDISKLINK_WRITE_MAX_SIZE`（单文件上限，默认 0=不限，建议用户按 `/data` 余量设）。

### 2.3 元数据一致性：本地 SQLite 与远端的同步窗口

写入成功后，本地元数据更新策略：

- **Upload 成功**：调 `baidu.Stat(newPath)` 拿到 fsid/size/mtime/md5 → `store.UpsertEntry`。若失败则 `ExpirePath(parent)` 让下次访问强制刷新。
- **Mkdir 成功**：`store.UpsertEntry`（is_dir=1），但**百度的目录 fsid 是服务端生成**，本地无法预测，必须 `baidu.List(parent)` 重新拉或 `Stat(newPath)`。
- **Rename/Move 成功**：旧路径 `store.DeletePath` + 新路径 `Stat → UpsertEntry`。注意百度的 move 可能跨目录，新 path 要正确计算。
- **负缓存失效**：任何创建操作成功后必须清除父目录的 negative cache 中相关 key，否则刚创建的文件 Lookup 会误判 ENOENT。

现有 `deleteChild` 已建立这套闭环（`remote.Delete → store.DeletePath → markMissing`），写操作可以照搬模式。

### 2.4 并发写与冲突

- FUSE 允许多个 fd 同时打开同一文件写。百度没有文件级锁。
- 需在 inode 层加 `sync.Mutex` 串行化同文件的 `Write`/`Fsync`/`Release`，或按"最后 Release 生效"语义。
- 跨进程：同一挂载点被 Emby + 用户 `cp` 同时写同一文件极少见，可文档化"不支持并发写同一文件"。

### 2.5 dlink / stream 缓存失效

写入/截断后，旧 dlink 指向的旧内容已失效：
- 上传新文件：新 fsid，无旧 dlink 问题。
- **截断现有文件**：百度无截断 API，只能"删旧 + 传新"，fsid 变化，`InvalidateDownloadLink(oldFsid)` + `store.DeletePath(old) + UpsertEntry(new)`。
- stream 的 chunkStore key 含 `{fsid}-{size}-{mtm}`，size/mtm 变化后旧 chunk 自然失效（key 不匹配），无需特殊处理。

### 2.6 错误恢复与残留清理

- 上传中途失败（网络/超时）：precreate 已创建占位，需 `opera=delete` 清理半成品，否则百度网盘里会留一个损坏文件。
- 本地临时文件：`Release` 上传失败时是否保留临时文件供重试？建议保留并 log 路径，加 `baidudisklink wbuf-recover` 子命令手动重试。
- 进程崩溃：启动时扫 `/data/wbuf/*.tmp`，可恢复未完成的 Release 阶段上传。

---

## 三、方案分级建议

按工程量与风险递增，给出三档方案。

### 方案 1：最小可写（Mkdir + Rename + 小文件上传）—— 推荐先做

| 项 | 实现 | 难度 |
|---|---|---|
| `Mkdir` | `baidu.CreateDir` → store upsert | 低 |
| `Rename`（同目录改名） | `baidu.Move([old→new])` → store DeletePath+Upsert | 低 |
| `Create` + `Write` + `Fsync`（小文件，≤阈值如 256 MiB） | 本地缓冲 → precreate→superfile2→create → store upsert | 中 |
| `Unlink`/`Rmdir` | 已有 | — |
| 截断/大文件/跨目录移动 | 返回 `ENOTSUP` 或 `EPERM` | — |

**适用场景**：Emby 刮削元数据（写 .nfo/.jpg 小文件）、用户手动整理目录结构、重命名。**覆盖 90% 媒体库场景的真实写入需求**，且不碰最难的截断/大文件随机写。

**工程量**：约 600-800 行（baidu 上传 ~250 行 + fs 写 handler ~200 行 + 本地缓冲管理 ~150 行 + 测试）。

### 方案 2：完整可写（含大文件上传 + 截断）—— 谨慎评估

在方案 1 基础上增加：
- 大文件上传（完整本地落盘缓冲，无 size 上限）。
- `Setattr` 截断：删旧 + 传新（语义损失：截断=重新上传整个文件，性能差）。
- 跨目录 Move。

**风险**：
- `/data` 磁盘压力（等于最大单文件大小）。
- 截断=重传，用户在播放器里"边播边录"式写入会极其缓慢。
- 完整 MD5 计算需要读完全部缓冲，大文件 Release 延迟高。

**工程量**：方案 1 + 约 300-400 行（大文件路径、截断重传、磁盘配额检查）。

### 方案 3：不做 FUSE 可写，改 HTTP 上传接口 —— 保守替代

不实现 FUSE 写接口，而是新增 `baidudisklink upload --local /path --remote /Videos/x.mkv` 子命令 + 可选 HTTP 上传 API。理由：
- FUSE 写语义与百度上传模型天然错配，强行对齐体验差（截断重传、Release 卡顿、本地磁盘吃紧）。
- HTTP/CLI 上传路径与百度"分片整文件"模型完全契合，无语义损耗。
- 用户真正需要"写"的场景（往网盘传新文件）用 CLI 更可控。

**适用场景**：用户主要需求是"把本地文件传到网盘"而非"在挂载点里像本地盘一样操作"。

---

## 四、推荐方案与实施计划

### 4.1 推荐：**方案 1（最小可写）为默认目标，方案 3 作为补充**

理由：

1. **媒体库场景的写需求集中在小文件元数据 + 目录整理**，方案 1 精准覆盖，不碰高风险的大文件/截断。
2. **大文件上传走 CLI**（方案 3 的 `upload` 子命令）更符合百度上传模型，避免 FUSE 写语义的尴尬。
3. **删除已有先例**（`enableDelete` 开关），写入用同样的开关模式（`enableWrite`）用户心智模型一致。

### 4.2 配置开关设计

```dotenv
# 新增配置项
BAIDUDISKLINK_ENABLE_WRITE=              # 总开关，默认关闭（与 enableDelete 同级）
BAIDUDISKLINK_WRITE_BUFFER_DIR=/data/wbuf # 写入本地缓冲目录
BAIDUDISKLINK_WRITE_MAX_SIZE=268435456    # 单文件上传上限，默认 256 MiB，超过返回 ENOSPC/EFBIG
BAIDUDISKLINK_WRITE_CHUNK_SIZE=4194304    # 上传分片大小，必须 4 MiB（百度固定），留配置便于未来
```

### 4.3 实施步骤（方案 1）

| 阶段 | 任务 | 依赖 | 验收 |
|---|---|---|---|
| 1 | `baidu.Client` 新增 `CreateDir/Move/Upload(precreate+superfile2+create)` 方法 + 单测（httptest mock） | 无 | `go test ./internal/baidu/...` 通过 |
| 2 | `internal/wbuf` 新建：本地缓冲文件管理（Write 累积、MD5 计算、4 MiB 分片切分、崩溃恢复扫 .tmp） | 阶段1 | 单测覆盖边界 |
| 3 | `fs.Filesystem` 实现 `NodeMkdirer/NodeRenamer`（简单路径） | 阶段1 | 跑 `make check` |
| 4 | `fs.Filesystem` 实现 `NodeWriter/NodeOpener(NodeOpener 返回可写 handle)/NodeFsyncer`，`Release` 触发上传 | 阶段2 | 集成测试：cp 小文件进挂载点 → 百度端可见 |
| 5 | 元数据同步：上传/创建成功后 `store.UpsertEntry` + 失效 negative cache + `ExpirePath(parent)` | 阶段3,4 | 写入后 `ls` 立即看到 |
| 6 | `app.New` 注入新配置，`mountAndWait` 设置 `enableWrite` | 阶段4 | docker-compose 跑通 |
| 7 | 崩溃恢复：启动扫 `/data/wbuf/*.tmp`，log 提示 + `wbuf-recover` 子命令 | 阶段2 | kill 后重启不丢临时文件 |
| 8 | 文档：README 加可写章节 + 风险说明 | 全部 | — |

### 4.4 风险登记表

| 风险 | 等级 | 缓解 |
|---|---|---|
| 本地磁盘吃紧（`/data` 余量 < 上传文件） | 高 | `WRITE_MAX_SIZE` 默认 256 MiB + 启动检查 `wbuf` 目录可用空间 |
| 上传中途失败留半成品 | 中 | Release 失败时 `opera=delete` 清理 + 保留 .tmp 供重试 |
| 截断/随机写体验差 | 中 | 方案 1 不支持截断；随机写按 offset 累积到缓冲，Release 时整体上传 |
| 并发写同一文件 | 低 | inode 级 mutex 串行化，文档化"不支持并发写" |
| 秒传误命中（本地算错 MD5） | 低 | 复用百度 `precreate` 的 return_type 判断，命中则跳过 superfile2 |
| 写入触发目录刷新风暴 | 低 | 写入成功只 `ExpirePath(父目录)`，不触发 `RefreshAll` |
| Emby 刮削大量小文件并发写 | 中 | 上传 worker 限并发（如 4），避免触发百度 QPS 限制 |

---

## 五、关键决策点（需老公拍板）

1. **目标范围**：方案 1（小文件 + 目录整理）够用，还是要方案 2（含大文件上传）？
2. **大文件上传出口**：FUSE 写入是否设硬上限（`WRITE_MAX_SIZE`），超过强制走 CLI `upload` 子命令？
3. **截断语义**：方案 1 不支持 `truncate`，应用调用 `ftruncate` 时返回 `ENOTSUP`——Emby 写 .nfo 时是否会 ftruncate？需实测。如果会，要么支持"截断=重传"（方案 2），要么文档化限制。
4. **缓冲目录**：`/data/wbuf` 是否合适？DSM 上 `/data` 映射到 `./data`，和 token/meta.db 同盘，需确认余量。

> 我的默认推荐：**方案 1 + 方案 3 并行**。FUSE 侧只做小文件和目录操作，大文件上传独立成 `baidudisklink upload` 子命令。这样 FUSE 写语义的尴尬（截断、大文件）被隔离到 CLI，FUSE 只承担它擅长的"像本地盘一样整理目录和写元数据"。
