# BaiduDiskLink P2 元数据与缓存索引优化设计

日期：2026-07-12
状态：用户授权设计后直接执行

## 目标

1. 目录刷新中的删除与插入保持原子性，避免中间空目录和失败后数据丢失。
2. 批量元数据写入使用单事务与复用语句，减少 SQLite 自动提交和 SQL 解析成本。
3. 读取缓存避免跨所有 FSID 线性扫描，降低多文件并发播放时的缓存查找成本。

## 非目标

- 不改变 SQLite 表结构和已有索引。
- 不把全部元数据复制到 Go 内存。
- 不实现目录增量 diff；远端列表仍作为权威快照替换直接子项。
- 不改变 64 MiB 缓存预算、Range 读取和 P1 预读行为。
- 不增加配置项或依赖。

## SQLite 写入

定义内部事务执行器接口，使 `UpsertEntry` 的 SQL 可同时用于 `*sql.DB` 与 `*sql.Tx`。批量路径开启事务、prepare 一次 upsert、循环绑定并执行、成功提交；任意失败回滚。

`ReplaceChildren` 在同一事务内：

1. 删除指定 `parent_fsid` 的直接子项；
2. 用 prepared upsert 逐项写入新快照；
3. 提交。

空列表也必须在事务中完成删除并提交。批量逐项执行避免动态拼接超长 SQL和 SQLite 参数上限。

`UpsertEntries` 与 `UpsertFromRemote` 复用同一批量事务 helper。`UpsertFromRemote` 只在 entry.Parent 为空时填入调用方 parent，不改变已有 parent。

## 缓存索引

保留现有 `map[cacheKey]cachedRead`、字节预算和 LRU 顺序；新增：

```go
cacheByFSID map[string]map[cacheKey]struct{}
```

读取只遍历目标 FSID 的键集合，再验证区间包含关系。写入、替换、驱逐和清空必须同步维护索引。LRU 命中仍提升到最新位置，缓存数据继续以不可变切片返回。

本次不引入区间树或排序树：64 MiB 预算下，同一文件窗口数量有限；按 FSID 分桶已消除多文件之间的无效扫描，复杂度与维护成本更合适。

## 错误与并发

- 事务 begin、prepare、exec、commit 任一失败均返回错误。
- defer rollback 作为未提交路径的兜底；提交成功后的 rollback 结果忽略。
- 缓存索引继续由 `Reader.mu` 保护，不引入新锁。
- 驱逐时必须从 `cached`、`cacheOrder`、`cacheByFSID` 同时移除；空 FSID 桶立即删除。

## 测试

- `ReplaceChildren` 正常替换、空列表删除、插入失败时保留旧子项。
- `UpsertEntries` 批量写入完整提交；中途约束失败不产生部分数据。
- `UpsertFromRemote` 正确填充空 Parent 且保持显式 Parent。
- 缓存查询不访问其他 FSID 桶；替换、LRU 驱逐和清空后索引无陈旧键。
- `go test -race` 无数据竞争。

## 验证

```bash
/usr/local/go/bin/go test -race ./internal/store ./internal/remote ./internal/fs
make check
```

真实 DSM 上仍需观察大型目录刷新耗时、SQLite WAL 写入量和多文件播放 CPU；单元测试不替代现场数据。
