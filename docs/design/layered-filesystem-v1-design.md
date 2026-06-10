# Drive9 Layer FS V1 Design

日期：2026-06-03

本文给出 Drive9 Layer FS V1 设计。V1 采用业界逐渐统一的基础方向：只读 base、单 writable overlay、copy-up、whiteout、显式 checkpoint/commit/rollback。本文暂不采用 DeltaFS/DeltaBox 的多 segment stack 过度优化；DeltaFS 思想可作为后续高频 checkpoint/rollback 的演进方向。

V1 的目标是：在 Drive9 上提供 agent/sandbox 友好的 layered filesystem，同时不破坏现有 Drive9 能力和性能。无 layer 时，现有 `/v1/fs`、FUSE、Git workspace、S3 multipart、semantic search、tenant fork 行为必须保持不变。

## 用户旅程

### 1. 开始一个 agent 工作会话

用户从现有 Drive9 路径创建 layer：

```bash
drive9 fs layer create :/repo --name fix-auth-bug --tag task=auth --tag env=dev --durability=restore-safe
```

返回：

```text
layer_id: lyr_abc
base: :/repo
status: active
durability: restore-safe
```

然后把这个 layer mount 出来：

```bash
drive9 mount :/repo ./repo --layer lyr_abc --profile=coding-agent
```

后续任何需要指定 layer 的地方，都可以使用以下任一引用：

```bash
drive9 mount :/repo ./repo --layer lyr_abc
drive9 mount :/repo ./repo --layer fix-auth-bug
drive9 mount :/repo ./repo --layer task=auth
drive9 mount :/repo ./repo --layer tag:task=auth
```

解析顺序为 `layer_id -> name -> tag`。如果 `name` 或 `tag` 命中多个 layer，命令必须报 conflict 并提示用户使用 layer_id 或添加更精确 tag；不能随机选择。

用户心智模型：

```text
:/repo 的 base 不变
agent 的修改先进 lyr_abc
```

### 2. Agent 正常读写

agent 或用户在 mount 中照常操作：

```bash
cd ./repo
vim pkg/server/auth.go
go test ./pkg/server
npm install
```

交互上不要求用户理解 Drive9 内部层级：

- 源码、文档、配置等 durable 文件进入 layer。
- `.git`、`node_modules`、build/cache 输出优先走现有 local-only overlay。
- 文件 `close`、`fsync`、checkpoint、unmount 会把 durable layer 推进到后端。
- 没有 layer 的普通 Drive9 mount 行为完全不变。

### 3. 查看状态和 diff

用户随时查看 layer 状态：

```bash
drive9 fs layer status lyr_abc
drive9 fs layer status fix-auth-bug
drive9 fs layer status task=auth
```

示例输出：

```text
Layer lyr_abc active
Base :/repo
Durable seq 42
Pending local writes 0

M pkg/server/auth.go
A docs/auth-flow.md
D pkg/server/legacy_token.go
L node_modules/        local-only, rebuildable
```

查看 diff：

```bash
drive9 fs layer diff lyr_abc
```

diff 应像 Git diff，但明确标出 base revision、layer revision、local-only 项。

### 4. 创建 restore checkpoint

在 sandbox 替换、长任务中间点、风险操作前，用户或 orchestrator 执行：

```bash
drive9 fs layer checkpoint lyr_abc --wait --label before-refactor
drive9 fs layer checkpoint fix-auth-bug --wait --label before-refactor
```

返回：

```text
checkpoint: cp_before_refactor
durable_seq: 57
restore_safe: true
```

含义：

- checkpoint 前所有 durable 文件修改都已写入 Drive9 后端。
- 新 sandbox 可以从这个 checkpoint 恢复。
- 未关闭、未 fsync、未 checkpoint 的 open dirty handle 不承诺跨 sandbox 恢复。

### 5. 跨 sandbox restore

新的 sandbox 启动后：

```bash
drive9 mount :/repo ./repo --layer lyr_abc --checkpoint cp_before_refactor --profile=coding-agent
drive9 mount :/repo ./repo --layer fix-auth-bug --checkpoint cp_before_refactor --profile=coding-agent
```

用户看到：

- layer 中新增、修改、删除的 durable 文件恢复。
- base 在 commit 前仍然不变。
- local-only 目录按策略重新生成或懒加载。
- Git workspace 继续使用已有 Git fast workspace restore 机制。

### 6. 完成后 commit 或 rollback

满意时提交：

```bash
drive9 fs layer commit lyr_abc
drive9 fs layer commit fix-auth-bug
```

commit 是 all-or-nothing。成功后：

```text
base :/repo updated
layer lyr_abc committed
```

不满意时回滚：

```bash
drive9 fs layer rollback lyr_abc
drive9 fs layer rollback task=auth
```

base 完全不变，layer 进入 `abandoned`，后续按 retention GC。

如果 base 被别人改过：

```text
conflict: pkg/server/auth.go
base revision changed: 12 -> 15
layer preserved for review
```

用户可以重新开 layer、手动合并，或 rollback。系统不能半提交。

## 设计原则

V1 采用基础 layered FS 模型：

```text
visible tree
  writable overlay
  immutable base
```

关键原则：

- 无 layer 时零行为变化。
- 有 layer 时 base 在 commit 前不可变。
- rollback 永远不修改 base。
- write hot path 优先本地，不把每个 `write(2)` 变成远端 round trip。
- checkpoint/close/fsync/unmount 是 restore-safe durability barrier。
- 后端 durable layer 是跨 sandbox restore 的权威来源。
- local-only overlay 是性能层和 rebuildable state，不自动进入 durable layer。
- Git workspace 保持专用实现，不强行并入通用 layer。

## 文件系统模型

V1 可见树由三层组成：

```text
visible tree
  local runtime overlay      # FUSE 热路径，本地 shadow/writeback/WAL
  durable fs layer           # Drive9 后端权威 overlay
  base Drive9 namespace      # 现有 file_nodes / inodes / contents / semantic
```

读取顺序：

1. 本地 dirty/pending data。
2. 后端 durable layer entry。
3. base Drive9 文件。

写入顺序：

1. `write(2)` 写本地 shadow/WAL。
2. `close`、`fsync`、checkpoint、unmount 推送到后端 layer。
3. `commit` 才应用到 base。

路径分类顺序：

1. `local-only` policy：`.git`、`node_modules`、build/cache 输出继续走现有本地 overlay。
2. `git_workspace`：现有 Git fast workspace 继续处理 clean tree + git overlay。
3. `fs_layer`：普通 Drive9 文件在 layer mount/API 下走通用 layer resolver。
4. `remote_persistent`：无 layer 或 layer miss 时走现有 base path。

## 数据模型

新增 tenant-local 表：

```text
fs_layers
fs_layer_entries
fs_layer_events
fs_layer_checkpoints
fs_layer_tags
```

### fs_layers

`fs_layers` 表示一次 agent session：

```text
layer_id
base_root_path
state = active | sealed | committed | abandoned | conflicted
durability_mode = restore-safe | write-through | local-fast
actor_id
durable_seq
created_at
updated_at
sealed_at
```

`name` 是可读引用，不要求全局唯一；解析时如果同名 layer 多于一个，返回 conflict。

### fs_layer_tags

`fs_layer_tags` 支持按业务语义引用 layer：

```text
layer_id
tag_key
tag_value
created_at
```

索引要求：

```text
PRIMARY KEY (layer_id, tag_key)
INDEX (tag_key, tag_value)
INDEX (tag_key)
```

命令和 API 中的 layer ref 统一支持：

```text
layer_id
name
tag:key=value
key=value
tag:key
```

其中 `key=value` 只有在没有同名 layer 时按 tag 解析；`tag:key` 表示 tag key 存在匹配。所有 tag/name 引用都必须解析为唯一 layer，否则返回 conflict。

### fs_layer_entries

`fs_layer_entries` 表示 overlay 变更：

```text
layer_id
path
path_hash
parent_path
parent_path_hash
name
op = upsert | whiteout | mkdir | symlink | chmod
kind = file | dir | symlink
base_inode_id
base_revision
storage_type
storage_ref
storage_ref_hash
content_blob
content_type
content_text
checksum_sha256
size_bytes
mode
entry_seq
created_at
updated_at
```

V1 存储边界：layer entry 内容当前以内联 `content_blob` 写入 metadata store，并受 server entry body limit 保护；`storage_ref` / 外置 blob layer content 是后续增强项，不作为 V1 性能或容量保证。

索引要求：

```text
PRIMARY KEY (layer_id, path_hash)
INDEX (layer_id, parent_path_hash)
INDEX (layer_id, entry_seq)
INDEX (layer_id, op)
```

`path_hash` 用于保持 TiDB/MySQL 长路径索引性能；`path` 原文仍是权威值，lookup 时需要 hash + path 双重校验。

### fs_layer_events

`fs_layer_events` 是 append-only audit log，用于 diff、审计、agent action 解释：

```text
event_id
layer_id
seq
actor_id
op
path
before_json
after_json
idempotency_key
created_at
```

### fs_layer_checkpoints

`fs_layer_checkpoints` 记录 durable restore point：

```text
checkpoint_id
layer_id
durable_seq
label
created_at
```

## Resolver 语义

### stat(path)

- layer 有 `whiteout`：返回 not found。
- layer 有 `upsert`、`mkdir`、`symlink`、`chmod`：返回 layer metadata。
- 否则查 base。

### readdir(dir)

- base children + layer children 合并。
- 同名 layer 覆盖 base。
- whiteout 隐藏 base child。
- local-only child 在 FUSE listing 中可见，但标记 rebuildable。

### read(path)

- local dirty data 优先。
- layer content 次之。
- base content 兜底。
- S3-backed layer content 继续使用 presign/read-plan 思路。

### write(path)

- 新文件直接写 layer。
- 修改 base 文件时记录 `base_inode_id` 和 `base_revision`，内容 copy-up 到 layer。
- 小文件 inline，大文件走现有 multipart/S3 通道。
- base 不变。

### delete(path)

- base 文件写 `whiteout`。
- layer-only 文件可删除 layer entry 或写 tombstone。
- V1 只保证文件和空目录删除；recursive delete 后续扩展。

### rename(old, new)

- V1 layer mount 对普通文件 rename 采用 copy-up：目标写 `upsert` entry，源路径写 `whiteout`。
- 如果源文件来自 base，rename 时会先从 base materialize 到目标 layer entry 和本地 shadow/pending；restore 不依赖 base 源路径仍存在。
- API 层的 `rename` entry 在 V1 仅允许文件 rename，commit 使用 no-replace 语义：target 已存在时返回 conflict，不覆盖 base target。
- directory rename 作为后续增强项，V1 在 API/FUSE 层都明确拒绝，避免 rollback 时需要递归目录树 snapshot。

## API / CLI

新增 API：

```text
POST /v1/fs-layers
GET  /v1/fs-layers
GET  /v1/fs-layers/{layer-ref}
GET  /v1/fs-layers/{layer-ref}/diff
GET  /v1/fs-layers/{layer-ref}/entries?path=/...
POST /v1/fs-layers/{layer-ref}/entries
POST /v1/fs-layers/{layer-ref}/checkpoint
POST /v1/fs-layers/{layer-ref}/commit
POST /v1/fs-layers/{layer-ref}/rollback
GET  /v1/fs-layer-checkpoints/{checkpoint-id}
```

V1 保持现有 `/v1/fs` API 代码路径不变。layer mount 通过显式
`/v1/fs-layers/{layer-ref}/entries` 写 layer entry；没有 `--layer` 或
layer API 调用时，旧客户端仍走原来的 `/v1/fs` 读写路径和性能路径。

```text
drive9 mount :/repo ./repo --layer <layer-ref>
```

CLI：

```bash
drive9 fs layer create :/repo --name task --durability=restore-safe
drive9 fs layer status <layer-ref>
drive9 fs layer diff <layer-ref>
drive9 fs layer checkpoint <layer-ref> --wait
drive9 fs layer commit <layer-ref>
drive9 fs layer rollback <layer-ref>
drive9 mount :/repo ./repo --layer <layer-ref>
```

FUSE mount options：

```text
MountOptions.LayerRef
MountOptions.CheckpointRef
```

`FlushAll` 必须 drain：

- open dirty handles
- layer checkpoint queue
- existing Git checkpoints
- existing writeback uploader
- existing commit queue

## Durability 策略

默认 `restore-safe`：

- `write(2)`：本地 durable WAL/shadow 后即可返回。
- `close`、`fsync`、checkpoint、unmount：必须等后端 layer durable。
- 跨 sandbox restore 只承诺 durable checkpoint 之前的数据。
- sandbox orchestrator 替换前必须执行 `checkpoint --wait`。

V1 接受并保存 `write-through` / `local-fast`，用于 API/CLI 兼容和后续调度策略扩展；当前 FUSE 写入实现统一采用 `restore-safe` 行为，不根据这两个模式改变 write/flush/checkpoint 路径。任何依赖更激进或更保守 durability 的调度器，在 V1 必须仍以显式 checkpoint 作为跨 sandbox restore 边界。

V1 restore-safe 边界：

- 已 `close`、已 `fsync`、已 checkpoint、已 unmount drain 的 durable 文件必须可跨 sandbox restore。
- 进程被杀时仍 open 且未 checkpoint 的 dirty handle 不承诺跨 sandbox restore；V1 的 `write-through` 只作为已记录的意图字段，尚不提供额外保证。

## Commit / Rollback

commit 流程：

1. 执行 `checkpoint --wait`。
2. seal layer。
3. 计算 final diff。
4. 校验每个 base path 的 revision。
5. 在 DB transaction 中应用到 base。
6. enqueue semantic tasks。
7. 旧 S3 refs 进入现有 GC。
8. layer 标记 `committed`。

rollback 流程：

1. layer 标记 `abandoned`。
2. visible tree 不再读取该 layer。
3. overlay content 按 retention 清理。
4. base 完全不变。

冲突处理：

- commit 前校验 `base_revision`。
- 如果 base revision 已变化，commit 返回 conflict list。
- layer 保留为 `conflicted`，用户可审查、手动合并或 rollback。
- 不能半提交。

## Search / Semantic

layer-aware search：

- base hits 必须被 layer whiteout/replacement 过滤。
- layer 新增/修改文件参与 `find` / `grep`。
- V1 可先支持 metadata/keyword search。
- vector/embedding 可在 V2 引入 layer-scoped semantic task 或 `fs_layer_semantic`。

关键约束：

- 如果 layer 删除或替换了 `/foo.txt`，base 中旧 `/foo.txt` 的 semantic hit 不能出现在 layer search 结果中。

## Compatibility

必须保持：

- 普通 `/v1/fs` 无 layer 行为不变。
- 现有 FUSE writeback/shadow 性能不回退。
- Git fast workspace 继续使用 `git_workspace_*`。
- tenant fork 继续作为重型分支能力，不被 layer 替代。
- S3 multipart、read redirect、batch-stat、batch-read-small 在 no-layer 下不增加开销。
- 现有 scoped FS authorization 不被绕开；layer header 必须进入 authorization model。

## Test Plan

核心测试：

- 无 layer 请求完全保持旧行为。
- layer upsert 覆盖 base read/stat/list。
- whiteout 隐藏 base。
- rollback 后 base 可见性恢复。
- commit 成功后 base 更新。
- commit conflict 保留 layer，不半提交。
- checkpoint 后新 sandbox restore 不丢 closed/fsynced 文件。
- local-only 目录不进入 durable layer。
- 大文件 layer write 在 V1 仍受 entry body limit 约束；外置 blob/S3 layer content 作为后续增强验证。
- layer-aware search 不返回被 whiteout/replacement 隐藏的 base hit。

性能测试：

- no-layer read/write/list benchmark 不增加额外 DB 查询。
- layer read miss 只增加 overlay lookup。
- layer readdir 使用 base list + layer children query + in-memory merge。
- 大文件 layer entry 外置存储后，再验证 shadow/S3 path 流式提交。

失败恢复测试：

- 本地 WAL 写入后进程 crash，同 sandbox 重启可 recover pending。
- checkpoint 成功后，新 sandbox 可 restore。
- checkpoint 失败不推进 durable seq。
- S3 上传失败不标记 entry durable。

## 明确不做

V1 不做：

- DeltaFS 多 segment stack。
- per-write 毫秒级 rollback。
- process/memory checkpoint。
- recursive directory rename。
- recursive delete 的完整 whiteout 展开。
- Git workspace 表迁移到通用 layer 表。
- 将 local-only build/cache output 自动持久化。

DeltaFS/DeltaBox 的多 segment checkpoint/rollback 可作为后续 V2/V3 优化方向，而不是 V1 复杂度来源。
