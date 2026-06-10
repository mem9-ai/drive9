# Drive9 Layered Filesystem 调研报告

日期：2026-06-01

本文调研 AgentFS、Cloudflare Sandbox SDK / ArtifactFS、Tilde、E2B、Modal、Daytona，以及 YoloFS / DeltaFS 等新兴 agent / sandbox filesystem 方案，重点研究它们如何实现 layered filesystem，并给出 Drive9 的落地建议。

结论先行：Drive9 不应该照搬 AgentFS 的“单 SQLite 文件作为权威上层文件系统”，而应该把 layered fs 做成 Drive9 原生的服务端 metadata overlay：以现有 `file_nodes` / `inodes` / `contents` / `semantic` 为 base，新增通用 `fs_layers` / `fs_layer_entries` 作为 writable upper layer，复用现有 db9/S3 content plane、revision、FUSE shadow/writeback 和 Git workspace overlay 的经验。

## 一句话结论

- 社区主流实现已经收敛到同一个模型：只读 base layer + 可写 upper/delta/session layer + whiteout 删除标记 + 显式 diff/commit/rollback。
- AgentFS 的价值不只是隔离文件，而是让 upper layer 可查询、可审计、可复制。
- Cloudflare Sandbox SDK 的价值在执行隔离；它的 filesystem 持久化依赖 backup/R2/squashfs/overlayfs，不适合作为 Drive9 的权威文件模型，但适合作为执行沙箱参考。
- Cloudflare ArtifactFS 和 Drive9 现有 `git_workspace_*` 最接近：Git clean tree 作为 base，dirty/new/delete 作为 overlay，读时 merge，写时 copy-up 或写 upper。
- Tilde 把 sandbox run 做成事务：成功则 commit，失败/取消/超时则 rollback。这非常适合 Drive9 的 agent session。
- E2B / Modal 的 snapshot 更偏 VM/container/image 级 checkpoint，适合运行时 fork/resume，但不适合作为 Drive9 文件级 diff、search、commit 的主模型。
- Daytona 的 persistent volume 更像共享持久目录，不是事务 overlay；对大数据集复用有价值，但对 agent code editing 不够安全。
- YoloFS 和 DeltaFS 给出两个方向：agent 可见的 staged effects，以及高频 checkpoint 时冻结 writable layer 并插入新 layer。

## 主要资料来源

- [AgentFS 官网](https://www.agentfs.ai/)、[GitHub README](https://github.com/tursodatabase/agentfs)、[Copy-on-Write Overlays 文档](https://docs.turso.tech/agentfs/guides/overlay)、[SQLite schema spec](https://github.com/tursodatabase/agentfs/blob/main/SPEC.md)、[FUSE 文章](https://turso.tech/blog/agentfs-fuse)。
- [Cloudflare Sandbox SDK 架构](https://developers.cloudflare.com/sandbox/concepts/architecture/)、[生命周期](https://developers.cloudflare.com/sandbox/concepts/sandboxes/)、[Files API](https://developers.cloudflare.com/sandbox/api/files/)、[Backup/Restore](https://developers.cloudflare.com/sandbox/guides/backup-restore/)、[Bucket mounts](https://developers.cloudflare.com/sandbox/guides/mount-buckets/)。
- [Cloudflare ArtifactFS](https://github.com/cloudflare/artifact-fs)。
- [Tilde Filesystem Isolation](https://docs.tilde.run/sandboxes/filesystem/)。
- [E2B Sandbox Snapshots](https://e2b.dev/docs/sandbox/snapshots)。
- [Modal Sandbox Snapshots](https://frontend.modal.com/docs/guide/sandbox-snapshots)。
- [Daytona Volumes](https://www.daytona.io/docs/en/volumes/) 和 [Volume 概览](https://www.daytona.io/dotfiles/volumes)。
- [YoloFS arXiv](https://arxiv.org/abs/2604.13536) 和 [Microsoft Research 摘要](https://www.microsoft.com/en-us/research/publication/dont-let-ai-agents-yolo-your-files-shifting-information-and-control-to-filesystems-for-agent-safety-and-autonomy/)。
- [DeltaBox / DeltaFS arXiv](https://arxiv.org/abs/2605.22781)。

## 方案对比

| 方案 | Base layer | Writable layer | Snapshot / fork | 对外接口 | 对 Drive9 的启发 |
| --- | --- | --- | --- | --- | --- |
| AgentFS | 原始目录或 AgentFS SQLite filesystem | SQLite delta layer | 复制 SQLite 文件即可 snapshot/fork | Linux FUSE、macOS NFS、SDK、浏览器 OPFS | 上层 layer 必须可查询、可审计、可携带；copy-up + whiteout 足够支撑 MVP。 |
| Cloudflare Sandbox SDK | 容器内 ephemeral filesystem；可从 backup restore | 容器 writable layer；restore 后 upper 不影响 backup | backup 存 R2，数据为 squashfs + metadata | TypeScript SDK、文件 API、inotify watch | 执行沙箱和持久文件状态要分离；默认容器状态不可当权威数据。 |
| Cloudflare ArtifactFS | Git tree snapshot / generation | SQLite overlay metadata + upper content dir | HEAD 变化后重新索引 base，并 reconcile overlay | FUSE | Git clean tree + dirty overlay 是 Drive9 Git workspace 的同源模式，可抽象成通用 resolver。 |
| Tilde | versioned repository state | transactional session | exit 0 commit；失败/取消/超时 rollback；可人工审批 | sandbox 内 FUSE mount | 把 agent run 视为事务；commit all-or-nothing；权限应在 syscall/open/create 阶段失败。 |
| E2B | template / running sandbox state | sandbox 内运行时修改 | snapshot 捕获 filesystem + memory，可一对多生成新 sandbox | SDK | 适合 runtime fork，不适合作文件级 diff/search/commit 的主模型。 |
| Modal | base image | sandbox filesystem changes | filesystem snapshot 是相对 base image 的 diff；directory snapshot 可 mount | SDK | 适合冷启动和环境复用；文件级可查询性弱于 metadata overlay。 |
| Daytona | mount 进 sandbox 的 persistent volume | 同一共享 volume | volume 生命周期独立于 sandbox；多 sandbox 可同时 mount | FUSE volume + SDK | 适合大数据集/模型复用；不是事务层，并发同路径写入需要业务协调。 |
| YoloFS | 用户/项目文件系统 | staged mutation layer | snapshot 帮 agent 自纠错；progressive permission 控制风险路径 | agent-native FS 研究 | FS 应向 agent/user 暴露 staged effects，而不是只在事后保护。 |
| DeltaFS | 分层 sandbox file state | 可冻结 writable layer | checkpoint 冻结当前 upper 并插入新 upper；rollback 是 layer switch | OS-level sandbox 研究 | 高频 checkpoint/rollback 要靠 layer 切换，而不是全量复制。 |

## Layered FS 的共性机制

### 1. Merged View

agent 看到的是 merged view：

```text
visible tree
  upper/delta/session layer
  base layer
```

典型规则：

- `stat(path)`：先查 upper；如果是 whiteout，返回 not found；否则 fallback 到 base。
- `readdir(dir)`：base children 和 upper children 合并；同名 upper 覆盖 base；whiteout 隐藏 base child。
- `read(path)`：upper 有内容读 upper，否则读 base。
- `write(path)`：新文件直接写 upper；base 文件首次写入时 copy-up，然后修改 upper。
- `delete(path)`：如果是 base 文件，写 whiteout；如果是 upper-only 文件，可直接删除 upper entry。
- `rename(old, new)`：MVP 通常可用 old whiteout + new upsert 表达；目录 rename 需要谨慎处理。

### 2. Whiteout

whiteout 是 layered fs 的关键。它把“删除”变成 upper layer 的一条显式记录，而不是修改 base：

```text
base:  /src/a.go exists
upper: /src/a.go op=whiteout
view:  /src/a.go not found
```

这让 rollback 非常便宜：丢弃 upper 即可。

### 3. Copy-up

当 agent 修改 base 文件时，系统一般先把 base 内容复制到 upper，再对 upper 写入。AgentFS 文档里就是这个模型；ArtifactFS 也有类似 `ensureOverlay`：如果 base blob 尚未本地化，先 hydrate，再写 upper。

Drive9 MVP 可以先用 full-file copy-up。原因是现有 FUSE shadow/writeback 已经倾向于把写入最终变成完整对象提交。大文件后续可做 chunk/extent overlay。

### 4. Snapshot / Fork

有三种主流做法：

- AgentFS：复制 SQLite session DB，或者利用 WAL/数据库层能力做 time-travel/fork。
- DeltaFS：冻结当前 writable layer，插入新的 writable layer；rollback 切回旧 layer。
- Modal/E2B：保存 sandbox/image/memory 的整体 snapshot。

Drive9 更适合第二种 metadata layer 模式：冻结当前 layer，创建 child layer 指向 parent；后续通过 compaction 控制 layer depth。

### 5. Audit / Diff

成熟 agent FS 都把“变更是什么”作为一等能力。AgentFS 强调 SQLite 上层可查询；Tilde 每次 sandbox commit 记录结构化 metadata；YoloFS 关注 staged effects 对 agent/user 可见。

Drive9 应该把 diff/audit 设计在第一版，而不是事后从对象变化里反推。

## 重点方案拆解

### AgentFS

AgentFS 的定位是“agent 可以用真实 CLI 工具，但不会直接破坏用户文件”。它用 SQLite 保存 agent filesystem state，并通过 FUSE/NFS/SDK/OPFS 暴露成文件系统。

Layered 实现要点：

- base layer 是原目录，只读。
- delta layer 是 AgentFS SQLite DB。
- read 先查 delta，再查 base。
- write base 文件时 copy-up 到 delta。
- delete base 文件时在 delta 写 whiteout。
- delta 中存文件、目录、块、tool call、KV 等结构化表。
- session DB 可复制、移动、查询，天然形成快照/审计单元。

优点：

- 极强的可移植性：一个 SQLite 文件就是一个 agent session。
- 审计友好：文件变化和 tool calls 都在结构化 DB 中。
- 与工具兼容：FUSE/NFS 后，`git`、`grep`、`cat` 等都能直接用。

局限：

- SQLite upper layer 更适合单机/单 session portable model。
- 服务端多租户、S3 大对象、semantic search、quota、GC 会要求额外整合。
- full-file copy-up 对超大文件需要后续优化。

Drive9 建议：

- 学习 AgentFS 的 copy-up / whiteout / diff / audit 语义。
- 不把 SQLite 作为 Drive9 权威层；Drive9 的权威 upper layer 应在 TiDB/db9 元数据表中。
- 可以后续提供“导出 layer 为 SQLite/zip bundle”的 portability 功能。

### Cloudflare Sandbox SDK

Cloudflare Sandbox SDK 是安全执行环境，架构上是 Worker -> Durable Object -> Container。它提供文件读写、命令执行、watch、bucket mount、backup/restore 等能力。

Layered 实现相关点：

- sandbox 自带独立 filesystem，但默认只在容器 active 期间保留；idle/restart 后状态会丢失。
- backup/restore 用 R2 存 `data.sqsh` 和 metadata。
- production restore 时把 backup 作为 read-only lower layer，通过 FUSE overlayfs 挂载；新写入进入 writable upper layer，不修改原 backup。
- 本地开发 restore 不是 COW overlay，而是 `unsquashfs` 解压替换目录。
- R2/S3 bucket 可 mount 到路径，支持 prefix 和 read-only；文档明确提醒 bucket mount 比本地 filesystem 慢。

Drive9 建议：

- execution sandbox lifecycle 不应决定 Drive9 文件生命周期。
- Drive9 layer 应作为 durable storage/session，与 Cloudflare/E2B/Modal 这类 sandbox 解耦。
- 如果将来 Drive9 对接 Cloudflare Sandbox，可把 Drive9 layer mount/restore 到 sandbox 内，而不是把 sandbox FS 当作 Drive9 权威存储。

### Cloudflare ArtifactFS

ArtifactFS 是 Git-backed FUSE filesystem，目标是让大 repo 快速可见，blob 按需 hydrate。它非常贴近 Drive9 当前 Git fast workspace。

Layered 实现相关点：

- base 是 Git commit tree snapshot，而不是完整 checkout。
- FUSE 立即暴露完整目录树。
- blob 内容按需通过 Git object 读取并缓存。
- writable overlay 存 dirty/new/delete。
- resolver 合并 snapshot + overlay。
- 删除用 whiteout。
- E2E 测试覆盖 FUSE、git 操作、commit 和 overlay reconciliation。

Drive9 当前已经有类似结构：

- `git_workspaces` 保存 workspace 元信息。
- `git_workspace_tree_nodes` 保存 clean tree manifest。
- `git_workspace_overlay` 保存 dirty/new/delete/chmod/symlink。
- FUSE 里 `pkg/fuse/git_workspace.go` 做 merged view、hydrate、overlay read/write。

Drive9 建议：

- 通用 layered FS 应复用这套思想：base identity + overlay op + path hash + merged resolver。
- Git workspace 保持专用 base tree，不要把 clean Git blobs 写进普通 `file_nodes`。
- 抽象公共 overlay resolver，但不要急着把 Git workspace 表强行并入通用表。

### Tilde

Tilde 的 model 是“每个 sandbox 获得一个 versioned FUSE mount，对应一个 transactional session”。sandbox 内对 mount 的写入都 staged；成功退出 commit，失败/取消/超时 rollback；同路径冲突会导致 commit failed。

Drive9 建议：

- 给 layer 明确状态机：`active`、`sealed`、`awaiting_approval`、`committed`、`abandoned`。
- `commit` 必须 all-or-nothing，避免半提交。
- 权限错误尽量在 `open/create/unlink` 发生，而不是到 commit 才发现。
- 同一路径 base revision 冲突时 commit fail，后续提供 rebase/resolve。

### E2B 和 Modal

E2B snapshot 捕获 running sandbox 的 filesystem + memory，可以一对多创建新 sandbox。Modal 有 filesystem snapshot、directory snapshot、memory snapshot；filesystem snapshot 是相对 base image 的 diff，并可长期保存。

它们适合：

- 缓存重环境，减少 cold start。
- fork 运行时状态。
- 复制 agent execution state。

它们不适合作为 Drive9 主文件模型：

- 文件级 diff/search/commit/rollback 不够透明。
- 语义检索、审计、quota、per-path conflict 需要 Drive9 自己掌握 metadata。

Drive9 建议：

- 把 E2B/Modal snapshot 当 execution acceleration。
- Drive9 layer ID 作为外部 sandbox 的持久文件状态句柄。

### Daytona Volumes

Daytona volumes 是 FUSE-backed persistent volume，数据落在 S3-compatible object store。它支持多个 sandbox mount 同一个 volume，并用 subpath 做隔离。

对 Drive9 的启发：

- 对大模型、大数据集、共享 artifacts，persistent volume 很有用。
- 但它不是事务 overlay。文档明确说明共享 FUSE volume 非 transactional，同路径并发写入是 last write wins。
- Drive9 agent editing 需要 review/rollback/conflict，所以不能只做共享 volume。

### YoloFS 和 DeltaFS

YoloFS 提出 agent-native filesystem 应把信息和控制下沉到 FS：staging 隔离所有 mutation，snapshot 让 agent 能自我纠错，progressive permission 减少无谓 prompt。

DeltaFS 关注高频 checkpoint/rollback：文件状态分层；checkpoint 时冻结当前 writable layer 并插入新 writable layer；rollback 变成 layer switch。

Drive9 建议：

- `layer diff`、`layer status`、`layer rollback` 应该可被 agent 自己调用，而不是只给人类 UI。
- snapshot 设计成 metadata 操作，避免全量复制。
- layer chain 必须有 depth limit 和 compaction。

## Drive9 现状映射

当前相关代码：

- 通用文件元数据和内容：`pkg/datastore/store.go`、`pkg/datastore/file_tx.go`、`pkg/backend/dat9.go`。
- FUSE 本地写入 staging：`pkg/fuse/shadow.go`、`pkg/fuse/writeback.go`、`pkg/fuse/commit_queue.go`。
- coding-agent local-only overlay：`pkg/fuse/local_overlay.go`、`pkg/fuse/local_policy.go`。
- Git 专用 layered workspace：`pkg/tenant/schema/git_workspace.go`、`pkg/datastore/git_workspace.go`、`pkg/fuse/git_workspace.go`、`docs/design/git-fast-clone-workspace.md`。

Drive9 已具备：

- 绝对路径、目录 `/` 结尾、文件非 `/` 结尾的 canonical path 规则。
- `file_nodes` / `inodes` / `contents` / `semantic` 分离。
- revision-based write conflict 机制。
- DB-inline / S3 大对象分层。
- FUSE shadow files、writeback、pending index。
- Git workspace 的 `upsert` / `whiteout` / `chmod` / `symlink` overlay 模型。

缺口：

- 普通 Drive9 文件还没有服务端通用 overlay layer。
- API/FUSE/backend 缺少 layer-aware merged resolver。
- 缺少 layer 生命周期、diff、commit、rollback。
- 缺少 layer-aware search / semantic indexing。
- 缺少通用 per-layer audit event。

## 推荐架构

### 核心模型

新增 Drive9 原生 layer：

```text
Merged view
  upper: fs_layer_entries(layer_id)
  lower: base Drive9 namespace(base_tenant_id, base_root_path)
  content: existing DB-inline / S3 content plane
  search: existing semantic plane + layer scope
```

layer 是 lightweight per-agent/per-task writable overlay，不是 tenant fork。tenant fork 可以继续作为重型隔离/分支能力存在。

### 表结构草案

MVP 建议模仿 `git_workspace_overlay`，减少一次性抽象复杂度：

```sql
CREATE TABLE fs_layers (
  layer_id          VARCHAR(64) PRIMARY KEY,
  base_tenant_id    VARCHAR(64) NOT NULL,
  base_root_path    VARCHAR(512) NOT NULL,
  parent_layer_id   VARCHAR(64) NOT NULL DEFAULT '',
  actor_id          VARCHAR(255) NOT NULL DEFAULT '',
  state             VARCHAR(32) NOT NULL DEFAULT 'active',
  commit_policy     VARCHAR(32) NOT NULL DEFAULT 'manual',
  created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  sealed_at         DATETIME(3),
  updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE fs_layer_entries (
  layer_id          VARCHAR(64) NOT NULL,
  path              VARCHAR(1024) NOT NULL,
  path_hash         VARCHAR(64) NOT NULL,
  parent_path       VARCHAR(1024) NOT NULL,
  parent_path_hash  VARCHAR(64) NOT NULL,
  name              VARCHAR(255) NOT NULL,
  op                VARCHAR(16) NOT NULL,
  kind              VARCHAR(16) NOT NULL DEFAULT 'file',
  mode              INT NOT NULL DEFAULT 420,
  size_bytes        BIGINT NOT NULL DEFAULT 0,
  base_inode_id     VARCHAR(64) NOT NULL DEFAULT '',
  base_revision     BIGINT NOT NULL DEFAULT 0,
  storage_type      VARCHAR(32) NOT NULL DEFAULT '',
  storage_ref       TEXT NOT NULL,
  storage_ref_hash  VARCHAR(64) NOT NULL DEFAULT '',
  checksum_sha256   VARCHAR(128) NOT NULL DEFAULT '',
  content_blob      LONGBLOB,
  content_type      VARCHAR(255),
  content_text      LONGTEXT,
  metadata_json     JSON,
  created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (layer_id, path_hash)
);

CREATE INDEX idx_fs_layer_parent
  ON fs_layer_entries(layer_id, parent_path_hash);

CREATE INDEX idx_fs_layer_op
  ON fs_layer_entries(layer_id, op);
```

设计要点：

- `path_hash` 复用 Drive9/Git workspace 的长路径索引策略。
- `path` 原文仍是权威值，lookup 时 hash + path 双重校验。
- `base_inode_id` / `base_revision` 用于 commit conflict detection。
- `content_blob` 适合小文件；大文件仍走现有 S3 `storage_ref`。
- 后续可以把 content 字段抽到 layer-scoped `inodes` / `contents`，但 MVP 不必一开始做过度抽象。

### Merged Resolver

`Stat(path, layer_id)`：

1. 查 exact overlay entry。
2. `op=whiteout` 返回 not found。
3. `op=upsert|mkdir|symlink|chmod` 返回 overlay metadata。
4. 否则查 base。

`ReadDir(dir, layer_id)`：

1. list base children。
2. list overlay children。
3. 同名 overlay 覆盖 base。
4. whiteout 隐藏 base child。
5. overlay-only child 加入结果。

`Read(path, layer_id)`：

1. overlay 有内容则读 overlay。
2. overlay 是 whiteout 则 not found。
3. 否则读 base。

`Write(path, layer_id)`：

1. overlay-only 文件直接更新 overlay entry。
2. base 文件首次写入：记录 `base_inode_id`、`base_revision`，copy-up 完整内容到 overlay，再写 overlay。
3. 大文件走现有 upload/S3 path，overlay entry 只保存 storage ref。

`Delete(path, layer_id)`：

1. overlay-only entry 可以删除或变成 tombstone。
2. base 文件写 `op=whiteout`。
3. 目录删除 MVP 只支持 empty dir；recursive delete 后续做批量 whiteout。

`Rename(old, new, layer_id)`：

- MVP 支持文件/软链：old whiteout + new upsert。
- 目录 rename 先返回 typed error 或 FUSE `EXDEV`，让调用方 fallback 到 copy/delete。
- 后续再做 atomic recursive rename 或 `op=rename`。

`Commit(layer_id)`：

1. seal layer，阻止新写入。
2. 开 DB transaction。
3. 对每个 entry 检查 base revision。
4. upsert 写入 base `file_nodes` / `inodes` / `contents`。
5. whiteout 执行 base delete。
6. enqueue semantic tasks。
7. 标记 layer committed。
8. 对未提交/被替换 S3 refs 入 GC。

`Rollback(layer_id)`：

1. 标记 layer abandoned。
2. merged view 不再读取该 layer。
3. overlay content 按 retention 入 GC。

`Snapshot(layer_id)`：

- seal 当前 layer 或记录 snapshot point。
- 创建 child layer，`parent_layer_id = current_layer_id`。
- MVP 限制 depth 为 1-2；后续增加 flatten/compaction。

## API / CLI 建议

Server：

- `POST /v1/layers`：创建 layer。
- `GET /v1/layers/{id}`：查看状态。
- `GET /v1/layers/{id}/diff`：查看 staged changes。
- `POST /v1/layers/{id}/checkpoints`：创建 restore checkpoint。
- `POST /v1/layers/{id}/commit`：all-or-nothing commit。
- `POST /v1/layers/{id}/rollback`：abandon。
- 现有 `/v1/fs/{path}` 通过 header 选择 layer，例如 `Drive9-Layer-ID`，避免污染 path。

CLI：

```bash
drive9 fs layer create :/project --name agent-task-123
drive9 mount :/project ./mnt --layer <layer-id>
drive9 fs layer diff <layer-id>
drive9 fs layer commit <layer-id>
drive9 fs layer rollback <layer-id>
```

FUSE：

- `MountOptions` 增加 `LayerID`。
- `Stat`、`ReadDir`、`Read`、`Write`、`Mkdir`、`Rename`、`Unlink`、`Symlink`、`Chmod`、`Flush` 都走 layer-aware client。
- coding-agent local-only overlay 继续保留，用于 `.git`、`node_modules`、build output、cache。
- local-only state 必须清晰标注为 rebuildable，不自动进入 durable layer。

## Search / Semantic 语义

Layer-aware search 的规则：

- base search result 必须被 overlay whiteout / replacement 过滤。
- overlay 新建/修改文件应在 commit 前可搜索。
- MVP 可先做 overlay `content_text` 的 keyword/FTS。
- P1 增加 `layer_id` 到 semantic task/resource identity，或引入 `fs_layer_semantic`。
- commit 后，semantic entry 应进入 base inode revision，或触发重新计算。

关键约束：如果 layer 删除或替换了 `/foo.txt`，base 里旧 `/foo.txt` 的 semantic hit 不应出现在 layer search 结果中。

## 权限与安全

建议拆分权限：

- 读 base：需要 base read。
- 写 layer：需要 layer write。
- commit 到 base：对每个受影响 path 需要 base write/delete。
- rollback：需要 layer owner 或 layer admin。

参考 Tilde/YoloFS：能在 open/create/unlink 阶段失败的权限错误，就不要拖到 commit 阶段才失败。这样 agent 能立刻感知并调整行为。

## Audit

建议增加 append-only layer event stream：

```sql
CREATE TABLE fs_layer_events (
  event_id       VARCHAR(64) PRIMARY KEY,
  layer_id       VARCHAR(64) NOT NULL,
  seq            BIGINT NOT NULL,
  actor_id       VARCHAR(255) NOT NULL DEFAULT '',
  tool_call_id   VARCHAR(255) NOT NULL DEFAULT '',
  op             VARCHAR(32) NOT NULL,
  path           VARCHAR(1024) NOT NULL,
  before_json    JSON,
  after_json     JSON,
  created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_layer_seq(layer_id, seq)
);
```

这让 Drive9 获得 AgentFS 最有价值的部分：layer 不只是若干 blob，而是可解释、可追溯、可审计的 agent session。后续可与现有 journal 产品打通。

## 分阶段落地计划

### Phase 0：设计收敛

- 写 ADR/spec，明确 `fs_layers`、`fs_layer_entries`、`fs_layer_events`。
- 确定 overlay content 是复制字段，还是引用 layer-scoped inode/content。
- 定义 file replace、file delete、directory delete、directory rename 的冲突规则。
- 定义 abandoned layer 和 uncommitted S3 refs 的 GC 策略。

### Phase 1：Backend MVP

- 更新 `pkg/tenant/schema/tidb_auto.go`、`tidb_app.go`、`db9/schema.go`。
- 增加 datastore 方法：
  - `CreateLayer`
  - `GetLayer`
  - `UpsertLayerEntry`
  - `GetLayerEntry`
  - `ListLayerChildren`
  - `DiffLayer`
  - `CommitLayer`
  - `RollbackLayer`
- 在 `Dat9Backend` 周围实现 layer-aware resolver。
- 单测覆盖 `Stat`、`ReadDir`、`Read`、`Write`、`Delete`、commit conflict。

### Phase 2：API / CLI

- 增加 layer lifecycle endpoints。
- client 支持 layer header/options。
- 增加 `drive9 fs layer` 命令组。
- 不带 layer ID 时，现有 `/v1/fs` 行为完全兼容。

### Phase 3：FUSE Agent Workflow

- 增加 `drive9 mount --layer`。
- FUSE 所有路径操作走 layer-aware client。
- `FlushAll` / unmount 时 drain overlay writes。
- 增加 FUSE 层 diff/rollback 测试。

### Phase 4：Search / Audit / Snapshot

- 接入 `fs_layer_events`。
- layer-aware search 过滤 base hits。
- overlay semantic indexing。
- 增加 snapshot/child layer，带 max-depth guard。
- 增加 compaction/flattening。

### Phase 5：与 Git Workspace 适度统一

Git workspace 有特殊的 clean Git tree、blob hydrate、`.git` checkpoint 需求，不应急于迁到通用表。但通用 layer 稳定后可以：

- 共享 whiteout/diff/commit 代码。
- 抽象共同 overlay resolver interface。
- 保持 Git base tree 独立于普通 `file_nodes`。

## 风险与注意事项

- 目录操作复杂度高，MVP 应保守处理 directory rename / recursive delete。
- layer-aware search 如果不正确过滤 base hits，会给 agent 错误上下文。
- S3 GC 必须同时考虑 uncommitted layer、committed base refs、tenant forks、abandoned sessions。
- FUSE kernel cache 需要在 overlay 被外部修改时正确 invalidation。
- layer chain 过深会拖慢 `stat/readdir/read`，必须早期限制 depth。
- commit 必须 all-or-nothing，否则 review/rollback 语义会崩。
- local-only overlay 和 durable layer 必须在 UI/CLI 中区分清楚，避免 agent 误以为 build output 已持久化。

## 最终建议

Drive9 的 layered filesystem 应以这个形态落地：

```text
existing base namespace
  + fs_layer_entries upper layer
  + existing DB-inline/S3 content plane
  + explicit diff/commit/rollback
```

这条路径吸收了 AgentFS 的安全和审计、ArtifactFS 的 lazy base + overlay resolver、Tilde 的 transactional session，又不放弃 Drive9 已有的多租户、semantic search、S3/db9 存储、FUSE writeback 和 Git workspace 基础。

最小可用产品形态：

```bash
drive9 fs layer create :/repo
drive9 mount :/repo ./mnt --layer <id>
# agent normal workflow
drive9 fs layer diff <id>
drive9 fs layer commit <id>   # or rollback
```

这会让 Drive9 的 agent workspace 具备四个核心能力：安全隔离、可审查、可恢复、可 fork。
