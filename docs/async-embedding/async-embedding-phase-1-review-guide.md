# Async Embedding Phase 1 Review Guide

**Date**: 2026-03-31
**Branch**: `feat/async-embedding-phase-a`
**Purpose**: 为 async embedding Phase 1 的 8 个独立 commit 提供审查顺序、每个 commit 的边界、关键改动点、风险点和建议关注问题，帮助 reviewer 按“可独立理解、可独立验证、可独立回滚”的方式阅读。

## Review Strategy

建议严格按提交顺序审查，不要跳着看。

原因：

- 前 4 个 commit 固定 schema / projection / durable task contract / tx seam，是后续行为改动的前置语义基础
- 第 5~7 个 commit 依次接入 write path、image bridge、query search path，每一步都依赖前一步已经固定好的 contract
- 第 8 个 commit 才把后台 worker lifecycle 接起来，审查时不应该再反推 schema 或 write path 语义

建议的审查方法：

1. 先确认 schema 和 store contract 是否与 proposal 对齐
2. 再确认 write path 是否把“写入 + clear stale state + enqueue”绑定进同一事务
3. 再确认 image extract bridge 是否补上了 `content_text -> embed` 的显式触发
4. 再确认 grep/search 是否彻底摆脱“DB 直接 embed query text”的旧假设
5. 最后审 worker 的 claim / obsolete / conditional writeback / retry / recover correctness

## Commit Overview

### 1. `b035297` — `tenant: unblock async embedding schema foundation`

**主题**

- 建立新的 tenant schema contract：`files.embedding` 改成 application-managed，可写；新增 `embedding_revision`；新增 `semantic_tasks`

**主要文件**

- `pkg/tenant/schema_zero.go`
- `pkg/tenant/schema_db9.go`
- `pkg/tenant/schema_starter.go`
- `pkg/tenant/starter.go`
- `pkg/backend/schema_test_helper.go`
- `pkg/server/schema_test_helper.go`
- `pkg/client/schema_test_helper.go`
- `pkg/datastore/schema_test_helper_test.go`
- `pkg/datastore/schema_test.go`

**这一步解决了什么**

- 去掉了对 generated `EMBED_TEXT(...)` 的错误依赖
- 让 starter provider 不再是“schema init 空实现”
- 让测试 helper schema 和真实 tenant schema 对齐

**审查重点**

- `files.embedding` 是否不再是 generated column
- `files.embedding_revision` 是否在所有 provider schema 中一致存在
- `semantic_tasks` 表和索引是否满足 claim / recover 的最小 contract
- starter provider 是否确实走到了 schema init

**风险点**

- schema helper 漂移
- starter provider 路径继续与 zero/mysql 路径不一致

### 2. `a21dadb` — `datastore: project embedding revision on file reads`

**主题**

- 扩展 file model 和常用 projection，把 `embedding_revision` 真实读出来

**主要文件**

- `pkg/datastore/store.go`
- `pkg/datastore/store_test.go`

**这一步解决了什么**

- 让应用层可以在 `GetFile` / `Stat` / `ListDir` / 删除返回值中看到 embedding freshness state

**审查重点**

- `File` 模型里是否新增了 `EmbeddingRevision`
- 所有常见 scan/projection 是否同步带上 `embedding_revision`
- nil / non-nil 的语义是否保持正确

**风险点**

- 查询列顺序和 scan 顺序不一致
- 某些 projection 漏掉 `embedding_revision`

### 3. `c376b03` — `semantic: add durable task datastore contract`

**主题**

- 落 semantic durable task substrate：enqueue / claim / ack / retry / recover

**主要文件**

- `pkg/semantic/task.go`
- `pkg/datastore/semantic_tasks.go`
- `pkg/datastore/semantic_tasks_test.go`

**这一步解决了什么**

- 把 QueueFS 级别的 durable queue contract 翻译成 dat9 的 semantic task contract
- 固定 receipt / lease / recover / dead-letter 行为

**审查重点**

- `UNIQUE(task_type, resource_id, resource_version)` 下 duplicate enqueue 的行为是否正确
- claim 是否递增 `attempt_count`、设置 receipt 和 lease
- ack 是否必须校验当前 receipt
- retry / dead-letter / recover 的状态迁移是否和 proposal 一致

**风险点**

- 错误 receipt 被错误接受
- recover 把不该恢复的任务重新排队
- retry / dead-letter 语义不一致导致 worker 未来难以推理

### 4. `5736343` — `datastore: add transactional semantic write seams`

**主题**

- 为 backend write path 改造准备 tx-aware seam，不改上层行为

**主要文件**

- `pkg/datastore/file_tx.go`
- `pkg/datastore/file_tx_test.go`
- `pkg/datastore/semantic_tasks.go`

**这一步解决了什么**

- 提供事务内 `InsertFileTx` / `UpdateFileContentTx` / `ClearFileEmbeddingStateTx`
- 提供事务内 `EnqueueSemanticTaskTx`

**审查重点**

- `UpdateFileContentTx` 是否同时完成：更新内容、递增 revision、清空 `embedding` / `embedding_revision`
- 事务回滚时 task enqueue 是否一起回滚
- 这一 commit 是否确实只做 seam，不混入 backend 行为变化

**风险点**

- 后续 write path 如果不复用这些 seam，会出现语义分叉

### 5. `1097745` — `backend: enqueue embed work inside committed writes`

**主题**

- 把 create / overwrite / multipart confirm 真正接到事务内 task enqueue

**主要文件**

- `pkg/backend/dat9.go`
- `pkg/backend/upload.go`
- `pkg/backend/semantic_tasks.go`
- `pkg/backend/semantic_tasks_test.go`

**这一步解决了什么**

- 文件写入和 semantic work 注册进入同一事务
- overwrite / upload overwrite 都会清空 stale embedding state
- upload overwrite 同时修复了 `content_type` 刷新问题

**审查重点**

- create path 是否在事务内完成 file insert / parent dir ensure / node insert / embed enqueue
- overwrite path 是否复用 `UpdateFileContentTx`
- upload confirm 的 overwrite 分支是否保留 inode/file_id
- upload confirm 是否在事务外只保留必要的 blob side effect

**风险点**

- 事务失败后的 blob 清理是否遗漏
- upload overwrite 是否把 upload row 正确重绑到 surviving inode
- non-image / large object path 是否错误创建 embed 任务

### 6. `47ce3d3` — `backend: requeue embed work after image text updates`

**主题**

- 给现有 image extract best-effort worker 补 durable embed bridge

**主要文件**

- `pkg/backend/image_extract.go`
- `pkg/backend/image_extract_test.go`
- `pkg/datastore/semantic_tasks.go`
- `pkg/datastore/semantic_tasks_test.go`
- `pkg/backend/semantic_tasks.go`

**这一步解决了什么**

- `UpdateFileSearchText(..., revision, text)` 成功后会 ensure 同 revision 的 `embed` task
- 已成功的旧任务可以被同 revision 的新 `content_text` 写回重新排回队列

**审查重点**

- bridge 是否只在 revision-gated `content_text` update 成功后触发
- stale image extract completion 是否不会重排旧 revision
- `EnsureSemanticTaskQueued` 对 terminal state 和 processing state 的区分是否合理

**风险点**

- 图片 caption/OCR 更新后没有向量更新
- 旧 revision 结果把已过时 task 错误 requeue

### 7. `497234c` — `search: move query embedding to app side`

**主题**

- query embedding 上移到应用层；datastore 只保留 SQL query seam

**主要文件**

- `pkg/embedding/client.go`
- `pkg/embedding/format.go`
- `pkg/embedding/openai.go`
- `pkg/embedding/openai_test.go`
- `pkg/datastore/search.go`
- `pkg/backend/dat9.go`
- `pkg/backend/options.go`
- `pkg/backend/grep_test.go`
- `cmd/dat9-server/main.go`

**这一步解决了什么**

- grep/search 不再假设数据库能把 query text 直接参与 vector distance 计算
- vector query 只看 `embedding_revision = revision`
- embedding provider 不可用时，grep 会退化到 FTS / keyword，而不是整体失败

**审查重点**

- `backend.Grep` 是否承担 orchestration，`datastore` 是否只剩纯查询函数
- `VectorSearch` 是否强制 `embedding_revision = revision`
- query embedder 的配置、fallback 和错误处理是否符合 proposal
- vector parameter 格式化是否统一收口到 `pkg/embedding`

**风险点**

- vector path 出错时把 grep 整体打挂
- stale vector 因过滤条件不严仍被命中

### 8. `d289630` — `server: process embed tasks in background workers`

**主题**

- 新增 server-owned semantic worker manager，并真正处理 embed task

**主要文件**

- `pkg/server/semantic_worker.go`
- `pkg/server/semantic_worker_test.go`
- `pkg/server/server.go`
- `pkg/datastore/embedding_writeback.go`
- `pkg/datastore/embedding_writeback_test.go`
- `cmd/dat9-server/main.go`

**这一步解决了什么**

- worker 生命周期从 backend / tenant pool LRU 中解耦，放到 server 托管
- 真正把 claim -> load file -> embed -> conditional writeback -> ack / retry / recover 串起来
- 提供 worker env config 和默认参数

**审查重点**

- worker 是否只在 `revision == resource_version` 且 `content_text` 非空时写回
- obsolete task 是否直接 ack，而不是 retry
- provider failure 是否 retry，超过上限是否 dead-letter
- recover sweep 是否能把过期 processing 任务重新变可执行
- 单租户和多租户下 store 获取方式是否都正确

**风险点**

- worker 和 tenant pool 生命周期再次耦合
- stale task 写回成功，污染当前 revision
- retry/backoff / recover 语义和 task substrate 不一致

## End State

到 `d289630` 为止，Phase 1 的关键链路已经闭合：

- 写入路径：create / overwrite / upload completion 会在事务内清空 stale embedding state，并 enqueue 同 revision 的 `embed`
- 异步 text 更新路径：image extract 成功写回 `content_text` 后，会显式 ensure 同 revision 的 `embed`
- 查询路径：query embedding 在应用层完成，vector search 只看 current revision vectors
- 后台处理路径：server-owned worker 会 claim task、生成 embedding、条件写回、ack / retry / recover

换句话说，旧的“generated embedding + DB 侧 raw-text vector distance”假设已经被完全替换为真实可运行的应用层路径。

## Recommended Review Commands

建议 reviewer 至少核对这些命令对应的测试面：

```bash
source ./scripts/test-podman.sh && go test ./pkg/datastore -count=1
source ./scripts/test-podman.sh && go test ./pkg/backend -count=1
source ./scripts/test-podman.sh && go test ./pkg/server -count=1
go test ./pkg/embedding -count=1
go test ./cmd/dat9-server -count=1
make test-podman
```

## Final Notes

- 当前工作区里 `docs/async-embedding/` 下的文档没有混进功能 commit；review 功能改动时可以只看上述 8 个 commit。
- `make test-podman` 在本分支最终通过；我遇到过一次 `pkg/client` 的瞬时 `invalid connection`，重跑后稳定通过，未观察到可重复失败。
- 这一阶段仍然没有把 image extract 本身迁移成 durable `extract_text` task；当前策略是保留它作为 best-effort `content_text` producer，并通过 bridge 触发 durable `embed`。
