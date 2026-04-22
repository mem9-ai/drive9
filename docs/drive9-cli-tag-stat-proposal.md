# Proposal: drive9 CLI `cp --tag` 与 `stat` 元数据展示

**Date**: 2026-04-22  
**Purpose**: 基于当前 `drive9` 代码现状、upstream issue #265（输入核对）以及本地草案 `/Users/jayson/Projects/claw-memories/images/drive9-cli-tag-stat-draft.md`，定义 `cp --tag` 与 `stat` 展示 `tags/content_text` 的可实现设计。

## Summary

本提案的决策是：

1. 为 `drive9 fs cp` 增加可重复参数 `--tag key=value`，仅在上传方向生效（`local/stdin -> remote`）。
2. 为服务端新增 metadata stat 读取路径（GET JSON），让 CLI `stat` 能展示 `tags` 与 `semantic_text`（映射 `files.content_text`）。
3. 保持现有 HEAD stat 协议不变（FUSE 和已有 SDK 行为不回归）。
4. 不引入新的 tag 子命令（`tag set/unset`），也不引入额外 schema 变更。

该方案复用已有 `file_tags` 表与上传完成事务，不新增持久化中间状态，改动集中在 `cmd/drive9/cli`、`pkg/client`、`pkg/server`、`pkg/backend`、`pkg/datastore`。

## Context

### Current State

已验证的当前行为：

- `cp` 仅支持 `--resume` 与 `--append`，没有 `--tag`，见 `cmd/drive9/cli/cp.go`。
- `stat` 仅通过 HEAD 输出 `size/isdir/revision`，无 `--json`，见 `cmd/drive9/cli/stat.go` 与 `pkg/client/client.go` (`StatCtx`)。
- 服务端 `GET /v1/fs/{path}` 仅分流到 `read/list/find/grep`，无 metadata stat JSON 路径，见 `pkg/server/server.go` (`handleFS`)。
- 租户 schema 已有 `file_tags` 表，`find -tag` 也已支持按 tag 过滤，见 `pkg/tenant/schema/tidb_auto.go`、`pkg/datastore/search.go`。
- 但当前写路径没有标准 tag 写入接口；`file_tags` 仅在删除路径中被清理，见 `pkg/datastore/store.go`。

### Requirement Input Check

- upstream issue #265 当前内容是 FUSE `EIO` 竞态问题，不是本功能需求本体（功能需求主要来自本地草案）。
- 本提案按草案定义 CLI 用户接口，并给出与现有代码一致的落地设计。

### Constraints

1. 兼容性：不传 `--tag` 时，`cp` 行为必须与当前一致。
2. 可落地性：不能破坏现有 multipart/v2/resume/append 上传路径。
3. 协议稳定：HEAD stat（`X-Dat9-*`）保持不变，避免影响 FUSE 与旧客户端。
4. 命名稳定：CLI/API 对外字段采用 `semantic_text`，内部映射 `files.content_text`。

## Terminology Baseline

| Term | Meaning |
| --- | --- |
| `semantic_text` | CLI/API 输出字段，表示可搜索语义文本 |
| `files.content_text` | 数据库存储列，`semantic_text` 的底层来源 |
| `tags` | 文件级 KV 元数据，对应 `file_tags(file_id, tag_key, tag_value)` |

## Goals

1. 用户可在上传时声明 tags：`drive9 fs cp ... --tag key=value ...`。
2. `drive9 fs stat` 默认可见 `semantic_text` 与 `tags.*`，并支持 `--json` 稳定输出。
3. 传入 tags 时，标签写入与文件新 revision 可见性保持同事务提交（避免“文件已成功但 tags 丢失”）。
4. 维持现有命令/接口主路径的行为兼容。

## Non-Goals

- 不引入 `drive9 fs tag set/unset` 独立命令。
- 不扩展 JS/Python/Rust SDK 设计（本提案只覆盖 Go client + CLI）。
- 不调整 `find -tag` 语义与查询语法。
- 不在本阶段新增 uploads 表字段或做 schema 迁移。

## Design

### 1) CLI Contract

`cp` 新语法：

```bash
drive9 fs cp [--resume] [--append] [--tag <key=value>]... <src> <dst>
```

规则：

- `--tag` 可重复，格式必须 `key=value`。
- `key` 不能为空；`value` 可为空（`key=`）。
- 同一命令重复 key 报错（CLI 侧先拦截）。
- 仅上传方向允许：
  - `local -> remote`
  - `stdin(-) -> remote`
- 以下方向携带 `--tag` 时报错：
  - `remote -> local`
  - `remote -> remote`

错误文案沿用草案：

```text
cp: --tag is only supported for uploads (local/stdin -> remote)
cp: invalid --tag "owner" (expected key=value)
cp: duplicate --tag key "owner"
```

### 2) 上传路径上的 tags 提交策略

采用“最终提交点带 tags”的统一策略，不新增持久化中间字段：

- 小文件 direct PUT：在写请求上携带 tags（header），由 `handleWrite` 解析。
- multipart v1/v2 与 append/resume：在 `complete` 请求体中携带 `tags`，由 `handleUploadComplete` / `handleV2UploadComplete` 解析。
- backend 在最终确认上传事务（`ConfirmUpload` / `ConfirmUploadV2` -> `finalizeUpload`）里提交 tags。

这样可覆盖：

- `WriteStream*`
- `ResumeUpload*`
- `AppendStream`（其最终也走 complete）

并保持“文件 revision 与 tags”一致提交。

### 3) Tag 持久化语义

为避免不透明叠加，定义如下：

- 当请求未提供 tags：不修改已有 tags。
- 当请求提供 tags：以本次提供的 tag 集合覆盖该文件原有 tags（同事务内先删后插）。

这保证行为可预测，且不依赖额外 `unset` 命令来清理历史键。

### 4) Metadata Stat API

新增读取路径（GET）用于 CLI/agent 元数据读取，不改变 HEAD：

- `GET /v1/fs/{path}?stat=1`

响应建议：

```json
{
  "size": 86773,
  "isdir": false,
  "revision": 1,
  "content_type": "image/jpeg",
  "semantic_text": "主要物体：一只小猫...",
  "tags": {
    "owner": "alice",
    "topic": "cat"
  }
}
```

实现上由服务端在 `StatNodeCtx` 后补充读取 `file_tags`；目录返回 `tags={}`，`semantic_text=""`。

### 5) CLI `stat` 展示

`drive9 fs stat [--json] <path>`

- 默认文本输出：
  - 保留现有字段（`size/isdir/revision`）
  - 新增 `content_type`
  - 新增 `semantic_text`（不截断）
  - 新增 `tags.<key>: <value>`（按 key 排序输出）
- `--json`：输出稳定 JSON，适合脚本与 agent。

### 6) 架构快照（变更后）

```text
CLI cp --tag
   -> pkg/client (direct PUT or multipart complete with tags)
      -> pkg/server
         -> pkg/backend finalize/write tx
            -> files + file_nodes + file_tags (same tx when tags provided)

CLI stat (--json/default)
   -> GET /v1/fs/{path}?stat=1
      -> stat node/file + read file_tags
      -> return semantic_text + tags
```

### 7) 兼容性与不变量

- HEAD stat (`X-Dat9-IsDir/X-Dat9-Revision/X-Dat9-Mtime`) 不变。
- `cp` 不带 `--tag` 时，网络协议和行为与当前一致。
- `find -tag` 语义不变，仅新增更易用的 tags 写入入口。
- 对外字段名固定为 `semantic_text` 与 `tags`。

## Rollout Plan

- Phase A: Server/Data 层
  - 增加 tag 解析、事务写入 helper、metadata stat JSON 路由。
  - 增加 datastore 的 tag 读写函数（Tx 覆盖写 + 非 Tx 查询）。
- Phase B: Client/CLI 层
  - client 上传路径增加可选 tags 透传。
  - CLI `cp --tag` 参数解析与校验。
  - CLI `stat --json` 与默认增强输出。
- Phase C: 文档与验收
  - 更新命令帮助与示例。
  - 增加回归测试与最小 e2e 覆盖。

## Validation Strategy

- CLI 单元测试：
  - `cp --tag` 方向校验、格式校验、重复 key 校验。
  - `stat` 文本输出与 `--json` 结构校验。
- Client 单元测试：
  - direct PUT 是否携带 tags。
  - v1/v2 complete 是否携带 tags。
  - resume/append 最终 complete 是否携带 tags。
- Server/Backend/Datastore 集成测试：
  - 上传时写入 tags，`find -tag` 可检索到。
  - 覆盖写时 tags 覆盖语义正确。
  - `GET ?stat=1` 返回 `semantic_text/tags` 与数据库一致。
  - 不带 tags 时不修改既有 tags。

## Risks and Mitigations

1. 风险：默认 `stat` 输出变长，可能影响历史文本解析脚本。  
缓解：保留原字段并新增 `--json` 稳定输出，文档明确建议脚本改用 `--json`。

2. 风险：不同上传模式（direct/v1/v2/resume/append）出现 tags 透传不一致。  
缓解：统一到“最终提交点”语义，并对每条上传路径加测试。

3. 风险：tag 覆盖策略引发“误删旧 tag”认知偏差。  
缓解：在 CLI help 和 proposal 文档中明确“提供 tags 时覆盖集合，不提供则保持不变”。

## Open Questions

无阻塞性开放问题。当前范围内关键行为均已收敛，可直接进入实现阶段。
