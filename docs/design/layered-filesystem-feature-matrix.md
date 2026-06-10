# Drive9 LayerFS Feature Matrix

本文档是 Drive9 + LayerFS 的功能能力总表，后续 LayerFS 开发应以此为 backlog 和验收清单。任何新增、删减或语义变化都需要同步更新本文件，避免 layer 模式和原生 Drive9 能力之间出现隐性分叉。

当前基线：PR #507 / `feat/layer`，截至 `61ee9e4 refactor: rename layer API endpoints`。

相关设计：

- [Layered Filesystem V1 Design](./layered-filesystem-v1-design.md)
- [Layered Filesystem Research](./layered-filesystem-research.md)

## 1. 读法和状态定义

### 1.1 状态

| 状态 | 含义 |
| --- | --- |
| Parity | layer 模式和非 layer 原生 Drive9 在用户可见能力、语义、错误行为和性能级别上基本一致。 |
| Supported | LayerFS 特有能力已经可用，当前实现符合 V1 设计目标。 |
| Partial | 可用但存在明确能力缺口、性能退化、边界语义差异或测试覆盖不足。 |
| Gap | 当前没有实现，或已有命令/API 在 `--layer` 场景下显式拒绝。 |
| N/A | 不属于 LayerFS 目标，或只能通过已有非 layer 能力间接满足。 |

### 1.2 优先级

| 优先级 | 含义 |
| --- | --- |
| P0 | 生产级阻断项；缺失时不应宣称 LayerFS production-ready。 |
| P1 | 原生 Drive9 parity 阻断项；影响常见用户路径或迁移体验。 |
| P2 | 质量、规模、性能或运维完备性问题；不阻断早期试用，但会限制规模化。 |
| P3 | 生态、体验或未来增强项。 |

### 1.3 目标类型

| 目标 | 含义 |
| --- | --- |
| Native parity | 与非 layer Drive9 功能保持一致。 |
| LayerFS core | 作为 layered filesystem 必须具备的核心能力。 |
| AgentFS-class | 对齐 AgentFS、Cloudflare Sandbox FS、ArtifactFS 等社区新兴方案的能力。 |
| Ops-prod | 生产运维、安全、观测、恢复和成本控制能力。 |

## 2. 总览结论

| 维度 | 当前判断 | 后续重点 |
| --- | --- | --- |
| 原生 Drive9 不带 layer 的行为 | Parity | 当前实现保持 opt-in；未设置 `--layer` 时不改变已有读写、mount、local overlay、git workspace、pack/unpack 等路径。 |
| LayerFS 核心模型 | Supported | 已具备 base root + writable layer、copy-up、whiteout、checkpoint、rollback、commit、name/tag ref。 |
| FUSE layer mount | Partial | 可写入、恢复、commit；仍需补齐 POSIX 边界、open dirty handle checkpoint barrier、multi-client consistency、WebDAV 策略。 |
| CLI layer parity | Partial | 写入类命令已覆盖一批关键路径，但 `cat/ls/stat/pack/unpack/git` 等直读/复合命令还未 layer-aware。 |
| 大文件能力 | Partial | 本地大文件和 FUSE spill 可走 object upload；仍缺 multipart、resume、range、direct-to-object-store、quota/GC。 |
| 搜索与索引 | Partial | `grep/find --layer` 有 overlay 视图；语义搜索、标签搜索、预提交索引、checkpoint 搜索仍缺。 |
| 事务与恢复 | Partial | commit 有 preflight、snapshot、best-effort rollback 和 committing recovery；还不是严格 exactly-once、全局事务或可审计 ledger。 |
| AgentFS/Cloudflare 等社区能力 | Partial | 基础 overlay 方向一致；缺 portable session export、provenance audit、progressive permission、branch/fork、squash/compact。 |
| 生产级别 | Partial | 还缺 GC/retention/quota、observability、load/stress/failpoint、schema rollout、security hardening。 |

## 3. 用户旅程矩阵

| 旅程 | 非 layer 原生行为 | 当前 LayerFS 状态 | 差距/风险 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| 创建 layer | 直接写 base filesystem | Supported：`drive9 fs layer create /base --name ... --tag ...` | name/tag 无唯一性约束，歧义时需要用户明确 ref。 | P1 | CLI/API 单测覆盖 id/name/tag 创建和解析。 |
| 挂载 layer | `drive9 mount` 直接读写 base | Supported：FUSE `drive9 mount --layer <ref>` | 仅 FUSE；WebDAV mode 未支持；checkpoint mount 当前依赖 active layer。 | P1 | FUSE e2e 覆盖 create/write/restore/commit。 |
| 修改文件 | 直接写 base，进入原生 commit queue | Supported：写入 layer entries/object，不直接改 base | 写入目的切换为 layer table/object；用户需要理解 commit 前 base 不变。 | P1 | FUSE + CLI 写入 e2e；base 不变断言。 |
| 查看 overlay 结果 | 原生 read/list/stat | Partial：FUSE 可见 overlay；部分 CLI 读命令未 layer-aware | `cat/ls/stat` 直接 CLI 不支持 `--layer`，体验不一致。 | P1 | 直接 CLI 读命令支持 `--layer` 并补 e2e。 |
| diff/review | 原生没有 session diff | Partial：`drive9 fs layer diff` 返回 entry 列表 | 无 git-style patch、内容预览、目录聚合、冲突标记。 | P2 | API/CLI diff 格式稳定，覆盖 rename/delete/large object。 |
| checkpoint | 原生无 session checkpoint | Supported：`layer checkpoint` 记录 durable seq | `--wait` 语义未形成真正 dirty-handle barrier。 | P0 | checkpoint 后跨 sandbox restore 不丢已确认写入。 |
| rollback | 原生需要手动改回 | Supported：rollback 到 checkpoint | rollback 后 layer 仍需保证事件、FUSE cache 和 sequence 一致。 | P1 | API + FUSE restore e2e 覆盖多 op 类型。 |
| commit | 写入已经在 base | Partial：commit 将 layer entries 应用到 base | 非严格全局事务；失败恢复依赖 snapshot/best-effort rollback；缺 exactly-once ledger。 | P0 | 冲突、失败恢复、重复 commit、large file commit 测试。 |
| 放弃/删除 layer | 删除未提交工作区 | Gap：无 delete/archive API | layer object 和 table 数据保留，缺 retention/GC。 | P0 | delete/archive/retention/GC 语义和测试。 |
| 分享/迁移 layer | 复制 workspace 或 base 数据 | Gap | 无 portable export/import、无 session bundle。 | P2 | export/import 包含 metadata、objects、tags、checkpoints。 |
| fork/branch layer | 原生不适用 | Gap | 无 layer chain、child layer、multi-lower。 | P3 | fork、merge、squash 设计后再实现。 |

## 4. API 和 SDK 矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| `POST /v1/layers` | Supported | base root 范围校验依赖服务端 canonical path；需要持续硬化路径边界。 | LayerFS core | P1 | base root、name、tag、durability、actor 测试。 |
| `GET /v1/layers` | Supported | list/filter 维度有限；无 pagination 强约束说明。 | LayerFS core | P2 | 大量 layer 分页/过滤测试。 |
| `GET /v1/layers/{ref}` | Supported | ref 可为 id/name/tag；歧义错误需要稳定错误码。 | LayerFS core | P1 | id/name/tag/ambiguous/not-found 测试。 |
| `GET /v1/layers/{ref}/diff` | Partial | 只返回 entry 级 diff；无 patch、无 content preview、无 tree summary。 | LayerFS core | P2 | diff 输出 schema 固化。 |
| `PUT/POST /v1/layers/{ref}/entries` | Partial | JSON body 受 128 MiB 限制；适合 small/metadata entry，不适合大对象。 | Native parity | P1 | small file、metadata op、body limit 测试。 |
| `PUT/POST /v1/layers/{ref}/objects` | Partial | 支持 raw object upload；缺 multipart、resume、range、direct upload URL。 | Native parity | P0 | >2 MiB、>96 MiB、大于内存阈值 e2e。 |
| `GET /v1/layers/{ref}/objects` | Partial | 可 stream read；缺 Range request、ETag/conditional request 语义。 | Native parity | P1 | 大文件 streaming read 和 checksum 测试。 |
| `POST /v1/layers/{ref}/checkpoints` | Partial | durable seq 可用；缺 wait-for-flush / dirty-handle barrier。 | LayerFS core | P0 | checkpoint 前后跨 mount restore 不丢确认写。 |
| `GET /v1/layer-checkpoints/{id}` | Supported | checkpoint 查询按 id；缺按 layer/name label 查询。 | LayerFS core | P2 | label 重名、not-found、cross-tenant 测试。 |
| `GET /v1/layers/{ref}/events` | Partial | polling；event payload 只包含基本 op/path/seq，未填 actor/before/after/idempotency。 | AgentFS-class | P1 | 多客户端刷新、事件去重、断点续拉测试。 |
| `POST /v1/layers/{ref}/rollback` | Supported | rollback 对已提交/冲突状态的策略需明确；FUSE cache 同步依赖事件。 | LayerFS core | P1 | rollback 后 diff、FUSE restore、event seq 测试。 |
| `POST /v1/layers/{ref}/commit` | Partial | 有 preflight 和 best-effort rollback；缺严格事务、ledger、后台恢复任务。 | Ops-prod | P0 | 注入失败、重复请求、并发 commit 测试。 |
| Go client | Supported | Go SDK 覆盖 layer API 和 object stream。 | Native parity | P1 | client 单测覆盖 endpoint path、large stream。 |
| JS/Python/其他 SDK | Gap | 若 Drive9 公开多语言 SDK，需要补 layer surface。 | Native parity | P2 | SDK parity matrix 和 generated client 测试。 |
| OpenAPI/API docs | Partial | 需要确认公开 docs 跟随 `/v1/layers` 新 endpoint。 | Ops-prod | P1 | API 文档、examples、错误码表。 |
| Backward compatibility | Partial | 旧 `/v1/fs-layers` 已被重命名；若已有试用用户需迁移说明。 | Ops-prod | P2 | changelog 和兼容窗口决策。 |

## 5. CLI 矩阵

### 5.1 Layer 生命周期命令

| 命令 | 当前 LayerFS 状态 | 差距/风险 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- |
| `drive9 fs layer create` | Supported | `--name`/`--tag` 可创建；缺唯一性策略和 rename/tag update。 | P1 | id/name/tag create e2e。 |
| `drive9 fs layer list` | Supported | 输出维度基础；大列表分页体验未验证。 | P2 | JSON/text 输出 snapshot 测试。 |
| `drive9 fs layer status` | Supported | 能按 id/name/tag 查；歧义错误需保持清晰。 | P1 | ambiguous ref 测试。 |
| `drive9 fs layer diff` | Partial | entry 列表，不是 patch；不展示大文件预览。 | P2 | 各 op diff e2e。 |
| `drive9 fs layer checkpoint` | Partial | `--wait` 目前不是完整 flush barrier。 | P0 | dirty handle/checkpoint 语义测试。 |
| `drive9 fs layer rollback` | Supported | 无 dry-run、无 conflict explain。 | P2 | rollback 多 op e2e。 |
| `drive9 fs layer commit` | Partial | text 输出；缺 `--json`、dry-run、conflict explain。 | P1 | commit 成功/冲突/重复 commit 测试。 |
| `drive9 fs layer delete/archive` | Gap | 不能释放未提交 layer 数据。 | P0 | delete/archive + object GC 测试。 |
| `drive9 fs layer tag/name update` | Gap | 创建后不能修改 name/tag。 | P2 | update/list/resolve 测试。 |
| `drive9 fs layer export/import` | Gap | 无 portable session。 | P2 | bundle 兼容和 checksum 测试。 |

### 5.2 普通 `drive9 fs` 命令在 `--layer` 下的能力

| 命令/能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| `cp --layer local -> remote` small file | Supported | 走 inline entry。 | Native parity | P1 | small file e2e。 |
| `cp --layer local -> remote` large file | Partial | 支持 raw object upload；缺 multipart/resume/progress consistency。 | Native parity | P0 | 100 MiB+ e2e，checksum 校验。 |
| `cp --layer stdin -> remote` | Partial | 需要确认大 stdin 不全量进内存。 | Native parity | P1 | stdin small/large e2e。 |
| `cp --layer remote -> remote` | Partial | copy-up 可能通过 client read 后重新上传；大远端对象 streaming 语义需验证。 | Native parity | P1 | remote copy large e2e。 |
| `cp --layer remote -> local` | Gap | 读本地下载不需要写 layer，但命令语义需明确是否允许。 | Native parity | P2 | 明确允许/拒绝并测试。 |
| `cp --layer -r` | Gap | recursive copy 显式不足；目录树 copy-up 语义未定义。 | Native parity | P1 | 递归目录、冲突、symlink、large file 测试。 |
| `cp --layer --append` | Gap | append/resume 与 layer entry 序列、partial object 未定义。 | Native parity | P1 | append/resume 设计和 e2e。 |
| `cp --layer --resume` | Gap | 同上。 | Native parity | P1 | resume 断点测试。 |
| `cp --layer --tag/--description` | Gap | layer entry 尚未承载 file tags/description parity。 | Native parity | P1 | tags/description commit 后一致。 |
| `rm --layer file` | Supported | whiteout file。 | Native parity | P1 | file delete e2e。 |
| `rm --layer empty-dir -r` | Supported | 空目录 whiteout/remove 可用。 | Native parity | P1 | empty dir delete e2e。 |
| `rm --layer non-empty-dir -r` | Partial | 当前为 conflict/拒绝；缺 recursive whiteout 或 opaque dir。 | Native parity | P1 | 非空目录删除语义选型和测试。 |
| `mkdir --layer` | Supported | mkdir entry。 | Native parity | P1 | mkdir/list/commit e2e。 |
| `chmod --layer file` | Supported | file chmod 可用。 | Native parity | P1 | file mode e2e。 |
| `chmod --layer dir/symlink` | Partial | CLI helper 对 kind 推断有限；FUSE/API 可表达更多。 | Native parity | P1 | dir/symlink mode 测试。 |
| `mv --layer file` | Supported | rename entry。 | Native parity | P1 | file rename e2e。 |
| `mv --layer dir` | Partial | API/FUSE 可支持目录 rename；CLI helper 目前 kind 推断需补齐。 | Native parity | P1 | dir rename subtree e2e。 |
| `ln -s --layer` | Supported | symlink entry。 | Native parity | P1 | symlink read/commit e2e。 |
| `ln --layer` hardlink | Partial | 当前更接近 copy-up；不保留 hardlink identity/nlink。 | Native parity | P2 | 明确 hardlink 策略或实现 inode parity。 |
| `cat --layer` | Gap | 直接 CLI read 未 overlay-aware。 | Native parity | P1 | cat layer/base/whiteout e2e。 |
| `ls --layer` | Gap | 直接 CLI list 未 overlay-aware。 | Native parity | P1 | list merged dirs/whiteout e2e。 |
| `stat --layer` | Gap | 直接 CLI stat 未 overlay-aware。 | Native parity | P1 | stat size/mode/symlink e2e。 |
| `grep --layer` | Partial | overlay grep 可用；不使用语义索引，仅 stream scan。 | Native parity | P1 | base + layer + whiteout e2e。 |
| `find --layer` | Partial | overlay find 可用；tag filters 不匹配 layer entries。 | Native parity | P1 | name/size/time/tag e2e。 |
| `pack --layer` | Gap | pack/unpack 未定义 layer 视图和写入语义。 | Native parity | P1 | pack overlay read、unpack layer write 测试。 |
| `unpack --layer` | Gap | 同上。 | Native parity | P1 | overwrite/delete/metadata 测试。 |
| `git ... --layer` direct CLI | Gap | git workspace 主要通过 FUSE mount 间接工作；直连 git 子命令未 layer-aware。 | Native parity | P2 | 明确支持边界和测试。 |
| context-scoped remote paths + `--layer` | Gap | 当前显式拒绝，防止 cross-context layer 语义不清。 | Native parity | P2 | 多 context 设计后实现。 |

## 6. Mount/FUSE/POSIX 矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| FUSE `--layer` mount | Supported | 仅 FUSE。 | LayerFS core | P1 | mount smoke e2e。 |
| WebDAV `--layer` mount | Gap | 当前未支持；需决定是否纳入 V1 parity。 | Native parity | P2 | WebDAV semantics 文档/测试。 |
| `--checkpoint` restore mount | Partial | 可按 durable seq 恢复；active-only layer 限制需确认。 | LayerFS core | P1 | checkpoint mount e2e。 |
| Base fallback read | Supported | 未命中 layer 时读 base。 | LayerFS core | P1 | base file read e2e。 |
| Copy-up on write | Supported | 通过 shadow/pending/local overlay + commit queue 写 layer。 | LayerFS core | P1 | overwrite existing base file e2e。 |
| Whiteout | Supported | file/empty dir 可用；非空 dir 缺 opaque/recursive。 | LayerFS core | P1 | whiteout list/read/commit 测试。 |
| Local overlay precedence | Supported | local hot path 优先，必要时上传 layer/backend。 | Native parity | P1 | local overlay + layer 写入测试。 |
| Git workspace coexistence | Partial | 通过 mount 路径可共存；git workspace 专属行为未系统验证。 | Native parity | P1 | git clone/status/commit-like workload e2e。 |
| Shadow store / spill | Supported | 大文件可避免 JSON inline。 | Native parity | P0 | spill + object upload 测试。 |
| Commit queue to layer | Supported | `--layer` mount 写入进入 layer entries/object，而不是 base commit queue。 | LayerFS core | P1 | 写入后 base 未变、diff 可见。 |
| `flush`/`fsync`/`release` durability | Partial | 已有 close/writeback 测试；checkpoint wait barrier 仍需补。 | Ops-prod | P0 | dirty handle、crash、unmount 测试。 |
| Multi-client event refresh | Partial | 1s polling；无 SSE/watch；payload 简化。 | AgentFS-class | P1 | 两个 sandbox 同 layer 同步 e2e。 |
| Rename file | Supported | no-replace 语义。 | Native parity | P1 | rename conflict 测试。 |
| Rename directory | Partial | API/FUSE 支持，snapshot rollback 覆盖 subtree；CLI parity 待补。 | Native parity | P1 | dir rename rollback/commit e2e。 |
| Truncate | Partial | 基础写路径可表达；sparse/hole/large truncate 未系统覆盖。 | Native parity | P1 | truncate grow/shrink/sparse 测试。 |
| Append | Partial | FUSE append 依赖 write path；CLI append 缺失。 | Native parity | P1 | append across reopen/crash 测试。 |
| Hardlink | Partial | 当前语义接近复制内容，不保留 inode identity。 | Native parity | P2 | hardlink decision + tests。 |
| Symlink | Supported | symlink entry。 | Native parity | P1 | symlink lstat/readlink/commit 测试。 |
| File mode | Partial | mode 可记录；uid/gid/mtime/xattr 不完整。 | Native parity | P1 | chmod + metadata parity 测试。 |
| UID/GID | Gap | 未作为 layer metadata 一等字段。 | Native parity | P2 | chown/chgrp 策略。 |
| mtime/atime/ctime | Partial | created/updated 存在；POSIX timestamp parity 不完整。 | Native parity | P2 | timestamp preservation tests。 |
| xattr | Gap | 未 layer-aware。 | Native parity | P2 | xattr API/FUSE tests。 |
| File locks/flock | Gap | 未定义 layer 多 sandbox 锁语义。 | Native parity | P2 | lock contention tests。 |
| mmap | Partial | 依赖 FUSE 通用读写；未专项测试。 | Native parity | P2 | mmap write/read e2e。 |
| Sparse file/fallocate | Gap | 未定义 hole preservation。 | Native parity | P2 | sparse file tests。 |
| Directory listing consistency | Partial | FUSE restore/overlay 合并；直接 CLI `ls --layer` 缺失。 | Native parity | P1 | list after write/delete/rollback。 |
| Unmount with pending writes | Partial | 已走 commit queue flush；异常断电恢复未充分测试。 | Ops-prod | P0 | kill/crash/failpoint e2e。 |

## 7. 数据面和元数据矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| Small file inline content | Supported | 受 JSON body/inline threshold 限制。 | LayerFS core | P1 | inline checksum/content type 测试。 |
| Large file object content | Partial | 支持 raw upload/read；缺 multipart、resume、range。 | Native parity | P0 | 100 MiB+ upload/read/commit e2e。 |
| S3/local object backend reuse | Partial | 复用 object storage；layer object 生命周期未独立管理。 | Native parity | P0 | orphan GC、retention tests。 |
| Encryption metadata | Partial | entry 有 encryption 字段；完整端到端验证需补。 | Native parity | P1 | encrypted large/small file tests。 |
| Checksum | Supported | upsert/object entry 记录 checksum。 | Native parity | P1 | checksum mismatch/final verification。 |
| Content type | Partial | inline 和 object metadata 可记录；检测一致性需验证。 | Native parity | P2 | MIME detection tests。 |
| File tags | Gap | layer entry 没有完整文件 tag parity；`find --tag` 不匹配 layer entries。 | Native parity | P1 | write/list/find/commit tags e2e。 |
| Description/semantic text | Gap | layer 中未提交内容不进入 extraction/index pipeline。 | Native parity | P1 | pre-commit metadata + commit 后一致。 |
| Base revision conflict | Supported | entry 携带 base inode/revision，commit preflight 检查。 | LayerFS core | P1 | stale base conflict tests。 |
| Rename target conflict | Supported | commit preflight 检查目标。 | LayerFS core | P1 | target exists tests。 |
| Directory subtree snapshot | Partial | commit 失败 rollback 使用 snapshot；规模和超时未压测。 | Ops-prod | P0 | large subtree rollback failpoint。 |
| Content-addressed dedup | Gap | layer object 不去重。 | Ops-prod | P2 | dedup design if needed。 |
| Quota accounting | Gap | layer object 和 entry 未形成独立 quota。 | Ops-prod | P0 | per-tenant/per-layer quota tests。 |
| Retention/GC | Gap | 未提交/已提交/回滚 layer 数据生命周期未实现。 | Ops-prod | P0 | GC reachability + safety tests。 |
| Object range read | Gap | 大文件读取缺 Range。 | Native parity | P1 | range/partial download tests。 |
| Direct-to-object-store upload | Gap | 服务端代理上传，成本和吞吐受限。 | Ops-prod | P2 | presigned/multipart tests。 |
| Cross-sandbox restore | Partial | checkpoint/event restore 可用；dirty-handle barrier 和 crash recovery 不足。 | Ops-prod | P0 | kill/restart/restore no-data-loss tests。 |

## 8. 搜索、发现和索引矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| Base search unchanged | Parity | 非 layer 查询不受影响。 | Native parity | P1 | regression tests。 |
| `grep --layer` overlay text search | Partial | stream scan，非索引；大规模 layer 会慢。 | Native parity | P1 | large layer grep perf/e2e。 |
| `find --layer` path/name search | Partial | 可合并 base/layer；tag filters 缺 layer entry 支持。 | Native parity | P1 | whiteout、rename、mkdir、tag 测试。 |
| Semantic/vector search before commit | Gap | 未提交 layer 内容不参与 embedding。 | Native parity | P1 | pre-commit semantic indexing design/tests。 |
| Semantic/vector search after commit | Partial | commit 后进入原生 pipeline，但异步时序需验证。 | Native parity | P1 | commit 后 embedding availability tests。 |
| Media/image/audio extraction | Gap | layer 未提交对象没有 extraction path。 | Native parity | P2 | extraction job e2e。 |
| Search by checkpoint | Gap | `grep/find` 无 checkpoint ref。 | LayerFS core | P2 | checkpoint search tests。 |
| Ranking and merge semantics | Partial | base + layer merge 以 overlay correctness 为主，ranking 简化。 | Native parity | P2 | score/order stability tests。 |
| Tag-key existence search | Gap | layer entries 不支持 tag-key existence matching。 | Native parity | P1 | `find -tag key --layer` tests。 |

## 9. 并发、冲突和事务矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| 多 entry 顺序 | Supported | layer entry seq 作为顺序源。 | LayerFS core | P1 | same path multi-write tests。 |
| 同 layer 多 writer | Partial | last-entry-wins 视图可工作；缺冲突提示、锁、actor attribution。 | AgentFS-class | P1 | multi-client concurrent write tests。 |
| event watch | Partial | polling + full restore；无 incremental apply optimization。 | AgentFS-class | P2 | event lag/perf tests。 |
| commit 与 active writes | Partial | 状态切换 active/committing；仍需阻断 late writes 和清晰错误。 | Ops-prod | P0 | concurrent write during commit tests。 |
| conflict preflight | Supported | stale base、target exists、non-empty dir 等已覆盖部分。 | LayerFS core | P1 | conflict matrix tests。 |
| conflict explain | Gap | 用户只能看到错误，缺结构化冲突报告。 | AgentFS-class | P2 | conflict report API/CLI。 |
| rebase | Gap | base 变化后不能自动 rebase layer。 | AgentFS-class | P2 | rebase design/tests。 |
| exactly-once commit | Gap | 无 durable apply ledger；重试依赖状态和 snapshot。 | Ops-prod | P0 | failpoint crash at each commit phase。 |
| rollback after failed commit | Partial | best-effort snapshot rollback。 | Ops-prod | P0 | injected failure and recovery tests。 |
| idempotency key | Gap | events table 有字段但 API 未形成完整 idempotency 语义。 | Ops-prod | P1 | duplicate request tests。 |
| distributed locking | Gap | 无显式 per-layer/per-path lease。 | Ops-prod | P2 | lock/lease tests if introduced。 |

## 10. 安全和权限矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| Tenant isolation | Partial | 复用现有 auth/tenant 体系；layer ref 解析需持续验证 cross-tenant 不泄漏。 | Ops-prod | P0 | cross-tenant id/name/tag tests。 |
| Path scope isolation | Partial | server 校验 entry path 在 base root 内；路径边界需 fuzz/hardening。 | Ops-prod | P0 | `/base2`、unicode、`..`、symlink scope tests。 |
| Per-layer ACL | Gap | 无 layer 级 share/read/write/commit 权限。 | Ops-prod | P1 | ACL API + auth tests。 |
| Delegated token / sandbox token | Gap | 无只允许写特定 layer 的 token。 | AgentFS-class | P1 | scoped token tests。 |
| Progressive permission | Gap | 未实现类似 YoloFS 的按操作授权。 | AgentFS-class | P3 | permission prompt/audit design。 |
| Audit log/provenance | Partial | events 记录 op/path/seq；actor/before/after/idempotency 未完整填充。 | AgentFS-class | P1 | audit completeness tests。 |
| Secret redaction/exfil controls | Gap | layer read/write 没有额外安全策略。 | Ops-prod | P2 | policy hooks tests。 |
| Integrity verification | Partial | checksum 存在；无 tamper-evident audit chain。 | Ops-prod | P2 | checksum verify + audit chain if needed。 |

## 11. 运维、成本和生产矩阵

| 能力 | 当前 LayerFS 状态 | 差距/风险 | 目标 | 优先级 | 必需验收 |
| --- | --- | --- | --- | --- | --- |
| Schema init | Supported | 新表已进入 tenant schema。 | Ops-prod | P1 | schema dump/init tests。 |
| Existing tenant migration | Partial | 需要确认生产迁移流程、rolling deploy、backfill/ALTER 策略。 | Ops-prod | P0 | migration dry-run and rollback plan。 |
| Feature flag / rollout | Partial | 功能 opt-in by `--layer`；服务端开关、租户开关未完整。 | Ops-prod | P1 | feature flag tests。 |
| Metrics | Gap | 缺 layer writes、object bytes、commit latency、conflicts、GC metrics。 | Ops-prod | P0 | metrics assertions/integration。 |
| Tracing/logging | Partial | 使用现有日志；缺 layer-specific spans/trace fields。 | Ops-prod | P1 | trace sampling and log field review。 |
| Dashboards/alerts | Gap | 无生产 dashboard。 | Ops-prod | P1 | SLO dashboard and alert runbook。 |
| Quota/cost attribution | Gap | layer object bytes 未独立计费/限额。 | Ops-prod | P0 | quota enforcement e2e。 |
| Retention policy | Gap | 无 TTL、archive、delete。 | Ops-prod | P0 | retention safety tests。 |
| Backup/restore | Partial | 依赖 DB/object store 备份；无 layer-level restore/export。 | Ops-prod | P1 | restore from backup drill。 |
| Load/perf benchmark | Gap | 缺 layer-specific benchmark。 | Ops-prod | P0 | write/read/commit/search benchmarks。 |
| Chaos/failpoint | Gap | commit/checkpoint/crash 缺系统 failpoint 测试。 | Ops-prod | P0 | failpoint suite for commit phases。 |
| Compatibility matrix | Gap | OS、FUSE/WebDAV、object backends、DB versions 未形成矩阵。 | Ops-prod | P2 | release qualification matrix。 |
| Cleanup of abandoned uploads | Gap | raw object upload 失败/abandon 的清理策略未实现。 | Ops-prod | P0 | orphan object tests。 |

## 12. 社区 LayerFS 能力对比矩阵

| 社区方向 | Drive9 当前状态 | 差距/风险 | 目标 | 优先级 |
| --- | --- | --- | --- | --- |
| Read-only base + writable upper | Supported | 与 AgentFS/ArtifactFS/overlayfs 方向一致。 | LayerFS core | P1 |
| Copy-up | Supported | 大对象 copy-up 仍需 streaming/remote copy 强化。 | LayerFS core | P1 |
| Whiteout | Partial | file/empty dir 支持；opaque dir/recursive whiteout 缺失。 | LayerFS core | P1 |
| Checkpoint/snapshot | Supported | 以 durable seq 记录；不是 DeltaFS 式层冻结/插层优化。 | LayerFS core | P1 |
| Commit/rollback | Partial | 可用但生产事务性不足。 | LayerFS core | P0 |
| Portable session DB/bundle | Gap | AgentFS SQLite session 类能力未实现。 | AgentFS-class | P2 |
| Provenance audit | Partial | event 表基础存在，未填完整上下文。 | AgentFS-class | P1 |
| Progressive permission/staged effects | Gap | YoloFS 类能力未实现。 | AgentFS-class | P3 |
| Branch/fork/merge layer chain | Gap | 无 multi-layer lower/upper chain。 | AgentFS-class | P3 |
| Squash/compact | Gap | 无 layer compaction。 | AgentFS-class | P2 |
| Runtime sandbox integration | Partial | Drive9 提供 filesystem；process/syscall isolation 依赖外部 sandbox。 | AgentFS-class | P2 |
| Bucket/volume mounts | N/A | Drive9 后端已有 S3/db9，不等同 Cloudflare bucket mount。 | AgentFS-class | P3 |
| Artifact hydrate/lazy blob | Partial | object stream 可读；缺 hydrate cache/range/lazy fetch 策略。 | AgentFS-class | P2 |
| Offline operation | Gap | 目前依赖后端服务。 | AgentFS-class | P3 |

## 13. 测试矩阵

| 测试层 | 当前状态 | 缺口 | 优先级 | 必需补充 |
| --- | --- | --- | --- | --- |
| Datastore unit tests | Partial | 需要覆盖 tag/name ambiguity、pagination、GC、quota、migration。 | P1 | `pkg/datastore` layer CRUD/conflict tests。 |
| Server unit tests | Partial | 需要覆盖 object range/multipart、commit failpoint、auth/path fuzz。 | P0 | commit phase failpoint suite。 |
| Client unit tests | Partial | 需要覆盖 all endpoints、streaming、error schema。 | P1 | endpoint path and retry tests。 |
| CLI unit tests | Partial | 生命周期和部分写命令已覆盖；读命令、recursive、metadata 缺。 | P1 | command matrix tests。 |
| FUSE unit tests | Partial | close/writeback/layer restore 有覆盖；POSIX edge 缺。 | P1 | hardlink/xattr/mmap/truncate/sparse tests。 |
| E2E smoke | Supported | 已覆盖 API lifecycle、CLI small/large、grep/find、FUSE restore/commit。 | P1 | 作为 PR 必跑 smoke。 |
| E2E large file | Partial | 有 100 MiB CLI；还需 FUSE、remote copy、range/resume。 | P0 | >100 MiB multi-path tests。 |
| E2E multi-sandbox | Gap | 缺两个 sandbox 同 layer restore/event。 | P0 | writer/reader sandbox no-data-loss test。 |
| E2E crash recovery | Gap | 缺 kill server/client/unmount 中断恢复。 | P0 | crash at upload/checkpoint/commit tests。 |
| E2E metadata parity | Gap | tag/description/mode/timestamps/xattr 缺。 | P1 | metadata matrix e2e。 |
| E2E search parity | Partial | grep/find 有基础；semantic/tag/checkpoint search 缺。 | P1 | search matrix e2e。 |
| E2E pack/unpack/git | Gap | 未覆盖。 | P1 | real workflow tests。 |
| Performance benchmarks | Gap | 无 layer-specific latency/throughput/cost benchmark。 | P0 | local overlay, object upload, commit, search benchmark。 |
| Fuzz/property tests | Gap | path scope、overlay merge、rename/whiteout order 可 fuzz。 | P1 | path and entry sequence fuzz tests。 |

## 14. 分阶段 Roadmap

### 14.1 P0：生产级阻断

| 项目 | 交付物 |
| --- | --- |
| Commit exactly-once/recovery | Durable apply ledger、phase recovery、failpoint suite、重复 commit 安全。 |
| Cross-sandbox no-data-loss | checkpoint dirty-handle barrier、event restore 强化、multi-sandbox crash e2e。 |
| Layer object lifecycle | delete/archive、GC reachability、orphan cleanup、retention policy。 |
| Quota/cost control | per-layer/per-tenant bytes、entry count、object count 限额和 metrics。 |
| Large file production path | multipart/resume/range/direct upload 或明确替代方案。 |
| Observability | metrics、tracing fields、dashboard、alerts、runbook。 |
| Security hardening | path scope fuzz、cross-tenant tests、scoped token/ACL 最小实现。 |
| Migration/rollout | production schema migration、feature flag、rollback plan。 |

### 14.2 P1：原生 Drive9 parity

| 项目 | 交付物 |
| --- | --- |
| CLI read parity | `cat/ls/stat --layer`，覆盖 base/layer/whiteout/rename/checkpoint。 |
| CLI write parity | recursive `cp/rm`、append/resume、remote copy streaming、metadata flags。 |
| Metadata parity | tags、description、content type、mode/timestamps、commit 后一致。 |
| Search parity | layer tag search、semantic pre-commit 或明确延迟策略、checkpoint search。 |
| FUSE POSIX parity | dir rename、truncate、append、symlink、hardlink 策略、xattr/timestamps。 |
| Conflict UX | structured conflict report、dry-run commit、rebase 设计。 |
| Pack/unpack/git workflows | 明确 layer-aware 行为并补真实工作流 e2e。 |

### 14.3 P2/P3：AgentFS-class 和生态增强

| 项目 | 交付物 |
| --- | --- |
| Portable session | export/import bundle，包含 objects、metadata、checkpoints、audit。 |
| Provenance audit | actor、tool call、before/after、idempotency、tamper-evident chain。 |
| Branch/fork/merge | child layer、multi-lower、squash/compact、merge conflict model。 |
| Progressive permission | staged effects、policy hooks、approval workflow。 |
| Offline/lazy hydrate | local session cache、range hydrate、offline replay。 |
| Multi-language SDK | layer API parity across published SDKs。 |

## 15. 后续开发 Definition of Done

任何本 matrix 中 `Gap` 或 `Partial` 项被声明完成时，至少需要同时满足：

1. 实现代码路径，不影响非 layer 原生行为和性能基线。
2. 更新 CLI/API 文档、错误码和用户交互说明。
3. 单元测试覆盖成功、冲突、权限、边界和回归路径。
4. E2E 覆盖真实用户旅程，尤其是跨 sandbox restore、large file、commit/rollback。
5. 若涉及 schema、object lifecycle、安全或运维，补迁移、metrics、GC/retention、runbook。
6. 更新本 matrix 状态；若能力被明确排除，标记为 `N/A` 并写明原因。
