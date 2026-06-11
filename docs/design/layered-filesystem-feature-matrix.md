# Drive9 LayerFS Feature Matrix

This document is the capability matrix for Drive9 + LayerFS. Future LayerFS work should use it as both the backlog and the acceptance checklist. Any addition, removal, or semantic change must update this file so that layer mode and native Drive9 do not silently diverge.

Current baseline: PR #507 / `feat/layer`, as of `61ee9e4 refactor: rename layer API endpoints`.

Related design documents:

- [Layered Filesystem V1 Design](./layered-filesystem-v1-design.md)
- [Layered Filesystem Research](./layered-filesystem-research.md)

## 1. How To Read This Matrix

### 1.1 Status

| Status | Meaning |
| --- | --- |
| Parity | Layer mode and native non-layer Drive9 are effectively equivalent in user-visible capability, semantics, error behavior, and performance class. |
| Supported | A LayerFS-specific capability is available and the current implementation meets the V1 design goal. |
| Partial | The capability is usable, but has known gaps in behavior, performance, edge semantics, or test coverage. |
| Gap | The capability is not implemented, or an existing command/API explicitly rejects the `--layer` scenario. |
| N/A | The capability is not a LayerFS goal, or can only be satisfied indirectly through existing non-layer behavior. |

### 1.2 Priority

| Priority | Meaning |
| --- | --- |
| P0 | Production blocker. LayerFS should not be called production-ready while this is missing. |
| P1 | Native Drive9 parity blocker. This affects common user paths or migration experience. |
| P2 | Quality, scale, performance, or operational completeness issue. It does not block early trials, but limits scale. |
| P3 | Ecosystem, experience, or future enhancement item. |

### 1.3 Target Type

| Target | Meaning |
| --- | --- |
| Native parity | Keep behavior consistent with non-layer Drive9. |
| LayerFS core | Core capability required by a layered filesystem. |
| AgentFS-class | Capability aligned with emerging community systems such as AgentFS, Cloudflare Sandbox FS, and ArtifactFS. |
| Ops-prod | Production operations, security, observability, recovery, and cost-control capability. |

## 2. Summary

| Area | Current assessment | Follow-up focus |
| --- | --- | --- |
| Native Drive9 behavior without layer | Parity | The current implementation remains opt-in. Existing read/write, mount, local overlay, git workspace, pack/unpack, and related paths are unchanged when `--layer` is not set. |
| LayerFS core model | Supported | Base root + writable layer, copy-up, whiteout, checkpoint, rollback, commit, and name/tag refs are available. |
| FUSE layer mount | Partial | Writes, restore, and commit work. POSIX edges, open dirty-handle checkpoint barriers, multi-client consistency, and WebDAV strategy still need work. |
| CLI layer parity | Partial | A batch of key write commands is covered, but direct read/composite commands such as `cat`, `ls`, `stat`, `pack`, `unpack`, and `git` are not layer-aware yet. |
| Large files | Partial | Local large files and FUSE spill can use object upload. Multipart, resume, range, direct-to-object-store, quota, and GC are still missing. |
| Search and indexing | Partial | `grep/find --layer` can produce an overlay view. Semantic search, tag search, pre-commit indexing, and checkpoint search are still missing. |
| Transactions and recovery | Partial | Commit has preflight, snapshot, best-effort rollback, and committing recovery. It is not yet strict exactly-once, globally transactional, or backed by an auditable ledger. |
| Community AgentFS/Cloudflare-class capability | Partial | The basic overlay direction matches the community trend. Portable session export, provenance audit, progressive permission, branch/fork, and squash/compact are missing. |
| Production readiness | Partial | GC/retention/quota, observability, load/stress/failpoint coverage, schema rollout, and security hardening are still missing. |

## 3. User Journey Matrix

| Journey | Native non-layer behavior | Current LayerFS status | Gap/risk | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| Create layer | Write directly to the base filesystem. | Supported: `drive9 fs layer create /base --name ... --tag ...` | Names/tags do not have uniqueness constraints; ambiguous refs require the user to specify a clearer ref. | P1 | CLI/API unit tests cover id/name/tag creation and resolution. |
| Mount layer | `drive9 mount` reads and writes base directly. | Supported: FUSE `drive9 mount --layer <ref>` | FUSE only. WebDAV mode is unsupported. Checkpoint mount currently depends on an active layer. | P1 | FUSE e2e covers create/write/restore/commit. |
| Modify file | Write directly to base and enter the native commit queue. | Supported: writes go to layer entries/objects and do not mutate base directly. | Write destination switches to the layer table/object store; users must understand base is unchanged before commit. | P1 | FUSE + CLI write e2e and assertions that base stays unchanged. |
| View overlay result | Native read/list/stat. | Partial: overlay is visible through FUSE; some direct CLI read commands are not layer-aware. | Direct CLI `cat/ls/stat` do not support `--layer`, so experience is inconsistent. | P1 | Direct CLI read commands support `--layer` and have e2e coverage. |
| Diff/review | Native Drive9 has no session diff. | Partial: `drive9 fs layer diff` returns an entry list. | No git-style patch, content preview, directory aggregation, or conflict markers. | P2 | API/CLI diff schema is stable and covers rename/delete/large object. |
| Checkpoint | Native Drive9 has no session checkpoint. | Supported: `layer checkpoint` records durable seq. | `--wait` is not yet a true dirty-handle barrier. | P0 | Cross-sandbox restore after checkpoint does not lose confirmed writes. |
| Rollback | User must manually revert changes. | Supported: rollback to checkpoint. | Rollback must keep events, FUSE cache, and sequence state consistent. | P1 | API + FUSE restore e2e covers multiple op types. |
| Commit | Writes are already in base. | Partial: commit applies layer entries to base. | No strict global transaction; failure recovery relies on snapshot/best-effort rollback; no exactly-once ledger. | P0 | Conflict, failure recovery, repeated commit, and large-file commit tests. |
| Abandon/delete layer | Delete an uncommitted workspace manually. | Gap: no delete/archive API. | Layer object and table data are retained; retention/GC are missing. | P0 | Delete/archive/retention/GC semantics and tests. |
| Share/migrate layer | Copy workspace or base data. | Gap | No portable export/import or session bundle. | P2 | Export/import includes metadata, objects, tags, and checkpoints. |
| Fork/branch layer | N/A for native Drive9. | Gap | No layer chain, child layer, or multi-lower support. | P3 | Design fork, merge, and squash before implementation. |

## 4. API And SDK Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| `POST /v1/layers` | Supported | Base-root validation depends on server-side canonical paths; path boundaries need continued hardening. | LayerFS core | P1 | Base root, name, tag, durability, and actor tests. |
| `GET /v1/layers` | Supported | List/filter dimensions are limited; pagination contract is not strongly specified. | LayerFS core | P2 | Tests for pagination/filtering with many layers. |
| `GET /v1/layers/{ref}` | Supported | `ref` may be id/name/tag; ambiguous errors need stable error codes. | LayerFS core | P1 | id/name/tag/ambiguous/not-found tests. |
| `GET /v1/layers/{ref}/diff` | Partial | Entry-level diff only; no patch, content preview, or tree summary. | LayerFS core | P2 | Diff output schema is fixed. |
| `PUT/POST /v1/layers/{ref}/entries` | Partial | JSON body is limited to 128 MiB; suitable for small/metadata entries, not large objects. | Native parity | P1 | Small file, metadata op, and body-limit tests. |
| `PUT/POST /v1/layers/{ref}/objects` | Partial | Supports raw object upload; multipart, resume, range, and direct upload URL are missing. | Native parity | P0 | e2e for >2 MiB, >96 MiB, and larger-than-memory-threshold files. |
| `GET /v1/layers/{ref}/objects` | Partial | Streaming read works; Range request and ETag/conditional semantics are missing. | Native parity | P1 | Large-file streaming read and checksum tests. |
| `POST /v1/layers/{ref}/checkpoints` | Partial | Durable seq works; wait-for-flush / dirty-handle barrier is missing. | LayerFS core | P0 | Cross-mount restore around checkpoint does not lose confirmed writes. |
| `GET /v1/layer-checkpoints/{id}` | Supported | Checkpoint lookup by id only; no lookup by layer/name label. | LayerFS core | P2 | Label duplicate, not-found, and cross-tenant tests. |
| `GET /v1/layers/{ref}/events` | Partial | Polling only; event payload contains basic op/path/seq and does not fully fill actor/before/after/idempotency. | AgentFS-class | P1 | Multi-client refresh, event dedupe, and resume-from-offset tests. |
| `POST /v1/layers/{ref}/rollback` | Supported | Behavior for committed/conflicted layers must be explicit; FUSE cache sync depends on events. | LayerFS core | P1 | Rollback diff, FUSE restore, and event seq tests. |
| `POST /v1/layers/{ref}/commit` | Partial | Has preflight and best-effort rollback; strict transaction, ledger, and background recovery tasks are missing. | Ops-prod | P0 | Injected failure, duplicate request, and concurrent commit tests. |
| Go client | Supported | Go SDK covers layer API and object streams. | Native parity | P1 | Client unit tests cover endpoint paths and large streams. |
| JS/Python/other SDKs | Gap | If Drive9 publishes multi-language SDKs, layer surfaces need parity. | Native parity | P2 | SDK parity matrix and generated-client tests. |
| OpenAPI/API docs | Partial | Public docs must be checked for the new `/v1/layers` endpoints. | Ops-prod | P1 | API docs, examples, and error-code table. |
| Backward compatibility | Partial | Old `/v1/fs-layers` was renamed; migration notes are needed if trial users exist. | Ops-prod | P2 | Changelog and compatibility-window decision. |

## 5. CLI Matrix

### 5.1 Layer Lifecycle Commands

| Command | Current LayerFS status | Gap/risk | Priority | Required acceptance |
| --- | --- | --- | --- | --- |
| `drive9 fs layer create` | Supported | `--name`/`--tag` can create layers; uniqueness policy and rename/tag update are missing. | P1 | id/name/tag create e2e. |
| `drive9 fs layer list` | Supported | Output is basic; large-list pagination UX is unverified. | P2 | JSON/text output snapshot tests. |
| `drive9 fs layer status` | Supported | Can query by id/name/tag; ambiguous errors must remain clear. | P1 | Ambiguous ref tests. |
| `drive9 fs layer diff` | Partial | Entry list, not a patch; no large-file preview. | P2 | Per-op diff e2e. |
| `drive9 fs layer checkpoint` | Partial | `--wait` is not yet a full flush barrier. | P0 | Dirty-handle/checkpoint semantics tests. |
| `drive9 fs layer rollback` | Supported | No dry-run or conflict explain. | P2 | Rollback multi-op e2e. |
| `drive9 fs layer commit` | Partial | Text output; `--json`, dry-run, and conflict explain are missing. | P1 | Successful, conflicted, and repeated commit tests. |
| `drive9 fs layer delete/archive` | Gap | Cannot release uncommitted layer data. | P0 | Delete/archive plus object GC tests. |
| `drive9 fs layer tag/name update` | Gap | Name/tag cannot be modified after creation. | P2 | Update/list/resolve tests. |
| `drive9 fs layer export/import` | Gap | No portable session. | P2 | Bundle compatibility and checksum tests. |

### 5.2 Ordinary `drive9 fs` Commands Under `--layer`

| Command/capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| `cp --layer local -> remote` small file | Supported | Uses inline entry. | Native parity | P1 | Small-file e2e. |
| `cp --layer local -> remote` large file | Partial | Supports raw object upload; multipart/resume/progress consistency is missing. | Native parity | P0 | 100 MiB+ e2e with checksum verification. |
| `cp --layer stdin -> remote` | Partial | Need to confirm large stdin is not fully loaded into memory. | Native parity | P1 | stdin small/large e2e. |
| `cp --layer remote -> remote` | Partial | Copy-up may read via client and upload again; streaming semantics for large remote objects need verification. | Native parity | P1 | Large remote-copy e2e. |
| `cp --layer remote -> local` | Gap | Local downloads do not need to write a layer, but command semantics must define whether this is allowed. | Native parity | P2 | Explicit allow/reject behavior and tests. |
| `cp --layer -r` | Gap | Recursive copy is explicitly incomplete; directory-tree copy-up semantics are undefined. | Native parity | P1 | Recursive directory, conflict, symlink, and large-file tests. |
| `cp --layer --append` | Gap | Append/resume semantics across layer entry sequence and partial objects are undefined. | Native parity | P1 | Append/resume design and e2e. |
| `cp --layer --resume` | Gap | Same as above. | Native parity | P1 | Resume interruption tests. |
| `cp --layer --tag/--description` | Gap | Layer entries do not yet provide file tag/description parity. | Native parity | P1 | Tags/description are consistent after commit. |
| `rm --layer file` | Supported | File whiteout. | Native parity | P1 | File delete e2e. |
| `rm --layer empty-dir -r` | Supported | Empty directory whiteout/remove works. | Native parity | P1 | Empty-directory delete e2e. |
| `rm --layer non-empty-dir -r` | Partial | Currently conflicts/rejects; recursive whiteout or opaque directory support is missing. | Native parity | P1 | Select semantics for non-empty directory delete and test them. |
| `mkdir --layer` | Supported | mkdir entry. | Native parity | P1 | mkdir/list/commit e2e. |
| `chmod --layer file` | Supported | File chmod works. | Native parity | P1 | File mode e2e. |
| `chmod --layer dir/symlink` | Partial | CLI helper has limited kind inference; FUSE/API can express more. | Native parity | P1 | Directory/symlink mode tests. |
| `mv --layer file` | Supported | Rename entry. | Native parity | P1 | File rename e2e. |
| `mv --layer dir` | Partial | API/FUSE can support directory rename; CLI helper kind inference needs work. | Native parity | P1 | Directory rename subtree e2e. |
| `ln -s --layer` | Supported | Symlink entry. | Native parity | P1 | Symlink read/commit e2e. |
| `ln --layer` hardlink | Partial | Current semantics are closer to copy-up and do not preserve hardlink identity/nlink. | Native parity | P2 | Explicit hardlink strategy or inode parity implementation. |
| `cat --layer` | Gap | Direct CLI read is not overlay-aware. | Native parity | P1 | cat layer/base/whiteout e2e. |
| `ls --layer` | Gap | Direct CLI list is not overlay-aware. | Native parity | P1 | List merged directories/whiteouts e2e. |
| `stat --layer` | Gap | Direct CLI stat is not overlay-aware. | Native parity | P1 | stat size/mode/symlink e2e. |
| `grep --layer` | Partial | Overlay grep works; it stream-scans and does not use semantic indexes. | Native parity | P1 | base + layer + whiteout e2e. |
| `find --layer` | Partial | Overlay find works; tag filters do not match layer entries. | Native parity | P1 | name/size/time/tag e2e. |
| `pack --layer` | Gap | pack/unpack layer view and write semantics are undefined. | Native parity | P1 | Pack overlay read and unpack layer write tests. |
| `unpack --layer` | Gap | Same as above. | Native parity | P1 | Overwrite/delete/metadata tests. |
| `git ... --layer` direct CLI | Gap | Git workspace mainly works indirectly through FUSE mount; direct git subcommands are not layer-aware. | Native parity | P2 | Define supported boundary and test it. |
| Context-scoped remote paths + `--layer` | Gap | Currently explicitly rejected to avoid unclear cross-context layer semantics. | Native parity | P2 | Implement after multi-context design and tests. |

## 6. Mount/FUSE/POSIX Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| FUSE `--layer` mount | Supported | FUSE only. | LayerFS core | P1 | Mount smoke e2e. |
| WebDAV `--layer` mount | Gap | Unsupported; decide whether V1 parity includes it. | Native parity | P2 | WebDAV semantics doc/tests. |
| `--checkpoint` restore mount | Partial | Can restore by durable seq; active-only layer limitation needs confirmation. | LayerFS core | P1 | Checkpoint mount e2e. |
| Base fallback read | Supported | Reads base when layer misses. | LayerFS core | P1 | Base-file read e2e. |
| Copy-up on write | Supported | Uses shadow/pending/local overlay + commit queue to write layer. | LayerFS core | P1 | Overwrite existing base file e2e. |
| Whiteout | Supported | File/empty directory works; non-empty directory lacks opaque/recursive behavior. | LayerFS core | P1 | Whiteout list/read/commit tests. |
| Local overlay precedence | Supported | Local hot path is preferred, with layer/backend only when needed. | Native parity | P1 | Local overlay + layer write tests. |
| Git workspace coexistence | Partial | Can coexist through mount path; git-workspace-specific behavior is not systematically verified. | Native parity | P1 | git clone/status/commit-like workload e2e. |
| Shadow store / spill | Supported | Large files can avoid JSON inline. | Native parity | P0 | Spill + object upload tests. |
| Commit queue to layer | Supported | `--layer` mount writes enter layer entries/objects instead of the base commit queue. | LayerFS core | P1 | After write, base is unchanged and diff is visible. |
| `flush`/`fsync`/`release` durability | Partial | Close/writeback tests exist; checkpoint wait barrier still needs work. | Ops-prod | P0 | Dirty-handle, crash, and unmount tests. |
| Multi-client event refresh | Partial | 1s polling; no SSE/watch; payload is simplified. | AgentFS-class | P1 | Two sandboxes sharing one layer sync in e2e. |
| Rename file | Supported | No-replace semantics. | Native parity | P1 | Rename conflict tests. |
| Rename directory | Partial | API/FUSE support it; snapshot rollback covers subtree; CLI parity is pending. | Native parity | P1 | Directory rename rollback/commit e2e. |
| Truncate | Partial | Basic write path can express it; sparse/hole/large truncate lacks systematic coverage. | Native parity | P1 | Grow/shrink/sparse truncate tests. |
| Append | Partial | FUSE append depends on the write path; CLI append is missing. | Native parity | P1 | Append across reopen/crash tests. |
| Hardlink | Partial | Current semantics are closer to content copy and do not preserve inode identity. | Native parity | P2 | Hardlink decision and tests. |
| Symlink | Supported | Symlink entry. | Native parity | P1 | symlink lstat/readlink/commit tests. |
| File mode | Partial | Mode can be recorded; uid/gid/mtime/xattr parity is incomplete. | Native parity | P1 | chmod + metadata parity tests. |
| UID/GID | Gap | Not first-class layer metadata. | Native parity | P2 | chown/chgrp strategy. |
| mtime/atime/ctime | Partial | created/updated exist; POSIX timestamp parity is incomplete. | Native parity | P2 | Timestamp preservation tests. |
| xattr | Gap | Not layer-aware. | Native parity | P2 | xattr API/FUSE tests. |
| File locks/flock | Gap | Multi-sandbox lock semantics are undefined. | Native parity | P2 | Lock contention tests. |
| mmap | Partial | Relies on general FUSE read/write; no targeted tests. | Native parity | P2 | mmap write/read e2e. |
| Sparse file/fallocate | Gap | Hole preservation is undefined. | Native parity | P2 | Sparse-file tests. |
| Directory listing consistency | Partial | FUSE restore/overlay merge works; direct CLI `ls --layer` is missing. | Native parity | P1 | List after write/delete/rollback. |
| Unmount with pending writes | Partial | Uses commit-queue flush; abnormal power-loss recovery is not sufficiently tested. | Ops-prod | P0 | kill/crash/failpoint e2e. |

## 7. Data Plane And Metadata Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| Small-file inline content | Supported | Limited by JSON body and inline thresholds. | LayerFS core | P1 | Inline checksum/content-type tests. |
| Large-file object content | Partial | Raw upload/read is supported; multipart, resume, and range are missing. | Native parity | P0 | 100 MiB+ upload/read/commit e2e. |
| S3/local object backend reuse | Partial | Reuses object storage; layer object lifecycle is not independently managed. | Native parity | P0 | Orphan GC and retention tests. |
| Encryption metadata | Partial | Entries have encryption fields; full end-to-end validation needs coverage. | Native parity | P1 | Encrypted large/small file tests. |
| Checksum | Supported | upsert/object entries record checksum. | Native parity | P1 | Checksum mismatch/final verification. |
| Content type | Partial | Inline and object metadata can record it; detection consistency needs verification. | Native parity | P2 | MIME detection tests. |
| File tags | Gap | Layer entries do not have full file tag parity; `find --tag` does not match layer entries. | Native parity | P1 | write/list/find/commit tags e2e. |
| Description/semantic text | Gap | Uncommitted layer content does not enter extraction/index pipeline. | Native parity | P1 | Pre-commit metadata and post-commit consistency. |
| Base revision conflict | Supported | Entry carries base inode/revision, and commit preflight checks it. | LayerFS core | P1 | Stale base conflict tests. |
| Rename target conflict | Supported | Commit preflight checks target. | LayerFS core | P1 | Target-exists tests. |
| Directory subtree snapshot | Partial | Failed commit rollback uses snapshot; scale and timeout are not stress-tested. | Ops-prod | P0 | Large subtree rollback failpoint. |
| Content-addressed dedup | Gap | Layer objects are not deduplicated. | Ops-prod | P2 | Dedup design if needed. |
| Quota accounting | Gap | Layer objects and entries do not have separate quota accounting. | Ops-prod | P0 | Per-tenant/per-layer quota tests. |
| Retention/GC | Gap | Lifecycle for uncommitted, committed, and rolled-back layer data is not implemented. | Ops-prod | P0 | GC reachability and safety tests. |
| Object range read | Gap | Large-file reads lack Range support. | Native parity | P1 | Range/partial download tests. |
| Direct-to-object-store upload | Gap | Server proxies uploads; cost and throughput are constrained. | Ops-prod | P2 | Presigned/multipart tests. |
| Cross-sandbox restore | Partial | Checkpoint/event restore works; dirty-handle barrier and crash recovery are insufficient. | Ops-prod | P0 | kill/restart/restore no-data-loss tests. |

## 8. Search, Discovery, And Indexing Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| Base search unchanged | Parity | Non-layer queries are unaffected. | Native parity | P1 | Regression tests. |
| `grep --layer` overlay text search | Partial | Stream scan, not indexed; large layers will be slow. | Native parity | P1 | Large-layer grep perf/e2e. |
| `find --layer` path/name search | Partial | Can merge base/layer; tag filters lack layer entry support. | Native parity | P1 | Whiteout, rename, mkdir, and tag tests. |
| Semantic/vector search before commit | Gap | Uncommitted layer content does not participate in embedding. | Native parity | P1 | Pre-commit semantic indexing design/tests. |
| Semantic/vector search after commit | Partial | Enters native pipeline after commit, but async timing needs verification. | Native parity | P1 | Post-commit embedding availability tests. |
| Media/image/audio extraction | Gap | Uncommitted layer objects have no extraction path. | Native parity | P2 | Extraction job e2e. |
| Search by checkpoint | Gap | `grep/find` have no checkpoint ref. | LayerFS core | P2 | Checkpoint search tests. |
| Ranking and merge semantics | Partial | Base + layer merge prioritizes overlay correctness; ranking is simplified. | Native parity | P2 | Score/order stability tests. |
| Tag-key existence search | Gap | Layer entries do not support tag-key existence matching. | Native parity | P1 | `find -tag key --layer` tests. |

## 9. Concurrency, Conflict, And Transaction Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| Multi-entry ordering | Supported | Layer entry seq is the ordering source. | LayerFS core | P1 | Same-path multi-write tests. |
| Multiple writers to same layer | Partial | Last-entry-wins view can work; conflict hints, locking, and actor attribution are missing. | AgentFS-class | P1 | Multi-client concurrent write tests. |
| Event watch | Partial | Polling + full restore; no incremental apply optimization. | AgentFS-class | P2 | Event lag/perf tests. |
| Commit with active writes | Partial | State transitions active/committing; late writes still need blocking and clear errors. | Ops-prod | P0 | Concurrent write during commit tests. |
| Conflict preflight | Supported | Stale base, target exists, non-empty dir, and some other cases are covered. | LayerFS core | P1 | Conflict matrix tests. |
| Conflict explain | Gap | Users only see an error; no structured conflict report. | AgentFS-class | P2 | Conflict report API/CLI. |
| rebase | Gap | Cannot automatically rebase a layer after base changes. | AgentFS-class | P2 | Rebase design/tests. |
| exactly-once commit | Gap | No durable apply ledger; retry relies on state and snapshot. | Ops-prod | P0 | Failpoint crash at each commit phase. |
| rollback after failed commit | Partial | Best-effort snapshot rollback. | Ops-prod | P0 | Injected failure and recovery tests. |
| idempotency key | Gap | Events table has fields, but APIs do not provide complete idempotency semantics. | Ops-prod | P1 | Duplicate request tests. |
| distributed locking | Gap | No explicit per-layer/per-path lease. | Ops-prod | P2 | Lock/lease tests if introduced. |

## 10. Security And Permission Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| Tenant isolation | Partial | Reuses existing auth/tenant model; layer ref resolution must continue validating cross-tenant leakage. | Ops-prod | P0 | cross-tenant id/name/tag tests. |
| Path scope isolation | Partial | Server validates entry paths under the base root; boundary handling needs fuzzing/hardening. | Ops-prod | P0 | `/base2`, unicode, `..`, and symlink scope tests. |
| Per-layer ACL | Gap | No layer-level share/read/write/commit permission. | Ops-prod | P1 | ACL API + auth tests. |
| Delegated token / sandbox token | Gap | No token that only allows writes to a specific layer. | AgentFS-class | P1 | Scoped token tests. |
| Progressive permission | Gap | No YoloFS-like per-operation authorization. | AgentFS-class | P3 | Permission prompt/audit design. |
| Audit log/provenance | Partial | Events record op/path/seq; actor/before/after/idempotency are not fully populated. | AgentFS-class | P1 | Audit completeness tests. |
| Secret redaction/exfil controls | Gap | Layer read/write has no additional security policy. | Ops-prod | P2 | Policy hook tests. |
| Integrity verification | Partial | Checksum exists; no tamper-evident audit chain. | Ops-prod | P2 | Checksum verification plus audit chain if needed. |

## 11. Operations, Cost, And Production Matrix

| Capability | Current LayerFS status | Gap/risk | Target | Priority | Required acceptance |
| --- | --- | --- | --- | --- | --- |
| Schema init | Supported | New tables are included in tenant schema. | Ops-prod | P1 | Schema dump/init tests. |
| Existing tenant migration | Partial | Production migration flow, rolling deploy, and backfill/ALTER strategy need confirmation. | Ops-prod | P0 | Migration dry-run and rollback plan. |
| Feature flag / rollout | Partial | Feature is opt-in via `--layer`; server-side and tenant-level switches are incomplete. | Ops-prod | P1 | Feature flag tests. |
| Metrics | Gap | Missing metrics for layer writes, object bytes, commit latency, conflicts, and GC. | Ops-prod | P0 | Metrics assertions/integration. |
| Tracing/logging | Partial | Uses existing logs; layer-specific spans/trace fields are missing. | Ops-prod | P1 | Trace sampling and log field review. |
| Dashboards/alerts | Gap | No production dashboard. | Ops-prod | P1 | SLO dashboard and alert runbook. |
| Quota/cost attribution | Gap | Layer object bytes are not separately billed or limited. | Ops-prod | P0 | Quota enforcement e2e. |
| Retention policy | Gap | No TTL, archive, or delete. | Ops-prod | P0 | Retention safety tests. |
| Backup/restore | Partial | Relies on DB/object-store backup; no layer-level restore/export. | Ops-prod | P1 | Restore-from-backup drill. |
| Load/perf benchmark | Gap | No layer-specific benchmark. | Ops-prod | P0 | write/read/commit/search benchmarks. |
| Chaos/failpoint | Gap | Commit/checkpoint/crash lack systematic failpoint coverage. | Ops-prod | P0 | Failpoint suite for commit phases. |
| Compatibility matrix | Gap | No matrix for OS, FUSE/WebDAV, object backends, or DB versions. | Ops-prod | P2 | Release qualification matrix. |
| Cleanup of abandoned uploads | Gap | No cleanup strategy for failed/abandoned raw object upload. | Ops-prod | P0 | Orphan object tests. |

## 12. Community LayerFS Capability Comparison

| Community direction | Current Drive9 status | Gap/risk | Target | Priority |
| --- | --- | --- | --- | --- |
| Read-only base + writable upper | Supported | Aligns with AgentFS/ArtifactFS/overlayfs direction. | LayerFS core | P1 |
| Copy-up | Supported | Large-object copy-up still needs stronger streaming/remote-copy behavior. | LayerFS core | P1 |
| Whiteout | Partial | File/empty directory supported; opaque directory/recursive whiteout missing. | LayerFS core | P1 |
| Checkpoint/snapshot | Supported | Uses durable seq; not DeltaFS-style layer freeze/insertion optimization. | LayerFS core | P1 |
| Commit/rollback | Partial | Usable, but production transactionality is insufficient. | LayerFS core | P0 |
| Portable session DB/bundle | Gap | AgentFS SQLite-session-like capability is not implemented. | AgentFS-class | P2 |
| Provenance audit | Partial | Event table foundation exists, but full context is not populated. | AgentFS-class | P1 |
| Progressive permission/staged effects | Gap | YoloFS-like capability is not implemented. | AgentFS-class | P3 |
| Branch/fork/merge layer chain | Gap | No multi-layer lower/upper chain. | AgentFS-class | P3 |
| Squash/compact | Gap | No layer compaction. | AgentFS-class | P2 |
| Runtime sandbox integration | Partial | Drive9 provides the filesystem; process/syscall isolation depends on external sandboxes. | AgentFS-class | P2 |
| Bucket/volume mounts | N/A | Drive9 already has S3/db9 backends; this is not equivalent to Cloudflare bucket mount. | AgentFS-class | P3 |
| Artifact hydrate/lazy blob | Partial | Object streams are readable; hydrate cache/range/lazy fetch strategy is missing. | AgentFS-class | P2 |
| Offline operation | Gap | Currently depends on backend service. | AgentFS-class | P3 |

## 13. Test Matrix

| Test layer | Current status | Gap | Priority | Required additions |
| --- | --- | --- | --- | --- |
| Datastore unit tests | Partial | Need coverage for tag/name ambiguity, pagination, GC, quota, and migration. | P1 | `pkg/datastore` layer CRUD/conflict tests. |
| Server unit tests | Partial | Need coverage for object range/multipart, commit failpoint, and auth/path fuzzing. | P0 | Commit phase failpoint suite. |
| Client unit tests | Partial | Need coverage for all endpoints, streaming, and error schema. | P1 | Endpoint path and retry tests. |
| CLI unit tests | Partial | Lifecycle and some write commands are covered; read commands, recursive behavior, and metadata are missing. | P1 | Command matrix tests. |
| FUSE unit tests | Partial | close/writeback/layer restore are covered; POSIX edges are missing. | P1 | hardlink/xattr/mmap/truncate/sparse tests. |
| E2E smoke | Supported | Covers API lifecycle, CLI small/large, grep/find, and FUSE restore/commit. | P1 | Required PR smoke. |
| E2E large file | Partial | 100 MiB CLI exists; still need FUSE, remote copy, range, and resume. | P0 | >100 MiB multi-path tests. |
| E2E multi-sandbox | Gap | No two-sandbox same-layer restore/event test. | P0 | writer/reader sandbox no-data-loss test. |
| E2E crash recovery | Gap | No recovery test for killed server/client/unmount interruption. | P0 | crash at upload/checkpoint/commit tests. |
| E2E metadata parity | Gap | Tags, description, mode, timestamps, and xattr are missing. | P1 | Metadata matrix e2e. |
| E2E search parity | Partial | Basic grep/find exists; semantic/tag/checkpoint search is missing. | P1 | Search matrix e2e. |
| E2E pack/unpack/git | Gap | Not covered. | P1 | Real workflow tests. |
| Performance benchmarks | Gap | No layer-specific latency/throughput/cost benchmark. | P0 | Local overlay, object upload, commit, and search benchmark. |
| Fuzz/property tests | Gap | Path scope, overlay merge, rename/whiteout order can be fuzzed. | P1 | Path and entry sequence fuzz tests. |

## 14. Phased Roadmap

### 14.1 P0: Production Blockers

| Item | Deliverable |
| --- | --- |
| Commit exactly-once/recovery | Durable apply ledger, phase recovery, failpoint suite, and safe repeated commit. |
| Cross-sandbox no-data-loss | checkpoint dirty-handle barrier, stronger event restore, and multi-sandbox crash e2e. |
| Layer object lifecycle | delete/archive, GC reachability, orphan cleanup, and retention policy. |
| Quota/cost control | per-layer/per-tenant byte, entry count, and object count limits plus metrics. |
| Large-file production path | multipart/resume/range/direct upload, or an explicit alternative. |
| Observability | metrics, tracing fields, dashboard, alerts, and runbook. |
| Security hardening | path scope fuzzing, cross-tenant tests, and minimal scoped token/ACL implementation. |
| Migration/rollout | production schema migration, feature flag, and rollback plan. |

### 14.2 P1: Native Drive9 Parity

| Item | Deliverable |
| --- | --- |
| CLI read parity | `cat/ls/stat --layer`, covering base/layer/whiteout/rename/checkpoint. |
| CLI write parity | recursive `cp/rm`, append/resume, remote copy streaming, and metadata flags. |
| Metadata parity | tags, description, content type, mode/timestamps, and post-commit consistency. |
| Search parity | layer tag search, semantic pre-commit or explicit delayed strategy, and checkpoint search. |
| FUSE POSIX parity | dir rename, truncate, append, symlink, hardlink strategy, xattr/timestamps. |
| Conflict UX | structured conflict report, dry-run commit, and rebase design. |
| Pack/unpack/git workflows | Define layer-aware behavior and add real workflow e2e. |

### 14.3 P2/P3: AgentFS-Class And Ecosystem Enhancements

| Item | Deliverable |
| --- | --- |
| Portable session | export/import bundle including objects, metadata, checkpoints, and audit. |
| Provenance audit | actor, tool call, before/after, idempotency, and tamper-evident chain. |
| Branch/fork/merge | child layer, multi-lower, squash/compact, and merge conflict model. |
| Progressive permission | staged effects, policy hooks, and approval workflow. |
| Offline/lazy hydrate | local session cache, range hydrate, and offline replay. |
| Multi-language SDK | layer API parity across published SDKs. |

## 15. Development Definition Of Done

Whenever any `Gap` or `Partial` item in this matrix is declared complete, it must at least satisfy:

1. Implement the code path without changing native non-layer behavior or performance baselines.
2. Update CLI/API documentation, error codes, and user interaction notes.
3. Cover success, conflict, permission, edge, and regression paths with unit tests.
4. Cover real user journeys with e2e tests, especially cross-sandbox restore, large files, commit, and rollback.
5. If schema, object lifecycle, security, or operations are involved, add migration, metrics, GC/retention, and runbook coverage.
6. Update this matrix status. If a capability is explicitly excluded, mark it `N/A` and explain why.
