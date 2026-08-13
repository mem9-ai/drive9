# LayerFS CoW Fork

Date: 2026-08-13  
Status: Canonical

This specification adds **zero-copy layer fork** and **server-side chain reads** to Drive9 LayerFS. Product surface stays **layer** (`fs layer`, `/v1/layers`, `mount --layer`). Checkpoint, commit, and rollback keep their V1 names and meanings.

Minimize break: without `--layer` (and without a layer binding), `/v1/fs` and classic mount stay identical to today.

Builds on:

- [Layered Filesystem V1 Design](./layered-filesystem-v1-design.md)
- [Layered Filesystem Research](./layered-filesystem-research.md)
- [Layered Filesystem Feature Matrix](./layered-filesystem-feature-matrix.md)

---

## 1. Goals and non-goals

### 1.1 Goals

| Capability | Meaning |
| --- | --- |
| **Fast fork** | O(1) metadata child layer from an existing layer. No copy of `file_nodes` or object bodies. |
| **Chain read** | Child view: self → parent@`origin_seq` → … → main, merged on the **server**. |
| **Checkpoint / commit / rollback** | Same verbs as V1. `commit` applies the effective view to main. |
| **Delete + pin GC** | Layers can be deleted. A child's origin pin retains parent entries/objects. |
| **Default flat disk** | No layer attachment ⇒ behavior identical to today. |

### 1.2 Non-goals

- Merge / rebase / three-way merge **between layers**, or commit-into-parent
- `git add` / staging / index
- Versioning main tables with `workspace_id`; do **not** change `file_nodes` / `inodes` / `contents` UNIQUE(path) current-state semantics
- Tenant / serverless DB branch as the share-mode filesystem fork path
- Always-on layer; live layer switch on a mounted FUSE instance
- Defaulting the classic disk to write into an overlay; checkpoints on main
- A real Git object store; cloning vault/journal with an FS fork
- A `publish` product verb instead of `commit`
- Renaming the product surface to **branch** (`fs branch`, `/v1/branches`)
- Renaming tables `fs_layer_*` → `fs_branch_*`
- Snapshot isolation that freezes **main** at fork time (the pin freezes the parent overlay only; see §5.5)

### 1.3 Document role

This file is the implementation contract for CoW fork. Server work evolves `fs_layer_*`. After review, §5–§6 may be excerpted into `tidbcloud/fs`.

---

## 2. Decisions

1. **Engine.** Evolve today's LayerFS overlay into a linear forkable chain (`parent_layer_id` + `origin_seq`). Do not rebuild a versioned main-table FS.
2. **Name.** The user-visible entity remains **layer**. Tables and HTTP stay `fs_layer_*` / `/v1/layers`.
3. **`create` / `fork`.** `create` opens a root layer on main. `fork` opens a child from a layer (zero-copy pin).
4. **`checkpoint`** is an in-layer history point and does not change main. **`commit`** applies the effective view to main. There is no `publish` verb.
5. **Usage.** Default mount is flat. `mount --layer <ref>` consumes a layer (must be `active`).
6. **Share-mode “fork”.** Use `fs layer fork`. Tenant `POST /v1/fork` / `ctx fork` on share returns **409** and points at layer fork.

**Vs Git:** `layer commit` means “apply this layer’s effective view to main”, not `git commit`. Recoverable in-layer points are checkpoints.

### 2.1 Frozen rules

| ID | Topic | Rule |
| --- | --- | --- |
| **D1** | Default fork pin | `origin_seq = MAX(parent.entry_seq)` in the same transaction as `SELECT parent FOR UPDATE`. Do **not** use `fs_layers.durable_seq` (`durable_seq` advances only on checkpoint). |
| **D2** | Checkpoint fork | If `checkpoint_id` is set: checkpoint must belong to the parent; `origin_seq = checkpoint.durable_seq`; store `origin_checkpoint_id`. |
| **D3** | Child-gate | Gates **delete / physical GC** only. Does **not** gate commit. |
| **D4** | Who may commit | Any `active` (or V1-eligible) layer may commit **to main only**. No commit-into-parent. |
| **D5** | Child commit | Flatten the **effective view** before apply. Do not replay only the child's own log. |
| **D6** | Root commit with children | Allowed. Parent entries stay until pins release. |
| **D7** | Isolation | Pin freezes the **parent overlay** (`entry_seq ≤ origin_seq`), not main. Sibling commits into main remain visible via main fallback. |
| **D8** | Chain read | Server-side resolve. Acceptance surface in §5.6. |
| **D9** | Share tenant fork | **409** + text pointing at layer fork. No CLI auto-downgrade that writes main. |
| **D10** | Checkpoint mount | `--checkpoint` is **read-only** and must still restore the overlay view. Writable history = `fork --checkpoint`. |
| **D11** | Apply ledger | Not required for this phase. Commit stays V1 preflight + best-effort apply. Optional later hardening. |
| **D12** | Branch branding | Out of scope. |
| **D13** | Forkable parent states | Only `active` or `sealed`. Reject `committing` / `committed` / `abandoned` / `conflicted`. Check under parent `FOR UPDATE`. |
| **D14** | Inheritance | Child **must** inherit `base_root_path` and `durability_mode`. `depth = parent.depth+1`. `root_layer_id` is the chain root. Tags/actor do not inherit by default. |
| **D15** | Depth | Reject `depth > 8` by default; hard cap 16; **409**. |
| **D16** | Sibling roots | Same as today: multiple roots may commit to main; conflicts surface at preflight. No extra base-root lease. |
| **D17** | DELETE lifecycle | This phase: **logical `abandoned` only**. Do not physically delete `fs_layers` rows or `entry_seq ≤ pin` entries while `still_pins(L)`. Physical delete is a later GC worker when `¬still_pins`. |
| **D18** | Commit apply set | `≡ DiffFSLayerVsMain(L)` (effective view − live main). Skip paths that already match main. Preflight claims are **live main at commit time**. Do not reuse ancestor `base_revision` for apply CAS. |
| **D19** | Materialize | Replay the chain log **in order** into an in-memory tree (preserve upsert→chmod/rename), then `tree_diff` vs live main. Do **not** treat raw latest-per-path ops as apply input (chmod-only latest drops content). Main-backed rename must remain a real rename (or equivalent copy-up + whiteout), not a dest chmod. |

---

## 3. Concepts

| Term | Implementation | Meaning |
| --- | --- | --- |
| **main / base** | `file_nodes` / `inodes` / `contents` | Live published current state |
| **layer** | `fs_layers` row + entries | CoW draft over main (or parent@layer pin) |
| **create** | `POST /v1/layers` | Root layer on main |
| **fork** | `POST /v1/layers/{ref}/fork` | Child from a layer tip or checkpoint |
| **checkpoint** | `fs_layer_checkpoints` | Durable in-layer point; does not change main |
| **commit** | `POST …/commit` | Apply effective view to main |
| **rollback** | `POST …/rollback` | Discard this layer (see §5.8 if pinned) |
| **delete** | `DELETE …/layers/{ref}` | Abandon; child-gate / cascade in §5.8 |

No staging: writes belong to the current active layer immediately. Default `status` / `diff` is effective view vs **live main**.

```text
main (live)
  ├── layer exp-a      create :/ --name exp-a
  │     ├── checkpoint cp1, cp2, …
  │     └── layer exp-b   fork from exp-a @ origin_seq
  └── layer exp-c      sibling root on main

Allowed:
  exp-a commit → main          (even if exp-b is still active)
  exp-b commit → main          (flatten effective view → main; not merge into exp-a)
  exp-c commit → main          (parallel with exp-a; conflicts at preflight)

Forbidden:
  exp-b merge/commit → exp-a
  exp-a merge → exp-c
```

---

## 4. Mechanics

```text
drive9 fs layer create :/ --name exp-a
  ≈  INSERT fs_layers(..., parent_layer_id='', depth=0, root_layer_id=self)

drive9 mount --layer exp-a :/ ./m
  read:  local dirty → Resolve(exp-a) chain → main
  write: only exp-a (copy-up / whiteout, §5.7)

drive9 fs layer fork exp-a --name exp-b
  in one transaction:
    SELECT parent FOR UPDATE
    require parent.state ∈ {active, sealed}
    origin_seq := MAX(entry_seq) of parent          -- tip pin (D1)
    -- or checkpoint_id → origin_seq := cp.durable_seq (D2)
    INSERT child (parent_layer_id, origin_seq, depth=parent.depth+1,
                  root_layer_id=parent.root, base_root_path=parent.base_root, …)
  no entry copy

commit any layer L:
  compute effective view (§5.7)
  preflight vs live main (base_revision CAS, V1 philosophy)
  apply → main; L → committed
  if children exist: do not delete L's entries (pin retains them)
```

Creation is not a full-tree copy. It is lazy per-path CoW plus a chain pin.

---

## 5. Server: data model and contracts

### 5.1 Principles

- Do not change main-table current-state semantics or UNIQUE(path).
- CoW state stays in **`fs_layer_*`**; chain support is **additive**.
- Shared and dedicated schemas stay in lockstep. TiDB and DB9 variants stay in lockstep. `dump-init-sql` (tidb_zero / tidb_cloud_native / db9) is the export source.
- Existing tenants: online `ALTER` plus backfill  
  `parent_layer_id=''`, `origin_seq=0`, `depth=0`, `root_layer_id=layer_id`, `origin_checkpoint_id=''`.

### 5.2 `fs_layers` additive columns

| Column | Meaning |
| --- | --- |
| `parent_layer_id` | Parent layer; `''` = root |
| `origin_seq` | Child may see parent entries with `entry_seq ≤ origin_seq` |
| `origin_checkpoint_id` | Set on checkpoint pin |
| `root_layer_id` | Chain root (`layer_id` for roots) |
| `depth` | Root = 0; child = parent + 1 |
| `origin` | Audit: `create` \| `fork` |

Indexes: `(parent_layer_id, state)`; `(root_layer_id)` as needed.

Do **not**: cross-tenant parents; materialize entries at fork; add merge-target columns.

### 5.3 Other tables

| Table | Change |
| --- | --- |
| `fs_layer_entries` | No schema change; chain uses existing `*AtSeq` |
| `fs_layer_checkpoints` | Unchanged |
| `fs_layer_events` | Keep. Child FUSE watchers only need **self** (ancestor pin is immutable) |
| `fs_layer_tags` | Keep |
| apply ledger | Optional later; not a product verb |

### 5.4 Fork

In one transaction:

1. `SELECT … FROM fs_layers WHERE layer_id=? FOR UPDATE` (parent).
2. State ∈ {`active`,`sealed`} or **409**.
3. Same tenant; `depth+1` ≤ 8 (hard 16) or **409**.
4. **Tip pin:**  
   `origin_seq = COALESCE((SELECT MAX(entry_seq) FROM fs_layer_entries WHERE layer_id=parent), 0)`  
   — **not** `fs_layers.durable_seq`.
5. **Checkpoint pin:** checkpoint belongs to parent; `origin_seq = checkpoint.durable_seq`; set `origin_checkpoint_id`.
6. Child inherits `base_root_path` and `durability_mode`; `depth` / `root_layer_id` per D14; `state=active`.
7. Do not require advancing parent `durable_seq`.

Concurrency: same parent-row lock as `UpsertFSLayerEntry` serializes fork vs parent writes. Parent entries after the pin are invisible to that child (intended).

Auth: write on the parent `base_root_path`. Scoped-token allowlist includes `…/fork`, `…/chain`, `DELETE …/layers`.

### 5.5 Isolation (user-facing)

| Guaranteed | Not guaranteed |
| --- | --- |
| Child does not see parent overlay entries written **after** the fork (`entry_seq > origin_seq`) | A snapshot of **main** at fork time |
| Pinned parent overlay stays readable under pin GC | After another layer commits to main, those changes appear via main fallback |
| Same-path upper entry / whiteout hides lower layers and main | A hard-isolated sandbox (would need a future main snapshot) |

**Fork pin freezes the parent overlay sheet, not the published book.**

### 5.6 Chain resolve

Reads must use a **folded** effective view (ordered replay), not raw latest-per-path. A later `chmod` on the same path must not hide the prior upsert body.

#### 5.6.1 Resolve

```text
Resolve(path, layerID):
  materialize overlay tree for the chain (tip optionally capped by max_seq)
  if path is whiteout: return WHITEOUT
  if path is present:  return HIT(folded entry)
  return MISS → read main(path)
```

Tip limit is current tip (or checkpoint `durable_seq`). Each ancestor A is limited by the direct child's `origin_seq`.

**ListDir:** union names top-down; whiteouts remove names; then merge with main readdir.

#### 5.6.2 Required chain-aware entry points

| Entry | Required |
| --- | --- |
| FUSE restore / lookup / readdir / open (`--layer`) | Yes (server API or shared datastore merge). Checkpoint mounts stay read-only **and** still restore. |
| `GET …/layers/{ref}/entries` and single-path entry/stat | Yes |
| layer **diff / status** (default vs main) | Yes: effective view − main |
| layer object/content GET | Yes. Whiteout → **404**. |
| search overlay | Should walk the chain |
| WebDAV + layer | Remain V1 unsupported |

Implement `ResolveFSLayerPath` / merged list / overlay materialize in datastore/server. **Do not** assemble the chain with client-side multi-hop RPC.

Ancestor entries with `entry_seq ≤ origin_seq` are immutable and cacheable as `(layer_id, origin_seq, path)`. FUSE events subscribe to **self** only.

#### 5.6.3 diff / status

- Default: effective view (self ∪ ancestors@origin_seq, upper path wins, whiteout hides) minus **live main**.
- Optional later: `vs=parent` (self vs parent@origin_seq).

### 5.7 Writes, copy-up, commit

#### 5.7.1 Live writes

- Only the **top active** layer is writable.
- Edit of a path whose visible body is **main**: copy-up records main `base_inode_id` + `base_revision` (V1).
- Edit of a path whose visible body is an **ancestor entry**: copy content into this layer; main claim = **current main revision** if the path exists on main, else `base_revision=0`.
- Delete: whiteout on this layer.

Optional live-write CAS matches V1. Skip is allowed when the source is an ancestor and main has no such path. **Commit preflight always recomputes claims from live main** (D18).

#### 5.7.2 Commit → main

1. Layer must be allowed to enter V1 `committing` (usually `active`).
2. Children do **not** block commit.
3. **Materialize (D19):**
   ```text
   tree = empty
   for A in chain_from_root_to_L:
     limit = tip(A) if A == L else origin_seq of the direct child on this chain
     for op in ListFSLayerEntryLogAtSeq(A, limit) in entry_seq order:
       apply_op_to_tree(tree, op)    # V1 upsert/chmod/rename/whiteout
   plan = tree_diff(tree, live_main)
   ```
   - **Root, no parent:** today's ordered log replay is allowed **iff** it matches `tree_diff`. Tests must cover upsert→chmod and rename.
   - **Has parent:** must materialize the chain. Must not replay only the child log. Must not apply raw latest-per-path rows. **Main-backed rename** must emit a real `rename` (or copy-up dest + whiteout src), never dest-as-chmod that then skips.
4. **Apply set (D18) = that plan:**
   - Paths equal to live main → **skip** (parent already committed, child unchanged → empty apply / success).
   - Symlinks compare target text.
   - Claims are live main file_id/revision at commit time.
5. Apply → main; `committed` (or `conflicted` on failure, V1).
6. Do not delete this layer's entries on commit if `still_pins`.

Apply order: creates/renames shallow-first; whiteouts deep-first (so directory deletes see an empty tree).

Forbidden: commit target = parent layer; layer merge.

### 5.8 Delete, rollback, pin GC

#### 5.8.1 Shared predicates

```text
exists_row(X) := fs_layers still has X (including abandoned/committed audit rows)

descendants(L) := { X | exists_row(X) ∧ X reaches L via parent_layer_id }

still_pins(L) := exists X in descendants(L) such that
  X.state ∈ {active, sealed, committing, conflicted}
  ∨ (X.state ∈ {abandoned, committed} ∧ still_pins(X))

# Conservative implementation is also valid:
# still_pins(L) := descendants(L) is non-empty

pin_seq(P) := MAX(C.origin_seq) over direct children C with exists_row(C)
              (no children ⇒ no pin)
```

`abandoned` / `committed` do **not** mean “ancestors may drop the chain”. If an active grandchild still walks through an abandoned middle layer, keep the ancestor row and `entry_seq ≤ pin_seq` entries.

`Resolve(D)` on `P → C(abandoned) → D(active)` must still read `P@C.origin_seq`.

#### 5.8.2 `DELETE /v1/layers/{ref}`

| Case | Behavior |
| --- | --- |
| `still_pins(ref)` | Default **409** (list blocking descendants). `?cascade=true`: DFS logical abandon of the subtree, then self. |
| `¬still_pins(ref)` | `state → abandoned` only. Do not physically delete the row in this phase. |
| Physical row / `entry_seq ≤ pin` delete | GC worker only, when `¬still_pins` |

Object bodies live as long as some non-GC'd layer entry still references the `storage_ref`.

#### 5.8.3 Rollback

- `¬still_pins`: V1 → `abandoned`; may enqueue cleanup.
- `still_pins`: → `abandoned`, **keep** `entry_seq ≤ pin_seq` entries and the row. Do not truncate a pinned prefix.
- Later `rollback --to <checkpoint>`: `active` only; `to_seq < pin_seq` → **409**.

#### 5.8.4 GC worker (minimum)

Only after `abandoned` or `committed` (per policy):

| Object | Condition |
| --- | --- |
| entries with `entry_seq > pin_seq(L)` | Layer no longer actively written, **or** no pin |
| entries with `entry_seq ≤ pin_seq(L)` | Only `¬still_pins(L)` |
| `fs_layers` row | Only `¬still_pins(L)` and retention policy allows |
| storage objects | No remaining entry references |

Never delete an **active** layer's post-fork writes because `entry_seq > pin_seq`. After all descendants are gone, ancestor pin release must be testable.

### 5.9 V1 mapping

| V1 API | This spec |
| --- | --- |
| `POST …/checkpoints` | Unchanged; advances that layer's `durable_seq` |
| `POST …/commit` | To main; any layer; flatten if parented |
| `POST …/rollback` | §5.8.3 |
| Single-layer read | Root unchanged; **child must use the chain** |

`committed` / `abandoned` cannot be mounted writable. `--checkpoint` is read-only and still restores the overlay.

---

## 6. HTTP

### 6.1 Compatibility

- No layer binding: **`/v1/fs` unchanged**.
- Keep `/v1/layers`. Responses may add `parent_layer_id`, `origin_seq`, `depth`, `root_layer_id` (old clients ignore them).
- Do **not** add `/v1/branches`.

### 6.2 Endpoints

| Endpoint | Notes |
| --- | --- |
| `POST /v1/layers` | Create root; unchanged |
| `POST /v1/layers/{ref}/fork` | New. Body: `name?`, `checkpoint_id?` |
| `GET /v1/layers/{ref}/chain` | New. Root→tip: `layer_id, name, state, parent_layer_id, origin_seq, origin_checkpoint_id, depth, root_layer_id, base_root_path, created_at` |
| `DELETE /v1/layers/{ref}?cascade=` | New. §5.8.2 |
| `GET …/entries` and object reads | Chain-aware (§5.6) |
| `POST …/checkpoints\|commit\|rollback` | Paths unchanged; semantics §5.7–5.8 |
| List layers | Include parent fields |

### 6.3 Tenant `POST /v1/fork`

- Dedicated + branch-capable: unchanged.
- Share / non-branch: **409** pointing at `POST /v1/layers/{ref}/fork` (or create then fork).
- CLI does not auto-create a layer.

---

## 7. CLI and client

### 7.1 Commands

```bash
drive9 fs layer create :/ --name exp-a

drive9 fs layer fork exp-a --name exp-b
drive9 fs layer fork exp-a --name exp-b --checkpoint <cp>

drive9 mount --mode=fuse --layer exp-b :/ ./work

drive9 fs layer status exp-b
drive9 fs layer diff exp-b
drive9 fs layer checkpoint exp-b -m "wip"
drive9 fs layer commit exp-b
drive9 fs layer commit exp-a                 # allowed even with children

drive9 fs layer rollback exp-b
drive9 fs layer delete exp-b
drive9 fs layer delete exp-a --cascade
drive9 fs layer chain exp-b
```

### 7.2 create vs fork

| Command | Meaning |
| --- | --- |
| `create <base-path> --name X` | Parent empty; covers main |
| `fork <layer-ref> --name Y` | Tip-pin child |
| `fork <layer-ref> --name Y --checkpoint C` | Checkpoint pin |
| `fork --name Y` | Requires a current layer in ctx, or error |

### 7.3 Mount

| Mode | Behavior |
| --- | --- |
| No `--layer` | Flat main |
| `--layer <ref>` | Chain view; writes go to top; must be **active** |
| `--layer <ref> --checkpoint <cp>` | **Read-only** restored view |
| Writable history | `fork --checkpoint` → new active layer |

### 7.4 SDK

Add `ForkFSLayer`, `DeleteFSLayer`, `ListFSLayerChain`, `ResolveFSLayerPath` (internal is fine). Existing commit/checkpoint names stay. Structs add `ParentLayerID`, `OriginSeq`, `OriginCheckpointID`, `Depth`, `RootLayerID`.

### 7.5 Before commit

- Main is unchanged; only the layer view shows drafts.
- **`commit` writes main.** `rollback` / `delete` drop this layer under pin rules.

---

## 8. Relation to V1 LayerFS

| V1 | This spec |
| --- | --- |
| Verbs | Kept. Chain commit = effective view → main |
| Single layer | Plus fork chain |
| Workspace / publish / branch rename | Not done |
| Commit-into-parent | Not done |
| Share tenant fork | 409 → layer fork |

---

## 9. Phasing

| Phase | Scope |
| --- | --- |
| **This release** | Additive schema + backfill (TiDB/DB9); tip/checkpoint fork; server chain resolve (FUSE + entries + diff/status + search); any-layer commit + child flatten; delete/cascade + pin rules; rollback keeps pins; CLI fork/chain/delete; share 409; tests in §9.1 |
| **Follow-up** | Apply ledger / failpoint hardening |
| **Later** | ctx-attached fork; `rollback --to`; `diff vs=parent`; optional CLI downgrade; squash (depth valve, not merge); branch alias if ever desired |

### 9.1 Required tests

1. Create root → write **without** checkpoint → tip fork → child sees all parent entries (D1).
2. Parent writes more → child does **not** see post-fork parent entries.
3. Child writes → commit child → main updated (D4/D5/D18).
4. Parent commit succeeds while a child is active; child still reads the pin prefix (D3/D6).
5. After parent commit, child commit with no extra edits → success / empty apply (D18).
6. Parent `upsert` + `chmod` same path → fork → child GET/commit keeps **body and mode** (D19).
7. Sibling root commits: same conflict behavior as V1.
8. Delete with pinned descendants → 409; `--cascade` succeeds; abandoned middle + active grandchild → DELETE ancestor 409 (D17).
9. Rollback parent with pinned child → parent abandoned, child still reads pin prefix.
10. depth > 8 → 409.
11. Share tenant fork → 409 pointing at layer fork.
12. Fork from `committed`/`abandoned` → 409; `fork --checkpoint` is writable; `--checkpoint` mount is read-only **and** shows the overlay.
13. Search overlay walks the chain (or an explicit known-gap, never a silent miss treated as pass).
14. Root ordered replay ≡ tree_diff materialize (upsert→chmod, rename).
15. Main-backed rename on a child commit: dest exists with content, src gone.
16. Directory whiteouts apply children-before-parent.

---

## 10. Risks

| Risk | Mitigation |
| --- | --- |
| Chain-read amplification | Depth cap; server merge; ancestor pin cache; self-only events |
| Pinning `durable_seq` by mistake | Code and tests use `MAX(entry_seq)`; test #1 |
| Non-atomic commit | Match V1; optional later ledger |
| Users treating fork as a main snapshot | Document §5.5; CLI help |
| Flatten bugs | Overlay tests; rename + chmod cases |
| Pin leak / mistaken GC | `pin_seq`; tests #8/#9 |
| Schema drift | `dump-init-sql` for all three providers + CI |

---

## 11. Open (does not block this release)

1. Whether search overlay is a hard guarantee vs documented best-effort.
2. Whether to ever alias layer → branch in CLI/HTTP.
3. Whether a future phase should snapshot **main** at fork time.
4. UX copy when a child commit is a no-op because the parent already landed.

---

## 12. Summary

LayerFS grows a tip-pinned linear CoW chain: `create` on main, `fork` for children. The pin freezes the parent overlay, not main. Reads merge on the server. Any layer may `commit` to main; the apply set is the ordered effective view minus live main. `still_pins` is transitive; delete is logical abandon. Share tenant fork is 409 toward layer fork. Classic flat-disk paths stay unchanged.
