# FUSE mount: on-demand Git Workspace discovery

**Status:** Final design
**Date:** 2026-08-09
**Scope:** FUSE-side git-workspace discovery and refresh; eliminate idle `/v1/git-workspaces*` traffic when the user never ran `drive9 git --fast`
**Non-goals:** Overlay write path, checkpoint, and hydrate protocol semantics
**Related:** [git-fast-clone-workspace.md](./git-fast-clone-workspace.md), [pack-unpack-profile-spec.md](./pack-unpack-profile-spec.md)

---

## 1. Problem

Today, overlay-profile mounts set `LocalRoot`, which enables `EnableGitWorkspaces=true`. Almost every Lookup enters `gitWorkspaceForPath` → `ensureGitWorkspaces` and calls `ListGitWorkspaces` on a **1s TTL**. Even when the list is always empty, any filesystem activity keeps hitting the backend.

User expectation: **if the user never registered a workspace with `drive9 git --fast`, produce zero `/v1/git-workspaces*` requests**, while still supporting same-machine live registration and cross-sandbox remount restore.

---

## 2. Goals and non-goals

### 2.1 Goals

| ID | Description |
| --- | --- |
| **G0** | When the remote index is confirmed missing/empty (404/empty) and there is no local arm signal, **`/v1/git-workspaces*` = 0** under any FS activity |
| **G1** | After a live `--fast` on the same LocalRoot, the **next FS op** can enter the git layer (typically sub-second on an active mount) |
| **G2** | New sandbox / new LocalRoot remount: if the remote index has entries visible to this mount, restore clean + overlay + git-state |
| **G3** | Once armed, no idle list polling; refresh is event-driven (local markers, index change, SSE, …) |
| **G4** | Error paths use backoff + singleflight; no per-op HTTP storms |

### 2.2 Non-goals

- Do not disable future `--fast` capability by default (default is DORMANT, armable).
- Do not promise: after mount-time index 404 confirmation, a first `--fast` from another machine becomes visible **without remount** (see §6 product boundaries).
- Do not treat the remote index as the runtime source of truth (no index-only tree/overlay load).
- Do not add forced hiding for `/.drive9/` (same class as packs: dotdir convention).
- Do not ship a `drive9 git reindex` user command.

---

## 3. Summary

| Scenario | `/v1/git-workspaces*` | Other remote I/O |
| --- | --- | --- |
| Mount only, never `--fast`, index 404 | **0** | **≤1×** FS `Stat` of index after mount |
| Same-machine live `--fast` | On-demand list / tree / overlay after local arm | No idle polling |
| New sandbox remount with index entries | **1×** list + on-demand tree/overlay | **1×** FS Stat/Get of index |
| Already ARMED; cross-host add/remove workspace | List only when the index signal changes | Throttled FS Stat and/or SSE |
| All workspaces removed and index empty | Back to DORMANT → **0** | — |

---

## 4. Core model: DORMANT → ARMED

```text
DORMANT (default)
  · No /v1/git-workspaces* calls
  · gitEntry always misses (same as no git layer)
  · Allowed: local arm checks; async FS Stat of index after mount
        │
        │ Local arm signal, or remote index has entries visible to this mount
        ▼
ARMED
  · ListGitWorkspaces builds runtime (DB is authoritative)
  · tree / overlay refresh is event-driven
  · No empty-list periodic poll
        │
        │ list empty and no local signal / index confirmed empty
        ▼
DORMANT
```

| Concept | Meaning |
| --- | --- |
| `EnableGitWorkspaces` | May enter the state machine (tied to mount capabilities such as `LocalRoot != ""`) |
| `armed` | Git-workspace APIs are allowed |
| `dormantConfirmed` | This mount confirmed no relevant remote index; no more proactive index Stat until local arm |

---

## 5. Metadata

### 5.1 Remote index (cross-sandbox)

**Path (tenant-absolute, same shape as `/.drive9/packs`):**

```text
/.drive9/git-workspaces/index.json
```

- Read/write with client absolute remote paths (`StatCtx` / `ReadStream` / `WriteCtxConditionalWithRevision`). **Do not** re-join through FUSE `remotePath`.
- Entry `root_path` values are tenant-absolute; the mount filters by its own `RemoteRoot`.
- **fs_scoped** credentials that cannot read tenant-root `/.drive9/`: cannot arm via index; use `--git-workspaces=on` or widen scope.

**Schema (existence only; keep fields small):**

```json
{
  "version": 1,
  "updated_at": "2026-08-09T00:00:00.000Z",
  "workspaces": [
    {
      "workspace_id": "ws_xxx",
      "root_path": "/repo/",
      "workspace_kind": "main"
    }
  ]
}
```

Rules:

- Do not put `repo_url` or other full restore fields in the index.
- **After arming, always build runtime via `ListGitWorkspaces` (or Get by id)**; the index only answers “should we arm?”.
- Missing file or empty `workspaces` → may set `dormantConfirmed`.
- Writes: single whole-document replace + revision CAS; when empty, CAS-write an empty document (avoid unconditional delete racing concurrent upserts).
- No separate `epoch` file; use revision / mtime / `updated_at`.

### 5.2 Local LocalRoot (same-machine live)

```text
<LocalRoot>/git-workspaces/armed
<LocalRoot>/git-workspaces/refresh/<id>
<LocalRoot>/git-workspaces/deleted/<id>
```

**Directory-level arm signal (must not rely only on per-id scans of already-loaded runtimes):**

```text
armed  = exists(armed) OR any file under refresh/
gen    = fingerprint(armed body + refresh/<id> names/bodies)
         (not max FS mtime alone)
force  = !wasArmed OR (armed AND gen != lastGen)
```

- `deleted/` is for post-list hiding and invalidation, not a standalone arm condition and not part of `gen`.
- Only mounts sharing the **same LocalRoot** share local signals; different `--local-root` / credentials → remote index path.
- When already ARMED, further local marker generation advances (e.g. second `--fast` on the same mount registering a new id) → **force list**.

### 5.3 Writers

| Event | Remote index | Local |
| --- | --- | --- |
| `git clone --fast` | upsert entry | touch armed + refresh |
| `git worktree add --fast` | upsert linked | same |
| `git worktree remove --fast` | remove entry; empty → write empty index | deleted marker |
| FUSE remove workspace root | **must** update index | local deleted |
| SDK / server Upsert·Delete | prefer server-maintained index | optional |

**CLI success path order:**

```text
1. UpsertGitWorkspace + ReplaceGitTree + git-state
2. Update remote index (CAS; failure → whole command fails)
3. Local armed / refresh (failure → fail)
4. Print success to the user
5. hydrate (optional, may be long; unrelated to discovery)
```

**Atomicity and partial-failure recovery:**

Steps 1–3 are **not** one distributed transaction across the metadata API, the
remote index document, and the local FS. Each step is independently durable and
idempotent (upserts/replaces keyed by workspace id / commit / root_path). The
command prints success only after steps 1–3 complete; hydrate never gates
registration. Recovery for steps 1–3 is **re-run the command** (or remount when
only local markers are missing). Prefer “registered but not yet discoverable”
over “discoverable but incomplete”.

| After | Failure | Observable state | Recovery |
| --- | --- | --- | --- |
| **1** Upsert + ReplaceTree + git-state | Any sub-write fails | May leave a partial workspace row/tree/state; **no** index entry; **no** local markers; mounts stay dormant | Fail the command. Re-run `--fast` / worktree op (sub-writes are upserts/replaces), or delete the orphan workspace and retry. |
| **2** Remote index CAS | Conflict exhausted / network | Workspace exists in DB; index missing or stale for this id; no local markers; same-LocalRoot live mount not armed; cross-sandbox remount not discovered via index | Fail the command. Client CAS already retries (~4×). Re-run the command so index is rewritten. Index is only an arming *hint*; `ListGitWorkspaces` remains runtime authority once armed. |
| **3** Local armed / refresh markers | Local FS error | Remote registration + index OK (remount discovery works); **this** LocalRoot may stay dormant until remount / another arm event | Fail the command. Re-run (marker writes are rewrites) or remount / use another LocalRoot that reads the index. |
| **4** Print success | — | Discovery contract met for this host | Users may use the FS immediately. |
| **5** hydrate | Timeout / network (sync may exit non-zero *after* success line; background start may warn) | Registration and discovery intact; objects may be incomplete | Re-run `drive9 git hydrate` or open paths that trigger on-demand hydrate. Never rolls back steps 1–3. |

Crash between steps 1–3 is recovered by re-running the command. Step 1 is **not**
rolled back when step 2 fails (deleting the registration would orphan tree/state
and is worse than a missing index hint). Delete-path asymmetry: after an
irreversible `DeleteGitWorkspace`, index/local cleanup failures are collected
but local cleanup still proceeds so the target stays retryable (`worktree remove --fast`).

Local arm markers are a **generation** over the marker set (armed body + each
`refresh/<id>` name and body), not max FS mtime alone — so same-second
registrations of a new id still force a live mount to re-list.

---

## 6. Behavior

### 6.1 DORMANT hot path

```text
if !EnableGitWorkspaces → miss
if localArmSignal → arm + list (as needed)
elif armed → ARMED path
else → miss (no list)
```

While unarmed, forbid: `ListGitWorkspaces`, tree, overlay, git-state APIs; forbid force-list solely because the path contains `.git`.

### 6.2 Mount-time remote index

1. After mount completes and the SSE watcher starts, **async** probe the index (must not block mount success).
2. **404 / empty / no entries for this mount after filter** → `dormantConfirmed`; zero git-workspace APIs until local arm.
3. **Relevant entries** → `armed`, then `ListGitWorkspaces` to build runtime.
4. **Network / 5xx / parse errors** → do not confirm dormant; exponential backoff Stat retries (cap ~30s).
5. **403** → permanently unreadable (e.g. fs_scoped cannot read `/.drive9/`); confirm dormant and stop the probe loop.
6. **401** → treat as transient (expired/refreshing auth); exponential backoff retry like network/5xx — do **not** permanently latch dormant on a single 401.

### 6.3 Refresh while ARMED

| Event | Action |
| --- | --- |
| Local armed / refresh marker generation change | force list (subject to backoff/throttle) |
| Path hits a loaded workspace | in-memory runtime |
| Remote index revision/mtime change (throttled Stat, default 60s while FS is active) | force list |
| SSE: index path change (when `EnableGitWorkspaces`) | force list / re-arm |
| No event | **zero** lists |

On list / tree / overlay failure:

- Keep a previous non-empty snapshot if any.
- If never successfully loaded → `loaded=false`; retry after backoff.
- Force requests during backoff are sticky (`pendingForce` + generation); a successful in-flight list must not swallow a mid-flight force.

Concurrent lists: singleflight; waiters re-enter when still needed.

### 6.4 Product boundaries

| Scenario | Git layer visible? |
| --- | --- |
| Never `--fast` | No |
| Same LocalRoot live `--fast` | Yes (next FS op) |
| Remount / new sandbox with index written | Yes (mount probe) |
| Already ARMED; cross-host add/remove | Yes (≤60s Stat or SSE) |
| Already dormantConfirmed; first `--fast` only on another host | **No** (remount or same-LocalRoot local signal) |

### 6.5 Legacy data and escape hatches

No `reindex` user command. If the DB already has workspaces but no index yet:

1. Run any path that writes the index (`--fast`, worktree ops, FUSE delete, …); or
2. Mount with `--git-workspaces=on` to list directly (debug/emergency); or
3. Server auto-maintains index on Upsert/Delete, with optional deploy-time backfill.

---

## 7. Key decisions

| # | Decision | Choice |
| --- | --- | --- |
| 1 | Default poll? | No; DORMANT |
| 2 | Mount-time index read? | Yes; async ≤1 FS Stat |
| 3 | Index path | Tenant-absolute `/.drive9/git-workspaces/index.json` |
| 4 | Index role | Existence only; runtime only via List/Get |
| 5 | Same-machine arm | Directory-level signals; not loaded-id-only scans |
| 6 | CLI index/local write failure | Fail the whole command |
| 7 | CLI write order | Index + local before success/hydrate |
| 8 | Stat failure | 404→dormant; network/5xx/401→backoff retry; 403→dormant, stop probe |
| 9 | ARMED cross-host freshness | 60s throttled Stat and/or SSE on index |
| 10 | After dormantConfirmed, remote first register | Requires remount (or same-LocalRoot local signal) |
| 11 | Default-path delivery | Ship discovery, index, CLI order, and SSE wiring together so “dormant without index writes” cannot break remount |
| 12 | `/.drive9` hiding | No forced hide; same as packs |
| 13 | Legacy | No reindex command; write paths / `--git-workspaces=on` / server maintenance |

---

## 8. Correctness constraints

1. `dormantConfirmed && !localArmSignal` ⇒ 0× git-workspace HTTP.
2. `localArmSignal` must be directory-level.
3. Index is existence-only; runtime is List/Get only.
4. Index path is tenant-absolute; filter by RemoteRoot.
5. CLI: index + local armed before success/hydrate; failure fails the whole command.
6. FUSE workspace delete must update the index.
7. List/arm singleflight + failure backoff; force does not bypass the backoff gate—use sticky re-issue.
8. While DORMANT, `.git` paths must not force-list.
9. No forced hiding of `/.drive9/`.

---

## 9. Implementation anchors

| Area | Location |
| --- | --- |
| State machine / arm / list | `pkg/fuse/git_workspace.go` |
| Local arm signals | `pkg/gitcache/arm.go` (plus existing refresh/deleted markers) |
| Remote index | `pkg/client/git_workspace_index.go` |
| CLI write order | `cmd/drive9/cli/git.go` |
| Mount probe | `pkg/fuse/mount.go` (async after SSE start) |
| SSE index | `pkg/fuse/sse.go` |
| FUSE workspace delete | workspace-root delete path in `pkg/fuse` |
| CAS | `WriteCtxConditionalWithRevision` |
| e2e | `e2e/git-workspace-smoke-test.sh` (includes sandbox remount) |

---

## 10. Acceptance criteria

1. No index, no local markers: mount + active FS for 120s → `ListGitWorkspaces` = **0**, index Stat **≤1** (404 expected).
2. Same LocalRoot: after `--fast`, next `ls`/stat enters the git layer.
3. Already armed, then a new workspace id (local marker generation advances) → another list.
4. `sandbox_restore` (fresh LocalRoot remount) passes on the default path.
5. After all workspaces removed and index empty, a new mount → 0 lists.
6. Under network faults, list count is backoff-bounded, not per-op.
7. Existing git-ops / git-workspace e2e pass.

---

## 11. Traffic magnitude (reference)

| Scenario (~10 min active mount) | Before | This design |
| --- | --- | --- |
| Never `--fast`, index 404 | ~1 list/s | **0** lists; **1×** FS Stat |
| Mid-session same-host `--fast` | continuous lists | On-demand after arm |
| New sandbox with existing workspace | continuous lists | 1× Stat + 1× list + on-demand |
| ARMED cross-host change | ~1s convergence | ≤60s (Stat) or near-real-time (SSE) |

---

## 12. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Ship dormant-only without index writes → remount loses discovery | Deliver DORMANT + index + CLI order in one release |
| Index drifts from DB | List is authoritative; server-maintained index; `--git-workspaces=on` escape |
| Concurrent index writers | Revision CAS + bounded retries |
| fs_scoped cannot read `/.drive9/` | Document; stop probe on 403; `--git-workspaces=on` |
| Forged index | After arm, validate via List; index omits sensitive full fields |
| After dormantConfirmed, remote first `--fast` invisible | Product boundary: remount; document clearly |

---

## 13. Optional follow-ups

- Skip `ListGitTree` when `HeadCommit` is unchanged (cost reduction).
- Server auto-maintains index on Upsert/Delete (bare API consistency; CLI CAS can simplify).
- CLI flag matrix: `--git-workspaces=auto|on|off|poll` and perf counters (`arm_local` / `arm_index` / `list` / `index_stat`).
- Whether fs_scoped needs a RemoteRoot-mirrored index path (if product requires it).

These do not block the default path described above.

---

## 14. Open items (non-blocking)

1. Long-term: whether fs_scoped must include `/.drive9/` in scope, or get a RemoteRoot-mirrored index path.
2. Schedule for server-side index maintenance.
3. Whether to keep a debug `poll` mode (default remains auto/on-demand).
