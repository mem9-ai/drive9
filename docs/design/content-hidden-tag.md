**Status**: Draft
**Date**: 2026-06-29
**Author**: dev-2 (Raft channel `#drive9`, msgs `7668a1bf` → `f0e77896`)

# Content-Hidden Tag Design Doc

> Goal: introduce a reserved `drive9:content_hidden` tag that lets an
> uploader mark a file as **list-visible but read-empty**. After the
> tag is set, `ls` / FUSE `readdir` still show the file's name, but
> `stat.size` reports 0 and `read` returns EOF. Removing the tag
> restores the file's real size and real content.

## 1. Background & Motivation

qiffang asked (Raft `#drive9` msg `7668a1bf`, `54af0155`, `f0e77896`) for an upload-time policy that:

1. Lets agents see that a file *exists* (so they can plan around it,
   reference it by path, see its metadata).
2. Hides the file's *content* from any reader, including human and
   agent, until the tag is removed.

Per qiffang msg `6e3c1255`, the use case is:
- **Human upload** with a special marker carried automatically by the
  client.
- After upload, the file shows up in `ls`/FUSE for everyone (no
  per-token scoping).
- Reads return empty content; metadata (name, mtime, owner) stays
  visible.
- Removing the tag immediately restores full read.

This is squarely a **content-redaction policy bound to a tag**, not a
permission / ACL feature. EACCES (path forbidden) and Workspace Zones
(per-token visibility) are out of scope — this is a same-namespace,
same-permissions, tag-driven content gate.

The team (Raft msgs `b8e315e5` strategy-2, `4d917a93` adversary-1,
`19453846` dev-1, `192f47bb` dev-2) considered an alternative virtual-
namespace design (shadow path under `:/tags/content-hidden/<file-id>`)
but qiffang ruled it out (msg `6e3c1255`): "感觉方案 B 太麻烦了, 因为
不涉及一个新的 token".

## 2. Design Principles

1. **Real path, tag-driven content gate**: the file stays at its
   original path; the tag flips `stat.size` and `read` behavior.
2. **Reserved tag namespace**: the special tag is `drive9:content_hidden`
   (key prefix `drive9:` reserves it for system policy). Other tags
   are user-controlled metadata as today.
3. **Fail-safe semantics**: if anything goes wrong (cache stale, tag
   write partial), the worst observable state is `size=0 / read empty`
   for a short window. The real bytes on storage are never modified
   by the tag toggle.
4. **POSIX-coherent**: `stat.size` and the actual `read()` byte count
   stay consistent (both 0 when tag is set; both real when tag is
   absent). No "size=N but read returns 0" mismatch — that's the only
   way to break POSIX expectations.
5. **Cache invalidation is mandatory**: tag toggle bumps
   `inodes.revision` and `inodes.mtime` (and `confirmed_at`) so FUSE
   revalidation and SSE listeners can detect the change. Without this
   step, mounted clients can show stale `size`/content for the
   `attr_timeout` window (default 1s).

## 3. Wire & API Surface

### 3.1 Reserved Tag

| Key | Value | Owner | Semantics |
|---|---|---|---|
| `drive9:content_hidden` | `"true"` (or `"1"`) | user-written at upload (or via `tags` PUT) | When present and truthy, the file's read path returns EOF and stat reports `size=0`. Absent / falsy → real behavior. |

The value comparison is **truthy-only**: `"true"`, `"1"`, `"yes"`,
`"on"` (case-insensitive) all enable content hiding. Any other value
(including empty string) is treated as absent. This matches how
existing env-gated features check booleans in drive9.

Constants land in a new helper:

```go
// pkg/server/content_hidden.go
package server

const ContentHiddenTagKey = "drive9:content_hidden"

func IsContentHidden(tags map[string]string) bool {
    v, ok := tags[ContentHiddenTagKey]
    if !ok {
        return false
    }
    switch strings.ToLower(strings.TrimSpace(v)) {
    case "true", "1", "yes", "on":
        return true
    }
    return false
}
```

### 3.2 CLI

`drive9 fs cp` is **not** modified in this PR — uploads already accept
arbitrary tags via the existing `--tag` flag (e.g. `drive9 fs cp foo
:/ --tag drive9:content_hidden=true`). qiffang's "client carries the
marker automatically" can be done at the calling layer (web UI,
client config); the adapter just honors the tag.

If a future PR wants a dedicated convenience flag (`--hidden-content`),
that's a thin alias over `--tag drive9:content_hidden=true`.

### 3.3 ACL

Per qiffang msg `f0e77896` ("any uploader can mark"), no ACL is added
in this PR. Any client with write access to the file can set or
remove the tag. A future PR can gate the reserved tag namespace
behind admin tokens — that's an additive change with no wire impact
on this design.

## 4. Server Read / Stat Path Changes

The two server entrypoints that surface a file's content / size are:

- `pkg/server/server.go:1719` — stat handler, `size = nf.File.SizeBytes`
- `pkg/server/server.go` GET file handler (range reads, full reads,
  HEAD)
- `pkg/server/server.go:1586` `ReadDirCtx` returns `[]Entry{Name,
  Size, IsDir, ...}` per file; stat info populated from `file_nodes`
  + `inodes`

The change is uniform: after loading the file's tags (already done
in the stat path via `GetFileTags`), if `IsContentHidden(tags)`:

- `Entry.Size` and `stat.size` return `0`.
- The GET handler short-circuits before fetching content from storage
  and writes `0` bytes with `Content-Length: 0` (and proper `Etag`
  derived from tag-revision so kernel cache sees a different ETag for
  hidden vs visible).
- Range reads return 0 bytes with a `Content-Range: bytes */0` header
  if a range was requested.

The read handler does **not** read from S3 / contents table when
hidden — saves bandwidth and prevents accidental side-channel leaks.

### 4.1 Where to load the tag

The stat handler already calls `GetFileTags`. The read handler
currently does not — adding the lookup is one DB query per read
request. For high-throughput read paths, we cache the
`content_hidden` bit on the inode in memory and invalidate on tag
write (see §6). The DB lookup is the slow path; the fast path is a
single boolean check on the cached struct.

Concretely, the cache lives on the in-flight request context:
`hiddenCacheKey{tenant, file_id} → struct{ hidden bool, revision int64 }`,
with a short TTL (matching FUSE attr TTL).

## 5. Tag Write Path Changes

The single hot path for tag writes is `Store.ReplaceFileTagsTx`
(`pkg/datastore/store.go:1160`). Plus tag-replace-by-prefix used by
image extraction (`pkg/backend/image_extract.go:165`).

After writing the new tag set inside the tx, the code computes
whether `IsContentHidden` flipped (old vs new tag map), and if so:

```go
// inside the same tx
if err := s.UpdateInodeContentTx(tx, inodeID, sizeBytes, revision+1, status, time.Now().UTC()); err != nil {
    return err
}
```

This bumps `inodes.revision` and `inodes.mtime`/`confirmed_at`, which
is already the established pattern for content changes (see
`pkg/datastore/file_tx.go:241`, `:299`).

If `IsContentHidden` did **not** flip (e.g. tag write only changed
unrelated user metadata), revision is NOT bumped. This preserves
no-op semantics for normal tag operations.

### 5.1 Special case: upload-time tag

When the client sets `drive9:content_hidden=true` during upload, the
sequence is:

1. `ConfirmPendingFileTx` writes the inode with the real size +
   `revision=1`.
2. `ReplaceFileTagsTx` writes the tags inside the same tx (or a
   sibling tx — see `pkg/backend/upload.go:1142, 1200`).
3. Since the inode is brand new and the tag was set at create time,
   no flip happens (there was no "previous" state). The hidden bit is
   correct from `revision=1`.

The flip-detection logic only kicks in for subsequent `ReplaceFileTagsTx`
calls.

## 6. FUSE Cache Invalidation

drive9 already has FUSE invalidation infrastructure
(`pkg/fuse/dir.go:330` `InvalidatePrefix`, `pkg/fuse/dat9fs.go:90`
`EntryNotify` / `InodeNotify`). The SSE event stream (`pkg/datastore/fs_events.go`)
is the canonical mechanism for "tell mounted clients something changed."

The tag-flip path emits a new SSE event:

```go
type FSEvent struct {
    Type    string // "file_attrs_changed" (new value)
    FileID  string
    InodeID string
    Revision int64
}
```

The FUSE-side listener (already in `pkg/fuse/`) handles `file_attrs_changed`
by calling `dat9fs.InodeNotify(inodeID)` and, if the file is in a
cached dir listing, `EntryNotify(parent, name)`.

This guarantees that:
- Other mounted clients (besides the one that toggled the tag) see
  the change within their FUSE round-trip TTL (default ~1s, bounded
  by SSE event propagation latency).
- The mount that triggered the toggle sees the change immediately on
  the next `getattr` after `InodeNotify` fires.

### 6.1 Cache semantics during toggle

| User action | Before invalidate (kernel cache) | After invalidate |
|---|---|---|
| Set tag → ls | shows old real size for `attr_timeout` | shows size=0 |
| Set tag → cat | reads from page cache (real bytes) until invalidate | reads 0 bytes |
| Remove tag → ls | shows size=0 for `attr_timeout` | shows real size |
| Remove tag → cat | reads 0 bytes from page cache until invalidate | reads real bytes |

The SSE invalidation reduces this window from `attr_timeout` to
roughly the SSE round-trip (10s to 100s of ms). It's still **not
synchronous** — clients must accept a brief window where stat and
read can disagree across the toggle moment.

Documented invariant: `stat.size` and `read()` byte count never
disagree *within* a single getattr/read pair on the same client; they
can only disagree across a toggle that arrived between two
operations.

## 7. Write Path When Tag Is Set

Per the principle "real bytes never modified by tag toggle", writes
to a content-hidden file are still **accepted** at the storage layer
— the bytes go to S3, the inode size updates, but the file remains
read-empty as long as the tag is set. This preserves the symmetry
"toggle off restores real content" even if a write happened during
the hidden window.

Open question for review (see §10): should writes be **rejected**
with `EROFS` while the tag is set? Pros: cleaner mental model, no
"write but can't read back" surprise. Cons: breaks the "tag is a
read gate" invariant — write would also need ACL semantics.

Recommended P0: accept writes (storage path unchanged), document the
behavior. Revisit if real users hit the "writes are silently going
nowhere visible" trap.

## 8. Tests

### 8.1 Unit

- `IsContentHidden` truthy/falsy parametrize (true / 1 / yes / on /
  TRUE / "false" / "" / missing).
- `ReplaceFileTagsTx` regression: tag flip bumps `inodes.revision`;
  tag write without flip does NOT bump.

### 8.2 Integration (server-level)

- Upload file with `drive9:content_hidden=true` → ls shows name with
  `size=0` → cat returns 0 bytes → real bytes still exist on storage
  (verify via direct backend probe).
- Remove tag → ls shows real size → cat returns real bytes.
- Set tag → verify SSE event emitted with type `file_attrs_changed`.
- Remove tag → verify SSE event emitted.

### 8.3 FUSE end-to-end

- Mount FUSE; upload file; set tag on a second mount; verify first
  mount sees size=0 within 2s.
- Mount FUSE; upload file with tag set; verify `ls` shows size=0 +
  `cat` returns empty + `stat` reports size=0.
- Mount FUSE; toggle tag rapidly (set/remove/set/remove); verify
  final state is consistent on the mount within 2s.

### 8.4 Race / negative

- Concurrent `ReplaceFileTagsTx` for the same file: only one ends up
  effective (last-writer-wins), revision bumps at most once per flip.
- Read in flight during tag set: in-progress read can complete with
  real bytes (we accept this — the read started before the tag
  applied); subsequent reads return empty.

## 9. Out of Scope

- Per-token / per-agent visibility (Workspace Zones / scoped tokens).
  Different problem class; document EACCES path if needed in a
  follow-up.
- ACL for who can set the reserved tag. Anyone can set in P0; admin
  gating is an additive future PR.
- A virtual-namespace view of all content-hidden files (e.g.
  `:/tags/content-hidden/`). Different feature class; not requested.
- Encryption-at-rest changes — bytes on storage are unchanged, the
  tag only gates the read path.
- "Soft delete" semantics — file_nodes are unchanged, file is still
  fully discoverable, only read content is gated.

## 10. Open Questions

1. **Write behavior when tag is set**: accept (current proposal) vs
   `EROFS` reject? Default: accept.
2. **`Content-Length: 0` vs `Content-Length: N (real)` + 0-byte body**:
   the proposal sends `Content-Length: 0` (consistent with stat).
   Alternative is to keep `Content-Length: N` for "transparency to
   clients that bypass cache" — rejected because it breaks POSIX
   coherence.
3. **ETag for hidden files**: include the tag-revision in the ETag so
   clients with HTTP cache see a different ETag for hidden vs visible
   versions. Default yes.
4. **Search / semantic index**: should `content_text` /
   `description_embedding` be hidden too? Default no — embeddings are
   metadata, not raw content. If qiffang wants them hidden too, that's
   an extension.

## 11. Risks

- **R1 (high)**: forget to bump `revision` on tag flip → FUSE shows
  stale size/content for up to `attr_timeout`. Mitigation: regression
  test in §8.1; code review checklist item.
- **R2 (medium)**: SSE event missed by a FUSE listener (network blip)
  → stale until next `getattr` evicts the cached attr. Mitigation:
  bound `attr_timeout` to ~1s; document the window.
- **R3 (medium)**: a slow read in flight when the tag is set might
  surface real bytes after the tag write completed → race accepted
  per §8.4; if this matters, P1 PR can add a per-file read-lock that
  blocks new reads while tag write is in progress.
- **R4 (low)**: someone writes the reserved tag with a non-truthy
  value expecting it to "hide". `IsContentHidden` returns false →
  file is fully readable. Mitigation: CLI validation rejects setting
  the reserved tag to anything other than `true/false`; client-side
  warning when value is unrecognized.
- **R5 (low)**: tag write without inode update (e.g. a direct SQL
  bypass) leaves cache inconsistent. Mitigation: documented as "do
  not bypass `ReplaceFileTagsTx`"; integration test verifies the
  helper bumps revision.

## 12. Roll-out

P0 ships behind no feature flag — the tag is reserved and unused
today, so adding the behavior is purely additive. If we need to back
out, removing the read/stat short-circuit code restores prior
behavior immediately (the tag becomes inert metadata again).

## 13. Implementation Plan

1. Add `pkg/server/content_hidden.go` (constant + `IsContentHidden`).
2. Stat handler short-circuit (`pkg/server/server.go:1719`).
3. Read handler short-circuit (find the GET file handler in
   `pkg/server/server.go` — same handler as `Mtime` lookup).
4. `ReadDirCtx` short-circuit (`pkg/server/server.go:1586`).
5. `Store.ReplaceFileTagsTx` flip detection + `UpdateInodeContentTx`
   on flip (`pkg/datastore/store.go:1160`).
6. Emit SSE `file_attrs_changed` event from the tag-flip path
   (`pkg/datastore/fs_events.go`).
7. FUSE listener: handle `file_attrs_changed` → `InodeNotify`,
   `EntryNotify` if dir cached (`pkg/fuse/dat9fs.go` event consumer).
8. Tests: §8 above.
9. Update `docs/design-overview.md` to mention the reserved tag.

Estimated effort: **12 hours for a PR with all tests + FUSE notify
wiring**; **6 hours** for a server-only PR (no FUSE cache invalidation,
relying on `attr_timeout` to converge).

Recommend the full 12-hour version since the FUSE delay is what
qiffang specifically asked about (msg `581f6ebc`).
