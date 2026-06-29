**Status**: Draft
**Date**: 2026-06-29
**Author**: dev-2 (Raft channel `#drive9`, msgs `7668a1bf` → `f0e77896`)

# Content-Hidden Tag Design Doc

> Goal: introduce a reserved `drive9.content_hidden` tag that lets an
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
2. **Reserved tag namespace**: the special tag is `drive9.content_hidden`.
   The `drive9.` prefix (dot, not colon) matches the existing
   system-owned tag style at `pkg/backend/image_extract_structured.go:11`
   (`imageExtractTagPrefix = "drive9.image."`), keeping policy-tag
   convention consistent. Other tags are user-controlled metadata as
   today.
3. **Officially wired paths fail closed; this is not a security
   boundary**. The wired read/stat/search surfaces honor the tag
   and return empty when set. If a future endpoint is added without
   wiring the check (or a code path was missed), the failure mode is
   a real-byte leak — there is no global interceptor. strategy-2 PR
   review msg `2f0fd334` flagged that the earlier draft overclaimed
   here. Mitigation: §6bis enumerates surfaces; §11 R6 calls out the
   "forgot a surface" risk; long term, a real ACL layer (Workspace
   Zones / scoped tokens) is the right primitive for hard isolation.
   Real bytes on storage are never modified by tag toggle, so removal
   restores the file exactly.
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
| `drive9.content_hidden` | `"true"` | user-written at upload (or via `tags` PUT) | When the value is exactly `"true"`, the file's read path returns EOF and stat reports `size=0`. Missing or any other value → real behavior. |

The value comparison is **strict**: only the literal string `"true"`
enables content hiding. Any other value (empty, `"1"`, `"yes"`,
case-different `"TRUE"`, anything else) is treated as absent.

Strategy-2 PR review msg `2f0fd334` flagged that a broader truthy
list expands the policy surface unnecessarily — for a reserved
policy tag, strict matching makes user intent, test cases, and
client implementations unambiguous.

Constants land in a new helper:

```go
// pkg/server/content_hidden.go
package server

const (
    ContentHiddenTagKey   = "drive9.content_hidden"
    ContentHiddenTagValue = "true"
)

func IsContentHidden(tags map[string]string) bool {
    return tags[ContentHiddenTagKey] == ContentHiddenTagValue
}
```

### 3.2 CLI

`drive9 fs cp` is **not** modified in this PR — uploads already accept
arbitrary tags via the existing `--tag` flag (e.g. `drive9 fs cp foo
:/ --tag drive9.content_hidden=true`). qiffang's "client carries the
marker automatically" can be done at the calling layer (web UI,
client config); the adapter just honors the tag.

If a future PR wants a dedicated convenience flag (`--hidden-content`),
that's a thin alias over `--tag drive9.content_hidden=true`.

### 3.3 ACL

Per qiffang msg `f0e77896` ("any uploader can mark"), no ACL is added
in this PR. Any client with write access to the file can set or
remove the tag. A future PR can gate the reserved tag namespace
behind admin tokens — that's an additive change with no wire impact
on this design.

## 4. Server Read / Stat Path Changes

The four entrypoints that surface a file's content / size are:

- `pkg/backend/dat9.go:1555` — `Dat9Backend.ReadPlanCtx` (returns
  inline data OR S3 presign URL).
- `pkg/backend/dat9.go:1642` — `Dat9Backend.ReadInlinePlanCtx` (batch
  / small-read fast path).
- `pkg/server/server.go:1719` — stat handler, `size = nf.File.SizeBytes`
  and `semanticText = …`.
- `pkg/server/server.go:1586` — `ReadDirCtx` returns `[]Entry{Name,
  Size, IsDir, ...}` per file.

The change is uniform: after loading the file's tags (already done
in the stat path via `GetFileTags`), if `IsContentHidden(tags)`:

- `Entry.Size` and `stat.size` return `0`.
- `ReadPlanCtx` returns `ReadPlan{InlineData: []byte{}, Size: 0,
  Revision: nf.File.Revision, Mtime: fileMtime(nf.File)}`. **PresignURL
  must NOT be set** — a presigned S3 redirect would bypass the read
  gate and let the client fetch the real bytes directly (strategy-2
  msg `6e779f6d` flagged this as the highest-risk leak).
- `ReadInlinePlanCtx` returns the same empty `ReadPlan` shape.
- Stat handler additionally blanks `semanticText` and any other
  derived-from-content field; otherwise `?stat=1` leaks content via
  the semantic-text channel (strategy-2 §"内容泄漏面").
- The HTTP GET handler ends up writing 0 bytes with
  `Content-Length: 0` because `ReadPlanCtx` returns empty
  `InlineData`. ETag is derived from `revision`, so kernel cache
  sees a fresh ETag across tag flips (revision bumps in §5).
- Range reads return 0 bytes with `Content-Range: bytes */0` if a
  range was requested.

The backend `ReadPlanCtx` is the chokepoint — putting the check
**after** `PresignGetObject` would leak S3 bytes. The gate must run
**before** any presign call. Server-layer handlers (`handleGet`,
`handleStatMetadata`) trust the `ReadPlan.InlineData` / `PresignURL`
contract, so once the backend is correct, handlers don't need
hidden-specific branches.

### 4.1 Where to load the tag

The stat handler already calls `GetFileTags`. The read handler
currently does not — adding the lookup is one DB query per read
request.

**P0 stance: no server-side hidden-bit cache.** strategy-2 PR
review msg `2f0fd334` flagged that a TTL cache on a policy gate has
exactly the wrong failure modes:

- Stale `false` after the tag is set → returns real content (leak).
- Stale `true` after the tag is removed → keeps returning empty
  content (data unavailable).

The revision-bump + SSE-invalidation chain (§5–§6) already exists
to keep FUSE / HTTP cache consistent; adding a second TTL cache on
the server side multiplies the consistency problem without buying
much. P0 does direct lookup in `ReadPlanCtx` /
`ReadInlinePlanCtx`, and `ReadDirCtx` uses a single batch
`GetFileTagsByFileIDs(fileIDs...)` (one query per dir-listing) to
avoid N+1.

If profiling later shows hot-path stat/read overhead from the tag
lookup, a P1 PR can add a cache *behind* a correctness-test gate.
Without correctness tests proving the cache is invalidated on every
flip, the cache stays out of tree.

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

When the client sets `drive9.content_hidden=true` during upload, the
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

## 6bis. Content Leak Surfaces (Beyond Direct Read)

strategy-2 msg `6e779f6d` flagged that the read/stat HTTP path is not
the only content surface — anything that serves derived-from-content
text must also honor the hidden tag, or P0 leaks via the side
channel.

Surfaces that need explicit hidden-tag handling in this PR:

| Surface | Where | Required behavior |
|---|---|---|
| Stat metadata `semantic_text` | stat handler at `pkg/server/server.go:1753` | blank `semantic_text` when hidden |
| Stat metadata `content_type` | stat handler `pkg/server/server.go:1752` | blank to `""` or `"application/octet-stream"` — leaking MIME reveals class (`application/pdf` → "this is a doc scan"). Default: blank to `""`. |
| Stat metadata `checksum_sha256` | `pkg/server/fs_layer.go:66` and `git_workspace.go:84,95,109,123,138` | blank when hidden — content hash enables rainbow-table identification against known-public files (forms, ID-doc templates) |
| Stat metadata `tags` (sibling system-derived) | stat handler `pkg/server/server.go:1754` | strip `drive9.image.*` and other `drive9.<derived>.*` system tags (they expose EXIF / extracted-content info); keep `drive9.content_hidden` itself and user-set business tags |
| Inline batch read | `Dat9Backend.ReadInlinePlanCtx` (`pkg/backend/dat9.go:1642`) | empty `InlineData`, no `PresignURL` |
| Grep / search / snippet | server search endpoints (find via `grep -r "snippet"` in `pkg/server`) | skip hidden files or return empty matches |
| Semantic / embedding query | server endpoints serving `description_embedding` or returning matched text | skip hidden files in result set |
| ETag / `Last-Modified` headers | GET handler | derive from `revision` so kernel/HTTP cache rotates on tag flip |

The three middle rows (`content_type`, `checksum_sha256`, sibling
system-derived tags) come from adversary-2 PR review msg `1c836768` —
they're the same family as `semantic_text`: derived-from-content
metadata leaked through stat. `checksum_sha256` is the highest-risk
of the three (cryptographic identity → rainbow-table lookup against
any public template).

User-set business tags stay visible: hiding them would break
client workflows that rely on tag-based file selection
(e.g. "ls all files tagged `project=alpha`"). System-derived
`drive9.<extracted>.*` tags are different — they're extracted from
content and would re-leak it.

The grep/search/embedding endpoints are the highest-risk side
channel — they return content excerpts, so leaving them untouched
makes "hidden" only true for direct reads. P0 explicitly includes
these.

If a search endpoint can't easily filter hidden files at the SQL
layer (e.g. it joins across multiple tables), the acceptable fallback
is to **post-filter** the result rows by re-checking the hidden tag.
The performance hit is acceptable for P0 because hidden files are
expected to be a small fraction of all files.

### 6bis.1 Raw SQL bypass

`ExecSQL` (`pkg/datastore/store.go:3130`) permits arbitrary
INSERT/UPDATE/DELETE against `file_tags`. A direct
`UPDATE file_tags SET tag_value='false' WHERE tag_key='drive9.content_hidden'`
would change visibility **without** bumping `inodes.revision`, so
FUSE/HTTP caches stay stale and the file appears hidden when it
should be visible (or vice-versa).

P0 stance (strategy-2 §"Raw tag change"): **document that raw SQL
must not be used to mutate the reserved `drive9.content_hidden`
tag**. The official `ReplaceFileTagsTx` path bumps revision; raw
mutations bypass it.

If documentation alone is insufficient, a P0 follow-up can intercept
`ExecSQL` at parse time and reject statements that target the
reserved tag key — this is a small predicate on the parsed SQL AST.
For this PR we lean on documentation and a regression test that
flags the gap.

## 7. Write Path When Tag Is Set

Per strategy-2 msg `6e779f6d` and dev-1 msg `85cf1089`, writes to a
content-hidden file are **rejected** at the server layer:

- PUT (full upload over an existing path), append, patch, truncate,
  and multipart upload-complete check the hidden tag on the target
  file before mutating bytes.
- If hidden, return `409 Conflict` (or `403 Forbidden`) with message
  `file content is hidden by policy; remove drive9.content_hidden tag before writing`.
- FUSE write returns `EROFS`.
- `delete` and `rename` are NOT gated by this PR — they're metadata
  operations that don't read or write content. If protecting hidden
  files from deletion ever matters, it belongs in a real ACL layer
  (Workspace Zones / scoped tokens), not in this policy tag.

This gives the user a clean mental model: while hidden, the file is
read-only-empty. To modify it, remove the tag first, write, then
reapply the tag.

The earlier draft proposed "accept writes silently" — that was
rejected by strategy-2/dev-1 because it creates a "write succeeded
but I can't read it back" trap that would surface as a bug report.
Explicit rejection is the right default.

## 8. Tests

### 8.1 Unit

- `IsContentHidden` truthy/falsy parametrize (true / 1 / yes / on /
  TRUE / "false" / "" / missing).
- `ReplaceFileTagsTx` regression: tag flip bumps `inodes.revision`;
  tag write without flip does NOT bump.

### 8.2 Integration (server-level)

- Upload file with `drive9.content_hidden=true` → ls shows name with
  `size=0` → cat returns 0 bytes → real bytes still exist on storage
  (verify via direct backend probe).
- Remove tag → ls shows real size → cat returns real bytes.
- Set tag → verify SSE event emitted with type `file_attrs_changed`.
- Remove tag → verify SSE event emitted.
- Stat hidden file → `semantic_text == ""`, `content_type == ""`,
  `checksum_sha256 == ""` in the response JSON; sibling
  `drive9.image.*` tags absent.
- Stat hidden file → `drive9.content_hidden` tag itself **is**
  present in the response (so clients can introspect the policy).
- Stat hidden file → user-set business tags (`project=alpha`,
  arbitrary non-`drive9.` keys) **are** preserved.
- `ReadPlanCtx` for hidden file → `PresignGetObject` mock asserts
  zero invocations.
- Search / grep / semantic endpoint → hidden file is not in result
  rows; non-hidden files in the same dir are returned normally.

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
- ACL for who can set the reserved tag. Anyone with write access can
  set in P0; admin gating is an additive future PR.
- A virtual-namespace view of all content-hidden files (e.g.
  `:/tags/content-hidden/`). Different feature class; not requested.
- Encryption-at-rest changes — bytes on storage are unchanged, the
  tag only gates the read path.
- "Soft delete" semantics — `file_nodes` are unchanged, file is
  still fully discoverable, only read content is gated.
- Strong security boundary. This is policy, not capability — already
  opened FDs, old read caches, and any not-yet-wired endpoint can
  still serve real bytes. Strategy-2 and dev-1 both flagged this:
  use scoped tokens / Workspace Zones for real isolation.
- Hiding `description_embedding` row from the database. Embeddings
  are derived metadata; blanking the visible `semantic_text` is in
  scope, but vectors themselves stay so re-enabling the file is a
  no-op recompute.

## 10. Resolved Design Decisions (was: Open Questions)

The previous draft of this doc left several questions open. Review
from strategy-2 (msg `6e779f6d`, `0c6d2b05`) and dev-1
(msg `85cf1089`, `579abbfe`) resolved them — captured here so the
implementation has a single answer to point at.

1. **Tag key spelling**: `drive9.content_hidden` (dot). Matches
   existing `drive9.image.` system-tag style. Source: strategy-2
   `0c6d2b05`, dev-1 `579abbfe`.
2. **`stat.size` while hidden**: `0`, not real size. `ls -l N + cat
   empty` looks like data corruption; `ls -l 0 + cat empty` is a
   self-consistent 0-byte file. Source: adversary-2 `1aaa22b5`,
   dev-1 `85cf1089`.
3. **Write behavior when tag is set**: **reject** writes (`409` over
   HTTP, `EROFS` over FUSE). Source: strategy-2 `6e779f6d`, dev-1
   `85cf1089`.
4. **Search / semantic index leak**: hidden files must be filtered
   out of grep/search/snippet/semantic responses and `semantic_text`
   blanked in stat metadata. Source: strategy-2 `6e779f6d`.
5. **`Content-Length`**: `0` (consistent with stat). Alternative
   (`Content-Length: N` with 0-byte body) was rejected as POSIX-
   incoherent.
6. **ETag**: derived from `inodes.revision` so HTTP/kernel cache
   rotates on tag flip. Default yes.
7. **Raw SQL tag bypass**: documented as unsupported in P0; possible
   `ExecSQL` parser intercept in a follow-up. Source: strategy-2
   `6e779f6d`.
8. **Tag value matching**: strict literal `"true"` only — no
   `1`/`yes`/`on`/`TRUE` aliases. Source: strategy-2 `2f0fd334`.
9. **No P0 hidden-bit server cache**: direct lookup in read paths,
   batch lookup in `ReadDirCtx`; cache is P1 only after correctness
   tests prove flip invalidation. Source: strategy-2 `2f0fd334`.

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
- **R5 (medium)**: tag write without inode update (e.g. a direct
  `ExecSQL` raw mutation) leaves cache inconsistent. Mitigation:
  documented as "do not bypass `ReplaceFileTagsTx`"; integration
  test verifies the helper bumps revision; follow-up PR can intercept
  `ExecSQL` AST to block raw mutation of the reserved tag.
- **R6 (high)**: forgetting to mask one read-adjacent surface (e.g.
  a future search endpoint added without a hidden-tag filter) leaks
  content despite tag being set. Mitigation: add a central
  `WhereNotHidden` SQL helper / Go middleware that all content-
  returning endpoints route through; CI lint scan for raw S3 reads
  outside the masked path.
- **R7 (medium)**: S3 presigned URL leaks if `PresignGetObject` is
  called before the hidden check. Mitigation: place the check in
  `ReadPlanCtx` at the top of the function, before any storage
  resolution; explicit test mocks the presigner and asserts zero
  invocations for hidden files.

## 12. Roll-out

P0 ships behind no feature flag — the tag is reserved and unused
today, so adding the behavior is purely additive. If we need to back
out, removing the read/stat short-circuit code restores prior
behavior immediately (the tag becomes inert metadata again).

## 13. Implementation Plan

Step order matches strategy-2's recommended sequence (`6e779f6d`):
helper → backend read mask → stat/list mask → tag toggle revision
bump → FUSE invalidation → leak-surface sweep.

1. **Helper**: add `pkg/tagutil/content_hidden.go` (or
   `pkg/server/content_hidden.go`) exporting
   `ContentHiddenTagKey = "drive9.content_hidden"` and
   `IsContentHidden(tags map[string]string) bool`.
2. **Backend read mask**:
   - `Dat9Backend.ReadPlanCtx` (`pkg/backend/dat9.go:1555`) — load
     tags, if hidden return `ReadPlan{InlineData: []byte{}, Size: 0,
     Revision: nf.File.Revision, Mtime: fileMtime(nf.File)}`. Must
     run **before** `PresignGetObject` (`pkg/backend/upload.go:1427`)
     otherwise S3 redirects leak.
   - `Dat9Backend.ReadInlinePlanCtx` (`pkg/backend/dat9.go:1642`) —
     same masking.
3. **Stat / list mask**:
   - Stat handler `pkg/server/server.go:1719` — set `size=0` and
     blank `semantic_text` when hidden.
   - `ReadDirCtx` (`pkg/server/server.go:1586`) — set `Entry.Size=0`
     for hidden entries. Batch-load hidden bits for the dir's
     file_ids to avoid N+1 `GetFileTags`.
4. **Write rejection**: HTTP PUT / append / patch / truncate /
   multipart-complete check the hidden tag and return `409` with the
   documented message. FUSE write returns `EROFS`.
5. **Search / semantic leak sweep**: locate grep / search / snippet /
   semantic endpoints (`pkg/server` `grep -r snippet|search|semantic`),
   filter out hidden files. Where filter is hard, post-filter rows
   by re-checking the tag.
6. **Tag toggle revision bump**:
   - `Store.ReplaceFileTagsTx` (`pkg/datastore/store.go:1160`) detects
     hidden-bit flip (old tags vs new tags) and calls
     `UpdateInodeContentTx` in the same tx, bumping `revision+1` and
     setting `mtime=now`.
   - Tag write **without** flip MUST NOT bump revision (preserves
     no-op semantics for unrelated tag writes).
   - Image-extract tag-by-prefix path (`pkg/backend/image_extract.go:165`)
     gets the same treatment.
7. **SSE event**: emit `file_attrs_changed` from the flip path
   (`pkg/datastore/fs_events.go`). Carries `FileID`, `InodeID`,
   `Revision`.
8. **FUSE listener**: handle `file_attrs_changed` in
   `pkg/fuse/dat9fs.go` event consumer → `InodeNotify(inodeID)` +
   `EntryNotify(parent, name)` if dir cached.
9. **Tests**: see §8. Specifically include:
   - `ReadPlanCtx` does NOT call `PresignGetObject` when hidden
     (mock the presigner, assert zero calls).
   - Raw `ExecSQL` mutation of the reserved tag does NOT bump
     revision (documents the gap; future-proofs the fix).
   - Search / grep / semantic endpoints skip hidden files.
10. **Docs**: update `docs/design-overview.md` and `docs/architecture-spec.md`
    to mention the reserved tag and its read/write contract.

Estimated effort: **~16 hours** for a single PR with all surfaces
(read mask + stat/list + write reject + search sweep + FUSE notify
+ tests). A staged version is possible (P0 server-only ~8 hours,
P0.1 FUSE invalidation ~4 hours, P0.2 search sweep ~4 hours), but
the leak surfaces in §6bis make staging risky — shipping P0 without
the search sweep is shipping a broken contract.

Recommend the full single-PR version.
