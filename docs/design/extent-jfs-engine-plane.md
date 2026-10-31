# Extent engine plane (Plan B) — the community JuiceFS engine on the v2 wire protocol

Status: implemented on `feat/extent-jfs-bridge` (stacked on
`feat/extent-v2-client`). This is the second of the two extent client
architectures; see `extent-content-layout-v2-client.md` for Plan A (the
thin client) and the trade-off discussion that produced both.

## Architecture

The community `github.com/juicedata/juicefs` module — unmodified, no fork —
provides `vfs` and the chunk store. Three files inside `pkg/extensions/extent`
adapt it to drive9's extent content layout v2:

- **`bridge.go` / `meta_impl.go`** (the bridge) implement `meta.Meta`. The interface carries
  unexported members (`getBase`, signatures naming unexported types), so no
  out-of-package type can implement it directly; the bridge embeds a nil
  `meta.Meta` to satisfy the interface and overrides every method the
  engine reaches. The namespace maps onto classic REST with FNV-derived
  stable inodes (folded below the engine's special-inode range);
  `Read(ino, chunk)` translates block read plans into JuiceFS slice lists
  (holes ride the engine's native `Id == 0` zero-slice; shadow-merged
  references carry `block_offset` through `Slice.Off`/`Size`);
  `Write` turns uploaded slices into PATCH dirty-block sessions, with
  fresh files created through sequential extent sessions. Compaction, GC,
  sessions, locks and xattrs are stubbed — maintenance is server-side by
  design.
- **`storage.go`** implements `object.ObjectStorage`:
  writes stage slice bytes in memory (the engine uploads before it
  commits; v2 presigns only inside a session, so the real PUT happens at
  commit time), reads resolve deterministic content-addressed ids
  (sha256 folded to 61 bits, disjoint from the provisional `NewSlice` id
  space) through plan URLs with Range-narrowed GETs and plan refetch on
  expiry.

Identity bridging summary: read-side slice ids are derived from the block
sha256, so the chunk store's derived object keys — and its persistent disk
cache — stay content-addressed across sessions; write-side ids are
provisional (content is unknown at `NewSlice` time) and die with the
session. Known semantic impedance: the engine assumes per-chunk
last-writer-wins while v2 commits carry a per-file revision CAS — the
bridge hides this with a re-stat-and-retry, which degenerates under
same-file multi-writer concurrency (documented posture, same as Plan A).

## Mount integration

`plane.go` (the extension handler) routes files onto the engine plane:

| situation | routing |
|---|---|
| existing file, `content_layout=extent` | claimed on open; the engine's writeback, block cache and prefetch own the file |
| fresh create, library `Threshold: 0` (full JuiceFS mode; not exposed to users) | claimed at create; the engine owns the file from byte zero |
| fresh create (default, hybrid) | classic write plane buffers; the writeback commit queue promotes large images to extent at the inline cutoff — files that used to become multipart become extent instead; the engine serves them from the next open |
| classic files | untouched — drive9's writeback/cache planes |

The extension sits inside `appendlog` and outside `writeback` in the
dispatch order, so claimed handles bypass drive9's write plane. Namespace
operations pass through to the classic REST plane. `GetAttr` overlays the
bridge's local size while uncommitted writes are pending; size-only
setattrs on open engine files are intercepted (truncate growth pads a
deduped zero block; shrink re-declares the head block verbatim — truncate
to zero remains a server protocol gap). `Sync`/`Flush` bound landing by
driving the engine flush plus `Bridge.FlushFile` (slice commits otherwise
ride the engine's asynchronous commit thread).

Wiring: no new user-facing knobs — the extent plane arms with the
default extension chain. Large-file promotion follows the server's
inline cutoff (the old multipart upgrade line); `--durability` keeps its
exact semantics (`write-sync` commits every Write through the engine
flush + bridge commit; other policies commit at Flush/Release). The
builtin `--profile extent` selects the library's full mode instead:
every fresh create rides the engine from byte zero (like using JuiceFS
directly). The extension
keeps the `extent` name and sits outside the write plane in the default
chain order (`core, sse, writeback, xattr, specials, extent, appendlog,
...`), so claimed handles own their buffering through the engine.
Legacy servers without the v2 protocol degrade the promotion to classic
multipart silently.

## Verification

- `pkg/extensions/extent` unit tests: claim routing (extent / classic /
  missing), hybrid passthrough, full-mode create+commit, GetAttr overlay,
  truncate interception — against a fake v2 server.
- `TestEnginePlaneLive` (env-gated) and the bridge's
  `TestBridgeEngineRoundTripLive`, both green against a live
  drive9-server built from the v2 server tree: engine write →
  server-side extent file → engine read-back byte-identical → v2 thin
  client cross-check → incremental PATCH round-trip.
- FUSE-level (real mount) validation still needs a macFUSE machine; the
  mixed-tree e2e rewrite applies to both plans.

## Server-side behavior (verified live, TestExtentServerAuditLive)

- Schema: exactly four extent layout tables beside the classic set; no
  v1 `jfs_*` remnants, no namespace duplication, no projection writes —
  the commit transaction touches only the extent tables plus the classic
  inode row (size/revision/mtime).
- Quota rides the classic reservation machinery (initiate reserves,
  confirm applies the old→new delta post-commit, asynchronously).
  Verified exact around create (+size), 70 in-place patches (±0 —
  logical size never moved), compaction (±0), and delete (−size).
- Compaction enqueues at ≥64 slices per chunk; the tenant worker may
  merge mid-write (commit kicks), leaving residue below the threshold
  for a later pass. Verified: task reaches COMPLETED, chunk merges to 1
  slice.
- Reclamation timings: file GC on the tenant worker's 5-minute tick
  (tombstone first), the delete reconciler every 10 minutes, block
  objects only after the 24-hour GC grace. All are server tunables, not
  client concerns.

## Known gaps and follow-ups

- The engine module is pinned at the community pseudo-version
  `v1.4.0-dev.0.20261023073824-b2a3be78a08b` (no fork, no local
  replace). `meta.Meta` is an internal-grade API — expect re-sync on
  upgrades.
- fsync semantics: `Sync`/`Flush` flush the bridge, but the engine's
  commit thread remains asynchronous between calls; tight fsync-latency
  guarantees would need a commit acknowledgement hook.
- Same-file multi-writer: revision-CAS retry loop only (see above).
- Truncate-to-zero on extent files: blocked by the PATCH `new_size > 0`
  constraint (server-side relaxation tracked with the fs session).
- The library-level full mode (Threshold 0) sends every file to extent
  layout: db9 embeddings and inline storage do not apply — not exposed
  through any mount knob; price it before ever exposing it.
