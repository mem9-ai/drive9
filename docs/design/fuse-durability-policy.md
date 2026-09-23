# FUSE Durability Policy

## Context

The drive9 FUSE mount optimizes by default for interactive latency. Ordinary
writes are buffered locally, close may hand remote upload to the write-back
cache or commit queue, and explicit fsync may choose a local-durable path when
the mount is far from the server.

That default is good for editors and WAN mounts, but it is not the only useful
contract. Some workloads need fsync to mean remote durability. Others need close
to wait for the cloud write. A smaller set of low-frequency or test workloads
need every write syscall to be remote-durable before it returns.

The public mount option is a single durability profile. Internally, the FUSE
implementation still keeps two lower-level concepts:

- sync mode: what explicit fsync means;
- write policy: when ordinary write or close paths become remote-durable.

Keeping those internal axes separate matches the FUSE operation model while
avoiding two overlapping user-facing knobs.

## CLI

```bash
drive9 mount --durability=auto        :/ /mnt/drive9
drive9 mount --durability=interactive :/ /mnt/drive9
drive9 mount --durability=fsync       :/ /mnt/drive9
drive9 mount --durability=close-sync  :/ /mnt/drive9
drive9 mount --durability=write-sync  :/ /mnt/drive9
```

Valid values:

| Durability | Write syscall | fsync syscall | Close syscall | Use case |
| --- | --- | --- | --- | --- |
| `auto` | Buffered locally. | RTT-based: strict on low-latency mounts, interactive on high-latency mounts. | Existing write-back behavior. | Default, preserve current latency/compatibility behavior. |
| `interactive` | Buffered locally. | Local shadow/journal durable; remote commit async. | Existing write-back behavior. | Editors and WAN mounts where low latency matters more than immediate cross-client visibility. |
| `fsync` | Buffered locally. | Remote-durable before fsync returns. | Existing write-back behavior except existing strict large-file flush behavior. | Tools that explicitly call fsync when they need durability. |
| `close-sync` ([local staging tradeoff](#expected-tradeoffs)) | Buffered locally. | Remote-durable before fsync returns. | Remote-durable before close can report success. | JuiceFS-like close-to-cloud semantics, sync tools, cross-client visibility after close. |
| `write-sync` | Remote-durable before each write returns. | Normally clean after successful writes. | Normally clean after successful writes. | Strongest semantics, tests, low-frequency writes. |

Default is `auto`.

The option applies only to FUSE mounts. WebDAV mounts use their native write
behavior; passing a non-default `--durability` with a WebDAV-resolved mount is
rejected instead of silently ignored.

## Internal Mapping

The CLI maps durability profiles to internal FUSE options:

| Public durability | Internal `SyncMode` | Internal `WritePolicy` |
| --- | --- | --- |
| `auto` | `SyncAuto` | `WritePolicyWriteBack` |
| `interactive` | `SyncInteractive` | `WritePolicyWriteBack` |
| `fsync` | `SyncStrict` | `WritePolicyWriteBack` |
| `close-sync` | `SyncStrict` | `WritePolicyCloseSync` |
| `write-sync` | `SyncStrict` | `WritePolicyWriteSync` |

`SyncAuto` is resolved at mount time by measuring server RTT. RTT above the
threshold resolves to `SyncInteractive`; low RTT resolves to `SyncStrict`.

`close-sync` and `write-sync` intentionally imply strict fsync semantics. This
keeps the public profile monotonic: if ordinary close or write is remote-durable,
an explicit fsync should not be weaker.

## Internal Semantics

### SyncMode

`SyncMode` controls explicit fsync durability.

- `SyncInteractive`: fsync stages dirty data into the local shadow store,
  records pending metadata, and fsyncs the journal. Remote commit happens
  asynchronously through the commit queue or uploader.
- `SyncStrict`: fsync uploads to the drive9 server before returning success.
- `SyncAuto`: mount-time RTT detection chooses one of the above.

The implementation also uses `SyncMode` on the large-file Flush path. In
interactive mode, a large Flush stages shadow/pending state so local
close/drop/open flows can still see the file without waiting for remote upload.
In strict mode, large Flush uploads before returning to avoid remote stat
misses after cache drop.

### WritePolicy

`WritePolicy` controls ordinary write and close behavior.

- `WritePolicyWriteBack`: preserve the existing behavior. Writes are buffered
  locally, Flush may snapshot local state, and Release can enqueue background
  upload through the write-back cache or commit queue.
- `WritePolicyCloseSync`: Flush bypasses write-back staging/debounce and uploads
  to the server before returning. Release keeps a best-effort fallback for
  unusual flows where dirty state reaches Release directly.
- `WritePolicyWriteSync`: Write uploads the current handle contents before
  returning success. On upload failure, the handle rolls back to the pre-write
  dirty-buffer snapshot so data the kernel believes failed is not uploaded later
  by close or fsync.

The mount-level write policy is copied into `FileHandle.WritePolicy` at Create
or Open time. It does not change for that handle.

When a writable handle is opened with `O_SYNC` or `O_DSYNC`, the handle is
promoted to `WritePolicyWriteSync` regardless of the mount durability profile.
This mirrors the operating-system model where per-descriptor sync behavior is
chosen at open time.

## FUSE Placement

go-fuse `Release` has no status return, so it cannot reliably report cloud
upload failure to the application that called close(2). The close error path is
`Flush`, whose status can propagate to close(2).

Therefore:

- `close-sync` is primarily enforced in `Flush`;
- `write-sync` is enforced at the end of `Write`;
- pending-new `rename(2)` (`write tmp; mv tmp final`) is also remote-durable
  on `close-sync` and `write-sync` before rename returns. Upload failure
  surfaces as `EAGAIN` and keeps the local shadow. Writeback/`auto`/
  `interactive`/`fsync` mounts still enqueue that rename asynchronously;
- `Release` handles cleanup and fallback synchronization, but it is not the
  primary close-sync error propagation point.

## Expected Tradeoffs

`interactive` and `auto` on high-RTT mounts keep editor latency low by making
local durability the fast path and remote durability asynchronous.

`fsync` makes explicit fsync calls stronger without forcing every close to wait
for remote upload. Workloads that already call fsync at the right points should
prefer this over close-sync.

`close-sync` improves cross-client/cloud visibility after close, but close
latency includes network, server, database, and S3/db9 latency.
Strict close-sync `Flush` and `Release` shadow uploads omit local fsync only
when the content generation is known and no live pending-index or write-back
metadata references the path. Generation fencing pins the source through remote
acknowledgement. Only Flush/Release request this optimization from the shared
uploader; generic shadow uploads for explicit fsync and link-source synchronization
retain the local barrier, including the append-log fallback into that uploader.
Failed uploads retain dirty state for an in-process retry; unacknowledged shadow
bytes are not guaranteed to survive a machine crash. Explicit strict `fsync(2)`
shadow uploads deliberately retain local sync, as do writeback, recovery, staged
handles, and library mounts combining close-sync with interactive/auto sync.
The eligibility check uses live staging state, not a scan of historical WAL
frames. Recovery of historical frames retains its recorded revision and existing
CAS checks; this optimization does not add journal commit markers or retire them.

For a new nonempty inline file with deferred permissions, close-sync can publish
content and mode together using a single-item batch write with create-only CAS.
This optimization requires a successful `/v1/status` negotiation advertising
`storage_capabilities.batch_write_mode_v1: true`. Missing or false capability,
failed negotiation, and older servers use the ordinary PUT-then-chmod flow.
The check reads the existing status cache and adds no hot-path request.
The acknowledged mode generation is cleared only if it is still current; newer
chmods remain pending and are applied against the committed file. A server that
rejects the batch endpoint with HTTP 404/405 uses the existing PUT-then-chmod path.
The close-sync path logs that fallback and retries batch support after a one-minute cooldown;
a transient gateway response does not disable the optimization for the mount's life.
This retry policy is limited to foreground close-sync. The existing background
commit queue still disables batching until remount after a top-level 404/405 and
uses its single-entry recovery path. Changing that queue's scheduling and retry
policy is separate work; the foreground optimization does not alter it.
Transport failures, malformed responses and other per-item errors do not fall
back to PUT, because the commit outcome may be unknown. Overwrites, empty files,
multipart uploads, locally staged recovery payloads and other durability policies
keep their existing mode flow.

On servers advertising `batch_write_mode_v1`, setting mode through batch-write
is owner-only, matching the chmod endpoint.
A scoped token may write content in its authorized paths without `hasMode`, but
an item with `hasMode` is rejected with per-item 403 before changing content or
permissions. A per-item 403 with no committed revision is a definite non-commit:
close-sync falls back to content-only PUT with the same create-only CAS, then
applies the pending mode through chmod. Authorized bytes can therefore commit
even when chmod is denied, matching the ordinary PUT-then-chmod path. Close still
reports the chmod error and retains the pending mode; a failed fallback PUT also
remains an error. This fallback neither acknowledges mode nor disables batching.
With perf enabled, `close_sync_mode_forbidden_fallback` counts these extra requests.
Backend per-item errors must never map to 403, and post-commit errors must retain
their committed revision. A zero revision on any other error does not establish
that nothing committed; top-level 403 and ambiguous results still fail closed.
The public client contract is documented on `client.BatchWriteResult`; server
authorization, actual mode application and post-commit revision tests live in
[`tidbcloud/fs#189`](https://github.com/tidbcloud/fs/pull/189).
The historical server source in this repository does not enforce or advertise
this contract, so this client's combined path stays disabled against it.
Deploying the server capability enables the optimization for newly negotiated
clients on providers supporting inline batch storage; existing mounts may need
remounting because status is cached. A failed or malformed initial status fetch
also leaves the optimization disabled until successful negotiation, normally
by remounting. The
[release note](../release-notes/2026-09-23-close-sync-create-mode.md) records this
compatibility boundary. The background commit queue requires the same capability
before attaching batch mode fields. Otherwise it batches content only, then
applies pending mode through the ordinary chmod endpoint. Chmod denial preserves
staging and is not acknowledged as a successful mode change. This client gate
does not repair authorization in older servers.

If the server commits a create but its acknowledgement is lost, the handle
retains its create-only CAS and later flushes can continue failing with a
conflict until the application reconciles with the remote file. This inherited
limitation is deliberate: equal bytes/mode alone do not prove commit ownership.
Automatic recovery needs a server-verifiable idempotency/commit identity; it
must not overwrite or adopt another writer's object based only on a stat.

`write-sync` can be dramatically slower for normal buffered writers because a
single logical file copy may be split into many FUSE write requests. It is
intended for explicit durability-sensitive workloads, not as the default.

For append-heavy workloads, `write-sync` may repeatedly upload or validate an
ever-growing file snapshot. A sequence of many small writes to a large file can
therefore have O(n^2) byte-transfer amplification. Use `close-sync` or `fsync`
when the required durability boundary is close or explicit fsync rather than
every individual write.
