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
uploader. Explicit fsync and link-source synchronization retain the local
barrier, including their append-log fallbacks into the generic uploader.
Foreground close-sync Flush/Release fallbacks remain eligible for remote-only
durability under the same generation and pending-metadata guards.
Failed uploads retain dirty state for an in-process retry; unacknowledged shadow
bytes are not guaranteed to survive a machine crash. Explicit strict `fsync(2)`
shadow uploads deliberately retain local sync, as do writeback, recovery, staged
handles, and library mounts combining close-sync with interactive/auto sync.
The eligibility check uses live staging state, not a scan of historical WAL
frames. Recovery of historical frames retains its recorded revision and existing
CAS checks; this optimization does not add journal commit markers or retire them.

For every path, read-only `Open` and `Read` share a checked shadow-pin path.
Without pending metadata, only resident caches with a base revision at least
as new as the handle, inode and locally observed commit can be selected.
Absence of append-log configuration or local commit history does not authorize
an unknown disk image, including a torn shadow left after an acknowledged
close-sync upload and restart.

Locally initialized new-file staging at revision zero is also readable while
no committed revision is known. Successful empty initialization (including
Create over a leftover shadow), actual writes into an empty image, and complete
content replacement establish that provenance. Merely opening, syncing,
partially resizing or partially overwriting recovered bytes does not.

Retirement has an explicit reason. Ordinary cleanup invalidates future reads
from that generation, even when the upload returned no revision. Cleanup alone
does not prove that the retired shadow matches the uploaded image, so it never
labels those bytes with a committed revision. Existing readers repin a current
source or use the remote/read cache. Explicit WAL reset and anonymous inode
snapshots (including rename-overwrite) keep their existing lifetime. Unlinked
handles read their captured generation directly; they do not use the ordinary
path's source-selection policy.

PendingIndex metadata may override a newer known remote revision only for the
exact shadow content generation bound when that metadata was published. Pin
identity alone does not prove content identity: writes can change the same fd.
The binding includes the ShadowStore instance and is never persisted. Unbound
metadata falls back to ordinary revision validation; WriteBackCache metadata
owns only its `.dat` payload and never authorizes `.shadow`. Recovery explicitly
binds the selected payload after journal replay and legacy `.dat` migration,
before requests are served. Recovery uploads retain the metadata's CAS base.

`Read` checks its shadow pin once at the shadow-read branch using resident state
only: no path lock, disk lookup or unlink on a cache miss. `Open` applies the
same eligibility rules and separately discards unclaimed disk-only orphans.
A newly published resident source remains discoverable even if its revision has
not changed. Raw path reads cannot bypass the checked pin. Creating a new file
detaches the old path incarnation so stale inode revisions cannot hide local
staging; old open handles and surviving hardlink aliases retain their inode.


The next lifecycle refactor is tracked in
[Shadow claim encapsulation](fuse-shadow-claim-lifecycle.md).

`write-sync` can be dramatically slower for normal buffered writers because a
single logical file copy may be split into many FUSE write requests. It is
intended for explicit durability-sensitive workloads, not as the default.

For append-heavy workloads, `write-sync` may repeatedly upload or validate an
ever-growing file snapshot. A sequence of many small writes to a large file can
therefore have O(n^2) byte-transfer amplification. Use `close-sync` or `fsync`
when the required durability boundary is close or explicit fsync rather than
every individual write.
