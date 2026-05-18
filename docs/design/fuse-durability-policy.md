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
| `close-sync` | Buffered locally. | Remote-durable before fsync returns. | Remote-durable before close can report success. | JuiceFS-like close-to-cloud semantics, sync tools, cross-client visibility after close. |
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

`write-sync` can be dramatically slower for normal buffered writers because a
single logical file copy may be split into many FUSE write requests. It is
intended for explicit durability-sensitive workloads, not as the default.

For append-heavy workloads, `write-sync` may repeatedly upload or validate an
ever-growing file snapshot. A sequence of many small writes to a large file can
therefore have O(n^2) byte-transfer amplification. Use `close-sync` or `fsync`
when the required durability boundary is close or explicit fsync rather than
every individual write.
