# FUSE Write Policy

## Context

The drive9 FUSE mount currently optimizes for interactive latency. A close
usually stages data into the local shadow/write-back state and then hands the
remote upload to the commit queue or write-back uploader. This preserves local
read-after-close behavior, but close returning successfully does not mean the
cloud backend has accepted the write.

Some workloads want stronger semantics:

- close should not return until the cloud write has succeeded;
- every write should be remote-durable before the write syscall returns.

This document defines a mount-level write policy that is captured on each
writable file handle when it is opened.

## CLI

Add a mount flag:

```bash
drive9 mount --write-policy=writeback   :/ /mnt/drive9
drive9 mount --write-policy=close-sync  :/ /mnt/drive9
drive9 mount --write-policy=write-sync  :/ /mnt/drive9
```

Valid values:

| Policy | Write syscall | Close syscall | Use case |
| --- | --- | --- | --- |
| `writeback` | Writes local buffer/shadow state. | Existing behavior: remote commit may be async. | Lowest latency, current default. |
| `close-sync` | Writes local buffer/shadow state. | Waits for remote commit before close can succeed. | JuiceFS-like close-to-cloud semantics. |
| `write-sync` | Waits for remote commit before each write succeeds. | Usually clean; close is a final no-op/verification. | Strong durability, tests, low-frequency writes. |

Default is `writeback` to preserve existing behavior.

The flag applies only to FUSE mounts. WebDAV mounts use their native write
behavior; passing a non-default `--write-policy` with a WebDAV-resolved mount is
rejected instead of silently ignored.

## Relationship To Existing Sync Mode

`--sync-mode=auto|interactive|strict` already exists and controls explicit
`fsync` durability semantics. The new `--write-policy` controls ordinary write
and close behavior.

Precedence:

- `writeback`: keep the existing `Flush`, `Release`, and `Fsync` behavior.
- `close-sync`: ordinary close is remote durable. Explicit `fsync` still follows
  `--sync-mode`, but close is stronger than the default interactive path.
- `write-sync`: each write is remote durable. `fsync` and close are normally
  clean unless a prior write failed or another path dirtied the handle.

## Per-Handle Decision

The mount-level policy is copied into `FileHandle.WritePolicy` at `Create` or
`Open` time. The policy does not change for that handle.

When a writable handle is opened with `O_SYNC` or `O_DSYNC`, it is promoted to
`write-sync` regardless of the mount default. This includes `close-sync` mounts:
the per-descriptor sync request is stronger than close-time durability. This
mirrors the operating system model where sync behavior is chosen when the
descriptor is opened.

## FUSE Placement

go-fuse `Release` has no status return, so it cannot reliably report cloud
upload failure to the application that called `close(2)`. The close error path
is `Flush`, which returns a FUSE status.

Therefore:

- `close-sync` is enforced in `Flush` by bypassing write-back staging,
  debouncing, and commit-queue enqueue. It uploads to the cloud before
  returning.
- `Release` still handles cleanup and remains a fallback for unusual flows, but
  it is not the primary close-sync error propagation point.
- `write-sync` is enforced at the end of `Write` by uploading the current
  handle contents before returning success.

## Upload Strategy

Remote synchronization reuses the existing upload machinery:

- `close-sync` handles reuse the flush-time direct PUT, patch, stream upload,
  multipart, and shadow-spill paths. After a successful shadow-spill upload, the
  local shadow is retired so later read-only opens do not pin stale local data.
- `write-sync` handles do not use the streaming uploader. Each write is treated
  as a serialized write-and-commit transaction for that file handle. This avoids
  reusing flush-time multipart state across multiple per-write commits.
- `write-sync` direct/small writes upload the current full handle contents.
  Existing large-file edits use the patch path when possible.
- Timeouts use `releaseTimeout(size)` so large files are not limited by the
  generic FUSE operation timeout.
- On success, dirty state is cleared and inode/read cache metadata is updated.
- On `write-sync` failure, the write reports failure and the handle rolls back to
  its pre-write dirty-buffer snapshot so data the kernel believes was not
  written is not uploaded later by close or flush.
- On `close-sync` failure, dirty state remains retryable because the write
  syscall already succeeded and `Flush` is the failing operation.

## Expected Tradeoffs

`close-sync` improves cross-client/cloud visibility after close but makes close
latency include network, server, database, and S3/db9 latency.

`write-sync` can be dramatically slower for normal buffered writers because a
single logical file copy may be split into many FUSE write requests. It is
intended for explicit durability-sensitive workloads, not as the default.

For append-heavy workloads, `write-sync` may repeatedly upload or validate an
ever-growing file snapshot. A sequence of many small writes to a large file can
therefore have O(n²) byte-transfer amplification. Use `close-sync` when the
required durability boundary is file close rather than every individual write.
