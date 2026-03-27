# TODO

## Known Issues

### P1: Reaper storage_ref overwrite data loss (upload.go:175-188)

When overwriting an existing file via large-file upload, `ConfirmUpload` updates the
surviving file's `storage_ref` to the new S3 key, then marks the PENDING file as DELETED.
Both the surviving file and the DELETED tombstone now share the same `storage_ref`.
When the Reaper processes the DELETED file, it will delete the S3 object that the
surviving file references — causing data loss.

Fix options:
1. Clear `storage_ref` on the DELETED file so the Reaper skips S3 cleanup.
2. Have the Reaper check refcount before deleting storage.

### P1: Presigned URL checksum integrity (§11.2)

Server creates multipart uploads with `ChecksumAlgorithm=SHA256`. The client SDK
computes SHA-256 per part and sends it via `x-amz-checksum-sha256` header on each
PUT to the presigned URL. S3 validates the checksum server-side. The checksum is
not bound into the presigned URL itself (client computes it at upload time in a
single pass without buffering the entire file).

### Future: One-time download ticket (§11.2)

Design doc §11.2 specifies download indirection via a one-time ticket. Currently the
server directly 302-redirects to a fresh presigned URL on every GET. The ticket pattern
prevents URL sharing/replay.

### Future: Per-tenant S3 credentials (§10)

AssumeRole and S3 prefix are currently process-global (single env var at startup).
Design §10 expects per-tenant credential/prefix resolution from the control plane
tenants table. This is blocked on control plane implementation.
