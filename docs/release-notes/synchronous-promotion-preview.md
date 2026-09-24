# Synchronous Promotion Preview

Synchronous local-to-remote promotion is an opt-in preview. It covers a closed,
ordinary local-only directory tree renamed to an absent remote target, such as
`dist/.site-next -> site`. It remains disabled by default.

The production server half is shipped from `tidbcloud/fs`; Drive9 contains only
the FUSE/client coordinator and shared wire contract. Enable the preview only
with mutually compatible, cross-repository-tested versions.

Operational behavior to know before enabling it:

- Every supported Unix mount with a non-empty LocalRoot takes the exclusive lock at
  `LocalRoot/.drive9/promotion/local-root.lock`, even when the promotion gate is
  off. A second mount of that LocalRoot exits permanently and reports this path.
  Windows FUSE mounts are unsupported; a gate-off library call on Windows does
  not provide this LocalRoot sharing guarantee.
- If a live publish outcome becomes unknown, the mount refuses all namespace
  work, including `open()` and `readdir()`/`ls`, with `EIO` until remount
  recovery resolves it.
- A durable recovery contradiction exits permanently rather than entering a
  supervisor restart loop. The error includes journal path, phase, and
  operation ID; follow the operator recovery procedure in
  `docs/design/local-to-remote-promotion.md`.
- `rename` returns after the remote commit and durable local source switch.
  Receipt acknowledgement and operation-private quarantine deletion continue
  as recoverable background cleanup; their outage does not turn an already
  completed rename into `EIO` or block unrelated mount work.
- Admission checks include every entry's mapped final path. A source tree that
  would land on a local-only or Git-workspace route is rejected with `EXDEV`
  before the journal or promotion endpoint is touched.
- Server namespace writers take an unconditional parent-row serialization lock
  so gate-on and gate-off replicas remain safe during rolling deployment. This
  has a default-off write-throughput cost that must be measured before the
  preview can become the default.

Unsupported shapes (replace, open handles, sparse files, foreign ownership,
xattrs/ACLs, links, special files, mixed-policy trees, or oversized manifests)
fail before any remote publish.
