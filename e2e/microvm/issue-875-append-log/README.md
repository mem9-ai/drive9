---
title: Issue 875 MicroVM FUSE case
---

This trusted dynamic FUSE case evaluates the observable acceptance criteria of
issue #875 on Linux/arm64:

1. 1,000 independent SQLite WAL commits with `synchronous=FULL` and default
   `wal_autocheckpoint=1000`;
2. close/reopen and fresh-remount integrity and fingerprint verification;
3. append-log perf counters with logical remote bytes at most 10x final WAL
   size and below cumulative WAL observations; and
4. final-100 commit P95 at most 4x initial-100 commit P95.

The image extends the generic FUSE image with Python 3 and the current
Linux/arm64 candidate CLI. The bundle carries only a generated copy of the
canonical FUSE workload script, keeping below the 32 MiB per-file limit.

Preparation is local-only:

```bash
e2e/microvm/issue-875-append-log/prepare-case.sh

/Users/shenjun/.codex/skills/drive9-microvm-e2e/bin/drive9-microvm-e2e \
  case-pack --source e2e/microvm/issue-875-append-log \
  --output /private/tmp/issue-875-append-log-case.zip
```

Case publication, custom image publication, and a target-mutating MicroVM run
are separate external actions requiring explicit authorization. This validates
the Linux MicroVM FUSE path only; issue #875's runc/gVisor comparison remains
a separate CSI environment validation.
