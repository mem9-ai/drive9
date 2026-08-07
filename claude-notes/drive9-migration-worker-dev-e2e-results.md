---
title: Drive9 Migration Worker V1 Dev E2E Results
updated: 2026-08-08
watches:
  - internal/migration/
  - cmd/drive9-migration/
---

## Summary

Drive9 Migration Worker V1 passed every Worker-owned scenario exercised against the TiDB Cloud Native Dev endpoint, including injected failure boundaries. No new unresolved Worker defect was found. The only material gap is proof of a real business workload completing the external T1 dual-write rollout; the Worker correctly does not infer T1.

## Environment and Evidence

1. Client host: `ubuntu@ec2-13-215-186-124.ap-southeast-1.compute.amazonaws.com`.
2. Endpoint: TiDB Cloud Native Drive9 Dev endpoint.
3. Evidence: `/home/ubuntu/migration-e2e/evidence/` on the client host.
4. Retained isolated spaces: `migration-e2e-faults-20260807`, `migration-e2e-state-20260807`, `migration-e2e-fence-live-20260807`, and `migration-e2e-target-safety-20260807`.
5. No credentials or file contents are recorded here. The retained spaces and evidence were not cleaned up.

## Covered Cases

| Area | Dev E2E case | Result | Execution |
| --- | --- | --- | --- |
| CLI and control | `plan`, foreground `run`, JSON `status`, JSONL `diff`, `verify-full`, `prepare-drive9-cutover` | Passed | Native Dev |
| Startup | Stale Unix socket reclamation; restart requires Deep Recovery; no Ready before recovery | Passed | Native Dev |
| SYNCING | Initial copy; incremental create, update, delete, and rename; checksum and revision convergence; mode repair | Passed | Native Dev |
| Incomplete scan | Source scan failure clears readiness and does not derive a target delete | Passed | Native Dev |
| Source mutation | Source changes during hashing and during a large upload discard stale work, retry from a fresh observation, and converge to the final checksum | Passed | Native Dev |
| Source safety | Invalid UTF-8, NFC collision, special file, nested mount, and live source mount identity change fail closed | Passed | Native Dev |
| Target safety | Unsafe target type, unknown target revision, and resource identity change do not overwrite the target; recovery converges after the fault is removed | Passed | Native Dev plus fault proxy |
| Configuration | Unknown fields rejected; simultaneous phase file and environment source rejected; environment fallback accepted | Passed | Native Dev |
| Checkpoint | Concurrent checkpoint creation conflict; immutable identity mismatch; configuration mismatch; phase rollback rejection | Passed | Native Dev |
| T0 and repair | Restart into `DUAL_WRITE_REPAIRING`; Deep Recovery; repair floor; Grace/CAS conditional repair; post-T0 delete and rename residue retained | Passed | Native Dev |
| Event reporting | Event endpoint failure increments the local failure counter without blocking repair or convergence | Passed | Native Dev |
| Verification | Clean full verification; injected mismatch and repair; persistent failure; crash while verification is running followed by restart and Deep Recovery | Passed | Native Dev |
| T2 fence | Normal cutover; idempotence; no post-fence migration write; restart from complete fence | Passed | Native Dev |
| Fence recovery | Live crash after durable Fence Intent but before Fence Complete; restart completes only forward; data added after intent remains absent | Passed | Native Dev plus fault proxy |
| T1 boundary | Successful verification leaves the Worker in `DUAL_WRITE_REPAIRING`; only explicit cutover preparation advances to T2 | Passed | Native Dev |

## Key Conclusions

1. Incomplete source observations never publish delete decisions.
2. Source-token, target-revision, and resource-identity changes are handled by fail-closed reread and retry paths, never by unconditional writes.
3. Fence Intent is the irreversible write boundary. A restart between intent and completion preserves the no-write invariant and completes the fence forward.
4. Verification state is in memory: a crash during verification clears that result and requires restart recovery before readiness can return.
5. Event delivery remains non-blocking as designed. Its observed failure is the known Server Contract issue [tidbcloud/fs#45](https://github.com/tidbcloud/fs/issues/45), not a Worker data-path failure.
6. Drive9 `chmod` returning 404 is the known Server issue [tidbcloud/fs#50](https://github.com/tidbcloud/fs/issues/50). The Worker now treats that response as a rescan condition and converges without re-uploading committed content (`internal/migration/apply.go:467`, `internal/migration/apply.go:490`).
7. The stale control-socket startup failure is fixed by reclaiming only a socket that is still the same inode and refuses connections (`internal/migration/control.go:127`).
8. No additional Migration Worker functional defect remained after these fixes.

## Code Locations

1. Source stable-read boundary: `internal/migration/scanner.go:343`.
2. Target identity and conditional-apply safety: `internal/migration/apply.go:174`, `internal/migration/apply.go:297`.
3. Phase-source and rollback validation: `internal/migration/config.go:243`, `internal/migration/checkpoint.go:138`.
4. Worker recovery and retry classification: `internal/migration/worker.go:430`, `internal/migration/worker.go:583`.
5. Full verification: `internal/migration/verification.go:15`.
6. Fence transition and recovery: `internal/migration/fence.go:9`, `internal/migration/checkpoint.go:253`.

## Remaining Dev Regression Gap

1. Deploy a real dual-write business workload.
2. Roll every business Pod to the dual-write version and validate the external T1 signal independently of Migration.
3. Run `verify-full` after that rollout and retain workload-side evidence before deciding T2.

This remaining item is outside Worker observation by design and does not invalidate the covered Worker E2E results.
