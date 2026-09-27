Perform an acceptance review of the development task that just ran on a drive9 mount, and output the report to stdout.

## Background (how this run works)

- The worker (the agent that executed the task) ran under an audit discipline: it was required to mechanically verify every file operation (read back after writes, verify after deletes/moves, self-check every artifact before completion), and on confirming a suspected filesystem problem it had to abort and report (fs-findings.md at the workspace root plus an `FS-SUSPECT:` marker as the last line of its reply) rather than work around it; a normal completion ends with a `TASK-COMPLETE` marker.
- Integrity checking beyond the worker's per-operation verification is handled by the deterministic workspace audit (executed by the harness after the worker exited): a write probe plus a two-pass full sha256 manifest stability comparison; its result is in the "Deterministic workspace audit" facts block.

## Your job (two things; do not redo what the machines already checked)

1. Delivery completeness: against the task prompt and the workspace tree / key files, judge whether the task was actually completed and the artifacts are complete and reasonably structured. Anything not listed in the tree does not exist.
2. Cross-check and attribution: put the worker's self-reported fs findings, the deterministic audit result, the mount.log anomaly summary, the session error scan, and the worker output tail side by side, and attribute each item to drive9-related / application-own / environment-noise. For every category with nothing found, state explicitly "none found"; never leave a section empty.

## Review mode (important)

- You have NO tools: do not attempt to browse directories, run commands, or read files; all evidence is in the "run facts and workspace snapshot" section below.
- Interpretation guide:
  - `TASK-COMPLETE` means the worker believed it finished normally; you must still independently verify delivery completeness and must not take it at face value.
  - `FS-SUSPECT` means the worker aborted and reported; the final verdict is decided by the harness's mechanical rule (FS-SUSPECT), so the value of your report is the attribution evidence — do not agonize over PASS vs FAIL.
  - A deterministic audit status of fail/timeout/error is hard evidence and must be cited in your attribution.

## Output format (strict; report in English)

```
## Delivery completeness
- ... (item by item)

## Cross-check and attribution
- Worker findings: ... (write "not reported" if none)
- Deterministic workspace audit: ...
- mount.log / session errors: ...
- Attribution conclusion: drive9-related / application-own / environment-noise, item by item

## Conclusion
VERDICT: PASS|FAIL|NEEDS-REVIEW
```

The last line must be `VERDICT: ...` (one of the three). PASS requires: no delivery gaps, and no evidence supporting drive9-attributable file loss, corruption, or hangs; anything attributable to drive9 → FAIL; insufficient evidence → NEEDS-REVIEW with a note on what evidence is missing.
