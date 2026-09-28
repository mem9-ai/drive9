# drive9-simulator Case Specification

- Version: `v2` (this document is the single authoritative specification of the language).
- The key words **MUST / MUST NOT / SHOULD / MAY** are to be interpreted as described in RFC 2119.

## 0. Overview

A case is a POSIX shell script (a `.test` file). The script's execution order is the timeline — everything linear lives in the script body; faults, observation and verdicts are carried by `drive9-test-*` commands injected into the script's environment. Each concern owns exactly one construct:

| Concern | Sole construct |
|---|---|
| workload | the script body (plain POSIX shell: any command, any control flow) |
| fault | `drive9-test-fault` declaration (what) + `drive9-test-arm` / `drive9-test-disarm` / `drive9-test-window` (when) |
| observation | `drive9-test-sample` / `drive9-test-drain` + `@name` snapshot references |
| oracle | `drive9-test-check` assertions (the **only** construct counted in pass/fail statistics) |

The script's `echo` and all natural stdout/stderr output are logs with **no semantics**; the engine performs no output parsing. Verdicts come from exactly two sources of fact: case entries produced by `drive9-test-check`, and the script's own terminal state.

Delivery contract (four-state verdicts and merging, evidence artifacts, attribution) is implemented by the engine and the report layer; it is not part of this language's syntax.

## 1. Definitions

- **Test script (case file)**: a `.test` file, a POSIX shell script; the unit of execution for `drive9-simulator run`.
- **Injected command (`drive9-test-*`)**: a command the engine places at the front of the script's `PATH`, talking to the engine over a control channel. The prefix `drive9-test-` is reserved (§4.3).
- **Case entry**: the execution record of one `drive9-test-check` assertion `{claim, kind, verdict, req, …}` — the smallest unit of pass/fail statistics. Echo output, primitive invocations and payload failures MUST NOT directly produce case entries.
- **Payload**: a command string handed to an injected command for execution (the part after `--` of `drive9-test-window` / `drive9-test-async` / `drive9-test-check shell`).
- **Window**: the execution interval of `drive9-test-window`; failures of the payload inside the window are adjudicated by the result classes of faults armed during the window.
- **Fault**: an environmental disturbance applied by the injector; lifetime = arm to disarm (or hold expiry).
- **Snapshot**: a hash manifest (type / size / mode / mtime / sha256) of some moment and vantage, referenced as `@name`.
- **Vantage**: an observation entry point: `mount` (the writer's mount) / `observer` (a second mount, opened early and long-lived) / `remote` (a stateless independent entry).
- **Evidence row**: a record produced by window adjudication and termination events: `ok` / `tolerated(<result class>)` / `engine-terminated` / `kill-interrupted` / `failed`. An evidence row is **not** a case entry but participates in run verdict aggregation (§5.2).
- **Control run**: an automatic replay of the same script in a stub environment against a plain local directory, producing artifacts under `control/` (§5.4).
- **Sandbox**: `host` (execution directly on the host) or `firecracker` (execution inside a microVM, controlled from the host).

## 2. File layout

```text
cases/
  fsync-cut.test          # one case: one POSIX shell script
  fsync-cut.fixtures.tgz  # a same-name non-.test file = private helper for this case (optional)
  lib/                    # shared helpers (optional)
```

- A case file MUST end in `.test`; `drive9-simulator run <file.test>` runs it, and `validate` recognizes only `*.test` files as cases.
- Case name = file stem; MUST be unique across the repository; reports and references use this name.
- Helper files (fixtures, helpers) live next to the case or in a subdirectory and MUST NOT use the `.test` suffix; scripts reference them via `$(dirname "$0")`.
- The script MUST be POSIX sh (passes `sh -n`); `#!/bin/sh` and starting with `set -eu` are recommended (`set -e` is a MUST, see §4.1).

## 3. Complete example

```sh
#!/bin/sh
set -eu
FILES=${FILES:-20}

# Fault declaration: blackhole from the 40th upload, held for 8 seconds.
drive9-test-fault cut blackhole \
    --pattern 'PUT /v1/fs' --count 40 \
    --hold 8s --expect may-fail-inflight

# --- Phase 1: a normal round trip; leave a baseline snapshot ---
mkdir -p settled
i=0; while [ $i -lt $FILES ]; do i=$((i+1)); printf 'payload-%s' "$i" > "settled/f$i"; done
drive9-test-drain --mode require-ok --timeout 120s
drive9-test-sample settled

# --- Phase 2: import a project from a local directory inside the cut window ---
drive9-test-arm cut
drive9-test-window --expect may-fail-inflight --timeout 10m -- \
    cp -R "$HOME/project" "$D9_MOUNT/imported"
drive9-test-disarm cut

# --- Phase 3: recovery and final state ---
drive9-test-drain --mode require-ok --timeout 120s
drive9-test-sample final --as remote

# --- Verdicts: each assertion below is one case entry in the statistics ---
drive9-test-check fault-fired --fault cut                          --req 'A-step2'
drive9-test-check manifest-contains --this @final --against @settled \
    --paths 'settled/'                                             --req 'A-check4'
drive9-test-check manifest-equals --this @final --against control \
    --paths 'imported/'                                            --req 'A-check5'
drive9-test-check shell --req 'A-check5' -- diff -r "$D9_MOUNT/imported" "$HOME/project"
drive9-test-check sync-ok                                          --req 'A-check5'
```

## 4. Specification

### 4.1 Execution environment

The engine MUST have the drive9 mount ready before starting the script. At script start:

| Item | Value |
|---|---|
| cwd | the **drive9 mount point root**. Every relative-path I/O in the script targets drive9; cwd is not an ordinary local directory — it is the object under test |
| `D9_MOUNT` | absolute path of the mount point. After `kill client` / `remount` the cwd and open fds may go stale; scripts SHOULD re-resolve via `$D9_MOUNT/...` and `cd "$D9_MOUNT"` after a remount |
| `PATH` | carries all `drive9-test-*` commands at the front |
| `D9_CONTROL` | engine control channel (used internally by injected commands; scripts MUST NOT touch it) |

Case-level environment configuration does **not** live in the script: which environment this run executes on (`--durability` / `--overlay` / `--write-cache-size-mb`) and the wall-clock limit (`--timeout`) are all `drive9-simulator run` CLI flags (§6). The script carries logical correctness only and MUST NOT depend on these values.

- The script MUST enable `set -e` before its first non-comment command (normally `set -eu`).
- When a local directory outside drive9 is needed for file preparation, the script MUST create it under `/tmp` or `$HOME` itself (prefer `mktemp -d`). The engine provides no extra scratch area.
- The control run executes the same script a second time (§5.4): local scratch paths MUST be unique or rebuildable per execution; the two executions MUST NOT interfere.
- stdout/stderr are captured in full into the run directory as logs only (§4.7).
- The whole-case wall-clock limit comes from the `timeout` CLI flag (default `30m`); on expiry the engine kills the script and records `engine-terminated`.

Duration literal syntax: `<number><unit>` with unit in `ms | s | m | h` (e.g. `"8s"`, `"120s"`, `"30m"`).

### 4.2 Parameterization (plain shell variables)

Tunable inputs of a case (file counts, concurrency) are ordinary shell variables with defaults:

```sh
FILES=${FILES:-100}
WORKERS=${WORKERS:-4}
```

- Callers MAY override them via the environment (`FILES=1000 drive9-simulator run …`) to obtain multiple independent runs.
- The engine performs no tier expansion and no text templating; this language has no tier concept.

**Custom payload (`PAYLOAD_DIR`)** — a case whose workload is a file set (import / build / export / recovery over a project tree) SHOULD accept an externally supplied one:

```sh
PAYLOAD_DIR=${PAYLOAD_DIR:-}          # reserved name
if [ -n "$PAYLOAD_DIR" ]; then
    cp -a "$PAYLOAD_DIR/." target/    # copy in; never modify the source
else
    : # ... default synthetic generation ...
fi
```

- `PAYLOAD_DIR` is provided either by the run CLI flag `--payload-dir <dir>` (validated: existing, non-empty directory; recorded in `run.json`; exported to BOTH the real run and the control replay) or directly as an environment variable.
- The payload source is **read-only input**: the script MUST copy from it and MUST NOT write, move, or delete anything inside it; both executions (real + control) read the same source, so a consumed/mutated source corrupts the control oracle (§4.1).
- With a custom payload the case's oracles must stay content-agnostic (derived from the copied input, or compared against `control`); fixed-content assertions that only hold for the default generation MUST be guarded by the existence of the referenced entries.
- A case that declares `PAYLOAD_DIR` support MUST fail loudly when the directory is empty (a scenario that cannot produce evidence must never degrade silently, §4.3).

**Bystander payload (`--seed`, engine-level, works with ANY case)** — the complement to `PAYLOAD_DIR` for cases that do not (or cannot) consume an external tree as their input:

```text
drive9-simulator run <case.test> --seed <dir>
```

- Before the script starts, the engine copies the tree onto the mount at the reserved path `__payload__/` (the copy itself travels through the mount under test) and drains, so the baseline lives on the server before any workload or fault runs. The control replay receives the identical tree, so every `manifest-equals` against `control` stays balanced.
- After the script ends, the engine samples the terminal state from a **remote vantage** and compares `__payload__/` against the source (path / kind / size / sha256; mode is not compared). Divergence is fail-grade evidence; the check is journal-recorded as `seed-integrity` and appears in `run.json` (`seed_dir`).
- Semantics: **non-interference + survival** — a real project tree (e.g. exported from a simulator workbench run) rides through the case's workload and faults untouched, which is the "the real project survives the scenario" reading of acceptance. `--seed` and `--payload-dir` MAY be combined.
- `__payload__/` is a reserved mount path; scripts MUST NOT write into it.

### 4.3 Injected-command rules

- The prefix `drive9-test-` is **reserved**: the script MUST NOT define, alias or shadow any `drive9-test-*` name (functions, aliases, PATH substitution are all forbidden); calling a `drive9-test-*` name outside the registry is an error.
- Injected commands talk to the engine over `D9_CONTROL`. An injected command's **operational failure** (broken channel, unknown reference, bad arguments, insufficient engine capability) returns a nonzero rc with an explanation on stderr — under `set -e` that aborts the script: **a scenario that cannot produce evidence must fail loudly, never degrade silently**.
- Exception: `drive9-test-check` verdicts are data, not operational errors — a failing assertion still returns 0 (the verdict was recorded); it returns nonzero only when the verdict could not be recorded (§4.6).
- Referencing an unimplemented effect / an effect unsupported by the sandbox: the run reports `blocked` as a whole (reason: effect not implemented), and MUST NOT skip silently or approximate.

### 4.4 Faults

#### Declaration

```sh
drive9-test-fault <name> <effect> <anchor…> [--hold D] [--expect C] [--delay D]
```

| Argument | Required | Meaning |
|---|---|---|
| `<name>` | ✓ | unique within the script; referenced by arm / disarm / `fault-fired`. MUST be a literal (§8 rule 3) |
| `<effect>` | ✓ | a value from the effect registry (§4.4.1), MUST be a literal |
| `--pattern P --count N` | one of three | wire anchor: the injector counts proxied requests by "method + path prefix" and triggers on the Nth match |
| `--phase mount` | one of three | phase anchor: hits the moment the mount needs the remote; MUST NOT be simulated by disconnecting an unrelated request |
| `--after D` | one of three | delay anchor: triggers D after arm |
| `--hold D` | ✗ | dwell duration; expires automatically (equivalent to an implicit disarm). A fault armed with neither hold nor disarm is a lint error |
| `--expect C` | ✓ | a result class (§4.4.3) — the shape of effects this fault promises to victims |
| `--delay D` | ✗ | `delay` effect only: per-connection added latency |

Declaration is a runtime action: `drive9-test-fault` registers with the engine when executed; the effect × sandbox capability check happens at that moment (§6). The declaration MUST precede its first arm.

**Ground-truth rule**: every armed fault MUST leave injector-side evidence of occurrence (armed record + per-request handling log); the `fault-fired` check adjudicates on this. Client-side "the network was down" logs MUST NOT count as trigger evidence.

#### 4.4.1 Effect registry

| effect | mechanism | host | firecracker | status |
|---|---|---|---|---|
| `blackhole` | suspend all matching connections inside the window (no response) | ✓ | ✓ | implemented |
| `reset` | immediately RST matching new connections | ✓ | ✓ | implemented |
| `drop-response` | request forwarded and executed upstream; success response swallowed and connection cut (one-shot) | ✓ | ✓ | implemented |
| `delay` | add N latency per connection | ✓ | ✓ | implemented |
| `http-limit` | throttle with HTTP 429/503 + Retry-After | ✓ | ✓ | implemented |
| `upload-mid-cut` | cut the upload body midway | ✓ | ✓ | not implemented (same) |
| `http-5xx` | rewrite the upstream response to 5xx | ✓ | ✓ | not implemented (same) |
| `kill-vm` / `pause-vm` | destroy / pause the microVM (drives host-side FcCtl) | ✗ | ✓ | implemented; firecracker only (§6) |

Wire effects require client→server traffic to pass through the simulator proxy (the engine establishes this by default in both sandboxes). If the chosen sandbox does not support an effect, the run MUST report an error listing "effect → required sandbox".

#### 4.4.2 arm / disarm / window

```sh
drive9-test-arm <name>
drive9-test-disarm <name>
drive9-test-window [--expect C] [--timeout D] -- <payload…>
```

- arm opens a window and primes the anchor; disarm releases explicitly; a fault may be armed multiple times.
- `drive9-test-window` places the payload in fault-window context: the payload's failure shape is adjudicated by result classes. The class used = the window's explicit `--expect`; if absent, the union of the `--expect` classes of all faults armed during the window. The adjudication produces an evidence row, not a case entry:

| Payload outcome | Window behavior |
|---|---|
| rc = 0 | record `ok`, window returns 0 |
| rc ≠ 0, shape tolerated by the result class | record `tolerated(<class>)`, window returns 0 (the script continues under `set -e`) |
| payload killed by the engine at `--timeout` | record `engine-terminated`; if the class declares that legal (e.g. `may-fail-inflight`) return 0, otherwise nonzero |
| rc ≠ 0 with an illegal shape, or no armed fault alive during the window | record `failed` (the latter records `blocked` evidence); the window MUST return nonzero |

- Outside windows, failures enjoy no result-class protection: an ordinary command's rc ≠ 0 aborts the script under `set -e`.
- Supplementary assertions required by a result class (e.g. errno comparison for `refuse-writes`) are performed by the script calling `drive9-test-check` inside the window (probe semantics: record any outcome without judging, via `--accept`, see §4.6).

#### 4.4.3 Result classes (closed vocabulary, 6 values)

Each class is a predicate table implemented in the adjudicator over the "failure / slowdown shapes observed in the window":

| value | predicate (legal shapes) |
|---|---|
| `may-fail-inflight` | failures allowed inside the window, but every failure MUST deliver an explicit error to the caller (rc / errno / stderr); a silent hang is killed by the engine at window timeout and recorded `engine-terminated` (a legal evidence form, labeled truthfully); zero tolerance for failures outside the window |
| `one-shot-ambiguous` | the anchored call may succeed or fail, not judged; only assert the original result was recorded verbatim and a later background completion must not rewrite it; adjudication deferred to the check layer via dual-entry comparison |
| `slower-not-broken` | any failure is illegal; only slowdown is allowed — payload rc = 0 with timing data collected |
| `refuse-writes` | the targeted operation must return exactly the expected refusal errno (the script's in-window assertion compares); probe assertions (record any outcome, never judge) are marked by the script |
| `workload-dies` | the payload being killed is the scenario; only assert a termination event exists at the right time; rc not judged |
| `partial-uncancelled-ok` | observed failures must be attributable to the exited sub-part; completion and content assertions for uncancelled entries apply as usual |

### 4.5 Observation and orchestration primitives

```sh
drive9-test-sample <name> [--as mount|observer|remote] [--optional]
drive9-test-drain [--mode require-ok|best-effort] [--timeout D]
drive9-test-async [--timeout D] -- <payload…>
drive9-test-wait [--expect ok|interrupted] [--timeout D]
drive9-test-kill <target> [--method kill|term]
drive9-test-remount [--cache same|fresh]
drive9-test-hold (--for D | --idle D)
```

| Primitive | Meaning |
|---|---|
| `sample` | collect snapshot `snapshot/<name>.manifest`; `name` unique within the script; `as = observer / remote` lazily starts that vantage on first use. MUST NOT sample from the mount vantage while an async payload is alive (stable-point sampling); a runtime violation is an operational failure |
| `drain` | remote sync confirmation. `require-ok` (treated as an operational failure if a successful verdict is not reached) / `best-effort` (result kept as-is, for "issued ≠ completed" scenarios). Artifact `sync-<n>.json` kept in full — MUST NOT be truncated to the last line or replaced by a plain `sync` |
| `async` | starts the payload in its own process group in the background; only ONE live async is allowed at a time (overlap is an operational error). `--timeout` expiry kills it and records `engine-terminated` |
| `wait` | waits for the most recent async payload. `--expect ok` (default): payload rc ≠ 0 returns nonzero (unexpected failure); `--expect interrupted`: rc not judged, only assert a `kill`/`term` interruption event occurred during the payload's lifetime (for orchestration-killed scenarios). An engine kill at wait timeout records `engine-terminated`, nonzero return |
| `kill` | `target = client` (the mount daemon) / `workload` (the live async payload's process group); `method = kill` (SIGKILL, sudden exit) / `term` (SIGTERM, cancellation semantics). `kill` is an orchestration action, not a fault: it produces no result class; its victim payload declares `wait --expect interrupted`. `vm` target reserved for the firecracker tier, unimplemented |
| `remount` | `cache = same` (default) / `fresh` (new environment without the original cache); `fresh` is one of the independent read entries. After remount the script's cwd and open fds are stale; SHOULD continue via `$D9_MOUNT` absolute references |
| `hold` | `--for D` wait wall-clock / `--idle D` wait for engine-observed quiet |

Sample evidence collection is engine-internal and tolerates transient vantage errors with bounded retries; a persistently unavailable vantage can be recorded instead of aborting via `--optional` — the snapshot is then recorded as missed and any check referencing it reports unconfirmed (visibility windows are not pre-agreed).

### 4.6 Assertions `drive9-test-check`

The **only** construct producing case entries. Plain in-script shell assertions (`cmp` / `test`) are control flow and are not counted; script-side comparisons that must be counted MUST go through the `shell` kind.

```sh
drive9-test-check <kind> [kind args…] [--req T] [--claim ID] [--accept S1,S2]
```

Common fields:

| Key | Required | Meaning |
|---|---|---|
| `--req` | ✓ | traceability anchor: identifies the external requirement clause this assertion corresponds to. Lexical: a non-empty compact token without whitespace or `[` `]` (e.g. `"S4-std-2"`, `"req-17"`). MUST be a literal (§8 rule 3); opaque to the engine, used only for equality and aggregation — feeds the coverage matrix. The value domain is supplied externally by the validate caller via a clause-mapping source (`--trace <file>`) and checked there (§8 rule 9) |
| `--claim` | ✗ | assertion semantic id; defaults to a derived `<kind>-<n>`. Key assertions SHOULD be written by hand |
| `--accept` | ✗ | accepted verdict set, values ⊆ `pass / fail / unsupported / unconfirmed`; default `["pass"]`. Typical: `--accept pass,unsupported` (a legal record of a capability boundary). Acceptance annotates only; it never changes a verdict or the aggregation |

Kind list (closed set, 9 kinds):

| kind | args | verdict |
|---|---|---|
| `shell` | `-- <cmd…>` | run the command: rc=0 → pass; rc=2 → unsupported (the payload declares a capability boundary); rc=3 → unconfirmed (the payload declares an unverifiable outcome, evidence attached); other rc → fail. Command output lands in evidence artifacts. The statistics entry point for script-side oracles |
| `manifest-equals` | `--this R --against R [--ignore G1,G2]` | two manifests equal (outside the ignore whitelist) |
| `manifest-contains` | `--this R --against R --paths P1,P2` | non-interference: the paths listed for `against` still exist in `this` with identical digests |
| `manifest-absent` | `--this R --paths P1,P2` | no revival: the listed paths do not exist in `this` |
| `sync-ok` | `[--drain N]` | the Nth (default: most recent) drain JSON reports success |
| `no-pending` | `[--drain N]` | the same drain JSON has no pending items |
| `fault-fired` | `--fault <name>` | injector ground truth proves the fault actually fired |
| `metrics` | `--metric M --budget B` | journal / telemetry aggregated by `metric` (e.g. `op.write-fsync.p95`) judged against the run's named budget (`--budget <name>=<threshold>`); without the budget → unconfirmed + data |
| `artifact-contains` | `--artifact A --pattern RE` | a named artifact (`mount.log` / `journal` / `sync-*.json` / `injector/proxy.jsonl`) matches |

- Reference syntax: `R` ∈ `@<snapshot name>` / `now` (walk the mount at evaluation time) / `control` (the control run's terminal state). Manifest checks MUST pass `--this` and `--against` explicitly.
- **Evaluation time = call time** (inline, in script order); a `@name` snapshot MUST be collected earlier in file order than the check referencing it (lint-enforced). "Before recovery" verdicts are expressed by placing the check before the recovery step. `now` requires the mount to be up; with the mount down (client killed and not remounted) the assertion records unconfirmed.
- Four verdict states: `pass / fail / unsupported / unconfirmed`; the engine additionally has `blocked` (evidence cannot be produced: injection not triggered, drain produced nothing, effect unimplemented, no armed fault alive during a window).
- Aggregation always uses the raw verdict (§5.2); `--accept` only annotates the capability boundary for reporting and the TAP TODO directive — it never changes a verdict or the aggregation.
- `drive9-test-check` NEVER returns nonzero because an assertion failed — verdicts are data; it returns nonzero only when the verdict could not be recorded (§4.3).

### 4.7 Logs

- All natural script stdout/stderr (`echo`, tool output) is log only: captured in full into the run directory `logs/`, never semantically parsed, never producing case entries. This language has no semantic log channel; per-entry evidence is carried by `drive9-test-check` (`shell` kind) or engine telemetry.
- Engine-collected artifacts live alongside but separate from script logs: `mount.log`, `journal`, `sync-*.json`, `injector/proxy.jsonl`, `snapshot/*.manifest`. The two-layer result principle stands: command-layer success ≠ file-operation-layer success — the `shell` kind's rc and the manifest / drain artifacts are different layers; either failing fails the run, and both layers' evidence is kept.
- When the engine itself kills a call at a timeout it records "did not return within the test deadline", never "drive9 returned a timeout".

## 5. Verdict model

### 5.1 Statistics unit

- The unit of pass/fail statistics is the case entry = one `drive9-test-check` assertion. Echo output, primitive calls and window adjudication do not produce statistics entries.
- The report layer aggregates case entries per script; `--repeat N` runs produce independent verdicts and evidence, reported per run, never merged.

### 5.2 Run verdict aggregation

Besides case entries, two run-level facts participate:

1. **Script terminal state**: `completed` (rc = 0) / `aborted` (rc ≠ 0) / `engine-terminated` (killed at timeout). A non-`completed` terminal state records fail-grade evidence with `{rc, start, end, dur_ms, stderr_tail}` attached.
2. **Evidence rows**: window adjudications and kill/term termination events (§1). A `failed` evidence row is fail-grade; `tolerated` / `engine-terminated` (declared legal by the result class) / `kill-interrupted` (declared legal by `--expect interrupted`) do not constitute failures but MUST be recorded.

The run verdict = the lowest state across all case entries and the facts above, ordered `fail(0) < blocked(1) < unconfirmed(2) < unsupported(3) < pass(4)`. A case file MUST contain at least one `drive9-test-check` (lint-enforced).

TAP normalization: each run produces `report.tap`, one line per case entry — pass → `ok`; fail → `not ok`; unsupported → `ok … # SKIP`; unconfirmed / accepted non-pass → `not ok … # TODO`. External `prove` and CI ecosystems consume it directly; internal adjudication remains four-state.

### 5.3 Capability boundary

- `blocked` beyond the four states means evidence could not be produced (injection not triggered, drain produced nothing, effect unimplemented, an armed window with no fault alive).
- A case referencing an unimplemented effect: parses fine, executes, and reports `blocked` as a whole (reason: effect unimplemented) — never silently skipped or approximated.

### 5.4 Control run

Any check referencing `control` enables it: the engine replays the **same script** against a plain local directory (no mount) with every `drive9-test-*` command switched to a stub implementation —

| command | stub behavior |
|---|---|
| `fault` / `arm` / `disarm` / `hold` / `drain` / `remount` / `kill` / `sample` | no-op |
| `window` | executes the payload directly, no result-class protection |
| `async` / `wait` | the payload runs synchronously to natural completion |
| `check` | no-op, produces no case entries |

The control run's artifacts live under `control/`; its terminal state is the value of the `control` reference; its checks and terminal state MUST NOT enter the real run's statistics or aggregation. The control run's results serve only as a manifest-comparison and attribution baseline (control fails the same way → application / environment, not a drive9 defect). The control run MUST NOT contain faults — in the stub environment fault injection does not exist by construction. Checks referencing `control` are deferred during the real run and resolved after the control replay.

## 6. Sandbox and capability validation

```text
drive9-simulator run <file.test> --sandbox host|firecracker [--server …] [--bin …]
    [--home …] [--repeat N] [--budget <name>=<threshold> …]
    [--timeout D] [--durability fsync|auto] [--overlay glob,…] [--write-cache-size-mb N]
    [--payload-dir <dir>]
drive9-simulator validate <file.test…> [--trace <file>]
```

| layer | host | firecracker |
|---|---|---|
| script execution / logs | host subprocess | VM agent, same semantics |
| wire effects (§4.4.1) | ✓ (client→server through a local proxy) | ✓ (VM through the host TAP→proxy) |
| orchestration primitives (kill / remount / hold / drain / sample) | ✓ (direct local process / directory control) | ✓ (same control channel) |
| `kill-vm` / `pause-vm` | ✗ | ✓ |

- Capability validation happens at runtime when `drive9-test-fault` declares: all effects referenced by the case ∈ the chosen sandbox's capability set; otherwise it MUST error listing "effect → required sandbox" and the run reports `blocked` as a whole.
- `validate` static scanning (§8) is sandbox-independent; the static effect × sandbox pre-check applies only to literal effects.

## 7. Command quick reference

```text
drive9-test-fault   <name> <effect> (--pattern P --count N | --phase mount | --after D)
                    [--hold D] [--expect C] [--delay D]
drive9-test-arm     <name>            drive9-test-disarm <name>
drive9-test-window  [--expect C] [--timeout D] -- <payload…>
drive9-test-async   [--timeout D] -- <payload…>
drive9-test-wait    [--expect ok|interrupted] [--timeout D]
drive9-test-kill    <client|workload> [--method kill|term]
drive9-test-remount [--cache same|fresh]
drive9-test-hold    (--for D | --idle D)
drive9-test-sample  <name> [--as mount|observer|remote] [--optional]
drive9-test-drain   [--mode require-ok|best-effort] [--timeout D]
drive9-test-check   <kind> [kind args…] --req T [--claim ID] [--accept S1,S2]
                    kind: shell | manifest-equals | manifest-contains | manifest-absent
                        | sync-ok | no-pending | fault-fired | metrics | artifact-contains

run CLI  --timeout  --durability  --overlay  --write-cache-size-mb  --payload-dir   (environment knobs, never in-script)
env      D9_MOUNT  D9_CONTROL  PAYLOAD_DIR (§4.2, optional custom workload file set)
```

(`vm` kill target = reserved, unimplemented.)

## 8. Validation rules (lint, implemented by `drive9-simulator validate`)

1. `.test` suffix; POSIX sh (`sh -n`); case name = stem and unique; `set -e` enabled before the first non-comment command; no directive headers in the script (environment configuration arrives only via run CLI flags).
2. Reserved prefix: the script must not define / shadow `drive9-test-*`; must not call a `drive9-test-*` name outside the registry.
3. **Literal discipline**: fault names / effects in `drive9-test-fault`, `drive9-test-sample` names, `--req` tokens and `@name` references MUST be literal words (never built from variables, command substitution or string concatenation) — this is the precondition for static reference checking and the req coverage matrix.
4. Every fault name referenced by `drive9-test-arm` / `drive9-test-disarm` / `fault-fired` must exist; every fault is armed at least once, and is covered by a disarm or `--hold` expiry (neither disarm nor hold is an error).
5. A `@name` referenced snapshot must exist with its sample call earlier in file order than the referencing check; manifest checks must pass `--this` and `--against` explicitly.
6. Every `drive9-test-check` must carry `--req` matching the anchor lexicon (§4.6); `--accept` ⊆ the four states.
7. Every case file contains at least one `drive9-test-check`; the orchestration allows only one live async (statically: a second `async` must be preceded by `wait` or `kill`).
8. Effect values ∈ the registry (statically, for literal effects); window / result-class vocabulary legal.
9. When validate is given a clause-mapping source (`--trace <file>`): the union of the case's `--req` tokens must be covered by it (anchor drift check); skipped otherwise.

## Appendix — syntax sketch

```abnf
case-file     = shebang? "set -e" … script-body
script-body   = 1*command                       ; POSIX sh; execution order is the timeline
test-command  = "drive9-test-" name *arg ["--" 1*payload]
snapshot-ref  = "@" name / "now" / "control"
```

All sources of verdicts: `drive9-test-check` (case entries) + script terminal state + evidence rows. Beyond these, script output and control flow carry no semantics.
