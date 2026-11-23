# drive9-simulator Case Specification

- Version: `v1` (single authoritative specification of the `.t` case language and runner contract).
- Key words **MUST / MUST NOT / SHOULD / MAY** follow RFC 2119.

## 0. Overview

A case is a POSIX shell script (a `.t` file). Execution order is the timeline: workload actions run as ordinary shell commands, while faults, observation, preconditions, and verdicts are carried by bare case commands (`fault`, `arm`, `require`, `check`, …) injected at the front of `PATH` alongside portable cross-platform helper tools (`fsync`, `d9-stat`, `d9-http`, `fstest`).

| Concern | Construct |
|---|---|
| Workload | Plain POSIX shell commands in the script body |
| Precondition | `require [--claim ID] [--req T] -- <cmd…>` (cleanly halts & records `unsupported` when unmet) |
| Fault injection | `fault` declaration + `arm` / `disarm` / `window` |
| Observation | `sample` / `drain` + `@name` / `now` / `control` snapshot references |
| Oracle | `check` assertions (produces TAP case entries) |

Natural script `stdout` / `stderr` (`echo`, tool output) is captured in `logs/script.out` as diagnostic evidence only; verdicts come strictly from `check` / `require` entries, window/await evidence rows, and the script's terminal exit status.

## 1. File Layout & Naming

```text
tests/
  simulator/          # drive9-simulator Rust crate + specs
  cases/
    suites.toml       # Checked-in tier manifest enrolling all *.t cases
    webdev/           # End-user / coding-agent scenarios & fault recovery
    drive9/           # Product surface cases (`api/`, `cli/`, `fuse/`)
    community/        # fstest-backed POSIX, durability, concurrency, and perf suites
    custom/           # Customer benchmark workloads
    juicefs/          # Deterministic POSIX/stress workloads
    git/              # Upstream Git test suites via fstest
```

- Every case file MUST end in `.t`, pass `sh -n`, and enable `set -e` before its first non-comment command (`set -eu` recommended).
- Every case stem (`<stem>.t`) MUST be globally unique across `tests/cases/**`.
- Helper files colocated with cases MUST NOT end in `.t` and SHOULD be referenced via `$(dirname "$0")`.

## 2. Execution Environment

Unless `--no-mount` is set, the engine mounts `drive9` on `$MOUNT` before starting the script; if the initial FUSE mount fails, the run aborts immediately as `blocked` without executing the script against an unmounted directory.

| Item | Value |
|---|---|
| `cwd` | `$MOUNT` (the mounted FUSE workspace, or a plain directory under `--no-mount`) |
| `MOUNT` | Absolute path of the workspace root. Re-enter via `cd "$MOUNT"` after `halt client` / `remount` |
| `RUN_DIR` | Scratch/artifact directory outside the mount (isolated as `control_scratch` during control replay). Use `$RUN_DIR` instead of shared `/tmp` paths |
| `HOME` | Isolated per-run home directory so ambient `~/.drive9/config` never hijacks credentials |
| `PATH` | Prepended with all case builtins (`§3`), `fstest`, and portable tools (`fsync`, `d9-stat`, `d9-http`) |
| `SERVER` | Backend HTTP URL (points through the fault-injection proxy whenever the case declares `fault`s; omitted during control replay) |
| `API_KEY` | Tenant Bearer API key (omitted during control replay) |
| `TENANT` | Active tenant ID |
| `EXISTING_TENANT` | Set to `1` when `--api-key` / `DRIVE9_API_KEY` reuses a pre-provisioned tenant |
| `CLI` | Absolute path to the `drive9` binary under test |
| `SIMULATOR_BIN` | Absolute path to `drive9-simulator` |
| `PAYLOAD_DIR` | Optional read-only external workload tree (`--payload-dir`, exported to both live and control runs) |
| `CONTROL` | Unix domain socket for engine control ops (internal; scripts MUST NOT access directly) |

### 2.1 Built-in Portable Tools on `PATH`

To keep `.t` cases portable across Linux and macOS without GNU coreutils or external `curl`:

- `fsync [-s|--syncfs] <path...>`: invokes `fsync(2)` (or `syncfs`) on files or directories.
- `d9-stat <subcmd> [-P] <path> [arg]`:
  - `size`, `mode` (octal, e.g. `644`), `mtime` (epoch secs), `set-mtime <path> <secs>`, `nlink`, `ino`, `type` (`file|dir|symlink|fifo|sock|char|block`), `sha256`
  - `xattr-get <path> <name>`, `xattr-set <path> <name> <val>`, `xattr-rm <path> <name>` (exits `2` on `ENOTSUP`/`EOPNOTSUPP`)
- `d9-http [-X METHOD] [-H 'K: V'] [-d DATA | --data-file PATH] [-o OUT] [--timeout SECS] [--status-only] [--expect CODES] <URL_OR_PATH>`:
  - Built-in HTTP/1.1 + TLS client. Relative `/v1/...` paths automatically prepend `$SERVER` and attach `Authorization: Bearer $API_KEY` unless overridden.

### 2.2 Payloads (`--payload-dir` and `--seed`)

- **`--payload-dir <dir>`**: exports read-only `PAYLOAD_DIR` to both the live run and control replay. Scripts MUST copy from `$PAYLOAD_DIR` and never mutate it.
- **`--seed <dir>`**: bystander survival tree copied onto `$MOUNT/__payload__/` (and `control/__payload__/`) before script execution, drained, and verified from an independent `remote` vantage at run end. Scripts MUST NOT modify `__payload__/`.

Duration literals use `<number><unit>` where `<unit>` is `ms | s | m | h` (e.g. `200ms`, `8s`, `15m`).

## 3. Reserved Case Commands

The 13 case commands are reserved and MUST NOT be shadowed by shell functions, aliases, or variable assignments:
`fault`, `arm`, `disarm`, `window`, `async`, `await`, `halt`, `remount`, `hold`, `sample`, `drain`, `check`, `require`.
(`await` and `halt` avoid colliding with POSIX shell builtins `wait` and `kill`.)

### 3.1 Preconditions: `require`

```sh
require [--claim ID] [--req T] -- <cmd…>
```

Executes `<cmd…>`. If `rc == 0`, returns `0`. If `rc != 0`, records an `unsupported` (`# SKIP`) case entry and exits `200`, cleanly halting the script under `set -e` so the entire case finishes as `unsupported` (`exit 4`) rather than `aborted`.

### 3.2 Fault Injection: `fault`, `arm`, `disarm`, `window`

```sh
fault <name> <effect> (--pattern P [--count N] | --phase mount | --after D) \
    [--hold D] [--expect C] [--delay D]
arm <name>
disarm <name>
window [--expect C] [--timeout D] -- <payload…>
```

- **Anchors** (specify exactly one):
  - `--pattern P [--count N]`: matches HTTP requests through the proxy (`N` defaults to `1`). `P` may be `"<METHOD> <PATH>"` (e.g. `'PUT /v1/fs/'`, `'PUT|POST /v1/uploads/*'`), `"* <PATH>"`, or a path prefix/glob `"/s3/*"`. Note: the built-in server routes presigned V1/V2/extent `/s3/*` and `/v1/uploads/*` URLs through the proxy `Host` automatically, so wire faults cover both metadata and presigned data-plane traffic.
  - `--phase mount`: fires only while `Runner::mount` is actively mounting/remounting.
  - `--after D`: fires `D` after `arm <name>`.
- **Lifetime**: every armed fault MUST be bounded by `--hold D` or an explicit `disarm <name>` (enforced by `validate`).
- **Effects**:
  - `blackhole`: holds matching connections open without responding while active.
  - `reset`: sends an immediate `TCP RST` (`SO_LINGER=0` -> `ECONNRESET`).
  - `drop-response`: forwards request + body to upstream, consumes upstream response, then sends `TCP RST` to client (one-shot).
  - `delay`: adds `--delay D` latency before forwarding.
  - `http-limit`: returns `429 Too Many Requests` (`Retry-After: 1`).
  - `http-5xx`: returns `503 Service Unavailable` (`Retry-After: 1`).
  - `upload-mid-cut`: accepts headers (and `100 Continue` if requested), reads half the upload body, forwards the truncated prefix, and sends `TCP RST` (one-shot unless `--hold` is set).
  - `kill-vm` / `pause-vm`: requires `--sandbox firecracker` (reports `blocked` on `host`).
- **Result classes (`--expect C`)**:
  `may-fail-inflight` | `one-shot-ambiguous` | `slower-not-broken` | `refuse-writes` | `workload-dies` | `partial-uncancelled-ok`.

### 3.3 Orchestration & Observation

```sh
sample <name> [--as mount|observer|remote] [--optional]
drain [--mode require-ok|record] [--timeout D]
async [--timeout D] -- <payload…>
await [--expect ok|interrupted] [--timeout D]
halt <client|workload> [--method kill|term]
remount [--cache same|fresh]
hold (--for D | --idle D)
```

- `sample`: writes `snapshot/<name>.manifest`. Sampling `--as mount` while an `async` workload is live is an operational error. `--optional` records `sample-missed` on persistent vantage failure instead of aborting (referencing checks become `unconfirmed`).
- `drain`: invokes `drive9 mount drain --json` and saves `sync-<n>.json`. `--mode require-ok` (default) retries once on transient race and aborts if still unsuccessful; `--mode record` records the outcome without aborting so subsequent `check sync-ok` / `check no-pending` can assert on it.
- `async` / `await` / `halt`: the runner owns the background process group directly and reaps it via `Condvar` (no `.rc` polling delay). `halt client --method kill` sends `SIGKILL` and force-unmounts the dead FUSE mount; `halt client --method term` triggers graceful `umount`/`SIGTERM`.
- `remount`: `--cache same` reuses the current cache generation; `--cache fresh` allocates a brand-new `cache-<gen>` and `local-<gen>` directory to prove data durability from server state rather than warm local cache.

### 3.4 Assertions: `check`

```sh
check <kind> [kind args…] [--claim ID] [--req T] [--accept S1,S2]
```

`check` records a TAP entry and **always exits `0`** when the entry is recorded (even when the assertion verdict is `fail`). Scripts MUST NOT branch on `check` (`if check ...`, `! check ...`, `check ... ||` are rejected by `validate`).

- `--claim ID`: human-readable assertion identifier in `report.tap` and `checks.json`.
- `--req T`: optional requirement traceability token (compact literal without spaces or `[]`). Enforced against `--trace <file>` when `drive9-simulator validate --trace <file>` is used.
- `--accept S1,S2`: comma-separated subset of `pass,fail,unsupported,unconfirmed,blocked` (default `pass`).

| Kind | Required / Optional Args | Semantics |
|---|---|---|
| `shell` | `-- <cmd…>` | Runs `<cmd…>`: `rc=0` → `pass`; `rc=2` → `unsupported`; `rc=3` → `unconfirmed`; other `rc` → `fail` |
| `manifest-equals` | `--this R --against R [--ignore G1,G2]` | Full tree equality (type, size, mode, sha256) modulo `--ignore` globs |
| `manifest-contains` | `--this R --against R --paths P1,P2` | Every non-dir entry matching `--paths` in `against` exists in `this` with identical `sha256` |
| `manifest-absent` | `--this R --paths P1,P2` | No entry in `this` matches any prefix/glob in `--paths` |
| `sync-ok` | `[--drain N]` | `sync-<N>.json` (default: latest drain) succeeded |
| `no-pending` | `[--drain N]` | `sync-<N>.json` (default: latest drain) reported zero pending items |
| `fault-fired` | `--fault <name>` | Proxy ground truth confirms `<name>` fired (`blocked` if never triggered) |
| `metrics` | `--metric M [--budget B]` | Evaluates metric `M` (from `fstest`, proxy percentiles `proxy.req.p95_ms`, or FUSE `--perf` `perf.jsonl` `fuse.<op>.p95_ms` / `op.write-fsync.p95`) against `--budget B` (`<=`, `=`, `>=`); `unconfirmed` when no budget is supplied |
| `artifact-contains` | `--artifact A --pattern RE` | Regex `RE` matches `journal` (`journal.jsonl`), `script.out` (`logs/script.out`), `mount.log` (`mount-*.log`), `injector/proxy.jsonl`, `injector/armed.jsonl`, or `sync-*.json` |
| `fstest` | `--suite S [--args '…']` | Runs built-in `fstest <S> <args…> --json <artifact> $MOUNT`, imports `metrics`, and expands `cases[]` (`schemaVersion: 1`) into TAP entries |

Snapshot references `R` are `@<name>` (sampled earlier in file order), `now`, or `control`.

## 4. Verdict Model & Suite Manifest

### 4.1 Five-State Verdict & `--accept` Aggregation

States are ordered by severity: `fail` (exit `1`) < `blocked` (exit `3`) < `unconfirmed` (exit `2`) < `unsupported` (exit `4`) < `pass` (exit `0`).

1. Any **unaccepted** entry (`raw_state` not in its `--accept` set) fails the case with the worst unaccepted state.
2. Unhandled script errors (`terminal != "completed"`), failed window/await evidence rows, or mount/proxy setup blocks override with `fail` / `blocked`.
3. When every entry is accepted:
   - If **all** entries in the case are `unsupported` (e.g. `require` skipped or an entire capability-gated case returned `rc=2` with `--accept pass,unsupported`), the run verdict is **`unsupported` (`exit 4`)**, reported as `ok <case> # SKIP` by `run-suite`.
   - If at least one entry is `pass` (or an explicitly accepted non-`unsupported` state) and remaining entries are accepted `unsupported` (such as individual `TCONF` sub-skips inside `check fstest`), the run verdict is **`pass` (`exit 0`)**.

### 4.2 Control Replay (`control`)

Referenced lazily when any check uses `--this control` or `--against control`. Replays the same `.t` script in a clean local directory (`STUB=1`, isolated `HOME` and `RUN_DIR=control_scratch`, `SERVER`/`API_KEY` unset, `window` and `async` synchronous, orchestration commands no-op) within the case timeout, and fails if the control script exits non-zero.

### 4.3 `suites.toml` Schema

`drive9-simulator run-suite [suites.toml] [--tier <tier>] [--jobs <N>] [--fail-on-all-skip]` reads `[[case]]` entries (`--tier pr` selects `pr-no-mount` + `pr-fuse`; `--tier` also accepts comma-separated tiers):

```toml
[[case]]
name = "webdev/save-read"
tier = "pr-fuse"              # pr-no-mount | pr-fuse | daily-fault | daily-fuse | daily-bench | daily-prod-ext | daily-sdk
timeout = "5m"
durability = "fsync"          # fsync | auto | interactive | close-sync | write-sync
profile = "coding-agent"      # coding-agent | portable | none
perf = false
no_mount = false
serial = false                # run sequentially after the parallel wave when --jobs > 1
allow_other = false
overlay = "node_modules/**,.venv/**"
write_cache_size_mb = 256
budgets = ["fio.read.iops>=100"]
env = { FILES = "50" }
```

## 5. Static Validation (`drive9-simulator validate`)

1. Syntax check via `sh -n`, `set -e` header gate, and globally unique `.t` stems.
2. Heredoc bodies (`<<EOF ... EOF`) are stripped before clause lexing so embedded Python/Node/SQL/JSON payloads never trigger false positives.
3. Reserved vocabulary shadowing (`check() { ... }`, `sample=...`) and branching on `check` (`if check ...`, `! check ...`, `check ... ||`) are rejected.
4. Every `fault` name is unique, literal, uses a valid effect/anchor/duration/class, is armed at least once, and is closed by `disarm` or `--hold`.
5. Every `@name` snapshot is sampled earlier in file order than any `check` referencing it; `async` blocks cannot overlap without `await` or `halt workload`.
6. Every case contains at least one `check` or `require`; when `--trace <file>` is supplied, every case's `--req` tags must appear in the trace set.
