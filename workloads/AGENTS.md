---
title: workloads - drive9 vs EFS vs EBS benchmark harness for AI agents
---

# What this directory is

This directory contains every script used on the reference AWS EC2 host to compare the drive9 FUSE mount against Amazon EFS and Amazon EBS: the case implementations, the serial run coordinator, the summarizer, the parameter-tuning sweeps and the profiling/diagnostic probes. It is self-contained: an agent that can reach an equivalent host can rebuild the whole comparison, including the numbers published in the Feishu report.

The reference run produced a single-sample matrix (per user direction, one measured round instead of twelve) across six groups.

| Group | What it is |
|---|---|
| `none-a` | drive9 mount, `--profile none --durability fsync` (writeback + strict fsync) |
| `none-b` | drive9 mount, `--profile none --durability interactive` (writeback + interactive fsync) |
| `coding-a` | drive9 mount, `--profile coding-agent --durability fsync` (local overlay for `.git/`, `node_modules/`) |
| `coding-b` | drive9 mount, `--profile coding-agent --durability interactive` |
| `efs` | Amazon EFS (NFSv4.1) mounted at `$DRIVE9_BENCH_MOUNT_ROOT/efs` |
| `ebs` | Amazon EBS gp3 volume (ext4) mounted at `$DRIVE9_BENCH_MOUNT_ROOT/ebs` |

# Layout

```
workloads/
  AGENTS.md              this file
  mount_groups.py        create the four isolated drive9 mounts (one tenant each)
  main/                  10-case baseline matrix (rename, copy, concurrent-files, ...)
    cases.py             case implementations + arg parsing
    run_matrix.py        serial coordinator: smoke -> warmup -> measured, drains, verification
    summarize.py         validate a completed run and emit summary.json / measured-rounds.csv
    atomic_probe.py      untimed concurrent-reader atomic-replace proof
    replay.py            packaging helper used for the recorded 2026-09-21 replay
  extended/              14 extended + operation-level cases
    cases.py             unzip-15k, git-clone-depth1, git-status, overlay-install,
                         bare-writes, op-write/open-create/flush/close/fsync/
                         unlink-pending/unlink-settled/rename-pending/utimes
    run_matrix.py        same coordinator (supports `full <case> <case> ...` subsets)
    summarize.py         subset-aware summarizer
    atomic_probe.py      atomic proof for the extended run
    build_fixtures.py    build the 300/15000-file zip and the local bare git repos
  tuning/                parameter sweeps (durability presets, batch window, upload concurrency)
    tune_sweep.py        restart the none-a mount with different flags, measure 300-file unzip
    tune_sweep2.py       write-sync / close-sync follow-ups
    tune_sweep3.py       flush-debounce follow-ups
    tune_alt.py          content-layout experiments (extent, append-log) - not pursued
    tune_alt2.py         further content-layout follow-ups - not pursued
    tune_batch.py        writeback-batch-window sweep
    tune_batch2.py       batch window + drain measurements
    tune_uc.py           --upload-concurrency sweep on parallel writes
    tune_uc_unzip.py     --upload-concurrency on the serial unzip case (expected: no help)
    batch_bench.py       parallel write bench, no drain
    batch_bench2.py      parallel write + drain end to end
    conc_decompose.py    per-op decomposition of the concurrent-files pattern
    disk_probe.py        local disk utilisation sampling during the same pattern
    op_bench.py          standalone per-op micro benchmark (argv-driven)
    probe_ws.py          write/close/utime latencies under write-sync
  diagnostics/           profiling and root-cause probes (used for issues #954 / #959)
    launch.sh            launch the profiling diagnostic mount (perf dir + pprof)
    diag_probe.py        mount-counter probes: flush stages, stat cache, utime curve, unlink
    local_fsync_control.py  same-disk fsync control + interleaved unlink probe
    flush_attrib.sh      CPU profile + strace summary during a flush workload
    strace_clean.sh      clean-window strace of the mount during 100 small-file flushes
    one_file_trace.sh    strace -y trace of three single-file flushes (per-file fsync sequence)
    parse_strace.py      aggregate a strace -T file by syscall
    parse_fsync_seq.py   resolve fsyncs to paths (first pass)
    parse_fsync_seq2.py  resolve fsyncs to paths and group them into per-file cycles
    server_op_probe.py   HTTP-level per-operation baseline (PUT/stat/rename/delete/mkdir/list)
  sixway-20260911/       the 2026-09-11 reference matrix (12 rounds) - the "previous round" baseline
    cases.py             same 10 baseline cases as main/, earlier generation
    mount_groups.py      four drive9 mounts for that run
    run_matrix.py        serial coordinator (no drain-verification rewrite of the newer harness)
    retry_ls_stat.py     ls-stat retry/profiling helper used after the first run
    test_cases.py        quick self-test for the case implementations
  sqlite/                SQLite WAL / commit / checkpoint / multi-connection cases
    wal4m_scale.py       4 MiB WAL scale driver (50 MiB database, commit + checkpoint phases)
    run_case.sh          orchestrates the WAL cases against a mount and asserts results
    case_assertions.py   assertions applied to the WAL run transcripts
    authority_http.py    REST helper; reads DRIVE9_SERVER and DRIVE9_API_KEY
    large_checkpoint.py  large-checkpoint timing/validation case
    sqlite_wal_single_txn.py       single-transaction WAL case
    sqlite_delete_single_txn.py    single-transaction delete case
    run_issue896_sqlite_once.sh    one-shot EC2 runner (issue 896)
    run_ec2_once.sh                one-shot EC2 runner (generic)
    drive9_same_fd_growth.py       same-FD growth case
    run_drive9_lineage_smoke.sh    lineage smoke runner
    sqlite-multiconn-harness.py    port of SQLite's multi-connection WAL test cases
  acceptance/            site-acceptance scenario suite run on the same host
    run_all.py           driver for s01..s12
    s01..s12             import/save/watch/assets/conflict/npm/refactor/git/build/export/external/recovery
    a_fsync_interrupt.py b_mount_retry.py c_response_lost.py d_sync_states.py e_attributes.py f_cache_cap.py
    repro_eio*.py repro_replace_visibility.py   failure reproductions
    common.py fixtures_gen.py read_verify.py scan_tree.py summarize.py build_tool.py
  legacy/                earlier experiments kept verbatim for provenance
    d9work/              baseline.sh, cmp.sh, cmp-new.sh, phenom.sh, runner-v2.sh, mount/upload-907.sh
    pr911/               PR911 cold/layer/live workloads + anonymous compare/merge-base helpers
    atomic-counterfactual/  atomic-replace counterfactual harness
```

# Environment prerequisites

1. Linux host with sudo (the reference host is an EC2 `t3.xlarge` in `us-east-1d`).
2. A drive9 CLI built from the commit under test (`make build-cli` then copy `bin/drive9` to the host).
3. Four drive9 tenants (owner API keys) - the reference setup used four separate tenants so that groups cannot interfere.
4. An EFS file system mounted at `$DRIVE9_BENCH_MOUNT_ROOT/efs` and a gp3 EBS volume mounted at `$DRIVE9_BENCH_MOUNT_ROOT/ebs`.
5. `python3`, `git`, `unzip`; plus `strace` and a Go toolchain for the diagnostics (pprof parsing).
6. `tmux` on the host: SSH sessions to the reference host are closed after roughly five minutes, so every long run must live in tmux.

## Environment variables

Set service addresses for your environment before running. Published endpoint defaults use example.invalid; local path defaults retain the reference Linux layout. Tenant IDs and API keys come from external credential files.

| Variable | Default | Used by |
|---|---|---|
| `DRIVE9_BENCH_BIN` | `/home/ubuntu/drive9-main-fe9cdcf/drive9` | mount_groups, run_matrix, cases (extended), tuning, diagnostics |
| `DRIVE9_BENCH_SERVER` | `https://drive9.example.invalid` | mount_groups, run_matrix |
| `DRIVE9_BENCH_CRED_DIR` | `/home/ubuntu/drive9-sixway-20260911/credentials` | all scripts that need an API key |
| `DRIVE9_BENCH_LOCK` | `/home/ubuntu/drive9-sixway-20260911/coordinator.lock` | run_matrix, atomic_probe (flock guard) |
| `DRIVE9_BENCH_MOUNT_ROOT` | `/mnt` | mount_groups, run_matrix |
| `DRIVE9_BENCH_REMOTE_ROOT` | `/benchmark` | mount_groups, run_matrix, atomic_probe, diagnostics |
| `DRIVE9_BENCH_RUN_PREFIX` | `replay-20260921-main` / `...-ext` | run_matrix remote verification path |
| `DRIVE9_BENCH_FIXTURES` | `/home/ubuntu/d9work/ext-fixtures` | extended/cases.py, build_fixtures.py |
| `DRIVE9_BENCH_RUN_DIR` | `/home/ubuntu/drive9-replays/replay-20260921-ext` | tuning/tune_sweep.py (result output) |
| `DRIVE9_BENCH_MOUNT` | `/mnt/d9-six-none-a` | tuning (single-group experiments) |
| `DRIVE9_BENCH_CACHE` | `.../replay-20260921-main/cache/none-a` | tuning/tune_sweep.py |
| `DRIVE9_BENCH_GROUP` | `none-a` | diagnostics shell scripts (which credential to use) |
| `DRIVE9_DIAG_DIR` | `/home/ubuntu/d9diag` | diagnostics |
| `DRIVE9_DIAG_MOUNT` | `/mnt/d9-diag` | diagnostics |
| `DRIVE9_DIAG_PERF` | `/home/ubuntu/d9diag/perf/perf.jsonl` | diagnostics/diag_probe.py |
| `DRIVE9_DIAG_PPROF_ADDR` | `127.0.0.1:16111` | diagnostics/launch.sh, flush_attrib.sh |
| `DRIVE9_BENCH_HOST` | `drive9.example.invalid` | diagnostics/server_op_probe.py |

## Credentials schema

`$DRIVE9_BENCH_CRED_DIR` must contain one JSON file per drive9 group (`none-a.json`, `none-b.json`, `coding-a.json`, `coding-b.json`):

```json
{"server": "https://drive9.example.invalid", "tenant_id": "<space uuid>", "api_key": "<owner key>"}
```

Never commit these files. `summarize.py` scans every produced artifact for `Bearer <token>` / GitHub token patterns and fails the run if one leaks into the results.

# Case catalog

Scale convention: `smoke` runs use `--scale 0.02` (fast correctness gate), `full`/`warmup`/`measured` use `--scale 1`. Every case is executed by one process against one group; the coordinator never overlaps two cases.

## main/ - 10 baseline cases (all use 4096-byte payloads unless noted)

| Case | Operations (timed phases) | Full scale | Correctness checks |
|---|---|---|---|
| `rename` | 100 same-directory renames, then 100 cross-directory renames (`same_directory`, `cross_directory`) | 100 + 100 | contents intact, source empty, remote sample verified |
| `atomic-replace` | write 4 KiB staged file + fsync, then `os.replace` over the target (`write_fsync_close`, `replace`) | 100 iterations | target equals last generation, exactly one file left |
| `ls-stat-consistency` | create 200 files with fsync, `os.listdir`, then `os.stat` each (`create_fsync`, `list`, `stat`) | 200 files | listing matches, sizes/modes correct |
| `copy-file` | copy 1 KiB / 64 KiB / 1 MiB files (`copy_1024_bytes`, `copy_65536_bytes`, `copy_1048576_bytes`) | 3 files | source == target |
| `copy-tree` | copy a 3-dir x 4-file tree (1 KiB, 4 KiB, 64 KiB, 1 MiB) | 12 files | tree contents equal |
| `overwrite-delete-recreate` | on one path: overwrite+fsync, unlink, recreate+fsync (`overwrite_fsync`, `delete`, `recreate_fsync`) | 100 iterations | final content matches |
| `many-small-files` | create 500 files with fsync, read all, list, delete all (`create_fsync`, `read`, `list`, `delete`) | 500 files | contents match, directory empty at the end |
| `medium-file` | write 16 MiB in 1 MiB chunks + flush/fsync/close, then read back (`write_16MiB`, `flush_fsync_close`, `read_16MiB`) | 1 file | bytes equal |
| `concurrent-files` | 8 processes x 50 files, each write + fsync + read + unlink; wall clock across workers (`eight_workers_wall`) | 400 files | no leftovers, remote absence verified |
| `permissions` | `chmod` file 0644->0600, `chmod` dir 0755->0700, then stat (`chmod_file`, `chmod_directory`, `stat`) | 2 nodes | modes applied |

## extended/ - 14 extended and operation-level cases

| Case | Operations | Smoke / full scale | Notes |
|---|---|---|---|
| `unzip-15k` | `unzip` a zip of 212-byte files into the mount | 300 / 15000 files | The 15k full run on drive9 takes ~40 minutes; iterate with the 300-file smoke and extrapolate x50 |
| `git-clone-depth1` | `git clone --depth=1 --no-local file://<local bare repo>` into the mount | 52 / 1191 files | Full fixture is the drive9 repo itself; `.git/**` goes to the coding-agent local overlay |
| `git-status` | `git status` inside a freshly cloned worktree | 1191 files | Dominated by one remote stat per file |
| `overlay-install` | 100 package dirs x 20 js files into `node_modules` + `package.json` | 2000 files | coding-agent groups route `node_modules` to the local overlay (remote returns 404 by design) |
| `bare-writes` | 2000 x 212-byte `open`/`write`/`close`, no fsync and no mtime | 2000 files | Isolates the local staging cost of a close |
| `op-write` | 100 x `write(2)` on a held-open fd, 4 KiB | 100 iterations | `op_stats` = avg/p50/p95/max over the 100 calls |
| `op-open-create` | 100 x `open(O_CREAT)` | 100 | |
| `op-flush` | 100 x `flush(2)` via `dup` + close of the duplicate | 100 | |
| `op-close` | 100 x final `close(2)` (flush + release) | 100 | |
| `op-fsync` | 100 x `fsync(2)` | 100 | Preset-dependent: strict ~118 ms vs interactive ~12-16 ms |
| `op-unlink-pending` | 100 x `unlink(2)` immediately after write+close (commit still pending) | 100 | |
| `op-unlink-settled` | 100 x `unlink(2)` after a drain (nothing pending) | 100 | |
| `op-rename-pending` | 100 x `rename(2)` of a just-written file | 100 | |
| `op-utimes` | 100 x `utimensat(2)` right after close | 100 | Root cause of issue #954 |

The extended cases write a `sample` witness (path, size, sha256) and the coordinator re-reads it through the HTTP API to prove the data really landed remotely; `absent` witnesses prove deletions.

## SQLite cases (`sqlite/`)

These predate the EFS/EBS matrix and answer a different question: how SQLite behaves on the mount (WAL append, commit latency, checkpoint, multi-connection locking). They are the source of the "SQLite WAL 1000 commits: drive9 67 s vs EFS 63 s" line in the report, and of the issue-896 / PR901 WAL investigations.

Conventions:

1. `wal4m_scale.py` and `large_checkpoint.py` are self-contained drivers: they create a database on the target path, run commit/checkpoint phases, and print per-phase timings plus integrity results.
2. `run_case.sh`, `run_ec2_once.sh`, `run_issue896_sqlite_once.sh` and `run_drive9_lineage_smoke.sh` are orchestrators. They read the owner key from `DRIVE9_API_KEY_FILE` (a mode-0600 file) or `DRIVE9_API_KEY`, and take the mount path / workspace as arguments - read the header of each script before running.
3. `authority_http.py` talks to the drive9 REST API for verification and only needs `DRIVE9_SERVER` and `DRIVE9_API_KEY`.
4. `sqlite-multiconn-harness.py` runs the ported SQLite multi-connection cases twice - once on an ext4 control path and once on the mount - and diffs the transcripts; deterministic cases must match exactly, stress cases compare invariants only.
5. Run these cases with the same discipline as the matrix: one case at a time, drain afterwards, keep failed transcripts.

Concrete way to reproduce the "SQLite WAL 1000 commits" case (verified against the collected copy on the drive9 mount - `prepare` produced an 8.5 MiB WAL database with `quick_check=ok`, `run` reached `wal_frames=1000` and completed with `live_verification.ok=true`, `sequence_exact=true`, `state_exact=true`, final TRUNCATE checkpoint 13.4 s):

```bash
DB=/mnt/d9-six-none-a/sqlite-verify/wal4m.db     # must live on the filesystem under test
mkdir -p "$(dirname "$DB")"

python3 workloads/sqlite/wal4m_scale.py prepare \
  --db "$DB" --target-bytes 8388608 --output /tmp/wal-prepare.json

python3 workloads/sqlite/wal4m_scale.py run \
  --db "$DB" --label wal1000 --output-dir /tmp/wal-run     # output dir must not exist yet

# optional independent re-check (needs --expected-filler-seq from /tmp/wal-run/result.json)
python3 workloads/sqlite/wal4m_scale.py verify \
  --db "$DB" --acks /tmp/wal-run/acks.jsonl --seed-rows 256 --expected-filler-seq 97 --quick-check
```

The multi-connection port is driven the same way:

```bash
python3 workloads/sqlite/sqlite-multiconn-harness.py run \
  --db /mnt/d9-six-none-a/sqlite-verify/multiconn.db --case wal10 --label wal10 --output-dir /tmp/mc-mount
python3 workloads/sqlite/sqlite-multiconn-harness.py compare --control /tmp/mc-control --mount /tmp/mc-mount
```

`wal10`, `walthread1`, `walthread4`, `waloverwrite`, `wal5` and `walro` are the available cases. Run the control pass on an ext4 path first and keep both transcripts.

## Acceptance suite (`acceptance/`)

`run_all.py` drives twelve product scenarios (import, save/read, watch, assets, conflict, npm, refactor, git, build, export, external tooling, recovery) plus six targeted probes (`a_*` .. `f_*`) and failure reproductions. These were written for the `d9-dev` GCP host and keep that host's paths and domain inside; treat them as reference implementations of scenario coverage rather than turn-key scripts. They are included because they are part of the case corpus that ran on the reference EC2 host.

## Legacy experiments (`legacy/`)

`legacy/d9work/`, `legacy/pr911/` and `legacy/atomic-counterfactual/` preserve earlier experiments verbatim, including their original absolute paths (`/home/ubuntu/...`) and CLI revisions. Use them to understand how a given number was produced; they are not parameterized and should not be run blindly.

## Case inventory of the reference host

Everything below was found on the reference EC2 host. The first block is included in this directory; the second block was deliberately left out because it is infrastructure rather than benchmark cases.

| Family on the host | Included as | Notes |
|---|---|---|
| `drive9-replays/replay-20260921-main`, `...-ext` | `main/`, `extended/` | the 2026-09-21 comparison matrix |
| `drive9-sixway-20260911/` | `sixway-20260911/` | 2026-09-11 12-round reference matrix (10 cases) |
| `drive9-test/sqlite50-fresh-main-*`, `sqlite-bigckpt-*`, `pr901-investigation-*`, `sqlite-multiconn-harness.py` | `sqlite/` | SQLite WAL / commit / checkpoint / multi-connection cases |
| `d9work/site-acceptance/` | `acceptance/` | twelve product scenarios + probes |
| `d9work/*.sh` | `legacy/d9work/` | 2026-09-15/16 comparison scripts |
| `dev2-pr911-d88af4e/*.py` | `legacy/pr911/` | PR911 cold/layer/live workloads |
| `drive9-atomic-counterfactual-20260913T1120/` | `legacy/atomic-counterfactual/` | atomic-replace counterfactual |
| `~/.t86-remote-cold-reset-*.py` | not copied | PR-specific remote reset helpers |
| `drive9-autopilot-*`, `drive9-autopilot-preflight-*` | not copied | agent/autopilot framework bundles, not cases |
| `t86-*`, `dev2-pr911-*` mount trees, `customer-e2e-*`, `customer-fault-*`, `bundle-det-a/b` | not copied | PR audits, customer acceptance runs and fault-injection experiments |
| `src/drive9` | not copied | git checkout of this repo; its `e2e/fuse-sqlite-*.sh` cases already live in git |

# How to run a full comparison

All commands run on the benchmark host. Set the variables you need, then:

```bash
export DRIVE9_BENCH_BIN=/home/ubuntu/drive9-main-<commit>/drive9

# 1) fixtures (once per host): 300/15000-file zip + local bare git repos
python3 workloads/extended/build_fixtures.py

# 2) four isolated drive9 mounts (one tenant each); exits non-zero if a mount already exists
python3 workloads/mount_groups.py

# 3) baseline matrix: smoke (scale 0.02) gate, then warmup + one measured round
cd workloads/main && python3 run_matrix.py smoke && python3 run_matrix.py full
python3 summarize.py

# 4) extended matrix (same coordinator, subset support)
cd ../extended && python3 run_matrix.py smoke && python3 run_matrix.py full
python3 summarize.py

# 5) optional: operation-level subset only
python3 run_matrix.py full op-write op-fsync op-unlink-pending
```

Rules the harness enforces:

1. Exactly one case at a time. `run_matrix.py` holds an exclusive `flock` on `DRIVE9_BENCH_LOCK`; a second runner fails immediately instead of overlapping.
2. Every case is bracketed by a drain: `drive9 mount drain` for the four drive9 groups (must report `ok` with all pending counters at zero) and `sync -f` for EFS/EBS.
3. `full` refuses to start until every selected case has a passing `smoke` row, so failures are caught on a 2%-scale workload.
4. Completed rows are skipped on restart (idempotent resume). The mount-side case root defaults to the harness directory name (`DRIVE9_BENCH_RUN_PREFIX`, e.g. `main`, `extended`, `replay-20260921-ext`); a fresh copy of the harness therefore never collides with directories left behind by an earlier run. Reusing the same prefix with an empty `results/` fails with `[Errno 17] File exists` - that is the harness refusing to overwrite an old case directory on purpose.
5. `summarize.py` asserts: expected row counts (6 groups x cases), no overlapping time windows, every drain ok, exit code 0, verified remote samples, and no credentials in the artifacts.

## Outputs

| Path | Contents |
|---|---|
| `results/<stage>-r<NN>-<group>-<case>.json` | one row per case/group/stage: durations, phases, `op_stats`, drain times, verification flags |
| `results/<...>.workload.json` | raw case output before the coordinator wraps it |
| `results/<...>.log` | stdio of the case process |
| `status.json` | current/last run state (`running`, `completed`, `completed_with_failures`, `stopped`) |
| `summary.json`, `measured-rounds.csv` | produced by `summarize.py`; the CSV is what feeds the report tables |

Every case directory on the mount is named after its label (`<mount>/<RUN_PREFIX>/<label>/`), so a failed sample can be inspected or preserved without touching the others.

# Parameter tuning

The tuning scripts restart the `none-a` mount with different flags and replay a small workload, so they must run while the main matrix is idle. They all derive their paths from `tune_sweep.py` (env-overridable).

```bash
export DRIVE9_BENCH_RUN_DIR=/home/ubuntu/drive9-replays/replay-<id>-ext   # where tuning results are written
python3 workloads/tuning/tune_sweep.py        # durability presets vs 300-file unzip
python3 workloads/tuning/tune_uc.py           # --upload-concurrency on parallel writes + drain
python3 workloads/tuning/tune_uc_unzip.py     # --upload-concurrency on serial unzip (expect no help)
python3 workloads/tuning/tune_batch.py        # --writeback-batch-window sweep
python3 workloads/tuning/conc_decompose.py <mount> <dir>   # which per-op drives concurrent-files
python3 workloads/tuning/disk_probe.py <mount> <dir>       # local disk utilisation during that pattern
```

Findings worth reusing (2026-09-21): `--writeback-batch-window` did not help serial small-file writes (100 ms window made unzip slower: 300 files 46.6 s -> 76.4 s); `--durability write-sync` was the best preset for unzip (300 files 30.9-31.3 s, extrapolated ~26 min for 15k); `--upload-concurrency 64` cut the parallel-write drain from 3.47 s to 0.27 s but does not help serial unzip.

# Diagnostics

These probes are how issues #954 and #959 were isolated. They need a dedicated diagnostic mount so the matrix mounts stay untouched.

```bash
# 1) launch the profiling mount (writeback, perf recorder every 2s, pprof on 127.0.0.1:16111)
python3 - <<'EOF'
import http.client, json, os, pathlib
cred = json.loads((pathlib.Path(os.environ.get("DRIVE9_BENCH_CRED_DIR", "/home/ubuntu/drive9-sixway-20260911/credentials")) / "none-a.json").read_text())
c = http.client.HTTPSConnection("drive9.example.invalid", 443, timeout=60)
c.request("POST", os.environ.get("DRIVE9_BENCH_REMOTE_ROOT", "/benchmark") + "/diag-mount?mkdir",
          headers={"Authorization": "Bearer " + cred["api_key"]})
print(c.getresponse().status)
EOF
tmux new-session -d -s diagmount "workloads/diagnostics/launch.sh > ~/diag-mount.log 2>&1"

# 2) mount-level counters: flush stages, stat cache behaviour, utime wait curve, unlink
PYTHONPATH=workloads/diagnostics python3 workloads/diagnostics/diag_probe.py

# 3) same-disk fsync control + interleaved (pending) unlink
PYTHONPATH=workloads/diagnostics python3 workloads/diagnostics/local_fsync_control.py

# 4) flush attribution: CPU profile + strace syscall summary
workloads/diagnostics/flush_attrib.sh

# 5) clean-window fsync counting and per-file sequence
workloads/diagnostics/strace_clean.sh
python3 workloads/diagnostics/parse_strace.py ~/d9diag/strace-clean.txt

# 6) HTTP-level per-operation baseline from inside the VPC (PUT / stat / rename / delete / mkdir / list)
python3 workloads/diagnostics/server_op_probe.py
```

Reference measurements from these probes (2026-09-21, EC2 gp3 root volume, main `fea9cdcf`):

| Probe | Result |
|---|---|
| `diag_probe.py` flush counters | one `flush_stage_shadow` (8.8 ms) + one `flush_snapshot_wb` (11.1 ms) per small file = 19.9-23.5 ms |
| `strace_clean.sh` | 700 `fsync` calls for 100 files (7 per file), 1.966 s total, 2.81 ms mean |
| `parse_fsync_seq2.py` | per file: 1 shadow fsync + 3 `atomicWrite` cycles (`.meta`, `.dat`, `.meta` again), each = temp fsync + rename + directory fsync |
| `local_fsync_control.py` | plain local write+fsync+rename+dir-fsync = 6.53 ms/file with 2 fsyncs; interleaved unlink on a pending file = 297 ms |
| `diag_probe.py` stat scan | first pass 300 files = 300 remote stats at 22.7 ms; immediate second pass = 0 remote calls (kernel attr cache); after the 30 s attr TTL = 302 remote stats again |
| `diag_probe.py` utime curve | delay 0 ms -> 103.8 ms; 50 ms -> 56.2 ms; 100 ms -> 8.0 ms; 200 ms -> 0.40 ms |
| `server_op_probe.py` | p50: stat 21.9 ms, list 31.4 ms, mkdir 45.5 ms, rmdir 43.4 ms, rename 59.1 ms, delete 63.4 ms, PUT 212 B 72.5 ms |

One finding from `staging_patterns.py` (same probe family) is worth knowing before interpreting write numbers: the 7-fsync staging path is only taken when the file is closed **without** a preceding `fsync(2)`.

| Write pattern | Per-file cost | Where the cost is |
|---|---|---|
| `open` + `write` + `close` (no fsync) | 21.9-22.2 ms | `flush` callback 20.8 ms = `flush_stage_shadow` 9.3 ms + `flush_snapshot_wb` 11.5 ms (this is issue #959) |
| `open` + `write` + `fsync` + `close` (strict) | 74.3 ms | `flush` is 0.045 ms; the synchronous remote upload happens inside `fsync` (73.1 ms), no shadow/snapshot counters |
| `open` + `write` + `fsync` + `close` (interactive) | ~14 ms | local-only fsync path; also no shadow/snapshot counters |

So `bare-writes`, `unzip`, `git clone` working-tree writes and any application that closes small files without fsync pay the #959 cost; cases whose helper writes with `flush` + `fsync` (`ls-stat-consistency`, `many-small-files`, `overwrite-delete-recreate`, `atomic-replace`, `copy-file`, `copy-tree`) do not.

## PrivateLink vs public path (A/B)

When an interface VPC endpoint exists with private DNS, `drive9.example.invalid` resolves to the endpoint ENI inside the VPC. To compare against the public NLB path in the same session:

```bash
# force the public path, then remount and re-run a subset
sudo sed -i '1i 192.0.2.10 drive9.example.invalid' /etc/hosts     # use the NLB public IP from `dig`
for g in none-a none-b coding-a coding-b; do fusermount -u /mnt/d9-six-$g; done
python3 workloads/mount_groups.py
cd workloads/extended && python3 run_matrix.py full op-open-create op-fsync op-unlink-settled

# revert afterwards (remove the /etc/hosts line and remount again)
```

Reference result: PrivateLink changed the pooled op-level mean from 78.67 ms to 77.83 ms (-1.1%); TCP connect p50 was 3.17 ms over PrivateLink vs 2.23 ms over the public NLB, both far below the per-operation server cost. PrivateLink buys isolation, not latency.

# Rules and pitfalls

1. Never run two case processes at once, and never start a second runner: the flock in `run_matrix.py` exists because cross-case concurrency invalidates every number.
2. Always drain between cases. `summarize.py` fails the run if any drain is unresolved or if case time windows overlap.
3. Long runs must live in tmux: SSH sessions to the reference host are closed after about five minutes.
4. Do not reuse a run directory that already has results (`run_matrix.py` skips completed rows). Create a new `replay-<id>` directory or delete the rows you intend to re-measure.
5. Preserve failed samples: `RETAIN FAILED SAMPLE <label>` means the case output was kept for inspection on purpose.
6. Credentials stay outside the repo; `summarize.py` fails if an artifact contains a `Bearer` token.
7. `interactive` groups look fast because durability is deferred to the drain; always compare "main + drain" for end-to-end statements.
8. `readdir` after a 2000-file burst can return one entry short for up to ~30 s (the `--dir-ttl` window). The harness uses bounded polling that is excluded from timing; do not count it as a case failure.
9. FUSE behaviour cannot be reproduced on macOS; run this harness on Linux.
10. When looking for regressions, compare against a same-day baseline on the same host: the local disk's fsync latency (2.8-3.4 ms on gp3 vs ~0.2 ms on NVMe) changes small-file numbers by an order of magnitude.

# Baseline results for sanity checks (2026-09-21 / 2026-09-22, main `fea9cdcf`)

| Workload | drive9 (4 groups) | EFS | EBS |
|---|---|---|---|
| concurrent-files (main + drain) | 14.4-15.6 s | 0.95 s | 0.34 s |
| atomic-replace | 20.0-24.6 s | 1.70 s | 0.34 s |
| rename | 16.5-18.6 s | 1.50 s | 0.014 s |
| ls-stat-consistency (interactive / fsync) | 2.78-2.88 s / 21.4-23.3 s | 2.34 s | 0.68 s |
| many-small-files | 53.4-105.7 s | 10.30 s | 1.70 s |
| medium-file (fsync) | 2.62-2.69 s | 0.30 s | 0.059 s |
| git-status (1191 files) | 19.5-23.3 s | 1.87 s | 0.12 s |
| git clone --depth=1 | 40.1-52.7 s | 20.03 s | 0.37 s |
| bare-writes (2000 files) | 45.1-45.9 s | 24.4 s | 0.079 s |
| overlay-install (coding-agent overlay) | 1.38-1.49 s | 23.51 s | 0.070 s |
| unzip-15k | ~39-43 min (extrapolated); 26 min with `write-sync` | 6.03 min | 0.82 s |

Operation-level means (ms, 100 iterations): `write` 0.12, `open(O_CREAT)` 0.43, `flush` 20.6, `close` 21.5, `fsync` strict 118 / interactive 12-16, `unlink` pending 225 / settled 76, `rename` pending 138-141, `utimes` 152. EFS equivalents are ~5 ms for the metadata operations.

# Related issues and reports

1. `#954` - `utimensat` on a just-closed file waits for its pending writeback commit (drives the unzip gap; `op-utimes`).
2. `#959` - writeback flush performs 7 synchronous fsyncs per small file, capping small-file creation at ~45 files/s (drives bare-writes / git clone / unzip staging cost; `diagnostics/`).
3. Internal benchmark report link omitted from the published harness.
