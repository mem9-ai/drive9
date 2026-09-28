---
title: GCP Drive9 document replay package
---

This package adapts 51 core cases to 306 case/group rows across six presets from
the original internal guide (link omitted),
revision 103. Execution evidence is stored separately on the test VM.

## Scope and sources

| Suite | Entry | Cases | Measured rows |
|---|---|---:|---:|
| A | `sixway/run_sixway_vm.py --group G01` | 10 | 60 |
| B | `formal_suite.py G01 B --run-id ...` | 18 | 108 |
| C | `formal_suite.py G01 C --run-id ...` | 23 | 138 |

G01–G03 use `none`; G04–G06 use `coding-agent`. Each profile runs
`interactive` (writeback), `close-sync`, then `write-sync`. All cases are serial.
The formal suite wrapper preserves dirty segments and resumes remaining cases
on a fresh mount. Auxiliary mounts inherit the selected preset; the main mount
is stopped before an auxiliary mount starts. A run uses `--scale 1` by default.

The 0920 report adds 244 official Node v22.13.0 `test-fs-*` files, pinned in
`node_fs/manifest.json` (240 parallel, 4 sequential), plus five fault
categories X1-X5. `faults/manifest.json` pins the GCP interpretations. They
exercise these categories on the GCP VM; their injection mechanism and timings
are not a reproduction of the report's Firecracker run.

1. A preserves the workload logic of `workloads/main/cases.py` (formatting only). Compared with the older
   `sixway-20260911/cases.py`, only assertion diagnostics and traceback capture
   differ. The GCP runner is reconstructed; the deleted VM's runner is unavailable.
2. B comes from `claude-notes/drive9-site-acceptance-fe581b9f.tar.gz`.
   The shared `config.py` and `common.py` use the parameterized Feishu appendix.
3. C comes from section 8 code blocks. Section 8.7's later D10/N07 split is
   applied; D12/N11 are included in both scheduling and summaries.
4. `provenance.json` records original source hashes. `SHA256SUMS` fixes the final
   package bytes. `case-manifest.json` lists every entrypoint; `matrix.json`
   expands all six configurations into 306 core rows. These are restored/adapted
   sources, not a claim of byte-identical recovery of the deleted VM.

## Official Node test-fs suite

`node_fs/prepare_source.py` downloads the official v22.13.0 source archive
from nodejs.org to the local disk and checks its pinned SHA-256. The package
contains a fixed manifest and runner; it does not copy or rewrite upstream
tests. Node's `tools/test.py` runs each file with the installed v22.13.0
binary, using `NODE_TEST_DIR`/`--temp-dir` to put temporary files on Drive9.
The runner uses a short mount path for each file: Node's Unix socket tests can
fail from the Linux socket pathname limit when the temporary path is deep.

```bash
cd site
set -a; source config.env; set +a
python3 ../node_fs/prepare_source.py
python3 ../node_fs/run_test_fs.py --list
python3 ../node_fs/run_test_fs.py --only parallel/test-fs-access
python3 ../node_fs/run_test_fs.py
```

The last two commands require a healthy `coding-agent` main mount. Set
`D9_ACTUAL_SYNC_MODE` from the mount log before running so each result records
the chosen auto policy. Use a fresh run ID/results directory for the full
suite after the single-test probe. The runner is
serial. Per test it records official TAP `duration_ms`, wall time, pre/post
drain, exit status and the full TAP log. It stops on dirty drain. Official
status-file skips are reported as `skip`, not counted as passes. A local ext4
control is a later comparison.

If a dirty drain stops the suite, preserve that cache and resume the remaining
test IDs on a fresh mount/cache and run ID. Merge the segment summaries with
`node_fs/merge_runs.py --output-dir <new-local-dir> <summary1> <summary2> ...`.
The merger requires every pinned test exactly once, keeps failures and skipped
tests visible, and writes a complete per-file timing CSV.

## X1-X5 GCP fault cases

The test-only Go helper built from `faults/proxy/` listens on loopback and
forwards to the configured PSC HTTPS endpoint. Each case uses a fresh remote
subpath, FUSE mount and local cache. Its evidence JSONL contains the request
method/path and fault outcome; it does not record authorization headers.

Build the proxy from the repository root before running X cases (Go is required).
The command below targets the Linux amd64 test host; generated binaries and
historical distribution archives are not committed.

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath \
  -o workloads/gcp-replay/faults/proxy-linux-amd64 \
  ./workloads/gcp-replay/faults/proxy
```

`python3 -B workloads/gcp-replay/validate.py` validates the source package
without requiring the generated proxy. `SHA256SUMS` covers source files only.

| Case | GCP fault |
|---|---|
| X1 | One write request waits five seconds, then loses its connection |
| X2 | Writes are delayed; the second matching request loses its connection |
| X3 | First write gets HTTP 429 with `Retry-After: 1` |
| X4 | First mount request loses its connection, then mount is retried |
| X5 | First upstream 2xx write response is dropped before the client sees it |

Run only when the other suites have stopped and their mounts are cleanly
unmounted. Use a fresh results/run ID for the single-case probe and full run.

```bash
cd site
set -a; source config.env; set +a
export D9_FAULT_PROXY="$PWD/../faults/proxy-linux-amd64"
python3 ../faults/run_x.py --list
python3 ../faults/run_x.py --only x3 --run-id gcp-COMMIT-x3-smoke
python3 ../faults/run_x.py --run-id gcp-COMMIT-x-full
```

Set `D9_RESULTS` to distinct local result directories for these two commands.
Check `result.json` and `events.jsonl` for each case. Successful fault
triggering and recovery are recorded separately from the original write
outcome and from drain. The five case timings include the GCP-local proxy;
compare them only with control runs using the same path and proxy setup.

## Local validation

```bash
python3 validate.py
python3 sixway/run_sixway_vm.py --list
bash site/run-all.sh --list
bash site/run-dn.sh --list
```

These commands do not import the case helpers or run workloads. Validation
checks syntax, local imports, actual entrypoint IDs, runner lists, summary
coverage and hashes. Linux/FUSE validation is deferred to the VM smoke step.

## Before running on Linux

1. Restore the PSC endpoint/DNS and verify TLS and tenant access.
2. Deploy a pinned Linux amd64 CLI. Record client SHA/version/hash, server
   revision, kernel, VM and disk type, and endpoint in the run evidence.
3. Install Python 3.9+, FUSE3, Git, tar, unzip/zip, patch, iptables, iproute2,
   tmux and curl. Enable `user_allow_other` and passwordless sudo.
4. Install Node 22.13.0/npm 10.9.2 on the local disk. Copy
   `site/config.example.env` to `site/config.env` and fill the placeholders.
   Set credentials through the selected context or environment; never include
   keys in this package. Both FUSE and CLI HTTP reads use `D9_SERVER`.
5. Use a dedicated test workspace. Set a fresh run ID and all state/results
   paths outside FUSE. Auxiliary recovery cases preserve the historical
   `none` profile; the main B/C mount uses `coding-agent` and auto durability.
6. Run `bash bootstrap.sh --check-only`, then `bash bootstrap.sh` and
   `bash setup-tools.sh` from `site/`. Fix every missing prerequisite first.
7. Pin the actual endpoint IP in `D9_ENDPOINT_IPS` before fault injection.
   Inspect the primary and auxiliary mount logs for their actual sync modes.

The cloud endpoint, credentials, main mount, CLI binary, fixtures and Node tools
are managed separately from this package.

## A: four groups in sequence

Source the configured environment. Ensure `/mnt` is traversable by `nobody` for
the permissions case, and no other workloads are running. Use tmux on Linux.

```bash
python3 sixway/run_sixway_vm.py serial-smoke --run-id gcp-COMMIT-smoke
python3 sixway/run_sixway_vm.py serial --run-id gcp-COMMIT-r01
```

Run full scale only after reviewing all 40 smoke results. The runner creates
one group's mount, runs all ten cases with pre/post drains, unmounts it, then
continues to the next group. It uses one authenticated workspace and distinct
remote subpaths. It never provisions tenants. Results live under
`~/drive9-replays/<run-id>/`; caches and failed samples are retained.

`duration_s` and `phases_s` remain the original case's timed work. The runner
adds case wall time, drain wall time and `duration_plus_drain_s`; this last field
excludes untimed preparation/checks. Remote sample contents are checked by CLI
HTTP reads. Original case assertions check absent paths locally.

## B and C: main mount and execution

After configuring the environment and preparing fixtures, create the main mount
with a fresh cache and overlay on local disk. Example for a supervised mount:

```bash
drive9 mount --server "$D9_SERVER" --mode=fuse --profile coding-agent \
  --allow-other --cache-dir "$D9_STATE/main-cache" \
  --local-root "$D9_STATE/main-overlay" \
  ":$D9_REMOTE_ROOT" "$D9_MOUNT"
```

Use the configured `$D9_BIN` in place of `drive9`. Omit `--durability` for the
historical auto policy; record the actual sync mode from the mount log before
comparing performance. Mount setup and its live verification are a later step.

```bash
cd site
bash run-all.sh
bash run-one.sh s02_save_read.py
bash run-dn.sh
DN_ONLY="d10 n07" bash run-dn.sh
```

Each command needs a clean mount and fresh result paths for the selected cases.
The single-case command above is an alternative, not a rerun into old results.
After B (whose last case is response-loss injection C), restore/restart a clean
main mount before C. Do not reuse an unresolved cache as a fresh measurement.

B schedules S1-S12 then E/F/D/B/A/C. C schedules D01-D11 and N01-N10 first,
then unmounts the main mount and runs D12 and N11 with separate fresh caches,
overlays and owned mounts. Isolated failures remain failures and retain their
cache evidence; the next isolated case can proceed only after unmount succeeds.
Unexpected dirty drains in the normal cases stop that suite for inspection.
On a process timeout, inspect remaining children and iptables/tc state before
resuming; runners do not silently clear unknown state or discard cached data.

N07's shared cache and N10's npx cache live on local disk. N10 still installs
corepack inside FUSE and checks file completeness. N11 explicitly tests the
shared cache inside FUSE. Historical FAIL annotations are context, not skips.

## Results and limits

1. Raw per-case JSON retains checks, steps, duration, compatibility observations
   and sync evidence. `run_all.json`/`run_dn.json` add process exit, pre/post
   drain time and case-wall-plus-drain time. Raw duration can include internal
   syncs, fixed waits and validation; do not label it pure I/O throughput.
2. `SUMMARY.*` and `DN-SUMMARY.*` retain all cases, including not-run entries.
   Use the runner's `ok` and raw sync evidence with the functional `verified`
   field. Compatibility limitations, environment blocks and missing quota
   triggers must be reported, not treated as complete feature proof.
3. Local validation does not prove that fault injection, current CLI flags,
   restore semantics or performance work on Linux. Verify them during the
   scheduled smoke/live stages and preserve differences from the old baseline.
4. `workloads/` is locally excluded from Git. Deploy the standalone archive;
   cloning the Drive9 repository alone will not include this package.
