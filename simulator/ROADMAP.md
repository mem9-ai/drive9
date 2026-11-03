# Simulator Unification Roadmap

Working branch: `feat/simulator-unify` (= `test/simulator` + `main`).
End state: the simulator is the repo's single test platform — every tier runs
against a REAL drive9 backend (`--server`), the PR gate brings that backend up
on the runner, daily/release-gate actions run the same suites plus benchmarks
with baseline gating, and `blackbox/` + `e2e/` are deleted.

Progress markers: [x] done · [~] in progress · [ ] todo.

> **Pivot (2026-11-03): mock backend deleted.** The embedded mock server
> (`pkg/mockserver` + `cmd/drive9-mock-server`, SQLite metadb/tenantdb +
> rustfs), the simulator `--stack mock` supervision (`src/stack.rs`), and the
> injector `latency` route-shaping primitive are all REMOVED. The simulator
> now takes a real backend's address via `--server` and nothing else; the
> case vocabulary no longer contains `latency`. CI brings up the real backend
> itself (TiDB playground + rustfs + drive9-server, provider=local) in
> `.github/workflows/real-backend.yml` — profile `pr` for pull requests
> (called by simulator-e2e.yml), profile `full` for the daily cron and the
> release gate. What the mock bought us is kept below as history: it seeded
> the api-spec, the case language, and surfaced four real product findings
> (G4) before the pivot.

## G1 — Migrate all blackbox/ + e2e/ tests into simulator/cases

Case tree (mirrors the blackbox grouping; spec: `case-spec.md` §2):

```
cases/webdev/      [x] the 27 former simulator cases (.test → .t)   DONE
cases/drive9/      [~] ex-e2e suites — api-fs + cli-fs + git-ops-fast-clone
                    landed (all green against the real server); fuse-*/
                    pack/extent families still to port
cases/community/   [~] fstest-backed suites — 4 cases landed (sqlite, fsx,
                    pjdfstest, ltp) via the `check fstest` kind; fio/
                    smallfile/mdtest benchmarks run in the bench tier (G6)
cases/custom/      [ ] customer benchmark scenarios (case1/case2 invariants)
cases/juicefs/     [x] all 5 ported (rmr, random-rw, random-stress, fsrand,
                    cache-consistency via the observer vantage); python
                    helpers beside the cases per case-spec §2
cases/git/         [ ] upstream git t/ suites (superseded by community/git — prefer fstest git)
```

Per-module migration mapping (from the blackbox/e2e/fstest inventories):

- fstest-backed: pjdfstest, fio (smoke sizing; `--direct=N` — blackbox used
  direct=0, fstest defaults direct on), fsx, ltp (syscalls + fs scenarios),
  mdtest (fstest port needs no MPI), git official functional (fstest tier a/b).
- Hand-ported cases: juicefs group (5), drive9.sqlite (WAL remount + mmap
  probe), lock, pyxattr, git clone/worktree workflows, pack/auto-pack family,
  node_watch hard invariant ("content eventually visible").
- Not cases (stay measurement): workflow_perf, local_overlay_build, case1/case2
  benchmark bodies, git.official_perf, node_watch latency numbers.
- Engine prerequisites for the above (tracked in G3/G4): D9_SERVER/D9_API_KEY
  export, remount --pack/--unpack, named extra mounts, fstest check kind,
  tools layer, root/sudo for pjdfstest+ltp.

Delete `blackbox/` and `e2e/` only after G4 (CI cut-over) lands.

## G2 — Server API spec ~~+ embedded mock backend~~

1. [x] `drive9-server-api-spec.md` — DONE (`simulator/drive9-server-api-spec.md`).
   Provenance-tagged ([prod]=tidbcloud/fs, [v1]=this repo, [both]); covers
   conventions/auth, lifecycle, fs domain (per-selector params/headers/errors),
   batch limits, uploads V1/V2 + presign rules, extent v1 protocol (op registry
   + credential branches + errno conventions), /v1/sql, SSE, journals,
   git-workspaces, layers, object-credentials, quota, tokens, vault,
   runtime/exec, control plane, error model (envelopes + status table +
   stable codes + errno), limits table. Refresh inputs if contract code
   changes: tidbcloud/fs openapi_registry.go + this repo's
   extent_meta.go/jfs_meta.go/server.go.
2. [x] ~~Embedded mock server~~ — REMOVED by the 2026-11-03 pivot (see the
   note above). It ran as `pkg/mockserver` + `cmd/drive9-mock-server`
   (SQLite metadb/tenantdb, `--stack mock` supervision in `src/stack.rs`
   with rustfs via scripts/local-object-store.sh) and its fidelity work
   (selector key-existence, EnsureParentDirs, hardlink content-level, mtime
   Unix seconds, X-Dat9-Checksum-SHA256, SSE actor tagging + cursor replay)
   is what proved the engine and seeded the case suite before the pivot.
   The spec's former §21 (mock requirement tiers) went with it.

## G3 — Case language v2 (this branch, first slice landed)

- [x] `.test` → `.t` suffix (engine, lint, all 27 cases).
- [x] `drive9-test-*` prefix dropped; vocabulary is now bare names:
  `fault arm disarm window async await halt remount hold sample drain check`.
  `wait`→`await` and `kill`→`halt` (renamed: POSIX shell builtins win over
  PATH injection — bare `wait`/`kill` would never reach the engine).
- [x] ~~`latency <range> [--pattern P]` / `latency off`~~ — REMOVED by the
  pivot (ambient route-delay shaping in front of the injector; existed to
  give benchmark suites a "slow but healthy backend" shape against the
  mock). Benchmarks now measure the real backend as-is.
- [x] case-spec.md updated (vocabulary, layout, rules).
- Still open under this umbrella (needed by G1 migrations, from the earlier
  framework-gap analysis):
  [x] `fstest` check kind — per-test/file/script expansion into case entries
      (extractor + 4 unit tests; runs the pinned binary, keeps the JSON
      report as a run artifact; ltp TCONF → unsupported)
  [x] tools/deps layer — `tools ensure-fstest [--ensure-fstest <ref>]`
      (PATH / D9_FSTEST_BIN / clone+build into ~/.cache; verified end-to-end:
      cloned, built, ran sqlite against a plain dir, envelope parsed)
  [x] batch runner — `run-suite <manifest> --tier <t>` + simulator/suites.toml
      (pr-gate / pr-gate-mount / nightly / bench tiers; suite-summary.json;
      worst-rc exit)
  [ ] remount --pack/--unpack extensions, metrics-kind wiring, no-mount mode
  [x] D9_SERVER/D9_API_KEY/D9_TENANT/D9_CLI server-channel export (landed
      earlier on this branch)

## G4 — CI cut-over (simulator e2e replaces local-e2e.yml)

- [x] `run-suite` batch runner + tier manifest (pr-gate / nightly seeded).
- [x] First Linux runs (PR #1071) while the mock existed, across 5
  iterations: gspt build tags (make build-cli carries GO_TAGS), run-suite
  shared-home wipe + swallowed errors, missing fuse3 apt, mock batch-stat
  501 → empty success, tier split (pr-gate = no-mount only) + GNU
  `timeout -k` guards. pr-gate tier GREEN on Linux CI (build + fstest +
  api-fs + cli-fs).
- [x] WEDGE RESOLVED: the runner hang was environmental — FUSE mounts inside
  containers on GH runners need `--privileged` (`--cap-add SYS_ADMIN` +
  `seccomp=unconfined` still got fusermount EPERM; Docker Desktop's
  permissive defaults masked this locally). Under the pivot the FUSE tiers
  moved back onto the host runner (the backend now runs there too), each
  bounded by outer `timeout -k`; GH runners are ephemeral so a worst-case
  wedged daemon costs the job, not the fleet.
- [x] Engine hardening proven during the mock era and carried forward:
  mount wait gates on the daemon pid state file (a 5.8ms startup race —
  script `drain` stat'ed the pid path just before the daemon's atomic
  rename); drain failures capture rc + stderr tail; empty mount walks retry
  bounded; engine CLI invocations carry explicit credentials + isolated
  HOME (`cli_env` — the CLI resolves active-context > env, so ambient
  ~/.drive9/config could hijack case traffic).
- [~] Post-pivot PR gate: `.github/workflows/simulator-e2e.yml` now calls
  `real-backend.yml` with `profile=pr` (pr-gate + pr-gate-mount + pjdfstest
  + bench against TiDB playground + rustfs + drive9-server on the runner).
  First full-profile run pending; local-e2e.yml retires once the PR gate is
  proven green.
- Four REAL product findings are parked in the nightly tier (assertions not
  weakened; recheck each against the real backend, they were diagnosed
  against the mock):
  - `webdev/save-then-read`: write-temp+rename publish path leaves the
    daemon's local image EMPTY after the rename while the server holds the
    correct bytes (TORN-READ; zero GETs issued by the daemon).
  - `community/fstest-fsx`: post-suite drain stuck on a permanent
    commit-queue conflict (LWW), zero auto-retry.
  - `juicefs/cache-consistency`: long-lived observer vantage flaps its
    directory cache (mostly-empty with rare populated windows) even with a
    stable SSE head.
  - `community/fstest-sqlite`: mptest coordinator reports broken 0/0
    (fstest-internal startup behavior).

## G5 — Real-backend actions + release gate

- [x] Removed .github/workflows/blackbox{,-daily,-weekly}.yml.
- [x] `.github/workflows/real-backend.yml` (renamed from
  daily-real-backend.yml): the ONLY backend the simulator knows. Brings up
  TiDB playground + rustfs (scripts/local-object-store.sh) + built
  drive9-server (provider=local, static extent credentials) on the runner.
  Profiles: `pr` (pull-request gate — called by simulator-e2e.yml: pr-gate +
  pr-gate-mount + pjdfstest + bench) and `full` (daily cron + dispatch +
  `workflow_call`: adds the nightly tier). Triggers: schedule, dispatch,
  workflow_call. Artifacts on failure.
- [x] release-cli.yml requires this workflow (`real-backend-gate` job) — a
  release cannot ship until it passes.
- [ ] First green full-profile run (nightly tier + bench baseline).

## G6 — Perf suites as regression gates

- [x] Benchmark cases (tier "bench"): community/fstest-fio, fstest-smallfile,
  fstest-mdtest — run against the real backend (the `latency` shaping was
  removed with the mock; the measured shape is the deployment's own).
- [x] `simulator/scripts/perf_baseline.py`: extracts per-suite metrics from
  the run's fstest-*.json artifacts (fio bwBytes per direction, smallfile
  filesPerSec per phase, mdtest totalRate per op), compares against the
  previous baseline, RED (exit 1) beyond --threshold-pct, writes the new
  baseline. Verified with synthetic data (OK +2.5%, RED -30%).
- [x] Both profiles wired: baseline caches keyed perf-baseline-pr- /
  perf-baseline-full- via actions/cache save/restore.
- [ ] First real baselines land after the first CI runs; tune thresholds
  (real-backend numbers on ephemeral runners are noisier than the shaped
  mock runs this gate was designed against — expect to widen --threshold-pct
  or gate on the full profile only).

## Housekeeping

- fstest worktree has an uncommitted change (reverts the FUSE-unbackable LTP
  removal list — "failures are product bugs, not harness filters"); commit or
  restore it before pinning a ref for CI.
- firecracker sandbox remains a skeleton (not needed for this roadmap).
- Mock-era CI divergence notes (history; the x86-vs-arm64 renameat2 question
  may still matter against the real server):
  - exclusive-create PASSES locally (arm64 container) but failed in CI at
    section 4 (renameat2 RENAME_NOREPLACE race: RN-WINNERS=00, both renames
    fail) — x86_64 CI + the rename path to compare next;
  - rmr's kernel entry caches hold removed names for minutes on CI — the
    case now asserts server truth (CLI stat 404) + a fresh-name cycle;
  - a stray 16MB `drive9-mock-server` binary that had been committed at the
    repo root was deleted with the pivot (git history still carries it).
