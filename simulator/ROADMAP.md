# Simulator Unification Roadmap

Working branch: `feat/simulator-unify` (= `test/simulator` + `main`).
End state: the simulator is the repo's single test platform — PR gate runs core
cases against the embedded mock backend (with `latency`-shaped benchmark
baselines and delta gating), daily/release-gate actions run the same suites
against a real backend, and `blackbox/` + `e2e/` are deleted.

Progress markers: [x] done · [~] in progress · [ ] todo.

## G1 — Migrate all blackbox/ + e2e/ tests into simulator/cases

Case tree (mirrors the blackbox grouping; spec: `case-spec.md` §2):

```
cases/webdev/      [x] the 27 former simulator cases (.test → .t)   DONE
cases/drive9/      [ ] ex-e2e suites (api/cli/fuse-*/git-*/extent-*/pack/…)
cases/community/   [~] fstest-backed suites — 4 cases landed (sqlite, fsx,
                    pjdfstest, ltp) via the new `check fstest` kind; fio/
                    mdtest/smallfile benchmarks next (G6 perf gating)
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

## G2 — Server API spec + embedded mock backend

1. [x] `drive9-server-api-spec.md` — DONE (`simulator/drive9-server-api-spec.md`).
   Provenance-tagged ([prod]=tidbcloud/fs, [v1]=this repo, [both]); covers
   conventions/auth, lifecycle, fs domain (per-selector params/headers/errors),
   batch limits, uploads V1/V2 + presign rules, extent v1 protocol (op registry
   + credential branches + errno conventions), /v1/sql, SSE, journals,
   git-workspaces, layers, object-credentials, quota, tokens, vault,
   runtime/exec, control plane, error model (envelopes + status table +
   stable codes + errno), limits table, and mock-server tiering (§21).
   Refresh inputs if contract code changes: tidbcloud/fs openapi_registry.go +
   this repo's extent_meta.go/jfs_meta.go/server.go.
2. [~] Embedded mock server — Tier 0 skeleton LANDED
   (`pkg/mockserver` + `cmd/drive9-mock-server`, pure-Go modernc.org/sqlite,
   CGO_ENABLED=0 build verified; tests in pkg/mockserver/mockserver_test.go):
   - lifecycle: /healthz, /v1/provision (202), /v1/status (v1 shape +
     inline_threshold), DELETE /v1/tenant (key invalidation covered)
   - fs domain: read (single Range + 416 + Content-Range), stat (X-Dat9-Mtime/
     Btime), list, HEAD (X-Dat9-*), PUT (X-Dat9-Expected-Revision CAS),
     mkdir, rename (409 conflict), copy, recursive delete (409 non-empty),
     append-log (expected-size/revision checks); grep/find/tasks return
     documented empty shapes; PATCH/batch → 501 Tier 1 markers
   - uploads V1: initiate (part_checksums required) / part PUT / complete
     (assembles inline) / resume / abort (+410 semantics)
   - extent v1: /v1/extent/meta dispatch with wire shapes mirroring
     pkg/datastore/jfs_meta.go (counters, sessions, lookup/getattr/setattr/
     mknod/unlink/rmdir/rename/readdir/write/read/truncate; unknown op →
     ENOSYS; errno + EIO→500 rule) against simplified jfs_* SQLite tables —
     FIDELITY TODO: port full JuiceFS semantics (slice interpretation,
     flock/getlk/setlk, compaction ops) before the real extent mount runs
     against the mock
   - /v1/data-credential: file scheme (objects dir) + s3 scheme
     (--s3-endpoint, e.g. rustfs managed by the stack layer; static keys,
     mock trust boundary = the process)
   - /v1/sql: read-only handle on the tenant's SQLite (black-box checks)
   - embedded S3-compatible endpoint on /s3/ (PUT/GET/DELETE object, bucket
     create, ListObjectsV2 XML, no SigV4 verification) used when no external
     endpoint is configured
   Still open: JuiceFS full-semantics port (slice interpretation, flock,
   compaction ops) before the real extent mount runs against the mock;
   conformance diff vs real server.
   LANDED since: [x] rustfs integration + `--stack mock` —
   `simulator/src/stack.rs` spawns drive9-mock-server (port 0 + --port-file +
   /healthz wait; binary via D9_MOCK_SERVER_BIN or PATH) and reuses
   scripts/local-object-store.sh apply for rustfs (container/binary
   fallback + DRIVE9_S3_* credential convention; degrades to the mock's
   builtin /s3/ endpoint when rustfs cannot start). Verified end-to-end:
   real rustfs 1.0.3 container up, mock's /v1/data-credential minted the
   s3-scheme credential against it, `run --stack mock` provisioned and tore
   down cleanly.
   Design notes: reuses pkg/pathutil (not a Rust reimpl — the contract's
   source of truth is Go); snapshot/restore = tenant db file copy; mock
   reimplements the HTTP surface against SQLite, it does NOT reuse server
   handlers (pkg/datastore is a concrete type with MySQL-dialect SQL).

## G3 — Case language v2 (this branch, first slice landed)

- [x] `.test` → `.t` suffix (engine, lint, all 27 cases).
- [x] `drive9-test-*` prefix dropped; vocabulary is now bare names:
  `fault arm disarm window async await halt remount hold sample drain check
  latency`. `wait`→`await` and `kill`→`halt` (renamed: POSIX shell builtins
  win over PATH injection — bare `wait`/`kill` would never reach the engine).
- [x] `latency <range> [--pattern P]` / `latency off`: ambient per-request
  route latency sampled uniformly from e.g. `80ms-400ms`, applied by the
  injector before forwarding (works for mock AND real upstreams); set/clear
  journaled + appended to injector/armed.jsonl; proxy now also starts when a
  case uses latency without faults; unit test
  `latency_profiles_delay_matching_requests_only`.
- [x] case-spec.md updated (vocabulary, layout, latency primitive, rules).
- Still open under this umbrella (needed by G1 migrations, from the earlier
  framework-gap analysis):
  [x] `fstest` check kind — per-test/file/script expansion into case entries
      (extractor + 4 unit tests; runs the pinned binary, keeps the JSON
      report as a run artifact; ltp TCONF → unsupported)
  [x] tools/deps layer — `tools ensure-fstest [--ensure-fstest <ref>]`
      (PATH / D9_FSTEST_BIN / clone+build into ~/.cache; verified end-to-end:
      cloned, built, ran sqlite against a plain dir, envelope parsed)
  [x] batch runner — `run-suite <manifest> --tier <t>` + simulator/suites.toml
      (pr-gate / nightly tiers seeded; suite-summary.json; worst-rc exit)
  [ ] remount --pack/--unpack extensions, metrics-kind wiring, no-mount mode
  [x] D9_SERVER/D9_API_KEY/D9_TENANT/D9_CLI server-channel export (landed
      earlier on this branch)

## G4 — CI cut-over (simulator e2e replaces local-e2e.yml)

- [x] `run-suite` batch runner + tier manifest (pr-gate / nightly seeded).
- [x] `.github/workflows/simulator-e2e.yml`: PR gate = build CLI+mock+simulator
  → tools ensure-fstest → `run-suite --tier pr-gate --stack mock --stop-on-fail`
  → no-skip pjdfstest gate (sudo) → artifacts on failure. local-e2e.yml stays
  until the simulator gate is proven green on Linux (first PR run), then
  retires step by step.
- [x] PR #1071: first Linux runs. Fallout fixed across 5 iterations: gspt
  build tags (make build-cli carries GO_TAGS), run-suite shared-home wipe +
  swallowed errors, missing fuse3 apt, mock batch-stat 501 → empty success,
  tier split (pr-gate = no-mount only) + GNU `timeout -k` guards.
- [x] **pr-gate tier GREEN on Linux CI** (build + fstest + api-fs + cli-fs).
- [x] WEDGE RESOLVED: the runner hang was environmental, not product —
  FUSE mounts inside containers on GH runners need `--privileged`
  (`--cap-add SYS_ADMIN` + `seccomp=unconfined` still got fusermount EPERM;
  Docker Desktop's permissive defaults masked this locally). All FUSE tiers
  (mount/pjdfstest/bench) now run inside privileged, bounded containers:
  artifacts volume-mounted out, outer `timeout -k`, `docker kill` fallback.
  Runs complete normally; per-case diagnostics (TAP not-ok lines +
  script.out/mount.log tails) echo into the step log. The original "wedge"
  theory (D-state daemon) was disproven by local docker repro — mock+mount
  startup, file IO, SQLite WAL+mmap+concurrency and the full engine all
  passed locally; the mock's new request access log made that conclusive.
- [ ] Remaining pr-gate-mount failures are SUBSTANTIVE. Progress this round:
  - mock inline-threshold mirrored to the production 50000 (the 1GiB Tier-0
    value diverted .db commits from the multipart path into a direct-PUT
    overwrite bookkeeping corner of the commit queue →
    "missing base revision for overwrite" terminal failures);
  - engine CLI invocations now carry explicit credentials + isolated HOME
    (cli_env): the drain/vantage CLIs resolve active-context > env, so
    ambient config absence made drain fail credential resolution instantly;
  - drain failures now capture rc + stderr tail into the step log; empty
    mount walks retry bounded with a readdir probe diagnostic.
  OPEN (precisely narrowed): after the above, drain in the ENGINE context
  still fails with `lstat /tmp/drive9-mount-<hash>.pid: ENOENT` — the
  foreground worker never writes its pid state file when spawned by the
  engine, while the identical daemon invocation from a shell probe in the
  SAME container writes it and drain succeeds. NOTE: hash mismatch is
  RULED OUT — a 64s watcher on ALL of /tmp saw zero `.pid` files during
  the engine run (the observed `.stop` token is umount-teardown noise), so
  the WriteProcessState call itself does not happen (or its guard
  `!cfg.SkipProcessState` is false) in the engine-spawned context. rmr+drain
  DID pass in one earlier tier run — flaky or invocation-dependent. Next
  session: strace both daemons (engine-spawned vs probe-spawned) around
  startup and diff which flags/env make the CLI set SkipProcessState or
  skip the write; also dump the daemon's parsed flags via `drive9 mount
  --help` diff between the two spawn contexts.
  - fstest-sqlite: mptest "broken (0 errors out of 0 tests)" — likely falls
    out once drain/commit paths are healthy (terminal commit failures were
    killing the db files); recheck after the drain fix.
  - cache-consistency observer/manifest emptiness — recheck after the same
    fix (samples ran while commit state was broken).

## G5 — Daily real-backend action + release gate

- [x] Removed .github/workflows/blackbox{,-daily,-weekly}.yml.
- [x] `.github/workflows/daily-real-backend.yml`: cron daily + dispatch +
      `workflow_call` (release workflows can gate on it). Real backend =
      TiDB playground + rustfs via local-object-store.sh + built
      drive9-server (provider=local, static extent credentials); runs
      pr-gate + nightly tiers, no-skip pjdfstest gate, benchmark tier with
      baseline comparison; artifacts on failure.
- [ ] Wire the release workflow to require this one (release-cli.yml edit
      when the first daily run is green).

## G6 — Perf suites as regression gates

- [x] Benchmark cases (tier "bench"): community/fstest-fio, fstest-smallfile,
      fstest-mdtest — each shapes backend latency via the `latency` primitive
      (e.g. 2ms-8ms on PUT/GET /v1/fs) before running the suite.
- [x] `simulator/scripts/perf_baseline.py`: extracts per-suite metrics from
      the run's fstest-*.json artifacts (fio bwBytes per direction, smallfile
      filesPerSec per phase, mdtest totalRate per op), compares against the
      previous baseline, RED (exit 1) beyond --threshold-pct, writes the new
      baseline. Verified with synthetic data (OK +2.5%, RED -30%).
- [x] Both gates wired: simulator-e2e.yml (PR, mock backend, cache key
      perf-baseline-mock-) and daily-real-backend.yml (real backend,
      perf-baseline-real-) via actions/cache save/restore.
- [ ] First real baselines land after the first CI runs; tune thresholds.

## Housekeeping

- fstest worktree has an uncommitted change (reverts the FUSE-unbackable LTP
  removal list — "failures are product bugs, not harness filters"); commit or
  restore it before pinning a ref for CI.
- firecracker sandbox remains a skeleton (not needed for this roadmap).
