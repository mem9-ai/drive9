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
- [ ] First green Linux run of the workflow; fix platform fallout.
- [ ] TAP → e2e-aggregate adapter (Feishu/signature plumbing) at cutover.

## G5 — Daily real-backend action + release gate

- [ ] Remove blackbox workflows (.github/workflows/blackbox*.yml).
- [ ] Daily action: real backend (TiDB + rustfs + drive9-server, provider=local
      first; hosted later) runs the full tier set; same perf-baseline gating as
      G6; wired as a required release gate.

## G6 — Perf suites as regression gates

- [ ] fstest benchmark suites (fio, smallfile, mdtest, fsmark, mdworkbench) in
      PR gate (mock backend + `latency` shaping) and daily (real backend).
- [ ] Baseline store: last run's JSON per suite/params; compare, mark red when
      worse than threshold %; report p50/p95/p99 deltas in the job summary.
- [ ] fstest prerequisites: pin by git ref (add a --version flag upstream),
      musl static build in the tools layer, system fio for the fio wrapper.

## Housekeeping

- fstest worktree has an uncommitted change (reverts the FUSE-unbackable LTP
  removal list — "failures are product bugs, not harness filters"); commit or
  restore it before pinning a ref for CI.
- firecracker sandbox remains a skeleton (not needed for this roadmap).
