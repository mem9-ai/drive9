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
cases/community/   [ ] fstest-backed suites (pjdfstest, fio, ltp, fsx, mdtest, git t/)
cases/custom/      [ ] customer benchmark scenarios (case1/case2 invariants)
cases/juicefs/     [ ] JuiceFS-inspired deterministic workloads (fsrand, random_rw, …)
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
2. [ ] Embedded mock server in the simulator:
   - Go program (reuses pkg/client types + pkg/pathutil; NOT a Rust reimpl —
     contract source of truth is Go) exposed as a stack flavor.
   - Embedded SQLite as metadb + per-tenant tenantdb (snapshot/restore = file
     copy; /v1/sql reads the mock's own tables).
   - Embedded rustfs as the S3-compatible endpoint (extent data plane minted
     via the file/s3 credential paths from the spec).
   - Anti-drift: nightly conformance diff of mock vs real server responses.
   - Note: pkg/datastore is a concrete type with MySQL-dialect SQL
     (ON DUPLICATE KEY ×17, FOR UPDATE ×91) — mock reimplements the HTTP
     surface against SQLite, it does NOT reuse server handlers.

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
  framework-gap analysis): `fstest` check kind (per-test TAP expansion),
  tools/deps layer, D9_SERVER/D9_API_KEY export, remount extensions,
  metrics-kind wiring, no-mount mode, batch runner + suites manifest.

## G4 — CI cut-over (simulator e2e replaces local-e2e.yml)

- [ ] `run-suite` batch runner + tier manifest (pr-gate / nightly / manual).
- [ ] New workflow: PR gate = embedded mock backend + core cases + pjdfstest
  as hard gate; TAP → e2e-aggregate adapter (keep Feishu/signature plumbing).
- [ ] Retire local-e2e.yml steps as suites migrate; keep Go unit tests as-is.

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
