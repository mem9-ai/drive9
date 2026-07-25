# shared-smoke-test — multi-tenant / shared-schema e2e

Covers **shared / multi-tenant** behavior:

- **Control plane**: placement, soft-cap, delete/count, fork/sql gates, metrics
- **Multi-tenant data plane**: cross-tenant isolation (same path, list/grep/find, mutations)

Does **not** cover single-tenant breadth — that stays with `api-smoke` / `cli-smoke` / `fuse-*`.

## Design

| Principle | Approach |
|-----------|----------|
| Clean env by default | **self**: start meta + `drive9-server` (LocalClustersAPI), tear down when done |
| Avoid wrong public BASE | Self-start by default; attach needs **BASE + META from the same stack** |
| Meta + metrics | First-class for control-plane assertions |
| Semantic case names | See table below |
| Independent cases | No cross-case order; each case provisions, checks meta, and cleans up |

## Quick start

```bash
# Clean stack + all shared cases
bash e2e/shared-smoke-test.sh

# Single case
bash e2e/shared-smoke-test.sh multi-tenant-isolation
bash e2e/shared-smoke-test.sh soft-cap-and-new-pool

bash e2e/shared-smoke-test.sh list
```

Defaults: soft=2, warm=2.

## Attach (existing local-shared stack)

```bash
export DRIVE9_BASE=http://127.0.0.1:9009
export DRIVE9_META_DSN='root:...@tcp(127.0.0.1:PORT)/drive9_meta?parseTime=true'
bash e2e/shared-smoke-test.sh attach
```

## Cases

| Case | Kind | Checks |
|------|------|--------|
| `identity-and-placement` | Control | provider / placement / co-located pool / no dedicated binding |
| `gates-fork-sql` | Control gates | fork 409, sql 400 |
| `lifecycle-delete-and-count` | Control | delete tenant, count, peer intact |
| `soft-cap-and-new-pool` | Control | soft-cap full → new physical pool |
| `metrics-consistency` | Observability | `/metrics` vs meta sample |
| `multi-tenant-isolation` | **Multi-tenant data plane** | same path / list / grep / find / cross-tenant mutations |

## Split vs single-tenant smokes

```text
e2e/shared-smoke-test.sh   ← shared / multi-tenant
e2e/api-smoke-test.sh      ← single-tenant API breadth
e2e/cli-smoke-test.sh      ← single-tenant CLI
e2e/fuse-*.sh              ← single-tenant FUSE
```

When running single-tenant suites against a shared backend:

```bash
RUN_SEMANTIC_CHECKS=0 RUN_SQL_CHECKS=0 RUN_CLI_FORK_CHECKS=0 ...
```

Load / loadgen is out of scope for this smoke.

## Layout

```text
e2e/shared-smoke-test.sh     # entry (same pattern as other *-smoke-test.sh)
e2e/shared-smoke-test/
  cases/                     # per-case scripts
  lib.sh                     # meta / provision helpers
  README.md
```

## Dependencies

- **self**: Docker/Podman, go, curl, jq, openssl, python3, mysql client
- **attach**: curl, jq; meta asserts also need mysql + `DRIVE9_META_DSN`
