---
title: AGENTS.md - drive9 development guide for AI coding agents
---

## Repository overview

drive9 is a Go agent-native filesystem — a network drive with semantic search built on top of
TiDB/MySQL (metadata), S3 (large files), and db9 (small files + embeddings).

Module: `github.com/mem9-ai/drive9`  
Go version: 1.25.1 (see `go.mod`)

**Critical prerequisite**: the project has a local `replace` directive pointing to a sibling
repo. Clone it before building or testing:

```bash
git clone --depth 1 https://github.com/c4pt0r/agfs ../agfs
```

---

## Build commands

```bash
make build           # build server + CLI → bin/drive9-server, bin/drive9
make build-server    # server only
make build-cli       # CLI only (supports VERSION= for ldflags)
make build-cli-release  # cross-compile for linux/amd64|arm64, darwin/amd64|arm64
```

Direct Go equivalents:

```bash
go build -o bin/drive9-server ./cmd/drive9-server
go build -o bin/drive9 ./cmd/drive9
```

All binaries are built with `CGO_ENABLED=0`.

---

## Test commands

```bash
make test                          # go test -v ./... (all packages)
go test -v ./pkg/datastore/...     # single package
go test -v -run TestInsertAndGetNode ./pkg/datastore/   # single test
go test -v ./...                   # full suite
```

MySQL-backed tests require a container runtime or an explicit DSN:

```bash
# Use an existing MySQL/TiDB instance
DRIVE9_TEST_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/drive9_test?parseTime=true' make test
```

If `DRIVE9_TEST_MYSQL_DSN` is unset and `podman` is available, `make test` auto-configures
testcontainers via `scripts/test-podman.sh`. Otherwise a Docker-compatible runtime is used.

If a direct `go test` run fails with `rootless Docker not found`, retry with `make test`
so the project can use `scripts/test-podman.sh` to route testcontainers through Podman.

**E2E smoke tests** (not `go test`) live in `e2e/` and target live deployments:

```bash
DRIVE9_BASE=https://... bash e2e/api-smoke-test.sh
DRIVE9_BASE=https://... bash e2e/cli-smoke-test.sh
DRIVE9_BASE=https://... bash e2e/smoke-all.sh
```

---

## Lint and format

```bash
make lint            # golangci-lint run (v2.5.0, installed to bin/)
make fmt             # golangci-lint run --fix
```

golangci-lint is auto-installed to `bin/golangci-lint` on first `make lint`. There is no
`.golangci.yml`; linter runs with default settings. CI (`code-ci.yml`) enforces lint before
tests on every PR to `main`.

---

## Local dev server

```bash
source ./scripts/drive9-server-local-env.sh
export DRIVE9_LOCAL_INIT_SCHEMA=true   # only for a fresh/disposable database
make run-server-local
```

The env script sets defaults for `DRIVE9_LOCAL_DSN`, local mock S3, and Ollama-compatible
embedding. Override any var before running.

---

## Project layout

```
cmd/drive9/             CLI entrypoint (cp, cat, ls, mv, rm, mount, umount, ...)
cmd/drive9-server/      Server entrypoint
.github/ISSUE_TEMPLATE/ GitHub issue templates (bug report / enhancement / feature request)
pkg/
  backend/              AGFS FileSystem implementation (Drive9Backend)
  client/               Go SDK HTTP client
  datastore/            Core metadata store (TiDB/MySQL)
  embedding/            Embedding provider integration
  encrypt/              Encryption helpers
  fuse/                 FUSE mount (go-fuse/v2)
  logger/               Structured logging (zap)
  meta/                 Metadata/search models
  metrics/              Metrics recording
  s3client/             S3 interface (AWS + local mock)
  server/               HTTP server (/v1/fs/{path} router)
  tenant/               Tenant schema management
  pathutil/             Path canonicalization and validation
  semantic/             Durable background task types
  traceid/              Trace ID helpers
internal/
  testmysql/            MySQL test helpers (shared across packages)
e2e/                    Live bash smoke tests (not go test)
scripts/                Shell helpers for local dev and test
docs/                   Design documents
site/                   Frontend / release assets
```

When creating a new GitHub issue, follow the templates under `.github/ISSUE_TEMPLATE/`
to keep issue structure and required context consistent.

---

## Code style guidelines

### Package structure

- One package per directory; package name matches directory name.
- Package-level doc comment on the first file: `// Package foo provides ...`
- Each package has a focused responsibility — avoid cross-cutting concerns.

### Imports

Group imports in three blocks separated by blank lines:

```go
import (
    "context"         // stdlib
    "fmt"

    "go.uber.org/zap" // third-party

    "github.com/mem9-ai/drive9/pkg/logger" // internal
)
```

Use an import alias only when there is a naming collision:

```go
pathpkg "path"  // disambiguates from a local "path" variable
```

### Naming conventions

- Packages: short, lowercase, no underscores (`datastore`, `pathutil`, `s3client`).
- Exported types/functions/consts: PascalCase (`FileNode`, `StorageType`, `RRFMerge`).
- Unexported: camelCase (`smallFileThreshold`, `newBaseBackend`).
- Sentinel errors: `ErrFoo` pattern (`ErrNotFound`, `ErrPathConflict`).
- Constructor functions: `New(...)` or `NewWithConfig(cfg Config)`.
- Config structs: `type Config struct { ... }` passed to `NewWithConfig`.
- Test helpers: accept `*testing.T` as first arg, call `t.Helper()` at top.

### Types and constants

- Use typed string constants for domain enums:

```go
type StorageType string
const (
    StorageDB9 StorageType = "db9"
    StorageS3  StorageType = "s3"
)
```

- Prefer `*T` return from constructors; embed only when there is a strong behavioral reason.
- Struct fields that can be absent: use pointer (`*int64`, `*time.Time`).

### Error handling

- Return errors; do not panic in library code.
- Wrap with context: `fmt.Errorf("insert node %s: %w", path, err)`.
- Sentinel errors defined at package level with `errors.New(...)`.
- Check `errors.Is(err, datastore.ErrNotFound)` at call sites; do not compare strings.
- Ignore errors explicitly: `_ = s.Close()` (not silent discard).

### Logging

Use `go.uber.org/zap` exclusively. Never use `log` or `fmt.Print*` in library code.

```go
logger.With(zap.String("path", path)).Error("failed to insert node", zap.Error(err))
```

Obtain a logger from `pkg/logger` or accept `*zap.Logger` via `Config`.

### Testing

- Test files use `package foo` (same package, not `foo_test`) for white-box access.
- Shared MySQL setup via `internal/testmysql`; call `testmysql.ResetDB(t, db)` to clean state.
- Test helper constructors (`newTestStore`, `newTestServer`) accept `*testing.T`, call
  `t.Helper()`, register cleanup with `t.Cleanup(func() { ... })`.
- Use `t.Fatal` / `t.Fatalf` for setup failures; use `t.Errorf` for assertion failures.
- No external assertion library — plain `if got != want { t.Fatalf(...) }`.
- `TestMain` in `testmain_test.go` wires up the shared DSN for each package.

### Failpoint testing

- Use failpoint only for high-value concurrency and failure-path boundaries (lease expiry,
  renew/stop races, panic cleanup, finalize ack/retry ownership checks), not as a blanket
  replacement for ordinary polling or simple mocks.
- Put failpoint-backed tests in `*_failpoint_test.go` files with `//go:build failpoint`.
- Prefer injection points at state-transition boundaries so tests can deterministically
  control ownership windows without production-only branching.
- Scope failpoint callbacks narrowly by task/resource/action so one test cannot accidentally
  perturb unrelated work in the same package.
- Always pair `failpoint.Enable(...)` / `failpoint.EnableCall(...)` with `t.Cleanup(...)`
  that disables the failpoint.
- Run failpoint tests through `python3 scripts/run_failpoint_tests.py` or `make test-failpoint`.
  Do not run them in parallel with ordinary `go test` commands or `make lint`:
  `failpoint-ctl enable/disable` rewrites source files during the run and can
  break concurrent non-failpoint builds and type-checking.
- Keep failpoint-off behavior identical to the non-instrumented code path.
- When a timing-sensitive test still needs synchronization, prefer channels plus failpoint
  gating over sleeps that guess at scheduler timing.

### Context

- All I/O and DB calls accept `context.Context` as their first parameter.
- Pass context through; do not store it in structs.

### Concurrency

- Protect shared mutable state with `sync.Mutex` (named `mu`); prefer fine-grained locking.
- Background goroutines use `sync.WaitGroup` + a cancel context for clean shutdown.

### HTTP server patterns

- Route on `*http.ServeMux`; handler methods on `*Server`.
- Response helpers write JSON with `encoding/json`; set `Content-Type: application/json`.
- Auth checked in a thin middleware layer (`pkg/server/auth.go`).

### Path conventions

- All drive9 paths are absolute, UTF-8, NFC-normalized, no backslashes, no `..` segments.
- Directories always end with `/`; files never do.
- Use `pkg/pathutil` for all path normalization — never manipulate raw strings directly.
