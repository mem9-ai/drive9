# Drive9

Persistent workspaces for AI agent sandboxes.

Sandboxes are disposable. Your agent's work should not be.

Drive9 keeps workspace state on the server. Agents mount it, use standard tools
such as Git, npm, grep, and test runners, and when the sandbox dies, the
workspace survives. Workspaces can be forked, checkpointed, rolled back, and
committed with server-side conflict detection when the base has changed.

Local disks run the process. Git stores the final result. Drive9 keeps the
working state in between.

## Why Drive9

Agents now clone repositories, edit files, install dependencies, run tests,
produce logs, create artifacts, and try multiple approaches. The sandbox may
last minutes; the work often needs to last longer.

Without a workspace state layer, teams usually stitch together local disks,
object storage, Git, databases, and custom checkpoint code. That pushes hard
state-management questions into every agent framework:

- Which files, logs, and artifacts survive sandbox replacement?
- Where do parallel attempts stay isolated, commit, or roll back?
- How do agents get path-scoped access instead of a full workspace key?

Drive9 moves those questions into the workspace layer.

## Core Capabilities

### 1. FUSE Mount

Mount a server-side workspace and keep using ordinary filesystem tools.

```bash
drive9 ctx add --name prod --server https://api.drive9.ai --api-key "$DRIVE9_API_KEY"
drive9 ctx use prod
drive9 mount --mode=fuse --profile=coding-agent :/ ~/drive9
git clone https://github.com/mem9-ai/drive9.git ~/drive9/drive9
```

### 2. Two-Tier Workspace State

Keep durable workspace state in Drive9 while `.git`, dependencies, build output,
and caches can stay local for tool performance.

```bash
drive9 mount --mode=fuse --profile=portable :/workspace /mnt/workspace
drive9 umount /mnt/workspace
drive9 mount --mode=fuse --profile=portable :/workspace /mnt/workspace
```

### 3. Scoped Tokens

Issue short-lived workspace credentials scoped by path and operation.

```bash
drive9 token issue agent-17 \
  --ttl 1h \
  --allow /repo/src:read,list,search,write \
  --allow /repo/tests:read,list,search
```

### 4. Context Fork

Create a server-side copy-on-write workspace context for heavier exploration.

```bash
drive9 ctx fork experiment-auth --from prod
drive9 ctx use experiment-auth
drive9 mount --mode=fuse :/ ./work
```

### 5. LayerFS Attempts

Run a writable attempt over a read-only base. The base changes only after commit.

```bash
drive9 fs layer create :/repo --name fix-auth --tag task=auth
drive9 mount --mode=fuse --profile=coding-agent --layer fix-auth :/repo ./attempt

# edit files, run tests, review the attempt
drive9 fs layer diff fix-auth
drive9 fs layer checkpoint fix-auth --label tests-pass
drive9 fs layer commit fix-auth      # or: drive9 fs layer rollback fix-auth
```

Drive9 checks the base before commit. If the base changed, the layer is
preserved as conflicted instead of silently overwriting the workspace.

### 6. Pack and Unpack Local State

Make selected local overlay state portable when a replacement sandbox needs a
warm workspace.

```bash
drive9 pack --mount /mnt/workspace
drive9 unpack --local-root /tmp/new-local --remote-root /workspace --profile portable
```

## Quick Start

Build the binaries:

```bash
go build -o bin/drive9 ./cmd/drive9
go build -o bin/drive9-server ./cmd/drive9-server
go build -o bin/drive9-server-local ./cmd/drive9-server-local
```

Connect to a Drive9 server and mount a workspace:

```bash
bin/drive9 ctx add --name dev --server https://api.drive9.ai --api-key "$DRIVE9_API_KEY"
bin/drive9 ctx use dev
bin/drive9 mount --mode=fuse --profile=coding-agent :/ ~/drive9
```

Simulate a sandbox handoff:

```bash
# Sandbox A
echo "notes from run 42" > ~/drive9/run-42.txt
bin/drive9 umount ~/drive9

# Sandbox B
bin/drive9 mount --mode=fuse --profile=coding-agent :/ ~/drive9
cat ~/drive9/run-42.txt
```

## Architecture

```text
agents / sandboxes / humans
        |
        | FUSE mount, CLI, Go SDK, HTTP API
        v
Drive9 server
        |
        | metadata, path tree, revisions, layers,
        | checkpoints, scoped tokens, audit/search metadata
        v
TiDB (MySQL-compatible) metadata store
        |
        | large bytes, layer objects, local-state packs
        v
S3-compatible object storage
```

Small files and metadata live in the metadata store. Large content uses
S3-compatible object storage. TiDB is the metadata and ledger backend; workspace
lifecycle semantics such as LayerFS, checkpoint, rollback, and conflict handling
are implemented by Drive9.

## How It Compares

| Approach | Where state lives | Best at | Drive9 difference |
| --- | --- | --- | --- |
| Local sandbox disk | One sandbox or VM | Fast execution | Workspace survives sandbox replacement |
| Git | Repository history | Final review and merge | Work-in-progress files, logs, generated artifacts, and failed attempts are first-class workspace state |
| Object storage + database | Application code | Custom persistence | Recovery, branching, permissions, and conflicts move out of every framework |
| ArtifactFS-style FUSE + Git systems | Git handoff | Repo cold start and Git-native workflow | Drive9 participates while work is happening |
| AgentFS-style session systems | Portable session database | Queryable agent/session state | Drive9 focuses on mountable workspace file state and ordinary tool compatibility |
| Drive9 | Server-side workspace state | Runtime workspace lifecycle | Attempts, checkpoints, rollback, scoped access, and commit/conflict handling |

## Boundaries

- Drive9 keeps workspace state; another system still runs the sandbox process.
- It does not preserve live processes, sockets, terminals, or in-memory model context.
- It is not a Git replacement; Git remains the final review and history layer.
- It targets agent workspace workloads, not full general-purpose POSIX compatibility.
- Search and semantic metadata exist, but Drive9 is not a vector-memory product.

## Documentation

- [LayerFS V1 design](docs/design/layered-filesystem-v1-design.md)
- [LayerFS feature matrix](docs/design/layered-filesystem-feature-matrix.md)
- [Pack/unpack profile spec](docs/design/pack-unpack-profile-spec.md)
- [Git fast clone workspace design](docs/design/git-fast-clone-workspace.md)
- [Weekly release notes](docs/release-notes/2026-05-18-to-2026-06-14.md)

## Development

```bash
go test ./...
go test -race ./pkg/fuse ./pkg/backend ./pkg/server
```

Focused end-to-end suites live under `e2e/`, including FUSE write/read gates,
Git workflow gates, portable pack/unpack, and LayerFS smoke tests.

The Go module path is currently `github.com/mem9-ai/dat9`.

## License

Apache 2.0.
