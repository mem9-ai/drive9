# drive9 Go SDK vs CLI parity notes

This document compares the current Go SDK (`pkg/client`) with the `drive9`
CLI surface. The SDK is strongest at typed server API calls. The CLI adds a
large amount of local workflow orchestration around those calls: config files,
mount lifecycle, local filesystem traversal, process execution, output
formatting, and safety checks.

The Go module path is `github.com/mem9-ai/drive9`, so SDK imports use:

```go
import drive9 "github.com/mem9-ai/drive9/pkg/client"
```

## Current SDK coverage

The Go SDK has typed coverage for these server-side surfaces:

- Filesystem CRUD: write, read, stat, list, mkdir, chmod, delete, recursive
  remove, copy, rename, symlink, hardlink.
- Bulk filesystem calls: batch stat and batch read-small.
- Transfer engine: direct PUT, multipart v2/v1 fallback, resume, append,
  streaming multipart writer, range reads, resolved read targets, parallel
  download helper, partial patch.
- Metadata and search: enriched stat, compatibility stat fallback, grep, find,
  SQL passthrough.
- Filesystem-scoped tokens: issue, revoke by token id, revoke by API key.
- Vault API: secret create/update/delete/list/read, grant issue/revoke, audit,
  delegated read helpers, legacy vault token helpers still present.
- Server-sent events: filesystem change watch with reconnect lifecycle hooks.
- LayerFS API: create/list/get layer, diff/replay, entry upsert/read, file
  upload/read, checkpoint, events, rollback, commit.
- Git workspace API: workspace metadata, tree, git state, object packs, overlay
  entries.
- Journal API: create, append entries, read entries, search, verify.
- Quota API: owner-key quota query, TiDB Cloud credential query, and
  credential-based quota update.
- Escape hatch: `RawGet`, `RawPost`, and `RawDelete` for endpoints that do not
  yet have a typed SDK method, including provisioning endpoints.

The compile-tested examples under `examples/go-sdk-cookbook` cover every
exported `*client.Client` method and every exported `StreamWriter` method.

## Gaps relative to CLI workflows

| CLI area | CLI capability | SDK status | Gap |
| --- | --- | --- | --- |
| `ctx` | `ctx show/add/import/fork/ls/use/rm`, context file storage, active context resolution, local JWT decode for UX | Not in `pkg/client` | SDK callers must pass server URL and credential explicitly or implement their own config resolver. |
| env credential resolution | `DRIVE9_API_KEY`, `DRIVE9_SERVER`, delegated token handling, env-unset mitigation | CLI-only (`cmd/drive9/cli`) | No SDK `NewFromEnv` package-level helper. |
| provisioning | `drive9 create`, `drive9 delete`, TiDB Cloud credential handling, local context registration/cleanup | Only possible via `RawPost("/v1/provision")` and `RawDelete("/v1/tenant")` | No typed provision/deprovision request/response helpers in the SDK. |
| regions | `region list`, manifest fetch/validation/fallback, region-code to server selection | CLI-only | No typed region manifest client or server-selection helper in SDK. |
| mount | `mount`, `mount vault`, `umount`, foreground/background process management, FUSE/WebDAV options, mount state files | Outside `pkg/client`; implemented through CLI + `pkg/fuse`/mount state packages | SDK is HTTP-only and does not expose a local mount workflow. |
| doctor | local FUSE/WebDAV/runtime diagnostics | CLI-only | No SDK diagnostics package. |
| update | self-update download, verification, binary replacement | CLI-only | Not a server SDK concern. |
| `fs cp` | local-to-remote, remote-to-local, remote-to-remote, append mode, recursive traversal, mode preservation, progress output | SDK has transfer/read/write/copy/list/stat primitives | No one-call SDK equivalent for CLI `cp` or `cp -r`. |
| `fs cat` | stdout streaming plus offset/length flag parsing | SDK has `ReadStream` and `ReadStreamRange` | No output/flag wrapper. |
| `fs ls/stat/rm/mkdir/chmod/mv` | command-line parsing, remote context syntax, human/JSON output | SDK has underlying typed calls | No CLI-style formatter or `context:/path` resolver in SDK. |
| `fs grep/find` | command-line grammar, tag flag parsing, layer flag parsing | SDK has `Grep`, `GrepWithLayer`, `Find` | No SDK parser for CLI flags or repeated `-tag` UX. |
| `fs sh` | interactive shell against remote filesystem | CLI-only orchestration | No SDK shell abstraction. |
| `fs layer` | CLI status/diff/checkpoint/rollback/commit UX and layer-aware write helpers for cp/mkdir/chmod/rm/mv | SDK has typed LayerFS API | No high-level layer-aware filesystem command helpers. |
| `pack` / `unpack` | local archive creation/extraction, profile-aware include/exclude rules, mounted-path discovery | SDK only reads/writes archive objects | No SDK pack/unpack workflow or profile parser. |
| `profile` | built-in and user profile loading/formatting | CLI-only | No SDK profile API. |
| `token issue/revoke` | `--allow prefix:ops` grammar, TTL parsing, local context save, rollback revoke if save fails, stdin/file revoke | SDK has issue/revoke API methods | No SDK helper for CLI grammar or local context persistence. |
| `quota get/set` | owner-key quota query, TiDB Cloud credential query, TiDB Cloud-only quota update, human/JSON output, local credential resolution | SDK has typed quota request/response helpers | No CLI-style formatter or local context/env credential resolver in the SDK. |
| `vault set/get/put/with/ls/rm/grant/revoke/audit` | path grammar, field parsing (`field=value`, `@file`, stdin), dotenv/JSON output, `put --from`, secure env injection into child process | SDK has raw vault API methods | No SDK helper for CLI UX, batch directory import, or `vault with` process execution. |
| `journal new/append/cat/find/verify` | flag parsing, JSONL/stdin parsing, idempotency-key defaults, human/JSON output | SDK has journal API methods | No SDK helper for CLI input/output formats. |
| `git clone --fast` | local `git` orchestration, worktree setup, GitHub tree-size enrichment, local `.git` archiving, hydration | SDK has server-side git workspace/tree/state/object/overlay APIs | No SDK one-shot fast-clone workflow. |
| remote context syntax | `ctxname:/path` arguments in fs commands | CLI-only | SDK methods accept already-resolved server/client and drive9 path. |

## SDK ergonomics gaps not strictly tied to CLI

- `SQL`, `Grep`, `GrepWithLayer`, and `Find` do not currently have `Ctx`
  variants, unlike most filesystem methods.
- There is no typed `Status` method that returns `/v1/status`; callers can
  access cached threshold values through `Warm`, `MaxUploadBytes`, and
  `SmallFileThreshold`.
- `RawGet`, `RawPost`, and `RawDelete` exist, but there is no `RawPut` or
  context-aware raw helper family.
- The SDK has no public config package for sharing CLI contexts with embedded
  Go programs. This is deliberate today, but it is the largest integration gap
  for applications that want "behave like drive9 CLI" credential discovery.

## Recommended next SDK additions

1. Add a small config/credentials package outside `cmd/drive9/cli`:
   `LoadConfig`, `ResolveCredentials`, `NewFromEnv`, and `NewFromContext(name)`.
2. Add typed provision/deprovision helpers for `/v1/provision` and `/v1/tenant`.
3. Add context-aware variants for `SQL`, `Grep`, `GrepWithLayer`, and `Find`.
4. Add high-level recursive copy helpers that mirror the CLI's local/remote
   matrix without pulling in command-line output behavior.
5. Add a vault convenience layer for `put --from` and `field=@file` parsing,
   but keep `vault with` process execution CLI-only unless there is a clear
   embedding use case.
6. Add a Git fast-clone workflow package only if Go callers need the local git
   orchestration; the current SDK already covers the server-side records.
