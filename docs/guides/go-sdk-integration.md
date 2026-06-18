# drive9 Go SDK integration guide

This guide shows how to integrate the drive9 Go SDK from another Go program,
how to run a local SDK smoke test, and which SDK calls to use for common
filesystem workflows.

For CLI parity analysis, see
[`docs/guides/go-sdk-cli-parity.md`](go-sdk-cli-parity.md).

The public project name is drive9. The current Go module path is still
`github.com/mem9-ai/dat9`, so Go imports must use `github.com/mem9-ai/dat9/pkg/client`
until the module path changes.

## Prerequisites

- Go 1.25.1 or newer.
- A reachable drive9 server URL.
- A tenant owner API key or an `fs_scoped` API key for filesystem operations.
- Either set `DRIVE9_SERVER`/`DRIVE9_API_KEY`, or have a usable owner/fs_scoped
  current context in `~/.drive9/config`.
- For large-file uploads, the source reader must be seekable (`io.ReaderAt`).
  `*bytes.Reader`, `*strings.Reader`, and `*os.File` all satisfy this.

## Install

In an external Go module:

```bash
go get github.com/mem9-ai/dat9
```

Import the SDK package with an alias that matches the product name:

```go
import drive9 "github.com/mem9-ai/dat9/pkg/client"
```

## Create a client

Use `client.New` for owner API keys and filesystem-scoped API keys.

```go
package main

import (
	"context"
	"os"
	"time"

	drive9 "github.com/mem9-ai/dat9/pkg/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := drive9.New(os.Getenv("DRIVE9_SERVER"), os.Getenv("DRIVE9_API_KEY"))

	// Optional, but recommended before uploads. This fetches /v1/status once
	// so the SDK knows the server's inline-vs-multipart threshold.
	c.Warm(ctx)
}
```

Use `client.NewWithToken` only when you intentionally hold a delegated
capability token, such as a vault JWT. Filesystem commands normally use
`client.New`.

## Run the included smoke test

This repository includes a minimal external-style example under
`examples/go-sdk-basic`.

There is also a compile-tested cookbook under `examples/go-sdk-cookbook`.
It covers every exported `*client.Client` method and every exported
`StreamWriter` method, with a reflection test that fails when a new SDK method
is added without an example coverage entry.

Compile and run its local SDK smoke test. This uses `httptest`; it does not
need a live drive9 server.

```bash
go test ./examples/go-sdk-basic
go test ./examples/go-sdk-cookbook
```

Run the same example against a live server:

```bash
export DRIVE9_SERVER=https://drive9.example.com
export DRIVE9_API_KEY=...

go run ./examples/go-sdk-basic
```

If those environment variables are unset, `go-sdk-basic` reads the current
owner or `fs_scoped` context from `~/.drive9/config`. `DRIVE9_CONFIG` can point
at an alternate config file for tests.

Optional:

```bash
export DRIVE9_SDK_ROOT=/sdk-go-basic-manual/
```

If `DRIVE9_SDK_ROOT` is omitted, the example creates a unique scratch
directory and removes it before exit.

## Paths

drive9 paths are absolute UTF-8 paths.

- Directories end with `/`.
- Files do not end with `/`.
- Do not use backslashes or `..` segments.
- Prefer building paths in one place and keeping the file/directory distinction
  explicit.

```go
root := "/agents/run-42/"
readme := root + "README.md"
```

## Basic filesystem operations

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

c := drive9.New(serverURL, apiKey)
c.Warm(ctx)

if err := c.MkdirCtx(ctx, "/agents/run-42/", 0o755); err != nil {
	return err
}

if err := c.WriteCtx(ctx, "/agents/run-42/README.md", []byte("hello drive9\n")); err != nil {
	return err
}

data, err := c.ReadCtx(ctx, "/agents/run-42/README.md")
if err != nil {
	return err
}
_ = data

entries, err := c.ListCtx(ctx, "/agents/run-42/")
if err != nil {
	return err
}
_ = entries

if err := c.DeleteCtx(ctx, "/agents/run-42/README.md"); err != nil {
	return err
}
```

Use the context-aware methods in services and CLIs. The non-`Ctx` methods are
convenience wrappers around `context.Background()`.

## Uploads

For small byte slices, `WriteCtx` is the simplest API. For production upload
paths, prefer `WriteStreamWithSummary` because it works for direct PUT and
multipart uploads and returns timing details.

```go
payload := []byte("drive9 sdk upload\n")
summary, err := c.WriteStreamWithSummary(
	ctx,
	"/agents/run-42/payload.txt",
	bytes.NewReader(payload),
	int64(len(payload)),
	func(part, total int, uploaded int64) {
		// Called after each multipart part. Direct PUT uploads may not call it.
	},
	drive9.WithTags(map[string]string{
		"owner": "agent-a",
		"kind":  "note",
	}),
	drive9.WithDescription("agent handoff note"),
)
if err != nil {
	return err
}
_ = summary.Mode // direct_put, multipart_v2, or multipart_v1
```

Large uploads require a seekable source:

```go
f, err := os.Open("large.bin")
if err != nil {
	return err
}
defer f.Close()

info, err := f.Stat()
if err != nil {
	return err
}

summary, err := c.WriteStreamWithSummary(ctx, "/artifacts/large.bin", f, info.Size(), nil)
if err != nil {
	return err
}
_ = summary
```

When the client has not successfully fetched `/v1/status`, non-empty uploads
fail safe through multipart instead of assuming a hard-coded inline threshold.
Call `Warm`, `SmallFileThreshold`, or `MaxUploadBytes` during startup if you
want the first upload to use the server-advertised threshold.

## Downloads

For small files or simple callers:

```go
data, err := c.ReadCtx(ctx, "/artifacts/result.json")
```

For streaming:

```go
rc, err := c.ReadStream(ctx, "/artifacts/large.bin")
if err != nil {
	return err
}
defer rc.Close()

_, err = io.Copy(dst, rc)
```

For local files with parallel range reads when possible:

```go
summary, err := c.DownloadToFileWithSummary(ctx, "/artifacts/large.bin", "./large.bin", size)
if err != nil {
	return err
}
_ = summary
```

For repeated range reads against the same large object, resolve the read target
once and reuse it until it expires:

```go
target, err := c.ResolveReadTarget(ctx, "/artifacts/large.bin")
if err != nil {
	return err
}

part, err := c.ReadObjectRange(ctx, target, 0, 8<<20)
if err != nil {
	if drive9.IsPresignExpired(err) {
		target, err = c.ResolveReadTarget(ctx, "/artifacts/large.bin")
	}
}
if part != nil {
	defer part.Close()
}
```

## Metadata

Use `StatCtx` for lightweight HEAD metadata:

```go
stat, err := c.StatCtx(ctx, "/agents/run-42/README.md")
if err != nil {
	return err
}
_ = stat.Revision
_ = stat.Size
_ = stat.Mode
```

Use `StatMetadataCompatCtx` when you need tags, content type, semantic text, or
a compatibility fallback for older servers:

```go
meta, err := c.StatMetadataCompatCtx(ctx, "/agents/run-42/README.md")
if err != nil {
	return err
}
_ = meta.Tags
_ = meta.SemanticText
_ = meta.Degraded // true when the SDK had to fall back to legacy HEAD stat.
```

## Batch calls

Batch calls avoid one HTTP request per file for common agent workflows.

```go
stats, err := c.BatchStatCtx(ctx, []string{
	"/agents/run-42/README.md",
	"/agents/run-42/config.json",
})
if err != nil {
	return err
}
for _, st := range stats {
	if !st.OK() {
		continue
	}
}

smallFiles, err := c.BatchReadSmallCtx(ctx, []string{
	"/agents/run-42/README.md",
	"/agents/run-42/config.json",
}, 1<<20)
if err != nil {
	return err
}
_ = smallFiles
```

Limits:

- `BatchStatCtx`: up to `client.MaxBatchStatPaths`.
- `BatchReadSmallCtx`: up to `client.MaxBatchReadSmallPaths`.

Per-path failures are returned inside each result. Transport or malformed
response errors fail the whole method.

## Search and find

Semantic/text search:

```go
matches, err := c.Grep("deployment checklist", "/agents/", 20)
if err != nil {
	return err
}
_ = matches
```

Find with structured query parameters:

```go
params := url.Values{}
params.Set("name", "*.md")
params.Set("tag", "kind=note")

matches, err := c.Find("/agents/", params)
if err != nil {
	return err
}
_ = matches
```

Tag matching semantics:

- `tag=key=value` is an exact key/value match.
- `tag=key` checks whether the tag key exists.
- Tags do not support fuzzy, prefix, contains, or regex matching.

## Compare-and-set writes

Use revision-guarded writes when multiple agents may update the same file.

```go
stat, err := c.StatCtx(ctx, "/agents/run-42/state.json")
if err != nil {
	return err
}

err = c.WriteCtxConditional(ctx, "/agents/run-42/state.json", nextState, stat.Revision)
if err != nil {
	if errors.Is(err, drive9.ErrConflict) {
		// Re-read, merge, and retry.
	}
	return err
}
```

Expected revision semantics:

- `-1`: unconditional write.
- `0`: create only if the path does not exist.
- Positive revision: update only if the current file revision matches.

Use `WriteCtxConditionalWithRevision` when you need the committed revision from
the response.

## Append

Use `AppendStream` for log-like writes:

```go
line := []byte("step completed\n")
err := c.AppendStream(ctx, "/agents/run-42/events.log", bytes.NewReader(line), int64(len(line)), nil)
if err != nil {
	return err
}
```

The SDK uses the server append path when available and falls back to a safe
rewrite path for older servers or unsupported storage shapes.

## Copy, rename, links, and permissions

```go
_ = c.CopyCtx(ctx, "/agents/run-42/README.md", "/agents/run-42/README-copy.md")
_ = c.RenameCtx(ctx, "/agents/run-42/README-copy.md", "/agents/run-42/README-final.md")
_ = c.SymlinkCtx(ctx, "README-final.md", "/agents/run-42/latest")
_ = c.HardlinkCtx(ctx, "/agents/run-42/README-final.md", "/agents/run-42/README-hardlink.md")
_ = c.ChmodCtx(ctx, "/agents/run-42/README-final.md", 0o640)
```

Use `DeleteFileCtx` and `DeleteDirCtx` when you want the server to enforce a
file-vs-directory delete hint. Use `RemoveAllCtx` for recursive cleanup.

## Error handling

The SDK returns `*client.StatusError` for HTTP API errors.

```go
_, err := c.ReadCtx(ctx, "/missing.txt")
if err != nil {
	if drive9.IsNotFound(err) {
		// Handle 404.
		return nil
	}

	var statusErr *drive9.StatusError
	if errors.As(err, &statusErr) {
		switch statusErr.StatusCode {
		case http.StatusUnauthorized:
			// Bad or expired credential.
		case http.StatusForbidden:
			// Credential lacks permission.
		case http.StatusConflict:
			// CAS conflict or path conflict.
		}
	}
	return err
}
```

`errors.Is(err, drive9.ErrConflict)` also matches HTTP 409 status errors.

## Scoped filesystem tokens

Owner clients can issue narrower filesystem-scoped tokens:

```go
issued, err := c.IssueScopedToken(ctx, drive9.IssueScopedTokenRequest{
	Subject:    "agent-a",
	TTLSeconds: int64((30 * time.Minute).Seconds()),
	Scopes: []drive9.FSScopeGrant{
		{Prefix: "/agents/run-42/", Ops: []string{"read", "write"}},
	},
})
if err != nil {
	return err
}

agentClient := drive9.New(serverURL, issued.Token)
```

Revoke by token id:

```go
err = c.RevokeScopedToken(ctx, issued.TokenID)
```

Or revoke by presenting the target API key to an owner-authenticated client:

```go
err = c.RevokeScopedTokenByAPIKey(ctx, issued.Token)
```

## Vault helpers

Owner API key clients can manage vault secrets:

```go
secret, err := c.CreateVaultSecret(ctx, "prod-db", map[string]string{
	"DB_URL": "postgres://...",
})
if err != nil {
	return err
}
_ = secret

fields, err := c.ReadVaultSecretAsOwner(ctx, "prod-db")
if err != nil {
	return err
}
_ = fields
```

Delegated vault JWTs should use `NewWithToken`:

```go
vaultClient := drive9.NewWithToken(serverURL, delegatedJWT)
value, err := vaultClient.ReadVaultSecretField(ctx, "prod-db", "DB_URL")
```

## Events

Use server-sent events to invalidate caches or react to remote filesystem
changes. Cancel the context to stop the watcher.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go c.WatchEventsWithLifecycle(ctx, "agent-a", func(change *drive9.ChangeEvent, reset *drive9.ResetEvent) {
	if change != nil {
		// change.Path, change.Op, change.Seq
	}
	if reset != nil {
		// Drop local caches and resync.
	}
}, drive9.EventLifecycle{
	OnCurrent: func(seq uint64) {
		// Stream has caught up through seq.
	},
	OnDisconnected: func(err error) {
		// SDK reconnects with backoff unless ctx is canceled.
	},
})
```

## Layer filesystem API

The SDK also exposes `CreateFSLayer`, `UpsertFSLayerEntry`, `DiffFSLayer`,
`CheckpointFSLayer`, `RollbackFSLayer`, and `CommitFSLayer` for agent sandbox
workflows that need an overlay before committing to the base filesystem. Use
the ordinary `/v1/fs` methods when you do not need an explicit layer.

## Testing integrations

For unit tests in your own application, use `httptest` to assert the request
shape and SDK behavior without requiring a live server. The repository example
does exactly this:

```bash
go test ./examples/go-sdk-basic
```

For live smoke testing:

```bash
DRIVE9_SERVER=https://drive9.example.com \
DRIVE9_API_KEY=... \
go run ./examples/go-sdk-basic
```

Expected output shape:

```text
root: /sdk-go-basic-...
file: /sdk-go-basic-.../hello.txt
upload_mode: direct_put
revision: <server revision>
size: 28
entries: 1
batch_status: 200
search_results: 0
```

`search_results` may be `0` on a real deployment if semantic indexing is still
catching up or the embedding backend is disabled.

## Operational notes

- Always pass request-scoped contexts with timeouts or cancellation.
- Call `Warm` once before upload-heavy workflows to avoid first-upload threshold
  ambiguity.
- Keep API keys out of logs and command arguments. Prefer environment variables
  or a secret manager.
- The SDK sends credentials as `Authorization: Bearer <token>`.
- Cross-host redirects strip `Authorization` and `X-Dat9-Actor` before following
  the redirect.
- `StatusError.Message` is server-provided when the server returns a structured
  error body.
