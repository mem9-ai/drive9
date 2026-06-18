# Drive9 Go SDK Smoke Demo

This demo exercises the current Go SDK (`pkg/client`) without going through the
CLI. It has two modes:

```bash
# Deterministic local mock server; no credentials required.
go run ./examples/go-sdk-smoke -mock

# Live Drive9 server.
DRIVE9_BASE_URL=https://your-drive9.example \
DRIVE9_API_KEY=drive9_xxx \
go run ./examples/go-sdk-smoke -root /go-sdk-smoke-$(date -u +%Y%m%dT%H%M%SZ)
```

The smoke run validates:

- Bearer authentication and `X-Dat9-Actor`.
- `/v1/status` warmup and cached inline threshold behavior.
- Small-file create-CAS, revision, `Stat`, `Read`, `ReadAt`, conflict errors, and `WriteStreamWithSummary` direct-PUT threshold routing.
- `BatchStatCtx` and `BatchReadSmallCtx` per-path success/error results.
- `StatMetadataCompatCtx`, copy, rename, chmod, list, and recursive cleanup.
- Large `WriteStreamWithSummaryAndTags` using multipart V2.
- `PatchFile` with explicit part size and expected revision.
- Presigned patch checksum-header behavior.

Customer-facing usage note: see `CUSTOMER_README.md`.

Run the mock regression directly:

```bash
go test ./examples/go-sdk-smoke
```
