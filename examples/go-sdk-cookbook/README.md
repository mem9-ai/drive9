# drive9 Go SDK cookbook

This package contains compile-tested examples for the drive9 Go SDK.

Run:

```bash
go test ./examples/go-sdk-cookbook
```

The examples are grouped by SDK surface:

- construction, status warmup, raw requests
- filesystem CRUD, metadata, search, SQL
- uploads, downloads, append, patch, streaming writer
- scoped tokens and vault APIs
- events, LayerFS, Git workspace, journal APIs
- error helper functions and local file transfer shape

`TestClientMethodExampleCoverage` uses reflection to fail when a new exported
`*client.Client` method is added without a cookbook coverage entry.
`TestStreamWriterMethodExampleCoverage` does the same for `StreamWriter`.

The Example functions intentionally do not include `Output:` comments. Go
therefore compiles them without executing network calls. For an executable
local smoke test backed by `httptest`, use `../go-sdk-basic`.
