# drive9-swift

Native Swift Drive9 SDK packaged with SwiftPM.

This package uses `URLSession` directly. It does not require Rust, generated
bindings, C interop, shared libraries, linker flags, or a regeneration step.

## Layout

- `Sources/Drive9Mobile/Drive9.swift` — public SDK surface. `Drive9Client`
  exposes `async throws` methods for the Drive9 v1 HTTP API.
- `Tests/Drive9MobileTests/` — smoke tests against an in-process
  `MockHTTPServer`.

## Supported APIs

- `Drive9Client(baseUrl, apiKey)`
- `Drive9Client.defaultClient()`, `withSmallFileThreshold`, `baseUrl`, `apiKey`
- `write`, `writeWithRevision`, `read`, `uploadFile`, `downloadFile`
- `list`, `stat`, `statMetadata`, `delete`, `copy`, `rename`, `mkdir`
- `grep`, `find`, `sql`
- `downloadStream`, `downloadRangeStream`, `uploadStream`
- `newStreamUpload` / `Drive9StreamUpload` for v2 multipart stream upload
- v1/v2 multipart file upload, v1 resume upload, and path-based `patchFileParts`
- vault secret, token, audit, readable-secret, and read-field APIs

`baseUrl` may include a trailing slash. If `apiKey` is non-empty, requests send
`Authorization: Bearer <apiKey>`; an empty key sends no authorization header.

`statMetadata` returns enriched file metadata - size, isDir, resourceId,
revision, mtime, contentType, semanticText, and tags - by calling
`GET /v1/fs/{path}?stat=1`.

## Parity notes

- The SDK is native HTTP and does not depend on Rust FFI.
- `sql` returns each row as a JSON string, while `drive9-rs` returns
  `serde_json::Value`.
- Vault dynamic JSON fields use `[String: Any]`; audit `detail` is exposed as a
  JSON string.
- Transfer progress/cancel is supported on file/stream APIs. A cancelled
  operation throws `Drive9Exception.Drive9(code: "cancelled", ...)`.

## Test

```bash
swift test
```

The tests cover the native HTTP contract, path/query/header behavior, file
upload/download temp-file handling, conflict/error mapping, stream upload,
download stream, patch upload, and vault read APIs.
