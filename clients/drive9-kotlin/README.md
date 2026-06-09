# drive9-kotlin

Native Kotlin/JVM/Android Drive9 SDK.

This package uses platform HTTP (`HttpURLConnection`) directly. It does not
require Rust, generated bindings, shared libraries, or a regeneration step.

## Layout

- `lib/src/main/kotlin/com/drive9/mobile/Drive9.kt` — public SDK surface.
  `Drive9Client` exposes coroutine-based methods and runs blocking HTTP/file
  work on `Dispatchers.IO`.
- `lib/src/test/kotlin/com/drive9/mobile/Drive9Test.kt` — JVM smoke tests
  against an in-process `HttpServer`.

## Supported APIs

- `Drive9Client(baseUrl, apiKey)`
- `Drive9Client.defaultClient()`, `withSmallFileThreshold`, `baseUrl`, `apiKey`
- `write`, `writeWithRevision`, `read`, `uploadFile`, `downloadFile`
- `list`, `stat`, `statMetadata`, `delete`, `copy`, `rename`, `mkdir`
- `grep`, `find`, `sql`
- `downloadFlow`, `downloadRangeFlow`, `uploadFlow`
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
- Vault JSON fields use `kotlinx.serialization.json.JsonElement`.
- Transfer progress/cancel is supported on file/flow APIs. A cancelled operation
  throws `Drive9Exception.Drive9(code = "cancelled", ...)`.

## Test

```bash
gradle test --no-daemon --console=plain
```

The tests cover the native HTTP contract, path/query/header behavior, file
upload/download temp-file handling, conflict/error mapping, stream upload,
download flow, patch upload, and vault read APIs.
