# Local object-store configuration

Local E2E, SDK integration tests, blackbox and `make run-server-local` use
`scripts/local-object-store.sh`. Native object-store CLI/FUSE smoke uses the
same `DRIVE9_OBJECT_STORE_*` settings. The default service is RustFS 1.0.3,
pinned by its multi-platform image digest, with no alternate image.

```sh
DRIVE9_S3_BACKEND=s3 make e2e-local   # require the real S3 service
DRIVE9_S3_BACKEND=mock make e2e-local # use the local filesystem mock
bash scripts/local-object-store.sh status
bash scripts/local-object-store.sh stop
```

`DRIVE9_S3_BACKEND=auto` remains the local development default: try the real
object store, then allow the mock. CI uses `s3`, so a startup failure fails
its checks. An existing `DRIVE9_S3_BUCKET` keeps the caller's S3 configuration.
Production `DRIVE9_S3_*` settings are unchanged.

| Setting | Default / purpose |
| --- | --- |
| `DRIVE9_OBJECT_STORE_IMAGE` | Pinned `rustfs/rustfs:1.0.3` digest |
| `DRIVE9_OBJECT_STORE_FALLBACK_IMAGE` | Empty; optional alternate compatible image |
| `DRIVE9_OBJECT_STORE_BINARY` | `rustfs`; executable name or absolute path for native fallback |
| `DRIVE9_OBJECT_STORE_DOWNLOAD_URL` | Unset; optional direct executable download when the binary is unavailable |
| `DRIVE9_OBJECT_STORE_PORT` / `BIND` | `19000` / `127.0.0.1` (native smoke always binds loopback) |
| `DRIVE9_OBJECT_STORE_ACCESS_KEY` / `SECRET_KEY` | `drive9local` / `drive9local`; disposable development values |
| `DRIVE9_OBJECT_STORE_BUCKET` | `drive9-local` for server bootstrap; `drive9-e2e` for native smoke |
| `DRIVE9_OBJECT_STORE_CONTAINER` | `drive9-local-object-store` for the reusable server |
| `DRIVE9_OBJECT_STORE_PID_FILE` / `DATA_DIR` | Temporary-directory paths for native server state |
| `DRIVE9_OBJECT_STORE_HEALTH_PATH` | `/health/ready`; override for another compatible service |
| `DRIVE9_OBJECT_STORE_HEALTH_TIMEOUT_S` | `40`, per-candidate readiness budget |
| `DRIVE9_OBJECT_STORE_HEALTH_CONNECT_TIMEOUT_S` | `2`, connection timeout |
| `DRIVE9_OBJECT_STORE_HEALTH_PROBE_TIMEOUT_S` | `5`, one-shot reuse/status probe |

The launch command is `server DATA --address HOST:PORT` for native binaries,
or `server /data` for containers. The launchers supply the RustFS credential
environment keys and the MinIO credential keys for compatible alternate
images. Those keys belong to the launched servers; configure this test
harness through `DRIVE9_OBJECT_STORE_ACCESS_KEY` / `SECRET_KEY`.

## Migration

Use `local-object-store.sh` and `make test-object-store-bootstrap`. The old
script names and `make test-minio-bootstrap` remain deprecated delegates.
`DRIVE9_S3_BACKEND=minio` remains an alias for `s3`.

The old `DRIVE9_MINIO_*`, `MINIO_IMAGE`, `MINIO_PORT`, `MINIO_BUCKET`,
`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_DOWNLOAD_URL` and
`MINIO_HEALTH_*` settings are accepted with warnings. New generic settings
win even when empty; in particular, an empty new fallback image overrides a
legacy nonempty fallback. Legacy callers retain the `minio` binary fallback
and `/minio/health/live` probe, local credential defaults and state paths
unless they explicitly set the new settings.

Stop an old default-named MinIO service before switching its port to RustFS:

```sh
DRIVE9_OBJECT_STORE_CONTAINER=drive9-local-minio \
DRIVE9_OBJECT_STORE_PID_FILE="${TMPDIR:-/tmp}/drive9-local-minio.pid" \
  bash scripts/local-object-store.sh stop
```

This changes local test/development configuration only. It does not convert
an existing object's on-disk storage format.
