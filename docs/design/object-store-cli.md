# Object-store access from the drive9 CLI

**Status:** implemented
**Date:** 2026-08-24
**Audience:** operators and users of `drive9 fs` / `drive9 mount`

---

## What you can do

`drive9 fs` and `drive9 mount` can talk to an object store the same way they talk
to drive9. Point them at a URI; the CLI chooses the backend.

Supported URI schemes:

| Scheme | Store |
| --- | --- |
| `s3://` | Amazon S3 and S3-compatible endpoints (including MinIO) |
| `cos://` | Tencent COS |
| `tos://` | Volcengine TOS |
| `oss://` | Alibaba OSS |
| `gs://` / `gcs://` | Google Cloud Storage |
| `az://` / `azure://` | Azure Blob |

Examples:

```bash
drive9 fs ls s3://bucket/prefix/
drive9 fs cat s3://bucket/prefix/readme.txt
drive9 fs cp ./local.txt s3://bucket/prefix/local.txt
drive9 fs cp s3://bucket/a.txt :/dst.txt
drive9 fs rm s3://bucket/prefix/local.txt
drive9 mount s3://bucket/prefix/ ./mnt
```

`:/path` and `drive9://…` stay on the drive9 filesystem. Local paths stay local.
An `s3://` URI is never treated as a local filename.

Object-store directories are prefixes. Listing, copy, and mount emulate a
folder tree on top of keys. There is no POSIX chmod/chown, and listing is
billed by the object store.

---

## Authentication (the important part)

There are two modes. **Server mint is the default.**

### Default: `--auth=server` (omit the flag)

The CLI asks drive9-server for short-lived credentials, then the **laptop talks
to the object store directly**. File bytes do not pass through drive9-server.

That only works after an org admin has configured:

1. An **object backend** for the bucket (the high-privilege identity).
2. An **object namespace** for this tenant (the customer’s own prefix id).

If either is missing, the command fails closed. A tenant API key cannot change
the mapping; it can only request a minted session.

Minted credentials are scoped to that tenant’s prefix. A key minted for
`s3://bucket/acme-prod/` cannot list or write `s3://bucket/acme-other/`.

Reads get a read-only session. Writes, deletes, mkdir, and a writable mount
get a write session.

Server mint covers **S3, COS, TOS, OSS, GCS, and Azure**.

- **S3** (and S3-compatible STS): `GetFederationToken` for static keys,
  `AssumeRole` for a role. `--sts-endpoint` overrides the STS URL (MinIO STS,
  custom gateways).
- **COS**: Tencent CAM `GetFederationToken` (static) or `AssumeRole`. Set
  `--account-id` to the APPID, or use a `bucket-appid` name.
- **TOS / OSS**: Volcengine / Aliyun **AssumeRole** only (`--role-arn` is
  required). Those clouds have no AWS-style federation token.
- **GCS**: server holds a service-account JSON and mints a short-lived OAuth
  access token. The token has the **same IAM as that service account** — it is
  not downscoped to one bucket. Register one SA per bucket (or an SA that can
  only access that bucket). Namespace must match the bucket; backend prefix
  must be empty.
- **Azure**: server holds the storage account key and mints a container SAS
  cryptographically scoped to that container. Isolation is **one container per
  tenant** (namespace must match the container; backend prefix must be empty).

### Escape hatch: `--auth=local`

Use the cloud’s native credential chain on this machine (environment variables,
shared config, instance role, gcloud/Azure CLI, and so on). drive9-server is
not involved.

```bash
drive9 fs ls --auth=local s3://bucket/prefix/
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  drive9 fs cp --auth=local ./file s3://minio-bucket/file?endpoint=http://127.0.0.1:9000&forcePathStyle=true
```

Local auth is for laptops, CI against MinIO, and stores that are not wired up
on the server. It is **not** a silent fallback: if you omit `--auth=local`,
existing AWS/TOS env vars are ignored.

`--auth` is accepted on `fs` commands and on `mount`.

---

## How an org admin sets this up

Admin commands use **TiDB Cloud public/private keys**, the same identity as
`drive9 admin tenant create` and `extract-config`. They sit next to `tenant`
and `pool`, not under a tenant API key.

### 1. Register the bucket

```bash
drive9 admin object-backend add \
  --scheme s3 --bucket example \
  --region us-east-1 \
  --credential-kind static \
  --access-key-id AKI... \
  --secret-access-key ... \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>
```

`--credential-kind role` with `--role-arn` is the other option (optional
access key; otherwise the server’s own IAM is used to assume the role).

An org may register **more than one backend** for the same bucket (different
`--prefix` or `--endpoint`). Mint picks the longest matching prefix; pass
`?endpoint=` on the URI when two endpoints share a bucket name.

`ls` never prints secrets. `get --id` shows one row. `update --id` patches
fields in place — use it to rotate `--secret-access-key` without delete+add.
`rm --id` deletes a backend.

Optional flags: `--name`, `--sts-endpoint`, `--account-id`, `--max-session-ttl`,
`--prefix` (extra path **above** every tenant namespace in that bucket).

### 2. Bind this tenant to a customer prefix

The prefix is the **customer’s own id**, not the drive9 tenant id.

```bash
drive9 admin tenant object-namespace set \
  --tenant-id tnt_xxx \
  --namespace-id acme-prod \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>
```

After this, the tenant may mint sessions only under `s3://example/acme-prod/`.
Empty namespace (the default, or `object-namespace clear`) means mint is
refused.

`get` / `set` / `clear` are the same shape as `admin tenant extract-config`.

Typical tenant command after setup:

```bash
drive9 fs ls s3://example/acme-prod/
drive9 fs cp ./report.pdf s3://example/acme-prod/report.pdf --force
drive9 mount s3://example/acme-prod/ ./mnt
```

---

## Copy and overwrite

Object overwrite is **irreversible**. Unlike drive9, there is no versioning or
soft-delete in this CLI path.

- Copy onto an existing object is refused unless you pass `-f` / `--force`.
- drive9 and local destinations keep their previous overwrite behavior.
- Recursive copy that stops mid-way leaves already-copied keys in place and
  says so.
- Symlinks and `..` path segments are refused on recursive copy.

---

## Mounts

```bash
drive9 mount s3://bucket/prefix/ ./mnt
drive9 mount --auth=local s3://bucket/prefix/ ./mnt
drive9 umount ./mnt
```

- Unix only (macOS needs `--mode=fuse`). Windows object mounts are not
  supported.
- The VFS root is the URI prefix, not the whole bucket.
- Default write-back cache is under `~/.cache/drive9/object`, namespaced per
  URI. Two mounts of the same URI cannot share one cache directory.
  Dirty data is uploaded when the kernel releases the handle (`WriteBack=0`,
  so rclone Close is a synchronous PUT). FUSE `Flush` does not close the
  handle, because the kernel FLUSHes after create and on dup/close before
  later writes. `--allow-other` enables kernel `default_permissions`.
- Umount waits briefly for in-flight uploads and does **not** wipe a dirty
  cache, so a remount of the same URI can resume.
- `mount drain` is drive9-only; object mounts reject it.
- Server-minted mounts keep themselves valid. Each STS/SAS/OAuth session is
  still short-lived (`--max-session-ttl`, default 1 hour; GCS tokens are
  typically ~1 hour), but the mount process remints before expiry and
  refreshes credentials on the **same** rclone Fs (S3/COS/TOS/OSS via rclone's
  `backend set`; GCS/Azure by rotating the token/SAS on the existing HTTP
  client). The Fs pointer and `Name()` stay stable so same-bucket copy/rename
  keep using server-side CopyObject instead of download+upload, and the old
  client is not leaked. A mint failure is retried; the previous session is
  kept until it expires. `--auth=local` does not remint.

---

## How the pieces fit (no file bytes on the server)

```
Admin (TiDB Cloud AK/SK)
  │  register bucket + high-priv key/role
  │  bind tenant → customer prefix id
  ▼
drive9-server (control plane)
  │  mints a short-lived, prefix-scoped session
  ▼
CLI / mount on the laptop
  │  uses that session (or --auth=local)
  ▼
Object store (data plane)
```

drive9-server never proxies object bytes and does not import the object-store
I/O stack. Isolation is the minted session, not a rewrite of `/v1/fs`.

---

## Schema (brief)

Control-plane metadata, not the tenant filesystem database.

**`org_object_backends`** — one row per org identity (scheme + bucket +
prefix + endpoint). Multiple rows per bucket are allowed.

Holds optional name, data-plane endpoint, STS endpoint, region, account id,
optional extra prefix, credential kind (`static` or `role`), role ARN, access
key id, and encrypted secret / external id. Secrets use the same org KMS
wrapping as other control-plane secrets. List APIs return “has secret”, never
the plaintext. `update` rotates the secret in place.

**`tenant_tidbcloud_org_bindings.object_namespace_id`** — customer prefix id
for that tenant. Empty means “object mint not allowed”. This is not a user ACL
table and is not the drive9 `tenant_id`.

Existing databases pick up the new table and column through the usual meta
schema self-repair (add column / create table only).

---

## Limits and non-goals

- Azure SAS isolates at container scope. GCS minted tokens are **not**
  bucket-scoped: isolation is the service account's IAM. Use one dedicated
  SA (or IAM-limited SA) per bucket, and one container/bucket per tenant.
- TOS and OSS mint require a role ARN (AssumeRole). COS static keys use
  GetFederationToken.
- Scoped (workspace-zone) tokens cannot mint object credentials.
- No `?profile=` and no rclone remote names. Credentials are either minted or
  the process-native chain.
- `chmod`, `find`, `grep`, `symlink`, and `hardlink` stay drive9-only.
- Cross-scheme `mv` (object ↔ local/drive9) is rejected; copy then delete
  yourself.
- Object `stat` does not yet surface ETag / Content-Type.
- This is not a replacement for drive9’s own S3 used for large tenant files.
