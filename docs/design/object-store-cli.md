# Object-store access from the drive9 CLI

**Status:** implemented
**Date:** 2026-08-26
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
AWS / TOS / COS environment variables already on the laptop are ignored.

That only works after an org admin has configured:

1. An **object backend** for the bucket — the high-privilege identity Drive9
   uses to mint.
2. An **object namespace** for this tenant — the customer’s own prefix id,
   not the Drive9 tenant id.

If either is missing, the command fails closed. A tenant API key cannot change
the mapping; it can only request a minted session. Scoped (workspace-zone)
tokens cannot mint.

Minted credentials are scoped to that tenant’s prefix. A session minted for
`s3://bucket/acme-prod/` cannot list or write `s3://bucket/acme-other/`, and
cannot list a sibling such as `acme-prod-evil/`. Isolation is both a mint-time
URI check and a vendor session policy (`prefix/` and `prefix/*`).

Reads get a read-only session. Writes, deletes, mkdir, and a writable mount
get a write session. A read that later needs to write remints.

Default session TTL is 1 hour (`--max-session-ttl` on the backend, capped at
12 hours). Server-minted mounts remint before expiry and refresh credentials
on the **same** rclone remote, so copy/rename inside the bucket stay
server-side.

Admin commands use **TiDB Cloud public/private keys**, the same identity as
`drive9 admin tenant create` and `extract-config`. They sit next to `tenant`
and `pool`, not under a tenant API key. Tenant users then use a normal tenant
API key with `drive9 fs` / `drive9 mount`.

The common workflow:

1. In the cloud console, create a bucket and an identity Drive9 can use to
   mint (see the S3 / COS / TOS sections below).
2. Register that identity as an object backend. Secrets are stored encrypted;
   `ls` / `get` never print them. `update --id` rotates a key in place.
3. Bind each Drive9 tenant to a unique prefix id
   (`drive9 admin tenant object-namespace set`). Empty namespace (the default,
   or `object-namespace clear`) means mint is refused.
4. Give the tenant its API key. Users talk to URIs under that prefix — no
   long-lived cloud keys on the laptop.

```bash
drive9 admin object-backend add \
  --scheme s3 --bucket example \
  --region us-east-1 \
  --credential-kind static \
  --access-key-id AKI... \
  --secret-access-key ... \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>

drive9 admin tenant object-namespace set \
  --tenant-id tnt_xxx \
  --namespace-id acme-prod \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>

# tenant API key from here on — omit --auth, or pass --auth=server
drive9 fs ls s3://example/acme-prod/
drive9 fs cp ./report.pdf s3://example/acme-prod/report.pdf --force
drive9 mount s3://example/acme-prod/ ./mnt   # macOS: --mode=fuse
```

Optional backend flags: `--name`, `--sts-endpoint`, `--account-id`,
`--max-session-ttl`, `--endpoint`, `--force-path-style`, `--prefix` (extra
path **above** every tenant namespace in that bucket). An org may register
**more than one backend** for the same bucket (different `--prefix` or
`--endpoint`). Mint picks the longest matching prefix; pass `?endpoint=` on
the URI when two endpoints share a bucket name.

The mint response fills region, endpoint, account id, and path-style from the
registered backend, so tenant URIs usually do not need `?region=` /
`?endpoint=`.

First-wave customer storage is **Amazon S3, Tencent COS, and Volcengine TOS**.
OSS, GCS, and Azure also mint; see [Other schemes](#other-schemes-oss-gcs-azure).

### Amazon S3 (`s3://`)

Drive9 mints an AWS STS session and downscopes it to this tenant’s prefix.
Static keys use `GetFederationToken`. A role uses `AssumeRole`.

**What to prepare in AWS**

1. Create a dedicated bucket (or a prefix you will isolate per tenant).
2. Create an IAM user (recommended for hosted Drive9) whose keys Drive9 will
   hold. Do **not** hand those keys to tenant users.
3. Attach a policy that already allows every S3 action the minted session
   will need on this bucket, plus `sts:GetFederationToken`. STS can only
   **narrow** permissions; it cannot grant what the caller does not have.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:GetFederationToken",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::example",
        "arn:aws:s3:::example/*"
      ]
    }
  ]
}
```

4. Create an access key for that user.

**Register the backend**

```bash
drive9 admin object-backend add \
  --scheme s3 --bucket example \
  --region ap-southeast-1 \
  --credential-kind static \
  --access-key-id AKI... \
  --secret-access-key ... \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>
```

To mint via a role instead: create a role with the S3 permissions above, let
the IAM user (or Drive9’s own instance role, self-hosted only) assume it, and
register `--credential-kind role --role-arn arn:aws:iam::ACCOUNT:role/NAME`.
Hosted Drive9 still needs `--access-key-id` / `--secret-access-key` of a
caller that can assume that role. `--external-id` is supported when the role
trust policy requires it.

S3-compatible endpoints (MinIO and similar): set `--endpoint` and
`--sts-endpoint`, and usually `--force-path-style`.

**Bind the tenant and use it**

```bash
drive9 admin tenant object-namespace set \
  --tenant-id tnt_xxx --namespace-id acme-prod \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>

drive9 fs ls s3://example/acme-prod/
drive9 fs cat s3://example/acme-prod/readme.txt
drive9 fs cp ./file s3://example/acme-prod/file --force
drive9 mount s3://example/acme-prod/ ./mnt
```

A tenant URI outside `acme-prod/` is refused at mint time. A minted session
that is asked to `ListBucket` under another prefix is refused by AWS.

AWS `GetFederationToken` requires a session of at least 15 minutes; leave
`--max-session-ttl` at the default (1 hour) unless you have a reason not to.

### Tencent COS (`cos://`)

Drive9 mints a Tencent CAM STS session. Static keys use `GetFederationToken`.
A role uses `AssumeRole`. Region and APPID are required for mint.

**What to prepare in Tencent Cloud**

1. Create a COS bucket. The bucket name includes the APPID, for example
   `example-1250000000`.
2. Create a CAM user whose SecretId / SecretKey Drive9 will hold. Do **not**
   hand those keys to tenant users.
3. Grant that user `name/sts:GetFederationToken` (or `name/sts:AssumeRole`
   if you will register a role) **and** COS object/bucket actions on this
   bucket. CAM federation, like AWS, can only downscope.

A working CAM policy for static mint looks like:

```json
{
  "version": "2.0",
  "statement": [
    {
      "effect": "allow",
      "action": ["name/sts:GetFederationToken"],
      "resource": ["*"]
    },
    {
      "effect": "allow",
      "action": [
        "name/cos:GetObject",
        "name/cos:HeadObject",
        "name/cos:PutObject",
        "name/cos:PostObject",
        "name/cos:DeleteObject",
        "name/cos:GetBucket",
        "name/cos:HeadBucket",
        "name/cos:InitiateMultipartUpload",
        "name/cos:ListMultipartUploads",
        "name/cos:ListParts",
        "name/cos:UploadPart",
        "name/cos:CompleteMultipartUpload",
        "name/cos:AbortMultipartUpload"
      ],
      "resource": [
        "qcs::cos:ap-beijing:uid/1250000000:example-1250000000",
        "qcs::cos:ap-beijing:uid/1250000000:example-1250000000/*"
      ]
    }
  ]
}
```

Replace region, APPID, and bucket with yours. Create an API key for the CAM
user.

**Register the backend**

```bash
drive9 admin object-backend add \
  --scheme cos --bucket example-1250000000 \
  --region ap-beijing \
  --account-id 1250000000 \
  --credential-kind static \
  --access-key-id AKI... \
  --secret-access-key ... \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>
```

`--region` is required (the COS region, for example `ap-beijing` or
`ap-guangzhou`). `--account-id` is the numeric APPID; if you omit it, Drive9
takes it from a `bucket-appid` name. You do not need `--endpoint` for public
COS: when region is set, the CLI uses `https://cos.<region>.myqcloud.com`.

Role mint: `--credential-kind role --role-arn qcs::cam::uin/<UIN>:roleName/<NAME>`
plus the CAM user’s SecretId / SecretKey (COS mint always needs those keys).

**Bind the tenant and use it**

```bash
drive9 admin tenant object-namespace set \
  --tenant-id tnt_xxx --namespace-id acme-prod \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>

drive9 fs ls cos://example-1250000000/acme-prod/
drive9 fs cp ./file cos://example-1250000000/acme-prod/file --force
drive9 mount cos://example-1250000000/acme-prod/ ./mnt
```

Use the `cos://` scheme, not `s3://`, even though the wire protocol is
S3-compatible. A wrong region or APPID fails at mint. Listing is authorized
on the tenant prefix (so `acme-prod/` cannot list `acme-prod-evil/`).

### Volcengine TOS (`tos://`)

Volcengine has no AWS-style federation token. Drive9 **always** mints with
`AssumeRole`. You must register both a **RoleTrn** and the permanent Access
Key of an IAM user that is allowed to assume that role.

**What to prepare in Volcengine**

1. Enable TOS and create a bucket in a region (for example `cn-beijing`).
2. Create an IAM role with TOS read/write on that bucket
   (`tos:GetObject`, `tos:PutObject`, `tos:DeleteObject`, `tos:ListBucket`,
   and the multipart actions). Note the RoleTrn:
   `trn:iam::<account-id>:role/<role-name>`.
3. Create an IAM user whose access key Drive9 will hold. Allow that user to
   `sts:AssumeRole` on the role (the role’s trust policy must accept this
   user). Do **not** hand those keys to tenant users.

**Register the backend**

```bash
drive9 admin object-backend add \
  --scheme tos --bucket example \
  --region cn-beijing \
  --credential-kind role \
  --role-arn trn:iam::2100000000:role/drive9-object \
  --access-key-id AKLT... \
  --secret-access-key ... \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>
```

`--region` is required. `--role-arn` is required. Static-only registration
(`--credential-kind static` without a role) is rejected at mint. You do not
need `--endpoint` for public TOS: the CLI talks to the S3-compatible gateway
`https://tos-s3-<region>.volces.com` with **virtual-host** addressing
(`force_path_style=false`). Path-style requests fail with `InvalidPathAccess`.
`--sts-endpoint` is only needed if you do not use the default
`https://sts.volcengineapi.com`. `--external-id` is supported when the role
trust policy requires it.

**Bind the tenant and use it**

```bash
drive9 admin tenant object-namespace set \
  --tenant-id tnt_xxx --namespace-id acme-prod \
  --tidbcloud-public-key <public-key> \
  --tidbcloud-private-key <private-key>

drive9 fs ls tos://example/acme-prod/
drive9 fs cp ./file tos://example/acme-prod/file --force
drive9 mount tos://example/acme-prod/ ./mnt
```

Use the `tos://` scheme. After mint, region and endpoint come from the
backend; tenant URIs do not need `?region=` unless you are using
`--auth=local`.

### Other schemes (OSS, GCS, Azure)

These mint as well; first-wave customer storage is S3 / COS / TOS.

- **OSS** (`oss://`): Aliyun **AssumeRole** only (`--role-arn` is required),
  same shape as TOS. Isolation is prefix-scoped, but list matching is a
  string prefix rather than the directory form used by S3 / COS / TOS.
- **GCS** (`gs://` / `gcs://`): server holds a service-account JSON and mints
  a short-lived OAuth access token. The token has the **same IAM as that
  service account** — it is not downscoped to one bucket. Register one SA per
  bucket (or an SA that can only access that bucket). Namespace must match
  the bucket; backend prefix must be empty.
- **Azure** (`az://` / `azure://`): server holds the storage account key and
  mints a container SAS cryptographically scoped to that container. Isolation
  is **one container per tenant** (namespace must match the container;
  backend prefix must be empty). `--account-id` is the storage account name.

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

`--auth` is accepted on `fs` commands and on `mount`. For COS and TOS with
local auth you typically pass `?region=` yourself (and TOS still needs
virtual-host addressing, which the CLI sets for `tos://`).

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
  Dirty data is uploaded on write-handle `Flush`/`Fsync` (`WriteBack=0` so
  rclone Close is a synchronous PUT). The handle is then reopened so later
  writes still work after the kernel FLUSHes following create. A failed
  object PUT is returned from that flush/fsync/close. Reads honor the
  kernel interrupt channel immediately (`EINTR`) and the 5-minute
  deadline (`ETIMEDOUT`); the read callback may keep running. Mutating
  ops (write, flush, create, setattr, mkdir, unlink, rename) wait until
  the rclone call finishes before returning, so a syscall never fails
  while a delayed PUT/delete/rename still commits. rclone cannot abort
  in-flight HTTP. The 5-minute op context is only a cancel hint: if the
  provider/VFS ignores it, a mutating syscall stays blocked until that
  call returns. That is the product contract (correctness over a hard
  FUSE return bound). `--allow-other` enables kernel `default_permissions`.
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

**`tenant_object_namespaces`** — one row per tenant. `namespace_id` is the
customer prefix id. Empty means “object mint not allowed”. This is not a user
ACL table and is not the drive9 `tenant_id`. Shared tenants keep namespace
here; they never write `tenant_tidbcloud_org_bindings`.

Existing databases pick up the new tables through the usual meta schema
self-repair (add column / create table only).

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
