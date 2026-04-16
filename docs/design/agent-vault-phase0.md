# Agent Vault: Phase 0 Design Spec

**Status**: Draft
**Date**: 2026-04-13
**Author**: architect-1
**Reviewers**: adversary-1, qiffang

---

## 1. Background

### Problem

AI agents need credentials to operate tools — GitHub tokens, AWS keys, database passwords, API keys. Today these are managed through ad-hoc mechanisms:

- Environment variables injected at startup (static, cannot rotate at runtime)
- `.env` files scattered across filesystems (plaintext, no access control)
- Secret managers (HashiCorp Vault, AWS Secrets Manager) requiring SDK/API integration (high friction, not all agents can call HTTP)
- Manual injection by orchestrators (does not scale, single point of failure)

None of these solutions are agent-native. They were designed for human developers or long-running services, not for autonomous agents that:
- Run in diverse environments (local, container, sandbox, remote runner)
- Need credentials scoped to a specific task, not a role
- Cannot reliably manage secret lifecycle (rotation, revocation)
- May inadvertently capture secrets in logs, prompts, or workspace snapshots

### Goal

Design a secret management system for drive9 that is:
1. **Low-friction consumption** — agents get secrets through the simplest possible interface
2. **Least-privilege by default** — agents see only what they need for the current task
3. **Rotation-safe** — credential updates propagate without agent restart
4. **Auditable** — every secret materialization is traceable
5. **Defense-in-depth** — no single compromise exposes all secrets

### Non-Goal: "1Password for agents"

This system is **not** a full zero-knowledge encrypted vault like 1Password. The trust model is different: 1Password assumes the server is untrusted (zero-knowledge); our system assumes the server is trusted infrastructure (like AWS Secrets Manager or HashiCorp Vault). We explicitly choose this because:
- Agents cannot perform interactive authentication (no biometrics, no master password prompt)
- The orchestrator/server must be able to inject and rotate secrets on behalf of agents
- Zero-knowledge encryption would require agents to hold decryption keys, creating a harder key distribution problem

The correct product framing is: **agent-native secret and config filesystem** — a purpose-built secret manager with consumption interfaces optimized for autonomous agents.

---

## 2. Threat Model

### 2.1 Actors

| Actor | Trust Level | Description |
|-------|------------|-------------|
| **Ops / Admin** | Trusted | Creates, rotates, revokes secrets. Sets ACL policies. Has access to server infrastructure. |
| **Orchestrator** | Trusted | Launches agents, assigns task-scoped capability tokens. Has server-side credentials. |
| **Agent** | Semi-trusted | Executes tasks autonomously. Follows instructions but may have bugs, be prompt-injected, or run untrusted tools. Should only see secrets needed for current task. |
| **Agent subprocess** | Untrusted | Tools, scripts, child processes spawned by agent. May read files, environment, or memory accessible to the agent process. |
| **External attacker** | Untrusted | Network-level attacker. Cannot access server internals. |

### 2.2 Trust Boundaries

```
┌─────────────────────────────────────────────┐
│  Server Trust Zone                          │
│  - Encrypted secret store (at rest)         │
│  - Capability token issuer                  │
│  - Audit log writer                         │
│  - Decryption happens here                  │
│                                             │
│  Server CAN see plaintext secrets.          │
│  This is the same trust model as            │
│  AWS Secrets Manager / HashiCorp Vault.     │
└──────────────────┬──────────────────────────┘
                   │ TLS
                   │ capability token authentication
┌──────────────────▼──────────────────────────┐
│  Agent Trust Zone                           │
│  - Receives plaintext only for              │
│    secrets bound to its capability token    │
│  - Plaintext exists in process memory       │
│  - Cannot enumerate secrets beyond scope    │
│  - Cannot write/modify/delete secrets       │
└──────────────────┬──────────────────────────┘
                   │ process boundary
┌──────────────────▼──────────────────────────┐
│  Subprocess Zone (untrusted)                │
│  - Inherits env vars if agent exports them  │
│  - Can read files agent has opened          │
│  - Cannot access capability token directly  │
│    (token is held by adapter, not exported) │
└─────────────────────────────────────────────┘
```

### 2.3 Key Hierarchy

```
Master Key (MK)
  │  Held by: server process (loaded from KMS / env / sealed config)
  │  Purpose: encrypt/decrypt Data Encryption Keys
  │
  ├── Data Encryption Key (DEK) per tenant
  │     Purpose: encrypt secret values at rest
  │     Stored: encrypted by MK in metadata DB
  │     Rotation: transparent re-wrap when MK rotates
  │
  └── Capability Signing Key (CSK)
        Purpose: HMAC-sign capability tokens
        Stored: derived from MK + tenant_id (deterministic)
        Not stored separately — derived on demand
```

**Encryption at rest**: Each secret value is encrypted with the tenant's DEK using AES-256-GCM before writing to the database. The DEK itself is encrypted (wrapped) by the MK.

**In transit**: All client-server communication over TLS. Plaintext secrets only appear:
1. In server memory during encrypt/decrypt
2. In the TLS-protected response body to the agent
3. In agent process memory after receipt

### 2.4 Threat Scenarios and Mitigations

| # | Threat | Severity | Mitigation |
|---|--------|----------|------------|
| T1 | Agent reads secrets beyond its task scope | High | Capability token scopes secret access to a named set. Secrets outside scope are invisible (not 403, but absent). |
| T2 | Agent subprocess/tool captures secrets from filesystem or env | High | FUSE adapter: `readdir` returns empty for subprocess PIDs not matching agent (best-effort). API adapter: secrets never touch filesystem. Env adapter: recommend short-lived subshell, not `export`. |
| T3 | Agent logs/prints secret values (prompt capture, debug dump) | Medium | Out of scope for infrastructure — this is an agent behavior problem. Mitigation: short-lived capability tokens limit blast radius. Audit log enables post-incident tracing. |
| T4 | Database compromise exposes secret values | High | Secrets encrypted at rest with per-tenant DEK. Attacker needs both DB dump and MK to recover plaintext. |
| T5 | Capability token stolen / replayed | Medium | **Mitigated, not eliminated.** Tokens are short-lived scoped bearer tokens (TTL, default 1 hour). A stolen token can be replayed within its TTL window from any machine — there is no holder binding (no mTLS, no channel binding, no agent-specific proof). Mitigations: (1) short TTL limits replay window, (2) narrow scope limits blast radius, (3) server-side revocation (`DELETE /v1/vault/tokens/{id}`) can kill a compromised token immediately, (4) audit log records all materializations for post-incident tracing. Tokens cannot be used to issue new tokens. Future enhancement: optional IP/session pinning at grant time (not in Phase 0 scope). |
| T6 | MK compromise | Critical | Rotate MK → re-wrap all DEKs. Plaintext secrets not affected (only DEK wrapping changes). Alert on anomalous MK access patterns. |
| T7 | Network eavesdropping | High | TLS required. FUSE adapter uses localhost loopback (no network exposure). API adapter over TLS only. |
| T8 | Stale secret used after rotation | Medium | See Materialization Contract §4. New reads get new value. Stale in-memory values are the agent's responsibility. |
| T9 | `rg`/`tar`/snapshot accidentally captures `/vault` contents | High | FUSE adapter: synthetic FS with `noexec,nosuid` mount flags. `readdir` can be restricted to show only explicitly opened paths (no enumeration mode). API/CLI adapter: secrets never on filesystem. See §3.3 Enumeration Policy. |

---

## 3. Capability Model

### 3.1 Capability Token

The core authorization primitive is a **capability token** — a short-lived, task-scoped bearer token that grants access to a specific set of secrets.

```json
{
  "token_id": "cap_a1b2c3d4",
  "tenant_id": "tenant-xyz",
  "agent_id": "agent-senior-1",
  "task_id": "task-20260413-deploy",
  "scope": ["aws-prod", "db-prod/password", "github-token"],
  "permissions": ["read"],
  "issued_at": "2026-04-13T19:00:00Z",
  "expires_at": "2026-04-13T20:00:00Z",
  "nonce": "random-bytes"
}
```

**Token format**: HMAC-SHA256 signed, base64url encoded. Same pattern as drive9's existing upload token (#116). The signed payload is self-describing (contains scope, TTL, etc.), but the server **also persists token state** in the `vault_tokens` table (see §6.1) to support revocation.

**Token verification flow** (every consumption request):
1. Verify HMAC signature (fast, no DB hit — rejects tampered/forged tokens)
2. Check `expires_at` from token payload (fast, no DB hit — rejects expired tokens)
3. Query `vault_tokens` table: confirm `token_id` exists AND `revoked_at IS NULL` (DB hit — rejects revoked tokens)
4. If all checks pass, extract scope from token payload and serve request

Step 3 is the critical addition: without server-side state, revocation before TTL expiry would be impossible. The DB lookup adds latency (~1ms for indexed primary key), but is required for revocation correctness.

**Scope semantics**:
- `"aws-prod"` — access all fields of the `aws-prod` secret
- `"db-prod/password"` — access only the `password` field of `db-prod`
- Scope is a whitelist. Anything not listed is invisible.

**Lifecycle**:
1. Orchestrator calls `POST /v1/vault/tokens` with agent identity + desired scope
2. Server validates orchestrator credentials, checks ACL policy, issues token, **persists token record to `vault_tokens`**
3. Orchestrator passes token to agent (env var, file, or direct injection)
4. Agent uses token to consume secrets through any adapter. **Every request is validated against server-side token state** (see verification flow above).
5. Token expires or is explicitly revoked (`DELETE /v1/vault/tokens/{token_id}` sets `revoked_at`). Agent loses access on next request.

### 3.2 Consumption Adapters

Three parallel consumption paths, same capability token:

```
                     Capability Token
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         FUSE Adapter  API Adapter  Exec Adapter
         (file read)   (HTTP GET)   (env inject)
```

#### Adapter 1: FUSE (local mount, read-only)

```bash
dat9 vault mount --token $CAP_TOKEN /vault
cat /vault/aws-prod/access-key    # → "AKIA..."
cat /vault/db-prod/password       # → "s3cret"
ls /vault/                        # → only scoped secrets visible
```

- Best for: local agents (Claude Code, Codex), interactive use, tools that read files
- Properties: read-only, scoped `readdir`, no plaintext on disk (in-memory FUSE)
- Limitation: requires FUSE support on host (not available in all containers/sandboxes)

#### Adapter 2: API (HTTP GET, universal)

```bash
curl -H "Authorization: Bearer $CAP_TOKEN" \
     https://drive9.example.com/v1/vault/secrets/aws-prod/access-key
# → "AKIA..."

curl -H "Authorization: Bearer $CAP_TOKEN" \
     https://drive9.example.com/v1/vault/secrets/db-prod?format=json
# → {"host":"db.internal","port":5432,"password":"s3cret"}
```

- Best for: remote runners, CI, sandboxed agents, programmatic access
- Properties: universal (only needs HTTP), explicit fetch, no filesystem involvement
- Limitation: agent must have outbound HTTP access

#### Adapter 3: Exec Injection (process-scoped env)

```bash
dat9 vault exec --token $CAP_TOKEN -- python agent.py
# agent.py sees: os.environ["AWS_ACCESS_KEY_ID"] = "AKIA..."
# env vars injected into child process only, not exported to parent shell
```

- Best for: agents/tools that only consume env vars, legacy integrations
- Properties: subprocess-scoped (env does not leak to parent), snapshot semantics
- Limitation: static at process start, no runtime rotation

### 3.3 Enumeration Policy

**Default: no-enumerate mode.**

When an agent mounts via FUSE or calls `GET /v1/vault/secrets`:
- `readdir` / `LIST` returns **only secret names the token has access to** — not the full vault
- If the scope contains `"aws-prod"` and `"github-token"`, `ls /vault/` shows exactly two entries
- Sub-fields within a secret are always listable if the secret is in scope

**Rationale**: prevents `rg`, `tar`, `find`, workspace snapshots from accidentally discovering or capturing secrets outside the agent's task scope. The blast radius of a compromised agent is bounded by its token scope.

**Optional: explicit-open mode** (stricter, for high-security deployments):
- `readdir` returns empty
- Agent must `open` a specific path it already knows (from task instructions)
- Prevents even enumeration of scoped secret names

---

## 4. Materialization Contract

"Materialization" = the moment a secret value becomes plaintext outside the server.

### 4.1 Materialization Semantics

| Property | Guarantee |
|----------|-----------|
| **New `open()`/`GET` after rotation** | Returns new secret value |
| **Already-opened file descriptor** | NOT guaranteed to update. Reads on an existing FD may return the version that was current when the FD was opened. (FUSE adapter may choose to invalidate, but this is best-effort.) |
| **Env var after exec injection** | Snapshot at process start. Never updates. This is inherent to how env vars work. |
| **In-memory value in agent code** | Agent's responsibility. Infrastructure cannot reach into process memory. |
| **After token expiry/revocation** | New `open()`/`GET` fails with 401/EACCES. Existing in-memory values remain (cannot be forcibly cleared). Existing FUSE FDs: **best-effort invalidation** — adapter attempts to return EIO on subsequent reads, but this depends on kernel page cache state and is not a hard guarantee. Data already read into userspace buffers or kernel page cache may remain accessible until the FD is closed. |

### 4.2 Materialization Formats

Each secret has typed fields. Adapters project these fields into consumption formats:

**Raw field files** (FUSE):
```
/vault/aws-prod/access-key    → raw value, no newline
/vault/aws-prod/secret-key    → raw value, no newline
```

**Aggregate views** (opt-in, not default):
```
/vault/aws-prod/.env           → KEY=VALUE format
/vault/aws-prod/.json          → JSON object
/vault/aws-prod/.connection-string  → type-specific formatted string
```

**Aggregate views are NOT generated by default.** They must be explicitly requested:
- FUSE: `dat9 vault mount --token $CAP_TOKEN --materialize env,json /vault`
- API: `GET /v1/vault/secrets/aws-prod?format=env` or `?format=json`

**Rationale** (from adversary-1 review): aggregate files like `.env` combine multiple fields into one blob, increasing blast radius of a single read. Making them opt-in ensures the default is minimal exposure.

### 4.3 Rotation Propagation

When ops rotates a secret:

```
1. Ops: PUT /v1/vault/secrets/aws-prod {new values}
2. Server: encrypt + store new version, bump secret revision
3. Server: emit SSE event: {"type":"secret_rotated","name":"aws-prod","revision":N}
4. FUSE adapter: receives SSE → InodeNotify for /vault/aws-prod/* → kernel cache invalidated
5. Next agent read(): fetches new plaintext from server
```

**What this guarantees**: future reads observe the new version.

**What this does NOT guarantee**:
- Already `source`'d env vars refresh (they don't — env is a snapshot)
- Already-established DB connections use new password (they don't — connection pool holds old credentials)
- Already-opened file descriptors see new content (best-effort, not guaranteed)

**Recommendation for agents**: sensitive integrations should re-read credentials periodically or on SSE notification, not cache them indefinitely. drive9 client SDK can provide a `SecretWatcher` callback for this.

### 4.4 Revocation

When a capability token is revoked or expires:

| Adapter | Behavior |
|---------|----------|
| FUSE | New `open()` returns `EACCES`. `readdir` returns empty. Mount stays alive (for graceful degradation) but serves no secrets. **Existing open FDs**: best-effort invalidation — adapter sets an internal revoked flag and attempts to return `EIO` on subsequent `read()` calls, but data already in kernel page cache or userspace buffers may still be readable until the FD is closed. This is NOT a hard guarantee. |
| API | Returns `401 Unauthorized` for all requests with the revoked token. |
| Exec | No effect on already-running process (env vars are in-memory). New `dat9 vault exec` with expired token fails. |

**No remote memory wipe.** Once plaintext enters agent process memory, infrastructure cannot reclaim it. This is an explicit non-goal and a fundamental property of any secret manager (including 1Password, Vault, AWS Secrets Manager). The mitigation is short token TTLs and scoped access.

---

## 5. Audit Semantics

### 5.1 What Is Audited

Every interaction with the vault that could expose secret plaintext:

| Event | Logged Fields | Trigger |
|-------|--------------|---------|
| `secret.read` | token_id, agent_id, task_id, secret_name, field (if specific), adapter (fuse/api/exec), timestamp, source_ip | Agent reads a secret value |
| `secret.list` | token_id, agent_id, task_id, adapter, timestamp | Agent enumerates available secrets |
| `secret.denied` | token_id, agent_id, task_id, requested_secret, reason (out_of_scope/expired/revoked), timestamp | Access attempt blocked |
| `token.issued` | token_id, issuer_id, agent_id, task_id, scope, ttl, timestamp | Orchestrator creates capability token |
| `token.revoked` | token_id, revoker_id, reason, timestamp | Token explicitly revoked |
| `token.expired` | token_id, timestamp | Token TTL elapsed |
| `secret.created` | secret_name, creator_id, timestamp | Ops creates a secret |
| `secret.rotated` | secret_name, rotator_id, old_revision, new_revision, timestamp | Ops rotates a secret |
| `secret.deleted` | secret_name, deleter_id, timestamp | Ops deletes a secret |

### 5.2 What Is NOT Audited

- **Cache hits**: If the FUSE adapter serves a value from its in-memory cache without hitting the server, no audit event is generated. The initial fetch is logged; subsequent cache-served reads are not.
- **In-process usage**: After the agent receives plaintext, infrastructure has no visibility into how it is used (printed, logged, passed to subprocess, etc.).
- **Read frequency from open FD**: Once a FUSE file descriptor is open, repeated `read()` syscalls on it are not individually logged.

### 5.3 Audit Accuracy Statement

> The audit log records **every server-side secret materialization** (the moment encrypted secret becomes plaintext in a response to an agent). It does NOT record every consumption of that plaintext by the agent or its subprocesses. The gap between "materialized once" and "used N times" is inherent and accepted.

### 5.4 Audit Storage

Audit events are written to a dedicated append-only table (`vault_audit_log`) in the tenant database. Events are immutable — no UPDATE or DELETE. Retention policy is configurable per tenant (default: 90 days).

---

## 6. Data Model

### 6.1 Server-Side Tables

> **Note**: All DDL uses TiDB/MySQL-compatible syntax. The tenant DB is TiDB — do NOT use PostgreSQL-only types (`BYTEA`, `TIMESTAMPTZ`, `JSONB`) or syntax (`$N` placeholders, `ON CONFLICT`). Runtime SQL must use `?` positional placeholders. See Bad Case #11.

```sql
-- Secret metadata and encrypted values
CREATE TABLE IF NOT EXISTS vault_secrets (
    secret_id     VARCHAR(64) PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    name          VARCHAR(255) NOT NULL,
    secret_type   VARCHAR(32) NOT NULL DEFAULT 'generic',
    revision      BIGINT NOT NULL DEFAULT 1,
    created_by    VARCHAR(255) NOT NULL,
    created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    deleted_at    DATETIME(3),
    UNIQUE KEY uk_tenant_name (tenant_id, name)
);

-- Individual fields within a secret (each encrypted separately)
CREATE TABLE IF NOT EXISTS vault_secret_fields (
    secret_id     VARCHAR(64) NOT NULL,
    field_name    VARCHAR(255) NOT NULL,
    encrypted_value BLOB NOT NULL,          -- AES-256-GCM encrypted
    nonce         BLOB NOT NULL,            -- GCM nonce (12 bytes)
    PRIMARY KEY (secret_id, field_name)
);

-- Capability tokens (server-side state for revocation + audit)
CREATE TABLE IF NOT EXISTS vault_tokens (
    token_id      VARCHAR(64) PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    agent_id      VARCHAR(255) NOT NULL,
    task_id       VARCHAR(255),
    scope_json    JSON NOT NULL,              -- ["aws-prod","db-prod/password"]
    issued_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    expires_at    DATETIME(3) NOT NULL,
    revoked_at    DATETIME(3),                -- NULL = active; non-NULL = revoked
    revoked_by    VARCHAR(255),
    revoke_reason VARCHAR(255),
    KEY idx_token_tenant (tenant_id),
    KEY idx_token_agent (agent_id)
);

-- ACL policies
CREATE TABLE IF NOT EXISTS vault_policies (
    policy_id     VARCHAR(64) PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    name          VARCHAR(255) NOT NULL,
    rules_json    JSON NOT NULL,            -- [{"match":"agent:senior-*","secrets":["aws-*","db-*"]}]
    created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

-- Audit log (append-only)
CREATE TABLE IF NOT EXISTS vault_audit_log (
    event_id      VARCHAR(64) PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    event_type    VARCHAR(32) NOT NULL,
    token_id      VARCHAR(64),
    agent_id      VARCHAR(255),
    task_id       VARCHAR(255),
    secret_name   VARCHAR(255),
    field_name    VARCHAR(255),
    adapter       VARCHAR(16),
    detail_json   JSON,
    timestamp     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    KEY idx_audit_tenant_time (tenant_id, timestamp),
    KEY idx_audit_secret (secret_name, timestamp)
);
```

### 6.2 Secret Types

| Type | Fields | Synthetic Views |
|------|--------|----------------|
| `generic` | Arbitrary key-value pairs | `.json` |
| `aws_credentials` | `access_key`, `secret_key`, `region` | `.env` (AWS_ACCESS_KEY_ID=...), `.json` |
| `database` | `host`, `port`, `user`, `password`, `dbname`, `sslmode` | `.connection-string`, `.env`, `.json` |
| `api_key` | `key`, `endpoint` | `.env`, `.json` |
| `ssh_key` | `private_key`, `public_key` | raw files (no aggregate) |
| `tls_cert` | `cert`, `key`, `ca` | raw PEM files |

Types are extensible. Unknown types fall back to `generic` behavior.

---

## 7. API Surface

### Management API (for Ops / Orchestrator)

```
POST   /v1/vault/secrets                  # Create secret
GET    /v1/vault/secrets                  # List secrets (admin)
GET    /v1/vault/secrets/{name}           # Get secret metadata (no values)
PUT    /v1/vault/secrets/{name}           # Update/rotate secret
DELETE /v1/vault/secrets/{name}           # Soft-delete secret

POST   /v1/vault/tokens                   # Issue capability token
DELETE /v1/vault/tokens/{token_id}        # Revoke token

GET    /v1/vault/audit                    # Query audit log
```

### Consumption API (for Agents, authenticated by capability token)

```
GET    /v1/vault/read/{name}              # Get all fields (plaintext)
GET    /v1/vault/read/{name}/{field}      # Get single field (plaintext)
GET    /v1/vault/read/{name}?format=env   # Get as .env format
GET    /v1/vault/read/{name}?format=json  # Get as JSON
```

Management API uses the existing drive9 auth (tenant token). Consumption API uses capability tokens only. The two namespaces are intentionally separate (`/v1/vault/secrets` vs `/v1/vault/read`) to prevent capability tokens from accessing management operations.

---

## 8. Implementation Phases

| Phase | Scope | Deliverable |
|-------|-------|-------------|
| **Phase 0** (this doc) | Threat model, capability model, contracts | Design spec |
| **Phase 1** | Vault data model + Management API + encryption at rest | Server-side vault CRUD, per-tenant DEK, capability token issuance |
| **Phase 2** | API consumption adapter | `GET /v1/vault/read/*` with capability token auth + audit logging |
| **Phase 3** | FUSE consumption adapter | Read-only FUSE mount with scoped readdir, SSE-driven invalidation |
| **Phase 4** | Exec injection adapter | `dat9 vault exec` subprocess env injection |
| **Phase 5** | Rotation + revocation | SSE secret_rotated events, token revocation, FUSE FD invalidation |

Phase 1-2 is the MVP: ops can store secrets, agents can read them via API. Phase 3 adds the drive9-differentiated FUSE experience. Phase 4-5 add operational maturity.

---

## 9. Open Questions

1. **KMS integration**: Should MK be loaded from an external KMS (AWS KMS, GCP KMS) or from a local sealed config? KMS adds latency but better operational security.
2. **Token delivery**: How does the orchestrator pass the capability token to the agent? Env var? File? Direct API injection? This affects the subprocess threat surface.
3. **Multi-tenant key isolation**: Should each tenant have its own MK, or share a global MK with per-tenant DEKs? Per-tenant MK is more isolated but harder to manage.
4. **Historical secret versions**: Should the vault keep old versions of rotated secrets (for rollback)? Or only current version?
5. **FUSE mount lifecycle**: Should the FUSE vault mount be a separate mount from the existing drive9 data mount, or a subdirectory within it?

---

## 10. References

- drive9 design overview: `docs/design-overview.md`
- drive9 SSE change notification: Issue #150
- drive9 revision guard: Issue #151
- drive9 FUSE conflict handling: Issue #152
- drive9 upload token pattern: Issue #116
- db9-server upload token: `qiffang/db9-server` (HMAC-SHA256 signed token)
- mem9 CRDT design: `mem9-ai/mem9/docs/design/crdt-memory-proposal.md`
- HashiCorp Vault threat model: https://developer.hashicorp.com/vault/docs/internals/security
