# drive9 vault — End-State Interaction Spec

Status: Proposed (four-way review: dev1 author, architect-1 / dev2 / adversary-2 review)
Scope: End-state CLI only. No current-implementation references, no transition/migration, no P0 tactics.

This spec is the single source of truth for the terminal shape of vault UX. It is the merged canonical of:

- `89603ee6` (four-way-signed terminal CLI surface) — §0–§12 below, absorbed verbatim in intent.
- v2 context-unified credential increment (grant token → `ctx` entry, env var = override) — §13–§17 below.

## 0. Mental Model

- **secret** = directory (e.g. `prod-db`)
- **key** = file inside that directory (e.g. `prod-db/DB_URL`)
- **Read/write a key**: POSIX file ops on `/n/vault/**` (`cat`, `printf >`, `ls`, `rm`)
- **Control plane**: 5 verbs — `put`, `grant`, `revoke`, `with`, `reauth`

One principle: authority follows the credential, not the command. Owners and delegatees use the same CLI; only the credential binding differs.

## 1. Mount

```bash
export DRIVE9_SERVER=https://drive9.example.com
export DRIVE9_API_KEY=<owner-api-key>

drive9 mount vault /n/vault
```

- `mount <backend> <path>` is the top-level command. `vault` is a backend name (alongside `fs`, `kv`, …).
- One mount binds one principal; the namespace the user sees is exactly what that principal can access.
- After the bound credential becomes invalid (expired / revoked), the mount does **not** fall back — the next syscall returns `EACCES`.

## 2. Create a Secret (Batch Write)

```bash
mkdir -p ./prod-db.envdir
printf '%s' 'postgres://db.example.com:5432/app' > ./prod-db.envdir/DB_URL
printf '%s' 'app_user'                           > ./prod-db.envdir/DB_USER
printf '%s' 'super-secret-password'              > ./prod-db.envdir/DB_PASSWORD

drive9 vault put /n/vault/prod-db --from ./prod-db.envdir
```

Whole-replace (delete keys not present in the source directory):

```bash
drive9 vault put /n/vault/prod-db --from ./prod-db.envdir --prune
```

Contract: `put` is a single transaction; concurrent readers see either the old complete state or the new complete state, never a half-update.

## 3. Add or Modify a Single Key

```bash
printf '%s' 'postgres://replica.example.com:5432/app' > /n/vault/prod-db/READ_REPLICA_URL
printf '%s' 'rotated-password-v2'                     > /n/vault/prod-db/DB_PASSWORD
```

Each `write(2)` is an atomic replacement of that key. There is no separate “set key” verb.

## 4. Read

### Single key
```bash
cat /n/vault/prod-db/DB_URL
```

### Whole-secret views
```bash
cat /n/vault/prod-db/@all       # JSON object of all keys (atomic snapshot)
cat /n/vault/prod-db/@env       # dotenv: KEY=VALUE, one per line
```

### List keys
```bash
ls /n/vault/prod-db
```

`ls` lists only the keys the current principal can see. Keys the principal cannot see are **indistinguishable from non-existent** (see §11 Errno table).

## 5. Delete

```bash
rm /n/vault/prod-db/OLD_KEY          # delete one key
rm -r /n/vault/prod-db               # delete the whole secret
```

`rm key` does **not** cascade-revoke grants. Holders of a grant over a removed key get `ENOENT` on the next read (POSIX “open-then-rm” mental model). The audit event records `affected_grants` so the owner can see which grants are now dangling.

## 6. Grant — Issue a Scoped Token

### Whole secret, read-only, 1 hour
```bash
drive9 vault grant /n/vault/prod-db --agent alice --perm read --ttl 1h
```

### Single key, 30 min
```bash
drive9 vault grant /n/vault/prod-db/DB_URL --agent alice --perm read --ttl 30m
```

### Multiple scopes in one grant
```bash
drive9 vault grant /n/vault/prod-db/DB_URL /n/vault/prod-db/DB_USER \
  --agent alice --perm read --ttl 1h
```

### Write permission (e.g. credential rotation)
```bash
drive9 vault grant /n/vault/prod-db/DB_PASSWORD --agent rotator --perm write --ttl 10m
```

### Default output (human)

```
drive9 ctx import <jwt>
grant_id:   grt_7f2a...
expires_at: 2026-04-18T19:00:00Z
```

The first line is a ready-to-paste command the delegatee runs to install the grant into their own `~/.drive9/config` (see §13). The JWT is displayed once and not re-fetchable.

### Machine output

```bash
drive9 vault grant ... --token-only       # prints raw JWT to stdout, nothing else
drive9 vault grant ... --json             # prints {token, grant_id, expires_at, scope[], perm, ttl}
```

Rules:
- Scope is either a whole secret (`/n/vault/prod-db`) or a single key (`/n/vault/prod-db/DB_URL`).
- Multiple scopes per grant are allowed.
- `--perm` ∈ {`read`, `write`}.
- `--ttl` is required.

## 7. The Delegatee Side

```bash
# Alice receives the one-line install command from §6
drive9 ctx import <jwt>               # writes a new delegated context into ~/.drive9/config
drive9 ctx use alice-prod-db          # activate it

drive9 mount vault /n/vault
cat /n/vault/prod-db/DB_URL           # OK
ls  /n/vault/prod-db                  # only scoped-visible keys
cat /n/vault/prod-db/DB_PASSWORD      # ENOENT (not visible under this grant)
```

Authority follows the credential. Owner and delegatee run identical `mount` / `cat` / `ls` / `with` commands; only the active context differs.

## 8. Revoke

```bash
drive9 vault revoke grt_7f2a
```

Effects:
- New requests fail immediately.
- Already-mounted mounts return `EACCES` on the next syscall.
- No silent fallback to owner identity.

## 9. Inject Secrets into a Child Process

```bash
drive9 vault with /n/vault/prod-db -- ./myapp
```

Semantics: read `@env`, fork, inject env vars into the child, exec. When the child exits, the env is gone (the parent shell never sees the values).

## 10. Audit

```bash
tail -f /n/vault/@audit                   # global stream
tail -f /n/vault/prod-db/@audit           # per-secret stream
```

Events: `put / read / write / grant / revoke / rm / reauth`.
`rm key` events carry `affected_grants`.

Grant introspection:

```bash
ls  /n/vault/prod-db/@grants/
cat /n/vault/prod-db/@grants/grt_7f2a
# agent:      alice
# scope:      prod-db/DB_URL
# perm:       read
# expires_at: 2026-04-18T19:00:00Z
# last_used:  2026-04-18T18:05:12Z from 10.0.3.42
```

## 11. Errno Table (Normative)

| Scenario | errno |
|---|---|
| Secret does not exist | `ENOENT` |
| Key does not exist | `ENOENT` |
| Key exists but current principal has no permission | `ENOENT` (existence-oracle defense) |
| Current principal attempts an unauthorized **write** | `EACCES` |
| Bound credential expired / revoked | `EACCES` + reauth hint |
| Infrastructure failure (FUSE daemon, backend) | `EIO` |

Core rules:
- **Reads**: invisible == non-existent (`ENOENT`). Key names are themselves sensitive metadata.
- **Writes**: `EACCES`. The write intent already names the key, so a fake-not-found would be a lie without benefit.
- **Stale auth**: `EACCES`, distinguished from “not found.”

This table is locked. The auth lifecycle (§17) layers **local short-circuits** (e.g. `ctx use` on an expired context refuses client-side) **on top of** the server-side stale-auth case — those short-circuits do not introduce a new errno.

## 12. Recovery After Credential Rotation

```bash
drive9 ctx use <new-context>
drive9 vault reauth /n/vault
```

`reauth` rebinds the running mount to the current active context without unmount/remount. The next syscall succeeds if the new credential is valid.

---

## 13. Contexts — Primary Credential Binding

Contexts live in `~/.drive9/config`. A context is either an **owner** context (long-lived API key) or a **delegated** context (JWT issued by `vault grant`). Both kinds coexist in the same config; exactly one is active at any moment (Invariant #6).

### 13.1 Context schema

Each context entry contains:

| Field | Owner | Delegated |
|---|---|---|
| `name` | required | required |
| `type` | `owner` | `delegated` |
| `server` | required | required |
| `api_key` | required | — |
| `token` (JWT) | — | required |
| `scope[]` | — | required (from JWT) |
| `perm` | — | required (from JWT) |
| `expires_at` | — | required (from JWT) |
| `grant_id` | — | required (from JWT) |
| `label_hint` | — | optional (from JWT, used as default `name`) |

The delegated fields are populated by locally decoding the JWT payload. See Invariant #7 — this decoding is UX-only; authorization is still server-side.

### 13.2 Context verbs

```bash
drive9 ctx add --api-key <key> [--name <n>] [--server <url>]      # add owner context
drive9 ctx import <jwt>                                           # add delegated context (primary UX for grants)
drive9 ctx add --from-token <jwt> [--name <n>]                    # low-level alias of ctx import (not promoted)
drive9 ctx ls                                                     # list contexts (offline — reads only local config)
drive9 ctx use <name>                                             # activate a context
drive9 ctx rm <name>                                              # delete a context
```

`ctx ls` output:

```
NAME              TYPE        SCOPE                         PERM   EXPIRES_AT            STATUS
owner-prod        owner       *                             rw     —                     active
alice-prod-db     delegated   prod-db/DB_URL                read   2026-04-18T19:00:00Z  active *
rotator-pwd       delegated   prod-db/DB_PASSWORD           write  2026-04-18T18:10:00Z  expired
```

`*` marks the currently active context. `STATUS` is computed locally from `expires_at` at display time.

### 13.3 `ctx import` contract (MUST)

- Input **MUST** be a delegated-JWT. If the payload indicates `principal_type=owner` (or any non-delegated credential), `ctx import` **MUST** refuse and instruct the user to use `ctx add --api-key`. `ctx import` is not a universal credential importer.
- If the JWT’s `exp` is already in the past at import time, `ctx import` **MUST** refuse (local short-circuit #1).
- Default context name is the JWT’s `label_hint`; on collision or absence, fall back to `<agent>-<scope-root>` with a numeric suffix as needed. `--name` overrides.

## 14. Environment Variables — Explicit Override

Environment variables are **not** the primary credential channel. They exist as an explicit override for ephemeral / non-interactive contexts (CI jobs, one-off scripts, air-gapped shells).

### 14.1 Variables

- `DRIVE9_API_KEY` — owner credential override.
- `DRIVE9_VAULT_TOKEN` — delegated credential override (JWT).
- `DRIVE9_SERVER` — server URL override.

The **dual-principal separation is locked**: there is no single combined variable. `DRIVE9_VAULT_TOKEN` and `DRIVE9_API_KEY` remain distinct knobs; collapsing them is prohibited.

### 14.2 Priority

The effective credential at mount / reauth time is resolved in this order (first match wins):

1. `DRIVE9_VAULT_TOKEN` (narrower — delegated)
2. `DRIVE9_API_KEY` (broader — owner)
3. Active context in `~/.drive9/config`

Rule 1 vs 2 implements narrower-wins so that a scoped token never falls back to owner authority within the env channel. Rule 3 means `current_context` only applies when no env override is present.

### 14.3 Activation mechanics

At most one credential is bound to a mount. When both env overrides and an active context exist, the env override wins. This is a mechanism detail, not a second authorization layer — the chosen credential is then validated by the server on every request (Invariant #7).

## 15. Grant → Context Flow (End-to-End)

### Owner

```bash
drive9 ctx use owner-prod
drive9 mount vault /n/vault

mkdir -p ./prod-db.envdir
printf 'postgres://db.example.com:5432/app' > ./prod-db.envdir/DB_URL
printf 'app_user'                           > ./prod-db.envdir/DB_USER
printf 'super-secret-password'              > ./prod-db.envdir/DB_PASSWORD
drive9 vault put /n/vault/prod-db --from ./prod-db.envdir

drive9 vault grant /n/vault/prod-db/DB_URL --agent alice --perm read --ttl 1h
# stdout:
# drive9 ctx import vt_eyJhbGc...
# grant_id:   grt_7f2a
# expires_at: 2026-04-18T19:00:00Z
```

Owner sends Alice the first line through a secure channel.

### Alice

```bash
drive9 ctx import vt_eyJhbGc...          # decodes JWT locally, writes delegated context
drive9 ctx use alice-prod-db             # activates it
drive9 mount vault /n/vault

cat /n/vault/prod-db/DB_URL              # OK
cat /n/vault/prod-db/DB_PASSWORD         # ENOENT
drive9 vault with /n/vault/prod-db -- ./alice-tool
```

### Owner revokes early

```bash
drive9 vault revoke grt_7f2a
```

### Alice’s next read

```bash
cat /n/vault/prod-db/DB_URL
# cat: Permission denied  (run `drive9 vault reauth` after updating the context)
```

## 16. Security Note (Normative MUST)

The JWT payload is self-describing. Clients **MAY** decode it locally to populate `ctx` metadata (`scope[]`, `perm`, `expires_at`, `grant_id`, `label_hint`) and to render `ctx ls` offline.

However:

- **The server MUST re-validate signature, TTL, and revocation status on every request.** The local decode is a UX convenience only.
- **Clients MUST NOT treat the decoded payload as authoritative for access decisions.** A decoded token payload is never a substitute for a server-side check.
- **Servers MUST NOT weaken their check based on what the client claims the token says.** Tampered tokens fail signature verification regardless of client-side decoding outcomes.

This is locked as Invariant #7.

## 17. Auth Lifecycle — Local Short-Circuits vs Server Checks

Local short-circuits exist to make UX responsive. They are layered **on top of** the normative errno table (§11), not instead of it.

| Stage | Check | Outcome |
|---|---|---|
| `ctx import <jwt>` | `exp` in past? | local refuse (no new errno; command error) |
| `ctx ls` | `exp` in past? | row marked `expired` |
| `ctx use <name>` | target context expired? | local error, do not activate |
| `mount` | context valid locally? | proceed; server then validates |
| Any FS op | server says stale / revoked | `EACCES` + reauth hint (§11) |

The three local short-circuits (`ctx import` / `ctx ls` / `ctx use`) are client-side UX. They do **not** introduce a new errno case. The locked 6-row errno table in §11 is unchanged.

## 18. Invariants (Normative, numbered)

1. **Atomic `put --prune`**: a put transaction is visible to concurrent readers either entirely in the old state or entirely in the new state; no half-update is observable.
2. **Existence oracle defense**: reads return `ENOENT` for both non-existent and invisible keys; the two are indistinguishable to clients.
3. **One mount, one principal**: a mount is bound to exactly one credential at mount time; stale/revoked credentials do not silently fall back to any other identity.
4. **Field names are sensitive metadata**: key names are not disclosed via errno, audit (to the delegatee), or listing unless the principal has permission.
5. **Grants do not cascade-revoke on `rm`**: removing a key leaves existing grants syntactically intact; holders observe `ENOENT`, and audit records `affected_grants`.
6. **One active context at a time**: `~/.drive9/config` MAY hold any number of contexts (owner and delegated, mixed); at most one is active. Switching contexts does not silently re-bind an already-mounted mount (use `reauth`).
7. **Client-side JWT decoding is UX-only**: local decode populates `ctx` metadata and enables offline `ctx ls`; it **MUST NOT** substitute for server-side validation. The server **MUST** re-check signature, TTL, and revocation on every request.

## 19. Failure Model (Summary)

| Failure | Detection | Client visible |
|---|---|---|
| Expired / revoked credential | server on next request | `EACCES` + reauth hint |
| Server unreachable | client | `EIO` |
| FUSE daemon crash | kernel | `EIO` |
| Malformed JWT at `ctx import` | client local decode | command error, no context written |
| Import of wrong credential kind (owner JWT, random string) | client local decode | command error, directing user to `ctx add --api-key` |
| Concurrent `put --prune` reads during transaction | server transaction | atomic — readers see old or new (Invariant #1) |

## 20. Non-Goals

- No migration or backward-compatibility surface in this spec; this document is terminal-state only.
- No single unified credential variable that merges `DRIVE9_API_KEY` and `DRIVE9_VAULT_TOKEN` (the dual-principal separation is a contract, §14.1).
- No wildcard scopes in v0 (`*` in key/scope is rejected at parse time).
- No client-side authorization (Invariant #7).
- No automatic token auto-mint on behalf of the owner; every delegated credential must come from an explicit `vault grant`.

## 21. Open Questions (Spec-Level)

- None at publication time. Review may add.

---

Appendix A — Command surface at a glance:

| Command | Role |
|---|---|
| `drive9 mount vault <path>` | Mount the vault namespace under `<path>`. |
| `drive9 umount <path>` | Unmount. |
| `drive9 ctx add --api-key <k>` | Register an owner context. |
| `drive9 ctx import <jwt>` | Register a delegated context from a grant JWT (primary UX). |
| `drive9 ctx add --from-token <jwt>` | Low-level alias of `ctx import`. |
| `drive9 ctx ls / use / rm` | Manage contexts (offline). |
| `drive9 vault put <path> --from <dir> [--prune]` | Atomic batch write. |
| `drive9 vault grant <scope>... --agent --perm --ttl` | Issue a scoped JWT. |
| `drive9 vault revoke <grant-id>` | Revoke a grant. |
| `drive9 vault with <path> -- <cmd>` | Exec child with `@env` injected. |
| `drive9 vault reauth <mountpoint>` | Rebind a running mount to the current context. |
| `cat / ls / rm / printf >` on `/n/vault/**` | Data plane. |

Appendix B — Canonical history: §0–§12 absorb `89603ee6`; §13–§17 are the v2 context-unified credential increment over that canonical; Invariants #1–#5 derive from `89603ee6`; Invariants #6–#7 are new.
