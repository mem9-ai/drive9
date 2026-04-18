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
drive9 ctx import --from-file -
<jwt>
---
grant_id:   grt_7f2a...
expires_at: 2026-04-18T19:00:00Z
```

Owner sends this message to the delegatee over a secure channel. The delegatee copies the JWT line and pipes it into `ctx import` (see §13.3 for supported input modes). The JWT is displayed once by the server and is not re-fetchable.

Delegatees **SHOULD NOT** paste the JWT as a positional argument in an interactive shell — it would land in shell history and in `/proc/<pid>/cmdline` while the import is running. `ctx import --from-file <path>` and `ctx import --from-file -` (stdin) are the safe forms; see §13.3.

The default human output is **not a stable parse target** — the exact layout (label prefix, `---` separator, field ordering) may change for readability. Scripts **MUST** use `--json` or `--token-only`.

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
# Alice receives the grant block from §6, saves the JWT body to a 0600 file
install -m 600 /dev/null ~/alice-grant.jwt
$EDITOR ~/alice-grant.jwt                        # paste the JWT line
drive9 ctx import --from-file ~/alice-grant.jwt  # writes a new delegated context
rm ~/alice-grant.jwt

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
| `server` | required (from `--server`) | required (from JWT `iss`) |
| `api_key` | required | — |
| `token` (JWT) | — | required |
| `agent` | — | required (from JWT) |
| `scope[]` | — | required (from JWT) |
| `perm` | — | required (from JWT) |
| `expires_at` | — | required (from JWT) |
| `grant_id` | — | required (from JWT) |
| `label_hint` | — | optional (from JWT, used as default `name`) |

The delegated fields are populated by locally decoding the JWT payload (see §16 for the claim list). This decoding is UX-only; authorization is still server-side (Invariant #7).

### 13.2 Context verbs

```bash
drive9 ctx add --api-key <key> [--name <n>] [--server <url>]      # add owner context
drive9 ctx import --from-file <path>                              # add delegated context from a file (recommended)
drive9 ctx import --from-file -                                   # add delegated context from stdin (recommended)
drive9 ctx import <jwt>                                           # convenience form; JWT leaks to shell history
drive9 ctx ls [-l|--json]                                         # list contexts (offline — reads only local config)
drive9 ctx use <name>                                             # activate a context
drive9 ctx rm <name>                                              # delete a context
```

All three `ctx import` forms are equivalent in effect. The file-based and stdin forms are safer for bearer credentials because the JWT never touches the shell's history file or `/proc/<pid>/cmdline`. The positional-argument form is retained for scripting and local testing only.

`ctx ls` output:

```
NAME              TYPE        SCOPE                      PERM   EXPIRES_AT            STATUS
owner-prod        owner       *                          rw     —                     active
alice-prod-db     delegated   prod-db/DB_URL             read   2026-04-18T19:00:00Z  active *
alice-multi       delegated   prod-db/DB_URL +1          read   2026-04-18T19:00:00Z  active
rotator-pwd       delegated   prod-db/DB_PASSWORD        write  2026-04-18T18:10:00Z  expired
```

`*` in the NAME column marks the currently active context. `STATUS` is computed locally from `expires_at` at display time.

SCOPE rendering:

- Single-scope delegated contexts show the full scope path.
- Multi-scope delegated contexts (§6 allows multiple scopes per grant) show the first scope followed by `+N`, where `N` is the count of remaining scopes.
- Owner contexts render as `*` (unbounded).
- To see the full scope list, use `drive9 ctx ls -l` (long form) or `drive9 ctx ls --json`.

### 13.3 `ctx import` contract (MUST)

Input modes:

- `drive9 ctx import --from-file <path>` reads the JWT from a file.
- `drive9 ctx import --from-file -` reads the JWT from stdin.
- `drive9 ctx import <jwt>` reads the JWT from the positional argument (convenience; see security note below).

In all three modes, the JWT must be a single token with surrounding whitespace trimmed.

Contract rules:

- Input **MUST** be a delegated JWT. If the payload indicates `principal_type=owner` (or any non-delegated credential), `ctx import` **MUST** refuse and instruct the user to use `ctx add --api-key`. `ctx import` is not a universal credential importer.
- If the JWT's `exp` is already in the past at import time, `ctx import` **MUST** refuse (local short-circuit #1 — see §17). The `exp` check uses the local wall clock with no skew tolerance in v0; delegatees with badly skewed clocks will see spurious refusals or admissions and are expected to fix their clocks (NTP).
- Default context name is the JWT's `label_hint`; on collision or absence, fall back to `<agent>-<scope-root>` with a numeric suffix as needed. `--name` overrides.
- The positional-argument form **SHOULD NOT** be used in interactive shells — the JWT would be recorded in shell history and exposed via `/proc/<pid>/cmdline` while the process runs. Owners **SHOULD** distribute grants as files or piped input; the default grant output (§6) is formatted accordingly.

**Issuer trust note (client-side first-use trust).** `ctx import` populates the delegated context's `server` field from the JWT's `iss` claim with **no network round-trip and no allow-list check**. This is trust-on-first-use: a fresh delegatee who imports a maliciously crafted JWT with attacker-controlled `iss` (self-signed by the attacker) will write an attacker server URL into their config, and subsequent requests will be routed there. Invariant #7 ("server MUST re-validate") does **not** protect against this path — the server being contacted is itself attacker-controlled and will happily validate its own signatures. Mitigation is out of scope for v1 and lives with the owner's distribution channel: grants **MUST** be transmitted through a channel that authenticates the sender (password-manager share, Signal, GPG-signed email — not plaintext email, not public paste services). A follow-up hardening path (issuer allow-list pinned at `ctx add --api-key` time, or an explicit `--expect-issuer` flag on `ctx import`) is tracked as a non-blocking follow-up; it is additive to the current payload and does not change §13.1 / §13.2 / §16.

## 14. Environment Variables — Explicit Override

Environment variables are **not** the primary credential channel. They exist as an explicit override for ephemeral / non-interactive contexts (CI jobs, one-off scripts, air-gapped shells).

### 14.1 Variables

- `DRIVE9_API_KEY` — owner credential override.
- `DRIVE9_VAULT_TOKEN` — delegated credential override (JWT).
- `DRIVE9_SERVER` — server URL override (orthogonal to credentials — see §14.2).

The **dual-principal separation is locked**: there is no single combined variable. `DRIVE9_VAULT_TOKEN` and `DRIVE9_API_KEY` remain distinct knobs; collapsing them is prohibited.

### 14.2 Priority

Credential resolution (first match wins):

1. `DRIVE9_VAULT_TOKEN` (narrower — delegated)
2. `DRIVE9_API_KEY` (broader — owner)
3. Active context in `~/.drive9/config`

Rule 1 vs 2 implements narrower-wins so that a scoped token never falls back to owner authority within the env channel. Rule 3 means the active context only applies when no env override is present.

Server URL resolution is **orthogonal** to credential resolution:

1. `DRIVE9_SERVER` (if set)
2. The `server` field of the active context

`ctx use` does **not** lock server and credential together: if `DRIVE9_SERVER` is set, it overrides the context's `server` field even when the active context is used for credentials. If the resulting (server, credential) pair is mismatched (e.g. a JWT signed by a different issuer), the server rejects the request with `EACCES` via the standard stale-auth path (§11). No new error model is introduced.

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
# drive9 ctx import --from-file -
# vt_eyJhbGc...
# ---
# grant_id:   grt_7f2a
# expires_at: 2026-04-18T19:00:00Z
```

Owner sends Alice the whole block through a secure channel (password-manager share, Signal, password-protected email). The JWT line is the bearer credential; do not paste it as a positional arg into a shell.

### Alice

```bash
install -m 600 /dev/null ~/alice-grant.jwt
$EDITOR ~/alice-grant.jwt                # paste the JWT body, save
drive9 ctx import --from-file ~/alice-grant.jwt  # decodes JWT locally, writes delegated context
rm ~/alice-grant.jwt

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

The JWT payload is self-describing and **MUST** contain the following claims:

| Claim | Purpose |
|---|---|
| `iss` | Issuer server URL; used by `ctx import` to populate the context's `server` field with no network call. |
| `grant_id` | Server-assigned grant identifier (e.g. `grt_7f2a`); appears in audit and is the argument to `vault revoke`. |
| `principal_type` | `delegated` (see §13.3 for the refusal rule on other kinds). |
| `agent` | Agent ID as named by the owner at `vault grant --agent`; appears in audit and is the default prefix for context `name`. |
| `scope[]` | List of granted paths (whole-secret or single-key; §6). |
| `perm` | `read` or `write`. |
| `exp` | Unix expiration timestamp. |
| `label_hint` | Optional short display label; used as default context `name` when present. |

Clients **MAY** decode this payload locally to populate `ctx` metadata and to render `ctx ls` offline.

However:

- **The server MUST re-validate signature, TTL, and revocation status on every request.** The local decode is a UX convenience only.
- **Clients MUST NOT treat the decoded payload as authoritative for access decisions.** A decoded token payload is never a substitute for a server-side check.
- **Servers MUST NOT weaken their check based on what the client claims the token says.** Tampered tokens fail signature verification regardless of client-side decoding outcomes.

This is locked as Invariant #7.

**Client endpoint trust is outside §16.** The three `MUST` clauses above bind the server's side of the contract. They do **not** protect a client from being pointed at a rogue server via a malicious `iss` claim — because a rogue server will happily "re-validate" its own signatures. On the delegatee bootstrap path, `ctx import` trusts the JWT's `iss` on first use with no allow-list check; see §13.3 for the TOFU note and the recommended follow-up hardening. Mitigation in v0 rests on the owner's grant-distribution channel authenticating the sender.

## 17. Auth Lifecycle — Local Short-Circuits vs Server Checks

A mount is bound to one credential at mount time and does not silently follow later context changes (Invariant #3). Running `ctx use <other>` after mounting **does not re-bind** the mount; the owner must call `vault reauth <mountpoint>` (§12) to rebind to the current active context. This is the intended behaviour — it keeps the authority model predictable for long-running mounts — and is captured in Invariant #6.

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
- No client-side issuer pinning or allow-list in v0. `ctx import` trusts the JWT `iss` on first use (§13.3 TOFU note). A follow-up spec may introduce `ctx add --trusted-issuer` and/or an `--expect-issuer` flag on `ctx import`; both are additive and do not change §13.1 or §16.

## 21. Open Questions (Spec-Level)

- None at publication time. Review may add.

---

Appendix A — Command surface at a glance:

| Command | Role |
|---|---|
| `drive9 mount vault <path>` | Mount the vault namespace under `<path>`. |
| `drive9 umount <path>` | Unmount. |
| `drive9 ctx add --api-key <k>` | Register an owner context. |
| `drive9 ctx import --from-file <path>` | Register a delegated context from a grant JWT file (primary UX). |
| `drive9 ctx import --from-file -` | Same, reading the JWT from stdin (recommended for piping). |
| `drive9 ctx import <jwt>` | Convenience form; leaks to shell history (see §13.3 / §16). |
| `drive9 ctx ls / use / rm` | Manage contexts (offline). |
| `drive9 vault put <path> --from <dir> [--prune]` | Atomic batch write. |
| `drive9 vault grant <scope>... --agent --perm --ttl` | Issue a scoped JWT. |
| `drive9 vault revoke <grant-id>` | Revoke a grant. |
| `drive9 vault with <path> -- <cmd>` | Exec child with `@env` injected. |
| `drive9 vault reauth <mountpoint>` | Rebind a running mount to the current context. |
| `cat / ls / rm / printf >` on `/n/vault/**` | Data plane. |

Appendix B — Canonical history: §0–§12 absorb `89603ee6`; §13–§17 are the v2 context-unified credential increment over that canonical; Invariants #1–#5 derive from `89603ee6`; Invariants #6–#7 are new.
