# drive9 vault — End-State Interaction Spec

Status: Proposed (four-way review: dev1 author, architect-1 / dev2 / adversary-2 review)
Scope: End-state CLI is the normative surface (§0–§22 + Appendices). No P0 tactics. The `## Implementation status` section is the single allowed exception to "end-state only": it exists purely so reviewers can audit whether every Appendix A verb is live on `main` and gate the `Proposed → Accepted` flip; the normative spec itself does not describe any transitional shape.

This spec is the single source of truth for the terminal shape of vault UX. It is the merged canonical of:

- `89603ee6` (four-way-signed terminal CLI surface) — §0–§12 below, absorbed verbatim in intent.
- v2 context-unified credential increment (grant token → `ctx` entry, env var = override) — §13–§17 below.

## 0. Mental Model

- **secret** = directory (e.g. `prod-db`)
- **key** = file inside that directory (e.g. `prod-db/DB_URL`)
- **Read/write a key**: POSIX file ops on `/n/vault/**` (`cat`, `printf >`, `ls`, `rm`)
- **Control plane**: 4 verbs — `put`, `grant`, `revoke`, `with`. A running mount's credential binding is fixed at mount time (Invariant #3); to change it, `umount` and `mount` again. An in-process rebind verb (`vault reauth`) was considered for M1 and deferred to a post-M1 increment — see §17.

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

## 2. Create or Replace a Secret (Batch Write)

```bash
mkdir -p ./prod-db.envdir
printf '%s' 'postgres://db.example.com:5432/app' > ./prod-db.envdir/DB_URL
printf '%s' 'app_user'                           > ./prod-db.envdir/DB_USER
printf '%s' 'super-secret-password'              > ./prod-db.envdir/DB_PASSWORD

drive9 vault put /n/vault/prod-db --from ./prod-db.envdir
```

`put` is defined as **wholesale replace**: the secret's visible key set after the call equals the key set in `<dir>`. Keys that exist server-side but are absent from `<dir>` are deleted; keys present in `<dir>` are written. There is exactly one mode — no merge/upsert flag, no `--prune` flag. Incremental edits use the data-plane (§3 `printf >` / §5 `rm`).

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

## 4.1 Virtual-File Output Contract (`@env`)

`@env` is a virtual file whose byte contract is normative. Consumers (human pipeline, `drive9 vault with`, CI scripts) MUST be able to parse it with no ambiguity.

### 4.1.1 Output format

For each key–value pair visible under the current principal, `cat @env` emits exactly one line:

```
<KEY>=<QUOTED_VALUE>\n
```

- `<KEY>` is the secret key name, unchanged, restricted to the charset `[A-Z_][A-Z0-9_]*`. Any key outside this charset MUST NOT appear in `@env`; see §4.1.3.
- `<QUOTED_VALUE>` is the value escaped per POSIX `printf %q` semantics (shell-safe round-trippable).
- Line terminator is `\n` (LF) only. The last line MUST also be LF-terminated.
- Lines are emitted in **lexicographic order of `<KEY>`** (byte-wise ASCII sort) so output is deterministic and diff-stable.

### 4.1.2 Empty-secret semantics

- Secret exists but has zero visible keys → `cat @env` writes 0 bytes and exits 0.
- Secret does not exist (or is invisible under the current principal) → `cat @env` fails with `ENOENT` (see §11). Consumers MUST distinguish "empty" (exit 0, 0 bytes) from "missing" (ENOENT) by exit code, not by byte count.

### 4.1.3 Illegal-key handling (fail-fast)

If any visible key violates `[A-Z_][A-Z0-9_]*`, `cat @env` MUST fail with `EACCES` and emit no partial output. The spec does not silently skip illegal keys, and does not coerce them (no lower-to-upper casing, no `-` → `_` substitution). Callers wanting to inject such keys into a child process must materialise them explicitly via `drive9 vault with` (which applies the same rejection) or handle them out of band.

If any value contains a control character (`\x00`–`\x1f` except `\t`) the same `EACCES` rule applies; `printf %q` is not defined over unrestricted control bytes and the contract refuses to invent framing.

### 4.1.4 Other whole-secret views

- `cat @all` — JSON object of all visible keys. **Byte-exact contract (key ordering, whitespace, escaping) is deferred to a follow-up spec** (PR-F). In v0, `@all` is consumed as valid JSON only; consumers MUST NOT depend on formatting.
- `cat @grants/<grant-id>` — owner-only introspection of a grant. **Byte-exact contract deferred to PR-F** (same reason). In v0, only presence/absence and the grant's scope/perm/expiry fields are stable; string representation is not.

Consumers that need stable byte output today MUST use `@env` (or `--json` on a control-plane verb; see §20).

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

`vault with` **MUST** strip `DRIVE9_API_KEY`, `DRIVE9_VAULT_TOKEN`, and `DRIVE9_SERVER` from the child's environment before injecting `@env`. Rationale: the parent shell may hold drive9 credentials for a different principal (e.g. the caller is a delegatee in one tenant and an owner in another); inheriting those alongside the injected `@env` would give the child ambient authority beyond the scope of the grant being used. The strip is unconditional — it applies even when the parent's `DRIVE9_*` variables are unset, absent, or identical to the current mount's credential — to keep the child's environment a function of `@env` alone.

## 10. Audit

```bash
tail -f /n/vault/@audit                   # global stream
tail -f /n/vault/prod-db/@audit           # per-secret stream
```

Events: `put / read / write / grant / revoke / rm`.
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

`@audit` (global and per-secret) and `@grants/` are **owner-only**. Delegated principals receive `ENOENT` for these pseudo-entries and their contents — including `ls @grants/` returning no entries and `cat @grants/<id>` returning `ENOENT` for every grant id, even the delegatee's own. This is a direct application of the existence-oracle rule (Invariant #2 / §11): audit metadata and grant identity/activity would otherwise leak other agents' IDs, scopes, and source IPs to every delegatee in the tenant. The rule reuses the locked `ENOENT` case in §11; no new errno is introduced.

## 11. Errno Table (Normative)

| Scenario | errno |
|---|---|
| Secret does not exist | `ENOENT` |
| Key does not exist | `ENOENT` |
| Key exists but current principal has no permission | `ENOENT` (existence-oracle defense) |
| Current principal attempts an unauthorized **write** | `EACCES` |
| Bound credential expired / revoked | `EACCES` + remount hint |
| Infrastructure failure (FUSE daemon, backend) | `EIO` |

Core rules:
- **Reads**: invisible == non-existent (`ENOENT`). Key names are themselves sensitive metadata.
- **Writes**: `EACCES`. The write intent already names the key, so a fake-not-found would be a lie without benefit.
- **Stale auth**: `EACCES`, distinguished from “not found.”

`ctx import` refusal causes — these are client-side command errors that also map to POSIX errno when the CLI exits. The CLI exit code convention is `2` for `EINVAL` (usage, per `sysexits.h` `EX_USAGE`) and `1` for `EACCES` / `ENOENT`.

| `ctx import` refusal cause | Errno | Exit code |
|---|---|---|
| Input empty after whitespace trim | `EINVAL` | `2` |
| Not a structurally valid JWT (three base64url segments, JSON middle) | `EINVAL` | `2` |
| Missing `principal_type` claim | `EINVAL` | `2` |
| `principal_type` is not `"delegated"` | `EINVAL` | `2` |
| Missing `exp` claim | `EINVAL` | `2` |
| `exp` already in the past at import | `EACCES` | `1` |
| Missing `iss` claim | `EINVAL` | `2` |
| Missing `agent` claim | `EINVAL` | `2` |
| Missing `grant_id` claim | `EINVAL` | `2` |
| Missing or empty `scope[]` claim | `EINVAL` | `2` |
| Missing `perm` claim | `EINVAL` | `2` |
| `perm` not in `{read, write}` | `EINVAL` | `2` |
| `--from-file <path>` names a non-existent or unreadable file | `ENOENT` | `1` |
| `--from-file <path>` has mode group- or world-readable (`mode & 0o077 != 0`) | `EACCES` | `1` |
| Stdin is a TTY and no input flag given | `EINVAL` (with help pointer) | `2` |

This table is locked. The auth lifecycle (§17) layers **local short-circuits** (e.g. `ctx use` on an expired context refuses client-side) **on top of** the server-side stale-auth case — those short-circuits do not introduce a new errno.

**Annotation convention.** Where example outputs in this spec and the quickstart show errno lines followed by parenthetical guidance (e.g. `cat: Permission denied  (run 'drive9 umount /n/vault && drive9 mount vault /n/vault' after updating the context)`), the parenthetical is a **documentation annotation** intended for the reader, not literal `cat`/`ls` stderr output. The POSIX `cat`/`ls` utilities emit only the errno text; the remount hint is a spec-level explanation of what the user should do next, delivered out-of-band (man page, quickstart). No new verb is introduced to deliver this hint inline.

## 12. Recovery After Credential Rotation

```bash
drive9 ctx use <new-context>
drive9 umount /n/vault
drive9 mount vault /n/vault
```

A mount's credential binding is fixed at mount time (Invariant #3). To rotate to a new context, `umount` the running mount and `mount` again — the new mount picks up the active context on startup. There is no in-process rebind in M1; `vault reauth` is deferred to a post-M1 increment (§17).

Operator note: `umount` refuses with `EBUSY` while any process holds an open file descriptor under the mount (Linux `fusermount3 -u`; macOS `umount`). Stop or detach those processes before unmounting. Best-effort: writes that are already staged in the write-back cache survive a clean `umount` and are re-attempted by the next `mount` instance; they are **not** guaranteed to recover losslessly under arbitrary failure modes (e.g. token expiry mid-flush) — the supported pattern is to quiesce writers before remounting.

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
drive9 ctx import --from-file <path>                              # add delegated context from a file
drive9 ctx import [--from-file -]                                 # add delegated context from stdin (default when stdin is a pipe)
drive9 ctx ls [-l|--json]                                         # list contexts (offline — reads only local config)
drive9 ctx use <name>                                             # activate a context
drive9 ctx rm <name>                                              # delete a context
```

Both `ctx import` forms are equivalent in effect. Stdin is read by default when stdin is a pipe (`isatty(0) == false`); the explicit `--from-file -` form is accepted for scripts that want the intent to be unambiguous. When stdin is a TTY and no `--from-file` is supplied, `ctx import` exits with `EINVAL` and prints a one-line help pointing at the two canonical forms (see §13.3).

**Canonical pipe handoff.** The default human output of `drive9 vault grant` (see §6) is *not* pipe-safe — it prints `grant_id` and `expires_at` lines in addition to the JWT. To pipe grant output directly into `ctx import`, use `drive9 vault grant ... --token-only`, which prints only the bare JWT. The end-to-end canonical pipeline is:

```bash
drive9 vault grant <scope> --agent <a> --perm <p> --ttl <t> --token-only | drive9 ctx import
```

The human default form is intentionally non-parseable; it exists so an owner reading their terminal can eyeball the grant id and expiry, not so it can be piped.

`ctx ls` output:

```
CURRENT   NAME              TYPE        SCOPE                      PERM   EXPIRES_AT            STATUS
          owner-prod        owner       *                          rw     —                     active
*         alice-prod-db     delegated   prod-db/DB_URL             read   2026-04-18T19:00:00Z  active
          alice-multi       delegated   prod-db/DB_URL +1          read   2026-04-18T19:00:00Z  active
          rotator-pwd       delegated   prod-db/DB_PASSWORD        write  2026-04-18T18:10:00Z  expired
```

`CURRENT` is a dedicated column (modeled on `kubectl config get-contexts`): exactly one row holds `*`, the rest are blank. `STATUS` is computed locally from `expires_at` at display time and is independent of `CURRENT`.

SCOPE rendering:

- Single-scope delegated contexts show the full scope path.
- Multi-scope delegated contexts (§6 allows multiple scopes per grant) show the first scope followed by `+N`, where `N` is the count of remaining scopes.
- Owner contexts render as `*` (unbounded).
- To see the full scope list, use `drive9 ctx ls -l` (long form) or `drive9 ctx ls --json`.

### 13.3 `ctx import` contract (MUST)

Input modes:

- `drive9 ctx import --from-file <path>` reads the JWT from a file.
- `drive9 ctx import --from-file -` reads the JWT from stdin explicitly.
- `drive9 ctx import` (no arguments) reads the JWT from stdin iff stdin is not a TTY. When stdin is a TTY, `ctx import` exits `EINVAL` and prints:

  ```
  error: no JWT on stdin. Use one of:
    drive9 ctx import --from-file <path>
    <producer> | drive9 ctx import
  ```

In both modes (file and stdin), the JWT must be a single token with surrounding whitespace trimmed. The JWT **MUST NOT** be passed as a positional argument; that form was removed because a runtime warning cannot unexpose a secret that has already reached shell history and `/proc/<pid>/cmdline`.

Contract rules:

- Input **MUST** be a delegated JWT. If the payload indicates `principal_type=owner` (or any non-delegated credential), `ctx import` **MUST** refuse and instruct the user to use `ctx add --api-key`. `ctx import` is not a universal credential importer.
- If the JWT's `exp` is already in the past at import time, `ctx import` **MUST** refuse (local short-circuit #1 — see §17). The `exp` check uses the local wall clock with no skew tolerance in v0; delegatees with badly skewed clocks will see spurious refusals or admissions and are expected to fix their clocks (NTP).
- The JWT **MUST** contain all required delegated-context claims: `iss`, `exp`, `principal_type=delegated`, `agent`, `grant_id`, `scope[]` (non-empty array), and `perm` ∈ `{read, write}`. Missing or malformed required claims refuse at import; the full per-claim error mapping is in §11.
- When `--from-file <path>` is given, the file **MUST** be mode `0600` (no group or world permission bits). If `stat.Mode().Perm() & 0o077 != 0`, `ctx import` refuses **before reading the contents** with `EACCES` and points the user at `chmod 600`. Rationale: a bearer-token file that has leaked to other local users is already-exposed credential; silently consuming it makes the CLI complicit in the lifecycle breach. This matches the argv-removal posture: the tool does not ingest credentials that have already escaped their intended confidentiality boundary.
- Default context name is the JWT's `label_hint`; on collision or absence, fall back to `<agent>-<scope-root>` with a numeric suffix as needed. `--name` overrides.

**Issuer trust note (client-side first-use trust).** `ctx import` populates the delegated context's `server` field from the JWT's `iss` claim with **no network round-trip and no allow-list check**. This is trust-on-first-use: a fresh delegatee who imports a maliciously crafted JWT with attacker-controlled `iss` (self-signed by the attacker) will write an attacker server URL into their config, and subsequent requests will be routed there. Invariant #7 ("server MUST re-validate") does **not** protect against this path — the server being contacted is itself attacker-controlled and will happily validate its own signatures. Mitigation is out of scope for v0 and lives with the owner's distribution channel: grants **MUST** be transmitted through a channel that authenticates the sender (password-manager share, Signal, GPG-signed email — not plaintext email, not public paste services). A follow-up hardening path (issuer allow-list pinned at `ctx add --api-key` time, or an explicit `--expect-issuer` flag on `ctx import`) is tracked as a non-blocking follow-up; it is additive to the current payload and does not change §13.1 / §13.2 / §16.

## 14. Environment Variables — Explicit Override

Environment variables are **not** the primary credential channel. They exist as an explicit override for ephemeral / non-interactive contexts (CI jobs, one-off scripts, air-gapped shells).

### 14.1 Variables

- `DRIVE9_API_KEY` — owner credential override.
- `DRIVE9_VAULT_TOKEN` — delegated credential override (JWT).
- `DRIVE9_SERVER` — server URL override (orthogonal to credentials — see §14.2).

The **dual-principal separation is locked**: there is no single combined variable. `DRIVE9_VAULT_TOKEN` and `DRIVE9_API_KEY` remain distinct knobs; collapsing them is prohibited.

### 14.2 Priority

Credential resolution (first match wins):

1. Explicit CLI flag (`--api-key`, etc., where the verb accepts one)
2. `DRIVE9_VAULT_TOKEN` (narrower — delegated)
3. `DRIVE9_API_KEY` (broader — owner)
4. Active context in `~/.drive9/config`

Rules 2 vs 3 implement narrower-wins so that a scoped token never falls back to owner authority within the env channel. Rule 4 means the active context only applies when no flag or env override is present.

**Explicit-empty flag values are rejected at parse time.** `--api-key=""` (or `--server=""`) MUST fail with a client-side error before any credential resolution runs. Treating an explicit empty string as "unset and fall through" would reintroduce the same silent-fallthrough class the set-but-invalid rule below closes — named, empty, and missing are three distinct states, and the only safe handling for "named-but-empty" is to refuse.

A set-but-invalid `DRIVE9_VAULT_TOKEN` (malformed, expired, revoked, or signed by an unknown issuer) fails as `EACCES` via the standard stale-auth path in §11. It **MUST NOT** fall through to `DRIVE9_API_KEY` or to the active context — "first match wins" is token-presence, not token-validity. Users who want the broader owner authority must unset `DRIVE9_VAULT_TOKEN` explicitly.

Server URL resolution is **orthogonal** to credential resolution:

1. Explicit `--server` flag
2. `DRIVE9_SERVER`
3. The `server` field of the active context

`ctx use` does **not** lock server and credential together: if `DRIVE9_SERVER` is set, it overrides the context's `server` field even when the active context is used for credentials. If the resulting (server, credential) pair is mismatched (e.g. a JWT signed by a different issuer), the server rejects the request with `EACCES` via the standard stale-auth path (§11). No new error model is introduced.

### 14.3 Activation mechanics

At most one credential is bound to a mount. When both env overrides and an active context exist, the env override wins. This is a mechanism detail, not a second authorization layer — the chosen credential is then validated by the server on every request (Invariant #7).

**Unsetenv-after-read (mitigation).** After the credential resolver reads `DRIVE9_VAULT_TOKEN`, `DRIVE9_API_KEY`, and `DRIVE9_SERVER`, it **MUST** unset those variables in the current process before spawning any child (e.g. `secret exec`, subprocesses invoked by CLI verbs). This prevents a drive9 invocation from leaking the caller's credential into a descendant process through `/proc/<pid>/environ` or through `os.Environ()` propagation. Callers that explicitly need a child to see a drive9 credential must re-inject it through the child's declared ingress (a file, a `--token` argument the child accepts, or its own env lookup before spawning drive9). This rule applies even when another credential "won" priority — all three variables are always sunk, regardless of which one was consumed, to avoid stranded leakable values.

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

Owner sends Alice the whole block through a secure channel (password-manager share, Signal, password-protected email). The JWT line is the bearer credential; Alice saves it to a file or pipes it into `ctx import` — see §13.3.

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
# cat: Permission denied  (run `drive9 umount /n/vault && drive9 mount vault /n/vault` after updating the context)
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

A mount is bound to one credential at mount time and does not silently follow later context changes (Invariant #3). Running `ctx use <other>` after mounting **does not re-bind** the mount. To change a running mount's credential, the owner `umount`s and then `mount`s again; the new mount binds to whatever the active context (or env override) resolves to at startup. This is the intended behaviour — it keeps the authority model predictable for long-running mounts — and is captured in Invariant #6.

**`vault reauth` is deferred.** An in-process rebind verb was considered for M1 and excluded: introducing it requires a second drive9-process control plane (Unix-domain socket listener in the mount process, a CLI-side dialer, a mount-disambiguation identity, and atomic client-pointer swap inside every FUSE op). The `umount + mount` escape hatch covers the same functional contract without expanding the IPC surface. A post-M1 spec increment MAY reintroduce `vault reauth` if operational evidence shows the remount cadence is disruptive; that increment is additive and does not alter the rules below.

Local short-circuits exist to make UX responsive. They are layered **on top of** the normative errno table (§11), not instead of it.

| Stage | Check | Outcome |
|---|---|---|
| `ctx import` | `exp` in past? | local refuse (no new errno; command error) |
| `ctx ls` | `exp` in past? | row marked `expired` |
| `ctx use <name>` | target context expired? | local error, do not activate |
| `mount` | context valid locally? | proceed; server then validates |
| Any FS op | server says stale / revoked | `EACCES` + remount hint (§11) |

The three local short-circuits (`ctx import` / `ctx ls` / `ctx use`) are client-side UX. They do **not** introduce a new errno case. The locked 6-row errno table in §11 is unchanged.

## 18. Invariants (Normative, numbered)

1. **Atomic `put`**: a put transaction is visible to concurrent readers either entirely in the old state or entirely in the new state; no half-update is observable. `put` has exactly one mode (wholesale replace, §2).
2. **Existence oracle defense**: reads return `ENOENT` for both non-existent and invisible keys; the two are indistinguishable to clients.
3. **One mount, one principal**: a mount is bound to exactly one credential at mount time; stale/revoked credentials do not silently fall back to any other identity.
4. **Field names are sensitive metadata**: key names are not disclosed via errno, audit (to the delegatee), or listing unless the principal has permission.
5. **Grants do not cascade-revoke on `rm`**: removing a key leaves existing grants syntactically intact; holders observe `ENOENT`, and audit records `affected_grants`.
6. **One active context at a time**: `~/.drive9/config` MAY hold any number of contexts (owner and delegated, mixed); at most one is active. Switching contexts does not silently re-bind an already-mounted mount. To change a running mount's credential, `umount` and `mount` again; the new mount picks up the current active context (or env override) at startup. An in-process rebind (`vault reauth`) is **not** part of M1 (§17) — it MAY be added in a later spec increment without altering this invariant.
7. **Client-side JWT decoding is UX-only**: local decode populates `ctx` metadata and enables offline `ctx ls`; it **MUST NOT** substitute for server-side validation. The server **MUST** re-check signature, TTL, and revocation on every request.
8. **Issuer trust is TOFU (trust-on-first-use) in v0**: `ctx import` populates the context's `server` field from the JWT's `iss` claim with no network round-trip and no allow-list check. Invariant #7 does **not** protect against a malicious `iss` — the server being contacted is itself attacker-controlled and will validate its own signatures. Mitigation is delivery-channel-level (see §13.3 and §16); an issuer allow-list / `--expect-issuer` path is deferred (see §22). Implementations **MUST NOT** add a silent issuer check that only validates shape or reachability; such a check provides false assurance and is prohibited.

## 19. Failure Model (Summary)

| Failure | Detection | Client visible |
|---|---|---|
| Expired / revoked credential | server on next request | `EACCES` + remount hint |
| Server unreachable | client | `EIO` |
| FUSE daemon crash | kernel | `EIO` |
| Malformed JWT at `ctx import` | client local decode | command error, no context written |
| Import of wrong credential kind (owner JWT, random string) | client local decode | command error, directing user to `ctx add --api-key` |
| Concurrent `put` reads during transaction | server transaction | atomic — readers see old or new (Invariant #1) |

## 20. I/O Contracts (CLI Emit Surface, Normative)

§20 defines the I/O framing contract for every `drive9` CLI verb specified at or after this section. It exists to preserve Unix-pipe composability ("reuse POSIX, don't invent new fan-in/fan-out protocols") while keeping credential-material and identifier-material on strictly separated channels.

**Scope.** Rules 1–5 apply to **Layer 2 (control-plane emit surface)** and **Layer 3 (state-binding verbs)** of the CLI. They do **not** apply to:
- **Layer 1** data-plane reads through the mounted FUSE tree (`cat /n/vault/<s>/<k>`, `ls`, `rm`, `printf >`) — the POSIX byte contract governs those, not §20.
- Verbs specified **before** this spec increment (legacy `drive9 secret get/grant/revoke/exec` etc.) — §20 is **applies-forward** and does not re-spec those surfaces.

**Identifier Invariant (Normative MUST).** The tokens `grant_id`, context name, and `scope path` are **handles**, not credentials. No verb MUST accept them as authentication input or as a means to re-derive/retrieve a token. They are safe to pass as command-line arguments, to log, and to distribute in-band. Credentials (`DRIVE9_API_KEY`, `DRIVE9_VAULT_TOKEN`, JWT bodies) remain §14 / §16 governed and MUST NOT flow through argv.

### Rule 1 — Emit mode is a three-way mutex

Every Layer-2 verb that produces machine-readable output MUST expose exactly one of three emit modes per invocation, selected by mutually-exclusive flags:

| Mode | Flag | Shape |
|---|---|---|
| `human` (default) | (no flag) | Unstructured multi-line text for terminal use. Not a stable contract. |
| `json` | `--json` | Single JSON object to stdout, trailing LF. Stable contract. |
| `token-only` | `--token-only` | Raw credential/artifact bytes to stdout, trailing LF. Stable contract. Intended for pipe composition. |

Any two flags combined → `EINVAL` with message `"--<a> and --<b> are mutually exclusive"`. The mutex is enforced per-verb with a spec-locked flag-class table; a verb MUST declare which modes it supports and MUST reject unsupported mode flags at argv-parse time.

**Out of scope for Rule 1.** Layer-3 state-binding verbs (§Rule 4) emit a single human confirmation line only; they do **not** expose `--json` or `--token-only`.

### Rule 2 — Exit codes follow sysexits.h

- `0` — success
- `1` — runtime errno mapped from the operation (e.g. ENOENT, EACCES). stderr carries the human errno hint.
- `2` — EINVAL: argv parse failure, flag mutex violation, missing required flag. stderr carries the usage line.
- `≥64` — reserved for future sysexits codes; not used in v0.

No verb may exit 0 on a partially-satisfied request. See Rule 3 for multi-scope continue-on-error semantics.

### Rule 3 — Stdin vs argv is determined by payload class

Verbs MUST route input by the **class of payload**, not by convenience:

| Class | Channel | Rationale |
|---|---|---|
| **1. Credential / bulk payload** (JWT body, large blob) | stdin (default when stdin is a pipe); optional `--from-file <path>` for explicit file read; `--from-file -` for explicit stdin | Credential material MUST NOT appear in argv (visible in `ps`, shell history, `/proc/<pid>/cmdline`). Bulk payloads exceed argv size limits on some platforms. |
| **2. Identifier list** (`grant_id`, context name, scope path) | Variadic argv: `<id1> <id2> …` | Identifiers are non-credential handles (Identifier Invariant). Fan-in composition MUST use POSIX `xargs`, not a CLI-internal stdin protocol. |

**Fan-in example** (class 2):

```bash
drive9 vault revoke grt_7f2a grt_9c13 grt_bbaa
# or via xargs:
cat ids.txt | xargs drive9 vault revoke
```

**Continue-on-error semantics for variadic class-2 verbs.** When processing more than one identifier, the verb MUST attempt every identifier, collect per-identifier errors, and exit with the **first** non-zero errno (preserving the semantic of the earliest failure). stderr emits one line per failed identifier. A partial success MUST NOT exit 0.

**Class-2 verbs ignore stdin.** When argv supplies one or more identifiers, stdin is not consumed. Redirecting stdin into a class-2 verb is not an error but the input is discarded — the verb is not a filter.

**Not in scope for Rule 3.** `drive9 ctx rm <name>` remains single-arg per §13 / B1; this increment does not re-spec it as variadic. A future spec increment may extend `ctx rm` if needed.

### Rule 4 — State-binding verbs are not filters

Verbs that bind local state (current context, filesystem mount point) — `ctx use`, `mount` — MUST take their target as an explicit argv argument and MUST NOT read stdin. They produce a single human confirmation line on stdout and are **exempt from Rule 1's `--json`/`--token-only` surface**. A future `vault reauth` verb (see §17, deferred post-M1) falls under this rule when introduced.

Rationale: state-binding verbs change global principal or mount identity. Allowing them to be the right-hand side of a pipe (`… | ctx use`) would let an unrelated producer silently rebind credentials for subsequent commands. The contract forbids it structurally; if a caller wants scripted rebinding, they compose with argv (`ctx use "$(compute_name)"`) so the data flow is explicit.

### Rule 5 — Advertised composition requires an executable example

Every verb that documents a pipe or composition pattern (`A | B`, `A | xargs B`) MUST ship at least one runnable example in the quickstart (`docs/guides/vault-quickstart.md`) whose exit code is asserted. Compliance is checked by the `quickstart-smoke-test` CI harness when available.

**Transition clause (rule #5 enforcement deferral).** Until the `quickstart-smoke-test` CI harness (introduced by a future PR, PR-G) lands, "literal runnable" is satisfied by **review-time manual grep plus an exit-code assertion written in the code block's inline comment** (e.g. `# exit 0`). **PR-G merge is the sole triggering SHA for rule #5 enforcement**: once PR-G merges, the harness MUST run on CI before the next spec/code PR cycle (i.e. the harness becomes a required check). PR-G is the sole deferral gate; **no calendar fallback is set**. If PR-G is re-scoped or cancelled, the owner of this spec (`dev1` as §20 author) MUST open a follow-up spec-increment to explicitly adjust rule #5's enforcement path. Silent decay is not permitted.

---

**Appliesforward scope.** §20 applies to verbs specified at or after this section's merge SHA. Pre-existing verbs retain their current contract; any subsequent spec increment that touches one of them MUST bring it into §20 compliance as part of the same delta.

## 21. Non-Goals

- No migration or backward-compatibility surface in this spec; this document is terminal-state only.
- No single unified credential variable that merges `DRIVE9_API_KEY` and `DRIVE9_VAULT_TOKEN` (the dual-principal separation is a contract, §14.1).
- No wildcard scopes in v0 (`*` in key/scope is rejected at parse time).
- No client-side authorization (Invariant #7).
- No automatic token auto-mint on behalf of the owner; every delegated credential must come from an explicit `vault grant`.
- No client-side issuer pinning or allow-list in v0. `ctx import` trusts the JWT `iss` on first use (§13.3 TOFU note). A follow-up spec may introduce `ctx add --trusted-issuer` and/or an `--expect-issuer` flag on `ctx import`; both are additive and do not change §13.1 or §16.

## 22. Open Questions (Spec-Level)

- **Issuer trust hardening (TOFU → pinned).** Invariant #8 locks v0 at trust-on-first-use. A follow-up spec should decide between (a) an issuer allow-list pinned at `ctx add --api-key` time, (b) an `--expect-issuer <url>` flag on `ctx import`, or (c) an out-of-band manifest fetched from the owner server during `ctx add`. Each has different forward-compat implications for `/etc/drive9.conf` site-policy files; none are trivially additive once deployed. Resolution target: the release that introduces multi-issuer federation.
- **Forward-compat of the `iss` claim under server rebranding / domain migration.** If an owner server migrates from `https://d9.old.example` to `https://d9.new.example`, all outstanding delegated contexts hold the old `iss` and will route to the old host. v0 has no in-band way to rotate `iss` across existing grants. A follow-up should specify whether this is handled by (a) explicit re-grant + `ctx import`, (b) a server-signed redirect manifest keyed off the old `iss`, or (c) left as "owner reissues all delegated tokens". Resolution target: the release that introduces `vault reauth --server <new>` or equivalent.

---

## Implementation status

The table below tracks whether each Appendix A verb is live on `main`. It is the single source of truth that reviewers use to decide when `Status: Proposed` may flip to `Accepted` (see header): the flip happens only once every row in the `drive9 vault` / `drive9 mount vault` block reads either `implemented` or `descoped-#NNN` (i.e. no row is left `not-yet`). Rows for verbs already shipped by merged PRs are pinned here for auditability; subsequent PRs in the Appendix-A alignment track MUST update this table in the same commit that ships the verb.

Legend:
- **implemented** — landed on `main`; verb shape matches the spec row above.
- **not-yet** — verb defined in Appendix A; no CLI entry on `main` yet. Scheduled within the Appendix-A alignment PR track; status flips in the PR that ships the verb.
- **descoped-#NNN** — explicitly removed from M1; follow-up issue carries the residual scope.

| Verb (Appendix A) | Status | Landed / tracked |
|---|---|---|
| `drive9 mount vault <path>` | not-yet | Appendix-A alignment PR track |
| `drive9 umount <path>` | implemented | pre-M1 |
| `drive9 ctx add --api-key` | implemented | #284 (PR-B) |
| `drive9 ctx import --from-file` | implemented | #284 (PR-B) |
| `drive9 ctx ls` / `use` / `rm` | implemented | #284 (PR-B) |
| `drive9 vault put <path> --from <dir>` | not-yet | Appendix-A alignment PR track |
| `drive9 vault grant <scope>... --agent --perm --ttl` | not-yet | Appendix-A alignment PR track (server endpoint live in #273; CLI still on legacy `drive9 secret grant` with no `--perm`) |
| `drive9 vault revoke <grant-id>` | not-yet | Appendix-A alignment PR track |
| `drive9 vault with <path> -- <cmd>` | not-yet | Appendix-A alignment PR track |
| `drive9 vault reauth <mountpoint>` | descoped-#302 | deferred post-M1 (§17) |
| Data-plane `cat / ls / rm / printf >` on `/n/vault/**` | implemented | pre-M1 |

Rows record the **final user-visible verb**. Interim steps (for example, fixing the legacy `drive9 secret grant` call path to hit `/v1/vault/grants` with `--perm` before the `vault` verb lands) do **not** flip `drive9 vault grant` to `implemented` — that row only flips when the `drive9 vault grant` CLI surface itself is live on `main`.

---

Appendix A — Command surface at a glance:

| Command | Role |
|---|---|
| `drive9 mount vault <path>` | Mount the vault namespace under `<path>`. |
| `drive9 umount <path>` | Unmount. |
| `drive9 ctx add --api-key <k>` | Register an owner context. |
| `drive9 ctx import --from-file <path>` | Register a delegated context from a grant JWT file (primary UX). |
| `drive9 ctx import [--from-file -]` | Same, reading the JWT from stdin (default when stdin is a pipe). |
| `drive9 ctx ls / use / rm` | Manage contexts (offline). |
| `drive9 vault put <path> --from <dir>` | Atomic wholesale-replace batch write (§2). |
| `drive9 vault grant <scope>... --agent --perm --ttl` | Issue a scoped JWT. |
| `drive9 vault revoke <grant-id>` | Revoke a grant. |
| `drive9 vault with <path> -- <cmd>` | Exec child with `@env` injected. |
| `cat / ls / rm / printf >` on `/n/vault/**` | Data plane. |

Deferred post-M1 (tracked as a follow-up increment, see §17): `drive9 vault reauth <mountpoint>` — rebind a running mount to the current context without `umount`.

Appendix B — Canonical history: §0–§12 absorb `89603ee6`; §13–§17 are the v2 context-unified credential increment over that canonical; Invariants #1–#5 derive from `89603ee6`; Invariants #6–#7 are new.
