# drive9 vault — Quickstart

This guide walks through using `drive9 vault` end-to-end. For the normative spec (errno table, invariants, contracts), see [`docs/specs/vault-interaction-end-state.md`](../specs/vault-interaction-end-state.md).

## Mental model

- A **secret** is a directory (e.g. `prod-db`).
- A **key** is a file inside that directory (e.g. `prod-db/DB_URL`).
- You read/write keys with ordinary POSIX commands (`cat`, `printf >`, `ls`, `rm`).
- The control plane has 5 verbs: `put`, `grant`, `revoke`, `with`, `reauth`.
- Credentials live in `~/.drive9/config` as **contexts**. The active context determines what you can see.

---

## Part 1 — Owner workflow

### 1. Register an owner context

Tenant owners start with an API key from provisioning:

```bash
drive9 ctx add --api-key <owner-api-key> --name owner-prod --server https://drive9.example.com
drive9 ctx use owner-prod
```

Verify:

```bash
drive9 ctx ls
# NAME         TYPE   SCOPE  PERM  EXPIRES_AT  STATUS
# owner-prod   owner  *      rw    —           active *
```

### 2. Mount

```bash
drive9 mount vault /n/vault
```

`/n/vault` is now a directory tree. Each subdirectory is a secret.

### 3. Create a secret (batch, recommended)

```bash
mkdir -p ./prod-db.envdir
printf 'postgres://db.example.com:5432/app' > ./prod-db.envdir/DB_URL
printf 'app_user'                           > ./prod-db.envdir/DB_USER
printf 'super-secret-password'              > ./prod-db.envdir/DB_PASSWORD

drive9 vault put /n/vault/prod-db --from ./prod-db.envdir
```

The `put` is atomic — concurrent readers see either the full old state or the full new state.

### 4. Add or rotate a single key

```bash
printf 'sk-live-rotated-456' > /n/vault/prod-db/API_KEY     # new key, or overwrite existing
```

### 5. Read

```bash
cat /n/vault/prod-db/DB_URL
cat /n/vault/prod-db/@env       # dotenv view
cat /n/vault/prod-db/@all       # JSON snapshot
ls  /n/vault/prod-db            # list keys
```

### 6. Inject a secret into a child process

```bash
drive9 vault with /n/vault/prod-db -- ./myapp
```

Reads `@env`, forks, injects into the child’s environment, execs. The parent shell never sees the values.

### 7. Delete

```bash
rm /n/vault/prod-db/OLD_KEY                 # remove a key
rm -r /n/vault/prod-db                      # remove the whole secret
```

Deleting a key does **not** cascade-revoke existing grants. Holders see `ENOENT` on subsequent reads; the audit log records `affected_grants`.

---

## Part 2 — Granting a scoped token

The canonical way to share a key with another agent is `vault grant`. Scope can be a whole secret or a single key.

### Grant a single key for 1 hour

```bash
drive9 vault grant /n/vault/prod-db/DB_URL --agent alice --perm read --ttl 1h
```

Output (human default):

```
drive9 ctx import --from-file -
vt_eyJhbGc...
---
grant_id:   grt_7f2a
expires_at: 2026-04-18T19:00:00Z
```

Send the whole block to Alice over a secure channel (email with a password-protected attachment, password manager share, Signal, etc.). Alice will save the JWT to a file and pipe it into `ctx import` (see Part 3). The JWT is displayed once and is not re-fetchable — if it is lost, issue a new grant.

Avoid distributing the JWT as a copyable one-liner (`drive9 ctx import <jwt>`). That form is valid (see Part 3) but records the token in the delegatee's shell history and process argument list.

### Script-friendly output

```bash
drive9 vault grant ... --token-only       # prints the raw JWT, nothing else
drive9 vault grant ... --json             # {token, grant_id, expires_at, scope[], perm, ttl}
```

### Grant variants

```bash
# Whole secret, read-only
drive9 vault grant /n/vault/prod-db --agent alice --perm read --ttl 1h

# Several scopes in one grant
drive9 vault grant /n/vault/prod-db/DB_URL /n/vault/prod-db/DB_USER \
  --agent alice --perm read --ttl 1h

# Write permission (e.g. credential rotation)
drive9 vault grant /n/vault/prod-db/DB_PASSWORD --agent rotator --perm write --ttl 10m
```

### Introspect existing grants

```bash
ls  /n/vault/prod-db/@grants/
cat /n/vault/prod-db/@grants/grt_7f2a
```

---

## Part 3 — Delegatee workflow

Alice receives a grant message from the owner containing the JWT.

### 1. Save and import the grant

Save the JWT to a file (e.g. from a password manager download):

```bash
# Save the JWT body to a file that only Alice can read
install -m 600 /dev/null ~/alice-grant.jwt
# paste the JWT into the editor, or save it from your password manager
$EDITOR ~/alice-grant.jwt

drive9 ctx import --from-file ~/alice-grant.jwt
rm ~/alice-grant.jwt
```

Or, if the JWT is already on the clipboard and the shell supports it, pipe via stdin:

```bash
pbpaste | drive9 ctx import --from-file -
# or on Linux: xclip -o | drive9 ctx import --from-file -
```

Either form writes a **delegated** context to Alice's `~/.drive9/config`. The JWT is decoded locally; no server round-trip is required. If the token is already expired, import is refused immediately. The JWT never lands in shell history or `/proc/<pid>/cmdline`.

`drive9 ctx import <jwt>` (positional) also works, but it will be recorded in shell history. Use it only for scripting and testing.

### 2. Activate and mount

```bash
drive9 ctx use alice-prod-db
drive9 mount vault /n/vault
```

### 3. Use

```bash
cat /n/vault/prod-db/DB_URL                     # OK
ls  /n/vault/prod-db                            # only keys Alice can see
cat /n/vault/prod-db/DB_PASSWORD                # ENOENT — not visible under this grant
drive9 vault with /n/vault/prod-db -- ./alice-tool
```

Owners and delegatees run the **same** commands. Only the active context differs.

### 4. Check context status locally

```bash
drive9 ctx ls
# NAME            TYPE        SCOPE                 PERM   EXPIRES_AT            STATUS
# alice-prod-db   delegated   prod-db/DB_URL        read   2026-04-18T19:00:00Z  active *
```

`ctx ls` is offline — it reads the local config only. Status (`active` / `expired`) is computed from `expires_at` at display time.

---

## Part 4 — Revocation and re-auth

### Owner revokes early

```bash
drive9 vault revoke grt_7f2a
```

### Delegatee sees stale auth

```bash
cat /n/vault/prod-db/DB_URL
# cat: Permission denied  (run `drive9 vault reauth` after updating the context)
```

### Rotate the context and rebind

If the delegatee receives a new grant, they import it, switch, and rebind the running mount without unmounting:

```bash
install -m 600 /dev/null ~/alice-grant-v2.jwt
$EDITOR ~/alice-grant-v2.jwt
drive9 ctx import --from-file ~/alice-grant-v2.jwt
rm ~/alice-grant-v2.jwt

drive9 ctx use alice-prod-db-v2
drive9 vault reauth /n/vault
```

---

## Part 5 — Environment variable overrides (CI / one-off)

Contexts are the primary channel. For ephemeral or non-interactive shells, three env vars act as overrides:

- `DRIVE9_API_KEY` — owner credential override
- `DRIVE9_VAULT_TOKEN` — delegated credential override (JWT)
- `DRIVE9_SERVER` — server URL override (resolved independently of credentials)

Credential priority (first match wins):

1. `DRIVE9_VAULT_TOKEN` (narrower — delegated)
2. `DRIVE9_API_KEY` (broader — owner)
3. Active context in `~/.drive9/config`

`DRIVE9_SERVER` is resolved independently: if set, it overrides the active context's `server` field even when credentials come from the active context.

Example CI job:

```bash
export DRIVE9_SERVER=https://drive9.example.com
export DRIVE9_VAULT_TOKEN=$CI_INJECTED_JWT
drive9 mount vault /n/vault
drive9 vault with /n/vault/prod-db -- ./deploy
drive9 umount /n/vault
```

The dual-principal separation (`DRIVE9_API_KEY` vs `DRIVE9_VAULT_TOKEN`) is intentional. Do not collapse them.

---

## Part 6 — End-to-end example

### Owner terminal

```bash
drive9 ctx add --api-key $OWNER_KEY --name owner-prod --server https://drive9.example.com
drive9 ctx use owner-prod
drive9 mount vault /n/vault

mkdir -p ./prod-db.envdir
printf 'postgres://db.example.com:5432/app' > ./prod-db.envdir/DB_URL
printf 'app_user'                           > ./prod-db.envdir/DB_USER
printf 'super-secret-password'              > ./prod-db.envdir/DB_PASSWORD
drive9 vault put /n/vault/prod-db --from ./prod-db.envdir

cat /n/vault/prod-db/DB_URL

drive9 vault grant /n/vault/prod-db/DB_URL --agent alice --perm read --ttl 1h
# -> drive9 ctx import --from-file -
#    vt_eyJhbGc...
#    ---
#    grant_id:   grt_7f2a
#    expires_at: 2026-04-18T19:00:00Z
```

### Alice terminal

```bash
# Save the JWT (e.g. from a secure channel into a file with 0600 perms)
install -m 600 /dev/null ~/alice-grant.jwt
$EDITOR ~/alice-grant.jwt
drive9 ctx import --from-file ~/alice-grant.jwt
rm ~/alice-grant.jwt

drive9 ctx use alice-prod-db
drive9 mount vault /n/vault

cat /n/vault/prod-db/DB_URL               # OK
cat /n/vault/prod-db/DB_PASSWORD          # ENOENT
drive9 vault with /n/vault/prod-db -- ./alice-tool
```

### Owner revokes early

```bash
drive9 vault revoke grt_7f2a
```

### Alice’s next read

```bash
cat /n/vault/prod-db/DB_URL
# cat: Permission denied   (run `drive9 vault reauth` after rotating the context)
```

---

## Quick reference

| Action | Command |
|---|---|
| Add owner credential | `drive9 ctx add --api-key <k> --name <n>` |
| Add delegated credential | `drive9 ctx import --from-file <path>` (or `--from-file -` for stdin) |
| List credentials | `drive9 ctx ls` |
| Switch | `drive9 ctx use <name>` |
| Mount | `drive9 mount vault /n/vault` |
| Create/replace a secret | `drive9 vault put /n/vault/<s> --from ./dir [--prune]` |
| Read a key | `cat /n/vault/<s>/<k>` |
| Write a key | `printf '…' > /n/vault/<s>/<k>` |
| Delete a key | `rm /n/vault/<s>/<k>` |
| List keys | `ls /n/vault/<s>` |
| Inject env | `drive9 vault with /n/vault/<s> -- <cmd>` |
| Grant | `drive9 vault grant <scope>... --agent <a> --perm <p> --ttl <t>` |
| Revoke | `drive9 vault revoke <grant-id>` |
| Rebind after rotation | `drive9 vault reauth /n/vault` |

## Errno quick reference

| Situation | errno |
|---|---|
| Key/secret not found | `ENOENT` |
| Key exists but not visible under current context | `ENOENT` (intentional) |
| Write without permission | `EACCES` |
| Context/token expired or revoked (runtime) | `EACCES` + `vault reauth` hint |
| `ctx use` on a locally-expired context | client-side error (not a new errno) |
| Backend / FUSE failure | `EIO` |

For full invariants and security rules, see the spec at `docs/specs/vault-interaction-end-state.md`.
