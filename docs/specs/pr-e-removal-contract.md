# PR-E — Removal Contract (stub)

**Parent:** `docs/specs/pr-a-jwt-implementation.md` §10
**Status:** stub — full implementation doc to be authored before PR-E opens.

PR-E is the sub-PR that reaches terminal state per `docs/specs/vault-interaction-end-state.md` §20 (no migration / no backward compat surface). PR-A through PR-D ship additively; PR-E is the deletion PR that closes the coexistence window.

## Scope — MUST include all of

### Types and core functions
- Delete `vault.CapToken` and `vault.CapTokenClaims` (in `pkg/vault/types.go`).
- Delete `Store.IssueCapToken`, `Store.VerifyAndResolveCapToken`, `Store.RevokeCapToken` (in `pkg/vault/store.go`).
- Delete `SignCapToken`, `VerifyCapToken`, `PeekCapTokenTenantID`, `capTokenPrefix` (in `pkg/vault/crypto.go`).

### Storage
- Drop `vault_tokens` DDL from `pkg/vault/schema.go`.
- Drop all `vault_tokens` references from `pkg/tenant/schema/vault.go` and `pkg/tenant/schema/tidb_auto.go`.
- Add a one-shot DROP migration for the `vault_tokens` table, named to match the repo's existing migration convention `NNNN_drop_vault_tokens.sql` (where `NNNN` is the next sequential migration number).

### HTTP
- Delete `handleVaultTokens`, `handleVaultTokenIssue`, `handleVaultTokenRevoke` in `pkg/server/vault.go`.
- Delete the `/v1/vault/tokens` routing branch.
- Delete `handleVaultRead`, `handleVaultReadEnumerate`, `handleVaultReadSecret`, `handleVaultReadField` in `pkg/server/vault.go`.
  - If PR-F has not yet shipped the FUSE backend that replaces this: PR-E MUST replace `/v1/vault/read/*` with a hard 410 Gone + "migrate to FUSE" message. It does NOT leave the endpoint functional.
- Delete `peekCapTokenTenantID` helper in `pkg/server/auth.go`.
- Delete the `capabilityAuthMiddleware` (or equivalent named middleware) that routes tokens via peek.

### CLI
- Delete `cmd/drive9/cli/secret.go` in full.
- Delete `cmd/drive9/cli/secret_test.go` in full.
- Delete `cmd/drive9/cli/secret_commands_test.go` in full.
- Remove the `Secret` command dispatch entry in `cmd/drive9/main.go`.
- Sweep `--cap-token` / `DRIVE9_CAP_TOKEN` references in CLI — delete or replace with `--vault-token` / `DRIVE9_VAULT_TOKEN` per §14.

### Client package
- Delete `IssueVaultToken`, `RevokeVaultToken`, `ListReadableVaultSecrets`, `ReadVaultSecret`, `ReadVaultSecretField`, `VaultTokenIssueResponse` in `pkg/client/vault.go`.
- Sweep `pkg/client/vault_test.go` for the corresponding test cases; delete them.
- Audit: PR-E's final diff on `pkg/client/vault.go` must have strictly fewer exported symbols than current main.

### SDKs (separate sub-PR is acceptable, but must land in the same release cycle)
- `clients/drive9-rs/src/vault.rs` + `clients/drive9-rs/examples/vault.rs` — delete old methods and examples.
- `clients/drive9-py/drive9/client.py` — delete `vault.*_token` / `vault.read_*` methods; `clients/drive9-py/tests/test_vault.py` — delete corresponding tests.
- `clients/drive9-js/tests/client.test.ts` — delete corresponding tests.
- `clients/drive9-rs/README.md` — remove mention of the old API.

### Documentation
- `docs/design/agent-vault-phase0.md` — archive (move to `docs/design/archive/agent-vault-phase0.md`). Do not delete; historical context is useful. Do not leave as live reference.
- `docs/guides/vault-quickstart.md` — remove any "legacy `drive9 secret`" disclaimer language.
- CHANGELOG — the first entry announcing the new ctx/vault flow MUST be in the PR-E commit, not before.

## Forbidden in PR-E

- No behavior changes beyond deletion. Do not take the opportunity to add new fields, new verbs, new errnos. Pure subtraction.
- No stub-leaving. `// deprecated, see PR-F` comments are not acceptable — the code is either deleted or it isn't.
- No compatibility shims. No `IssueVaultToken` that internally delegates to `IssueVaultGrant`. If a caller still uses the old API at PR-E time, the caller's PR is blocked, not PR-E's shim.

## Review gate

- Adversary-2 is the designated blocker for this PR per `0934b9b3`.
- Adversary-1 signs off.
- Both adversaries must walk the deletion list line-by-line and assert "deleted" on each item before approving.
- No "partial PR-E" is acceptable — either the full list ships in one PR, or PR-E is split into PR-E.1 (server-side deletion) and PR-E.2 (client-side deletion) both landing in the same release cycle.

## Triggering condition

PR-E opens when:
- PR-A (grants payload + server) is merged.
- PR-B (ctx) is merged.
- PR-C (resolution ladder) is merged.
- PR-D (mount binding) is merged.
- PR-F (FUSE data plane) is merged OR PR-E ships the 410-Gone fallback for `/v1/vault/read`.

PR-G (regression tests) runs against the post-PR-E state, not before.

## Release gate

Per `pr-a-jwt-implementation.md` §10 guard 3, no user-facing "done" signal (tag, CHANGELOG, docs update advertising the new flow) lands before PR-E merges. CI passing on PR-A..PR-D is fine; external announcement is not.