# PR-A — JWT payload + server issue/revoke/validate

**Spec anchor:** `docs/specs/vault-interaction-end-state.md` §16 (JWT MUST / payload), §8 (revoke), Invariant #7 (client-decode-UX-only), §11 (errno table — EACCES rows), §13.3 (TOFU trust).

**Goal of this PR:** re-shape the capability-token surface so that the token payload matches the locked spec claim set and the server-side issue / revoke / validate paths speak in the new vocabulary. **No CLI, no FUSE, no ctx config — those are later sub-PRs.**

This document is the hand-off for whoever implements PR-A. Follow it literally; deviate only after posting a question in `#onepassword`.

---

## 1. Files touched

**Scope note (additive):** PR-A ADDS the new grant surface alongside the existing `CapToken*` / `vault_tokens` / `/v1/vault/tokens` / `/v1/vault/read` code. It does NOT delete or modify the old code. Old code continues to work so existing SDKs and the legacy `drive9 secret` CLI keep functioning during the transition. Old code is removed in PR-E (CLI cleanup) and PR-F (FUSE data-plane rewrite). Spec §20 "no backward compat" is about the **externally observable contract**, not about forbidding a repo-internal transition window.

| File | Change |
|---|---|
| `pkg/vault/types.go` | **ADD** `VaultGrant` and `VaultGrantClaims` types with the §16 claim set. Leave `CapToken`/`CapTokenClaims` untouched. |
| `pkg/vault/grant.go` (**new file**) | New file for `IssueGrant`, `RevokeGrant`, `VerifyAndResolveGrant` against `vault_grants`. Do NOT touch `store.go`. |
| `pkg/vault/grant_sign.go` (**new file**) | New file for `SignGrant`/`VerifyGrant` (JWT-style HMAC-SHA256 on the new payload shape). Do NOT touch `crypto.go`. Prefix constant: `vt_`. |
| `pkg/vault/schema.go` + `pkg/tenant/schema/vault.go` + `pkg/tenant/schema/tidb_auto.go` | **ADD** `vault_grants` table DDL. Leave `vault_tokens` DDL untouched. |
| `pkg/server/vault.go` | **ADD** `handleVaultGrants`/`handleVaultGrantIssue`/`handleVaultGrantRevoke` and route `/v1/vault/grants`. Leave `/v1/vault/tokens` handlers untouched. |
| `pkg/vault/grant_test.go` + `pkg/vault/grant_sign_test.go` (**new**) | Tests for the 20 cases in §6. Do NOT touch `store_test.go` / `crypto_test.go`. |
| `pkg/client/vault.go` | **ADD** `IssueVaultGrant(ctx, scope, agent, perm, ttl, labelHint)` and `RevokeVaultGrant(grantID)` helpers. Leave existing `IssueVaultToken`/`RevokeVaultToken` untouched. |
| `pkg/client/vault_test.go` | **ADD** tests for the two new helpers; do not touch existing tests. |

**Out of scope for PR-A** (do NOT touch in this PR):
- `cmd/drive9/cli/secret.go` — deleted in PR-E/Remove-secret sub-PR
- `cmd/drive9/cli/mount.go` — PR-D
- `cmd/drive9/cli/db.go` (ctx) — PR-B
- FUSE daemon — PR-F
- Env-var resolution ladder — PR-C

Submitting a PR that grows beyond the table above = automatic block. Keep sub-PRs small.

---

## 2. JWT payload — the **exact** claim set (§16)

The payload MUST contain **all** of these claims and **only** these claims (no `task_id`, no `tenant_id` in the token body, no `iat` if you don't verify it — see note):

| JSON key | Go field | Type | Required | Notes |
|---|---|---|---|---|
| `iss` | `Issuer` | `string` | ✅ | The server URL the grant was minted at. Delegatees trust this via TOFU on `ctx import` (§13.3). Treat as opaque; do not parse. |
| `grant_id` | `GrantID` | `string` | ✅ | Server-unique grant id. Prefix `grt_` per quickstart example. |
| `principal_type` | `PrincipalType` | `string` enum | ✅ | Exactly one of: `"owner"` or `"delegated"`. No other values. |
| `agent` | `Agent` | `string` | ✅ | Opaque label the owner supplies at issue time (`--agent alice`). Not validated against any identity system in v0. |
| `scope` | `Scope` | `[]string` | ✅ | Non-empty. Path-style entries like `prod-db` (whole secret) or `prod-db/DB_URL` (single key). Validated by `vault.ValidateScope`. |
| `perm` | `Perm` | `string` enum | ✅ | Exactly one of: `"read"` or `"write"`. |
| `exp` | `ExpiresAt` | `int64` (unix sec) | ✅ | Grants without `exp` are refused at verify time. |
| `label_hint` | `LabelHint` | `string` | optional | UX-only hint for ctx naming (e.g. `"prod-db-readonly"`); NEVER used for authz (Invariant #7). |

**Removed vs current `CapTokenClaims`**:
- `token_id` → renamed to `grant_id`
- `tenant_id` → **removed from the token body**. Server derives tenant from the `iss` + its own registry, or from the auth middleware scope (for owner endpoints); tenant MUST NOT be trusted from the token body. (This closes a forgery class where an attacker sets `tenant_id` in an unsigned portion of the token.)
- `task_id` → removed entirely (§20 no-backward-compat; was a Phase-0 concept).
- `agent_id` → renamed to `agent`.
- `iat` → keep as `int64` `iat` if you want freshness logs, but verify paths rely only on `exp`. If removed, remove from signing too — no asymmetric shape.
- `nonce` → keep as a signing-layer detail (outside the claims struct, inside the HMAC computation) to prevent deterministic-token replay. Do not expose in JSON.

**Signing algorithm:** unchanged — HMAC-SHA256 with the tenant-derived CSK (`MasterKey.DeriveCSK(tenantID)`). v0 does NOT need asymmetric keys; the delegatee trusts `iss` via TOFU and verifies by calling the server.

**Wire format (locked to end-state spec §16):** `vt_` + base64url(header) + `.` + base64url(payload) + `.` + base64url(mac). Header is the literal bytes `{"alg":"HS256","typ":"JWT"}`. Verify computes `HMAC-SHA256` unconditionally — the header `alg` value is informational for downstream JWT tooling (PR-B `ctx import`) and MUST NOT be parsed to pick an algorithm (alg-confusion prevention). The `vt_` display prefix lives on the token everywhere the token lives (wire, CLI output, `--token-only`, ctx import input) — it is NOT a CLI-only decoration. This supersedes an earlier draft that suggested header-less v0 wire; header-less requires a spec-amendment PR per adversary review `c8e3dd17`/`67796681`.

---

## 3. SQL schema — `vault_grants` table

ADD alongside `vault_tokens` (old table untouched; removed in PR-F):

```sql
CREATE TABLE vault_grants (
  grant_id        VARCHAR(64)  NOT NULL PRIMARY KEY,
  tenant_id       VARCHAR(64)  NOT NULL,
  issuer          VARCHAR(256) NOT NULL,      -- iss claim, for audit
  principal_type  VARCHAR(16)  NOT NULL,      -- "owner" | "delegated"
  agent           VARCHAR(128) NOT NULL,
  scope_json      JSON         NOT NULL,
  perm            VARCHAR(8)   NOT NULL,      -- "read" | "write"
  label_hint      VARCHAR(128) NULL,
  issued_at       DATETIME(3)  NOT NULL,
  expires_at      DATETIME(3)  NOT NULL,
  revoked_at      DATETIME(3)  NULL,
  revoked_by      VARCHAR(128) NULL,
  revoke_reason   VARCHAR(256) NULL,
  INDEX idx_vault_grants_tenant (tenant_id),
  INDEX idx_vault_grants_expires (expires_at)
);
```

**No migration from `vault_tokens`** (§20). PR-A is additive: `vault_tokens` and its CapToken code path stay intact and keep serving existing bearer tokens until PR-E performs the one-shot DROP per the explicit deletion list in [`pr-e-removal-contract.md`](pr-e-removal-contract.md). Do not drop `vault_tokens` in this PR or any PR-B/C/D fast-follow — the only permitted drop point is PR-E.

---

## 4. HTTP surface

### POST /v1/vault/grants

Request:
```json
{
  "agent": "alice",
  "scope": ["prod-db/DB_URL"],
  "perm": "read",
  "ttl_seconds": 3600,
  "label_hint": "prod-db-readonly"    // optional
}
```

`ttl_seconds` is REQUIRED and MUST be > 0. Omitted / zero / negative values return HTTP 400 with `ttl_seconds is required and must be > 0` — the handler MUST NOT silently default (end-state spec §6 "`--ttl` is required"; no silent policy).

`principal_type` is NOT a caller-controllable field. It is DELIBERATELY absent from the request shape because `vault grant` output is delegated-only per end-state spec §16 (claim table) and §13.3 (`ctx import` refuses non-delegated JWTs). The server mints `principal_type="delegated"` unconditionally; any field named `principal_type` in the request body is silently ignored, not honored. Re-delegation ("delegated caller mints a further grant") is blocked structurally by the router — `/v1/vault/grants` sits behind the tenant-owner auth middleware, which delegated JWTs cannot satisfy. This removes the HTTP 403 re-delegation case entirely: the delegated caller cannot reach the handler to begin with.

Auth: tenant owner API key (existing `tenantAuthMiddleware`).

Issuer (`iss` claim): server-side only, sourced from `server.Config.VaultIssuerURL` which entrypoints populate from `DRIVE9_VAULT_ISSUER_URL` (with a sane default of `publicBaseURL(addr)` for loopback / `DRIVE9_PUBLIC_URL` deployments). If `VaultIssuerURL` is empty when a request lands, the handler returns HTTP 503 `vault issuer URL not configured` — fail-closed; no forgeable issuer-less grant is ever minted.

Response (201):
```json
{
  "token": "vt_eyJhbGc...",   // the signed JWT, `vt_` prefix emitted by SignGrant per §16 wire format
  "grant_id": "grt_7f2a",
  "expires_at": "2026-04-18T19:00:00Z",
  "scope": ["prod-db/DB_URL"],
  "perm": "read"
}
```

### DELETE /v1/vault/grants/{grant_id}

Auth: tenant owner. Body optional (`{"revoked_by": "...", "reason": "..."}`).

Returns 200 on success, 404 on unknown/already-revoked grant id.

### Validate (internal, called by data-plane reads)

`VerifyAndResolveGrant(ctx, tenantID, rawJWT) -> *VaultGrantClaims, error`

Ordered checks (abort on first fail):
1. HMAC signature with CSK. Fail → return `"invalid grant"` → caller maps to EACCES.
2. `exp` > now. Fail → `"grant expired"` → EACCES.
3. `principal_type` ∈ {"owner","delegated"}. Fail → `"malformed grant"` → EACCES.
4. `perm` ∈ {"read","write"}. Fail → `"malformed grant"` → EACCES.
5. `scope` non-empty and passes `ValidateScope`. Fail → `"malformed grant"` → EACCES.
6. DB row exists `WHERE tenant_id=? AND grant_id=?` and `revoked_at IS NULL`. Fail → `"grant revoked"` or `"grant not found"` → EACCES.

**Silent-requirement pass (from `feedback_silent_requirement_blind_spot.md`):**
- **Missing required claim** → default is EACCES, not "treat as empty". This PR MUST explicitly reject each missing claim at step 3/4/5 above.
- **`iss` mismatch** — the server's own URL vs the `iss` in the token. v0 is tenant-scoped and the CSK is tenant-derived; a token minted by tenant A cannot verify at tenant B anyway. Still: log `iss` in audit and fail if it doesn't match the server's canonical URL. This matters for future multi-issuer / migration scenarios and closes a trust-chain gap.
- **Clock skew** — follow spec §16 (±60s leeway on `exp`). Do not add leeway on revocation (revoked_at is authoritative).

---

## 5. Audit events

Every issue/revoke/validate path writes to `vault_audit_log` (reuse existing `WriteAuditEvent`). Event types:

| Event type | When | Key fields |
|---|---|---|
| `grant.issued` | After successful INSERT into `vault_grants` | `grant_id`, `agent`, `principal_type`, `perm`, `scope` in detail JSON |
| `grant.revoked` | After successful UPDATE | `grant_id`, `revoked_by`, `reason` in detail |
| `grant.verify.denied` | When `VerifyAndResolveGrant` fails **after** the HMAC check passes (so we know `grant_id`) | `grant_id`, `reason` ∈ {expired, revoked, malformed} |
| `grant.verify.ok` | Successful verify — **do not** write on every read (too noisy); write only at data-plane entry points (PR-F concern, stub for now) | — |

**Critical**: do NOT write audit events on HMAC-failure — attacker can flood audit with a forged grant_id field (carried over from `pkg/server/vault.go:435` comment, still applies).

---

## 6. Test plan (MUST ship with the PR)

New tests (add to `pkg/vault/store_test.go` + `crypto_test.go`):

1. **Happy path (delegated)**: `IssueGrant` with `PrincipalDelegated` → `VerifyAndResolveGrant` → success; claims round-trip all §16 fields (iss, grant_id, principal_type, agent, scope, perm, exp, label_hint).
2. **Wire format lock**: `SignGrant` emits `vt_<headerB64>.<payloadB64>.<macB64>` with header = `{"alg":"HS256","typ":"JWT"}`. `VerifyGrant` is HS256-only regardless of header `alg` value (alg-confusion hardcoded closed).
3. **Expired**: issue with ttl=1s, sleep 2s, verify → EACCES "expired".
4. **Revoked**: issue → revoke → verify → EACCES "revoked". Double-revoke returns `ErrNotFound`.
5. **Cross-tenant replay**: issue under tenant A, verify under tenant B → CSK mismatch → EACCES "invalid".
6. **Missing required claim**: table-driven over all §16 required claims — hand-craft HMAC-valid token with one claim deleted → `VerifyGrant` rejects. (Lives in `grant_sign_test.go`.)
7. **Unknown claim**: hand-craft HMAC-valid token with a smuggled field (e.g. `tenant_id`) → `VerifyGrant` rejects via `DisallowUnknownFields`.
8. **Bad `perm` value** at issue time (e.g. `"admin"`) → `IssueGrant` rejects. At sign time, `SignGrant` also rejects.
9. **Bad `principal_type`** at issue time (e.g. `"root"`) → `IssueGrant` rejects. `SignGrant` also rejects.
10. **Empty scope / empty agent / empty issuer / ttl≤0** at issue time → `IssueGrant` rejects. At HTTP boundary, ttl≤0 → HTTP 400 `"ttl_seconds is required and must be > 0"`.
11. **Delegated-only mint contract**: client helper `IssueVaultGrant` MUST NOT send `principal_type` on the wire; test decodes request into `map[string]any` and asserts the key is absent. Server hardcodes `vault.PrincipalDelegated` — not caller-controllable. (Re-delegation from delegated callers is blocked upstream by `tenantAuthMiddleware`; no handler-level HTTP 403 test needed in PR-A because delegated JWTs cannot structurally reach the owner-only handler. Active delegated-caller rejection lands in PR-D when `VerifyAndResolveGrant` is wired into middleware.)
12. **`iss` mismatch**: mint with server URL A, verify under server URL B → EACCES "issuer mismatch". Empty `expectedIssuer` skips the check (test-only carve-out, documented in `VerifyAndResolveGrant`).
13. **`exp` with −30s clock skew**: still verifies (within ±60s leeway).
14. **`exp` with −61s clock skew (beyond skew)**: EACCES "expired".
15. **Tampered signature**: replace last segment with valid-looking bytes → `VerifyGrant` rejects.
16. **Wrong CSK**: sign with CSK1, verify with CSK2 → rejects (cross-tenant replay guard at crypto layer).
17. **Missing `vt_` prefix**: strip prefix from a valid token → `VerifyGrant` rejects.
18. **Client helper round-trip**: `IssueVaultGrant` / `RevokeVaultGrant` against `httptest.NewServer` — payload shape, auth header (`Bearer tenant-key`), decoded response, and `*StatusError` propagation on 4xx (400 on bad ttl, 404 on revoke-missing).
19. **`grant.issued` audit Detail contract**: after a successful mint, the audit event carries `Detail` as a map containing `grant_id`, `agent`, `principal_type`, `perm`, `scope` per §5 — roundtrip through `WriteAuditEvent`+`QueryAuditLog`, assert each field value, guard against accidental drop of the Detail map. (Regression after adv-2 Block B1.)
20. **`grant.revoked` audit Detail contract**: after a successful revoke, the audit event carries `Detail` with `grant_id`, `revoked_by`, `reason` per §5, AND top-level `AgentID` mirrors `revoked_by` so filter queries by actor work without parsing detail_json. Assert both. (Regression after adv-2 Block B2.)

No integration tests for HTTP in PR-A — leave that to PR-B onward once there's a full `ctx` flow to exercise.

---

## 7. PR mechanics

- **Base branch**: `main` (per @qiffang Q1, msg `9026d8e4`).
- **Branch name**: `dev1/vault-impl-pr-a-jwt-server` (or `dev2/...` / whoever writes it).
- **Commit author**: `qiffang <qiffang33@gmail.com>` (per @qiffang Q1). Use `git -c user.name='qiffang' -c user.email='qiffang33@gmail.com' commit ...` for every commit.
- **Commit granularity**: one commit per file-group is fine; squash on merge.
- **PR title**: `feat(vault): PR-A — JWT grant payload + server issue/revoke/validate`
- **PR body**: link to this doc, to `docs/specs/vault-interaction-end-state.md#16`, and to Invariant #7.
- **Depends on**: PR #272 merged (so the spec is canonical in `main`). Do NOT open PR-A until #272 is in.
- **Review gate**: per @qiffang Q4, request `@adversary-1` and `@adversary-2` review **before** waiting for CI.
- **CI**: expect failures in `cmd/drive9/cli/secret_*_test.go` once the old `CapToken` type is gone. That's fine — flag it in the PR body, PR-E will delete the secret surface. Do not delete secret tests in PR-A; leave them broken with a one-line `t.Skip("removed in PR-E")` if needed to keep `go test ./...` green for reviewers.

---

## 8. What this PR deliberately leaves for later

| Concern | Handled in |
|---|---|
| `drive9 ctx import` decoding the JWT client-side | PR-B |
| Server URL override / `DRIVE9_SERVER` | PR-C |
| `drive9 mount vault` binding a principal | PR-D |
| HTTP auth middleware wiring `VerifyAndResolveGrant` on data-plane reads | PR-D |
| `drive9 vault grant` CLI wrapping this HTTP endpoint | PR-E |
| `vault with` env scrub (F14) | PR-E |
| `@grants/` pseudo-dir visibility (F8 owner-only) | PR-F |
| Errno mapping for FUSE readdir/open | PR-F |

**Explicit on verify wiring** (adversary review `6fa20200` / `67796681` Q C): `vault.Store.VerifyAndResolveGrant` ships in PR-A as exercised-by-unit-test code only. It is intentionally NOT wired into any HTTP auth middleware in this PR — no active grant-auth integration path exists on the data plane yet. PR-D is where the middleware that calls `VerifyAndResolveGrant` on every vault read/write request lands (bound at `drive9 mount vault --principal vault-token` time). Reviewers should NOT expect HTTP-auth integration tests in PR-A's diff.

If a reviewer says "but PR-A doesn't implement X" and X is in the right-hand column, point them here.

---

## 9. Cross-reference to review checklist

See `docs/specs/pr-a-review-checklist.md` (drafted alongside this doc). Reviewers (adv-1, adv-2) should go through every item there before approving.

---

## 10. PR-E Deletion Contract (binding)

PR-A is additive, which creates a repo-internal coexistence window of old and new token systems. To prevent "transitional" from silently becoming "permanent" (which would violate §20 of the spec), **the following three guards are binding on this PR-A and all subsequent sub-PRs through PR-G**. Adversary-2 raised this on msg `0934b9b3`; dev1 accepts.

### Guard 1 — Explicit deletion list for PR-E

PR-E's scope MUST include the deletion of every item below. This is not "cleanup eventually"; it is a hard contract. If PR-E merges without any item below deleted, that's a §20 violation.

- `pkg/vault/types.go`: delete `CapToken`, `CapTokenClaims` types.
- `pkg/vault/store.go`: delete `IssueCapToken`, `VerifyAndResolveCapToken`, `RevokeCapToken`.
- `pkg/vault/crypto.go`: delete `SignCapToken`, `VerifyCapToken`, `PeekCapTokenTenantID`, `capTokenPrefix` constant.
- `pkg/vault/schema.go`: drop `vault_tokens` DDL.
- `pkg/tenant/schema/vault.go` + `pkg/tenant/schema/tidb_auto.go`: drop all references to `vault_tokens`.
- `pkg/server/vault.go`: delete `handleVaultTokens`, `handleVaultTokenIssue`, `handleVaultTokenRevoke`. Delete `/v1/vault/tokens` routing.
- `pkg/server/vault.go`: delete `handleVaultRead`, `handleVaultReadEnumerate`, `handleVaultReadSecret`, `handleVaultReadField`. The data-plane read path is replaced by PR-F's FUSE backend. If PR-F hasn't shipped yet, PR-E moves `/v1/vault/read` to return 410 Gone.
- `pkg/server/auth.go`: delete `peekCapTokenTenantID` helper and its capability-auth middleware path.
- `pkg/server/vault.go`: delete all references to `tenant_id` field in `CapTokenClaims` (already scrubbed from the new `VaultGrantClaims`).
- `pkg/client/vault.go`: delete `IssueVaultToken`, `RevokeVaultToken`, `ListReadableVaultSecrets`, `ReadVaultSecret`, `ReadVaultSecretField`, `VaultTokenIssueResponse`. (Note: `QueryVaultAudit` stays — PR-A leaves audit alone; PR-F or a later PR decides the audit surface.)
- `cmd/drive9/cli/secret.go`: delete entire file.
- `cmd/drive9/cli/secret_test.go` + `cmd/drive9/cli/secret_commands_test.go`: delete both files.
- `cmd/drive9/main.go`: delete the `Secret` command dispatch entry.
- SQL migrations: add a migration file named `NNNN_drop_vault_tokens.sql` (or the repo's equivalent naming convention) that issues `DROP TABLE vault_tokens` plus `DROP INDEX` for every index this file currently creates on `vault_tokens` (none named at spec-time; the migration author verifies against `vault_tokens` DDL in `pkg/vault/schema.go` at PR-E time). Migration runs once, irreversible.
- Client SDKs (`clients/drive9-rs/src/vault.rs`, `clients/drive9-rs/examples/vault.rs`, `clients/drive9-py/drive9/client.py`, `clients/drive9-py/tests/test_vault.py`, `clients/drive9-js/tests/client.test.ts`, `clients/drive9-rs/README.md`): delete every method and test name matching `IssueVaultToken` / `ReadVaultSecret*` / `ListReadableVaultSecrets` and remove any doc mention of the old API.

**Precision rule for this list**: each item names a specific file and either (a) "delete entire file", or (b) a specific symbol/identifier. "Remove related helpers" or "clean up references" are forbidden phrasings — if a helper is implied but not named, it's not in the contract.

The stub for PR-E lives at `docs/specs/pr-e-removal-contract.md`. Any change to the list above requires both @adversary-1 and @adversary-2 approval AND a linked `@qiffang` decision.

### Guard 2 — No new callers of old token code (PR-B..PR-D review gate)

From the moment PR-A merges until PR-E merges, any PR that adds a new import, reference, or caller of the following symbols MUST be blocked in review:

- `vault.CapToken`, `vault.CapTokenClaims`
- `vault.IssueCapToken`, `vault.VerifyAndResolveCapToken`, `vault.RevokeCapToken`
- `vault.SignCapToken`, `vault.VerifyCapToken`, `vault.PeekCapTokenTenantID`
- `client.IssueVaultToken`, `client.RevokeVaultToken`, `client.ReadVaultSecret`, `client.ReadVaultSecretField`, `client.ListReadableVaultSecrets`
- HTTP paths `/v1/vault/tokens`, `/v1/vault/read/*`

Adversary-2 has asserted block authority on this guard. Adversary-1 supports. Reviewers MUST grep the diff for these strings as part of the pre-approve check (not "may" — "MUST").

**Any source reference** to the symbols above, **including in comments**, is blocked. Specifically a comment like `// legacy CapToken refactor — will remove` is NOT allowed; either delete the line entirely or don't add it. The only exception is this documentation file and `docs/specs/pr-e-removal-contract.md`, where the identifiers appear as contract text.

Moves (renames, file splits) of the old code that don't add new *callers* are fine — a `git mv` that relocates `crypto.go` to another path without adding new import sites passes the gate.

### Guard 3 — Release gate (no user-facing "done" until PR-E lands)

The sub-PR delivery PR-A through PR-G must NOT be marked user-facing-complete until PR-E's deletion PR merges. Specifically:

- No release tag naming "vault v0" / "grants v0" / similar until PR-E is in main.
- No CHANGELOG entry advertising the new `ctx`/`vault` flow until PR-E is in main.
- No documentation update on `docs/guides/vault-quickstart.md` removing "legacy `drive9 secret`" disclaimers until PR-E is in main.
- The internal squash order or temporary feature flagging inside the delivery is fine. What's gated is the external "go live" signal.

If the delivery stalls between PR-A and PR-E for more than two release cycles (counted as two consecutive merged releases of the repo, whichever semantic is canonical at the time), the escalation actor is **@qiffang**: dev1 pings @qiffang with a single message titled "PR-E overdue — fix-forward delete-now required" and links this section. @qiffang is expected to either (a) authorize dev1 to open the fix-forward delete-PR immediately, or (b) state explicitly why the coexistence window extends. Silence for one more release cycle = dev1 opens the delete-PR unilaterally, not a violation of ownership but an execution of this pre-authorized fallback.

---

## 11. Acceptance of the deletion contract

By merging PR-A, @qiffang and both adversaries have accepted guards 1–3 above. Future changes to PR-A's scope that weaken or defer the deletion contract are reverts, not amendments, and need the same sign-off as a spec change.
