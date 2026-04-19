# PR-A review checklist

**Use with:** `docs/specs/pr-a-jwt-implementation.md` and `docs/specs/vault-interaction-end-state.md`.

Reviewers: `@adversary-1`, `@adversary-2`. Walk every item. If any item fails, block the PR with a specific comment — do not approve conditionally.

---

## A. Claim-set conformance (§16)

- [ ] Payload contains exactly `iss, grant_id, principal_type, agent, scope, perm, exp, label_hint` — no more, no fewer.
- [ ] `tenant_id` is NOT in the token payload. (If you find `TenantID` in `VaultGrantClaims`, block.)
- [ ] `task_id` is NOT in the token payload. (Block if carried over from `CapTokenClaims`.)
- [ ] `agent_id` renamed to `agent`.
- [ ] `token_id` renamed to `grant_id`.
- [ ] Header is `{"alg":"HS256","typ":"JWT"}`. No `alg:none` accepted anywhere.
- [ ] `label_hint` is never consulted on an authz path — grep for every use and confirm it only appears in audit logs and display strings (Invariant #7).

## B. Verify path ordering and fail-closed (§16 + silent-requirement)

- [ ] `VerifyAndResolveGrant` aborts on first failure at each of: HMAC → exp → principal_type enum → perm enum → scope validity → DB revocation.
- [ ] Missing required claim → EACCES (not "zero-value fallthrough"). Test case present for each missing claim.
- [ ] `principal_type` outside {"owner","delegated"} → EACCES. Tested.
- [ ] `perm` outside {"read","write"} → EACCES. Tested.
- [ ] `scope` empty or invalid → EACCES. Tested.
- [ ] `iss` mismatch with server's canonical URL → EACCES AND audit event. Tested.
- [ ] Clock skew on `exp`: within ±60s leeway → pass; beyond → fail. Both sides tested.
- [ ] **No audit event on HMAC failure.** (Grep `WriteAuditEvent` before `VerifyCapToken`-returning-error paths. If present, block — attacker can flood audit.)
- [ ] No information leak in error strings returned to HTTP clients — `"invalid grant"` / `"grant expired"` / `"grant revoked"` are OK; `"grant not found for tenant foo"` is NOT.

## C. Authorization (§13 one-active-ctx, §20 non-goal)

- [ ] `/v1/vault/grants` mints `principal_type=delegated` **unconditionally**. `principal_type` MUST NOT be a caller-controllable request field — the handler hardcodes `vault.PrincipalDelegated` regardless of request body. (Per end-state spec §16 `vault grant` output is delegated-only.)
- [ ] Owner grants are NOT mintable through this endpoint. If you see a request-struct field for `principal_type` or a code path that honors a client-supplied principal, block.
- [ ] Router layer (`tenantAuthMiddleware`) restricts the endpoint to tenant owners — delegated JWTs cannot structurally reach the handler. This is the outer guard; the delegated-only minting above is the inner guard (belt + suspenders).
- [ ] Scope at issue time is validated by `ValidateScope` — no `../` escape, no wildcard, no absolute paths. Tested.
- [ ] `ttl_seconds` is REQUIRED at the HTTP boundary: missing / 0 / negative → HTTP 400 with `"ttl_seconds is required and must be > 0"`. No silent default. (Per spec §6 "--ttl is required".) No server-side upper bound in PR-A; any cap is a follow-up spec amendment.

## D. Schema & migration (§20 — no backward compat)

- [ ] `vault_grants` table created with the exact column set from `pr-a-jwt-implementation.md` §3.
- [ ] `vault_tokens` table dropped (or at least flagged for drop in a follow-up). No dual-write code.
- [ ] No code path references `task_id`.
- [ ] Indexes on `tenant_id` and `expires_at` present (revocation queries + pruning).

## E. Isolation (Invariant #3 foreshadowing, full enforcement in PR-D)

- [ ] All store-level queries in `pkg/vault/store.go` are tenant-scoped: `WHERE tenant_id = ? AND ...`. No bare `WHERE grant_id = ?`.
- [ ] CSK is derived from `tenantID`, not global — cross-tenant verify impossible even with DB access.
- [ ] Test case: issue under tenant A, try to verify under tenant B (forge DB row with matching grant_id) → fails on HMAC.

## F. CLI/client decode (Invariant #7, looking forward to PR-B)

- [ ] `pkg/client/vault.go` helpers return `{token_string, grant_id, expires_at, scope, perm}`. Client-side decode of the JWT payload is OK for UX (Invariant #7), but NO client code may gate behavior on the payload — server is always authoritative.
- [ ] Wire-format prefix (`vt_`) IS emitted by `SignGrant` in `pkg/vault/grant_sign.go` as part of the token string per merged spec §16. The token the server returns and the client receives already carries `vt_`; it is not a CLI display concern. If you see CLI code concatenating `vt_` on top of server output, block.

## G. Audit event correctness

- [ ] `grant.issued` written AFTER commit, never before. On rollback, no event.
- [ ] `grant.revoked` written AFTER UPDATE succeeds.
- [ ] `grant.verify.denied` carries `grant_id` ONLY when HMAC passed (otherwise `grant_id` is attacker-controlled).
- [ ] `detail_json` does not contain the raw JWT or the HMAC — only claim metadata.

## H. Test suite

- [ ] All 12 cases from `pr-a-jwt-implementation.md` §6 present and passing.
- [ ] No `t.Skip` in the new tests (only in the existing `secret_commands_test.go` if needed, with a comment pointing to PR-E).
- [ ] Failure-case assertions check the returned errno/HTTP-code, not just "err != nil".

## I. Silent-requirement pass (per `feedback_silent_requirement_blind_spot.md`)

Run this pass **last**, as a separate gate — not folded into any of A–H above. For each externally observable behavior of the verify path, ask: **"if a strict implementer took the spec literally with no additional assumption, is the default safe / fail-closed?"**

Specific cases to check:

- [ ] **Grant replay across servers**: if a grant is minted at `https://drive9.example.com` and later a different server at the same DNS is stood up with a different master key, can the grant verify? (Expected: no — CSK-derivation fails. Test it.)
- [ ] **Scope narrowing at verify time**: if scope in DB differs from scope in JWT (DB tampered), which wins? Spec says JWT is authoritative post-HMAC, but DB has revocation → does narrowing in DB actually work? (Expected: JWT scope wins; revocation is DB-only. Document.)
- [ ] **`label_hint` with newlines / escape sequences**: can it break an audit log line? (Expected: detail_json escapes; test with `"evil\n[INJECTED]"`.)
- [ ] **Empty string `agent`**: accepted or rejected? Spec doesn't say. (Recommend: reject at issue time with 400 — close the silent default.)
- [ ] **TTL = 0 or negative**: handler rejects with HTTP 400 (`"ttl_seconds is required and must be > 0"`). No silent default. Confirm the rejection fires before any DB / audit write (fail-closed at the request edge).
- [ ] **HMAC timing**: is verify using `hmac.Equal` / `subtle.ConstantTimeCompare`, not `bytes.Equal`? Timing side-channel.

If any silent-requirement item uncovers a gap, the fix is typically a MUST line added to `vault-interaction-end-state.md` §16 (follow-up PR to spec) plus a check in `VerifyAndResolveGrant`. Do not let the PR merge with a known silent-requirement gap.

---

## Sign-off

Both adversaries must post one of:
- `APPROVE — walked all items A–I, no gaps`
- `REQUEST CHANGES — items [X.N, Y.M] failed: ...`

Partial approvals ("approve A–F, not checked G–I") are not acceptable — block instead.
