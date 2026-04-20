# PR-B review checklist

**Use with:** `docs/specs/pr-b-ctx-implementation.md` and `docs/specs/vault-interaction-end-state.md`.

**Reviewers:** `@adversary-1`, `@adversary-2`. Walk every item. Sign-off is SHA-bound: `APPROVE SHA=<git-sha>` or `REQUEST CHANGES SHA=<git-sha>`. Partial approvals are rejected in favor of REQUEST CHANGES — name the specific items that failed.

**Review is two-pass:** first the spec-only PR (this B1 PR), then the code PR (B2/B3/B4). This checklist covers **both** passes; items are tagged `[spec]` (B1 gate) or `[code]` (B2/B3/B4 gate). Items untagged apply to both.

Per `feedback_review_gate_axis_enumeration.md`: before sign-off, walk every numbered section of `vault-interaction-end-state.md §13`, `§14`, `§15`, `§6`, `§11`, `§17`, `§19` and ask **"is this a contract axis I need to verify against code / spec?"** The checklist below is the axis list *we currently know about* — the enumeration pass is the safety net against axes we forgot.

---

## A. Spec conformance (§13.1 / §13.2 / §13.3 / §14 / §15)

- [ ] **[spec+code]** Verb set is exactly `{add, import, ls, use, rm}` plus the bare `ctx` (no-verb) carry-over whose contract is "print the current context name, or 'no current context'; no side effects; not listed in §13.2". No `create`, no `sh`, no `login`, no other aliases beyond the `ls` / `list` synonym already in merged §13.2. The bare form is a documented non-spec compat (impl §4.5), not a silent sixth verb.
- [ ] **[spec]** §13.2 verb table in `vault-interaction-end-state.md` has positional-JWT row removed; `ctx import` rows show `--from-file <path>` and `--from-file -` only.
- [ ] **[spec]** §13.3 "Input modes" bullet list contains exactly three bullets: `--from-file <path>`, `--from-file -`, and no-arg (pipe default).
- [ ] **[spec]** §13.3 includes the literal TTY-detection error string:
  ```
  error: no JWT on stdin. Use one of:
    drive9 ctx import --from-file <path>
    <producer> | drive9 ctx import
  ```
  (exact wording so implementations cannot diverge).
- [ ] **[spec]** §13.3 no longer contains the `SHOULD NOT` paragraph about positional-in-interactive-shell (line 309 of pre-B1) — it is obsolete after positional removal.
- [ ] **[spec]** §15 Alice flow uses `ctx import --from-file ~/alice-grant.jwt` (or piped stdin); no positional form.
- [ ] **[spec]** §6 grant default output block line 125 `SHOULD NOT` paragraph is removed (positional no longer exists to warn about).
- [ ] **[spec]** `docs/guides/vault-quickstart.md` line 113 rewritten (from endorsement to anti-endorsement with explicit removal note); line 172 deleted. Grep gate is scoped to **normative user-facing docs and executable code**: `git grep -n 'ctx import <jwt>' -- 'docs/guides/**' 'docs/reference/**' 'README.md' 'cmd/**' 'pkg/**'` returns zero matches. The PR-B impl spec and checklist in `docs/specs/` are exempt because they intentionally cite the removed form in migration / before-after / gate text; treating those as violations would make the gate unsatisfiable.
- [ ] **[code]** `cmd/drive9/cli/ctx.go` dispatcher does not accept a bare positional JWT. A bare non-flag arg on `ctx import` triggers the §2.2 error message (including migration postscript).

## B. Zero-legacy gate (§10 deletion contract)

- [ ] **[code]** `git diff main...<pr-b-branch> -- 'cmd/**' 'pkg/**'` returns **zero** matches for: `CapToken`, `CapTokenClaims`, `vault_tokens`, `cap_token`. Scope is **executable code only** (`cmd/** pkg/**`) — not `docs/**`, because the impl spec and checklist intentionally cite the removed positional form in migration / before-after / gate text. This is mechanical; any hit in the scoped paths blocks.
- [ ] **[code]** No new call into `pkg/vault/tokens*.go` or any file owned by the PR-E deletion list.
- [ ] **[code]** `pkg/**` is not modified at all by PR-B code diff. (Spec edits allowed in `docs/specs/`.)

## C. TTY / pipe / argv behavior (§5.3 of impl spec)

- [ ] **[code]** `isatty(0) == false` + no flag → stdin is read. Regression test: `TestCtxImport_PipedStdinDefault`.
- [ ] **[code]** `isatty(0) == true` + no flag → `EINVAL` with exact error string (§A). Regression test: `TestCtxImport_TTYWithoutFlag`.
- [ ] **[code]** Bare positional arg on `ctx import` → same `EINVAL` plus migration postscript. Regression test: `TestCtxImport_TTYWithBarePositional`.
- [ ] **[code]** `--from-file <path>` and `--from-file -` both work regardless of `isatty(0)`.
- [ ] **[code]** `--from-file <path>` where `path` is unreadable → `ENOENT`. Regression test required.
- [ ] **[code]** `--from-file <path>` where `path` has mode bits `0o077` set (group- or world-readable) → `EACCES`, refused **before reading file contents**, error message names `chmod 600`. Regression test: `TestCtxImport_InsecureGrantFileMode_Refused`. Verify the file body is never parsed (fake-fs spy or parser-invocation assertion).
- [ ] **[code]** `ctx add --api-key <key>` passes the key in argv. This is a known-accepted tradeoff for bootstrap (see impl §5.3). Confirm it is **flagged in the PR-B body** as a non-regression tradeoff. No silent shipping.

## D. Parse-stability fork (§13.3 + §17 short-circuit #1)

- [ ] **[code]** Order of refusal on `ctx import`, enforced in code (twelve steps per impl §2.2):
  1. empty input → `EINVAL`
  2. not-JWT → `EINVAL`
  3. missing `principal_type` → `EINVAL`
  4. `principal_type` != `delegated` → `EINVAL`
  5. missing `exp` → `EINVAL`
  6. `exp` in past → `EACCES`
  7. missing `iss` → `EINVAL`
  8. missing `agent` → `EINVAL`
  9. missing `grant_id` → `EINVAL`
  10. missing or empty `scope[]` → `EINVAL`
  11. missing `perm` → `EINVAL`
  12. `perm` not in `{read, write}` → `EINVAL`

  **Each** refusal happens before any config write. Regression test per case.
- [ ] **[code]** `principal_type=owner` → `EINVAL` with message directing to `ctx add --api-key`. Regression test: `TestCtxImport_PrincipalTypeOwner_Rejected`.
- [ ] **[code]** `exp` already-past → `EACCES`, no config write. Regression test: `TestCtxImport_ExpiredToken_NoConfigWrite`. After the test, `~/.drive9/config` does not contain the rejected context — verify via read-back, not just err check.
- [ ] **[code]** `exp` check uses local wall clock with **zero** skew tolerance (matches merged §13.3). Server verify tolerates ±60s (§16); client does not. Asymmetry documented in impl §5.6.
- [ ] **[code]** `iss` empty → `EINVAL`. Regression test: `TestCtxImport_EmptyIss_Rejected`.
- [ ] **[code]** Missing `grant_id` → `EINVAL`. Regression test: `TestCtxImport_MissingGrantID_Rejected`.
- [ ] **[code]** Missing or empty `scope[]` → `EINVAL`. Regression test: `TestCtxImport_EmptyScope_Rejected`.
- [ ] **[code]** `perm` not in enum → `EINVAL`. Regression test required.
- [ ] **[spec+code]** Parse fork in impl §2.2, end-state §11 sub-table, and `cmd/drive9/cli/ctx.go` are 1:1 in rows and order. Any row added to one MUST be added to all three in the same change — enforced at review time by column-count diff.

## E. Config file security (§5.2 of impl spec)

- [ ] **[code]** `~/.drive9/config` mode is `0600` after every `ctx` verb that writes. Regression test: `TestCtxAdd_ConfigMode0600`.
- [ ] **[code]** `~/.drive9/` directory mode is `0700`. Enforced at save time.
- [ ] **[code]** Save is atomic: write to `<path>.tmp`, `rename` to `<path>`. Regression test: `TestConfig_AtomicWriteSurvivesPartialCrash`.
- [ ] **[code]** Old-format configs (no `type` field on Context) are loaded with `Type = "owner"` backfill. Regression test: `TestConfig_LoadDefaultTypeBackfill`. **Silent-requirement bullet** — the spec does not say "backfill"; refusing old configs is the alternative and would break all existing users.

## F. Client decode ≠ server authority (Invariant #7)

- [ ] **[code]** `pkg/client/vault.go` and `cmd/drive9/cli/jwt.go` decode the JWT payload for UX only. No client code gates a data-plane request on the decoded `perm` / `scope`.
- [ ] **[code]** grep `cmd/drive9/cli/**` for any `claims.Perm` / `claims.Scope` check that short-circuits a read or write request to the server. Expected count: **zero** in PR-B (PR-B does not touch the data plane).
- [ ] **[code]** Local short-circuit on `ctx use` for expired delegated context (§17 #1) is acceptable — it refuses to activate a context whose `exp` is in the past. This is not "client authorizing", it is "client not wasting a round trip". Regression test: `TestCtxUse_ExpiredDelegated_Refused`.

## G. `label_hint` handling (Invariant #7 + §I silent-requirement)

- [ ] **[code]** `label_hint` is used only in: (a) display in `ctx ls`, (b) default-name derivation on `ctx import`, (c) audit-log string (server side, already done in PR-A). It **MUST NOT** appear in any authz decision. grep confirms this.
- [ ] **[code]** `label_hint` with newlines / escape sequences / shell metachars does not break `ctx ls` table output or JSON output. Regression test: `TestCtxImport_LabelHintNewlineEscape` with `"evil\n[INJECTED]"` — output remains one logical row (tabwriter does NOT escape by default; impl must escape explicitly in the render path).

## H. `ctx ls` output contract (§13.2)

- [ ] **[spec+code]** Table header is exactly `CURRENT\tNAME\tTYPE\tSCOPE\tPERM\tEXPIRES_AT\tSTATUS` (tab-separated, tabwriter-rendered).
- [ ] **[code]** `CURRENT` column holds `*` for exactly one row, or blank on all rows if `Config.CurrentContext == ""`.
- [ ] **[code]** Multi-scope delegated context renders as `<first> +N` in default mode, comma-joined in `-l` mode.
- [ ] **[code]** Owner context renders SCOPE as `*`, PERM as `rw`, EXPIRES_AT as `—` (em-dash).
- [ ] **[code]** `--json` output has stable field order (`name, current, type, server, scope, perm, expires_at, status, agent, grant_id`). Indent is 2 spaces.
- [ ] **[code]** `-l` and `--json` mutually exclusive → `EINVAL`.
- [ ] **[code]** Empty context set prints the two-line help. Regression test: `TestCtxLs_EmptySetHelpMessage`.

## I. `ctx use` / `ctx rm` invariants

- [ ] **[code]** `ctx use` does no FUSE-side work. Invariant #6. grep `cmd/drive9/cli/ctx.go` for any call into `pkg/mount` or similar — expected zero.
- [ ] **[code]** `ctx use <already-current>` is a no-op on `saveConfig` (avoids mtime churn). It still prints the descriptor line.
- [ ] **[code]** `ctx rm <current>` clears `Config.CurrentContext` and prints the "no current context" hint.
- [ ] **[code]** `ctx rm` does not contact the server. Server-side revocation is a separate verb (`drive9 vault revoke`, PR-A).

## J. Test suite

- [ ] **[code]** All 34 test cases from impl §6 present and passing in CI. §6 is the **canonical** enumeration; §4.2 / §5 / §K prose mentions must name tests by the same identifier as §6. If the enumeration changes, header count + prose + checklist update in the same delta.
- [ ] **[code]** No `t.Skip` in the new tests.
- [ ] **[code]** Tests assert exact errno / exit code, not just `err != nil`.
- [ ] **[code]** Tests use `testify/require` per agent standard. No hand-rolled assertions.
- [ ] **[code]** CI green on linux-amd64, darwin-amd64, darwin-arm64.

## K. Silent-requirement pass

**Run this pass last**, as a separate gate — not folded into any of A–J above. For each externally observable behavior of the `ctx` verbs, ask: **"if a strict implementer took the spec literally with no additional assumption, is the default safe / fail-closed?"**

Specific cases:

- [ ] **TTY detection reliability:** `isatty(0)` returns false on pipe, false on `</dev/null`, true on terminal. The `</dev/null` case → reads EOF immediately → empty input → refused at step 1 of parse-stability fork (EINVAL). Verify the error message in that edge case is sensible (not "not a valid JWT" which would be misleading on zero bytes). Regression test recommended.
- [ ] **Config file TOCTOU:** between `load` and `save` a concurrent `ctx` command could corrupt state. PR-B does not lock `~/.drive9/config`. Document as known limitation; add flock in a follow-up if it becomes a real issue. Not blocking for PR-B because CLI is single-user single-process typical use.
- [ ] **Grant replay across servers:** a grant minted at server A is imported at a client whose TOFU populates `Context.Server = claims.iss`. If the user later runs `drive9 ctx add --api-key --server B` and swaps which context is current, subsequent requests hit server B with a grant issued by A — which A's HMAC key will reject on verify. Covered server-side by PR-A. Client-side: no additional check. Document.
- [ ] **`label_hint` injection into randomName fallback:** if label_hint is `../../etc/passwd`, does it get used as default name and then fail on JSON map key? `Context.Name` is only a config-map key (in-process JSON), not a filesystem path. Safe. Verify by reading impl.
- [ ] **Timing on HMAC / decode:** PR-B does **not** do HMAC verify (server side only). Decode of JWT payload is `base64url + json.Unmarshal`. No timing leak concern at decode level. Document as non-applicable to PR-B.
- [ ] **`ctx import` + `--name` collision with existing delegated context:** attacker gives user a grant with `label_hint = owner-prod` (the user's existing owner context name). Default-name derivation uses label_hint → collision detected → numeric suffix appended. Owner context not overwritten. Regression test: `TestCtxImport_LabelHintCollidesWithExistingOwner`.

If any silent-requirement item uncovers a gap, the fix is typically a MUST line added to `vault-interaction-end-state.md` §13.3 (follow-up PR to spec) plus a check in `ctx.go`. Do not let PR-B merge with a known silent-requirement gap.

## L. Doc-cascade (per `e4f41feb` / `bb68b6e6` convergence)

- [ ] **[spec]** `docs/guides/vault-quickstart.md` edits listed in impl §3.5 are applied in the same spec PR.
- [ ] **[spec]** After the spec PR merges, `git grep 'ctx import <jwt>' -- 'docs/guides/**' 'docs/reference/**' 'README.md' 'cmd/**' 'pkg/**'` returns zero matches. Scope excludes `docs/specs/` because the PR-B impl spec and checklist intentionally cite the removed form in migration / before-after / gate text — treating those as violations would make the gate unsatisfiable.
- [ ] **[spec]** No other user-facing doc file (CHANGELOG, cmd-help strings in Go, e2e test fixtures) contains stale positional-form references that would survive the spec edit. Grep check at B1 review time; flag any findings as "fold into B1 or ship as B1.1 doc-only PR".

## M. Axis-enumeration sweep (per `feedback_review_gate_axis_enumeration.md`)

Before signing off, walk these end-state sections and confirm each is **either** mirrored in this checklist **or** out of scope for PR-B:

- `§13.1 context schema` → mirrored in §E, §F above. Covered.
- `§13.2 verb surface` → mirrored in §A. Covered.
- `§13.3 ctx import contract` → mirrored in §A, §C, §D. Covered.
- `§14 env vars` → **out of scope for PR-B** (deferred to PR-C per impl spec §0 scope-lock).
- `§15 grant → context flow` → mirrored in §A (Alice example edit). Covered.
- `§16 JWT claim set` → **out of scope for PR-B** (locked by PR-A; PR-B decodes, does not define).
- `§17 short-circuit table` → mirrored in §D, §F. Covered.
- `§19 parse stability` → mirrored in §D. Covered.
- `§6 grant output` → mirrored in §A. Covered.
- `§11 errno table` → mirrored in §A (§3.6 addition). Covered.

**If this sweep reveals an end-state section that is neither mirrored nor deferred, it is a silent-requirement bug in this checklist. Block and add a bullet before signing off.**

---

## Sign-off

Both adversaries must post one of:
- `APPROVE SHA=<spec-sha>` (B1 gate) or `APPROVE SHA=<code-sha>` (B4 gate) — walked all applicable items, no gaps
- `REQUEST CHANGES SHA=<sha> — items [X.N, Y.M] failed: ...`

Partial approvals ("approve A–H, not checked I–M") are not acceptable — block instead. Post-approval edits above-trivial trigger delta review against the new SHA.
