# PR-B — CLI `ctx` verb family (owner + delegated context management)

**Status:** draft (spec-first; SHA-bound sign-off gate)
**Author:** @dat9-dev1
**Reviewers:** @adversary-1, @adversary-2
**Related:** end-state `docs/specs/vault-interaction-end-state.md` §13 / §14 / §15; impl `docs/specs/pr-a-jwt-implementation.md` §1 file-map partition

---

## 0. Context and scope

PR-A (#273, `3960805`, merged 2026-04-19) shipped the JWT grant primitive on the server side: `POST /v1/vault/grants`, `DELETE /v1/vault/grants/{grant_id}`, HMAC verify path, audit events with §5 Detail conformance.

PR-B is the **CLI side** of the grant → context flow: the verbs that take a JWT produced by `drive9 vault grant` and install it into the delegatee's `~/.drive9/config`, plus the owner-side `ctx add --api-key` bootstrap. After PR-B, the user-facing path described in end-state §15 (Owner issues grant → Alice imports → Alice reads under narrowed authority) is executable end-to-end from the CLI for the first time.

### Scope lock

**In scope for PR-B (this PR):**
- 5 CLI verbs per end-state §13.2: `drive9 ctx add`, `drive9 ctx import`, `drive9 ctx ls`, `drive9 ctx use`, `drive9 ctx rm`
- Client-side JWT payload decode (UX only; server remains authoritative per Invariant #7)
- `~/.drive9/config` schema extension for dual-principal (owner / delegated) contexts
- Minor edits to already-merged `docs/specs/vault-interaction-end-state.md` to drop the positional-JWT import form (§13.2 table, §13.3 contract, §15 Alice example, §6 grant-output note, §11 errno table)

**Explicitly deferred to later PRs:**
- Env-var resolution (`DRIVE9_VAULT_TOKEN` / `DRIVE9_API_KEY` / `DRIVE9_SERVER`) → **PR-C**
- Legacy `cap_token` / `CapTokenClaims` / `vault_tokens` table deletion → **PR-E** (§10 deletion contract, binding)
- Mount-layer credential re-binding (`drive9 vault reauth`, Invariant #6) → **PR-D**
- Any issuer allow-list hardening beyond trust-on-first-use → follow-up (§13.3 issuer trust note)

### Non-goals

- No change to any server endpoint, DB schema, or audit event written by PR-A.
- No new CLI verb outside the five locked in §13.2. The `drive9 ctx` bare form (no verb) remains a non-spec compatibility convenience that prints the current context name; it is not load-bearing.
- No auto-mint, auto-refresh, auto-rotate, or auto-anything at `ctx` layer. A context that expires is refused on `ctx use`; the user must re-import.

---

## 1. Files touched

Code (final list; B2 cherry-picks, B3 reconciles):

| Path | Change | Origin |
| --- | --- | --- |
| `cmd/drive9/cli/ctx.go` | **new** — 5-verb dispatcher + handlers | port of PR #274 with positional form removed |
| `cmd/drive9/cli/ctx_test.go` | **new** — table-driven tests for 5 verbs | port of PR #274, regression tests added per §6 below |
| `cmd/drive9/cli/jwt.go` | **new** — client-side JWT payload decode (no verify) | port of PR #274 |
| `cmd/drive9/cli/config.go` | **extend** — `Context` dual-principal fields, `Config.loadConfig` default-type backfill | port of PR #274 |
| `cmd/drive9/cli/main.go` | **edit** — route `drive9 ctx` to `cli.Ctx` | minimal wiring; one-line change |
| `cmd/drive9/cli/randomname.go` | **new, tiny** — entropy-backed default-name generator when JWT has no `label_hint` | new, see §4.3 |

Spec (this PR):

| Path | Change |
| --- | --- |
| `docs/specs/pr-b-ctx-implementation.md` | **new** — this document |
| `docs/specs/pr-b-review-checklist.md` | **new** — mirrors `pr-a-review-checklist.md` shape |
| `docs/specs/vault-interaction-end-state.md` | **edit** §13.2 / §13.3 / §15 / §6 / §11 — drop positional-JWT import, add TTY/pipe default, consistent Alice example |
| `docs/guides/vault-quickstart.md` | **edit** lines 113, 172, 329 — drop positional mentions, align with stdin/`--from-file` canonical forms |

Explicitly NOT touched:
- `pkg/server/**` — no server change.
- `pkg/vault/**` — no store, sign, verify change.
- `pkg/client/vault.go` — no env-var wiring (that is PR-C).
- Legacy `CapToken*` / `vault_tokens*` — zero references added (enforced by B1 review gate "grep=0").

---

## 2. Behavior — verb by verb

End-state §13.2 is the source of truth. This section is **operational**: it says how each verb behaves on the happy and sad paths, what it writes to `~/.drive9/config`, and what stderr/stdout look like. Where §13.2 is silent, this section fills the gap; any fill-in is flagged `[fill]` so reviewers can decide whether to fold back into end-state.

### 2.1 `drive9 ctx add`

```
drive9 ctx add --api-key <key> [--name <n>] [--server <url>]
```

Owner-principal context bootstrap. Writes a `Context{Type: "owner", APIKey: <key>, Server: <resolved>}` entry.

- `--api-key`: **required**. No prompt, no stdin fallback in PR-B (API keys are pasted once at setup; a prompt would be kubectl-style bloat). Empty / missing → error `"--api-key is required"`, exit 1.
- `--name`: optional. On absence, generate a random two-word name (adjective-noun, ~10 bits) so first-time users never have to think of one. On collision with an existing context, append a numeric suffix.
- `--server`: optional. On absence, inherit from `Config.Server` (populated by a prior `ctx add` or `ctx use`); if that is also empty, fall back to the compiled default `https://drive9.dev` `[fill]` — this matches §14.3 resolution for unset `DRIVE9_SERVER`.
- If `Config.CurrentContext == ""` at save time, the new context becomes current. Spec §13.1 invariant: "exactly one current context, or zero iff Contexts is empty".

Output on success:

```
added context "owner-prod" (owner)
current context is now "owner-prod"
```

If the new context did not become current (i.e. there was already one), the second line is omitted.

### 2.2 `drive9 ctx import`

```
drive9 ctx import --from-file <path>           # explicit file
drive9 ctx import --from-file -                # explicit stdin
drive9 ctx import                              # default: stdin iff !isatty(0)
```

Delegated-principal context bootstrap from a JWT minted by `drive9 vault grant`.

**Input resolution ladder (PR-B lock):**

1. If `--from-file <path>` given and `path != "-"` → read file.
2. If `--from-file -` given → read stdin until EOF.
3. If no flag given AND `isatty(0) == false` → read stdin until EOF (matches `pass insert`, `gpg --import`, `vault login -method=token`).
4. If no flag given AND `isatty(0) == true` → **error** with exact message:

   ```
   error: no JWT on stdin. Use one of:
     drive9 ctx import --from-file <path>
     <producer> | drive9 ctx import
   ```

   Exit 1, mapped to `EINVAL` per §11.

The positional-JWT form (`drive9 ctx import <jwt>`) is **not** accepted — see §3 for the end-state spec edit that drops it. If a bare positional argument is present (e.g. `drive9 ctx import eyJhbGc...`), the verb errors with the same "no JWT on stdin" message **plus** a one-line postscript: `note: the positional-JWT form was removed; paste via stdin or save to a file first.` This protects users migrating from older docs or the PR #274 prior draft.

**Parse-stability fork (end-state §19 / §13.3 refusal cases), in order:**

1. Input empty after whitespace trim → `EINVAL`, message `"ctx import: empty input"`.
2. Not a structurally valid JWT (three base64url segments, JSON middle) → `EINVAL`, message `"ctx import: not a valid JWT: <decode error>"`.
3. `principal_type` claim is not `"delegated"` → `EINVAL`, message `"ctx import: token principal_type is %q, not \"delegated\"; use \`drive9 ctx add --api-key\` for owner credentials"`.
4. `exp` claim is in the past (local wall clock, no skew tolerance — matches end-state §17 short-circuit #1) → `EACCES`, message `"ctx import: token already expired at <RFC3339>"`.
5. `iss` claim is empty → `EINVAL`, message `"ctx import: token is missing the \`iss\` claim"`.
6. `perm` claim is not one of `{"read", "write"}` → `EINVAL`, message `"ctx import: token perm is %q, expected one of {read, write}"`.

All six refusals happen **before** any write to `~/.drive9/config`. No partial-write state is ever reachable.

**On success:**
- Default name derivation (matches merged §13.3):
  1. `claims.LabelHint` if set AND not already in use.
  2. Else `<agent>-<first-scope-root>` where `<first-scope-root>` is the secret name from `/n/vault/<secret>[/<key>]`.
  3. Else `<agent>` alone.
  4. Else `<first-scope-root>` alone.
  5. Else random name.
  6. On collision, append `-2`, `-3`, …
- `--name <n>` overrides (1)–(5) entirely.
- Context fields populated from JWT claims per §13.1:
  - `Type: "delegated"`
  - `Server: claims.iss` (trust-on-first-use; §13.3 issuer trust note applies)
  - `Token: <raw JWT string>` (stored verbatim for re-transmission)
  - `Agent, Scope, Perm, ExpiresAt, GrantID, LabelHint` from the corresponding claims
- If `Config.CurrentContext == ""` → new context becomes current.
- If `Config.Server == ""` → `Config.Server = claims.iss` (first-server-wins).

Output on success:

```
imported context "alice-prod-db" (delegated, grant grt_7f2a...)
current context is now "alice-prod-db"
```

(Second line omitted if another context was already current.)

### 2.3 `drive9 ctx ls [-l|--json]`

Output matches end-state §13.2 (`CURRENT NAME TYPE SCOPE PERM EXPIRES_AT STATUS`).

- No args → default table form, SCOPE rendered as `first +N` for multi-scope delegated contexts.
- `-l` / `--long` → SCOPE rendered as comma-joined full list.
- `--json` → `{current_context, contexts: [{name, current, type, server, scope[], perm, expires_at, status, agent, grant_id}]}`, indent 2 spaces. Stable key order.
- `-l` and `--json` are mutually exclusive → `EINVAL`.
- Empty context set → stdout:
  ```
  no contexts configured
  run: drive9 ctx add --api-key <key>  (owner)
       <producer> | drive9 ctx import  (delegated)
  ```
- `STATUS` computed at display time from `ExpiresAt` (§17 local short-circuit). Owner contexts always `active`. Delegated contexts: `expired` iff `!ExpiresAt.IsZero() && !ExpiresAt.After(now)`, else `active`. No other values in PR-B.

### 2.4 `drive9 ctx use <name>`

Activates a context (rewrites `Config.CurrentContext`).

- No-arg / flag-arg / >1-arg → `EINVAL`, message `"usage: drive9 ctx use <name>"`.
- Name not in `Config.Contexts` → `ENOENT`-equivalent exit, message `"context %q not found; run: drive9 ctx ls"`.
- Context is `delegated` with expired `exp` → **refuse** (§17 short-circuit on activation), message `"context %q expired at %s; request a new grant and re-import"`.
- Context is already current → still succeed, print `"context %q is already active"` + descriptor line. No-op on `saveConfig` (avoids atime/mtime churn).

**Descriptor line** (per end-state §15 F15, two-line success notice):

```
switched to context "alice-prod-db"
  delegated: scope prod-db/DB_URL, perm read, expires 2026-04-18T19:00:00Z
```

Or for owner:

```
switched to context "owner-prod"
  owner credentials, server https://drive9.dev
```

**Invariant #6 (no auto-rebind):** `ctx use` does **no** FUSE-side work. It only rewrites `~/.drive9/config`. Running mounts continue to hold whatever credential they were bound to at mount time; the only way to rebind is `drive9 vault reauth` (PR-D). This is enforced structurally by `ctx use` not depending on any mount-manager package.

### 2.5 `drive9 ctx rm <name>`

Deletes a context entry.

- Args-validation identical to `ctx use`.
- Name not found → `ENOENT`-equivalent, message `"context %q not found"`.
- Delete the entry from `Config.Contexts`. If it was current, set `Config.CurrentContext = ""`.
- Output:
  ```
  removed context "alice-prod-db"
  ```
  If current was cleared, add a second line: `no current context; run 'drive9 ctx use <name>' to activate one`.
- Does **not** attempt to notify the server (delegated revocation is server-side via `drive9 vault revoke`, not client-side via `ctx rm`). `ctx rm` is a purely local operation. Stale audit would record `grant.revoked` via a separate command; §13.3 notes this separation.

---

## 3. End-state spec edits (in this PR)

The following edits to `docs/specs/vault-interaction-end-state.md` drop the positional-JWT `ctx import` form and tighten the TTY / pipe default. All other §13 invariants (schema, verb set, resolution ladder, trust-on-first-use) are unchanged.

### 3.1 §13.2 verb table

**Before** (current merged, lines 266–272):

```
drive9 ctx add --api-key <key> [--name <n>] [--server <url>]      # add owner context
drive9 ctx import --from-file <path>                              # add delegated context from a file (recommended)
drive9 ctx import --from-file -                                   # add delegated context from stdin (recommended)
drive9 ctx import <jwt>                                           # convenience form; JWT leaks to shell history
drive9 ctx ls [-l|--json]                                         # list contexts (offline — reads only local config)
drive9 ctx use <name>                                             # activate a context
drive9 ctx rm <name>                                              # delete a context
```

**After:**

```
drive9 ctx add --api-key <key> [--name <n>] [--server <url>]      # add owner context
drive9 ctx import --from-file <path>                              # add delegated context from a file
drive9 ctx import [--from-file -]                                 # add delegated context from stdin (default when stdin is a pipe)
drive9 ctx ls [-l|--json]                                         # list contexts (offline — reads only local config)
drive9 ctx use <name>                                             # activate a context
drive9 ctx rm <name>                                              # delete a context
```

Following paragraph (current line 273) becomes:

> Both `ctx import` forms are equivalent in effect. Stdin is read by default when stdin is a pipe (`isatty(0) == false`); the explicit `--from-file -` form is accepted for scripts that want the intent to be unambiguous. When stdin is a TTY and no `--from-file` is supplied, `ctx import` exits with `EINVAL` and prints a one-line help pointing at the two canonical forms.

### 3.2 §13.3 ctx import contract

Delete current lines 298–302 (the three-bullet input-modes list) and replace with:

> Input modes:
>
> - `drive9 ctx import --from-file <path>` reads the JWT from a file.
> - `drive9 ctx import --from-file -` reads the JWT from stdin explicitly.
> - `drive9 ctx import` (no arguments) reads the JWT from stdin iff stdin is not a TTY. When stdin is a TTY, `ctx import` exits `EINVAL` and prints:
>
>   ```
>   error: no JWT on stdin. Use one of:
>     drive9 ctx import --from-file <path>
>     <producer> | drive9 ctx import
>   ```
>
> In all three modes, the JWT must be a single token with surrounding whitespace trimmed. The JWT **MUST NOT** be passed as a positional argument; that form was removed in PR-B because a warning cannot unexpose a secret that has already reached shell history and `/proc/<pid>/cmdline`.

Delete current lines 309 (the SHOULD-NOT positional-argument paragraph) — rationale is now captured in the removal note above.

Delete current line 125 (the "Delegatees **SHOULD NOT** paste" paragraph in §6) — same reason; the form no longer exists.

### 3.3 §15 Alice example

The Alice block at lines 373–379 is already correct (uses `--from-file ~/alice-grant.jwt`) — no edit needed.

### 3.4 §6 grant output

Line 115 currently shows the default human output:

```
drive9 ctx import --from-file -
<jwt>
---
...
```

This remains pipe-friendly and correct after the positional drop. No edit.

Line 125 (the `SHOULD NOT` paragraph about positional paste) is deleted as noted in §3.2 above.

### 3.5 `docs/guides/vault-quickstart.md` doc-cascade

Per `feedback_review_gate_blindness.md` round-4 (stale cross-doc references surviving a supposedly-complete spec pass), the user-facing quickstart must be kept in lock-step with the end-state spec. Verified via `grep -n 'ctx import\|positional\|<jwt>' docs/guides/vault-quickstart.md` (SHA `3b65a3c`):

- Line 113 — "Avoid distributing the JWT as a copyable one-liner (`drive9 ctx import <jwt>`). That form is valid (see Part 3) but records the token in the delegatee's shell history and process argument list." → **rewrite**: "Distribute the JWT as a file attachment or as piped input. Do not paste it as a positional argument; the positional form was removed for this reason."
- Line 172 — "`drive9 ctx import <jwt>` (positional) also works, but it will be recorded in shell history. Use it only for scripting and testing." → **delete**.
- Line 329 (quick reference table) — already says `drive9 ctx import --from-file <path>` (or `--from-file -` for stdin). **No edit** — already correct.
- Other `ctx import <jwt>` references: none beyond 113 and 172.

No other doc files require edits (verified by `git grep -l 'ctx import <jwt>' -- docs/ README.md` returns only `end-state` and `quickstart`).

### 3.6 §11 errno table

Current `§11 Errno Table (Normative)` lines 208–215 do not explicitly cover `ctx import` EINVAL / EACCES / ENOENT cases. Add a new sub-table under the existing one:

```
| `ctx import` refusal cause | Errno |
| --- | --- |
| Empty / unparseable / structurally invalid JWT | `EINVAL` |
| Missing required claim (`iss`, `exp`, `perm`, `principal_type`, `agent`) | `EINVAL` |
| Token `principal_type` is not `"delegated"` | `EINVAL` |
| Token `exp` already in the past at import | `EACCES` |
| `--from-file <path>` names a non-existent or unreadable file | `ENOENT` |
| Stdin is a TTY and no input flag given | `EINVAL` (with help pointer) |
```

---

## 4. Implementation notes (B3 reconciliation)

PR #274's `ctx.go` is the starting point. B3 work:

### 4.1 Drop positional-JWT

PR #274 accepts a bare positional JWT after `--from-file` check (ctx.go:195). B3 removes that branch and replaces with the TTY-detection default.

- Add `isatty(0)` check via `golang.org/x/term`. We already depend on `x/term` transitively via FUSE; if not, add it.
- If `--from-file` flag is absent and `isatty(0) == true` → return the §2.2 error verbatim.
- If a bare non-flag positional is present → same error plus the migration postscript.

### 4.2 Friend-of-cherry-pick regression tests

Port PR #274's `ctx_test.go`, then add (B3-new) tests for:

- `TestCtxImport_TTYWithoutFlag`: fake TTY stdin, no flag → `EINVAL` exit, stderr contains the exact help-pointer string.
- `TestCtxImport_TTYWithBarePositional`: fake TTY stdin, bare positional → same error + postscript line.
- `TestCtxImport_PipedStdinDefault`: non-TTY stdin with JWT bytes, no flag → imports successfully.
- `TestCtxImport_ExpiredToken`: JWT with `exp = now - 1h` → `EACCES` exit, no config write. Read `~/.drive9/config` afterwards, assert the context does **not** appear.
- `TestCtxImport_PrincipalTypeOwner`: JWT with `principal_type=owner` → `EINVAL` exit, stderr directs to `ctx add --api-key`.
- `TestCtxImport_EmptyIss`: JWT missing `iss` claim → `EINVAL` exit.
- `TestCtxImport_LabelHintNewlineEscape`: `label_hint` = `"evil\n[INJECTED]"`. After import, `ctx ls` renders it on a single line (tabwriter). Matches `pr-a-review-checklist.md` §I silent-requirement bullet on `label_hint` injection.
- `TestCtxUse_ExpiredDelegated`: importing a not-yet-expired grant, then fast-forwarding time past `exp` and calling `ctx use` → refused with exact message.
- `TestCtxUse_ActivateOwner`: owner context descriptor line format check.
- `TestCtxAdd_GeneratesName`: no `--name` → generated name of form `<adj>-<noun>` (regex `^[a-z]+-[a-z]+$`).
- `TestCtxAdd_CollisionSuffix`: two `ctx add` without `--name`, force seed to collide → second gets `-2` suffix.
- `TestCtxRm_CurrentClears`: removing the current context → `CurrentContext == ""` in persisted config.
- `TestConfig_LoadDefaultTypeBackfill`: old-format `~/.drive9/config` (no `type` field on Context) → loaded with `Type = "owner"`. Ensures first-run users who upgrade don't lose access. **This is a silent-requirement test** (`feedback_silent_requirement_blind_spot.md`): the spec doesn't explicitly say "backfill", but the alternative (refuse old configs) would break every existing user. The backfill is in PR #274's `loadConfig`; this test pins it.

All tests are `testify/require`-style per agent standard.

### 4.3 Random name generator

PR #274's `randomName` uses `crypto/rand` over a small wordlist. Keep that — do **not** use `math/rand` even with seeding, because a predictable context name is a weak-but-real information leak (config file contents are not public, but an attacker who gets a shell can enumerate).

Wordlist sizing:
- Adjectives: ≥32 (5 bits)
- Nouns: ≥32 (5 bits)
- 10 bits → ~1000 name combinations. Collision probability at N contexts: `N*(N-1)/2 * 1/1024`. For N ≤ 10 (realistic ceiling), collision is <5%. Adequate — we fall back to `-2` suffix on collision anyway.

### 4.4 `Context` JSON compatibility

`~/.drive9/config` is a single JSON file, 0600, written atomically via `os.WriteFile`. Schema extension in PR #274 is additive: old configs have only `api_key`, new configs have `type, server, api_key, token, agent, scope, perm, expires_at, grant_id, label_hint`. `loadConfig` backfills `type=owner` for old entries (see §4.2 test).

**Atomic write check:** PR #274 uses `os.WriteFile(path, data, 0o600)`. That is **not atomic** on POSIX — a crash mid-write leaves a truncated file. B3 task: replace with write-to-`path.tmp`+`rename`, which is atomic on POSIX. Regression test: simulate a partial write (write only half the bytes, then crash), reload → old config still intact.

### 4.5 `drive9 ctx` bare form (compatibility)

Current pre-PR-B implementation: `drive9 ctx` (no verb) prints the current context name. This is a non-spec convenience not listed in §13.2. Keep it as-is for backwards compat; the dispatcher routes `args == []` to `ctxShow()`. **Flag this in PR-B body as a known non-spec carry-over**; removal would be a separate UX-cleanup PR, not a bundle into PR-B.

---

## 5. Security review lines (to walk)

These are the adversary-hat concerns. Each is a reviewer gate in `pr-b-review-checklist.md`.

### 5.1 Client-side decode ≠ server authority (Invariant #7)

Claim: "the JWT payload is decoded client-side at `ctx import` time and used to populate context fields for display / activation; authorization always remains server-side."

Test: grep the B2/B3 diff for any use of decoded claims that *gates* a request. Acceptable uses: display in `ctx ls`, scope rendering, expired-on-activation refusal (local short-circuit §17). Unacceptable uses: any code path that denies a read/write based on the client-decoded `perm` or `scope` without the server being consulted. There should be zero in PR-B because PR-B does not touch the data plane — but the gate is here in case the cherry-pick drags in a client-side authz check.

### 5.2 Config file security

- Mode must be `0600` at both create and save time. (PR #274 does this.)
- Directory `~/.drive9/` must be `0700`. Check / enforce on every save.
- Atomic write via `.tmp` + `rename` (see §4.4).

Test: after any `ctx` verb, assert `stat(~/.drive9/config).mode & 0o777 == 0o600` and `stat(~/.drive9).mode & 0o777 == 0o700`.

### 5.3 No secret-in-argv, ever

Enforced by §3.2 end-state edit + §2.2 behavior. Regression test: `TestCtxImport_TTYWithBarePositional`.

Additional check: `drive9 ctx add --api-key <key>` passes the API key in argv, which has the same `/proc/<pid>/cmdline` exposure. This is accepted by the current spec because API keys are setup-time credentials (paste-once) and the alternative (stdin prompt) complicates the bootstrap. **Flag for qiffang** as a known-accepted tradeoff; a `--api-key-file` option could be a follow-up. Not blocking for PR-B.

### 5.4 Trust-on-first-use on `iss`

`ctx import` writes `Context.Server = claims.iss` with no network check. Attack: owner crafts a grant with `iss=https://evil.example.com`; delegatee imports; delegatee's subsequent requests hit the attacker's server.

Mitigation (per end-state §13.3 issuer trust note): the owner's distribution channel is the trust anchor, not the CLI. PR-B does not add an allow-list. Follow-up work: `--expect-issuer` flag or allow-list pinned at `ctx add --api-key` time.

Test: `TestCtxImport_TofuIssuerPopulated`. Import a JWT with a specific `iss`, read back the context, assert `Server == iss`. Documents the behavior; does not prevent the attack.

### 5.5 `label_hint` log injection (§I silent-requirement)

`label_hint` is attacker-controllable (the owner picks it, but a compromised owner or a delegated-redelegation chain future-PR could introduce untrusted values). If `label_hint` is `"evil\n[INJECTED]"`, it MUST NOT break audit-log parsing or table output.

Test: `TestCtxImport_LabelHintNewlineEscape` — verifies tabwriter output stays on one physical line (tabwriter quotes newlines implicitly? verify). If tabwriter does **not** escape, B3 must escape explicitly in `renderScope` / table-format code.

### 5.6 Clock skew on `exp` check

End-state §13.3 locks **zero** clock skew on the local short-circuit. Test: grant with `exp = now + 500ms`, import succeeds; sleep 1s; `ctx use` fails. This matches merged spec behavior exactly and prevents drift.

Server-side verification has ±60s leeway (§16 / PR-A). Client-side has 0. Document this asymmetry in `pr-b-review-checklist.md` for reviewer awareness.

---

## 6. Test plan (must ship with PR)

Reconciles PR #274's test coverage (18 tests) with B3-new tests from §4.2. Full list (22 cases after consolidation):

**From PR #274 (port as-is):**
1. `TestCtxAdd_Owner_WritesConfig`
2. `TestCtxAdd_MultipleContexts_NewBecomesNonCurrent`
3. `TestCtxAdd_DuplicateName_Rejected`
4. `TestCtxImport_Delegated_FromFile_WritesContext`
5. `TestCtxImport_Delegated_FromStdin`
6. `TestCtxImport_DefaultNameFromLabelHint`
7. `TestCtxImport_DefaultNameFromAgentScope`
8. `TestCtxLs_TableOutput`
9. `TestCtxLs_JSONOutput`
10. `TestCtxLs_EmptySetHelpMessage`
11. `TestCtxUse_SwitchesCurrent`
12. `TestCtxUse_NotFound`
13. `TestCtxRm_Removes`
14. `TestCtxRm_NotFound`

**B3-new:**
15. `TestCtxImport_TTYWithoutFlag` (§5.3, §4.2)
16. `TestCtxImport_TTYWithBarePositional` (§5.3, §4.2)
17. `TestCtxImport_ExpiredToken_NoConfigWrite` (§4.2)
18. `TestCtxImport_PrincipalTypeOwner_Rejected` (§4.2)
19. `TestCtxImport_EmptyIss_Rejected` (§4.2)
20. `TestCtxImport_LabelHintNewlineEscape` (§5.5)
21. `TestCtxUse_ExpiredDelegated_Refused` (§5.6)
22. `TestConfig_LoadDefaultTypeBackfill` (§4.2)
23. `TestConfig_AtomicWriteSurvivesPartialCrash` (§4.4)
24. `TestCtxAdd_ConfigMode0600` (§5.2)
25. `TestCtxImport_TofuIssuerPopulated` (§5.4, documents behavior)

(Final count may shift; B3 adds any missing per review.)

**CI matrix:** linux-amd64, darwin-amd64, darwin-arm64 (already covered by main matrix). No Windows (FUSE dep elsewhere already excludes Windows).

---

## 7. PR mechanics

- **Branch:** `dev1/vault-impl-pr-b` (code), `dev1/vault-impl-pr-b-spec` (this spec).
- **Commit identity:** `qiffang <qiffang33@gmail.com>` per memory `feedback_git_commit_email.md`.
- **CI gates (mandatory green):** `go test ./...`, `go vet`, `staticcheck`, gofmt.
- **Review protocol:** SHA-bound sign-off per `678e76a9` / `ccb1755d`. adversaries run the checklist against a specific SHA and `APPROVE SHA=<...>` or `REQUEST CHANGES SHA=<...>`. Any post-approval edit above-trivial (>5 lines, behavioral) triggers delta-review against the new SHA.

---

## 8. Cross-reference to review checklist

See `docs/specs/pr-b-review-checklist.md`. Each item is traceable to a numbered section here.

---

## 9. Deferred explicitly

| Item | Deferred to | Reason |
| --- | --- | --- |
| `DRIVE9_VAULT_TOKEN` env var resolution | PR-C | Per `pr-a-jwt-implementation.md` line 31, env vars are a separate concern. Folding them here doubles the review surface. |
| `DRIVE9_API_KEY` env var resolution | PR-C | Same. |
| `DRIVE9_SERVER` env var resolution | PR-C | Same. |
| `drive9 vault reauth` (mount rebind) | PR-D | Mount-layer work; depends on FUSE manager refactor that is not in PR-B. |
| `CapToken*` / `vault_tokens` deletion | PR-E | §10 deletion contract is binding. PR-B adds **zero** references to legacy types. |
| Issuer allow-list / `--expect-issuer` | future hardening | Out of scope per end-state §13.3 trust note. |
| `drive9 ctx` bare-form removal | separate UX-cleanup PR | Non-spec compat carry-over, noted in §4.5. |

---

## 10. Non-regression with PR-A

PR-B adds no DB query, no HTTP handler, no audit event, no token signing path. Grep rules for reviewer:

- `git diff main...dev1/vault-impl-pr-b -- 'pkg/**'` must be empty (no server-side change).
- `git diff main...dev1/vault-impl-pr-b -- 'cmd/drive9/cli/**'` is the full PR-B code surface.
- Zero matches for `CapToken`, `CapTokenClaims`, `vault_tokens`, `cap_token` in the diff.

Enforced mechanically in `pr-b-review-checklist.md` §B.

---

## 11. Open items (for sign-off)

- [ ] adversary-1 sign-off on this spec SHA
- [ ] adversary-2 sign-off on this spec SHA
- [ ] qiffang notice (spec PR opened; no action needed unless the verb-surface question from `31d41dc7` / `5e171449` warrants escalation — retracted in `7efae6dc`, noted here for traceability)
