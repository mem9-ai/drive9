package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// captureStderrE runs fn with os.Stderr redirected to a pipe and
// returns whatever fn wrote to stderr. Mirrors captureStdoutE.
func captureStderrE(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	done := make(chan []byte, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.Bytes()
	}()
	fnErr := fn()
	_ = w.Close()
	return string(<-done), fnErr
}

func TestIsHelpArgsScansBeforeDashDash(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "empty", args: nil, want: false},
		{name: "bare help first arg", args: []string{"help"}, want: true},
		{name: "long help first arg", args: []string{"--help"}, want: true},
		{name: "dash prefixed help flag value position", args: []string{"--from-file", "--help"}, want: true},
		{name: "short help after flag", args: []string{"--name", "-h"}, want: true},
		{name: "bare help after first arg is data", args: []string{"pattern", "help"}, want: false},
		{name: "bare help as flag value is data", args: []string{"--name", "help"}, want: false},
		{name: "bare help data before dash prefixed help keeps scanning", args: []string{"pattern", "help", "--help"}, want: true},
		{name: "after dash dash is data", args: []string{"/n/vault/aws", "--", "env", "--help"}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsHelpArgs(tc.args); got != tc.want {
				t.Fatalf("IsHelpArgs(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}

func TestCtxLeafHelpScansFullArgumentList(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		firstLine string
	}{
		{name: "show", args: []string{"show", "--json", "--help"}, firstLine: "usage: drive9 ctx show [--json] [--reveal]"},
		{name: "add", args: []string{"add", "--api-key", "--help"}, firstLine: "usage: drive9 ctx add --api-key <key> [--name <n>] [--server <url>]"},
		{name: "import", args: []string{"import", "--from-file", "--help"}, firstLine: "usage: drive9 ctx import [--from-file <path|->] [--name <name>]"},
		{name: "fork", args: []string{"fork", "--from", "--help"}, firstLine: "usage: drive9 ctx fork [<new>] [--from <ctx>] [--json]"},
		{name: "ls", args: []string{"ls", "--type", "--help"}, firstLine: "usage: drive9 ctx ls [-l|--json] [--type <kind>|--scoped]"},
		{name: "use", args: []string{"use", "--help"}, firstLine: "usage: drive9 ctx use [--] <name>"},
		{name: "rm", args: []string{"rm", "old", "--help"}, firstLine: "usage: drive9 ctx rm <name>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			out, err := captureStdoutE(t, func() error {
				return Ctx(tc.args)
			})
			if err != nil {
				t.Fatalf("Ctx(%v): %v", tc.args, err)
			}
			if !strings.HasPrefix(out, tc.firstLine) {
				t.Fatalf("stdout = %q, want first line %q", out, tc.firstLine)
			}
		})
	}
}

// seedThreeContexts writes one owner + one delegated + one fs_scoped
// context to local config under an isolated $HOME for use by the
// ctx list / ctx rm / token deprecation alias tests in this file.
func seedThreeContexts(t *testing.T) {
	t.Helper()
	withIsolatedHome(t)
	cfg := &Config{Contexts: map[string]*Context{}}
	if _, err := ctxAdd(cfg, "owner-ctx", &Context{Type: PrincipalOwner, Server: "https://s", APIKey: "dat9_owner"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctxAdd(cfg, "delegated-ctx", &Context{Type: PrincipalDelegated, Server: "https://s", Token: "delegated-jwt", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctxAdd(cfg, "smoke", &Context{Type: PrincipalFSScoped, Server: "https://s", APIKey: "dat9_scoped", Scope: []string{"/scratch/smoke:read,write,list"}, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	cfg.CurrentContext = "owner-ctx"
	if err := saveConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

// TestCtxListFiltersByTypeFlag verifies `drive9 ctx list --type fs_scoped`
// returns only fs_scoped contexts. Covers task #11 acceptance criterion
// from @dev-1 msg `17ccf84e`.
func TestCtxListFiltersByTypeFlag(t *testing.T) {
	seedThreeContexts(t)

	out, err := captureStdoutE(t, func() error {
		return Ctx([]string{"list", "--type", "fs_scoped"})
	})
	if err != nil {
		t.Fatalf("ctx list --type fs_scoped: %v", err)
	}
	if !strings.Contains(out, "smoke") {
		t.Fatalf("expected fs_scoped 'smoke' in output, got: %s", out)
	}
	if strings.Contains(out, "owner-ctx") || strings.Contains(out, "delegated-ctx") {
		t.Fatalf("expected only fs_scoped rows, got: %s", out)
	}
}

// TestCtxListScopedShortcutMatchesTypeFilter verifies the `--scoped`
// shorthand behaves identically to `--type fs_scoped`. This is the
// user-friendly alias from @gtm-1 msg `2eb962dd` ("--scoped shortcut").
func TestCtxListScopedShortcutMatchesTypeFilter(t *testing.T) {
	seedThreeContexts(t)

	withType, err := captureStdoutE(t, func() error {
		return Ctx([]string{"list", "--type", "fs_scoped"})
	})
	if err != nil {
		t.Fatalf("--type form: %v", err)
	}
	withScoped, err := captureStdoutE(t, func() error {
		return Ctx([]string{"list", "--scoped"})
	})
	if err != nil {
		t.Fatalf("--scoped form: %v", err)
	}
	if withType != withScoped {
		t.Fatalf("--scoped output diverged from --type fs_scoped\n--type:\n%s\n--scoped:\n%s", withType, withScoped)
	}
}

// TestCtxListTypeFilterEqualsForm verifies `--type=fs_scoped` works
// the same as `--type fs_scoped`. Standard Go flag-parsing parity.
func TestCtxListTypeFilterEqualsForm(t *testing.T) {
	seedThreeContexts(t)

	out, err := captureStdoutE(t, func() error {
		return Ctx([]string{"list", "--type=fs_scoped"})
	})
	if err != nil {
		t.Fatalf("--type=fs_scoped: %v", err)
	}
	if !strings.Contains(out, "smoke") {
		t.Fatalf("expected smoke row, got: %s", out)
	}
}

// TestCtxListRejectsUnknownType verifies the filter validates the
// principal-type literal up front, so a typo like `--type scoped`
// gets a clear error instead of silently returning all rows.
func TestCtxListRejectsUnknownType(t *testing.T) {
	seedThreeContexts(t)

	_, err := captureStdoutE(t, func() error {
		return Ctx([]string{"list", "--type", "scoped"}) // not a valid value
	})
	if err == nil {
		t.Fatal("expected error for unknown --type, got nil")
	}
	if !strings.Contains(err.Error(), "unknown --type") {
		t.Fatalf("expected 'unknown --type' error, got: %v", err)
	}
}

// TestCtxListJSONFilterRespectsType verifies the --json output path
// also applies the filter. Otherwise scripts would silently get an
// unfiltered list when combining --json with --type.
func TestCtxListJSONFilterRespectsType(t *testing.T) {
	seedThreeContexts(t)

	out, err := captureStdoutE(t, func() error {
		return Ctx([]string{"list", "--json", "--type", "fs_scoped"})
	})
	if err != nil {
		t.Fatalf("ctx list --json --type: %v", err)
	}
	var payload struct {
		Contexts []map[string]any `json:"contexts"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode json: %v\noutput: %s", err, out)
	}
	if len(payload.Contexts) != 1 {
		t.Fatalf("expected 1 fs_scoped row in JSON, got %d: %s", len(payload.Contexts), out)
	}
	if payload.Contexts[0]["type"] != string(PrincipalFSScoped) {
		t.Fatalf("expected type=fs_scoped, got: %v", payload.Contexts[0])
	}
}

// TestCtxRmRefusesCurrentContext is the @adversary-1 msg `7c8dc13c`
// safety guard: removing the active context would leave a dangling
// CurrentContext pointer. The user must `ctx use <other>` first.
func TestCtxRmRefusesCurrentContext(t *testing.T) {
	seedThreeContexts(t)

	// Fixture has owner-ctx as current.
	err := Ctx([]string{"rm", "owner-ctx"})
	if err == nil {
		t.Fatal("expected error refusing to remove current context, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to remove current context") {
		t.Fatalf("error must explain refusal, got: %v", err)
	}
	if !strings.Contains(err.Error(), "ctx use") {
		t.Fatalf("error must point user to `ctx use`, got: %v", err)
	}

	// Config must be unchanged.
	cfg := loadConfig()
	if _, ok := cfg.Contexts["owner-ctx"]; !ok {
		t.Fatal("owner-ctx removed despite refusal")
	}
	if cfg.CurrentContext != "owner-ctx" {
		t.Fatalf("CurrentContext changed to %q", cfg.CurrentContext)
	}
}

// TestCtxRmFSScopedEmitsServerNotRevokedWarning is the @qiffang msg
// `7472f701` safety usage requirement: removing a fs_scoped context
// must explicitly tell the user that the server-side token remains
// valid, and direct them to `drive9 token revoke -` for revocation.
func TestCtxRmFSScopedEmitsServerNotRevokedWarning(t *testing.T) {
	seedThreeContexts(t)

	// Switch off owner-ctx so we can rm smoke (it's not current).
	if err := Ctx([]string{"use", "delegated-ctx"}); err != nil {
		t.Fatalf("ctx use delegated-ctx: %v", err)
	}

	out, err := captureStdoutE(t, func() error {
		return Ctx([]string{"rm", "smoke"})
	})
	if err != nil {
		t.Fatalf("ctx rm smoke: %v", err)
	}
	if !strings.Contains(out, "removed local context") {
		t.Fatalf("missing success line, got: %s", out)
	}
	if !strings.Contains(out, "NOT revoked") {
		t.Fatalf("fs_scoped removal MUST warn the server token is NOT revoked, got: %s", out)
	}
	if !strings.Contains(out, "drive9 token revoke -") {
		t.Fatalf("must point user to `drive9 token revoke -` for revocation, got: %s", out)
	}

	// Verify the local entry is actually gone.
	if _, ok := loadConfig().Contexts["smoke"]; ok {
		t.Fatal("local smoke context still present after rm")
	}
}

func TestCtxRmRemovesDashPrefixedContextNames(t *testing.T) {
	seedThreeContexts(t)
	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "-foo", &Context{Type: PrincipalFSScoped, Server: "https://s", APIKey: "dat9_scoped", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("ctxAdd -foo: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("saveConfig: %v", err)
	}

	out, err := captureStdoutE(t, func() error {
		return Ctx([]string{"rm", "-foo"})
	})
	if err != nil {
		t.Fatalf("ctx rm -foo: %v", err)
	}
	if !strings.Contains(out, "removed local context") {
		t.Fatalf("missing success line, got: %s", out)
	}
	if _, ok := loadConfig().Contexts["-foo"]; ok {
		t.Fatal("local -foo context still present after rm")
	}
}

func TestCtxRmRemovesEscapedHelpAliasContextNames(t *testing.T) {
	for _, name := range []string{"help", "-h", "-help", "--help"} {
		t.Run(name, func(t *testing.T) {
			seedThreeContexts(t)
			cfg := loadConfig()
			if _, err := ctxAdd(cfg, name, &Context{Type: PrincipalFSScoped, Server: "https://s", APIKey: "dat9_scoped", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatalf("ctxAdd %q: %v", name, err)
			}
			if err := saveConfig(cfg); err != nil {
				t.Fatalf("saveConfig: %v", err)
			}

			out, err := captureStdoutE(t, func() error {
				return Ctx([]string{"rm", "--", name})
			})
			if err != nil {
				t.Fatalf("ctx rm %q: %v", name, err)
			}
			if !strings.Contains(out, "removed local context") {
				t.Fatalf("missing success line, got: %s", out)
			}
			if _, ok := loadConfig().Contexts[name]; ok {
				t.Fatalf("local %q context still present after rm", name)
			}
		})
	}
}

func TestCtxUseUsesEscapedHelpAliasContextNames(t *testing.T) {
	for _, name := range []string{"help", "-h", "-help", "--help"} {
		t.Run(name, func(t *testing.T) {
			seedThreeContexts(t)
			cfg := loadConfig()
			if _, err := ctxAdd(cfg, name, &Context{Type: PrincipalFSScoped, Server: "https://s", APIKey: "dat9_scoped", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatalf("ctxAdd %q: %v", name, err)
			}
			if err := saveConfig(cfg); err != nil {
				t.Fatalf("saveConfig: %v", err)
			}

			out, err := captureStdoutE(t, func() error {
				return Ctx([]string{"use", "--", name})
			})
			if err != nil {
				t.Fatalf("ctx use -- %q: %v", name, err)
			}
			if !strings.Contains(out, "switched to context "+strconv.Quote(name)) {
				t.Fatalf("missing switch confirmation, got: %s", out)
			}
			if got := loadConfig().CurrentContext; got != name {
				t.Fatalf("CurrentContext = %q, want %q", got, name)
			}
		})
	}
}

// TestCtxRmOwnerRequiresConfirmation verifies the [y/N] prompt on
// owner-context removal. A "no" answer (empty line) MUST abort the
// removal so owner key loss is never accidental.
func TestCtxRmOwnerRequiresConfirmation(t *testing.T) {
	seedThreeContexts(t)
	if err := Ctx([]string{"use", "delegated-ctx"}); err != nil {
		t.Fatalf("ctx use: %v", err)
	}

	// Simulate user pressing Enter (empty answer = refusal).
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.WriteString("\n")
	_ = w.Close()
	oldStdin := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = oldStdin })

	out, err := captureStdoutE(t, func() error {
		return Ctx([]string{"rm", "owner-ctx"})
	})
	if err != nil {
		t.Fatalf("ctx rm: %v", err)
	}
	if !strings.Contains(out, "aborted") {
		t.Fatalf("empty answer should produce 'aborted', got: %s", out)
	}
	if _, ok := loadConfig().Contexts["owner-ctx"]; !ok {
		t.Fatal("owner-ctx removed despite [y/N] refusal — safety guard failed")
	}
}

// TestCtxRmOwnerAcceptsYesConfirmation verifies the [y/N] prompt
// accepts "y" and proceeds with the removal.
func TestCtxRmOwnerAcceptsYesConfirmation(t *testing.T) {
	seedThreeContexts(t)
	if err := Ctx([]string{"use", "delegated-ctx"}); err != nil {
		t.Fatalf("ctx use: %v", err)
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.WriteString("y\n")
	_ = w.Close()
	oldStdin := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = oldStdin })

	if err := Ctx([]string{"rm", "owner-ctx"}); err != nil {
		t.Fatalf("ctx rm with 'y' confirmation: %v", err)
	}
	if _, ok := loadConfig().Contexts["owner-ctx"]; ok {
		t.Fatal("owner-ctx still present after confirmed removal")
	}
}

// TestCtxRmHelpLeadsWithSafetyWarning is the @gtm-1 msg `30bf6704`
// "scan-first" requirement: the first screen of `drive9 ctx rm --help`
// must lead with the WARNING line, NOT bury it below detail blocks.
func TestCtxRmHelpLeadsWithSafetyWarning(t *testing.T) {
	help := ctxRmUsage()
	lines := strings.Split(help, "\n")
	if len(lines) < 4 {
		t.Fatalf("usage too short: %s", help)
	}
	// Line 0: "usage: drive9 ctx rm <name>"
	// Line 1: blank
	// Lines 2-3: WARNING block (the scan-first lock)
	combinedTop := strings.Join(lines[:5], "\n")
	if !strings.Contains(combinedTop, "WARNING:") {
		t.Fatalf("WARNING line must appear in the first 5 lines (scan-first), got:\n%s", combinedTop)
	}
	if !strings.Contains(combinedTop, "Does NOT revoke") {
		t.Fatalf("first screen must explicitly say 'Does NOT revoke', got:\n%s", combinedTop)
	}
	if !strings.Contains(combinedTop, "drive9 token revoke") {
		t.Fatalf("first screen must point to `drive9 token revoke` for actual revocation, got:\n%s", combinedTop)
	}
	// English-only per @qiffang msg `405e0ae9`. Spot-check for CJK
	// codepoints anywhere in the usage block.
	for _, r := range help {
		if r >= 0x4E00 && r <= 0x9FFF {
			t.Fatalf("ctx rm usage contains CJK character %q; CLI surface must be English-only", r)
		}
	}
}

// TestTokenListEmitsDeprecationWarningAndDispatchesToCtxList covers
// the @dev-1 msg `17ccf84e` deprecation contract: `drive9 token list`
// must (a) warn on stderr that it's deprecated, (b) point users to
// the canonical `drive9 ctx list --type fs_scoped`, and (c) still
// produce equivalent output by dispatching through the new path.
func TestTokenListEmitsDeprecationWarningAndDispatchesToCtxList(t *testing.T) {
	seedThreeContexts(t)

	var (
		stdout string
		stderr string
	)
	// Capture both streams: stderr for the deprecation warning,
	// stdout for the dispatched output.
	stderr, err := captureStderrE(t, func() error {
		out, runErr := captureStdoutE(t, func() error {
			return Token([]string{"list"})
		})
		stdout = out
		return runErr
	})
	if err != nil {
		t.Fatalf("Token list: %v", err)
	}

	if !strings.Contains(stderr, "deprecated") {
		t.Fatalf("stderr must contain 'deprecated' warning, got: %s", stderr)
	}
	if !strings.Contains(stderr, "drive9 ctx list --type fs_scoped") {
		t.Fatalf("stderr must point to canonical command, got: %s", stderr)
	}
	// Stdout should match what `ctx list --type fs_scoped` produces.
	if !strings.Contains(stdout, "smoke") {
		t.Fatalf("stdout should still list smoke (dispatched), got: %s", stdout)
	}
	if strings.Contains(stdout, "owner-ctx") {
		t.Fatalf("stdout should NOT include owner-ctx (filtered), got: %s", stdout)
	}
}

// TestTokenForgetEmitsDeprecationWarningAndDispatchesToCtxRm covers
// the parallel deprecation contract for `drive9 token forget`. Must
// warn on stderr and route through `drive9 ctx rm`.
func TestTokenForgetEmitsDeprecationWarningAndDispatchesToCtxRm(t *testing.T) {
	seedThreeContexts(t)
	// Switch off owner-ctx so smoke isn't blocked by "current"
	// refusal when dispatched.
	if err := Ctx([]string{"use", "delegated-ctx"}); err != nil {
		t.Fatalf("ctx use: %v", err)
	}

	stderr, err := captureStderrE(t, func() error {
		_, runErr := captureStdoutE(t, func() error {
			return Token([]string{"forget", "smoke"})
		})
		return runErr
	})
	if err != nil {
		t.Fatalf("Token forget: %v", err)
	}

	if !strings.Contains(stderr, "deprecated") {
		t.Fatalf("stderr must warn deprecated, got: %s", stderr)
	}
	if !strings.Contains(stderr, "drive9 ctx rm") {
		t.Fatalf("stderr must point to `drive9 ctx rm`, got: %s", stderr)
	}
	if _, ok := loadConfig().Contexts["smoke"]; ok {
		t.Fatal("smoke still present after token forget dispatched to ctx rm")
	}
}

// TestTokenIssueDuplicateNameDoesNotRecommendDeprecatedForget is the
// @adversary-1 msg `b07b633a` primary-review blocker fix: when
// `drive9 token issue <name>` finds an existing local context with
// that name, the error must NOT steer the user to the deprecated
// `drive9 token forget`. It must point at the canonical
// `drive9 ctx rm <name>` cleanup path so the user keeps a single
// mental model (Plan B lock).
//
// Two code sites are affected (token.go lines 146 + 337 — both
// `TokenIssue` pre-check and `saveScopedTokenContext` write-time
// duplicate-name guard). The test exercises the pre-check site
// directly via TokenIssue.
func TestTokenIssueDuplicateNameDoesNotRecommendDeprecatedForget(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg := &Config{Contexts: map[string]*Context{}}
	if _, err := ctxAdd(cfg, "smoke", &Context{Type: PrincipalFSScoped, Server: "https://s", APIKey: "dat9_existing"}); err != nil {
		t.Fatal(err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvServer, "https://s")
	t.Setenv(EnvAPIKey, "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := TokenIssue([]string{"smoke", "--ttl", "1h", "--allow", "/scratch/smoke:read"})
	if err == nil {
		t.Fatal("expected duplicate-name error, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, "drive9 token forget") {
		t.Fatalf("duplicate-name error must NOT recommend deprecated `drive9 token forget`, got: %s", msg)
	}
	if !strings.Contains(msg, "drive9 ctx rm") {
		t.Fatalf("duplicate-name error must point at canonical `drive9 ctx rm`, got: %s", msg)
	}
	if !strings.Contains(msg, "smoke") {
		t.Fatalf("error must include the conflicting name 'smoke', got: %s", msg)
	}
}

// TestTokenUsageHidesListAndForget verifies the canonical
// `drive9 token --help` no longer advertises `list` or `forget`
// as subcommands. They remain reachable as hidden aliases but the
// help text steers users toward `ctx list` / `ctx rm`.
func TestTokenUsageHidesListAndForget(t *testing.T) {
	usage := tokenUsage()
	// `list` and `forget` must not appear as advertised subcommands.
	// (They may appear in cross-references like "To list local
	// contexts ... drive9 ctx list", which is intentional.)
	for _, line := range strings.Split(usage, "\n") {
		trimmed := strings.TrimSpace(line)
		// Match the pattern "list   ..." or "forget   ..." at the
		// start of a help row.
		if strings.HasPrefix(trimmed, "list ") || trimmed == "list" {
			t.Fatalf("token usage still lists 'list' subcommand: %s", line)
		}
		if strings.HasPrefix(trimmed, "forget ") || trimmed == "forget" {
			t.Fatalf("token usage still lists 'forget' subcommand: %s", line)
		}
	}
	// And the cross-reference to the canonical ctx commands MUST
	// be present so users can find the replacement.
	if !strings.Contains(usage, "drive9 ctx list") {
		t.Fatalf("token usage must point users to `drive9 ctx list` for listing, got:\n%s", usage)
	}
	if !strings.Contains(usage, "drive9 ctx rm") {
		t.Fatalf("token usage must point users to `drive9 ctx rm` for local removal, got:\n%s", usage)
	}
}
