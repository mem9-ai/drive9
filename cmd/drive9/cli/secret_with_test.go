package cli

import (
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
)

// This file holds the V2c `drive9 vault with` gate-floor test matrix:
//
//	G-V2c-1   path shape: only `/n/vault/<secret>` accepted; bare/subpath rejected
//	G-V2c-2   F14 scrub drops exactly {API_KEY, VAULT_TOKEN, SERVER}
//	G-V2c-3   @env key charset is strict [A-Z_][A-Z0-9_]*
//	G-V2c-4   @env value forbids control bytes 0x00-0x1f except \t
//	G-V2c-5   empty secret (0 visible keys) → child still runs, 0 injected vars
//	G-V2c-7   child exit/signal pass-through (covered indirectly via *exec.ExitError
//	          surfacing to main.go fatal()'s exitCoder branch)
//	G-V2c-8   any illegal key → whole EACCES, no partial inject
//	G-V2c-9   legal+illegal mix → whole EACCES, no partial inject
//
// G-V2c-6 (`drive9 vault exec` falls to generic unknown-command) and
// G-V2c-10 (SecretExec is undefined) live in TestSecretDispatchRemovesExec
// and at compile time respectively — see notes on that test.

// G-V2c-1: path shape enforcement. The CLI accepts EXACTLY `/n/vault/<secret>`.
// Bare names, subpaths, and empty names are rejected loudly with a message
// that names the required prefix (so operators migrating from `vault exec`
// learn the new shape from the first failure, without us adding a bespoke
// rename hint in the dispatch layer — the error is a path-validation
// error, not an alias lookup failure).
func TestSecretWithPathShapeEnforcement(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://example.invalid")
	t.Setenv(EnvVaultToken, "cap-token")

	cases := []struct {
		name    string
		path    string
		wantSub string
	}{
		{"bare name rejected", "aws-prod", "must start with /n/vault/"},
		{"wrong prefix rejected", "/mnt/vault/aws-prod", "must start with /n/vault/"},
		{"empty name rejected", "/n/vault/", "missing a secret name"},
		{"subpath rejected", "/n/vault/aws-prod/access_key", "subpath selection is not supported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCredentialCacheForTest()
			t.Cleanup(resetCredentialCacheForTest)
			err := SecretWith([]string{tc.path, "--", "/bin/true"})
			if err == nil {
				t.Fatalf("expected error for path %q, got nil", tc.path)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantSub)
			}
		})
	}
}

// G-V2c-2: the F14 scrub MUST drop exactly DRIVE9_API_KEY, DRIVE9_VAULT_TOKEN,
// and DRIVE9_SERVER from the child's environment, and MUST preserve every
// other variable — including unrelated DRIVE9_* knobs like profiling or log
// level. This pins the scrub as a whitelist-of-removals, not a blanket
// "strip all DRIVE9_*" which would break profiling and debug pipelines.
//
// Note on credential resolver behavior: ResolveCredentials consumes (reads
// and os.Unsetenv's) DRIVE9_VAULT_TOKEN / DRIVE9_API_KEY / DRIVE9_SERVER on
// first call. So by the time the child forks, those three are already
// gone from os.Environ() regardless of the scrub. The scrub's job is to
// hold the line if that resolver behavior ever changes, or if a future
// call path skips it — hence we assert the scrub on os.Environ() directly,
// separately from the end-to-end SecretWith test.
func TestScrubDrive9CredEnv(t *testing.T) {
	base := []string{
		"PATH=/usr/bin",
		"DRIVE9_API_KEY=owner-key",
		"DRIVE9_VAULT_TOKEN=cap-token",
		"DRIVE9_SERVER=https://api.example",
		"DRIVE9_PROF_CPU_PROFILE=/tmp/cpu.pprof", // preserved (profiling knob)
		"DRIVE9_CLI_LOG_LEVEL=debug",             // preserved (log knob)
		"HOME=/home/drive9",
	}
	got := scrubDrive9CredEnv(base)

	mustHave := []string{
		"PATH=/usr/bin",
		"DRIVE9_PROF_CPU_PROFILE=/tmp/cpu.pprof",
		"DRIVE9_CLI_LOG_LEVEL=debug",
		"HOME=/home/drive9",
	}
	mustNotHave := []string{
		"DRIVE9_API_KEY=owner-key",
		"DRIVE9_VAULT_TOKEN=cap-token",
		"DRIVE9_SERVER=https://api.example",
	}
	for _, want := range mustHave {
		if !containsEntry(got, want) {
			t.Fatalf("scrubbed env missing %q: %v", want, got)
		}
	}
	for _, banned := range mustNotHave {
		if containsEntry(got, banned) {
			t.Fatalf("scrubbed env still contains %q: %v", banned, got)
		}
	}

	// Scrub is also unconditional: if the three vars are absent, the
	// scrub MUST still not blow up and MUST still preserve everything
	// else. This covers the §9 L209 "even when ... unset, absent" clause.
	baseNoCreds := []string{"PATH=/usr/bin", "HOME=/home/drive9"}
	gotNoCreds := scrubDrive9CredEnv(baseNoCreds)
	if len(gotNoCreds) != len(baseNoCreds) {
		t.Fatalf("scrub changed env when no credentials were present: got %v, want %v", gotNoCreds, baseNoCreds)
	}
}

func containsEntry(env []string, want string) bool {
	for _, e := range env {
		if e == want {
			return true
		}
	}
	return false
}

// G-V2c-3: the @env key charset is the strict spec charset. The parser
// REJECTS lowercase, hyphens, digit-prefix keys, and Unicode. It does NOT
// normalize — that is the whole point of separating this from the legacy
// buildSecretEnvMap path. (R-V2c-3 enforces the isolation.)
func TestIsValidVaultEnvKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"ACCESS_KEY", true},
		{"_LEADING_UNDERSCORE", true},
		{"WITH9DIGIT", true},
		{"A", true},
		{"_", true},

		{"", false},               // empty
		{"access_key", false},     // lowercase
		{"AccessKey", false},      // mixed case
		{"ACCESS-KEY", false},     // hyphen
		{"9DIGIT_FIRST", false},   // leading digit
		{"ACCESS.KEY", false},     // dot
		{"ACCESS KEY", false},     // space
		{"ACCESS_KEY\x00", false}, // embedded NUL
		{"ÄCCESS", false},         // non-ASCII
	}
	for _, tc := range cases {
		if got := isValidVaultEnvKey(tc.key); got != tc.want {
			t.Fatalf("isValidVaultEnvKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

// G-V2c-4: value-side control byte enforcement. 0x00-0x1f is banned EXCEPT
// 0x09 (tab). 0x20 (space) and above pass. DEL (0x7f) passes — the spec
// only outlaws the C0 control range.
func TestIndexOfForbiddenEnvByte(t *testing.T) {
	cases := []struct {
		name string
		v    string
		want int
	}{
		{"plain ascii", "hello world", -1},
		{"tab allowed", "col1\tcol2", -1},
		{"high bit allowed", "café", -1},
		{"del allowed", "x\x7fy", -1},
		{"space allowed", " leading", -1},

		{"embedded NUL", "bad\x00value", 3},
		{"embedded LF", "multi\nline", 5},
		{"embedded CR", "cr\rhere", 2},
		{"leading control", "\x01rest", 0},
		{"bell", "ring\x07bell", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := indexOfForbiddenEnvByte(tc.v); got != tc.want {
				t.Fatalf("indexOfForbiddenEnvByte(%q) = %d, want %d", tc.v, got, tc.want)
			}
		})
	}
}

// G-V2c-5: empty ≠ missing. A secret that exists but has zero visible keys
// MUST fork the child with zero injected vars (spec §4.1.2 L96). The read
// path returns 200 with `{}`. The child should run normally and see no
// DRIVE9_* credential from the parent (F14 scrub).
func TestSecretWithEmptySecretStillForksChild(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/empty-prod" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	out := captureStdout(t, func() {
		// The child writes `ran-with-no-injection` to stdout unconditionally.
		// If the CLI had refused to fork on empty, SecretWith would return
		// an error and stdout would stay empty.
		if err := SecretWith([]string{"/n/vault/empty-prod", "--", "/bin/sh", "-c", "printf ran-with-no-injection"}); err != nil {
			t.Fatalf("SecretWith(empty): %v", err)
		}
	})
	if out != "ran-with-no-injection" {
		t.Fatalf("child output = %q, want %q", out, "ran-with-no-injection")
	}
}

// G-V2c-7: when the child exits non-zero, SecretWith returns an error that
// carries the child's exit code via the exitCoder interface that main.go's
// fatal() already understands (see cmd/drive9/main.go exitCoder branch).
// We assert the error is an *exec.ExitError and that ExitCode() matches.
func TestSecretWithPropagatesChildExitCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := SecretWith([]string{"/n/vault/anything", "--", "/bin/sh", "-c", "exit 42"})
	if err == nil {
		t.Fatal("expected error from child exit 42, got nil")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("error type = %T, want *exec.ExitError", err)
	}
	if code := exitErr.ExitCode(); code != 42 {
		t.Fatalf("child exit code = %d, want 42", code)
	}
}

// G-V2c-8: a single illegal key fails the WHOLE command with an EACCES
// message. The error mentions the offending key so operators can fix the
// secret. No partial injection — validateVaultEnvFields never returns a
// mixed map. We assert via SecretWith end-to-end: if the child had been
// forked, the command would have succeeded (/bin/true returns 0).
func TestSecretWithIllegalKeyFailsWhole(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/bad-keys" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access-key":"AKIA"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	err := SecretWith([]string{"/n/vault/bad-keys", "--", "/bin/true"})
	if err == nil {
		t.Fatal("expected EACCES-shaped error for illegal key, got nil")
	}
	if !strings.Contains(err.Error(), `"access-key"`) {
		t.Fatalf("error should name offending key: %q", err)
	}
	if !strings.Contains(err.Error(), "EACCES") {
		t.Fatalf("error should mention EACCES: %q", err)
	}
}

// G-V2c-9: a mix of legal and illegal keys fails the WHOLE command — the
// legal key MUST NOT leak into the child. We verify this by running the
// child as a script that would print the legal key's value if injected;
// any non-empty stdout would mean partial injection happened.
func TestSecretWithLegalPlusIllegalFailsWhole(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/mixed" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// ACCESS_KEY is legal; illegal-key has a hyphen.
		_, _ = w.Write([]byte(`{"ACCESS_KEY":"AKIA","illegal-key":"x"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	out := captureStdout(t, func() {
		err := SecretWith([]string{"/n/vault/mixed", "--", "/bin/sh", "-c", `printf 'LEAK:%s' "$ACCESS_KEY"`})
		if err == nil {
			t.Fatal("expected error for legal+illegal mix, got nil")
		}
	})
	if out != "" {
		t.Fatalf("partial injection detected: child stdout = %q; expected no child to have run", out)
	}
}

// G-V2c-4 end-to-end: a control byte in a value rejects the whole command
// with the same EACCES framing. This is the value-side twin of the
// illegal-key test and keeps the two branches of §4.1.3 (L101 key rule +
// L103 value rule) covered from user-visible behavior, not just from unit
// tests on the helpers.
func TestSecretWithControlByteValueFailsWhole(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/has-nl" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// Value contains a literal LF (0x0a). JSON-escaped as `\n`.
		_, _ = w.Write([]byte(`{"MULTILINE":"line1\nline2"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	err := SecretWith([]string{"/n/vault/has-nl", "--", "/bin/true"})
	if err == nil {
		t.Fatal("expected EACCES-shaped error for control byte in value, got nil")
	}
	if !strings.Contains(err.Error(), "MULTILINE") {
		t.Fatalf("error should name offending key: %q", err)
	}
	if !strings.Contains(err.Error(), "EACCES") {
		t.Fatalf("error should mention EACCES: %q", err)
	}
}

// G-V2c-6: `drive9 vault exec` MUST fall into the `unknown vault command`
// branch with no bespoke rename hint. Framing must be identical to any
// other typo. This pins the hard-cut policy: the old verb is simply not
// part of the vault sub-dispatcher anymore.
func TestSecretDispatchRemovesExec(t *testing.T) {
	execErr := Secret([]string{"exec", "aws-prod", "--", "/bin/true"})
	if execErr == nil {
		t.Fatal("expected error for `vault exec` after V2c hard-cut, got nil")
	}
	if !strings.Contains(execErr.Error(), `unknown vault command "exec"`) {
		t.Fatalf("expected generic unknown-command framing, got: %q", execErr)
	}

	// And the framing must be identical to any other typo — no bespoke
	// rename hint for `exec`. If someone smuggled a "use `with` instead"
	// hint back in, the two errors would diverge.
	typoErr := Secret([]string{"xyz-typo"})
	normalized := strings.Replace(execErr.Error(), `"exec"`, `"xyz-typo"`, 1)
	if normalized != typoErr.Error() {
		t.Fatalf("`exec` path diverges from generic unknown-vault-command path.\n  exec (normalized): %q\n  xyz-typo         : %q", normalized, typoErr)
	}

	// Same policy for banned substrings: no alias, no rename, no "use `with`".
	lowered := strings.ToLower(execErr.Error())
	for _, banned := range []string{"rename", "renamed", "alias", "legacy", "deprecated", "use `with`", "use with", "replaced"} {
		if strings.Contains(lowered, banned) {
			t.Fatalf("`vault exec` error contains bespoke hint %q: %q", banned, execErr)
		}
	}
}

// G-V2c-10 is a compile-time contract: the symbol `SecretExec` MUST NOT
// exist in package cli. The compiler is the assertion. If SecretExec came
// back, `vault exec` would start routing again and G-V2c-6 would start
// failing — but that's downstream. The durable artifact for this gate is
// the git diff on this PR, which deletes the function outright.
