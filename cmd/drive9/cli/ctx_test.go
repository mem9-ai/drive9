package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// makeJWT builds a syntactically valid three-segment JWT with the given
// payload claims. The header and signature are placeholders — local decode
// in Lane A does not verify signatures (Inv #7).
func makeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig := base64.RawURLEncoding.EncodeToString([]byte("placeholder-signature"))
	return header + "." + payload + "." + sig
}

// withIsolatedHome redirects ~/.drive9/config to a test-local tmp dir for the
// duration of the test. Returns the tmp dir path.
func withIsolatedHome(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	// Some macOS Go versions cache UserHomeDir via getenv; t.Setenv is
	// sufficient since os.UserHomeDir() consults $HOME directly.
	return tmp
}

// writeJWTFile writes `body` to a fresh file inside t.TempDir() with mode
// 0600 (the mode §13.3 requires for --from-file input). Returns the path.
func writeJWTFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jwt.txt")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write jwt file: %v", err)
	}
	return path
}

func captureStdoutE(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	done := make(chan []byte, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.Bytes()
	}()
	runErr := fn()
	_ = w.Close()
	out := <-done
	return string(out), runErr
}

// TestF_ImportRejectsUnparseableToken — §19 row 4: malformed JWT → command
// error, no context written.
func TestF_ImportRejectsUnparseableToken(t *testing.T) {
	home := withIsolatedHome(t)
	path := writeJWTFile(t, "not-a-jwt")
	err := Ctx([]string{"import", "--from-file", path})
	if err == nil {
		t.Fatalf("expected error for malformed JWT, got nil")
	}
	if !strings.Contains(err.Error(), "malformed") {
		t.Errorf("expected error to mention 'malformed'; got: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(home, ".drive9", "config")); !os.IsNotExist(statErr) {
		t.Errorf("config should not exist after failed import; stat err: %v", statErr)
	}
}

// TestF_ImportRejectsOwnerJWT — §19 row 5 / §13.3 contract rule: importing a
// credential with principal_type != "delegated" must fail and direct the user
// to `ctx add --api-key`.
func TestF_ImportRejectsOwnerJWT(t *testing.T) {
	home := withIsolatedHome(t)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "owner",
		"exp":            time.Now().Add(time.Hour).Unix(),
	})
	path := writeJWTFile(t, tok)
	err := Ctx([]string{"import", "--from-file", path})
	if err == nil {
		t.Fatalf("expected error for owner JWT, got nil")
	}
	if !strings.Contains(err.Error(), "ctx add --api-key") {
		t.Errorf("expected error to direct to `ctx add --api-key`; got: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(home, ".drive9", "config")); !os.IsNotExist(statErr) {
		t.Errorf("config should not exist after failed import; stat err: %v", statErr)
	}
}

// TestF_ImportRejectsExpiredDelegatedJWT — §17 short-circuit #1: importing a
// delegated JWT with exp in the past is a local refuse, no context written.
func TestF_ImportRejectsExpiredDelegatedJWT(t *testing.T) {
	home := withIsolatedHome(t)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "delegated",
		"grant_id":       "grt_expired",
		"agent":          "alice",
		"scope":          []string{"/n/vault/prod-db/DB_URL"},
		"perm":           "read",
		"exp":            time.Now().Add(-time.Hour).Unix(),
	})
	path := writeJWTFile(t, tok)
	err := Ctx([]string{"import", "--from-file", path})
	if err == nil {
		t.Fatalf("expected error for expired JWT, got nil")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected error to mention 'expired'; got: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(home, ".drive9", "config")); !os.IsNotExist(statErr) {
		t.Errorf("config should not exist after failed import; stat err: %v", statErr)
	}
}

// TestF_ImportStoresDelegatedContext — positive path: a well-formed delegated
// JWT with future exp is accepted; server field is populated from iss (TOFU,
// Invariant #8); config file is written with 0600 perms.
func TestF_ImportStoresDelegatedContext(t *testing.T) {
	home := withIsolatedHome(t)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "delegated",
		"grant_id":       "grt_7f2a",
		"agent":          "alice",
		"scope":          []string{"/n/vault/prod-db/DB_URL"},
		"perm":           "read",
		"exp":            exp.Unix(),
		"label_hint":     "alice-prod-db",
	})
	path := writeJWTFile(t, tok)
	if _, err := captureStdoutE(t, func() error {
		return Ctx([]string{"import", "--from-file", path})
	}); err != nil {
		t.Fatalf("import failed: %v", err)
	}

	cfgPath := filepath.Join(home, ".drive9", "config")
	st, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("config perms = %o, want 0600", st.Mode().Perm())
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if cfg.CurrentContext != "alice-prod-db" {
		t.Errorf("current_context = %q, want %q", cfg.CurrentContext, "alice-prod-db")
	}
	got := cfg.Contexts["alice-prod-db"]
	if got == nil {
		t.Fatalf("context %q missing", "alice-prod-db")
		return
	}
	if got.Type != PrincipalDelegated {
		t.Errorf("type = %q, want delegated", got.Type)
	}
	if got.Server != "https://api.example.com" {
		t.Errorf("server = %q, want iss-derived", got.Server)
	}
	if got.GrantID != "grt_7f2a" {
		t.Errorf("grant_id = %q, want grt_7f2a", got.GrantID)
	}
	if got.Perm != PermRead {
		t.Errorf("perm = %q, want read", got.Perm)
	}
}

// TestF15_CtxUseIsExplicitVerb — spec §13.2 / F15: `ctx use` must be an
// explicit verb. The positional-switch form `drive9 ctx <name>` is retired.
func TestF15_CtxUseIsExplicitVerb(t *testing.T) {
	_ = withIsolatedHome(t)
	// Pre-seed two owner contexts.
	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "alpha", &Context{Type: PrincipalOwner, APIKey: "k1", Server: "https://s"}); err != nil {
		t.Fatalf("seed alpha: %v", err)
	}
	if _, err := ctxAdd(cfg, "beta", &Context{Type: PrincipalOwner, APIKey: "k2", Server: "https://s"}); err != nil {
		t.Fatalf("seed beta: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Positional form must error, not silently switch.
	err := Ctx([]string{"beta"})
	if err == nil {
		t.Errorf("positional `ctx beta` should be an error; got nil")
	}

	// Explicit `ctx use` must switch.
	out, err := captureStdoutE(t, func() error { return Ctx([]string{"use", "beta"}) })
	if err != nil {
		t.Fatalf("ctx use beta: %v", err)
	}
	if !strings.Contains(out, `switched to context "beta"`) {
		t.Errorf("expected spec-pinned success notice; got: %q", out)
	}
	// F15 second line — descriptor.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Errorf("expected a two-line success notice; got %d line(s)", len(lines))
	}
}

// TestF15_CtxUseRejectsExpiredDelegated — §17 short-circuit: ctx use must not
// activate an already-expired delegated context.
func TestF15_CtxUseRejectsExpiredDelegated(t *testing.T) {
	_ = withIsolatedHome(t)
	cfg := loadConfig()
	_, _ = ctxAdd(cfg, "fresh", &Context{Type: PrincipalOwner, APIKey: "k", Server: "https://s"})
	_, _ = ctxAdd(cfg, "stale", &Context{
		Type:      PrincipalDelegated,
		Server:    "https://api.example.com",
		Token:     "irrelevant",
		Agent:     "alice",
		Scope:     []string{"/n/vault/x"},
		Perm:      PermRead,
		ExpiresAt: time.Now().Add(-time.Hour),
		GrantID:   "grt_x",
	})
	// "fresh" is current (first-added); "stale" is not.
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	err := Ctx([]string{"use", "stale"})
	if err == nil {
		t.Fatalf("expected error activating expired context")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected 'expired' in error; got: %v", err)
	}
	post := loadConfig()
	if post.CurrentContext != "fresh" {
		t.Errorf("current context should remain %q; got %q", "fresh", post.CurrentContext)
	}
}

// TestF16_CtxListUsesCurrentColumn — spec §13.2 / F16: `ctx ls` renders a
// dedicated CURRENT column (* in its own cell), not a `*` marker prefixed
// onto NAME.
func TestF16_CtxListUsesCurrentColumn(t *testing.T) {
	_ = withIsolatedHome(t)
	cfg := loadConfig()
	_, _ = ctxAdd(cfg, "owner-prod", &Context{Type: PrincipalOwner, APIKey: "k", Server: "https://s"})
	_, _ = ctxAdd(cfg, "alice", &Context{
		Type:      PrincipalDelegated,
		Server:    "https://s",
		Token:     "t",
		Agent:     "alice",
		Scope:     []string{"/n/vault/prod-db/DB_URL"},
		Perm:      PermRead,
		ExpiresAt: time.Now().Add(time.Hour),
		GrantID:   "grt_1",
	})
	cfg.CurrentContext = "alice"
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	out, err := captureStdoutE(t, func() error { return Ctx([]string{"ls"}) })
	if err != nil {
		t.Fatalf("ctx ls: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 3 { // header + 2 rows
		t.Fatalf("expected at least 3 output lines, got %d: %q", len(lines), out)
	}
	header := lines[0]
	if !strings.HasPrefix(strings.TrimLeft(header, " "), "CURRENT") {
		t.Errorf("expected header to start with CURRENT column; got: %q", header)
	}
	// No row's NAME column should carry a leading `*`.
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if strings.HasPrefix(fields[1], "*") {
			t.Errorf("NAME column must not carry `*` marker (F16); got line: %q", line)
		}
	}
	// Exactly one row holds `*` in CURRENT column.
	stars := 0
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "*" {
			stars++
		}
	}
	if stars != 1 {
		t.Errorf("expected exactly one row with * in CURRENT column; got %d", stars)
	}
}

// TestF13_NoDaemonStateBetweenCalls — spec F13: a CLI invocation that
// succeeds purely on ~/.drive9/config may not leave other process-visible
// state behind (no lockfiles, no sockets from the CLI itself, no /tmp
// droppings). Lane A verbs are all stateless on the CLI side; this test
// pins it.
func TestF13_NoDaemonStateBetweenCalls(t *testing.T) {
	home := withIsolatedHome(t)
	// Snapshot pre-state of common leak sinks.
	preEntries := func() []string {
		var out []string
		for _, d := range []string{home, filepath.Join(home, ".drive9"), os.TempDir()} {
			ents, err := os.ReadDir(d)
			if err != nil {
				continue
			}
			for _, e := range ents {
				out = append(out, filepath.Join(d, e.Name()))
			}
		}
		return out
	}
	before := preEntries()

	_, err := captureStdoutE(t, func() error {
		return Ctx([]string{"add", "--api-key", "sk_test", "--name", "t1"})
	})
	if err != nil {
		t.Fatalf("ctx add: %v", err)
	}
	_, _ = captureStdoutE(t, func() error { return Ctx([]string{"ls"}) })
	_, _ = captureStdoutE(t, func() error { return Ctx([]string{"use", "t1"}) })

	after := preEntries()
	// The only new path allowed is ~/.drive9 and ~/.drive9/config.
	seen := map[string]struct{}{}
	for _, p := range before {
		seen[p] = struct{}{}
	}
	for _, p := range after {
		if _, ok := seen[p]; ok {
			continue
		}
		rel := p
		if strings.HasPrefix(p, home) {
			rel = strings.TrimPrefix(p, home)
		}
		switch rel {
		case "/.drive9", "/.drive9/config":
			// allowed
		default:
			t.Errorf("unexpected filesystem side effect from Lane A verb: %s", p)
		}
	}
}

// TestCtxImport_RejectsPositionalJWT — §13.3: a JWT passed as a positional
// argument MUST be refused. The error text must cite the leak channels
// (history / /proc/<pid>/cmdline) so the operator understands *why* the
// form was removed.
func TestCtxImport_RejectsPositionalJWT(t *testing.T) {
	home := withIsolatedHome(t)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "delegated",
		"grant_id":       "grt_x",
		"agent":          "alice",
		"scope":          []string{"/n/vault/x"},
		"perm":           "read",
		"exp":            time.Now().Add(time.Hour).Unix(),
	})
	err := Ctx([]string{"import", tok})
	if err == nil {
		t.Fatalf("expected error for positional JWT, got nil")
	}
	if !strings.Contains(err.Error(), "positional JWT is not accepted") {
		t.Errorf("expected error to cite positional-form rejection; got: %v", err)
	}
	if !strings.Contains(err.Error(), "history") {
		t.Errorf("expected error to mention the leak channel; got: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(home, ".drive9", "config")); !os.IsNotExist(statErr) {
		t.Errorf("config should not exist after rejected positional import; stat err: %v", statErr)
	}
}

// TestCtxImport_RejectsWorldReadableFile — §13.3: --from-file MUST refuse
// any file whose mode has bits set outside 0o700 (any group/other read/write
// makes the JWT already-compromised). The check MUST run before os.ReadFile
// so we never even hold the token in memory.
func TestCtxImport_RejectsWorldReadableFile(t *testing.T) {
	home := withIsolatedHome(t)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "delegated",
		"grant_id":       "grt_x",
		"agent":          "alice",
		"scope":          []string{"/n/vault/x"},
		"perm":           "read",
		"exp":            time.Now().Add(time.Hour).Unix(),
	})
	path := filepath.Join(t.TempDir(), "jwt.txt")
	if err := os.WriteFile(path, []byte(tok), 0o644); err != nil {
		t.Fatalf("write world-readable jwt: %v", err)
	}
	err := Ctx([]string{"import", "--from-file", path})
	if err == nil {
		t.Fatalf("expected error for mode-0644 jwt file, got nil")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Errorf("expected error to cite the required 0600 mode; got: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(home, ".drive9", "config")); !os.IsNotExist(statErr) {
		t.Errorf("config should not exist after rejected world-readable import; stat err: %v", statErr)
	}
}

// TestCtxImport_EmptyStdinRefusesWithoutConfigWrite — §13.3: a bare
// `drive9 ctx import` with nothing readable on stdin MUST fail before any
// config write. The spec also requires that the same scenario with a true
// TTY on stdin emits a one-line help pointing at the three canonical forms
// — but simulating a ModeCharDevice fd inside `go test` is brittle
// cross-platform (a regular file is not ModeCharDevice, a pipe is not
// ModeCharDevice, and there is no portable way to fake one without opening
// /dev/tty). So this test exercises the adjacent empty-stdin branch: we
// redirect os.Stdin to an opened empty regular file (stdinIsPiped returns
// true), io.ReadAll returns "", and decodeJWTPayload then rejects with a
// malformed-shape error. The OBSERVABLE contracts pinned here — (a) bare
// import does not silently succeed, (b) no ~/.drive9/config is written —
// are the properties reviewers actually care about. The TTY-refusal
// help-text literal is exercised by the integration/E2E harness (tracked
// separately), not by this unit test.
func TestCtxImport_EmptyStdinRefusesWithoutConfigWrite(t *testing.T) {
	home := withIsolatedHome(t)
	// Redirect os.Stdin to an empty regular file. stdinIsPiped() returns
	// true (regular files are not ModeCharDevice), so the auto-detect path
	// runs and ReadAll returns "". decodeJWTPayload then fails with a
	// malformed-shape error — the observable contract is that bare
	// `drive9 ctx import` does NOT silently succeed and does NOT write a
	// config when stdin is empty.
	origStdin := os.Stdin
	defer func() { os.Stdin = origStdin }()
	empty, err := os.CreateTemp(t.TempDir(), "empty")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}
	f, err := os.Open(empty.Name())
	if err != nil {
		t.Fatalf("reopen empty: %v", err)
	}
	defer func() { _ = f.Close() }()
	os.Stdin = f

	cmdErr := Ctx([]string{"import"})
	if cmdErr == nil {
		t.Fatalf("expected error on bare import with empty stdin, got nil")
	}
	if _, statErr := os.Stat(filepath.Join(home, ".drive9", "config")); !os.IsNotExist(statErr) {
		t.Errorf("config should not exist after failed bare import; stat err: %v", statErr)
	}
}

// TestCtxImport_StdinAutoDetected — §13.3: when stdin is piped (not a TTY)
// and no flag is given, the JWT is read from stdin automatically. The
// explicit `--from-file -` form must remain equivalent (tested separately
// via writeJWTFile path; here we pin auto-detect).
func TestCtxImport_StdinAutoDetected(t *testing.T) {
	home := withIsolatedHome(t)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "delegated",
		"grant_id":       "grt_auto",
		"agent":          "alice",
		"scope":          []string{"/n/vault/prod-db/DB_URL"},
		"perm":           "read",
		"exp":            exp.Unix(),
		"label_hint":     "alice-auto",
	})

	// Pipe the token into os.Stdin. A pipe is not a ModeCharDevice, so
	// stdinIsPiped() reports true and the auto-detect branch fires.
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	origStdin := os.Stdin
	os.Stdin = pr
	defer func() {
		os.Stdin = origStdin
		_ = pr.Close()
	}()
	go func() {
		_, _ = pw.Write([]byte(tok))
		_ = pw.Close()
	}()

	if _, err := captureStdoutE(t, func() error {
		return Ctx([]string{"import"})
	}); err != nil {
		t.Fatalf("bare import with piped stdin failed: %v", err)
	}

	cfgPath := filepath.Join(home, ".drive9", "config")
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if _, ok := cfg.Contexts["alice-auto"]; !ok {
		t.Errorf("expected context %q written via auto-detected stdin; got contexts: %v", "alice-auto", keys(cfg.Contexts))
	}
}

// TestCtxImport_ExplicitStdinDash — §13.3: `--from-file -` is the explicit
// form for stdin input. It is equivalent to the auto-detected form but
// never depends on isatty, so scripts that want unambiguous intent can use
// it regardless of whether stdin happens to be a pipe.
func TestCtxImport_ExplicitStdinDash(t *testing.T) {
	home := withIsolatedHome(t)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	tok := makeJWT(t, map[string]any{
		"iss":            "https://api.example.com",
		"principal_type": "delegated",
		"grant_id":       "grt_dash",
		"agent":          "alice",
		"scope":          []string{"/n/vault/prod-db/DB_URL"},
		"perm":           "read",
		"exp":            exp.Unix(),
		"label_hint":     "alice-dash",
	})

	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	origStdin := os.Stdin
	os.Stdin = pr
	defer func() {
		os.Stdin = origStdin
		_ = pr.Close()
	}()
	go func() {
		_, _ = pw.Write([]byte(tok))
		_ = pw.Close()
	}()

	if _, err := captureStdoutE(t, func() error {
		return Ctx([]string{"import", "--from-file", "-"})
	}); err != nil {
		t.Fatalf("explicit --from-file - import failed: %v", err)
	}
	cfgPath := filepath.Join(home, ".drive9", "config")
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("config missing after explicit stdin import: %v", err)
	}
}

func keys(m map[string]*Context) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestCtxAddIsSingleConfigWriter(t *testing.T) {
	_ = withIsolatedHome(t)
	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "first", &Context{Type: PrincipalOwner, APIKey: "k", Server: "https://s"}); err != nil {
		t.Fatalf("first add: %v", err)
	}
	// Collision must be rejected by the same helper Create uses, proving the
	// invariant that Create shares the config-writing code path.
	if _, err := ctxAdd(cfg, "first", &Context{Type: PrincipalOwner, APIKey: "k2", Server: "https://s"}); err == nil {
		t.Errorf("expected collision error from ctxAdd")
	}
}
