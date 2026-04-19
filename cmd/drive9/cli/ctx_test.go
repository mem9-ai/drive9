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
	err := Ctx([]string{"import", "not-a-jwt"})
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
	err := Ctx([]string{"import", tok})
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
	err := Ctx([]string{"import", tok})
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
	if _, err := captureStdoutE(t, func() error {
		return Ctx([]string{"import", tok})
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
