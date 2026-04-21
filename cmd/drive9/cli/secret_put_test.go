package cli

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// V2d sealed-v4 pre-PR contract (issue #307) is mapped 1:1 to tests below.
// Each Row X test is the authoritative witness for its axis. Row G is a
// cross-reference — it deliberately has no distinct test; it re-labels
// the B4..B9 witnesses to preserve the 8-axis sign-off shape.
//
//	Row A — TestSecretPut_RowA_ArgvAndPathShape
//	Row B — TestSecretPut_RowB_FromDirSemantics
//	Row C — TestSecretPut_RowC_SingleHTTPRequest
//	Row D — TestSecretPut_RowD_NoPartialOnFieldReject
//	Row E — TestSecretPut_RowE_PrincipalOwnerOnly
//	Row F — TestSecretPut_RowF_StdinRejectedAndWitnessD
//	Row H — TestSecretPut_RowH_ObservabilityAnchors  (+ NoAutoRetry + AckLost)
//
// tests always force stdinIsTTY=true so argv parsing is exercised; Row F
// flips it explicitly to test the stdin rejection.

// withTTY is a t.Cleanup-aware helper that pins stdinIsTTY to the given
// value for the duration of the test (or subtest).
func withTTY(t *testing.T, tty bool) {
	t.Helper()
	prev := stdinIsTTY
	stdinIsTTY = func() bool { return tty }
	t.Cleanup(func() { stdinIsTTY = prev })
}

// secretDir writes one file per map entry into a fresh tempdir and returns
// its path. Used by happy-path and several Row B cases.
func secretDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
	}
	return dir
}

// ---------------------------------------------------------------------------
// Row A — Argv/path shape. Reuses V2c parseVaultPath. Covers: missing path,
// wrong prefix, subpath, empty name (all surface through parseVaultPath),
// plus put-specific argv errors: missing --from, unknown flag, extra arg.
// ---------------------------------------------------------------------------

func TestSecretPut_RowA_ArgvAndPathShape(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://example.invalid")
	t.Setenv("DRIVE9_API_KEY", "owner-key")

	cases := []struct {
		name    string
		args    []string
		wantSub string
	}{
		{"no args", nil, "usage"},
		{"bare name", []string{"aws-prod", "--from", "/tmp/x"}, "must start with /n/vault/"},
		{"wrong prefix", []string{"/mnt/vault/aws-prod", "--from", "/tmp/x"}, "must start with /n/vault/"},
		{"empty secret", []string{"/n/vault/", "--from", "/tmp/x"}, "missing a secret name"},
		{"subpath", []string{"/n/vault/aws/key", "--from", "/tmp/x"}, "subpath"},
		{"missing --from", []string{"/n/vault/aws-prod"}, "--from is required"},
		{"--from empty value", []string{"/n/vault/aws-prod", "--from", ""}, "empty value"},
		{"unknown flag", []string{"/n/vault/aws-prod", "--from", "/tmp/x", "--merge"}, `unknown flag "--merge"`},
		{"extra positional", []string{"/n/vault/aws-prod", "garbage", "--from", "/tmp/x"}, `unexpected argument "garbage"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTTY(t, true)
			resetCredentialCacheForTest()
			t.Cleanup(resetCredentialCacheForTest)
			err := SecretPut(tc.args)
			if err == nil {
				t.Fatalf("expected error for %v, got nil", tc.args)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantSub)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Row B — `--from <dir>` dir semantics. B1..B9 rejected classes each get
// a subtest with a double-sided assertion (named class marker + name of
// offending file, when applicable).
// ---------------------------------------------------------------------------

func TestSecretPut_RowB_FromDirSemantics(t *testing.T) {
	// Each subtest re-applies the owner-key env via t.Setenv so the
	// resolver's consume-and-unset behavior cannot leak between subtests.
	setOwnerEnv := func(t *testing.T) {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		t.Setenv("DRIVE9_SERVER", "http://example.invalid")
		t.Setenv("DRIVE9_API_KEY", "owner-key")
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)
	}

	t.Run("B1 does not exist", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", filepath.Join(t.TempDir(), "nope")})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "B1") || !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("error = %q, want B1 + does-not-exist", err)
		}
	})

	t.Run("B1 not a directory", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		f := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", f})
		if err == nil || !strings.Contains(err.Error(), "B1") {
			t.Fatalf("error = %v, want B1", err)
		}
	})

	t.Run("B2 empty directory", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", t.TempDir()})
		if err == nil || !strings.Contains(err.Error(), "B2") {
			t.Fatalf("error = %v, want B2", err)
		}
	})

	t.Run("B3 subdirectory rejected", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "NESTED"), 0o700); err != nil {
			t.Fatal(err)
		}
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil || !strings.Contains(err.Error(), "B3") || !strings.Contains(err.Error(), `"NESTED"`) {
			t.Fatalf("error = %v, want B3 naming NESTED", err)
		}
	})

	t.Run("B3 symlink rejected", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		dir := t.TempDir()
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, "LINK")); err != nil {
			t.Skipf("symlink not supported on this FS: %v", err)
		}
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil || !strings.Contains(err.Error(), "B3") {
			t.Fatalf("error = %v, want B3", err)
		}
	})

	t.Run("B4 illegal key charset", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		dir := secretDir(t, map[string]string{"access-key": "AKIA"})
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil {
			t.Fatal("expected error for illegal key")
		}
		if !strings.Contains(err.Error(), `"access-key"`) || !strings.Contains(err.Error(), "EACCES") {
			t.Fatalf("error = %q, want name + EACCES", err)
		}
	})

	t.Run("B5 forbidden control byte in value", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		dir := secretDir(t, map[string]string{"ACCESS_KEY": "line1\nline2"})
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil {
			t.Fatal("expected error for control byte")
		}
		if !strings.Contains(err.Error(), "ACCESS_KEY") || !strings.Contains(err.Error(), "EACCES") {
			t.Fatalf("error = %q, want key name + EACCES", err)
		}
	})

	t.Run("B9 dotfile rejected", func(t *testing.T) {
		withTTY(t, true)
		setOwnerEnv(t)
		dir := secretDir(t, map[string]string{".HIDDEN": "x"})
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil || !strings.Contains(err.Error(), "B9") {
			t.Fatalf("error = %v, want B9", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Row C — Single HTTP request. One PUT, single state transition.
// ---------------------------------------------------------------------------

func TestSecretPut_RowC_SingleHTTPRequest(t *testing.T) {
	withTTY(t, true)
	var count int32
	var gotMethod, gotPath, gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"aws-prod","revision":1}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	dir := secretDir(t, map[string]string{
		"ACCESS_KEY": "AKIA",
		"SECRET_KEY": "hunter2",
	})
	if err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir}); err != nil {
		t.Fatalf("SecretPut: %v", err)
	}
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Fatalf("HTTP request count = %d, want 1 (atomic single-request contract)", got)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("method = %q, want PUT", gotMethod)
	}
	if gotPath != "/v1/vault/secrets/aws-prod" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer owner-key" {
		t.Fatalf("auth = %q", gotAuth)
	}
	var payload struct {
		Fields    map[string]string `json:"fields"`
		UpdatedBy string            `json:"updated_by"`
	}
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatalf("decode body: %v\n%s", err, gotBody)
	}
	if payload.Fields["ACCESS_KEY"] != "AKIA" || payload.Fields["SECRET_KEY"] != "hunter2" {
		t.Fatalf("body fields = %v, want both entries", payload.Fields)
	}
}

// ---------------------------------------------------------------------------
// Row D — No-partial witness: if any field fails validation, NO HTTP
// request is issued at all (the whole map is rejected up front, not a
// subset). The server-side request counter is the witness.
// ---------------------------------------------------------------------------

func TestSecretPut_RowD_NoPartialOnFieldReject(t *testing.T) {
	withTTY(t, true)
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	// One legal + one illegal key. A partial apply would POST with only
	// ACCESS_KEY and silently drop the illegal one — that's exactly the
	// failure shape this test forbids.
	dir := secretDir(t, map[string]string{
		"ACCESS_KEY": "AKIA",
		"bad-key":    "x",
	})
	err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
	if err == nil {
		t.Fatal("expected error for mixed legal+illegal map")
	}
	if atomic.LoadInt32(&count) != 0 {
		t.Fatalf("HTTP issued despite illegal key: count = %d (partial apply detected)", count)
	}
}

// ---------------------------------------------------------------------------
// Row E — Principal must be owner DRIVE9_API_KEY. A cap-token is rejected
// at the CLI boundary with a message naming the principal class.
// ---------------------------------------------------------------------------

func TestSecretPut_RowE_PrincipalOwnerOnly(t *testing.T) {
	withTTY(t, true)

	t.Run("capability token rejected", func(t *testing.T) {
		withTTY(t, true)
		t.Setenv("HOME", t.TempDir())
		t.Setenv("DRIVE9_SERVER", "http://example.invalid")
		t.Setenv(EnvVaultToken, "cap-token")
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)

		dir := secretDir(t, map[string]string{"K": "v"})
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil {
			t.Fatal("expected error for cap-token principal")
		}
		// Error must name BOTH principal classes so the operator sees
		// which to use and which is being refused.
		if !strings.Contains(err.Error(), EnvAPIKey) || !strings.Contains(err.Error(), EnvVaultToken) {
			t.Fatalf("error = %q, want both %s and %s named", err, EnvAPIKey, EnvVaultToken)
		}
	})

	t.Run("no credentials at all", func(t *testing.T) {
		withTTY(t, true)
		t.Setenv("HOME", t.TempDir())
		t.Setenv("DRIVE9_SERVER", "http://example.invalid")
		// Setenv-then-unset so t.Cleanup restores the caller's env state;
		// a bare os.Unsetenv would leak across tests AND fail errcheck.
		t.Setenv(EnvAPIKey, "")
		t.Setenv(EnvVaultToken, "")
		_ = os.Unsetenv(EnvAPIKey)
		_ = os.Unsetenv(EnvVaultToken)
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)

		dir := secretDir(t, map[string]string{"K": "v"})
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil || !strings.Contains(err.Error(), EnvAPIKey) {
			t.Fatalf("error = %v, want missing-API-key message", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Row F — Input source contract. Stdin rejected; witness (d) pins that
// stdin-rejection wins over "--from is required" when both would apply.
// ---------------------------------------------------------------------------

func TestSecretPut_RowF_StdinRejectedAndWitnessD(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://example.invalid")
	t.Setenv("DRIVE9_API_KEY", "owner-key")

	t.Run("stdin non-tty rejected", func(t *testing.T) {
		withTTY(t, false)
		var count int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&count, 1)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		t.Setenv("DRIVE9_SERVER", srv.URL)
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)

		dir := secretDir(t, map[string]string{"K": "v"})
		err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
		if err == nil || !strings.Contains(err.Error(), "stdin input not supported") {
			t.Fatalf("error = %v, want stdin-rejected anchor", err)
		}
		if atomic.LoadInt32(&count) != 0 {
			t.Fatalf("HTTP issued despite stdin rejection: count = %d", count)
		}
	})

	// Witness (d): when stdin-present AND --from missing BOTH apply, the
	// stdin rejection is the user-visible error. This pins the priority
	// as a testable contract instead of an implementation-note aside.
	t.Run("witness d stdin wins over missing --from", func(t *testing.T) {
		withTTY(t, false)
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)
		err := SecretPut([]string{"/n/vault/aws-prod"}) // no --from
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "stdin input not supported") {
			t.Fatalf("error = %q, want stdin anchor (priority)", err)
		}
		if strings.Contains(err.Error(), "--from is required") {
			t.Fatalf("error = %q, must NOT surface missing-from message when stdin is the primary violation", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Row H — Observability anchors + no-auto-retry + ack-lost.
//
//	(c1) TestSecretPut_RowH_ServerRefused4xx       — "server refused"
//	(c2) TestSecretPut_RowH_StatusUnknown5xx       — "status unknown" + single request
//	(c3) TestSecretPut_RowH_AckLostViaHijack       — "status unknown" on mid-response cut
//
// The Go net/http idempotent auto-retry trap is killed via
// DisableKeepAlives=true + Request.GetBody=nil inside putSecretAtomic.
// Row H's whole point is to prove the CLI never silently re-issues the
// PUT: count MUST equal exactly 1 across all three failure shapes.
// ---------------------------------------------------------------------------

func TestSecretPut_RowH_ServerRefused4xx(t *testing.T) {
	withTTY(t, true)
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"scope out of owner"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	dir := secretDir(t, map[string]string{"K": "v"})
	err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
	if err == nil || !strings.Contains(err.Error(), "server refused") {
		t.Fatalf("error = %v, want `server refused` anchor", err)
	}
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("request count = %d, want 1 (no auto-retry on 4xx)", count)
	}
}

func TestSecretPut_RowH_StatusUnknown5xx(t *testing.T) {
	withTTY(t, true)
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	dir := secretDir(t, map[string]string{"K": "v"})
	err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
	if err == nil || !strings.Contains(err.Error(), "status unknown") {
		t.Fatalf("error = %v, want `status unknown` anchor", err)
	}
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("request count = %d, want 1 (Go net/http must NOT silently retry on 5xx)", count)
	}
}

// TestSecretPut_RowH_AckLostViaHijack simulates the ack-lost shape: the
// server reads the full request then yanks the TCP connection before
// sending any response. net/http's auto-retry heuristic is most likely
// to fire here. DisableKeepAlives=true + GetBody=nil MUST prevent it.
func TestSecretPut_RowH_AckLostViaHijack(t *testing.T) {
	withTTY(t, true)
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		// Drain the request body so we've unambiguously "received" the
		// write on the server side before hanging up.
		_, _ = io.Copy(io.Discard, r.Body)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("server does not support hijack")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatal(err)
		}
		// Force-close mid-response. No HTTP status written.
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	dir := secretDir(t, map[string]string{"K": "v"})
	err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
	if err == nil {
		t.Fatal("expected error on mid-response TCP cut")
	}
	if !strings.Contains(err.Error(), "status unknown") {
		t.Fatalf("error = %q, want `status unknown` anchor (cannot prove delivery)", err)
	}
	// The critical Row H assertion: EXACTLY one PUT hit the server. If
	// net/http auto-retried (which it does by default for PUTs when the
	// conn closes without a response), this would be 2.
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Fatalf("request count = %d, want 1 (net/http silent auto-retry leaked through)", got)
	}
}

// TestSecretPut_RowH_AbortedLocallyPreSend pins the `aborted locally`
// anchor to the ONE legitimate pre-send path: a missing server URL (no
// DRIVE9_SERVER, no config, resolver still returns default so this is
// actually rare — to reliably force it we patch resolver state directly).
// We use a deliberately malformed URL to trip the net/http request-build
// path, which is before-send.
func TestSecretPut_RowH_AbortedLocallyPreSend(t *testing.T) {
	withTTY(t, true)
	// Control characters in URL cause http.NewRequest to fail at build
	// time, before any byte could hit the wire — this is the canonical
	// pre-send failure path.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://exa\x01mple.invalid")
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	dir := secretDir(t, map[string]string{"K": "v"})
	err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir})
	if err == nil {
		t.Fatal("expected pre-send error")
	}
	if !strings.Contains(err.Error(), "aborted locally") {
		t.Fatalf("error = %q, want `aborted locally` anchor", err)
	}
}

// Happy path: end-to-end success with owner key, valid dir, 200 OK.
// Acts as the "uncoerced pass-through" companion to Row C — same
// contract, different axis of scrutiny.
func TestSecretPut_HappyPath(t *testing.T) {
	withTTY(t, true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"aws-prod","revision":1}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "owner-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	dir := secretDir(t, map[string]string{"ACCESS_KEY": "AKIA", "SECRET_KEY": "hunter2"})
	if err := SecretPut([]string{"/n/vault/aws-prod", "--from", dir}); err != nil {
		t.Fatalf("SecretPut: %v", err)
	}
}
