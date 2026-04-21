package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSecretSetFallsBackToUpdateOnConflict(t *testing.T) {
	var postCount, putCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/vault/secrets":
			atomic.AddInt32(&postCount, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"secret already exists"}`))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/vault/secrets/aws-prod":
			atomic.AddInt32(&putCount, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"aws-prod","secret_type":"generic","revision":2,"created_by":"drive9-cli","created_at":"2026-04-13T00:00:00Z","updated_at":"2026-04-13T00:00:00Z"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	if err := SecretSet([]string{"aws-prod", "access_key=AKIA", "secret_key=secret"}); err != nil {
		t.Fatalf("SecretSet: %v", err)
	}
	if atomic.LoadInt32(&postCount) != 1 {
		t.Fatalf("POST count = %d, want 1", postCount)
	}
	if atomic.LoadInt32(&putCount) != 1 {
		t.Fatalf("PUT count = %d, want 1", putCount)
	}
}

func TestSecretGetUsesCapabilityToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer cap-token" {
			t.Fatalf("Authorization = %q, want Bearer cap-token", got)
		}
		if r.URL.Path != "/v1/vault/read/aws-prod" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_key":"AKIA","secret_key":"SECRET"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	out := captureStdout(t, func() {
		if err := SecretGet([]string{"aws-prod"}); err != nil {
			t.Fatalf("SecretGet: %v", err)
		}
	})
	if !strings.Contains(out, `"access_key": "AKIA"`) {
		t.Fatalf("output = %q", out)
	}
}

func TestSecretGrantPrintsGrantMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/vault/grants" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"vt_abc","grant_id":"grt_123","expires_at":"2026-04-14T00:00:00Z","scope":["aws-prod","db-prod/password"],"perm":"read"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	out := captureStdout(t, func() {
		if err := SecretGrant([]string{"aws-prod", "db-prod/password", "--agent", "deploy-agent", "--ttl", "1h", "--perm", "read"}); err != nil {
			t.Fatalf("SecretGrant: %v", err)
		}
	})
	if !strings.Contains(out, "token=vt_abc") || !strings.Contains(out, "grant_id=grt_123") {
		t.Fatalf("output = %q", out)
	}
	// Pre-V2a label must be gone so automation can't false-positive on the old id field.
	if strings.Contains(out, "token_id=") {
		t.Fatalf("output still contains legacy token_id= label: %q", out)
	}
}

func TestSecretLsFallsBackToReadableScopeWithCapabilityToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"secrets":["db-prod","aws-prod"]}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	out := captureStdout(t, func() {
		if err := SecretLs(nil); err != nil {
			t.Fatalf("SecretLs: %v", err)
		}
	})
	if out != "aws-prod\ndb-prod\n" {
		t.Fatalf("output = %q", out)
	}
}

func TestSecretLsPrefersExplicitCapabilityTokenOverAmbientAPIKey(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://example.invalid")
	t.Setenv("DRIVE9_API_KEY", "tenant-key")
	t.Setenv(EnvVaultToken, "cap-token")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/vault/read":
			if got := r.Header.Get("Authorization"); got != "Bearer cap-token" {
				t.Fatalf("Authorization = %q, want Bearer cap-token", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"secrets":["scoped-secret"]}`))
		case "/v1/vault/secrets":
			t.Fatalf("management list should not be used when an explicit capability token is set")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv("DRIVE9_SERVER", srv.URL)

	out := captureStdout(t, func() {
		if err := SecretLs(nil); err != nil {
			t.Fatalf("SecretLs: %v", err)
		}
	})
	if out != "scoped-secret\n" {
		t.Fatalf("output = %q", out)
	}
}

// G-V2c-1 (positive half): the spec-shaped path `/n/vault/<secret>` MUST
// reach the server at `/v1/vault/read/<secret>` and the child MUST see the
// injected env vars. The two keys are strict-charset legal and values are
// plain printable, so the happy path exercises the "uncoerced pass-through"
// side of SecretWith in isolation.
func TestSecretWithInjectsSecretIntoChildEnv(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/aws-prod" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ACCESS_KEY":"AKIA","SECRET_KEY":"SECRET"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(EnvVaultToken, "cap-token")

	out := captureStdout(t, func() {
		if err := SecretWith([]string{"/n/vault/aws-prod", "--", "/bin/sh", "-c", "printf '%s:%s' \"$ACCESS_KEY\" \"$SECRET_KEY\""}); err != nil {
			t.Fatalf("SecretWith: %v", err)
		}
	})
	if out != "AKIA:SECRET" {
		t.Fatalf("output = %q", out)
	}
}

func TestSecretAuditFiltersClientSide(t *testing.T) {
	now := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/audit" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events":[` +
			`{"event_id":"1","event_type":"secret.read","agent_id":"deploy-agent","secret_name":"aws-prod","timestamp":"` + now.Add(-10*time.Minute).Format(time.RFC3339) + `"},` +
			`{"event_id":"2","event_type":"secret.read","agent_id":"test-agent","secret_name":"aws-prod","timestamp":"` + now.Add(-2*time.Hour).Format(time.RFC3339) + `"}` +
			`]}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	out := captureStdout(t, func() {
		if err := SecretAudit([]string{"--agent", "deploy-agent", "--since", "1h", "--json"}); err != nil {
			t.Fatalf("SecretAudit: %v", err)
		}
	})
	if !strings.Contains(out, `"agent_id": "deploy-agent"`) || strings.Contains(out, `"agent_id": "test-agent"`) {
		t.Fatalf("output = %q", out)
	}
}

// G-V2a-5: --perm validation. `read` and `write` MUST succeed; anything else
// (including empty / missing / "admin" / typos) MUST be rejected BEFORE any
// HTTP call. Fail-closed is the point — there is no spec-supported default.
func TestSecretGrantPermValidation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://example.invalid")
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	cases := []struct {
		name    string
		args    []string
		wantErr string // substring; empty means "must succeed"
	}{
		{
			name:    "missing --perm",
			args:    []string{"aws-prod", "--agent", "a", "--ttl", "1h"},
			wantErr: "--perm is required",
		},
		{
			name:    "unknown --perm value",
			args:    []string{"aws-prod", "--agent", "a", "--ttl", "1h", "--perm", "admin"},
			wantErr: `invalid --perm "admin"`,
		},
		{
			name:    "empty --perm value",
			args:    []string{"aws-prod", "--agent", "a", "--ttl", "1h", "--perm", ""},
			wantErr: "--perm is required",
		},
		{
			name:    "missing scope",
			args:    []string{"--agent", "a", "--ttl", "1h", "--perm", "read"},
			wantErr: "scope",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := SecretGrant(tc.args)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// V2a removed `--task` from `secret grant` (task_id is not part of the
// /v1/vault/grants contract). Accepting a removed flag silently would be a
// hidden downgrade of semantics for callers still passing it; instead we
// fail loudly via the `unknown flag` branch so automation breaks at the
// earliest possible point rather than getting a successful return with
// dropped inputs.
func TestSecretGrantRejectsRemovedTaskFlag(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", "http://example.invalid")
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	err := SecretGrant([]string{"aws-prod", "--agent", "a", "--task", "t-42", "--ttl", "1h", "--perm", "read"})
	if err == nil {
		t.Fatal("expected error for removed --task flag, got nil")
	}
	if !strings.Contains(err.Error(), `unknown flag "--task"`) {
		t.Fatalf("error = %q, want substring `unknown flag \"--task\"`", err)
	}
}

// G-V2a-5 (positive half): both valid --perm values MUST reach the server
// with the request body echoing the flag verbatim. Server-side validation
// of perm semantics is out of scope here; the contract is "pass through".
func TestSecretGrantPermReadAndWriteReachServer(t *testing.T) {
	for _, perm := range []string{"read", "write"} {
		t.Run(perm, func(t *testing.T) {
			var gotPerm string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/vault/grants" {
					http.NotFound(w, r)
					return
				}
				var req map[string]any
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatalf("decode request: %v", err)
				}
				gotPerm, _ = req["perm"].(string)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"token":"vt_x","grant_id":"grt_x","expires_at":"2026-04-14T00:00:00Z","scope":["s"],"perm":"` + perm + `"}`))
			}))
			defer srv.Close()

			t.Setenv("HOME", t.TempDir())
			t.Setenv("DRIVE9_SERVER", srv.URL)
			t.Setenv("DRIVE9_API_KEY", "tenant-key")

			_ = captureStdout(t, func() {
				if err := SecretGrant([]string{"aws-prod", "--agent", "a", "--ttl", "1h", "--perm", perm}); err != nil {
					t.Fatalf("SecretGrant(%s): %v", perm, err)
				}
			})
			if gotPerm != perm {
				t.Fatalf("perm sent = %q, want %q", gotPerm, perm)
			}
		})
	}
}

// G-V2a-5 (server 4xx): a 4xx from /v1/vault/grants MUST surface as an error
// without synthesizing a fake grant_id locally — we never present server-side
// rejections as success.
func TestSecretGrantServer4xxSurfacesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"scope out of owner's namespace"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := SecretGrant([]string{"other-tenant/key", "--agent", "a", "--ttl", "1h", "--perm", "read"})
	if err == nil {
		t.Fatal("expected error from server 403, got nil")
	}
}

// G-V2a-8: --json output MUST contain the new key set (token, grant_id,
// expires_at, scope, perm) and MUST NOT contain the legacy token_id key.
// This pins the machine-readable contract change so downstream tools break
// loudly at parse time rather than silently.
func TestSecretGrantJSONKeySet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/grants" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"vt_abc","grant_id":"grt_123","expires_at":"2026-04-14T00:00:00Z","scope":["aws-prod"],"perm":"read"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	out := captureStdout(t, func() {
		if err := SecretGrant([]string{"aws-prod", "--agent", "a", "--ttl", "1h", "--perm", "read", "--json"}); err != nil {
			t.Fatalf("SecretGrant: %v", err)
		}
	})
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("--json output not valid JSON: %v\n%s", err, out)
	}
	for _, key := range []string{"token", "grant_id", "expires_at", "scope", "perm"} {
		if _, ok := parsed[key]; !ok {
			t.Fatalf("--json output missing key %q: %s", key, out)
		}
	}
	if _, ok := parsed["token_id"]; ok {
		t.Fatalf("--json output still contains legacy token_id key: %s", out)
	}
}

// --token-only stays byte-identical to the pre-V2a shape: the token string
// followed by a single newline, nothing else. Automation pipelines that
// already consume this form MUST NOT need to change.
func TestSecretGrantTokenOnlyUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"vt_abc","grant_id":"grt_123","expires_at":"2026-04-14T00:00:00Z","scope":["s"],"perm":"read"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	out := captureStdout(t, func() {
		if err := SecretGrant([]string{"aws-prod", "--agent", "a", "--ttl", "1h", "--perm", "read", "--token-only"}); err != nil {
			t.Fatalf("SecretGrant: %v", err)
		}
	})
	if out != "vt_abc\n" {
		t.Fatalf("--token-only output = %q, want %q", out, "vt_abc\n")
	}
}

// G-V2a-7 + G-V2a-9: SecretRevoke MUST dispatch by id prefix. grt_* ids
// target DELETE /v1/vault/grants/<id> with {"revoked_by":"cli","reason":""}.
// Non-grt_ ids (legacy token ids) target DELETE /v1/vault/tokens/<id> with
// no body — this keeps pre-V2a tokens revocable through the cleanup wave.
func TestSecretRevokeDispatchesByPrefix(t *testing.T) {
	t.Run("grt_ prefix -> grants endpoint with revoke metadata", func(t *testing.T) {
		var (
			gotPath      string
			gotMethod    string
			gotRevokedBy string
			gotReason    string
			sawBody      bool
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotMethod = r.Method
			if r.Body != nil {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
					if v, ok := body["revoked_by"].(string); ok {
						gotRevokedBy = v
					}
					if v, ok := body["reason"].(string); ok {
						gotReason = v
					}
					sawBody = true
				}
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		t.Setenv("HOME", t.TempDir())
		t.Setenv("DRIVE9_SERVER", srv.URL)
		t.Setenv("DRIVE9_API_KEY", "tenant-key")
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)

		if err := SecretRevoke([]string{"grt_abc123"}); err != nil {
			t.Fatalf("SecretRevoke: %v", err)
		}
		if gotMethod != http.MethodDelete {
			t.Fatalf("method = %q, want DELETE", gotMethod)
		}
		if gotPath != "/v1/vault/grants/grt_abc123" {
			t.Fatalf("path = %q, want /v1/vault/grants/grt_abc123", gotPath)
		}
		if !sawBody {
			t.Fatal("expected JSON body on DELETE /v1/vault/grants/<id>")
		}
		if gotRevokedBy != "cli" {
			t.Fatalf("revoked_by = %q, want %q", gotRevokedBy, "cli")
		}
		if gotReason != "" {
			t.Fatalf("reason = %q, want empty string", gotReason)
		}
	})

	t.Run("legacy id -> tokens endpoint", func(t *testing.T) {
		var (
			gotPath   string
			gotMethod string
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotMethod = r.Method
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		t.Setenv("HOME", t.TempDir())
		t.Setenv("DRIVE9_SERVER", srv.URL)
		t.Setenv("DRIVE9_API_KEY", "tenant-key")
		resetCredentialCacheForTest()
		t.Cleanup(resetCredentialCacheForTest)

		if err := SecretRevoke([]string{"cap_legacy_42"}); err != nil {
			t.Fatalf("SecretRevoke: %v", err)
		}
		if gotMethod != http.MethodDelete {
			t.Fatalf("method = %q, want DELETE", gotMethod)
		}
		if gotPath != "/v1/vault/tokens/cap_legacy_42" {
			t.Fatalf("path = %q, want /v1/vault/tokens/cap_legacy_42", gotPath)
		}
	})
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	// Reset the credential resolver cache before invoking the CLI entry point.
	// The resolver uses sync.Once, so per-test t.Setenv values only flow
	// through if the cache is cleared first. Also reset on cleanup so any
	// later test that calls the resolver directly sees a clean slate.
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	fn()

	_ = w.Close()
	return <-done
}
