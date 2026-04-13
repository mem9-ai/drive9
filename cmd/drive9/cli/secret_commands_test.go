package cli

import (
	"bytes"
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
	t.Setenv(vaultTokenEnv, "cap-token")

	out := captureStdout(t, func() {
		if err := SecretGet([]string{"aws-prod"}); err != nil {
			t.Fatalf("SecretGet: %v", err)
		}
	})
	if !strings.Contains(out, `"access_key": "AKIA"`) {
		t.Fatalf("output = %q", out)
	}
}

func TestSecretGrantPrintsTokenMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/vault/tokens" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"vault_abc","token_id":"cap_123","expires_at":"2026-04-14T00:00:00Z"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv("DRIVE9_API_KEY", "tenant-key")

	out := captureStdout(t, func() {
		if err := SecretGrant([]string{"aws-prod", "db-prod/password", "--agent", "deploy-agent", "--ttl", "1h"}); err != nil {
			t.Fatalf("SecretGrant: %v", err)
		}
	})
	if !strings.Contains(out, "token=vault_abc") || !strings.Contains(out, "token_id=cap_123") {
		t.Fatalf("output = %q", out)
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
	t.Setenv(vaultTokenEnv, "cap-token")

	out := captureStdout(t, func() {
		if err := SecretLs(nil); err != nil {
			t.Fatalf("SecretLs: %v", err)
		}
	})
	if out != "aws-prod\ndb-prod\n" {
		t.Fatalf("output = %q", out)
	}
}

func TestSecretExecInjectsSecretIntoChildEnv(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/vault/read/aws-prod" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_key":"AKIA","secret_key":"SECRET"}`))
	}))
	defer srv.Close()

	t.Setenv("HOME", t.TempDir())
	t.Setenv("DRIVE9_SERVER", srv.URL)
	t.Setenv(vaultTokenEnv, "cap-token")

	out := captureStdout(t, func() {
		if err := SecretExec([]string{"aws-prod", "--", "/bin/sh", "-c", "printf '%s:%s' \"$ACCESS_KEY\" \"$SECRET_KEY\""}); err != nil {
			t.Fatalf("SecretExec: %v", err)
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

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
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
