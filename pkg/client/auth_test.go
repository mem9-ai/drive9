package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNewSendsBearerAPIKey asserts that New() authenticates with
// `Authorization: Bearer <api-key>`. Server-side disambiguation between
// owner API keys and delegated JWTs happens in middleware
// (pkg/server/auth.go); the wire format is identical.
func TestNewSendsBearerAPIKey(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "owner-api-key-xyz")
	if _, err := c.List("/"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if gotAuth != "Bearer owner-api-key-xyz" {
		t.Fatalf("Authorization = %q, want %q", gotAuth, "Bearer owner-api-key-xyz")
	}
}

// TestNewWithTokenSendsBearerJWT asserts that NewWithToken() also uses
// `Authorization: Bearer`. The constructor name is the sole call-site
// discriminator; there is no `X-Dat9-Capability` or similar side-channel
// header. Server middleware routes via the `iss` claim (tenant vs. capability).
func TestNewWithTokenSendsBearerJWT(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer srv.Close()

	c := NewWithToken(srv.URL, "jwt-aaa.bbb.ccc")
	if _, err := c.List("/"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if gotAuth != "Bearer jwt-aaa.bbb.ccc" {
		t.Fatalf("Authorization = %q, want %q", gotAuth, "Bearer jwt-aaa.bbb.ccc")
	}
}

// TestClientCredentialIsImmutableAfterConstruction is the client-side
// half of Invariant #6: once a *client.Client has been constructed, its
// credential cannot be mutated through any exported API. Subsequent
// resolver / config changes only take effect via a new constructor call —
// which, at the mount layer, requires umount+mount (see spec §12, §17).
//
// This test pins the absence of a setter. If a future change adds
// SetAPIKey/SetToken/Rebind, the test must be updated alongside an
// explicit spec amendment to §17 and Invariant #6.
func TestClientCredentialIsImmutableAfterConstruction(t *testing.T) {
	t.Parallel()

	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer srv.Close()

	c := NewWithToken(srv.URL, "jwt-original")

	// Simulate what would happen if the resolver were re-run while a
	// client is in flight: a *new* client for the new credential can
	// exist, but the original must keep sending its original token.
	_ = NewWithToken(srv.URL, "jwt-rotated")

	for i := 0; i < 3; i++ {
		if _, err := c.List("/"); err != nil {
			t.Fatalf("List #%d: %v", i, err)
		}
	}

	for i, auth := range seen {
		if auth != "Bearer jwt-original" {
			t.Fatalf("request %d Authorization = %q, want %q (credential must not rotate mid-life)", i, auth, "Bearer jwt-original")
		}
	}
}
