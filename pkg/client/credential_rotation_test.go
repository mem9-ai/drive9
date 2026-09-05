package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSetAPIKeyInvalidatesCredentialScopeKind(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		scopeKind := "owner"
		if r.Header.Get("Authorization") == "Bearer replacement" {
			scopeKind = "fs_scoped"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"status":"active","scope_kind":%q,"max_upload_bytes":1}`, scopeKind)
	}))
	defer server.Close()

	client := NewWithToken(server.URL, "previous")
	kind, err := client.CredentialScopeKind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if kind != "owner" {
		t.Fatalf("initial scope kind = %q, want owner", kind)
	}

	client.SetAPIKey("replacement")
	kind, err = client.CredentialScopeKind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if kind != "fs_scoped" {
		t.Fatalf("replacement scope kind = %q, want fs_scoped", kind)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("status calls = %d, want 2", got)
	}
}
