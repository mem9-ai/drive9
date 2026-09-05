package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminCreateTenantSendsIdempotencyKey(t *testing.T) {
	const key = "manus-user:user-1"
	received := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tenant_id":"tenant-1","api_key":"owner-key","status":"provisioning"}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL, "").AdminCreateTenant(context.Background(), AdminTenantCreateRequest{
		PublicKey:      "public",
		PrivateKey:     "private",
		IdempotencyKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-received; got != key {
		t.Fatalf("Idempotency-Key = %q, want %q", got, key)
	}
}
