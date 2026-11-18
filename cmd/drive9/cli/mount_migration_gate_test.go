package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRefuseMigratedSourceMount(t *testing.T) {
	state := "migrated"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"tenant_id":"t1","status":"read_only","migration_state":"` + state + `"}`))
	}))
	defer ts.Close()

	err := refuseMigratedSourceMount(context.Background(), ts.URL, "key", "")
	if err == nil || !strings.Contains(err.Error(), "migrated") {
		t.Fatalf("err = %v, want migrated rejection", err)
	}
	state = "syncing"
	if err := refuseMigratedSourceMount(context.Background(), ts.URL, "key", ""); err != nil {
		t.Fatalf("syncing mount err = %v", err)
	}
}
