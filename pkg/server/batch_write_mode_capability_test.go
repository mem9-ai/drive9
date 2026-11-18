package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
)

// /v1/status is where a client negotiates storage capabilities, and
// batch_write_mode_v1 promises atomic content+mode batch writes that the server enforces
// owner-only. If an fs_scoped key could read it, its mount would attach hasMode to every
// writeback commit and be denied per item on each cycle. The route is registered directly
// on the mux, so the scoped business allowlist never runs for it: the refusal has to be
// part of the handler, and this drives the real server to pin it.
func TestTenantStatusRefusesScopedKey(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	get := func() (int, []byte) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+rt.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}

	status, body := get()
	if status != http.StatusOK {
		t.Fatalf("owner status = %d, want 200: %s", status, body)
	}
	if strings.Contains(string(body), "storage_capabilities") || strings.Contains(string(body), "batch_write_mode") {
		t.Fatalf("owner status advertises a capability the scoped rule cannot gate: %s", body)
	}

	setAuthRuntimeScopeKind(t, rt, meta.APIKeyScopeKindFS)
	// The resolve cache keeps the owner record the first request loaded.
	rt.meta.InvalidateAPIKeyResolveCacheForTenant(rt.tenantID)
	status, body = get()
	if status != http.StatusForbidden {
		t.Fatalf("scoped status = %d, want 403: %s", status, body)
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("scoped status body is not JSON: %v (%s)", err, body)
	}
	if _, ok := parsed["storage_capabilities"]; ok {
		t.Fatalf("scoped status carried a capability contract: %s", body)
	}

	// The backstop for the other half: a scoped scope must not be chmod-capable, so a
	// future per-token advertisement computed from CanChmod() stays false.
	if (&TenantScope{IsScoped: true}).CanChmod() {
		t.Fatal("scoped scope reports chmod authority")
	}
}
