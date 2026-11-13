package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// batch_write_mode_v1 promises atomic content+mode batch writes, and the server enforces
// owner-only mode bits for it. If the capability were ever negotiated by an fs_scoped
// token, its mount would attach hasMode to every commit and be denied per item on each
// writeback cycle. Nothing advertises the contract today, so this pins the two ways that
// could regress: the status response must not grow the field unconditionally, and the
// route that carries it must stay closed to scoped tokens.
func TestBatchWriteModeCapabilityStaysOwnerOnly(t *testing.T) {
	body, err := json.Marshal(TenantStatusResponse{Status: "active", MaxUploadBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "batch_write_mode") || strings.Contains(string(body), "storage_capabilities") {
		t.Fatalf("tenant status advertises a storage capability unconditionally: %s", body)
	}

	// /v1/status serves the negotiated contract and is not on the scoped allowlist, so a
	// scoped token cannot learn the capability in the first place.
	scoped := &TenantScope{IsScoped: true, FSScopes: []FSScope{{Prefix: "/", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}}}
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req = req.WithContext(withScope(req.Context(), scoped))
	if isScopedBusinessRequestAllowed(req) {
		t.Fatal("scoped token may reach GET /v1/status and negotiate the capability")
	}
}
