package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/backend"
)

func TestLocalStatusIdentityRequiresAuthentication(t *testing.T) {
	s := &Server{fallback: &backend.Dat9Backend{}, localTenantAPIKey: "local-status-test-key"}
	for _, tc := range []struct {
		name, method, token string
		status              int
	}{
		{"authenticated", http.MethodGet, s.localTenantAPIKey, http.StatusOK},
		{"missing key", http.MethodGet, "", http.StatusUnauthorized},
		{"wrong key", http.MethodGet, "wrong", http.StatusUnauthorized},
		{"wrong method", http.MethodPost, s.localTenantAPIKey, http.StatusMethodNotAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/v1/status?tenant_id=other", nil)
			req.Header.Set("X-Tenant-ID", "other")
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			s.handleTenantStatus(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if tc.status == http.StatusOK {
				if string(body["tenant_id"]) != `"local"` || string(body["status"]) != `"active"` {
					t.Fatalf("wrong authenticated identity: %s", rec.Body.Bytes())
				}
			} else if _, ok := body["tenant_id"]; ok {
				t.Fatal("rejected request disclosed tenant identity")
			}
		})
	}
}
