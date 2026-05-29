package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateNameFlagValueCanBeHelpLike(t *testing.T) {
	withIsolatedHome(t)
	var provisionCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/provision" {
			http.NotFound(w, r)
			return
		}
		provisionCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"tenant_id": "tenant-help",
			"api_key":   "owner-key",
			"status":    "provisioning",
		})
	}))
	defer srv.Close()

	out, err := captureStdoutE(t, func() error {
		return Create([]string{"--name", "--help", "--server", srv.URL})
	})
	if err != nil {
		t.Fatalf("Create --name --help: %v", err)
	}
	if provisionCalls != 1 {
		t.Fatalf("provision calls = %d, want 1; output = %q", provisionCalls, out)
	}
	ctx := loadConfig().Contexts["--help"]
	if ctx == nil || ctx.Type != PrincipalOwner || ctx.APIKey != "owner-key" || ctx.Server != srv.URL {
		t.Fatalf("saved context --help = %+v", ctx)
	}
	if !strings.Contains(out, `created "--help"`) {
		t.Fatalf("output = %q", out)
	}
}
