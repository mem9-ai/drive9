package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestApplyExtentCapabilityGate(t *testing.T) {
	tests := []struct {
		name       string
		capability bool
		wantPaths  bool
		wantExtent bool
	}{
		{name: "legacy server falls back", wantPaths: false, wantExtent: false},
		{name: "capable server keeps extent", capability: true, wantPaths: true, wantExtent: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tc.capability {
					_, _ = w.Write([]byte(`{"status":"active","max_upload_bytes":1,"inline_threshold":1,"storage_capabilities":{"completion_identity_v1":true}}`))
					return
				}
				_, _ = w.Write([]byte(`{"status":"active","max_upload_bytes":1,"inline_threshold":1}`))
			}))
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "key")
			cl.Warm(context.Background())
			cfg := MountConfig{Server: ts.URL, ExtentPaths: []string{"*.db"}}
			wantExt := map[string]bool{"extent": true}
			applyExtentCapabilityGate(&cfg, cl, wantExt)
			if got := len(cfg.ExtentPaths) > 0; got != tc.wantPaths {
				t.Fatalf("ExtentPaths present = %v, want %v", got, tc.wantPaths)
			}
			if got := wantExt["extent"]; got != tc.wantExtent {
				t.Fatalf("extent extension enabled = %v, want %v", got, tc.wantExtent)
			}
		})
	}
}
