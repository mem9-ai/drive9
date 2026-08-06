package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func mappingConfig(t *testing.T, jobs string) *Config {
	t.Helper()
	body := strings.Replace(validConfigYAML, `jobs:
  - volume_id: vol-001
    node_name: node-a
    source:
      type: ebs
      root: /ebs/001
    target:
      space_ref: space-001
      prefix: /
`, "jobs:\n"+jobs, 1)
	cfg, err := LoadConfig(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestValidateMappingsSupportsBothLayoutsAndUsesSegmentBoundaries(t *testing.T) {
	rootLayout := mappingConfig(t, `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /}
`)
	if err := ValidateMappings(rootLayout); err != nil {
		t.Fatalf("one-Space root layout: %v", err)
	}
	shared := mappingConfig(t, `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /vol-1}
  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /vol-10}
`)
	if err := ValidateMappings(shared); err != nil {
		t.Fatalf("segment-distinct prefixes: %v", err)
	}
}

func TestValidateMappingsRejectsOverlapRootSharingAndControlPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		jobs string
	}{
		{
			name: "ancestor overlap",
			jobs: `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /team}
  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /team/sub}
`,
		},
		{
			name: "shared root",
			jobs: `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /}
  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /other}
`,
		},
		{
			name: "control prefix",
			jobs: `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /.drive9-migration/jobs}
`,
		},
		{
			name: "duplicate node",
			jobs: `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /one}
  - volume_id: vol-002
    node_name: node-a
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /two}
`,
		},
		{
			name: "duplicate source root",
			jobs: `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /one}
  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /two}
`,
		},
		{
			name: "non-NFC prefix",
			jobs: `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /é}
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateMappings(mappingConfig(t, tc.jobs)); err == nil {
				t.Fatal("invalid mapping accepted")
			}
		})
	}
}

func preflightStartup(t *testing.T, endpoint, root string) *Startup {
	t.Helper()
	body := strings.Replace(validConfigYAML, "https://drive9.example.com", endpoint, 1)
	body = strings.Replace(body, "/ebs/001", root, 1)
	configPath := writeConfig(t, body)
	secretRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(secretRoot, "space-001-key"), []byte("owner-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	startup, err := LoadStartup(configPath, "node-a", string(PhaseSyncing), secretRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	return startup
}

func TestPreflightChecksSelectedSourceClientCapabilitiesAndEmptyTarget(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	var statusHits, listHits, mutations atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer owner-key" {
			http.Error(w, "missing owner key", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			statusHits.Add(1)
			_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true,"event_ingest":false}}`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{{Name: ".drive9-migration", IsDir: true}}})
		default:
			mutations.Add(1)
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	startup := preflightStartup(t, srv.URL, root)
	result, err := preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.VolumeID != "vol-001" || result.EntryCount != 1 || result.LogicalBytes != 3 || !result.VolumeIdentityVerified {
		t.Fatalf("preflight=%+v", result)
	}
	if !result.RequiredCapabilities || result.EventReportingAvailable || !result.RecoveryControlPresent || result.ControlPrefix != ControlPrefix {
		t.Fatalf("preflight capabilities/control=%+v", result)
	}
	if result.MaxUploadBytes != 1048576 || result.InlineThreshold != 1024 || statusHits.Load() != 1 || listHits.Load() != 1 || mutations.Load() != 0 {
		t.Fatalf("limits/status/list/mutations=%+v %d/%d/%d", result, statusHits.Load(), listHits.Load(), mutations.Load())
	}
}

func TestPreflightRequiredCapabilitiesFailClosedAndEventIsOptional(t *testing.T) {
	root := t.TempDir()
	for _, missing := range []string{"checksum_read", "checksum_complete", "conditional_create", "conditional_update"} {
		t.Run(missing, func(t *testing.T) {
			caps := map[string]bool{
				"checksum_read": true, "checksum_complete": true,
				"conditional_create": true, "conditional_update": true,
			}
			caps[missing] = false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(map[string]any{"max_upload_bytes": 10, "inline_threshold": 5, "migration_capabilities": caps})
					return
				}
				t.Error("target listing reached with missing required capability")
				http.Error(w, "unexpected", http.StatusInternalServerError)
			}))
			defer srv.Close()
			_, err := Preflight(context.Background(), preflightStartup(t, srv.URL, root))
			if !errors.Is(err, ErrPreflight) || !strings.Contains(err.Error(), missing) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestPreflightRejectsBusinessTargetAndRootControlCollision(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".drive9-migration"), 0o700); err != nil {
		t.Fatal(err)
	}
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"max_upload_bytes":10,"inline_threshold":5,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{{Name: "business", IsDir: false}}})
	}))
	defer srv.Close()
	_, err := Preflight(context.Background(), preflightStartup(t, srv.URL, root))
	if !errors.Is(err, ErrPreflight) || !strings.Contains(err.Error(), "control") {
		t.Fatalf("control collision error=%v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("control collision made %d remote calls", hits.Load())
	}

	if err := os.Remove(filepath.Join(root, ".drive9-migration")); err != nil {
		t.Fatal(err)
	}
	_, err = Preflight(context.Background(), preflightStartup(t, srv.URL, root))
	if !errors.Is(err, ErrPreflight) || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("business target error=%v", err)
	}
}

func TestPreflightStaticMappingFailsBeforeRemoteCall(t *testing.T) {
	root := t.TempDir()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()
	startup := preflightStartup(t, srv.URL, root)
	startup.Config.Jobs = append(startup.Config.Jobs, Job{
		VolumeID: "vol-002", NodeName: "node-b", Source: SourceConfig{Type: "ebs", Root: "/ebs/002"},
		Target: TargetConfig{SpaceRef: "space-001", Prefix: "/sub"},
	})
	_, err := Preflight(context.Background(), startup)
	if !errors.Is(err, ErrPreflight) || hits.Load() != 0 {
		t.Fatalf("error=%v remote hits=%d", err, hits.Load())
	}
}

func TestPreflightPreservesOldServerAndTypedClientErrors(t *testing.T) {
	root := t.TempDir()
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if status == http.StatusOK {
					_, _ = w.Write([]byte(`{"max_upload_bytes":10,"inline_threshold":5}`))
					return
				}
				http.Error(w, "failure", status)
			}))
			defer srv.Close()
			_, err := Preflight(context.Background(), preflightStartup(t, srv.URL, root))
			if status == http.StatusOK {
				if !errors.Is(err, client.ErrMigrationUnsupported) {
					t.Fatalf("old server error=%v", err)
				}
				return
			}
			var statusErr *client.StatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != status {
				t.Fatalf("typed error=%T %v", err, err)
			}
		})
	}
}
