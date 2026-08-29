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
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

func mappingConfig(t *testing.T, jobs string) *Config {
	t.Helper()
	var legacy []struct {
		VolumeID string       `yaml:"volume_id"`
		NodeName string       `yaml:"node_name"`
		Source   SourceConfig `yaml:"source"`
		Target   TargetConfig `yaml:"target"`
	}
	if err := yaml.Unmarshal([]byte(jobs), &legacy); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		Version: ConfigVersion, Drive9: Drive9Config{Endpoint: "https://drive9.example.com"},
		JobDefaults: JobDefaults{
			Sync: SyncDefaults{GracePeriod: Duration(DefaultGracePeriod)},
			Performance: PerformanceDefaults{
				MaxBytesPerSecond: 1024, SmallFileWorkers: 2, LargeFileWorkers: 1,
			},
		},
		Spaces:     map[string]SpaceConfig{"space-001": {CredentialRef: "space-001-key"}},
		EBSSources: make([]EBSSourceConfig, 0, len(legacy)),
	}
	for _, job := range legacy {
		cfg.EBSSources = append(cfg.EBSSources, EBSSourceConfig{
			VolumeID: job.VolumeID, NodeName: job.NodeName, Root: job.Source.Root,
			Jobs: []JobConfig{{
				JobID: job.VolumeID + "-root", Subpath: "/", Target: job.Target,
			}},
		})
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

func TestValidateMappingsUsesCredentialMappingForSpaceAliases(t *testing.T) {
	newConfig := func(t *testing.T, aliasCredential, aliasPrefix string) *Config {
		t.Helper()
		cfg := mappingConfig(t, `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/001}
    target: {space_ref: space-001, prefix: /team}
  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /other}
`)
		cfg.Spaces["alias"] = SpaceConfig{CredentialRef: aliasCredential}
		cfg.EBSSources[1].Jobs[0].Target = TargetConfig{SpaceRef: "alias", Prefix: aliasPrefix}
		return cfg
	}

	sameCredential := "space-001-key"
	if err := ValidateMappings(newConfig(t, sameCredential, "/team/sub")); err == nil {
		t.Fatal("overlapping Space aliases using one credential_ref were accepted")
	}
	if err := ValidateMappings(newConfig(t, sameCredential, "/other")); err != nil {
		t.Fatalf("non-overlapping Space aliases using one credential_ref: %v", err)
	}
	if err := ValidateMappings(newConfig(t, "other-space-key", "/team/sub")); err != nil {
		t.Fatalf("independent credentials were treated as one Space: %v", err)
	}
}

func TestValidateAuthenticatedTargetsUsesTenantIdentity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		tenantA     string
		tenantB     string
		prefixA     string
		prefixB     string
		wantErr     bool
		crossSource bool
	}{
		{name: "same tenant equal", tenantA: "tenant-1", tenantB: "tenant-1", prefixA: "/team", prefixB: "/team", wantErr: true},
		{name: "same tenant ancestor", tenantA: "tenant-1", tenantB: "tenant-1", prefixA: "/team", prefixB: "/team/sub", wantErr: true},
		{name: "same tenant segment distinct", tenantA: "tenant-1", tenantB: "tenant-1", prefixA: "/team", prefixB: "/team-2"},
		{name: "different tenants equal", tenantA: "tenant-1", tenantB: "tenant-2", prefixA: "/team", prefixB: "/team"},
		{name: "cross source overlap", tenantA: "tenant-1", tenantB: "tenant-1", prefixA: "/team", prefixB: "/team/sub", wantErr: true, crossSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, closeServer := authenticatedTargetRuntime(t, tc.tenantA, tc.tenantB, tc.prefixA, tc.prefixB, tc.crossSource)
			defer closeServer()

			err := validateAuthenticatedTargets(context.Background(), runtime)
			if tc.wantErr && err == nil {
				t.Fatal("overlapping authenticated targets were accepted")
			}
			if !tc.wantErr && err != nil {
				t.Fatal(err)
			}
			if !tc.wantErr {
				if runtime.Jobs[0].acceptedTenantID != tc.tenantA {
					t.Fatalf("accepted tenant ID = %q, want %q", runtime.Jobs[0].acceptedTenantID, tc.tenantA)
				}
			}
		})
	}
}

func TestValidateAuthenticatedTargetsRejectsMissingTenantIdentity(t *testing.T) {
	runtime, closeServer := authenticatedTargetRuntime(t, "", "tenant-2", "/a", "/b", false)
	defer closeServer()

	err := validateAuthenticatedTargets(context.Background(), runtime)
	if !errors.Is(err, client.ErrMigrationUnsupported) {
		t.Fatalf("error = %v, want ErrMigrationUnsupported", err)
	}
	for _, job := range runtime.Jobs {
		if job.acceptedTenantID != "" {
			t.Fatalf("partially accepted tenant identity for %s", job.Job.JobID)
		}
	}
}

func TestPlanAuthenticatedTargetGateFailureMarksEverySelectedJob(t *testing.T) {
	runtime, closeServer := authenticatedTargetRuntime(t, "tenant-1", "tenant-1", "/team", "/team/sub", false)
	defer closeServer()
	root := t.TempDir()
	runtime.Source.Root = root
	runtime.Config.EBSSources[0].Root = root
	for _, job := range runtime.Jobs {
		if err := os.Mkdir(filepath.Join(root, strings.TrimPrefix(job.Job.Subpath, "/")), 0o755); err != nil {
			t.Fatal(err)
		}
		job.Job.EBSRoot = root
		job.Job.Source.Root = effectiveSourceRoot(root, job.Job.Subpath)
		job.mountProbe = testMountedSourceProbe
	}

	result, err := Plan(context.Background(), runtime)
	if !errors.Is(err, ErrPlanFailed) || len(result.Jobs) != 2 {
		t.Fatalf("plan error = %v, result = %+v", err, result)
	}
	for _, job := range result.Jobs {
		if job.Result != nil || !strings.Contains(job.Error, "overlapping prefixes") {
			t.Fatalf("plan Job = %+v", job)
		}
	}
}

func authenticatedTargetRuntime(t *testing.T, tenantA, tenantB, prefixA, prefixB string, crossSource bool) (*RuntimeStartup, func()) {
	t.Helper()
	tenantByAuth := map[string]string{"Bearer key-a": tenantA, "Bearer key-b": tenantB}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": tenantByAuth[r.Header.Get("Authorization")]})
	}))
	secretRoot := t.TempDir()
	for name, key := range map[string]string{"key-a-file": "key-a\n", "key-b-file": "key-b\n"} {
		if err := os.WriteFile(filepath.Join(secretRoot, name), []byte(key), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	spaces := map[string]SpaceConfig{
		"space-a": {CredentialRef: "key-a-file"},
		"space-b": {CredentialRef: "key-b-file"},
	}
	jobsA := []JobConfig{
		{JobID: "job-a", Subpath: "/A", Target: TargetConfig{SpaceRef: "space-a", Prefix: prefixA}},
	}
	sources := []EBSSourceConfig{{VolumeID: "vol-001", NodeName: "node-a", Root: "/ebs/a", Jobs: jobsA}}
	if crossSource {
		sources = append(sources, EBSSourceConfig{VolumeID: "vol-002", NodeName: "node-b", Root: "/ebs/b", Jobs: []JobConfig{
			{JobID: "job-b", Subpath: "/B", Target: TargetConfig{SpaceRef: "space-b", Prefix: prefixB}},
		}})
	} else {
		sources[0].Jobs = append(sources[0].Jobs, JobConfig{
			JobID: "job-b", Subpath: "/B", Target: TargetConfig{SpaceRef: "space-b", Prefix: prefixB},
		})
	}
	cfg := &Config{
		Version: ConfigVersion, Drive9: Drive9Config{Endpoint: server.URL},
		JobDefaults: JobDefaults{
			Sync:        SyncDefaults{GracePeriod: Duration(DefaultGracePeriod)},
			Performance: PerformanceDefaults{MaxBytesPerSecond: 1024, SmallFileWorkers: 1, LargeFileWorkers: 1},
		},
		Spaces: spaces, EBSSources: sources,
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	runtime := &RuntimeStartup{Config: cfg, Source: sources[0], Phase: PhaseSyncing, targetCredentials: make(map[string]CredentialSource)}
	for spaceRef, space := range spaces {
		credential, err := NewCredentialSource(secretRoot, space.CredentialRef)
		if err != nil {
			t.Fatal(err)
		}
		runtime.targetCredentials[spaceRef] = credential
	}
	for _, configured := range sources[0].Jobs {
		job := resolveJob(sources[0], configured)
		runtime.Jobs = append(runtime.Jobs, &Startup{Config: cfg, Job: job, Space: spaces[job.Target.SpaceRef], Phase: PhaseSyncing})
	}
	return runtime, server.Close
}

func TestValidateTargetPrefixUsesDrive9PathRules(t *testing.T) {
	for _, prefix := range []string{"/bad\\path", "/bad\x01path", "/dot/../path"} {
		if _, err := validateTargetPrefix(prefix); err == nil {
			t.Fatalf("invalid Drive9 prefix %q was accepted", prefix)
		}
	}
}

func TestValidateMappingsAllowsSameNodeLocalMountPathOnDifferentNodes(t *testing.T) {
	cfg := mappingConfig(t, `  - volume_id: vol-001
    node_name: node-a
    source: {type: ebs, root: /ebs/volume}
    target: {space_ref: space-001, prefix: /one}
  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/volume}
    target: {space_ref: space-001, prefix: /two}
`)
	if err := ValidateMappings(cfg); err != nil {
		t.Fatalf("node-local mount paths were treated as globally unique: %v", err)
	}
}

func preflightStartup(t *testing.T, endpoint, root string) *Startup {
	t.Helper()
	body := strings.Replace(validConfigYAML, "https://drive9.example.com", endpoint, 1)
	if body == validConfigYAML {
		t.Fatal("preflight endpoint substitution did not apply")
	}
	withRoot := strings.Replace(body, "/ebs/001", root, 1)
	if withRoot == body {
		t.Fatal("preflight Source Root substitution did not apply")
	}
	body = withRoot
	configPath := writeConfig(t, body)
	secretRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(secretRoot, "space-001-key"), []byte("owner-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	startup, err := LoadStartup(configPath, "node-a", string(PhaseSyncing), secretRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	startup.acceptedTenantID = "tenant-a"
	startup.mountProbe = testMountedSourceProbe
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
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true,"event_ingest":false}}`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{{Name: ".drive9-migration", IsDir: true}}})
		case r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/checkpoint.json"):
			http.NotFound(w, r)
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
	if !result.RequiredCapabilities || result.EventReportingAvailable || result.RecoveryControlPresent || !result.TargetEmpty || result.ControlPrefix != ControlPrefix {
		t.Fatalf("preflight capabilities/control=%+v", result)
	}
	if result.MaxUploadBytes != 1048576 || result.InlineThreshold != 1024 || statusHits.Load() != 1 || listHits.Load() != 1 || mutations.Load() != 0 {
		t.Fatalf("limits/status/list/mutations=%+v %d/%d/%d", result, statusHits.Load(), listHits.Load(), mutations.Load())
	}
	if result.RegularFileCount != 1 || result.LargestFileBytes != 3 || result.InlineFileCount != 1 || result.MultipartFileCount != 0 || result.SmallFileRatio != 1 {
		t.Fatalf("file distribution=%+v", result)
	}
}

func TestPreflightLargeScaleProbesManifestContractBeforeTargetList(t *testing.T) {
	for _, tc := range []struct {
		name         string
		manifestBody string
		wantErr      bool
		wantListHits int32
	}{
		{name: "complete", manifestBody: `{"entries":[],"next_cursor":"","done":true}`, wantListHits: 1},
		{name: "complete null cursor", manifestBody: `{"entries":[],"next_cursor":null,"done":true}`, wantListHits: 1},
		{name: "malformed", manifestBody: `{"entries":[],"next_cursor":""}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "file"), []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			var manifestHits, listHits, mutations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
					_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
				case r.Method == http.MethodGet && r.URL.Path == "/v1/migration/manifest":
					manifestHits.Add(1)
					if r.URL.Query().Get("prefix") != "/" || r.URL.Query().Get("limit") != "1" {
						t.Fatalf("manifest query = %v", r.URL.Query())
					}
					_, _ = w.Write([]byte(tc.manifestBody))
				case r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/checkpoint.json"):
					http.NotFound(w, r)
				case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
					listHits.Add(1)
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{{Name: ".drive9-migration", IsDir: true}}})
				default:
					mutations.Add(1)
					http.Error(w, "unexpected", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			startup := preflightStartup(t, server.URL, root)
			startup.LargeScale = true
			_, err := preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil })
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%t", err, tc.wantErr)
			}
			if manifestHits.Load() != 1 || listHits.Load() != tc.wantListHits || mutations.Load() != 0 {
				t.Fatalf("hits manifest/list/mutation = %d/%d/%d", manifestHits.Load(), listHits.Load(), mutations.Load())
			}
		})
	}
}

func TestPreflightRejectsAuthenticatedTenantMismatchBeforeTargetAccess(t *testing.T) {
	root := t.TempDir()
	var targetHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-b","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
			return
		}
		targetHits.Add(1)
		http.Error(w, "unexpected target access", http.StatusInternalServerError)
	}))
	defer server.Close()

	startup := preflightStartup(t, server.URL, root)
	startup.acceptedTenantID = "tenant-a"
	_, err := Preflight(context.Background(), startup)
	if !errors.Is(err, ErrTargetIdentityMismatch) {
		t.Fatalf("error = %v, want ErrTargetIdentityMismatch", err)
	}
	if targetHits.Load() != 0 {
		t.Fatalf("tenant mismatch made %d target requests", targetHits.Load())
	}
}

func TestPreflightRequiresBatchAcceptedTenantIdentity(t *testing.T) {
	root := t.TempDir()
	var targetHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
			return
		}
		targetHits.Add(1)
		http.Error(w, "unexpected target access", http.StatusInternalServerError)
	}))
	defer server.Close()

	startup := preflightStartup(t, server.URL, root)
	startup.acceptedTenantID = ""
	_, err := Preflight(context.Background(), startup)
	if !errors.Is(err, ErrTargetIdentityMismatch) || targetHits.Load() != 0 {
		t.Fatalf("error = %v, target hits = %d", err, targetHits.Load())
	}
}

func TestPreflightSourceIdentityIsRequiredByWorkerConstruction(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "source")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
		case r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/checkpoint.json"):
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_, _ = w.Write([]byte(`{"entries":[]}`))
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	startup := preflightStartup(t, server.URL, root)
	if _, err := Preflight(context.Background(), startup); err != nil {
		t.Fatal(err)
	}
	if !startup.acceptedSource.present() {
		t.Fatal("preflight did not retain the accepted Source Root identity")
	}
	replaceSourceRootWithEmptyDirectory(t, root)
	if _, err := NewWorker(context.Background(), startup); !errors.Is(err, ErrSourceMountChanged) {
		t.Fatalf("Worker accepted a different post-preflight Source Root: %v", err)
	}
}

func TestPreflightRejectsSourceIdentityChangeDuringValidation(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "source")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
		case r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/checkpoint.json"):
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_, _ = w.Write([]byte(`{"entries":[]}`))
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	startup := preflightStartup(t, server.URL, root)
	changed := false
	_, err := preflightWithProbe(
		context.Background(),
		startup,
		func(root, _ string) (sourceMountIdentity, error) { return observeSourceRoot(root) },
		func(sourceRoot *os.Root, relative string) (*os.File, error) {
			file, openErr := openRootSourceFile(sourceRoot, relative)
			if openErr == nil && !changed {
				changed = true
				replaceSourceRootWithEmptyDirectory(t, root)
			}
			return file, openErr
		},
	)
	if !changed || !errors.Is(err, ErrPreflight) || !errors.Is(err, ErrSourceMountChanged) {
		t.Fatalf("Source replacement changed=%v error=%v", changed, err)
	}
	if startup.acceptedSource.present() {
		t.Fatalf("failed preflight retained Source identity: %+v", startup.acceptedSource)
	}
}

func TestPreflightRejectsUnreadableAndOversizedRegularFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	var remoteHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteHits.Add(1)
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":2,"inline_threshold":1,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()
	startup := preflightStartup(t, server.URL, root)

	_, err := preflightWithChecks(context.Background(), startup,
		func(string, string) (bool, error) { return true, nil },
		func(*os.Root, string) (*os.File, error) { return nil, os.ErrPermission })
	if !errors.Is(err, ErrPreflight) || !strings.Contains(err.Error(), "read access") || remoteHits.Load() != 0 {
		t.Fatalf("unreadable error=%v remote_hits=%d", err, remoteHits.Load())
	}

	_, err = preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil })
	if !errors.Is(err, ErrPreflight) || !strings.Contains(err.Error(), "max_upload_bytes") || remoteHits.Load() != 1 {
		t.Fatalf("oversized error=%v remote_hits=%d", err, remoteHits.Load())
	}
}

func TestPreflightRegularToFIFORaceDoesNotBlock(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "file")
	if err := os.WriteFile(name, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	var remoteHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		remoteHits.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()
	startup := preflightStartup(t, server.URL, root)
	swapped := false
	err := runSourceOperationWithoutFIFOBlock(t, name, func() error {
		_, preflightErr := preflightWithChecks(
			context.Background(),
			startup,
			func(string, string) (bool, error) { return true, nil },
			func(sourceRoot *os.Root, relative string) (*os.File, error) {
				if !swapped {
					swapped = true
					if removeErr := os.Remove(name); removeErr != nil {
						return nil, removeErr
					}
					if fifoErr := unix.Mkfifo(name, 0o600); fifoErr != nil {
						return nil, fifoErr
					}
				}
				return openRootSourceFile(sourceRoot, relative)
			},
		)
		return preflightErr
	})
	if !swapped || !errors.Is(err, ErrPreflight) || !errors.Is(err, ErrSourceChanged) || remoteHits.Load() != 0 {
		t.Fatalf("FIFO race swapped=%v error=%v remote_hits=%d", swapped, err, remoteHits.Load())
	}
}

func TestPreflightAllowsNonEmptyTargetOnlyForMatchingCheckpoint(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	var checkpointBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{{Name: "business", IsDir: false}, {Name: ".drive9-migration", IsDir: true}}})
		case r.URL.Path == "/v1/fs/.drive9-migration/jobs/vol-001/checkpoint.json" && r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
		case r.URL.Path == "/v1/fs/.drive9-migration/jobs/vol-001/checkpoint.json" && r.Method == http.MethodGet:
			_, _ = w.Write(checkpointBody)
		default:
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	startup := preflightStartup(t, server.URL, root)
	checkpoint := checkpointFromStartup(startup)
	body, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	checkpointBody = body
	result, err := preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.TargetEmpty || !result.RecoveryControlPresent {
		t.Fatalf("restart preflight=%+v", result)
	}
	startup.Phase = PhaseDualWriteRepairing
	result, err = preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil })
	if err != nil || result.TargetEmpty || !result.RecoveryControlPresent {
		t.Fatalf("T0 rollout restart preflight=%+v err=%v", result, err)
	}
	startup.Phase = PhaseCutoverReady
	if _, err := preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil }); !errors.Is(err, ErrPreflight) || !errors.Is(err, ErrInvalidPhase) || !strings.Contains(err.Error(), "DUAL_WRITE_REPAIRING") {
		t.Fatalf("cutover request from SYNCING error=%v", err)
	}
	checkpoint.HighestPhase = PhaseDualWriteRepairing
	checkpointBody, err = json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	result, err = preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil })
	if err != nil || result.TargetEmpty || !result.RecoveryControlPresent {
		t.Fatalf("cutover rollout restart preflight=%+v err=%v", result, err)
	}
	startup.Phase = PhaseSyncing

	checkpoint.ConfigHash = "other-config"
	checkpointBody, err = json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preflightWithVerifier(context.Background(), startup, func(string, string) (bool, error) { return true, nil }); !errors.Is(err, ErrPreflight) || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("mismatched checkpoint error=%v", err)
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
					_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": "tenant-a", "max_upload_bytes": 10, "inline_threshold": 5, "migration_capabilities": caps})
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
			_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":10,"inline_threshold":5,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true}}`))
			return
		}
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/checkpoint.json") {
			http.NotFound(w, r)
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
	startup.Config.EBSSources = append(startup.Config.EBSSources, EBSSourceConfig{
		VolumeID: "vol-002", NodeName: "node-a", Root: "/ebs/002",
		Jobs: []JobConfig{{
			JobID: "vol-002-root", Subpath: "/",
			Target: TargetConfig{SpaceRef: "space-001", Prefix: "/sub"},
		}},
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

func TestPlanRetainsAllJobResultsOnPartialFailure(t *testing.T) {
	root := t.TempDir()
	for _, subpath := range []string{"A", "B"} {
		if err := os.Mkdir(filepath.Join(root, subpath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, subpath, "file"), []byte(subpath), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			tenantID := "tenant-a"
			if r.Header.Get("Authorization") == "Bearer owner-key-b" {
				tenantID = "tenant-b"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": tenantID, "max_upload_bytes": 1048576, "inline_threshold": 1024,
				"migration_capabilities": map[string]bool{
					"checksum_read": true, "checksum_complete": true,
					"conditional_create": true, "conditional_update": true,
				},
			})
		case r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/checkpoint.json"):
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			if r.Header.Get("Authorization") == "Bearer owner-key-b" {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{{Name: ".drive9-migration", IsDir: true}}})
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	body := strings.Replace(validV4ConfigYAML, "https://drive9.example.com", server.URL, 1)
	body = strings.Replace(body, "/ebs/001", root, 1)
	secretRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(secretRoot, "space-a-key"), []byte("owner-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secretRoot, "space-b-key"), []byte("owner-key-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime, err := LoadRuntimeStartup(writeConfig(t, body), "node-a", string(PhaseSyncing), secretRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range runtime.Jobs {
		job.mountProbe = testMountedSourceProbe
	}
	result, err := Plan(context.Background(), runtime)
	if !errors.Is(err, ErrPlanFailed) {
		t.Fatalf("plan error=%v", err)
	}
	if len(result.Jobs) != 2 || result.Jobs[0].Result == nil || result.Jobs[0].Error != "" || result.Jobs[1].Result != nil || result.Jobs[1].Error == "" {
		t.Fatalf("plan=%+v", result)
	}
}
