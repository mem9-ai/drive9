package migration

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validConfigYAML = `version: v3
drive9:
  endpoint: https://drive9.example.com
job_defaults:
  sync:
    grace_period: 60s
  performance:
    max_bytes_per_second: 1024
    small_file_workers: 2
    large_file_workers: 1
spaces:
  space-001:
    credential_ref: space-001-key
jobs:
  - volume_id: vol-001
    node_name: node-a
    source:
      type: ebs
      root: /ebs/001
    target:
      space_ref: space-001
      prefix: /
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigStrictAndDefaults(t *testing.T) {
	path := writeConfig(t, strings.Replace(validConfigYAML, "    grace_period: 60s\n", "", 1))
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Version != "v3" || cfg.Drive9.Endpoint != "https://drive9.example.com" {
		t.Fatalf("config = %+v", cfg)
	}
	if time.Duration(cfg.JobDefaults.Sync.GracePeriod) != time.Minute {
		t.Fatalf("default grace = %s", time.Duration(cfg.JobDefaults.Sync.GracePeriod))
	}
	if got, err := cfg.SelectJob("node-a"); err != nil || got.VolumeID != "vol-001" {
		t.Fatalf("selected job=%+v err=%v", got, err)
	}
}

func TestLoadConfigRejectsMalformedUnknownAndUnsupportedFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "malformed", body: "version: ["},
		{name: "unknown top level", body: validConfigYAML + "extra: true\n"},
		{name: "per-job override", body: strings.Replace(validConfigYAML, "    target:\n", "    grace_period: 30s\n    target:\n", 1)},
		{name: "wrong version", body: strings.Replace(validConfigYAML, "version: v3", "version: v2", 1)},
		{name: "multiple documents", body: validConfigYAML + "---\nversion: v3\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, tc.body)); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestLoadConfigRejectsDuplicateAndInvalidJobIdentity(t *testing.T) {
	duplicate := validConfigYAML + `  - volume_id: vol-001
    node_name: node-b
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /other}
`
	if _, err := LoadConfig(writeConfig(t, duplicate)); err == nil || !strings.Contains(err.Error(), "duplicate volume_id") {
		t.Fatalf("duplicate error = %v", err)
	}
	for _, volumeID := range []string{"", "disk-001", "vol-XYZ", "../vol-001"} {
		body := strings.Replace(validConfigYAML, "vol-001", volumeID, 1)
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Fatalf("invalid volume_id %q accepted", volumeID)
		}
	}
}

func TestSelectJobRequiresExactlyOneNodeMatch(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, validConfigYAML))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.SelectJob("missing"); err == nil {
		t.Fatal("missing node match accepted")
	}
	cfg.Jobs = append(cfg.Jobs, Job{
		VolumeID: "vol-002",
		NodeName: "node-a",
		Source:   SourceConfig{Type: "ebs", Root: "/ebs/002"},
		Target:   TargetConfig{SpaceRef: "space-001", Prefix: "/other"},
	})
	if _, err := cfg.SelectJob("node-a"); err == nil {
		t.Fatal("multiple node matches accepted")
	}
}

func TestReadStartupPhaseSourcesAndRollback(t *testing.T) {
	configPath := writeConfig(t, validConfigYAML)
	phasePath := filepath.Join(filepath.Dir(configPath), "phase")
	if err := os.WriteFile(phasePath, []byte("SYNCING\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if phase, err := ReadStartupPhase(configPath, "", ""); err != nil || phase != PhaseSyncing {
		t.Fatalf("file phase=%q err=%v", phase, err)
	}
	if _, err := ReadStartupPhase(configPath, string(PhaseSyncing), ""); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("dual source error=%v", err)
	}
	if err := os.Remove(phasePath); err != nil {
		t.Fatal(err)
	}
	if phase, err := ReadStartupPhase(configPath, string(PhaseDualWriteRepairing), ""); err != nil || phase != PhaseDualWriteRepairing {
		t.Fatalf("env phase=%q err=%v", phase, err)
	}
	if _, err := ReadStartupPhase(configPath, "", ""); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("missing source error=%v", err)
	}
	if _, err := ReadStartupPhase(configPath, "UNKNOWN", ""); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("unknown phase error=%v", err)
	}
	if _, err := ReadStartupPhase(configPath, string(PhaseSyncing), PhaseDualWriteRepairing); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("rollback error=%v", err)
	}
	if phase, err := ReadStartupPhase(configPath, string(PhaseDualWriteRepairing), PhaseDualWriteRepairing); err != nil || phase != PhaseDualWriteRepairing {
		t.Fatalf("idempotent phase=%q err=%v", phase, err)
	}
}

func TestCredentialSourceReloadsRotationAndDoesNotLeak(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "space-001-key")
	if err := os.WriteFile(path, []byte("first-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := NewCredentialSource(root, "space-001-key")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source.Read(); err != nil || got != "first-secret" {
		t.Fatalf("first read=%q err=%v", got, err)
	}
	if err := os.WriteFile(path, []byte("rotated-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := source.Read(); err != nil || got != "rotated-secret" {
		t.Fatalf("rotated read=%q err=%v", got, err)
	}
	for _, ref := range []string{"", ".", "../key", "dir/key"} {
		if _, err := NewCredentialSource(root, ref); err == nil {
			t.Fatalf("unsafe credential ref %q accepted", ref)
		}
	}
	missing, err := NewCredentialSource(root, "missing-key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.Read(); err == nil || strings.Contains(err.Error(), "first-secret") {
		t.Fatalf("missing credential error=%v", err)
	}
}

func TestLoadStartupHashExcludesPhaseAndSecret(t *testing.T) {
	configPath := writeConfig(t, validConfigYAML)
	secretRoot := t.TempDir()
	secretPath := filepath.Join(secretRoot, "space-001-key")
	if err := os.WriteFile(secretPath, []byte("secret-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := LoadStartup(configPath, "node-a", string(PhaseSyncing), secretRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secretPath, []byte("secret-two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := LoadStartup(configPath, "node-a", string(PhaseDualWriteRepairing), secretRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.ConfigHash != second.ConfigHash {
		t.Fatalf("hash changed with phase/key: %s != %s", first.ConfigHash, second.ConfigHash)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-one", "secret-two"} {
		if strings.Contains(string(encoded), secret) || strings.Contains(first.ConfigHash, secret) {
			t.Fatalf("startup snapshot leaked %q", secret)
		}
	}
}

func TestConfigHashCoversOnlySelectedEffectiveJob(t *testing.T) {
	extraJob := `  - volume_id: vol-002
    node_name: node-b
    source: {type: ebs, root: /ebs/002}
    target: {space_ref: space-001, prefix: /other}
`
	first, err := LoadConfig(writeConfig(t, validConfigYAML+extraJob))
	if err != nil {
		t.Fatal(err)
	}
	selected, err := first.SelectJob("node-a")
	if err != nil {
		t.Fatal(err)
	}
	firstHash, err := ConfigHash(first, selected)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadConfig(writeConfig(t, strings.Replace(validConfigYAML+extraJob, "/ebs/002", "/ebs/changed", 1)))
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := ConfigHash(second, selected)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatal("unrelated Job changed selected config_hash")
	}
	selected.Source.Root = "/ebs/changed"
	selectedHash, err := ConfigHash(second, selected)
	if err != nil {
		t.Fatal(err)
	}
	if selectedHash == secondHash {
		t.Fatal("selected Source change did not change config_hash")
	}
}

func TestLoadStartupRejectsMissingCredential(t *testing.T) {
	_, err := LoadStartup(writeConfig(t, validConfigYAML), "node-a", string(PhaseSyncing), t.TempDir(), "")
	if err == nil || strings.Contains(err.Error(), "Bearer") {
		t.Fatalf("missing credential error=%v", err)
	}
}
