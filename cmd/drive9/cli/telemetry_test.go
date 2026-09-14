package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTelemetryPreferenceReadsConfig(t *testing.T) {
	for _, test := range []struct {
		name         string
		config       string
		writeConfig  bool
		wantEnabled  bool
		wantRecorded bool
	}{
		{name: "recorded enable", config: `{"telemetry":{"enabled":true}}`, writeConfig: true, wantEnabled: true, wantRecorded: true},
		{name: "recorded disable", config: `{"telemetry":{"enabled":false}}`, writeConfig: true, wantRecorded: true},
		{name: "absent section", config: `{"contexts":{}}`, writeConfig: true},
		{name: "empty section", config: `{"telemetry":{}}`, writeConfig: true},
		{name: "missing file"},
		{name: "malformed json", config: `{"telemetry":`, writeConfig: true},
		{name: "wrong value type", config: `{"telemetry":{"enabled":"yes"}}`, writeConfig: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			if test.writeConfig {
				writeCLITestConfig(t, home, test.config)
			}
			enabled, recorded := TelemetryPreference()
			if enabled != test.wantEnabled || recorded != test.wantRecorded {
				t.Fatalf("TelemetryPreference() = %t, %t; want %t, %t", enabled, recorded, test.wantEnabled, test.wantRecorded)
			}
		})
	}
}

// TestTelemetryPreferenceSurvivesConfigRewrites guards the footgun that makes
// the preference part of the Config struct: ctx mutations rewrite the whole
// document, and an unlisted field would be dropped, silently resetting the
// user's opt-in.
func TestTelemetryPreferenceSurvivesConfigRewrites(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeCLITestConfig(t, home, `{"current_context":"a","contexts":{"a":{"type":"owner","api_key":"k","server":"https://s"}},"telemetry":{"enabled":true}}`)

	cfg := loadConfig()
	cfg.Contexts["b"] = &Context{Type: PrincipalOwner, APIKey: "k2", Server: "https://s"}
	if err := saveConfig(cfg); err != nil {
		t.Fatal(err)
	}

	enabled, recorded := TelemetryPreference()
	if !recorded || !enabled {
		t.Fatalf("TelemetryPreference() = %t, %t after a config rewrite", enabled, recorded)
	}
	data, err := os.ReadFile(configPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"telemetry"`) {
		t.Fatalf("config rewrite dropped the telemetry preference: %s", data)
	}
}

func writeCLITestConfig(t *testing.T, home, content string) {
	t.Helper()
	dir := filepath.Join(home, ".drive9")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestTelemetryContextMetadataOmitsSecretsAndNames(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(EnvAPIKey, "")
	t.Setenv(EnvVaultToken, "")
	dir := filepath.Join(home, ".drive9")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		CurrentContext: "prod-secret-name",
		Contexts: map[string]*Context{
			"prod-secret-name": {
				Type:          PrincipalOwner,
				APIKey:        "drive9_must-not-leak",
				CloudProvider: "aws",
				Region:        "aws-ap-southeast-1",
			},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	provider, region, source := TelemetryContextMetadata()
	if provider != "aws" || region != "aws-ap-southeast-1" || source != "default" {
		t.Fatalf("provider=%q region=%q source=%q", provider, region, source)
	}
	joined := provider + region + source
	if strings.Contains(joined, "drive9_must-not-leak") || strings.Contains(joined, "prod-secret-name") {
		t.Fatalf("telemetry metadata leaked identity: %q %q %q", provider, region, source)
	}
}

func TestTelemetryContextMetadataUsesEnvSource(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(EnvAPIKey, "drive9_env_key")
	t.Setenv(EnvVaultToken, "")

	_, _, source := TelemetryContextMetadata()
	if source != "env" {
		t.Fatalf("source = %q, want env", source)
	}
}
