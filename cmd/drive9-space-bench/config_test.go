package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	cfg, err := parseConfig([]string{"--server", "https://drive9.example.com"}, func(key string) string {
		switch key {
		case envPublicKey:
			return "env-public"
		case envPrivateKey:
			return "env-private"
		default:
			return ""
		}
	}, home, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}

	if cfg.Server != "https://drive9.example.com" {
		t.Fatalf("server = %q", cfg.Server)
	}
	if cfg.SpaceCount != 500 {
		t.Fatalf("space count = %d, want 500", cfg.SpaceCount)
	}
	if cfg.SpendingLimit != 10000 {
		t.Fatalf("spending limit = %d, want 10000", cfg.SpendingLimit)
	}
	if cfg.PublicKey != "env-public" || cfg.PrivateKey != "env-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
	if cfg.SpacesFile != filepath.Join(home, ".drive9", "bench", "spaces.json") {
		t.Fatalf("spaces file = %q", cfg.SpacesFile)
	}
	if cfg.Out != filepath.Join(home, ".drive9", "bench", "latest-report.json") {
		t.Fatalf("out = %q", cfg.Out)
	}
	if cfg.WorkersPerSpace != 1 || cfg.FilesPerWorker != 16 || cfg.FileSize != 4096 {
		t.Fatalf("workload defaults = workers:%d files:%d size:%d",
			cfg.WorkersPerSpace, cfg.FilesPerWorker, cfg.FileSize)
	}
	if cfg.DeleteEvery != 0 {
		t.Fatalf("delete every = %d, want 0", cfg.DeleteEvery)
	}
	if cfg.SpaceStartRPS != 0 {
		t.Fatalf("space start rps = %f, want 0", cfg.SpaceStartRPS)
	}
	if cfg.Duration != 0 || cfg.ReportInterval != 10*time.Second {
		t.Fatalf("timing defaults = duration:%s report:%s", cfg.Duration, cfg.ReportInterval)
	}
}

func TestParseConfigSpaceStartRPS(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig(
		[]string{
			"--server", "https://drive9.example.com",
			"--space-start-rps", "20",
		},
		func(string) string { return "" },
		t.TempDir(),
		io.Discard,
	)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.SpaceStartRPS != 20 {
		t.Fatalf("space start rps = %f, want 20", cfg.SpaceStartRPS)
	}
}

func TestParseConfigDeleteEvery(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig(
		[]string{
			"--server", "https://drive9.example.com",
			"--delete-every", "10",
		},
		func(string) string { return "" },
		t.TempDir(),
		io.Discard,
	)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.DeleteEvery != 10 {
		t.Fatalf("delete every = %d, want 10", cfg.DeleteEvery)
	}
}

func TestParseConfigLoadsDefaultConfigFile(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	configPath := filepath.Join(home, ".drive9", "bench", "config.json")
	writeBenchConfigFile(t, configPath, `{
  "server": "https://config.drive9.example.com/",
  "tidbcloud_public_key": "config-public",
  "tidbcloud_private_key": "config-private",
  "spaces": 600,
  "tidbcloud_spending_limit": 11000
}`, 0o600)

	cfg, err := parseConfig(nil, func(string) string { return "" }, home, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Server != "https://config.drive9.example.com" {
		t.Fatalf("server = %q", cfg.Server)
	}
	if cfg.PublicKey != "config-public" || cfg.PrivateKey != "config-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
	if cfg.SpaceCount != 600 {
		t.Fatalf("space count = %d, want 600", cfg.SpaceCount)
	}
	if cfg.SpendingLimit != 11000 {
		t.Fatalf("spending limit = %d, want 11000", cfg.SpendingLimit)
	}
}

func TestParseConfigEnvironmentOverridesConfigFile(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	configPath := filepath.Join(home, ".drive9", "bench", "config.json")
	writeBenchConfigFile(t, configPath, `{
  "server": "https://config.drive9.example.com",
  "tidbcloud_public_key": "config-public",
  "tidbcloud_private_key": "config-private",
  "spaces": 600,
  "tidbcloud_spending_limit": 11000
}`, 0o600)

	cfg, err := parseConfig(nil, func(key string) string {
		switch key {
		case envServer:
			return "https://env.drive9.example.com/"
		case envPublicKey:
			return "env-public"
		case envPrivateKey:
			return "env-private"
		default:
			return ""
		}
	}, home, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Server != "https://env.drive9.example.com" {
		t.Fatalf("server = %q", cfg.Server)
	}
	if cfg.PublicKey != "env-public" || cfg.PrivateKey != "env-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
	if cfg.SpaceCount != 600 || cfg.SpendingLimit != 11000 {
		t.Fatalf("file values = spaces:%d spending-limit:%d", cfg.SpaceCount, cfg.SpendingLimit)
	}
}

func TestParseConfigFlagsOverrideExplicitConfigAndEnvironment(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "custom-config.json")
	writeBenchConfigFile(t, configPath, `{
  "server": "https://config.drive9.example.com",
  "tidbcloud_public_key": "config-public",
  "tidbcloud_private_key": "config-private",
  "spaces": 600,
  "tidbcloud_spending_limit": 11000
}`, 0o600)

	cfg, err := parseConfig([]string{
		"--config", configPath,
		"--server", "https://flag.drive9.example.com/",
		"--tidbcloud-public-key", "flag-public",
		"--tidbcloud-private-key", "flag-private",
		"--spaces", "700",
		"--tidbcloud-spending-limit", "12000",
	}, func(key string) string {
		switch key {
		case envServer:
			return "https://env.drive9.example.com"
		case envPublicKey:
			return "env-public"
		case envPrivateKey:
			return "env-private"
		default:
			return ""
		}
	}, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Server != "https://flag.drive9.example.com" {
		t.Fatalf("server = %q", cfg.Server)
	}
	if cfg.PublicKey != "flag-public" || cfg.PrivateKey != "flag-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
	if cfg.SpaceCount != 700 {
		t.Fatalf("space count = %d, want 700", cfg.SpaceCount)
	}
	if cfg.SpendingLimit != 12000 {
		t.Fatalf("spending limit = %d, want 12000", cfg.SpendingLimit)
	}
}

func TestParseConfigRejectsMissingExplicitConfigFile(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing.json")
	_, err := parseConfig(
		[]string{"--config", missing, "--server", "https://drive9.example.com"},
		func(string) string { return "" },
		t.TempDir(),
		io.Discard,
	)
	if err == nil || !strings.Contains(err.Error(), "config file does not exist") {
		t.Fatalf("error = %v, want missing config error", err)
	}
}

func TestParseConfigRejectsInsecureConfigFileMode(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.json")
	writeBenchConfigFile(t, configPath, `{"server":"https://drive9.example.com"}`, 0o644)
	_, err := parseConfig(
		[]string{"--config", configPath},
		func(string) string { return "" },
		t.TempDir(),
		io.Discard,
	)
	if err == nil || !strings.Contains(err.Error(), "mode 0600") {
		t.Fatalf("error = %v, want secure mode error", err)
	}
}

func TestParseConfigRejectsNonRegularConfigFile(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config")
	if err := os.Mkdir(configPath, 0o700); err != nil {
		t.Fatalf("create config directory: %v", err)
	}
	_, err := parseConfig(
		[]string{"--config", configPath},
		func(string) string { return "" },
		t.TempDir(),
		io.Discard,
	)
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("error = %v, want regular file error", err)
	}
}

func TestParseConfigRejectsInvalidJSONShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown field", raw: `{"server":"https://drive9.example.com","spacez":500}`},
		{name: "multiple documents", raw: `{"server":"https://drive9.example.com"} {}`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			configPath := filepath.Join(t.TempDir(), "config.json")
			writeBenchConfigFile(t, configPath, tc.raw, 0o600)
			_, err := parseConfig(
				[]string{"--config", configPath},
				func(string) string { return "" },
				t.TempDir(),
				io.Discard,
			)
			if err == nil {
				t.Fatal("parseConfig succeeded, want JSON validation error")
			}
		})
	}
}

func TestParseConfigFlagsOverrideCredentialEnvironment(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--server", "https://drive9.example.com/",
		"--tidbcloud-public-key", "flag-public",
		"--tidbcloud-private-key", "flag-private",
		"--tidbcloud-spending-limit", "12000",
	}, func(key string) string {
		switch key {
		case envPublicKey:
			return "env-public"
		case envPrivateKey:
			return "env-private"
		default:
			return ""
		}
	}, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}

	if cfg.Server != "https://drive9.example.com" {
		t.Fatalf("server = %q", cfg.Server)
	}
	if cfg.PublicKey != "flag-public" || cfg.PrivateKey != "flag-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
	if cfg.SpendingLimit != 12000 {
		t.Fatalf("spending limit = %d", cfg.SpendingLimit)
	}
}

func writeBenchConfigFile(t *testing.T, path, raw string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create config directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(raw), mode); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod config file: %v", err)
	}
}

func TestParseConfigRejectsPartialCredentials(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--server", "https://drive9.example.com",
		"--tidbcloud-public-key", "only-public",
	}, func(string) string { return "" }, t.TempDir(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "both") {
		t.Fatalf("error = %v, want credential pair error", err)
	}
}

func TestParseConfigRejectsInvalidWorkloadValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "spaces", args: []string{"--spaces", "0"}, want: "spaces must be positive"},
		{name: "workers", args: []string{"--workers-per-space", "0"}, want: "workers-per-space must be positive"},
		{name: "files", args: []string{"--files-per-worker", "0"}, want: "files-per-worker must be positive"},
		{name: "file size", args: []string{"--file-size", "0"}, want: "file-size must be positive"},
		{name: "negative duration", args: []string{"--duration", "-1s"}, want: "duration must not be negative"},
		{name: "negative io rps", args: []string{"--io-rps", "-1"}, want: "io-rps must not be negative"},
		{name: "negative space start rps", args: []string{"--space-start-rps", "-1"}, want: "space-start-rps must not be negative"},
		{name: "negative delete every", args: []string{"--delete-every", "-1"}, want: "delete-every must not be negative"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			args := append([]string{"--server", "https://drive9.example.com"}, tc.args...)
			_, err := parseConfig(args, func(string) string { return "" }, t.TempDir(), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}
