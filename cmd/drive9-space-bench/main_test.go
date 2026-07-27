package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCLIHelpDoesNotExposeCredentialEnvironment(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	writeBenchConfigFile(
		t,
		filepath.Join(home, ".drive9", "bench", "config.json"),
		`{
  "tidbcloud_public_key": "config-public-super-secret",
  "tidbcloud_private_key": "config-private-super-secret"
}`,
		0o600,
	)
	var stdout, stderr bytes.Buffer
	code := runCLI(
		context.Background(),
		[]string{"--help"},
		func(key string) string {
			switch key {
			case envPublicKey:
				return "public-super-secret"
			case envPrivateKey:
				return "private-super-secret"
			default:
				return ""
			}
		},
		func() (string, error) { return home, nil },
		&stdout,
		&stderr,
	)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	got := stderr.String()
	for _, want := range []string{
		"tidbcloud-public-key",
		"tidbcloud-private-key",
		"tidbcloud-spending-limit",
		"spaces-file",
		"delete-every",
		"config",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("help missing %q:\n%s", want, got)
		}
	}
	for _, secret := range []string{
		"public-super-secret",
		"private-super-secret",
		"config-public-super-secret",
		"config-private-super-secret",
	} {
		if strings.Contains(got, secret) {
			t.Fatalf("help exposed %q:\n%s", secret, got)
		}
	}
}
