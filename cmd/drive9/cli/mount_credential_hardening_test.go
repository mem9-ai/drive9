//go:build !windows

package cli

import (
	"os"
	"testing"
)

func TestScrubMountCredentialEnvironment(t *testing.T) {
	for _, name := range []string{"DRIVE9_API_KEY", "DRIVE9_VAULT_TOKEN", "DRIVE9_SERVER"} {
		t.Setenv(name, "must-not-remain")
	}
	if err := scrubMountCredentialEnvironment(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"DRIVE9_API_KEY", "DRIVE9_VAULT_TOKEN", "DRIVE9_SERVER"} {
		if _, exists := os.LookupEnv(name); exists {
			t.Fatalf("%s remains in mount process environment", name)
		}
	}
}
