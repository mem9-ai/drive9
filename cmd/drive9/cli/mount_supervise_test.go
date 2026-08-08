//go:build !windows

package cli

import (
	"os"
	"reflect"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestPeelSystemdUnitFlagsPassesMountFlags(t *testing.T) {
	install, name, rest, err := peelSystemdUnitFlags([]string{
		"--install",
		"--name", "my-drive9",
		"--mode=fuse",
		"--server", "https://example",
		":/repo",
		"/mnt/d9",
	})
	if err != nil {
		t.Fatalf("peelSystemdUnitFlags: %v", err)
	}
	if !install {
		t.Fatal("install = false, want true")
	}
	if name != "my-drive9" {
		t.Fatalf("name = %q, want my-drive9", name)
	}
	want := []string{"--mode=fuse", "--server", "https://example", ":/repo", "/mnt/d9"}
	if !reflect.DeepEqual(rest, want) {
		t.Fatalf("rest = %v, want %v", rest, want)
	}
}

func TestPeelSystemdUnitFlagsDoubleDash(t *testing.T) {
	_, _, rest, err := peelSystemdUnitFlags([]string{"--", "--install", "/mnt/d9"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// After --, --install is a mount arg, not the unit flag.
	want := []string{"--install", "/mnt/d9"}
	if !reflect.DeepEqual(rest, want) {
		t.Fatalf("rest = %v, want %v", rest, want)
	}
}

func TestInjectEnsureCredentials(t *testing.T) {
	got := injectEnsureCredentials([]string{"--mode=fuse", ":/r", "/m"}, "https://s", "sk-1")
	want := []string{"--api-key", "sk-1", "--server", "https://s", "--mode=fuse", ":/r", "/m"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Do not duplicate existing flags.
	got2 := injectEnsureCredentials([]string{"--server", "https://keep", ":/r", "/m"}, "https://s", "sk-1")
	want2 := []string{"--api-key", "sk-1", "--server", "https://keep", ":/r", "/m"}
	if !reflect.DeepEqual(got2, want2) {
		t.Fatalf("got2 %v, want %v", got2, want2)
	}
}

func TestWithEnsureCredentialEnvRestores(t *testing.T) {
	t.Setenv(EnvAPIKey, "old-key")
	t.Setenv(EnvVaultToken, "old-token")
	err := withEnsureCredentialEnv("https://s", "new-key", "", func() error {
		if os.Getenv(EnvAPIKey) != "new-key" {
			t.Fatalf("api key during fn = %q", os.Getenv(EnvAPIKey))
		}
		if _, ok := os.LookupEnv(EnvVaultToken); ok {
			t.Fatal("vault token should be unset when using api key")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv(EnvAPIKey) != "old-key" {
		t.Fatalf("api key restored = %q, want old-key", os.Getenv(EnvAPIKey))
	}
	if os.Getenv(EnvVaultToken) != "old-token" {
		t.Fatalf("token restored = %q, want old-token", os.Getenv(EnvVaultToken))
	}
}

func TestApplyScrubbedMountEnvUnsetsOmitted(t *testing.T) {
	t.Setenv(EnvTiDBCloudPublicKey, "pub")
	t.Setenv(EnvTiDBCloudPrivateKey, "priv")
	t.Setenv(EnvAPIKey, "old")
	t.Setenv(EnvServer, "https://old")
	t.Setenv(EnvVaultToken, "old-tok")
	scrubbed := mountBackgroundEnv(os.Environ(), mountBackgroundRequest{
		Server: "https://s",
		APIKey: "new",
	})
	applyScrubbedMountEnv(scrubbed)
	if _, ok := os.LookupEnv(EnvTiDBCloudPublicKey); ok {
		t.Fatal("public key should be unset after scrub")
	}
	if _, ok := os.LookupEnv(EnvTiDBCloudPrivateKey); ok {
		t.Fatal("private key should be unset after scrub")
	}
	if os.Getenv(EnvAPIKey) != "new" {
		t.Fatalf("api key = %q, want new", os.Getenv(EnvAPIKey))
	}
	if os.Getenv(EnvServer) != "https://s" {
		t.Fatalf("server = %q", os.Getenv(EnvServer))
	}
	if _, ok := os.LookupEnv(EnvVaultToken); ok {
		t.Fatal("vault token should be unset when using api key snapshot")
	}
}

func TestProcessMatchesIdentitySelf(t *testing.T) {
	pid := os.Getpid()
	// Must use the same source writers use (mountstate.ProcessCreationTime),
	// not CLI processCreationTimeByPID (differs on Darwin).
	ct, err := mountstate.ProcessCreationTime(pid)
	if err != nil {
		t.Fatalf("creation time: %v", err)
	}
	if ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	if !processMatchesIdentity(pid, ct) {
		t.Fatal("self identity should match")
	}
	if processMatchesIdentity(pid, ct+1) {
		t.Fatal("wrong creation should not match")
	}
	if processMatchesIdentity(0, 0) {
		t.Fatal("pid 0 should not match")
	}
}
