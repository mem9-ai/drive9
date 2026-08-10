//go:build !windows

package cli

import (
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestInjectEnsureServerFlag(t *testing.T) {
	got := injectEnsureServerFlag([]string{"--mode=fuse", ":/r", "/m"}, "https://s")
	want := []string{"--server", "https://s", "--mode=fuse", ":/r", "/m"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Do not duplicate existing flags; never inject --api-key onto argv.
	got2 := injectEnsureServerFlag([]string{"--server", "https://keep", ":/r", "/m"}, "https://s")
	want2 := []string{"--server", "https://keep", ":/r", "/m"}
	if !reflect.DeepEqual(got2, want2) {
		t.Fatalf("got2 %v, want %v", got2, want2)
	}
}

func TestValidateSystemdUnitName(t *testing.T) {
	if err := validateSystemdUnitName("drive9-mount"); err != nil {
		t.Fatalf("valid name: %v", err)
	}
	if err := validateSystemdUnitName("../../evil"); err == nil {
		t.Fatal("expected path traversal reject")
	}
	if err := validateSystemdUnitName("a/b"); err == nil {
		t.Fatal("expected slash reject")
	}
}

func TestSystemdEscapePercents(t *testing.T) {
	if got, want := systemdEscapePercents("a%b%%c"), "a%%b%%%%c"; got != want {
		t.Fatalf("got %q, want %q", got, want)
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

func TestRunMountSuperviseAdoptRequiresCreation(t *testing.T) {
	mp := t.TempDir()
	cases := []struct {
		name string
		args []string
	}{
		{"no creation", []string{"--mountpoint", mp, "--adopt", "--adopt-worker-pid", "12345"}},
		{"zero creation", []string{"--mountpoint", mp, "--adopt", "--adopt-worker-pid", "12345", "--adopt-worker-creation", "0"}},
		{"no pid", []string{"--mountpoint", mp, "--adopt", "--adopt-worker-creation", "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runMountSupervise(tc.args)
			if err == nil {
				t.Fatal("expected adopt identity error")
			}
			if !strings.Contains(err.Error(), "adopt") {
				t.Fatalf("error = %v, want adopt mention", err)
			}
		})
	}
}

func TestSupervisedReadyStateMatches(t *testing.T) {
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	match := mountstate.SupervisorState{
		PID:            self,
		CreationTime:   ct,
		WorkerPID:      self,
		WorkerCreation: ct,
	}
	cases := []struct {
		name        string
		st          mountstate.SupervisorState
		supPID      int
		supCreation uint64
		want        bool
	}{
		{"match", match, self, ct, true},
		{"wrong-gen supervisor", func() mountstate.SupervisorState {
			s := match
			s.CreationTime = ct + 1
			return s
		}(), self, ct, false},
		{"missing supervisor creation", func() mountstate.SupervisorState {
			s := match
			s.CreationTime = 0
			return s
		}(), self, ct, false},
		{"stale supervisor pid", match, self + 1, ct, false},
		{"missing worker creation", func() mountstate.SupervisorState {
			s := match
			s.WorkerCreation = 0
			return s
		}(), self, ct, false},
		{"no worker", func() mountstate.SupervisorState {
			s := match
			s.WorkerPID = 0
			return s
		}(), self, ct, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := supervisedReadyStateMatches(tc.st, tc.supPID, tc.supCreation); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestWaitForSupervisedMountReadyIgnoresStaleState(t *testing.T) {
	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	// Stale state looks fully healthy for *this* process, but parent spawned a
	// different supervisor PID (foreign generation).
	st := mountstate.SupervisorState{
		PID:            self,
		CreationTime:   ct,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })

	// Finished child → non-live supervisorPID that will not match stale state.
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	foreignPID := cmd.ProcessState.Pid()
	if foreignPID == self || processAliveImpl(foreignPID) {
		t.Skip("could not obtain a dead foreign pid")
	}

	waitCh := make(chan error) // never closes
	err = waitForSupervisedMountReady(mp, foreignPID, waitCh, "/dev/null", 300*time.Millisecond)
	if err == nil {
		t.Fatal("stale healthy state must not satisfy ready for a different supervisor PID")
	}
	if !strings.Contains(err.Error(), "timed out waiting") {
		t.Fatalf("error = %v, want ready timeout", err)
	}
}
