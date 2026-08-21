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

func TestAdoptSupervisorCommandArgsIncludesMountKind(t *testing.T) {
	args := adoptSupervisorCommandArgs("/mnt/obj", "/tmp/obj.log", "https://s", `["s3://b/","/mnt/obj"]`, 42, 7, mountstate.MountKindObject)
	kind := ""
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--mount-kind" {
			kind = args[i+1]
		}
	}
	if kind != mountstate.MountKindObject {
		t.Fatalf("args %v: --mount-kind=%q", args, kind)
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

func TestFinishOwnedGenerationCleanupSkipsSuccessorEnsureUmount(t *testing.T) {
	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	succ := mountstate.SupervisorState{
		PID:            self,
		CreationTime:   ct,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, succ); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })
	succTS, err := mountstate.WriteStopTokenReceipt(mp, "umount")
	if err != nil {
		t.Fatal(err)
	}
	oldReceipt := succTS.Add(-time.Second)

	cleaned := false
	oldClean := ensureCleanAfterReadyTimeout
	oldLock := tryExclusiveReadyTimeout
	ensureCleanAfterReadyTimeout = func(string) (bool, error) {
		cleaned = true
		return true, nil
	}
	tryExclusiveReadyTimeout = func(_ string, fn func() error) (bool, error) {
		return false, nil
	}
	t.Cleanup(func() {
		ensureCleanAfterReadyTimeout = oldClean
		tryExclusiveReadyTimeout = oldLock
	})

	owned := finishOwnedGenerationCleanup(mp, self+1, ct+1, oldReceipt, func() {
		_, _ = ensureCleanAfterReadyTimeout(mp)
	})
	if owned {
		t.Fatal("successor-held lock must report not owned")
	}
	if cleaned {
		t.Fatal("ensure/umount tail must not detach successor mount")
	}
	got, ok := mountstate.ReadStopTokenTime(mp)
	if !ok || !got.Equal(succTS) {
		t.Fatalf("successor token cleared: ok=%v ts=%v", ok, got)
	}

	cleaned = false
	tryExclusiveReadyTimeout = func(_ string, fn func() error) (bool, error) {
		return true, fn()
	}
	if err := mountstate.WriteSupervisorState(mp, succ); err != nil {
		t.Fatal(err)
	}
	succTS2, err := mountstate.WriteStopTokenReceipt(mp, "ensure")
	if err != nil {
		t.Fatal(err)
	}
	owned = finishOwnedGenerationCleanup(mp, self+1, ct+1, oldReceipt, func() {
		_, _ = ensureCleanAfterReadyTimeout(mp)
	})
	if owned {
		t.Fatal("successor generation under lock must report not owned")
	}
	if cleaned {
		t.Fatal("must not detach successor when lock is free but state is B")
	}
	got, ok = mountstate.ReadStopTokenTime(mp)
	if !ok || !got.Equal(succTS2) {
		t.Fatalf("successor token cleared under lock: ok=%v ts=%v", ok, got)
	}
	if _, _, err := mountstate.ReadSupervisorState(mp); err != nil {
		t.Fatalf("successor state cleared: %v", err)
	}
}

func TestCleanupSupervisedReadyTimeoutDoesNotDetachSuccessor(t *testing.T) {
	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}

	// Successor supervisor state (different generation).
	succ := mountstate.SupervisorState{
		PID:            self,
		CreationTime:   ct,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, succ); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })

	// Timed-out generation A wrote a receipt, then successor B overwrote it
	// (or wrote a newer umount token) after taking the lock.
	oldReceipt := time.Now().UTC().Add(-time.Second)
	succTS, err := mountstate.WriteStopTokenReceipt(mp, "umount")
	if err != nil {
		t.Fatal(err)
	}
	if succTS.Equal(oldReceipt) {
		t.Fatal("need distinct receipts")
	}

	cleaned := false
	oldClean := ensureCleanAfterReadyTimeout
	oldLock := tryExclusiveReadyTimeout
	ensureCleanAfterReadyTimeout = func(string) (bool, error) {
		cleaned = true
		return true, nil
	}
	// Successor holds the flock.
	tryExclusiveReadyTimeout = func(string, func() error) (bool, error) {
		return false, nil
	}
	t.Cleanup(func() {
		ensureCleanAfterReadyTimeout = oldClean
		tryExclusiveReadyTimeout = oldLock
	})

	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	foreign := cmd.ProcessState.Pid()
	if foreign == self || processAliveImpl(foreign) {
		t.Skip("need a dead foreign pid")
	}

	// Post-kill tail only: A already wrote its token before B started.
	finishReadyTimeoutCleanup(mp, foreign, ct+999, oldReceipt)
	if cleaned {
		t.Fatal("ready-timeout cleanup must not detach successor mount")
	}
	got, ok := mountstate.ReadStopTokenTime(mp)
	if !ok || !got.Equal(succTS) {
		t.Fatalf("successor stop token cleared: ok=%v ts=%v want %v", ok, got, succTS)
	}
	if _, _, err := mountstate.ReadSupervisorState(mp); err != nil {
		t.Fatalf("successor supervisor state cleared: %v", err)
	}

	// Same assertions when TryExclusive succeeds but recorded state is B's
	// generation (lock-path mismatch / upgrade): still must not detach.
	cleaned = false
	tryExclusiveReadyTimeout = func(_ string, fn func() error) (bool, error) {
		return true, fn()
	}
	if err := mountstate.WriteSupervisorState(mp, succ); err != nil {
		t.Fatal(err)
	}
	succTS2, err := mountstate.WriteStopTokenReceipt(mp, "umount")
	if err != nil {
		t.Fatal(err)
	}
	finishReadyTimeoutCleanup(mp, foreign, ct+999, oldReceipt)
	if cleaned {
		t.Fatal("must not detach when lock is free but state is successor generation")
	}
	got, ok = mountstate.ReadStopTokenTime(mp)
	if !ok || !got.Equal(succTS2) {
		t.Fatalf("successor token cleared under acquired lock: ok=%v ts=%v", ok, got)
	}
}

func TestCleanupSupervisedReadyTimeoutIgnoresStaleWorker(t *testing.T) {
	// Stale SupervisorState for a different generation must not authorize worker kill.
	// We assert the pure generation gate + that cleanup does not panic / does not
	// require matching worker identity when generation mismatches.
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	stale := mountstate.SupervisorState{
		PID:            self,
		CreationTime:   ct,
		WorkerPID:      self,
		WorkerCreation: ct,
	}
	// Foreign supervisor PID (not self) → generation mismatch.
	if supervisorStateMatchesGeneration(stale, self+1, ct) {
		t.Fatal("stale state must not match foreign supervisor generation")
	}
	// Matching generation but missing worker creation → ready false (fail closed).
	noWorkerCT := stale
	noWorkerCT.WorkerCreation = 0
	if supervisedReadyStateMatches(noWorkerCT, self, ct) {
		t.Fatal("missing worker creation must fail closed for ready")
	}
	// cleanupSupervisedReadyTimeout with foreign pid must not treat stale as ours.
	mp := t.TempDir()
	if err := mountstate.WriteSupervisorState(mp, stale); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })
	// Use a dead foreign PID so we never signal ourselves as the "supervisor".
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	foreign := cmd.ProcessState.Pid()
	if foreign == self || processAliveImpl(foreign) {
		t.Skip("need a dead foreign pid")
	}
	// Should not kill self (stale worker identity) because generation does not match foreign.
	cleanupSupervisedReadyTimeout(mp, foreign, ct+999)
	// Stale state should remain (generation mismatch → not cleared as our generation).
	if _, _, err := mountstate.ReadSupervisorState(mp); err != nil {
		t.Fatalf("stale foreign-generation state should not be cleared: %v", err)
	}
	// Self still alive.
	if !processAliveImpl(self) {
		t.Fatal("cleanup must not have killed the test process")
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
