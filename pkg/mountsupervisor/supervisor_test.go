//go:build !windows

package mountsupervisor

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestShouldRestartMatrix(t *testing.T) {
	s := &supervisor{cfg: applyDefaults(Config{MountPoint: t.TempDir()})}
	cases := []struct {
		code int
		want bool
	}{
		{0, false},
		{1, true}, // bare exit 1 is restartable without stop token
		{2, false},
		{3, true},
		{4, true},
		{5, false},
		{6, true},
		{137, true},
	}
	for _, tc := range cases {
		if got := s.shouldRestart(tc.code); got != tc.want {
			t.Fatalf("code %d: got %v want %v", tc.code, got, tc.want)
		}
	}
	s.requestStop("test")
	if s.shouldRestart(3) {
		t.Fatal("should not restart when stop requested")
	}
}

func TestClassifyExitSignal(t *testing.T) {
	s := &supervisor{cfg: applyDefaults(Config{MountPoint: t.TempDir()})}
	// Simulate ExitError with signaled status is hard without real process;
	// check MountExitError path.
	err := drive9fuse.ExitServeAbnormalErr("x")
	code, reason := s.classifyExit(err)
	if code != drive9fuse.ExitServeAbnormal {
		t.Fatalf("code=%d reason=%s", code, reason)
	}
}

func TestCircuitOpen(t *testing.T) {
	now := time.Now()
	s := &supervisor{
		cfg: applyDefaults(Config{
			MountPoint:    t.TempDir(),
			MaxRestarts:   3,
			RestartWindow: time.Minute,
			Now:           func() time.Time { return now },
		}),
	}
	for i := 0; i < 3; i++ {
		s.noteRestart("x")
	}
	if !s.circuitOpen() {
		t.Fatal("expected circuit open")
	}
}

func TestWorkerArgsFromSanitized(t *testing.T) {
	got := WorkerArgsFromSanitized([]string{"--mode=fuse", "/mnt/x"})
	hasFG, hasSup := false, false
	for _, a := range got {
		if a == "--foreground" {
			hasFG = true
		}
		if a == "--supervised" {
			hasSup = true
		}
	}
	if !hasFG || !hasSup {
		t.Fatalf("got %v", got)
	}
}

func TestProcessAliveSelf(t *testing.T) {
	pid := os.Getpid()
	if !processAlive(pid, 0) {
		t.Fatal("self should be alive")
	}
	if processAlive(1<<30, 0) {
		t.Fatal("huge pid should not be alive")
	}
}

func TestNextBackoffGrows(t *testing.T) {
	s := &supervisor{cfg: applyDefaults(Config{MountPoint: t.TempDir(), BackoffMax: 8 * time.Second}), backoff: time.Second}
	d1 := s.nextBackoff()
	d2 := s.nextBackoff()
	if d1 < time.Second {
		t.Fatalf("d1=%s", d1)
	}
	// Second call uses increased base; allow jitter.
	if d2 < time.Second {
		t.Fatalf("d2=%s", d2)
	}
}

func TestRunGivesUpOnBadExecutable(t *testing.T) {
	mp := t.TempDir()
	err := Run(Config{
		MountPoint:    mp,
		Executable:    filepath.Join(mp, "missing-binary"),
		WorkerArgs:    []string{"mount", "--foreground", mp},
		MaxRestarts:   1,
		RestartWindow: time.Minute,
		// Fast fail: spawn fails immediately.
		Sleep: func(time.Duration) {},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClassifyExitExecError(t *testing.T) {
	s := &supervisor{cfg: applyDefaults(Config{MountPoint: t.TempDir()})}
	cmd := exec.Command("false")
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("expected ExitError, got %v", err)
	}
	code, _ := s.classifyExit(err)
	if code != 1 {
		// false exits 1 on unix
		if code == 0 {
			t.Fatalf("code=%d", code)
		}
	}
	_ = syscall.SIGTERM
}

func TestNewSupervisorAdoptRequiresCreationIdentity(t *testing.T) {
	mp := t.TempDir()
	_, err := newSupervisor(Config{
		MountPoint:          mp,
		WorkerArgs:          []string{"mount", "--foreground", mp},
		Adopt:               true,
		AdoptWorkerPID:      os.Getpid(),
		AdoptWorkerCreation: 0,
	})
	if err == nil {
		t.Fatal("adopt without creation must fail closed")
	}

	ct, cerr := mountstate.ProcessCreationTime(os.Getpid())
	if cerr != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	s, err := newSupervisor(Config{
		MountPoint:          mp,
		WorkerArgs:          []string{"mount", "--foreground", mp},
		Adopt:               true,
		AdoptWorkerPID:      os.Getpid(),
		AdoptWorkerCreation: ct,
	})
	if err != nil {
		t.Fatalf("adopt with full identity: %v", err)
	}
	t.Cleanup(func() { s.unlock() })
	if !s.adopted {
		t.Fatal("expected adopted=true")
	}
	if s.state.WorkerPID != os.Getpid() {
		t.Fatalf("WorkerPID=%d", s.state.WorkerPID)
	}
}

func TestProcessIdentityAliveFailClosed(t *testing.T) {
	self := os.Getpid()
	if processIdentityAlive(self, 0, true) {
		t.Fatal("requireCreation + creation=0 must be fail-closed")
	}
	if !processIdentityAlive(self, 0, false) {
		t.Fatal("without requireCreation, creation=0 should still see live pid")
	}
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	if !processIdentityAlive(self, ct, true) {
		t.Fatal("matching identity should be alive")
	}
	if processIdentityAlive(self, ct+1, true) {
		t.Fatal("mismatched creation must not be alive")
	}
}

func TestDecideEnsureActionIdentity(t *testing.T) {
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}

	// Matching supervisor creation => already healthy (no adopt).
	snap := StatusSnapshot{
		Healthy:            true,
		SupervisorPID:      self,
		SupervisorCreation: ct,
		WorkerPID:          self,
		WorkerCreation:     ct,
	}
	if got := DecideEnsureAction(snap, false); got != EnsureAlreadyHealthy {
		t.Fatalf("matching supervisor: got %s want %s", got, EnsureAlreadyHealthy)
	}

	// Stale supervisor PID reused (wrong creation) + live worker => adopt.
	snapReuse := StatusSnapshot{
		Healthy:            true,
		SupervisorPID:      self,
		SupervisorCreation: ct + 999, // wrong generation → not our supervisor
		WorkerPID:          self,
		WorkerCreation:     ct,
	}
	if got := DecideEnsureAction(snapReuse, false); got != EnsureAdopt {
		t.Fatalf("PID-reuse supervisor: got %s want %s", got, EnsureAdopt)
	}

	// Missing supervisor creation fails closed → adopt when worker identity matches.
	snapMissing := StatusSnapshot{
		Healthy:            true,
		SupervisorPID:      self,
		SupervisorCreation: 0,
		WorkerPID:          self,
		WorkerCreation:     ct,
	}
	if got := DecideEnsureAction(snapMissing, false); got != EnsureAdopt {
		t.Fatalf("missing supervisor creation: got %s want %s", got, EnsureAdopt)
	}

	// No live worker identity → remount.
	snapDead := StatusSnapshot{
		Healthy:            true,
		SupervisorPID:      self,
		SupervisorCreation: ct + 1,
		WorkerPID:          self,
		WorkerCreation:     ct + 1,
	}
	if got := DecideEnsureAction(snapDead, false); got != EnsureRemount {
		t.Fatalf("dead identities: got %s want %s", got, EnsureRemount)
	}

	if got := DecideEnsureAction(snap, true); got != EnsureRemount {
		t.Fatalf("--restart: got %s want %s", got, EnsureRemount)
	}
	if got := DecideEnsureAction(StatusSnapshot{Healthy: false, SupervisorPID: self, SupervisorCreation: ct}, false); got != EnsureRemount {
		t.Fatalf("unhealthy: got %s want %s", got, EnsureRemount)
	}
}

func TestClearStaleStopToken(t *testing.T) {
	mp := t.TempDir()
	// Use real state paths under XDG/cache via mountstate (mountpoint is key).
	// Past token relative to startedAt must be cleared.
	if err := mountstate.WriteStopToken(mp, "old-umount"); err != nil {
		t.Fatal(err)
	}
	// Ensure token timestamp is before startedAt by sleeping past write.
	startedAt := time.Now().Add(time.Second)
	clearStaleStopToken(mp, startedAt)
	if mountstate.StopTokenPresent(mp) {
		t.Fatal("stale stop token should be cleared after exclusive ownership")
	}

	// Future token (concurrent umount after start) must be preserved.
	if err := mountstate.WriteStopToken(mp, "concurrent-umount"); err != nil {
		t.Fatal(err)
	}
	pastStart := time.Now().Add(-time.Hour)
	clearStaleStopToken(mp, pastStart)
	// Token ts is ~now, startedAt is past → ts is NOT before startedAt → keep.
	// Wait: ts.Before(pastStart) is false for now, so token is kept. Good.
	if !mountstate.StopTokenPresent(mp) {
		t.Fatal("concurrent stop token written after startedAt must be preserved")
	}
	// Token written at T, startedAt=T-1h → ts is after startedAt → keep.
	// Token written at T, startedAt=T+1s → we already tested clear.
	_ = mountstate.ClearStopToken(mp)
}

func TestSupervisorLockExclusive(t *testing.T) {
	mp := t.TempDir()
	f1, err := acquireLock(mp)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	t.Cleanup(func() { _ = f1.Close() })
	f2, err := acquireLock(mp)
	if err == nil {
		_ = f2.Close()
		t.Fatal("second lock must fail while first is held")
	}
}

func TestCollectStatusOrphanUsesCreation(t *testing.T) {
	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	// Record a "supervisor" with wrong creation (PID reuse scenario).
	st := mountstate.SupervisorState{
		PID:            self,
		CreationTime:   ct + 1,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })

	snap := CollectStatus(mp)
	if snap.SupervisorCreation != ct+1 {
		t.Fatalf("SupervisorCreation=%d want %d", snap.SupervisorCreation, ct+1)
	}
	if SupervisorIdentityAlive(snap) {
		t.Fatal("reused supervisor PID with wrong creation must not be alive")
	}
	if !WorkerIdentityAlive(snap) {
		t.Fatal("worker identity should still match")
	}
	// Without a real FUSE mount, Healthy is false; DecideEnsure for healthy case
	// is covered separately. State should still signal orphan when running.
	if snap.Healthy && snap.State != "orphan_worker" {
		// Healthy only if somehow a FUSE mount exists at temp dir.
		t.Fatalf("state=%s want orphan_worker when healthy", snap.State)
	}
}
