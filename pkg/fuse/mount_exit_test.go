package fuse

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestMountExitErrorExitCode(t *testing.T) {
	err := ExitServeAbnormalErr("boom")
	if err.ExitCode() != ExitServeAbnormal {
		t.Fatalf("ExitCode=%d want %d", err.ExitCode(), ExitServeAbnormal)
	}
	if err.Error() == "" {
		t.Fatal("empty error string")
	}
	if !errors.Is(err, err) {
		t.Fatal("self-is failed")
	}
}

func TestClassifyServeEndSignal(t *testing.T) {
	reason, detail := classifyServeEnd(true, "/tmp/not-a-real-mount")
	if reason != ExitReasonSignal {
		t.Fatalf("reason=%s want signal (%s)", reason, detail)
	}
}

func TestClassifyServeEndMissingPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gone")
	reason, _ := classifyServeEnd(false, dir)
	if reason != ExitReasonExternalUnmount && reason != ExitReasonServeAbnormal {
		// Missing path is external unmount; some platforms may report differently.
		t.Fatalf("reason=%s", reason)
	}
}

func TestClassifyServeEndLocalDirNotActive(t *testing.T) {
	dir := t.TempDir()
	reason, detail := classifyServeEnd(false, dir)
	// Local dir is typically not an active FUSE mount → external_unmount.
	if reason != ExitReasonExternalUnmount {
		t.Fatalf("reason=%s detail=%s want external_unmount", reason, detail)
	}
}

func TestEnsureCleanMountpointMissing(t *testing.T) {
	cleaned, err := EnsureCleanMountpoint(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if cleaned {
		t.Fatal("expected no clean on missing path")
	}
}

func TestEnsureCleanMountpointPlainDir(t *testing.T) {
	dir := t.TempDir()
	cleaned, err := EnsureCleanMountpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cleaned {
		t.Fatal("plain dir should not be cleaned")
	}
	// Ensure dir still exists.
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}

func TestIsTransportBrokenPermissionDenied(t *testing.T) {
	// After FUSE daemon SIGKILL some kernels surface EACCES instead of ENOTCONN.
	cases := []string{
		"permission denied",
		"Permission denied",
		"operation not permitted",
		"transport endpoint is not connected",
		"input/output error",
	}
	for _, msg := range cases {
		if !IsTransportBroken(errors.New(msg)) {
			t.Fatalf("IsTransportBroken(%q) = false, want true", msg)
		}
	}
	if IsTransportBroken(nil) {
		t.Fatal("IsTransportBroken(nil) = true, want false")
	}
	if IsTransportBroken(errors.New("file not found")) {
		t.Fatal("IsTransportBroken(ordinary error) = true, want false")
	}
}

func TestOwnerAliveSupervisorWithoutWorker(t *testing.T) {
	// Supervised mount with WorkerPID cleared must not report owner alive
	// just because the supervisor process is still running.
	mp := t.TempDir()
	st := mountstate.SupervisorState{
		PID:        os.Getpid(),
		WorkerPID:  0,
		MountPoint: mp,
		State:      mountstate.SupervisorStateRestarting,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })
	if ownerAlive(mp) {
		t.Fatal("ownerAlive with supervisor-only state should be false")
	}
}

func TestOwnerAliveWorkerIdentity(t *testing.T) {
	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	st := mountstate.SupervisorState{
		PID:            self + 1, // not used as FUSE owner
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })
	if !ownerAlive(mp) {
		t.Fatal("ownerAlive with matching worker identity should be true")
	}
	st.WorkerCreation = ct + 1
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	if ownerAlive(mp) {
		t.Fatal("ownerAlive with mismatched worker creation should be false")
	}
}

func TestEnsureCleanMountpointBoundedWhenProbeHangs(t *testing.T) {
	// Inject a hanging active-mount probe: cleanup must time out, not block forever.
	oldProbe := activeMountProbe
	oldTimeout := cleanupActiveMountTimeout
	activeMountProbe = func(string) (bool, error) {
		time.Sleep(30 * time.Second)
		return true, nil
	}
	cleanupActiveMountTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		activeMountProbe = oldProbe
		cleanupActiveMountTimeout = oldTimeout
	})

	dir := t.TempDir()
	start := time.Now()
	cleaned, err := EnsureCleanMountpoint(dir)
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("EnsureClean blocked for %s (want bounded ~timeout)", elapsed)
	}
	// Hanging probe → timeout error path → force unmount attempted.
	if !cleaned {
		t.Fatalf("expected cleaned=true on hang path, err=%v", err)
	}
	// force-unmount of a plain dir is best-effort; err may be non-nil if still "needs clean"
	// because the probe still hangs on the verification pass.
	_ = err
}

func TestActiveMountPointBoundedTimesOut(t *testing.T) {
	oldProbe := activeMountProbe
	oldTimeout := cleanupActiveMountTimeout
	activeMountProbe = func(string) (bool, error) {
		time.Sleep(5 * time.Second)
		return true, nil
	}
	cleanupActiveMountTimeout = 30 * time.Millisecond
	t.Cleanup(func() {
		activeMountProbe = oldProbe
		cleanupActiveMountTimeout = oldTimeout
	})
	start := time.Now()
	_, err := ActiveMountPointBounded(t.TempDir())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("bounded probe took %s", elapsed)
	}
}
