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

func TestIsTransportBrokenDefinitiveOnly(t *testing.T) {
	// Definitive disconnects only — not permission errors.
	for _, msg := range []string{
		"transport endpoint is not connected",
		"socket is not connected",
		"input/output error",
		"enotconn",
	} {
		if !IsTransportBroken(errors.New(msg)) {
			t.Fatalf("IsTransportBroken(%q) = false, want true", msg)
		}
	}
	for _, msg := range []string{
		"permission denied",
		"Permission denied",
		"operation not permitted",
		"eacces",
		"eperm",
		"file not found",
	} {
		if IsTransportBroken(errors.New(msg)) {
			t.Fatalf("IsTransportBroken(%q) = true, want false (must use owner gate)", msg)
		}
	}
	if IsTransportBroken(nil) {
		t.Fatal("IsTransportBroken(nil) = true, want false")
	}
}

func TestEnsureCleanMountpointPermissionDeniedWithLiveOwner(t *testing.T) {
	// EACCES/EPERM must not bypass ownerAlive (restricted but live mount).
	oldProbe := activeMountProbe
	activeMountProbe = func(string) (bool, error) {
		return false, errors.New("permission denied")
	}
	t.Cleanup(func() { activeMountProbe = oldProbe })

	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	st := mountstate.SupervisorState{
		PID:            self + 1,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })

	cleaned, err := EnsureCleanMountpoint(mp)
	if cleaned {
		t.Fatalf("EACCES with live owner must not force-unmount, cleaned=%v err=%v", cleaned, err)
	}
	if err != nil {
		t.Fatalf("err=%v, want nil", err)
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

func TestEnsureCleanMountpointTimeoutWithLiveOwner(t *testing.T) {
	// Slow-but-live owned mount must not be force-unmounted on probe timeout.
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

	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	st := mountstate.SupervisorState{
		PID:            self + 1,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })

	start := time.Now()
	cleaned, err := EnsureCleanMountpoint(mp)
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("EnsureClean blocked for %s", elapsed)
	}
	if cleaned {
		t.Fatalf("cleaned=true with live owner on timeout (would detach live mount), err=%v", err)
	}
	if err != nil {
		t.Fatalf("err=%v, want nil for live-owner timeout", err)
	}
}

func TestEnsureCleanMountpointTimeoutWithDeadOwner(t *testing.T) {
	// Orphan (no owner identity): timeout still forces detach path.
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
		t.Fatalf("EnsureClean blocked for %s", elapsed)
	}
	if !cleaned {
		t.Fatalf("expected cleaned=true for dead/missing owner on hang path, err=%v", err)
	}
	_ = err // verification re-probe may also time out
}

func TestEnsureCleanMountpointTransportBrokenWithLiveOwner(t *testing.T) {
	// Transport-broken endpoints detach even if owner identity is live.
	oldProbe := activeMountProbe
	activeMountProbe = func(string) (bool, error) {
		return false, errors.New("transport endpoint is not connected")
	}
	t.Cleanup(func() { activeMountProbe = oldProbe })

	mp := t.TempDir()
	self := os.Getpid()
	ct, err := mountstate.ProcessCreationTime(self)
	if err != nil || ct == 0 {
		t.Skip("platform has no process creation metadata")
	}
	st := mountstate.SupervisorState{
		PID:            self + 1,
		WorkerPID:      self,
		WorkerCreation: ct,
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
	}
	if err := mountstate.WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })

	cleaned, err := EnsureCleanMountpoint(mp)
	if !cleaned {
		t.Fatalf("transport-broken must force detach even with live owner, cleaned=%v err=%v", cleaned, err)
	}
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

func TestShouldForceUnmountAfterAbnormalServe(t *testing.T) {
	cases := []struct {
		active bool
		err    error
		want   bool
	}{
		{true, nil, true},
		{false, errors.New("transport endpoint is not connected"), true},
		{false, errors.New("active mount check timed out after 2s"), false},
		{false, os.ErrNotExist, false},
		{false, nil, false},
	}
	for _, tc := range cases {
		if got := shouldForceUnmountAfterAbnormalServe(tc.active, tc.err); got != tc.want {
			t.Fatalf("active=%v err=%v: got %v want %v", tc.active, tc.err, got, tc.want)
		}
	}
}

func TestClassifyServeEndBoundedWhenProbeHangs(t *testing.T) {
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

	start := time.Now()
	reason, _ := classifyServeEnd(false, t.TempDir())
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("classifyServeEnd blocked for %s", elapsed)
	}
	if reason != ExitReasonServeAbnormal {
		t.Fatalf("reason=%s want serve_abnormal (not external_unmount on indeterminate)", reason)
	}
}
