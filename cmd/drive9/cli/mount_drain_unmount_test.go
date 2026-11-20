//go:build !windows

package cli

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// drainTestDeps builds umount deps that answer the pre-unmount drain from the
// given state/response without touching a real mount.
func drainTestDeps(state mountstate.ProcessState, resp *mountcontrol.DrainResponse, drainErr error, calls *int) umountDeps {
	deps := defaultUmountDeps()
	deps.goos = "linux"
	deps.readProcessState = func(string) (mountstate.ProcessState, string, error) {
		return state, "", nil
	}
	deps.readPID = func(string) (int, string, error) { return 0, "", os.ErrNotExist }
	deps.requestDrain = func(_ context.Context, socket string, timeout time.Duration) (*mountcontrol.DrainResponse, error) {
		*calls++
		if socket != state.ControlSocket {
			return nil, errors.New("unexpected socket " + socket)
		}
		if timeout <= 0 {
			return nil, errors.New("non-positive drain timeout")
		}
		return resp, drainErr
	}
	deps.run = func([]string) error { return nil }
	deps.printErrf = func(string, ...any) {}
	deps.packAfterUnmount = nil
	deps.readExitReason = func(string) (mountstate.ExitReason, string, error) {
		return mountstate.ExitReason{}, "", os.ErrNotExist
	}
	return deps
}

func okDrainResponse(mp string, duration time.Duration) *mountcontrol.DrainResponse {
	start := time.Now()
	resp := mountcontrol.NewDrainResponse(mp, start)
	resp.Finish(start.Add(duration))
	return &resp
}

func TestDrainMountBeforeUnmountSendsRequest(t *testing.T) {
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/drive9-test.sock"}
	calls := 0
	deps := drainTestDeps(state, okDrainResponse(mp, 5*time.Millisecond), nil, &calls)
	if err := drainMountBeforeUnmount(deps, mp, state, nil, 2*time.Second); err != nil {
		t.Fatalf("drain = %v", err)
	}
	if calls != 1 {
		t.Fatalf("drain requests = %d, want 1", calls)
	}
}

func TestDrainMountBeforeUnmountReportsPending(t *testing.T) {
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/drive9-test.sock"}
	resp := okDrainResponse(mp, time.Millisecond)
	resp.Fail("pending_work_remaining", "/big.bin", errors.New("12 entries remain"))
	resp.Pending.DirtyHandles = 3
	resp.Pending.CommitQueuePending = 9
	calls := 0
	deps := drainTestDeps(state, resp, nil, &calls)
	err := drainMountBeforeUnmount(deps, mp, state, nil, time.Second)
	if err == nil {
		t.Fatal("drain reported no error for pending work")
	}
	for _, want := range []string{"pending_work_remaining", "dirty_handles=3", "commit_queue=9"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
}

func TestDrainMountBeforeUnmountReportsTransportFailure(t *testing.T) {
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/drive9-test.sock"}
	calls := 0
	deps := drainTestDeps(state, nil, errors.New("dial: connection refused"), &calls)
	err := drainMountBeforeUnmount(deps, mp, state, nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("drain error = %v, want the transport failure", err)
	}
}

func TestDrainMountBeforeUnmountWithoutSocketWarns(t *testing.T) {
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE}
	calls := 0
	deps := drainTestDeps(state, okDrainResponse(mp, 0), nil, &calls)
	err := drainMountBeforeUnmount(deps, mp, state, nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "control socket") {
		t.Fatalf("drain error = %v, want the missing-socket error", err)
	}
	if calls != 0 {
		t.Fatalf("drain requests = %d, want 0", calls)
	}
}

func TestDrainMountBeforeUnmountSkipsNonFuseMount(t *testing.T) {
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindWebDAV, ControlSocket: "/tmp/drive9-test.sock"}
	calls := 0
	deps := drainTestDeps(state, okDrainResponse(mp, 0), nil, &calls)
	if err := drainMountBeforeUnmount(deps, mp, state, nil, time.Second); err != nil {
		t.Fatalf("drain = %v", err)
	}
	if calls != 0 {
		t.Fatalf("drain requests = %d, want 0 for a non-FUSE mount", calls)
	}
}

func TestDrainMountBeforeUnmountNoProcessState(t *testing.T) {
	mp := t.TempDir()
	calls := 0
	deps := defaultUmountDeps()
	deps.goos = "linux"
	deps.readProcessState = func(string) (mountstate.ProcessState, string, error) {
		return mountstate.ProcessState{}, "", os.ErrNotExist
	}
	deps.readPID = func(string) (int, string, error) { return 0, "", os.ErrNotExist }
	deps.requestDrain = func(context.Context, string, time.Duration) (*mountcontrol.DrainResponse, error) {
		calls++
		return nil, nil
	}
	if err := drainMountBeforeUnmount(deps, mp, mountstate.ProcessState{}, os.ErrNotExist, time.Second); err != nil {
		t.Fatalf("drain = %v, want nil for a gone mount", err)
	}
	if calls != 0 {
		t.Fatalf("drain requests = %d, want 0", calls)
	}
}

// TestRunUmountRequireDrainAbortsBeforeStopping proves the strict mode never
// stops the writer: the error surfaces before the stop token exists.
func TestRunUmountRequireDrainAbortsBeforeStopping(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/drive9-test.sock"}
	resp := okDrainResponse(mp, time.Millisecond)
	resp.Fail("timeout", "", errors.New("drain timed out with 4 entries remaining"))
	calls := 0
	deps := drainTestDeps(state, resp, nil, &calls)
	deps.run = func([]string) error {
		t.Fatal("must not unmount when the drain cannot finish under --require-drain")
		return nil
	}
	deps.terminate = func(int, time.Duration) error {
		t.Fatal("must not stop the mount process when the drain cannot finish")
		return nil
	}
	deps.terminateState = func(mountstate.ProcessState, time.Duration) error {
		t.Fatal("must not stop the mount process when the drain cannot finish")
		return nil
	}
	err := runUmount([]string{"--no-auto-pack", "--require-drain", "--timeout", "1s", mp}, deps)
	if err == nil || !strings.Contains(err.Error(), "refusing to unmount") {
		t.Fatalf("runUmount error = %v, want the require-drain refusal", err)
	}
	if calls != 1 {
		t.Fatalf("drain requests = %d, want 1", calls)
	}
	if mountstate.StopTokenPresent(mp) {
		t.Fatal("stop token written before the drain verdict")
	}
}

// TestRunUmountDrainFailureWarnsAndContinues keeps the default behavior: the
// unmount proceeds, with a warning, when the drain cannot finish.
func TestRunUmountDrainFailureWarnsAndContinues(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/drive9-test.sock"}
	resp := okDrainResponse(mp, time.Millisecond)
	resp.Fail("timeout", "", errors.New("drain timed out"))
	calls := 0
	deps := drainTestDeps(state, resp, nil, &calls)
	var warnings []string
	deps.printErrf = func(format string, args ...any) {
		warnings = append(warnings, format)
	}
	// Let the unmount path complete without a real mount.
	stubMountStillActiveAfterUmount(t, func(string) bool { return false })
	deps.run = func([]string) error { return nil }
	// --timeout 0 skips the process-exit wait; this test only cares that the
	// unmount proceeds after the drain warning.
	if err := runUmount([]string{"--no-auto-pack", "--timeout", "0", mp}, deps); err != nil {
		t.Fatalf("runUmount = %v, want the unmount to continue", err)
	}
	if calls != 1 {
		t.Fatalf("drain requests = %d, want 1", calls)
	}
	if len(warnings) == 0 || !strings.Contains(warnings[0], "warning:") {
		t.Fatalf("warnings = %v, want a drain warning", warnings)
	}
}

func TestRunUmountNoDrainFlagSkipsRequest(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	state := mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/drive9-test.sock"}
	calls := 0
	deps := drainTestDeps(state, okDrainResponse(mp, 0), nil, &calls)
	stubMountStillActiveAfterUmount(t, func(string) bool { return false })
	if err := runUmount([]string{"--no-auto-pack", "--drain=false", "--timeout", "0", mp}, deps); err != nil {
		t.Fatalf("runUmount = %v", err)
	}
	if calls != 0 {
		t.Fatalf("drain requests = %d, want 0 with --drain=false", calls)
	}
}

// TestRunUmountDrainSocketSurvivesSupervisedState covers the supervised shape:
// the worker writes no process state, so the socket comes from the supervisor's
// record.
func TestRunUmountDrainSocketSurvivesSupervisedState(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	state := mountstate.ProcessState{
		PID:           4242,
		Role:          mountstate.RoleSupervisor,
		MountKind:     mountstate.MountKindFUSE,
		ControlSocket: mountstate.ControlSocketPath(mp),
	}
	calls := 0
	deps := drainTestDeps(state, okDrainResponse(mp, 0), nil, &calls)
	if err := drainMountBeforeUnmount(deps, mp, state, nil, time.Second); err != nil {
		t.Fatalf("drain = %v", err)
	}
	if calls != 1 {
		t.Fatalf("drain requests = %d, want 1", calls)
	}
}
