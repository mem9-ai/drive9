//go:build !windows

package mountsupervisor

import (
	"os"
	"os/exec"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// startStandInWorker starts a process that outlives the previous supervisor and
// returns its identity plus a reaper the caller may use to end it early.
func startStandInWorker(t *testing.T) (pid int, creation uint64, reap func()) {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start stand-in worker: %v", err)
	}
	creation, err := mountstate.ProcessCreationTime(cmd.Process.Pid)
	if err != nil || creation == 0 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Skip("platform has no process creation metadata")
	}
	pid = cmd.Process.Pid
	reaped := false
	reap = func() {
		if reaped {
			return
		}
		reaped = true
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	t.Cleanup(reap)
	return pid, creation, reap
}

func writePriorSupervisorState(t *testing.T, mp string, pid int, creation uint64) {
	t.Helper()
	if err := mountstate.WriteSupervisorState(mp, mountstate.SupervisorState{
		PID:            os.Getpid(),
		MountPoint:     mp,
		State:          mountstate.SupervisorStateRunning,
		WorkerPID:      pid,
		WorkerCreation: creation,
	}); err != nil {
		t.Fatalf("write prior supervisor state: %v", err)
	}
	t.Cleanup(func() { _ = mountstate.ClearSupervisorState(mp) })
}

func newTestSupervisor(t *testing.T, mp string) *supervisor {
	t.Helper()
	s, err := newSupervisor(Config{
		MountPoint: mp,
		WorkerArgs: []string{"mount", "--foreground", mp},
	})
	if err != nil {
		t.Fatalf("newSupervisor: %v", err)
	}
	t.Cleanup(func() { s.unlock() })
	return s
}

// A supervisor that died leaves its worker serving the FUSE endpoint. A later
// mount on the same path must adopt that worker: persisting a worker-less state
// first would erase the live ownership record, and the pre-start clean would
// then force-unmount a healthy mount while a second process shared its staging
// directory.
func TestNewSupervisorAdoptsLiveWorkerFromPriorSupervisor(t *testing.T) {
	mp := t.TempDir()
	pid, creation, _ := startStandInWorker(t)
	writePriorSupervisorState(t, mp, pid, creation)

	s := newTestSupervisor(t, mp)

	if !s.adopted {
		t.Fatal("a live worker from a dead supervisor must be adopted, not cleaned")
	}
	if !s.autoAdopted {
		t.Fatal("implicit adoption must be recorded as auto-adopted")
	}
	if s.state.WorkerPID != pid || s.state.WorkerCreation != creation {
		t.Fatalf("adopted worker identity = (%d,%d), want (%d,%d)",
			s.state.WorkerPID, s.state.WorkerCreation, pid, creation)
	}

	persisted, _, err := mountstate.ReadSupervisorState(mp)
	if err != nil {
		t.Fatalf("read persisted state: %v", err)
	}
	if persisted.WorkerPID != pid || persisted.WorkerCreation != creation {
		t.Fatalf("persisted worker identity = (%d,%d), want (%d,%d): the live owner record was clobbered",
			persisted.WorkerPID, persisted.WorkerCreation, pid, creation)
	}
	if !processAlive(pid, creation) {
		t.Fatal("adoption must not stop the live worker")
	}
}

// A recorded worker that is already gone must not be adopted: the clean path has
// to run so a stale FUSE endpoint is detached instead of monitored forever.
func TestNewSupervisorDoesNotAdoptDeadWorker(t *testing.T) {
	mp := t.TempDir()
	pid, creation, reap := startStandInWorker(t)
	writePriorSupervisorState(t, mp, pid, creation)
	reap()

	if processAlive(pid, creation) {
		t.Fatalf("stand-in worker %d is still alive after reap", pid)
	}

	s := newTestSupervisor(t, mp)
	if s.adopted || s.autoAdopted {
		t.Fatal("a dead worker must not be adopted")
	}
	if s.state.WorkerPID != 0 {
		t.Fatalf("WorkerPID = %d, want 0 for a fresh spawn", s.state.WorkerPID)
	}
}
