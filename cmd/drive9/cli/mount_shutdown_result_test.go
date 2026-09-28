//go:build !windows

package cli

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// Signals target only this test child; no real mount is created.
func TestRunUmountReportsForcedKill(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	child := exec.Command("sh", "-c", "trap '' TERM; printf 'ready\\n'; exec sleep 30")
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var childErr error
	go func() { childErr = child.Wait(); close(done) }()
	t.Cleanup(func() { _ = child.Process.Kill(); <-done })
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("child readiness: line=%q err=%v", line, err)
	}
	creation, err := mountstate.ProcessCreationTime(child.Process.Pid)
	if err != nil || creation == 0 {
		t.Fatalf("process creation time: %d, %v", creation, err)
	}
	mp := t.TempDir()
	if err := mountstate.WriteSupervisorState(mp, mountstate.SupervisorState{
		PID: child.Process.Pid, CreationTime: creation,
		MountPoint: mp, State: mountstate.SupervisorStateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = mountstate.ClearSupervisorState(mp)
		_ = mountstate.ClearStopToken(mp)
	})
	stubMountStillActiveAfterUmount(t, func(string) bool { return false })
	deps := defaultUmountDeps()
	deps.readProcessState = func(string) (mountstate.ProcessState, string, error) {
		return mountstate.ProcessState{PID: child.Process.Pid, Role: mountstate.RoleSupervisor, CreationTime: creation}, "", nil
	}
	deps.readPID = func(string) (int, string, error) { return 0, "", os.ErrNotExist }
	deps.run = func([]string) error { t.Fatal("must not invoke unmount helper on a real mount"); return nil }
	started := time.Now()
	err = runUmount([]string{"--timeout", "100ms", "--no-auto-pack", mp}, deps)
	<-done
	if childErr == nil {
		t.Fatal("child unexpectedly exited successfully")
	}
	status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child exit = %v, want SIGKILL", childErr)
	}
	if !errors.Is(err, errMountForcedStop) {
		t.Fatalf("forced shutdown error = %v", err)
	}
	t.Logf("runUmount returned %v in %s, child exit=%v", err, time.Since(started), childErr)
}

func TestUnmountWorkerExitErrorIdentity(t *testing.T) {
	requested := time.Now()
	for _, tc := range []struct {
		name string
		rec  mountstate.ExitReason
		want bool
	}{
		{"clean", mountstate.ExitReason{PID: 7, CreationTime: 11, At: requested}, false},
		{"force_quit", mountstate.ExitReason{PID: 7, CreationTime: 11, At: requested, Code: 1, Reason: "signal", Detail: "force quit"}, true},
		{"drain_failed", mountstate.ExitReason{PID: 7, CreationTime: 11, At: requested, Code: 1, Reason: "drain_failed"}, true},
		{"panic", mountstate.ExitReason{PID: 7, CreationTime: 11, At: requested, Code: 2}, true},
		{"legacy_worker", mountstate.ExitReason{PID: 7, At: requested, Code: 1}, true},
		{"old_record", mountstate.ExitReason{PID: 7, CreationTime: 11, At: requested.Add(-time.Second), Code: 1}, false},
		{"successor", mountstate.ExitReason{PID: 8, CreationTime: 12, At: requested, Code: 1}, false},
		{"reused_pid", mountstate.ExitReason{PID: 7, CreationTime: 12, At: requested, Code: 1}, false},
		{"missing_pid", mountstate.ExitReason{At: requested, Code: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := unmountWorkerExitError(tc.rec, 7, 11, requested)
			if (err != nil) != tc.want {
				t.Fatalf("error=%v, want failure=%t", err, tc.want)
			}
		})
	}
}

func TestRunUmountReportsWorkerFailureBeforePacking(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	mp := t.TempDir()
	pid := spawnUmountStateChild(t, mp)
	creation, err := mountstate.ProcessCreationTime(pid)
	if err != nil {
		t.Fatal(err)
	}
	stubMountStillActiveAfterUmount(t, func(string) bool { return false })
	deps := defaultUmountDeps()
	deps.readProcessState = func(string) (mountstate.ProcessState, string, error) {
		return mountstate.ProcessState{PID: pid, WorkerPID: pid, CreationTime: creation}, "", nil
	}
	deps.readPID = func(string) (int, string, error) { return 0, "", os.ErrNotExist }
	deps.readExitReason = func(string) (mountstate.ExitReason, string, error) {
		return mountstate.ExitReason{PID: pid, CreationTime: creation, At: time.Now(), Code: 1, Reason: "drain_failed", Detail: "uncommitted data"}, "", nil
	}
	deps.run = func([]string) error { t.Fatal("unexpected unmount helper"); return nil }
	deps.packAfterUnmount = func(_ context.Context, _ mountstate.ProcessState, _ []string, _ []string) error {
		t.Fatal("must not pack after failed shutdown")
		return nil
	}
	err = runUmount([]string{"--timeout", "1s", mp}, deps)
	if err == nil || !strings.Contains(err.Error(), "drain_failed") {
		t.Fatalf("error=%v", err)
	}
}

func TestRunUmountSuccessorPreservesOldWorkerExit(t *testing.T) {
	for _, scenario := range []string{"failed", "clean", "successor_record", "stale_record", "missing", "unreadable", "no_wait"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
			mp := t.TempDir()
			child := exec.Command("true")
			if err := child.Run(); err != nil {
				t.Fatal(err)
			}
			pid := child.ProcessState.Pid()
			if processAliveImpl(pid) {
				t.Skip("child PID was reused")
			}
			oldLock := tryExclusiveReadyTimeout
			tryExclusiveReadyTimeout = func(string, func() error) (bool, error) { return false, nil }
			t.Cleanup(func() { tryExclusiveReadyTimeout = oldLock; _ = mountstate.ClearStopToken(mp) })
			deps := defaultUmountDeps()
			reads := 0
			deps.readProcessState = func(string) (mountstate.ProcessState, string, error) {
				reads++
				if reads > 1 {
					t.Fatal("must not read successor process state")
				}
				return mountstate.ProcessState{PID: pid, CreationTime: 11}, "", nil
			}
			deps.readExitReason = func(string) (mountstate.ExitReason, string, error) {
				rec := mountstate.ExitReason{PID: pid, CreationTime: 11, At: time.Now(), Code: 1, Reason: "drain_failed"}
				switch scenario {
				case "no_wait":
					t.Fatal("no-wait unmount must not read an incomplete exit record")
				case "clean":
					rec.Code = 0
				case "successor_record":
					rec.CreationTime++
				case "stale_record":
					rec.At = time.Now().Add(-time.Minute)
				case "missing":
					return rec, "", os.ErrNotExist
				case "unreadable":
					return rec, "", os.ErrPermission
				}
				return rec, "", nil
			}
			deps.run = func([]string) error { t.Fatal("must not detach successor"); return nil }
			deps.packAfterUnmount = func(context.Context, mountstate.ProcessState, []string, []string) error {
				t.Fatal("must not pack successor")
				return nil
			}
			timeout := "1s"
			if scenario == "no_wait" {
				timeout = "0"
			}
			err := runUmount([]string{"--no-auto-pack", "--timeout", timeout, mp}, deps)
			wantError := scenario == "failed" || scenario == "unreadable"
			if (err != nil) != wantError {
				t.Fatalf("umount error = %v, want error=%t", err, wantError)
			}
		})
	}
}
