//go:build !windows

package mountsupervisor

import (
	"bufio"
	"os/exec"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestStopWorkerPreservesShutdownFailure(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		wantCode     int
	}{
		{"clean", `trap 'exit 0' TERM; printf 'ready\n'; while :; do sleep 0.02; done`, 0},
		{"nonzero", `trap 'exit 7' TERM; printf 'ready\n'; while :; do sleep 0.02; done`, 7},
		{"forced", `trap '' TERM; printf 'ready\n'; exec sleep 30`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
			cmd := exec.Command("sh", "-c", tc.script)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waitCh := make(chan exitResult, 1)
			done := make(chan struct{})
			go func() {
				err := cmd.Wait()
				waitCh <- exitResult{err: err, code: cmd.ProcessState.ExitCode()}
				close(done)
			}()
			t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
			if ready, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || ready != "ready\n" {
				t.Fatalf("readiness=%q, %v", ready, err)
			}
			creation, err := mountstate.ProcessCreationTime(cmd.Process.Pid)
			if err != nil {
				t.Fatal(err)
			}
			timeout := time.Second
			if tc.name == "forced" {
				timeout = 50 * time.Millisecond
			}
			s := &supervisor{cfg: applyDefaults(Config{MountPoint: t.TempDir(), StopTimeout: timeout}), workerCmd: cmd, workerWait: waitCh, state: mountstate.SupervisorState{WorkerPID: cmd.Process.Pid, WorkerCreation: creation}}
			s.stopWorker()
			s.stopWorker() // shutdownClean must not discard the first result.
			if (s.stopErr != nil) != (tc.wantCode != 0) {
				t.Fatalf("stop error=%v", s.stopErr)
			}
			rec, _, err := mountstate.ReadExitReason(s.cfg.MountPoint)
			if tc.wantCode != 0 && (err != nil || rec.Code != tc.wantCode || rec.PID != cmd.Process.Pid || rec.CreationTime != creation) {
				t.Fatalf("exit=%+v, err=%v", rec, err)
			}
			if err := s.shutdownClean(); (err != nil) != (tc.wantCode != 0) {
				t.Fatalf("shutdownClean error=%v", err)
			}
		})
	}
}
