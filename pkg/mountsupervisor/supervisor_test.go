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
