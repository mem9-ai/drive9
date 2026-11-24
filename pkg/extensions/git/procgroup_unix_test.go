//go:build unix

package git

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitForPIDGone(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d still alive after %v", pid, timeout)
}

// A git subprocess that spawns a shell + background grandchild holding both
// stdout and stderr open must return promptly when ctx is canceled and must
// reap the entire process group (leaving zero orphan grandchildren).
func TestNewGitCommandContextCancelReapsGrandchildHoldingPipes(t *testing.T) {
	gitBin(t)
	tmp := t.TempDir()
	pidFile := filepath.Join(tmp, "grandchild.pid")
	readyFile := filepath.Join(tmp, "ready")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aliasCmd := "!sh -c 'sleep 60 & echo $! > \"" + pidFile + "\"; : > \"" + readyFile + "\"; wait'"
	cmd := newGitCommandContext(ctx, "-c", "alias.hold="+aliasCmd, "hold")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyFile); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rawPID, err := os.ReadFile(pidFile)
	require.NoError(t, err, "grandchild pid file must be written before cancel")
	grandchildPID, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(grandchildPID, 0), "precondition: grandchild must be alive")

	cancelStart := time.Now()
	cancel()

	select {
	case runErr := <-done:
		require.Error(t, runErr)
		require.Less(t, time.Since(cancelStart), 2*time.Second)
	case <-time.After(2 * time.Second):
		_ = syscall.Kill(grandchildPID, syscall.SIGKILL)
		t.Fatal("cmd.Run blocked after ctx cancel while grandchild held stdout/stderr")
	}

	waitForPIDGone(t, grandchildPID, 2*time.Second)
}
