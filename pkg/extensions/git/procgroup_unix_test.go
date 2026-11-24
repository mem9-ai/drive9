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

func TestStopInterruptsBlockedCatFileAndClosesAdmission(t *testing.T) {
	for _, mode := range []string{"content", "size"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			gitPath := filepath.Join(dir, "git")
			require.NoError(t, os.WriteFile(gitPath, []byte("#!/bin/sh\nread -r oid || exit 1\necho $$ > \"$READER_PID\"\nexec sleep 60\n"), 0o755))
			pidFile := filepath.Join(dir, "reader.pid")
			t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
			t.Setenv("READER_PID", pidFile)
			g := &Git{}
			b := g.batchReaderFor(dir)
			done := make(chan struct{})
			go func() {
				defer close(done)
				oid := strings.Repeat("1", 40)
				if mode == "size" {
					b.sizeBlob(oid)
				} else {
					_, _, _ = b.readBlob(oid)
				}
			}()
			require.Eventually(t, func() bool { data, _ := os.ReadFile(pidFile); return len(data) > 1 }, 3*time.Second, time.Millisecond*10)
			raw, err := os.ReadFile(pidFile)
			require.NoError(t, err)
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			require.NoError(t, err)
			t.Cleanup(b.close)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			stop := make(chan error, 1)
			go func() { stop <- g.Stop(ctx) }()
			select {
			case err := <-stop:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("Stop blocked on cat-file I/O")
			}
			<-done
			waitForPIDGone(t, pid, time.Second)
			require.Nil(t, g.batchReaderFor(dir))
			require.Empty(t, g.batchReaders)
		})
	}
}

func TestBatchReaderRejectsInvalidAndNonBlobObjects(t *testing.T) {
	dir := t.TempDir()
	run(t, dir, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("blob"), 0o644))
	run(t, dir, "add", "f")
	tree := run(t, dir, "write-tree")
	blob := run(t, dir, "rev-parse", ":f")
	g := &Git{}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	b := g.batchReaderFor(filepath.Join(dir, ".git"))
	for _, invalid := range []string{"", blob + "\n" + blob, tree} {
		_, found, err := b.readBlob(invalid)
		require.Error(t, err)
		require.False(t, found)
		_, ok := b.sizeBlob(invalid)
		require.False(t, ok)
		data, found, err := b.readBlob(blob)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "blob", string(data))
		size, ok := b.sizeBlob(blob)
		require.True(t, ok)
		require.EqualValues(t, 4, size)
	}
}
