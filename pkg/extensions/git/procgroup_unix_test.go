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

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
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
					_, _, _ = b.query(context.Background(), oid, false)
				} else {
					_, _, _ = b.query(context.Background(), oid, true)
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
		_, size, err := b.query(t.Context(), invalid, true)
		require.Error(t, err)
		require.Negative(t, size)
		_, _, err = b.query(t.Context(), invalid, false)
		require.Error(t, err)
		data, size, err := b.query(t.Context(), blob, true)
		require.NoError(t, err)
		require.EqualValues(t, 4, size)
		require.Equal(t, "blob", string(data))
		_, size, err = b.query(t.Context(), blob, false)
		require.NoError(t, err)
		require.EqualValues(t, 4, size)
	}
}

// Exercise the real handle read path. The first process accepts a query and
// stalls. Each caller can cancel independently, and the surviving caller
// reads the requested object without Stop or a new mount.
func TestGitReadCancellationIsolatedAcrossConcurrentCallers(t *testing.T) {
	for _, cancelFirst := range []bool{true, false} {
		t.Run(strconv.FormatBool(cancelFirst), func(t *testing.T) {
			dir := t.TempDir()
			ready := filepath.Join(dir, "ready")
			release := filepath.Join(dir, "release")
			t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
			t.Setenv("READER_READY", ready)
			t.Setenv("READER_RELEASE", release)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "git"), []byte(`#!/bin/sh
while read -r oid; do
  if mkdir "$READER_READY" 2>/dev/null; then
    while [ ! -f "$READER_RELEASE" ]; do sleep 0.05; done
  fi
  printf '%s blob 4\nblob\n' "$oid"
done
`), 0o755))
			g := &Git{localRoot: dir}
			t.Cleanup(func() { _ = g.Stop(context.Background()) })
			w := &workspace{root: "/repo", id: dir}
			require.NoError(t, os.MkdirAll(g.gitDirFor(w.root), 0o755))
			sha := strings.Repeat("a", 40)
			read := func(ctx context.Context) <-chan error {
				ch := make(chan error, 1)
				go func() {
					h := &gitHandle{g: g, w: w, rel: "f", treeSHA: sha}
					buf := make([]byte, 4)
					n, err := h.Read(ctx, 0, buf)
					if err == nil && (n != 4 || string(buf) != "blob") {
						err = errors.New("incorrect blob")
					}
					ch <- err
				}()
				return ch
			}
			first, stopFirst := context.WithCancel(t.Context())
			defer stopFirst()
			second, stopSecond := context.WithCancel(t.Context())
			defer stopSecond()
			left := read(first)
			require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 3*time.Second, 5*time.Millisecond)
			right := read(second)
			victim, survivor := right, left
			if cancelFirst {
				victim, survivor = left, right
				stopFirst()
			} else {
				stopSecond()
			}
			select {
			case err := <-victim:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("cancelled read remained blocked")
			}
			if !cancelFirst {
				require.NoError(t, os.WriteFile(release, nil, 0o644))
			}
			select {
			case err := <-survivor:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("live reader failed to finish")
			}
		})
	}
}

func TestCanceledGetAttrDoesNotCacheZeroSize(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	t.Setenv("READER_READY", ready)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "git"), []byte(`#!/bin/sh
while read -r oid; do
  if mkdir "$READER_READY" 2>/dev/null; then exec sleep 60; fi
  printf '%s blob 4\n' "$oid"
done
`), 0o755))
	w := &workspace{root: "/repo", id: dir, tree: map[string]client.GitTreeNode{"f": {Kind: "file", ObjectSHA: strings.Repeat("a", 40), SizeBytes: -1}}}
	g := &Git{localRoot: dir, loaded: true, workspaces: []*workspace{w}}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	require.NoError(t, os.MkdirAll(g.gitDirFor(w.root), 0o755))
	attr := func(ctx context.Context) (fs.Result, error) {
		return g.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/f"}, func(context.Context, *fs.Op) (fs.Result, error) {
			return fs.Result{}, errors.New("unexpected fallback")
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := attr(ctx); done <- err }()
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 3*time.Second, time.Millisecond*5)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled getattr remained blocked")
	}
	node, ok := w.treeLookup("f")
	require.True(t, ok)
	require.EqualValues(t, -1, node.SizeBytes)
	next, err := attr(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 4, next.Attr.Size)
}
