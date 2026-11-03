//go:build darwin

package fuse_test

// Local macFUSE mount test: reproduces the fuse-smoke [8.1] git config
// lockfile sequence (create EXCL lock → write → close → rename) against a
// real kernel mount over the in-memory fstest backend, so mount-path
// deadlocks surface with a goroutine dump under `go test -timeout`.

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fuse"
)

func waitForMount(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// In-process statfs: a macfuse fs reports a drive9 fstypename once
		// the kernel mount is live. Avoids `mount` (getfsstat), which any
		// other wedged fuse mount on the machine would block.
		var st unix.Statfs_t
		if err := unix.Statfs(dir, &st); err == nil {
			name := strings.TrimRight(string(st.Fstypename[:]), "\x00")
			if strings.Contains(name, "drive9") || strings.Contains(strings.ToLower(name), "fuse") {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second)
	t.Fatalf("mount did not come up on %s within 15s", dir)
}

func runCmd(t *testing.T, dir string, timeout time.Duration, name string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("%s timed out after %s", name, timeout)
	}
	return string(out), err
}

func TestGitConfigLockfileOnMount(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()

	mountDir := t.TempDir()

	mountDone := make(chan error, 1)
	go func() {
		mountDone <- fuse.MountWithConfig(fuse.MountConfig{
			Server:     ts.URL,
			APIKey:     "test",
			MountPoint: mountDir,
			Debug:      true,
		})
	}()
	t.Cleanup(func() {
		_ = exec.Command("umount", "-f", mountDir).Run()
		select {
		case <-mountDone:
		case <-time.After(10 * time.Second):
			t.Errorf("mount did not exit after umount")
		}
	})
	waitForMount(t, mountDir)

	// The smoke's core write path: `printf x > f` then `cat f` must see the
	// bytes through a fresh open (flush commits, reads see them).
	if out, err := runCmd(t, mountDir, 10*time.Second, "sh", "-c", "printf create-abc > w.txt; cat w.txt"); err != nil {
		t.Fatalf("write+read: %v\n%s", err, out)
	} else if strings.TrimSpace(out) != "create-abc" {
		t.Fatalf("read after write = %q", out)
	}
	if n, ok := b.Node("/w.txt"); !ok || string(n.Data) != "create-abc" {
		t.Fatalf("backend = %q ok=%v", n.Data, ok)
	}
	// hardlink: `ln a b; cat b` must serve the shared content
	if out, err := runCmd(t, mountDir, 10*time.Second, "sh", "-c", "ln w.txt w2.txt; cat w2.txt"); err != nil {
		t.Fatalf("hardlink: %v\n%s", err, out)
	} else if strings.TrimSpace(out) != "create-abc" {
		t.Fatalf("hardlink cat = %q", out)
	}

	gp := mountDir
	if out, err := runCmd(t, gp, 30*time.Second, "git", "init", "gp"); err != nil {
		for p := range b.Snapshot() {
			t.Logf("backend path: %q", p)
		}
		t.Fatalf("git init: %v\n%s", err, out)
	}
	gp = filepath.Join(mountDir, "gp")
	if out, err := runCmd(t, gp, 15*time.Second, "git", "config", "core.filemode", "false"); err != nil {
		t.Fatalf("git config: %v\n%s", err, out)
	}
	if out, err := runCmd(t, gp, 15*time.Second, "git", "remote", "add", "origin", "https://github.com/mem9-ai/drive9.git"); err != nil {
		t.Fatalf("git remote add: %v\n%s", err, out)
	}
	out, err := runCmd(t, gp, 15*time.Second, "git", "config", "--get", "remote.origin.url")
	if err != nil {
		t.Fatalf("git config --get: %v\n%s", err, out)
	}
	if strings.TrimSpace(out) != "https://github.com/mem9-ai/drive9.git" {
		t.Fatalf("remote.origin.url = %q", out)
	}
}
