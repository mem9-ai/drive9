//go:build darwin

package fuse_test

import (
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fuse"
)

// TestReaddirThenMkdirMissingName reproduces the supervised-mount phantom:
// after a READDIRPLUS populates the namespace cache, looking up a missing
// name must stay ENOENT and mkdir must succeed (EC2 us-west-2 repro: LOOKUP
// returned a positive zero attr -> mkdir EEXIST).
func TestReaddirThenMkdirMissingName(t *testing.T) {
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
		}
	})
	waitForMount(t, mountDir)

	// 1. readdir first (populates the namespace cache), like the supervisor
	// readiness probe on Linux.
	if out, err := runCmd(t, mountDir, 10*time.Second, "ls", "-a", mountDir); err != nil {
		t.Fatalf("ls: %v\n%s", err, out)
	}

	// 2. mkdir a fresh name must succeed.
	if out, err := runCmd(t, mountDir, 10*time.Second, "mkdir", filepath.Join(mountDir, "d1")); err != nil {
		t.Fatalf("mkdir d1 after readdir: %v\n%s", err, out)
	}
	// 3. create a fresh file must succeed too.
	if out, err := runCmd(t, mountDir, 10*time.Second, "sh", "-c", "printf hi > "+mountDir+"/fresh.txt"); err != nil {
		t.Fatalf("create fresh.txt after readdir: %v\n%s", err, out)
	}
}
