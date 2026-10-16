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

// TestCoreOnlyExtensionsMount verifies the --extensions core configuration
// (stage-1 bring-up): a minimal write-sync filesystem over the facade.
func TestCoreOnlyExtensionsMount(t *testing.T) {
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
			Extensions: []string{"core"},
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

	// core is write-sync: mkdir + write + read must round-trip.
	if out, err := runCmd(t, mountDir, 10*time.Second, "sh", "-c",
		"mkdir -p "+filepath.Join(mountDir, "d")+" && printf hello-stage1 > "+filepath.Join(mountDir, "d", "f.txt")+" && cat "+filepath.Join(mountDir, "d", "f.txt")); err != nil {
		t.Fatalf("core-only flow: %v\n%s", err, out)
	} else if out != "hello-stage1" {
		t.Fatalf("core-only read = %q", out)
	}
}
