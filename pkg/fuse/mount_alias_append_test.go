//go:build darwin

package fuse_test

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fuse"
)

// TestHardlinkAliasWriteVisibility: a write through one hardlink alias must
// be visible through the other alias immediately (shared inode → shared
// kernel page cache).
func TestHardlinkAliasWriteVisibility(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	mountDir := t.TempDir()
	mountDone := make(chan error, 1)
	go func() {
		mountDone <- fuse.MountWithConfig(fuse.MountConfig{Server: ts.URL, APIKey: "test", MountPoint: mountDir, Debug: true})
	}()
	t.Cleanup(func() {
		_ = exec.Command("umount", "-f", mountDir).Run()
		select {
		case <-mountDone:
		case <-time.After(10 * time.Second):
		}
	})
	waitForMount(t, mountDir)

	src := filepath.Join(mountDir, "text.txt")
	alias := filepath.Join(mountDir, "hard.txt")
	if out, err := runCmd(t, mountDir, 10*time.Second, "sh", "-c",
		"printf overwrite-base > "+src+" && ln "+src+" "+alias+" && printf hardlink-new > "+alias+" && cat "+src); err != nil {
		t.Fatalf("alias flow: %v\n%s", err, out)
	} else if out != "hardlink-new" {
		t.Fatalf("source after alias write = %q, want hardlink-new", out)
	}
}

// TestAppendGrowsSize: O_APPEND appends must grow the file size.
func TestAppendGrowsSize(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	mountDir := t.TempDir()
	mountDone := make(chan error, 1)
	go func() {
		mountDone <- fuse.MountWithConfig(fuse.MountConfig{Server: ts.URL, APIKey: "test", MountPoint: mountDir, Debug: true})
	}()
	t.Cleanup(func() {
		_ = exec.Command("umount", "-f", mountDir).Run()
		select {
		case <-mountDone:
		case <-time.After(10 * time.Second):
		}
	})
	waitForMount(t, mountDir)

	f := filepath.Join(mountDir, "attr.txt")
	if out, err := runCmd(t, mountDir, 10*time.Second, "sh", "-c",
		"printf attr-base > "+f+" && s1=$(stat -f %z "+f+") && printf x >> "+f+" && s2=$(stat -f %z "+f+") && echo $s1 $s2"); err != nil {
		t.Fatalf("append flow: %v\n%s", err, out)
	} else if out == "9 9" {
		t.Fatalf("append did not grow size: %q", out)
	}
}
