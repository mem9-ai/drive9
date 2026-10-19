package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestFindLocalMountMapsCurrentDirectory(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mountPoint, "web", "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID: os.Getpid(), MountKind: mountstate.MountKindFUSE, MountPoint: mountPoint,
		RemoteRoot: "/repo", Server: "https://drive9.example", ControlSocket: "/tmp/sock",
		Args: []string{"mount", "--layer", "fork-1", ":/repo", mountPoint},
	}); err != nil {
		t.Fatal(err)
	}
	mount, err := findLocalMount(filepath.Join(mountPoint, "web", "src"))
	if err != nil || mount == nil {
		t.Fatalf("mount = %#v, %v", mount, err)
	}
	if mount.workspace() != "/repo" || mount.remoteCwd() != "web/src" || mount.Layer != "fork-1" || mount.ControlSocket != "/tmp/sock" {
		t.Fatalf("mount = %#v", mount)
	}
	root, err := findLocalMount(mountPoint)
	if err != nil || root == nil || root.remoteCwd() != "" {
		t.Fatalf("mount root = %#v, %v", root, err)
	}
	if outside, err := findLocalMount(t.TempDir()); err != nil || outside != nil {
		t.Fatalf("outside = %#v, %v", outside, err)
	}
}

func TestFindLocalMountIgnoresDeadMounts(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint := t.TempDir()
	if _, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID: 1 << 30, MountKind: mountstate.MountKindFUSE, MountPoint: mountPoint, RemoteRoot: "/repo",
	}); err != nil {
		t.Fatal(err)
	}
	if mount, err := findLocalMount(mountPoint); err != nil || mount != nil {
		t.Fatalf("dead mount = %#v, %v", mount, err)
	}
}
