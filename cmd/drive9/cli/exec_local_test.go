package cli

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestFindLocalMountMapsCurrentDirectory(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mountPoint, "web", "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	creation, err := mountstate.ProcessCreationTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID: os.Getpid(), CreationTime: creation, MountKind: mountstate.MountKindFUSE, MountPoint: mountPoint,
		RemoteRoot: "/repo", Server: "https://drive9.example", ControlSocket: "/tmp/sock",
		APIKey: "owner-key-a",
	}); err != nil {
		t.Fatal(err)
	}
	mount, err := findLocalMount(filepath.Join(mountPoint, "web", "src"))
	if err != nil || mount == nil {
		t.Fatalf("mount = %#v, %v", mount, err)
	}
	if mount.workspace() != "/repo" || mount.remoteCwd() != "web/src" || mount.ControlSocket != "/tmp/sock" ||
		mount.Credential != credentialFingerprint("owner-key-a") || mount.Credential == "owner-key-a" {
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

func TestFindLocalMountRejectsReusedPID(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint := t.TempDir()
	creation, err := mountstate.ProcessCreationTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	for name, state := range map[string]mountstate.ProcessState{
		"other process identity": {PID: os.Getpid(), CreationTime: creation + 1},
		"no recorded identity":   {PID: os.Getpid()},
	} {
		state.MountKind, state.MountPoint, state.RemoteRoot = mountstate.MountKindFUSE, mountPoint, "/repo"
		if _, err := mountstate.WriteProcessState(mountPoint, state); err != nil {
			t.Fatal(err)
		}
		if mount, err := findLocalMount(mountPoint); err != nil || mount != nil {
			t.Fatalf("%s: mount = %#v, %v", name, mount, err)
		}
	}
}

// A supervised mount (the default) records no layer on disk; exec must take
// the resolved layer from the live worker, never from the mount arguments.
func TestExecUsesLayerReportedBySupervisedMount(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	creation, err := mountstate.ProcessCreationTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		reply string
		layer string
		err   bool
	}{
		"layered mount":      {reply: `{"ok":true,"layer":"fsl_resolved"}`, layer: "fsl_resolved"},
		"base workspace":     {reply: `{"ok":true,"layer":""}`},
		"pre-upgrade worker": {reply: `{"ok":true}`, err: true},
	} {
		t.Run(name, func(t *testing.T) {
			socket := fakeMountControlSocket(t, tc.reply)
			if _, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
				SupervisorPID: os.Getpid(), SupervisorCreationTime: creation, PID: os.Getpid(), CreationTime: creation,
				Role: mountstate.RoleSupervisor, Supervise: true, MountKind: mountstate.MountKindFUSE,
				MountPoint: mountPoint, RemoteRoot: "/repo", Server: "https://drive9.example", APIKey: "key-a",
				ControlSocket: socket, Args: []string{"mount", "-layer", "my-fork", ":/repo", mountPoint},
			}); err != nil {
				t.Fatal(err)
			}
			request, _ := execTestServer(t, execStarted, execExit0)
			rt := testExecRuntime(io.Discard, io.Discard)
			rt.dir = mountPoint
			rt.server = func() string { return "https://drive9.example" }
			rt.credential = func() string { return credentialFingerprint("key-a") }
			rt.findMount, rt.mountLayer = findLocalMount, localMountLayer
			synced := false
			rt.syncMount = func(context.Context, *localMount) error { synced = true; return nil }
			err := runExec(t.Context(), []string{"true"}, rt)
			if tc.err {
				if err == nil || !strings.Contains(err.Error(), "pass -w") || synced {
					t.Fatalf("error = %v synced = %v", err, synced)
				}
				return
			}
			if err != nil || !synced || request.Workspace.Root != "/repo" || request.Workspace.LayerID != tc.layer {
				t.Fatalf("err=%v synced=%v request=%#v", err, synced, request)
			}
		})
	}
}

// fakeMountControlSocket answers every control request with reply.
func fakeMountControlSocket(t *testing.T, reply string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "d9ctl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "c.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_, _ = bufio.NewReader(conn).ReadString('\n')
			_, _ = io.WriteString(conn, reply+"\n")
			_ = conn.Close()
		}
	}()
	return path
}

func TestFindLocalMountThroughSymlinkAlias(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mountPoint, "web"), 0o755); err != nil {
		t.Fatal(err)
	}
	creation, err := mountstate.ProcessCreationTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID: os.Getpid(), CreationTime: creation, MountKind: mountstate.MountKindFUSE,
		MountPoint: mountPoint, RemoteRoot: "/repo",
	}); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(mountPoint, alias); err != nil {
		t.Fatal(err)
	}
	mount, err := findLocalMount(filepath.Join(alias, "web"))
	if err != nil || mount == nil || mount.MountPoint != mountPoint || mount.remoteCwd() != "web" {
		t.Fatalf("mount through alias = %#v, %v", mount, err)
	}
}

func TestFindLocalMountReportsNonFUSEMount(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	mountPoint := t.TempDir()
	creation, err := mountstate.ProcessCreationTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID: os.Getpid(), CreationTime: creation, MountKind: mountstate.MountKindWebDAV, MountPoint: mountPoint, RemoteRoot: "/repo",
	}); err != nil {
		t.Fatal(err)
	}
	mount, err := findLocalMount(mountPoint)
	if err != nil || mount == nil || mount.Kind != mountstate.MountKindWebDAV {
		t.Fatalf("webdav mount = %#v, %v", mount, err)
	}
}
