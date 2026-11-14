//go:build !windows

package fuse

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

func controlListenerPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "d9cl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("XDG_RUNTIME_DIR", dir)
	return filepath.Join(dir, "control.sock")
}

func TestMountControlOwnershipPreventsSocketReplacement(t *testing.T) {
	s, _, _, _ := viewSwitchFixture(t)
	server := startViewControlFixture(t, s)
	// Use the exact same mountpoint without resolving a new temporary one.
	if second, err := listenMountControlSecond(server.SocketPath()); err == nil || second != nil {
		t.Fatalf("second listener replaced original: %v %v", second, err)
	}
	status, err := mountcontrol.RequestStatus(t.Context(), server.SocketPath(), time.Second)
	if err != nil || status.MountID != "mount-1" {
		t.Fatalf("original control stopped responding: %+v %v", status, err)
	}
	server.Close()
	listener, lock, err := listenMountControl(server.SocketPath())
	if err != nil {
		t.Fatalf("owner did not release lock: %v", err)
	}
	_ = listener.Close()
	_ = lock.Close()
}

func listenMountControlSecond(path string) (*net.UnixListener, error) {
	listener, lock, err := listenMountControl(path)
	if lock != nil {
		_ = lock.Close()
	}
	if listener != nil {
		_ = listener.Close()
	}
	return listener, err
}

func TestMountControlRecoversOnlyStaleSocket(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "stale"}[stale], func(t *testing.T) {
			path := controlListenerPath(t)
			old, err := net.ListenUnix("unix", &net.UnixAddr{Net: "unix", Name: path})
			if err != nil {
				t.Fatal(err)
			}
			old.SetUnlinkOnClose(false)
			defer func() { _ = old.Close() }()
			if stale {
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
			}
			listener, lock, err := listenMountControl(path)
			if stale {
				if err != nil || listener == nil || lock == nil {
					t.Fatalf("stale recovery: %v", err)
				}
				defer func() { _ = listener.Close(); _ = lock.Close() }()
			} else if err == nil || listener != nil || lock != nil {
				t.Fatalf("live listener replaced: %v", err)
			}
			conn, err := net.DialTimeout("unix", path, time.Second)
			if err != nil {
				t.Fatalf("selected listener unavailable: %v", err)
			}
			_ = conn.Close()
		})
	}
}

func TestMountControlRejectsNonSocketAndSymlinkLock(t *testing.T) {
	for _, lockSymlink := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular-path", true: "symlink-lock"}[lockSymlink], func(t *testing.T) {
			path := controlListenerPath(t)
			protected := path
			if lockSymlink {
				protected = path + ".protected"
			}
			if err := os.WriteFile(protected, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			if lockSymlink {
				if err := os.Symlink(protected, path+".lock"); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := listenMountControl(path); err == nil {
				t.Fatal("unsafe control path accepted")
			}
			data, err := os.ReadFile(protected)
			if err != nil || string(data) != "keep" {
				t.Fatalf("protected file changed: %q %v", data, err)
			}
		})
	}
}
