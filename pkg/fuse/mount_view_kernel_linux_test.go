//go:build linux

package fuse

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// Explicitly opt-in: the complete MountWithConfig entry creates a Linux FUSE
// mount over the local HTTP fixture. No database or deployed server is used.
func TestMountViewKernelSameConnectionSwitch(t *testing.T) {
	if os.Getenv("DRIVE9_RUN_FUSE_KERNEL_TEST") != "1" {
		t.Skip("manual Linux kernel test: set DRIVE9_RUN_FUSE_KERNEL_TEST=1")
	}
	cfg, _, _, _ := mountViewFixture(t)
	dir, err := os.MkdirTemp("/tmp", "d9mv")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("XDG_RUNTIME_DIR", dir)
	socket := mountstate.ControlSocketPath(cfg.MountPoint) // Resolve BEFORE mount.
	done := make(chan error, 1)
	go func() { done <- MountWithConfig(cfg) }()
	t.Cleanup(func() {
		_ = unix.Unmount(cfg.MountPoint, 0)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("mount exit: %v", err)
			}
		case <-time.After(10 * time.Second):
			_ = unix.Unmount(cfg.MountPoint, unix.MNT_DETACH)
			t.Error("mount did not stop")
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	var status *mountcontrol.StatusResponse
	for {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("mount startup: %v", err)
		default:
		}
		status, err = mountcontrol.RequestStatus(t.Context(), socket, time.Second)
		if err == nil && status.OK && status.MountID != "" {
			var st unix.Statfs_t
			if err := unix.Statfs(cfg.MountPoint, &st); err == nil && uint64(st.Type) == 0x65735546 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("mount not ready: %+v %v", status, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	pid, mountID := status.PID, status.MountID
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	if err := os.Chdir(cfg.MountPoint); err != nil {
		t.Fatal(err)
	}
	read := func(path, want string) {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want {
			t.Fatalf("read %s: %q %v", path, data, err)
		}
	}
	read("a", "main")
	request := mountcontrol.SwitchRequest{MountID: mountID, OperationID: "kernel-switch-a", ExpectedGeneration: 1, Target: viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}}
	held, err := os.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	response, err := mountcontrol.RequestSwitch(t.Context(), socket, request)
	if err == nil || response == nil || response.ErrorKind != "BUSY" || response.Receipt != nil {
		_ = held.Close()
		t.Fatalf("open file must refuse before deciding: %+v %v", response, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	// Invalidations may cause queued Forget callbacks. Retry the same ID only
	// on definite pre-decision BUSY; never replace an unknown operation.
	switchView := func(req mountcontrol.SwitchRequest) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		for {
			resp, err := mountcontrol.RequestSwitch(ctx, socket, req)
			if err == nil && resp.Receipt != nil && resp.Receipt.State == mountstate.SwitchDone {
				return
			}
			if resp == nil || resp.Receipt != nil || resp.ErrorKind != "BUSY" || ctx.Err() != nil {
				t.Fatalf("switch: %+v %v", resp, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	switchView(request)
	read("a", "private") // cwd was already the root before switching.
	read(filepath.Join(cfg.MountPoint, "a"), "private")
	status, err = mountcontrol.RequestStatus(t.Context(), socket, time.Second)
	if err != nil || status.PID != pid || status.MountID != mountID || status.Generation != 2 || status.Binding == nil || *status.Binding != request.Target {
		t.Fatalf("switch replaced mount identity: %+v %v", status, err)
	}
	request.OperationID, request.ExpectedGeneration, request.Target = "kernel-switch-main", 2, viewprotocol.Binding{}
	switchView(request)
	read("a", "main")
}
