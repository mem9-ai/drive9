package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestRunMountDrainJSON(t *testing.T) {
	start := time.Now().UTC()
	deps := mountDrainDeps{
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "/mnt/drive9" {
				t.Fatalf("readProcessState mountPoint = %q", mountPoint)
			}
			return mountstate.ProcessState{
				PID:           123,
				MountKind:     mountstate.MountKindFUSE,
				ControlSocket: "/tmp/drive9.sock",
			}, "", nil
		},
		syncfs: func(mountPoint string) error {
			if mountPoint != "/mnt/drive9" {
				t.Fatalf("syncfs mountPoint = %q", mountPoint)
			}
			return nil
		},
		requestDrain: func(ctx context.Context, socketPath string, timeout time.Duration) (*mountcontrol.DrainResponse, error) {
			if socketPath != "/tmp/drive9.sock" {
				t.Fatalf("requestDrain socketPath = %q", socketPath)
			}
			if timeout != 2*time.Second {
				t.Fatalf("requestDrain timeout = %s", timeout)
			}
			resp := mountcontrol.NewDrainResponse("/mnt/drive9", start)
			resp.Pending.CommitQueuePending = 1
			resp.Pending.CommitQueueBytes = 42
			resp.Finish(start.Add(25 * time.Millisecond))
			return &resp, nil
		},
	}

	out, err := captureStdoutE(t, func() error {
		return runMountDrain([]string{"--json", "--timeout", "2s", "/mnt/drive9"}, deps)
	})
	if err != nil {
		t.Fatalf("runMountDrain: %v", err)
	}
	var got mountcontrol.DrainResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out)
	}
	if !got.OK || got.MountPoint != "/mnt/drive9" || got.DurationMS != 25 {
		t.Fatalf("drain response = %#v", got)
	}
	if got.Pending.CommitQueuePending != 1 || got.Pending.CommitQueueBytes != 42 {
		t.Fatalf("pending = %#v", got.Pending)
	}
}

func TestRunMountDrainJSONReturnsErrorForNonOKResponse(t *testing.T) {
	start := time.Now().UTC()
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:           123,
				MountKind:     mountstate.MountKindFUSE,
				ControlSocket: "/tmp/drive9.sock",
			}, "", nil
		},
		syncfs: func(string) error { return nil },
		requestDrain: func(context.Context, string, time.Duration) (*mountcontrol.DrainResponse, error) {
			resp := mountcontrol.NewDrainResponse("/mnt/drive9", start)
			resp.Pending.UploaderCached = 1
			resp.Fail("pending_work_remaining", "", errors.New("pending work remains after drain"))
			resp.Finish(start.Add(10 * time.Millisecond))
			return &resp, nil
		},
	}

	out, err := captureStdoutE(t, func() error {
		return runMountDrain([]string{"--json", "/mnt/drive9"}, deps)
	})
	if err == nil || !strings.Contains(err.Error(), "pending work remains after drain") {
		t.Fatalf("runMountDrain error = %v", err)
	}
	var got mountcontrol.DrainResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out)
	}
	if got.OK || got.Pending.UploaderCached != 1 {
		t.Fatalf("drain response = %#v", got)
	}
}

func TestRunMountDrainCallsSyncfsBeforeRequest(t *testing.T) {
	start := time.Now().UTC()
	var got []string
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:           123,
				MountKind:     mountstate.MountKindFUSE,
				ControlSocket: "/tmp/drive9.sock",
			}, "", nil
		},
		syncfs: func(mountPoint string) error {
			got = append(got, "syncfs:"+mountPoint)
			return nil
		},
		requestDrain: func(context.Context, string, time.Duration) (*mountcontrol.DrainResponse, error) {
			got = append(got, "drain")
			resp := mountcontrol.NewDrainResponse("/mnt/drive9", start)
			resp.Finish(start.Add(time.Millisecond))
			return &resp, nil
		},
	}
	if err := runMountDrain([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runMountDrain: %v", err)
	}
	want := []string{"syncfs:/mnt/drive9", "drain"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("call order = %v, want %v", got, want)
	}
}

func TestRunMountDrainIgnoresSyncfsENOENT(t *testing.T) {
	start := time.Now().UTC()
	drainRan := false
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:           123,
				MountKind:     mountstate.MountKindFUSE,
				ControlSocket: "/tmp/drive9.sock",
			}, "", nil
		},
		syncfs: func(string) error { return syscall.ENOENT },
		requestDrain: func(context.Context, string, time.Duration) (*mountcontrol.DrainResponse, error) {
			drainRan = true
			resp := mountcontrol.NewDrainResponse("/mnt/drive9", start)
			resp.Finish(start.Add(time.Millisecond))
			return &resp, nil
		},
	}
	if err := runMountDrain([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runMountDrain: %v", err)
	}
	if !drainRan {
		t.Fatal("requestDrain skipped after ignorable syncfs ENOENT")
	}
}

func TestRunMountDrainSyncfsError(t *testing.T) {
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:           123,
				MountKind:     mountstate.MountKindFUSE,
				ControlSocket: "/tmp/drive9.sock",
			}, "", nil
		},
		syncfs: func(string) error {
			return syscall.EIO
		},
		requestDrain: func(context.Context, string, time.Duration) (*mountcontrol.DrainResponse, error) {
			t.Fatal("requestDrain must not run after syncfs error")
			return nil, nil
		},
	}
	err := runMountDrain([]string{"/mnt/drive9"}, deps)
	if err == nil || !strings.Contains(err.Error(), "syncfs") {
		t.Fatalf("runMountDrain error = %v, want syncfs error", err)
	}
}

func TestRunMountDrainRejectsObjectMount(t *testing.T) {
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:       123,
				MountKind: mountstate.MountKindObject,
			}, "", nil
		},
	}

	err := runMountDrain([]string{"/mnt/obj"}, deps)
	if err == nil || !strings.Contains(err.Error(), "only supported for FUSE mounts") {
		t.Fatalf("runMountDrain error = %v", err)
	}
}

func TestRunMountDrainRejectsNonFuseMount(t *testing.T) {
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:       123,
				MountKind: mountstate.MountKindWebDAV,
			}, "", nil
		},
	}

	err := runMountDrain([]string{"/mnt/drive9"}, deps)
	if err == nil || !strings.Contains(err.Error(), "only supported for FUSE mounts") {
		t.Fatalf("runMountDrain error = %v", err)
	}
}

func TestRunMountDrainRejectsMountWithoutControlSocket(t *testing.T) {
	deps := mountDrainDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{
				PID:       123,
				MountKind: mountstate.MountKindFUSE,
			}, "", nil
		},
	}

	err := runMountDrain([]string{"/mnt/drive9"}, deps)
	if err == nil || !strings.Contains(err.Error(), "does not expose a control socket") {
		t.Fatalf("runMountDrain error = %v", err)
	}
}
