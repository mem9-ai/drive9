package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/mountcontrol"
	"github.com/mem9-ai/dat9/pkg/mountstate"
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
