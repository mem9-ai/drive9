package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestRunMountCheckpointJSON(t *testing.T) {
	deps := mountCheckpointDeps{
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "/mnt/drive9" {
				t.Fatalf("mount point = %q", mountPoint)
			}
			return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/control.sock"}, "", nil
		},
		requestCheckpoint: func(_ context.Context, socketPath, checkpointID string, timeout time.Duration) (*mountcontrol.DrainResponse, error) {
			if socketPath != "/tmp/control.sock" || checkpointID != "cp-1" || timeout != 2*time.Second {
				t.Fatalf("request = socket=%q checkpoint=%q timeout=%s", socketPath, checkpointID, timeout)
			}
			return &mountcontrol.DrainResponse{
				OK:         true,
				MountPoint: "/mnt/drive9",
				Checkpoint: &mountcontrol.Checkpoint{CheckpointID: "cp-1", LayerID: "layer-1", DurableSeq: 14},
			}, nil
		},
	}

	out, err := captureStdoutE(t, func() error {
		return runMountCheckpoint([]string{"--checkpoint-id", "cp-1", "--timeout", "2s", "--json", "/mnt/drive9"}, deps)
	})
	if err != nil {
		t.Fatalf("runMountCheckpoint: %v", err)
	}
	var resp mountcontrol.DrainResponse
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if !resp.OK || resp.Checkpoint == nil || resp.Checkpoint.DurableSeq != 14 {
		t.Fatalf("response = %+v", resp)
	}
}

func TestRunMountCheckpointRejectsUnsupportedResponse(t *testing.T) {
	deps := mountCheckpointDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/control.sock"}, "", nil
		},
		requestCheckpoint: func(context.Context, string, string, time.Duration) (*mountcontrol.DrainResponse, error) {
			return &mountcontrol.DrainResponse{OK: false, ErrorKind: "unsupported", Error: "not a writable LayerFS mount"}, nil
		},
	}

	_, err := captureStdoutE(t, func() error {
		return runMountCheckpoint([]string{"--checkpoint-id", "cp-1", "/mnt/drive9"}, deps)
	})
	if err == nil || !strings.Contains(err.Error(), "not a writable LayerFS mount") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunMountCheckpointRequiresIdentity(t *testing.T) {
	err := runMountCheckpoint([]string{"/mnt/drive9"}, mountCheckpointDeps{})
	if err == nil || !strings.Contains(err.Error(), "--checkpoint-id is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunMountCheckpointRejectsNonFuseMount(t *testing.T) {
	deps := mountCheckpointDeps{
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{MountKind: mountstate.MountKindObject}, "", nil
		},
	}
	err := runMountCheckpoint([]string{"--checkpoint-id", "cp-1", "/mnt/object"}, deps)
	if err == nil || !strings.Contains(err.Error(), "only supported for FUSE mounts") {
		t.Fatalf("error = %v", err)
	}
}
