package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestMountSwitchCLIFreshAutoInitAndExplicitRetry(t *testing.T) {
	var capturedReq mountcontrol.SwitchRequest
	deps := mountSwitchDeps{
		goos: "linux",
		readState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/fake.sock"}, "/tmp/pid", nil
		},
		queryStatus: func(context.Context, string, time.Duration) (*mountcontrol.StatusResponse, error) {
			b := mountstate.SwitchBinding{}
			return &mountcontrol.StatusResponse{OK: true, ViewSwitch: true, MountID: "mnt_abc", Generation: 3, Binding: &b}, nil
		},
		newOpID: func() (string, error) { return "op_generated_1", nil },
		switchView: func(_ context.Context, _ string, req mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
			capturedReq = req
			rcpt := mountstate.SwitchReceipt{
				OperationID:    req.OperationID,
				State:          mountstate.SwitchDone,
				FromGeneration: req.ExpectedGeneration,
				Generation:     req.ExpectedGeneration + 1,
				Target:         mountstate.SwitchBinding{LayerID: "layer_01j00000000000000000000001", BaseRootPath: "/", Fence: 1},
			}
			return &mountcontrol.SwitchResponse{MountID: req.MountID, OperationID: req.OperationID, Receipt: &rcpt}, nil
		},
		queryView: func(_ context.Context, _ string, req mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
			capturedReq = req
			rcpt := mountstate.SwitchReceipt{OperationID: req.OperationID, State: mountstate.SwitchDone, Generation: req.ExpectedGeneration + 1}
			return &mountcontrol.SwitchResponse{MountID: req.MountID, OperationID: req.OperationID, Receipt: &rcpt}, nil
		},
	}

	var out bytes.Buffer
	err := runMountSwitch([]string{"--layer", "L1", "/mnt/ws"}, false, &out, deps)
	require.NoError(t, err)
	require.Equal(t, "mnt_abc", capturedReq.MountID)
	require.Equal(t, uint64(3), capturedReq.ExpectedGeneration)
	require.Equal(t, "op_generated_1", capturedReq.OperationID)
	require.Equal(t, "L1", capturedReq.Layer)

	var parsed struct {
		Request  mountcontrol.SwitchRequest   `json:"request"`
		Response *mountcontrol.SwitchResponse `json:"response"`
		Unknown  bool                         `json:"unknown"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &parsed))
	require.False(t, parsed.Unknown)
	require.Equal(t, uint64(4), parsed.Response.Receipt.Generation)

	// Retrying with --operation MUST reject missing --mount-id or --generation.
	out.Reset()
	err = runMountSwitch([]string{"--operation", "op_generated_1", "--layer", "L1", "/mnt/ws"}, false, &out, deps)
	require.ErrorContains(t, err, "requires explicit --mount-id and --generation")

	// Explicit retry with all 3 identity fields succeeds without calling queryStatus.
	deps.queryStatus = func(context.Context, string, time.Duration) (*mountcontrol.StatusResponse, error) {
		t.Fatal("queryStatus must not be called on explicit retry")
		return nil, nil
	}
	out.Reset()
	err = runMountSwitch([]string{
		"--mount-id", "mnt_abc", "--generation", "3", "--operation", "op_generated_1", "--recover", "--layer", "main", "/mnt/ws",
	}, false, &out, deps)
	require.NoError(t, err)
	require.True(t, capturedReq.Recover)

	// switch-operation requires all identity flags and routes to queryView.
	out.Reset()
	err = runMountSwitch([]string{"--layer", "L1", "/mnt/ws"}, true, &out, deps)
	require.ErrorContains(t, err, "requires --mount-id, --generation, --operation")

	out.Reset()
	err = runMountSwitch([]string{
		"--mount-id", "mnt_abc", "--generation", "3", "--operation", "op_generated_1", "--layer", "L1", "/mnt/ws",
	}, true, &out, deps)
	require.NoError(t, err)
}

func TestMountSwitchCLIAmbiguousErrorSetsUnknown(t *testing.T) {
	deps := mountSwitchDeps{
		goos: "linux",
		readState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/fake.sock"}, "/tmp/pid", nil
		},
		switchView: func(_ context.Context, _ string, req mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
			return nil, &mountcontrol.SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: context.DeadlineExceeded}
		},
	}
	var out bytes.Buffer
	err := runMountSwitch([]string{
		"--mount-id", "mnt_1", "--generation", "1", "--operation", "op_1", "--layer", "L1", "/mnt/ws",
	}, false, &out, deps)
	require.Error(t, err)
	var parsed struct {
		Unknown bool `json:"unknown"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &parsed))
	require.True(t, parsed.Unknown)
}
