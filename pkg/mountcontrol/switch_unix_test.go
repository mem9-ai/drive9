//go:build !windows

package mountcontrol

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func shortUnixSocketDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "d9mc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

func TestRequestSwitchNegotiationUnsupportedAndDone(t *testing.T) {
	ctx := context.Background()
	sockDir := shortUnixSocketDir(t)

	// 1. Daemon with ViewSwitch=false returns deterministic UNSUPPORTED without sending switch op.
	sock1 := filepath.Join(sockDir, "unsup.sock")
	ln1, err := net.Listen("unix", sock1)
	require.NoError(t, err)
	defer func() { _ = ln1.Close() }()
	go func() {
		conn, aerr := ln1.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var ping DrainRequest
		_ = json.NewDecoder(conn).Decode(&ping)
		_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: false, MountID: "m1", Generation: 1})
	}()

	req := SwitchRequest{MountID: "m1", OperationID: "op-1", ExpectedGeneration: 1, Layer: "work-a"}
	_, err = RequestSwitch(ctx, sock1, req)
	var se *SwitchError
	require.ErrorAs(t, err, &se)
	require.Equal(t, viewprotocol.CodeSwitchUnsupported, se.Code)
	require.False(t, se.Unknown)

	// 2. Daemon with ViewSwitch=true negotiates ping -> switch on the same stream and returns DONE receipt.
	sock2 := filepath.Join(sockDir, "ok.sock")
	ln2, err := net.Listen("unix", sock2)
	require.NoError(t, err)
	defer func() { _ = ln2.Close() }()
	target := viewprotocol.SwitchBinding{
		LayerID:      "layer_01j8z4r9w3m5n7p9q2s4t6v8xz",
		Fence:        1,
		BaseRootPath: "/workspace",
	}
	go func() {
		conn, aerr := ln2.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		line, _ := r.ReadBytes('\n')
		var ping DrainRequest
		_ = json.Unmarshal(line, &ping)
		_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: true, MountID: "m1", Generation: 1})
		line2, _ := r.ReadBytes('\n')
		var sw SwitchRequest
		_ = json.Unmarshal(line2, &sw)
		_ = json.NewEncoder(conn).Encode(SwitchResponse{
			MountID:     sw.MountID,
			OperationID: sw.OperationID,
			Receipt: &mountstate.SwitchReceipt{
				OperationID:    sw.OperationID,
				FromGeneration: 1,
				Generation:     2,
				Target:         target,
				State:          mountstate.SwitchDone,
			},
		})
	}()

	resp, err := RequestSwitch(ctx, sock2, req)
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.Receipt.Generation)
	require.Equal(t, target, resp.Receipt.Target)
}

func TestValidateSwitchResponseRecoverAndQueryAborted(t *testing.T) {
	target := viewprotocol.SwitchBinding{
		LayerID:      "layer_01j8z4r9w3m5n7p9q2s4t6v8xz",
		Fence:        1,
		BaseRootPath: "/workspace",
	}

	// 1. QuerySwitch ("switch-operation") accepts ABORTED receipt, while RequestSwitch ("switch") rejects it.
	qReq := SwitchRequest{MountID: "m1", OperationID: "op-rev", ExpectedGeneration: 2, Layer: "work-a"}
	abortedResp := SwitchResponse{
		MountID:     "m1",
		OperationID: "op-rev",
		Receipt: &mountstate.SwitchReceipt{
			OperationID:    "op-rev",
			FromGeneration: 2,
			Generation:     3,
			Target:         target,
			State:          mountstate.SwitchAborted,
		},
	}
	require.NoError(t, validateSwitchResponse("switch-operation", qReq, abortedResp))
	require.Error(t, validateSwitchResponse("switch", qReq, abortedResp))

	// 2. RequestSwitch with Recover=true STILL enforces OperationID, FromGeneration, and Generation+1.
	recReq := SwitchRequest{MountID: "m1", OperationID: "op-rec", ExpectedGeneration: 3, Layer: "main", Recover: true}
	validRecResp := SwitchResponse{
		MountID:     "m1",
		OperationID: "op-rec",
		Receipt: &mountstate.SwitchReceipt{
			OperationID:    "op-rec",
			FromGeneration: 3,
			Generation:     4,
			From:           target,
			State:          mountstate.SwitchDone,
		},
	}
	require.NoError(t, validateSwitchResponse("switch", recReq, validRecResp))

	badGenResp := validRecResp
	badReceipt := *validRecResp.Receipt
	badReceipt.Generation = 5
	badGenResp.Receipt = &badReceipt
	require.Error(t, validateSwitchResponse("switch", recReq, badGenResp))

	badOpResp := validRecResp
	badOpReceipt := *validRecResp.Receipt
	badOpReceipt.OperationID = "other-op"
	badOpResp.Receipt = &badOpReceipt
	require.Error(t, validateSwitchResponse("switch", recReq, badOpResp))
}
