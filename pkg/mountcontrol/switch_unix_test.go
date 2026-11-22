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
