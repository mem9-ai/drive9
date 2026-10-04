//go:build !windows

package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

func TestMountControlServerDispatchesCheckpoint(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	server := &mountControlServer{
		fs:   newTestDrainFS(),
		gate: newWorkspaceMutationGate(),
		checkpoint: func(_ context.Context, checkpointID string) (mountcontrol.Checkpoint, error) {
			return mountcontrol.Checkpoint{
				CheckpointID: checkpointID,
				LayerID:      "layer-1",
				DurableSeq:   12,
			}, nil
		},
	}
	go server.handleConn(serverConn)

	if err := json.NewEncoder(clientConn).Encode(mountcontrol.DrainRequest{
		Op:           "checkpoint",
		CheckpointID: "cp-1",
		TimeoutMS:    time.Second.Milliseconds(),
	}); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	var resp mountcontrol.DrainResponse
	if err := json.NewDecoder(bufio.NewReader(clientConn)).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.OK || resp.Checkpoint == nil || resp.Checkpoint.CheckpointID != "cp-1" || resp.Checkpoint.DurableSeq != 12 {
		t.Fatalf("response = %+v", resp)
	}
}

func TestMountControlServerRejectsUnknownOperation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	server := &mountControlServer{fs: newTestDrainFS()}
	go server.handleConn(serverConn)

	if err := json.NewEncoder(clientConn).Encode(mountcontrol.DrainRequest{Op: "unknown"}); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	var resp mountcontrol.DrainResponse
	if err := json.NewDecoder(bufio.NewReader(clientConn)).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.OK || resp.ErrorKind != "bad_request" {
		t.Fatalf("response = %+v", resp)
	}
}
