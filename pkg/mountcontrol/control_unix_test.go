//go:build !windows

package mountcontrol

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRequestCheckpointSendsIdentityAndReturnsVerifiedRecord(t *testing.T) {
	socketDir, err := os.MkdirTemp("/tmp", "drive9-mountcontrol-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "control.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	request := make(chan DrainRequest, 1)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		var req DrainRequest
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			serverErr <- err
			return
		}
		request <- req
		serverErr <- json.NewEncoder(conn).Encode(DrainResponse{
			OK: true,
			Checkpoint: &Checkpoint{
				CheckpointID: "cp-1",
				LayerID:      "layer-1",
				DurableSeq:   11,
			},
		})
	}()

	resp, err := RequestCheckpoint(context.Background(), socketPath, "cp-1", 2*time.Second)
	if err != nil {
		t.Fatalf("RequestCheckpoint: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
	req := <-request
	if req.Op != "checkpoint" || req.CheckpointID != "cp-1" || req.TimeoutMS != 2000 {
		t.Fatalf("request = %+v", req)
	}
	if !resp.OK || resp.Checkpoint == nil || resp.Checkpoint.DurableSeq != 11 {
		t.Fatalf("response = %+v", resp)
	}
}

func TestRequestCheckpointRejectsMissingIdentity(t *testing.T) {
	_, err := RequestCheckpoint(context.Background(), "unused", "", time.Second)
	if err == nil {
		t.Fatal("RequestCheckpoint error = nil")
	}
}
