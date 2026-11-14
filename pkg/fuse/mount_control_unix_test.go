//go:build !windows

package fuse

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

func TestMountControlStatusReportsResolvedLayer(t *testing.T) {
	runtimeDir, err := os.MkdirTemp("/tmp", "d9rt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeDir) })
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	t.Setenv("TMPDIR", runtimeDir)
	for _, layer := range []string{"fsl_resolved", ""} {
		server, err := startMountControlServer(t.TempDir(), layer, nil)
		if err != nil {
			t.Fatal(err)
		}
		status, err := mountcontrol.RequestStatus(t.Context(), server.SocketPath(), time.Second)
		server.Close()
		if err != nil || status.Layer == nil || *status.Layer != layer {
			t.Fatalf("layer %q: status = %#v, %v", layer, status, err)
		}
	}
}

func TestMountControlUnknownOperationDoesNotDrain(t *testing.T) {
	runtimeDir, err := os.MkdirTemp("/tmp", "d9rt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeDir) })
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	t.Setenv("TMPDIR", runtimeDir)
	server, err := startMountControlServer(t.TempDir(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, op := range []string{"switch", "switch-operation", "mistyped-drain"} {
		conn, err := net.Dial("unix", server.SocketPath())
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(conn).Encode(map[string]string{"op": op}); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		var response mountcontrol.DrainResponse
		err = json.NewDecoder(conn).Decode(&response)
		_ = conn.Close()
		if err != nil || response.OK || response.ErrorKind != "unsupported_operation" {
			t.Fatalf("op=%s response=%+v err=%v", op, response, err)
		}
	}
}
