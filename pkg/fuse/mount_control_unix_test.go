//go:build !windows

package fuse

import (
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
