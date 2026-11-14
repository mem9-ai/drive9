//go:build !windows

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/mountsupervisor"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountStatusViewFromLiveControl(t *testing.T) {
	for _, mode := range []string{"main", "layer", "recovering", "legacy", "invalid", "lost-reply"} {
		t.Run(mode, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "d9-st")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			t.Setenv("TMPDIR", dir)
			t.Setenv("XDG_RUNTIME_DIR", dir)
			point, socket := filepath.Join(dir, "mount"), filepath.Join(dir, "control.sock")
			if err := os.Mkdir(point, 0o700); err != nil {
				t.Fatal(err)
			}
			stateFile, err := mountstate.WriteProcessState(point, mountstate.ProcessState{PID: os.Getpid(), MountKind: mountstate.MountKindFUSE, ControlSocket: socket})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Remove(stateFile) })
			ln, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			binding := viewprotocol.Binding{}
			if mode != "main" {
				binding = viewprotocol.Binding{LayerID: "layer-a", WorkID: "work-a", Fence: 7}
			}
			status := mountcontrol.StatusResponse{OK: true, ViewSwitch: true, MountID: "mount-a", Generation: 2, Binding: &binding}
			switch mode {
			case "recovering":
				status.Pending = &mountstate.SwitchReceipt{OperationID: "original", FromGeneration: 1, Generation: 2, Target: binding, State: mountstate.SwitchRecovering}
			case "legacy":
				status = mountcontrol.StatusResponse{OK: true}
			case "invalid":
				status.Binding = nil
			}
			done := make(chan error, 1)
			go func() {
				done <- func() error {
					conn, err := ln.Accept()
					if err != nil {
						return err
					}
					defer func() { _ = conn.Close() }()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					var req mountcontrol.DrainRequest
					if err := json.NewDecoder(conn).Decode(&req); err != nil {
						return err
					}
					if req.Op != "ping" {
						return fmt.Errorf("status mutated mount: %+v", req)
					}
					if mode == "lost-reply" {
						return nil
					}
					return json.NewEncoder(conn).Encode(status)
				}()
			}()
			printed, err := captureStdoutE(t, func() error { return runMountStatus([]string{"--json", point}) })
			if err != nil {
				t.Fatal(err)
			}
			var actual struct {
				View  *mountstate.SwitchSnapshot `json:"view"`
				Error string                     `json:"view_error"`
			}
			if err := json.Unmarshal([]byte(printed), &actual); err != nil {
				t.Fatalf("CLI status: %q %v", printed, err)
			}
			view := actual.View
			var readErr error
			if actual.Error != "" {
				readErr = errors.New(actual.Error)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if mode == "invalid" || mode == "lost-reply" {
				if readErr == nil || view != nil {
					t.Fatalf("invented view: %+v %v", view, readErr)
				}
			} else if readErr != nil {
				t.Fatal(readErr)
			} else if mode == "legacy" {
				if view != nil {
					t.Fatal("legacy inferred main")
				}
			} else if view == nil || view.MountID != status.MountID || view.Generation != 2 || view.Binding != binding || (view.Pending != nil) != (mode == "recovering") {
				t.Fatalf("live identity lost: %+v", view)
			}
			var out bytes.Buffer
			if err := writeMountStatusJSON(&out, mountsupervisor.StatusSnapshot{MountPoint: point}, view, readErr); err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				View  *mountstate.SwitchSnapshot `json:"view"`
				Error string                     `json:"view_error"`
			}
			if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if (decoded.View == nil) != (view == nil) || (decoded.Error != "") != (readErr != nil) {
				t.Fatalf("status: %s", out.String())
			}
			if view != nil && (!strings.Contains(out.String(), `"mount_id": "mount-a"`) || !strings.Contains(out.String(), `"generation": 2`)) {
				t.Fatalf("missing switch arguments: %s", out.String())
			}
			out.Reset()
			if err := writeMountStatusView(&out, view, readErr); err != nil {
				t.Fatal(err)
			}
			if mode == "recovering" && (!strings.Contains(out.String(), "view: layer-a") || !strings.Contains(out.String(), "view_state: RECOVERING") || !strings.Contains(out.String(), "switch_operation: original")) {
				t.Fatalf("pending display: %s", out.String())
			}
			if mode == "main" && !strings.Contains(out.String(), "view: main") {
				t.Fatalf("main display: %s", out.String())
			}
		})
	}
}

func TestMountStatusViewRejectsInvalidPendingAndMissingState(t *testing.T) {
	binding := viewprotocol.Binding{LayerID: "a", WorkID: "w", Fence: 1}
	for _, mutate := range []func(*mountstate.SwitchReceipt){
		func(p *mountstate.SwitchReceipt) { p.Generation = 3 },
		func(p *mountstate.SwitchReceipt) { p.FromGeneration = 2 },
		func(p *mountstate.SwitchReceipt) { p.State = mountstate.SwitchDone },
		func(p *mountstate.SwitchReceipt) { p.Target = viewprotocol.Binding{} },
		func(p *mountstate.SwitchReceipt) { p.OperationID = "" },
	} {
		pending := mountstate.SwitchReceipt{OperationID: "op", FromGeneration: 1, Generation: 2, Target: binding, State: mountstate.SwitchRecovering}
		mutate(&pending)
		if view, err := fixedMountStatus(&mountcontrol.StatusResponse{OK: true, ViewSwitch: true, MountID: "mount-a", Generation: 2, Binding: &binding, Pending: &pending}); err == nil || view != nil {
			t.Fatalf("invalid pending accepted: %+v", pending)
		}
	}
	if view, err := readMountStatusView(filepath.Join(t.TempDir(), "missing")); err != nil || view != nil {
		t.Fatalf("missing state invented view: %+v %v", view, err)
	}
	var out bytes.Buffer
	if err := writeMountStatusJSON(&out, mountsupervisor.StatusSnapshot{}, &mountstate.SwitchSnapshot{MountID: "stale"}, errors.New("unavailable")); err != nil || strings.Contains(out.String(), "stale") {
		t.Fatalf("failed read retained stale view: %s %v", out.String(), err)
	}
}
