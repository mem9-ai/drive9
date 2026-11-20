//go:build !windows

package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestMountSwitchUnixControlRoundTripAndLostReply(t *testing.T) {
	for _, mode := range []string{"switch", "query", "lost-reply"} {
		t.Run(mode, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "d9-sw-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			socket := filepath.Join(dir, "control.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			served := make(chan error, 1)
			go func() {
				served <- func() error {
					conn, err := listener.Accept()
					if err != nil {
						return err
					}
					defer func() { _ = conn.Close() }()
					_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
					decoder := json.NewDecoder(conn)
					var ping mountcontrol.DrainRequest
					if err := decoder.Decode(&ping); err != nil || ping.Op != "ping" {
						return fmt.Errorf("expected capability handshake: %v %+v", err, ping)
					}
					if err := json.NewEncoder(conn).Encode(mountcontrol.StatusResponse{OK: true, ViewSwitch: true, MountID: "mount-a"}); err != nil {
						return err
					}
					var wire struct {
						Op string
						mountcontrol.SwitchRequest
					}
					if err := decoder.Decode(&wire); err != nil {
						return err
					}
					wantOp := "switch"
					if mode == "query" {
						wantOp = "switch-operation"
					}
					if wire.Op != wantOp || wire.SwitchRequest != mountSwitchTestRequest() {
						return fmt.Errorf("wrong operation: %+v", wire)
					}
					if mode == "lost-reply" {
						return nil
					}
					return json.NewEncoder(conn).Encode(mountcontrol.SwitchResponse{MountID: wire.MountID, OperationID: wire.OperationID, Receipt: &mountstate.SwitchReceipt{OperationID: wire.OperationID, FromGeneration: 8, Generation: 9, Target: wire.Target, State: mountstate.SwitchDone}})
				}()
			}()
			deps := mountSwitchDeps{readState: func(string) (mountstate.ProcessState, string, error) {
				return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: socket}, "", nil
			}, switchView: mountcontrol.RequestSwitch, queryView: mountcontrol.QuerySwitch}
			var out bytes.Buffer
			err = runMountSwitch(mountSwitchTestArgs(), mode == "query", &out, deps)
			if (err != nil) != (mode == "lost-reply") {
				t.Fatalf("mode=%s error=%v", mode, err)
			}
			if serverErr := <-served; serverErr != nil {
				t.Fatal(serverErr)
			}
			var result struct {
				Request mountcontrol.SwitchRequest
				Unknown bool
			}
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Request != mountSwitchTestRequest() || result.Unknown != (mode == "lost-reply") {
				t.Fatalf("result: %+v", result)
			}
		})
	}
}
