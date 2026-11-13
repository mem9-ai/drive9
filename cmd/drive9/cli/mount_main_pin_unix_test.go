//go:build !windows

package cli

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestPinMainMountRequiresMainAndCompleteStatus(t *testing.T) {
	for _, mode := range []string{"legacy", "main", "layer", "recovering", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			point := t.TempDir()
			closeDir, err := pinMainMountWithStatus(point, func(got string) (*mountstate.SwitchSnapshot, error) {
				if got != point {
					t.Fatal("wrong mount queried")
				}
				v := &mountstate.SwitchSnapshot{}
				switch mode {
				case "legacy":
					return nil, nil
				case "layer":
					v.Binding = viewprotocol.Binding{LayerID: "A", WorkID: "work-A", Fence: 1}
				case "recovering":
					v.Pending = &mountstate.SwitchReceipt{}
				case "unknown":
					return nil, errors.New("lost status")
				}
				return v, nil
			})
			wantOK := mode == "legacy" || mode == "main"
			if (err == nil) != wantOK || (closeDir != nil) != wantOK {
				t.Fatalf("release=%v err=%v", closeDir != nil, err)
			}
			if closeDir != nil {
				closeDir()
			}
		})
	}
	point := filepath.Join(t.TempDir(), "missing")
	if release, err := pinMainMountWithStatus(point, func(string) (*mountstate.SwitchSnapshot, error) {
		t.Fatal("queried binding before opening directory")
		return nil, nil
	}); err == nil || release != nil {
		t.Fatal("missing mount was accepted")
	}
}

func TestMainOnlyHelpersRejectSelectedLayerBeforeMutation(t *testing.T) {
	for _, command := range []string{"clone", "worktree-add", "worktree-remove", "hydrate", "pack", "unpack"} {
		t.Run(command, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "d9-pin-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(dir) }()
			point, root, socket := filepath.Join(dir, "mount"), filepath.Join(dir, "local"), filepath.Join(dir, "control.sock")
			if err := os.Mkdir(point, 0700); err != nil {
				t.Fatal(err)
			}
			statePath, err := mountstate.WriteProcessState(point, mountstate.ProcessState{PID: os.Getpid(), MountKind: mountstate.MountKindFUSE, MountPoint: point, RemoteRoot: "/", LocalRoot: root, ControlSocket: socket})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.Remove(statePath) }()
			ln, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
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
						return errors.New("helper mutated mount control")
					}
					binding := viewprotocol.Binding{LayerID: "A", WorkID: "work-A", Fence: 1}
					return json.NewEncoder(conn).Encode(mountcontrol.StatusResponse{OK: true, ViewSwitch: true, MountID: "mount-A", Generation: 2, Binding: &binding})
				}()
			}()
			cl := client.New("http://example.invalid", "")
			switch command {
			case "clone":
				err = gitClone([]string{"--fast", "https://example.invalid/repo", filepath.Join(point, "new")})
			case "worktree-add":
				err = gitWorktreeAdd([]string{"--fast", point, filepath.Join(point, "new")})
			case "worktree-remove":
				err = gitWorktreeRemove([]string{"--fast", point})
			case "hydrate":
				err = gitHydrate([]string{point})
			case "pack":
				err = Pack(cl, []string{"--mount", point})
			case "unpack":
				err = Unpack(cl, []string{"--mount", point})
			}
			if err == nil || !strings.Contains(err.Error(), "requires the mount to select main") {
				t.Fatalf("helper bypassed selected-view check: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("helper touched main local state: %v", err)
			}
		})
	}
}
