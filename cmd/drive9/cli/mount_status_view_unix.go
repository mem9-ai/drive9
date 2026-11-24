//go:build !windows

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/mountsupervisor"
)

// readMountStatusView queries the live control socket for the mount's current
// view generation and binding. Missing/unreadable process state never synthesizes
// a fallback generation from pidfile metadata.
func readMountStatusView(point string) (*mountstate.SwitchSnapshot, error) {
	state, _, err := mountstate.ReadProcessState(backgroundMountStatePoint(point))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE || state.ControlSocket == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := mountcontrol.RequestStatus(ctx, state.ControlSocket, time.Second)
	if err != nil {
		return nil, err
	}
	return fixedMountStatus(status)
}

func writeMountStatusJSON(w io.Writer, status mountsupervisor.StatusSnapshot, view *mountstate.SwitchSnapshot, viewErr error) error {
	out := struct {
		mountsupervisor.StatusSnapshot
		View      *mountstate.SwitchSnapshot `json:"view,omitempty"`
		ViewError string                     `json:"view_error,omitempty"`
	}{StatusSnapshot: status, View: view}
	if viewErr != nil {
		out.View = nil
		out.ViewError = viewErr.Error()
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func writeMountStatusView(w io.Writer, view *mountstate.SwitchSnapshot, viewErr error) error {
	if viewErr != nil {
		_, err := fmt.Fprintf(w, "view_error: %s\n", viewErr)
		return err
	}
	if view == nil {
		return nil
	}
	layer := view.Binding.LayerID
	if layer == "" {
		layer = "main"
	}
	state, op := mountstate.SwitchDone, ""
	if view.Pending != nil {
		state, op = view.Pending.State, view.Pending.OperationID
	} else if view.Recovering {
		state = mountstate.SwitchRecovering
	}
	if _, err := fmt.Fprintf(w, "mount_id: %s\ngeneration: %d\nview: %s\nview_state: %s\n", view.MountID, view.Generation, layer, state); err != nil {
		return err
	}
	if view.Binding.Fence > 0 {
		if _, err := fmt.Fprintf(w, "fence: %d\n", view.Binding.Fence); err != nil {
			return err
		}
	}
	if op != "" {
		if _, err := fmt.Fprintf(w, "switch_operation: %s\n", op); err != nil {
			return err
		}
	}
	return nil
}
