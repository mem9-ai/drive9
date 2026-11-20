//go:build !windows

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/mountsupervisor"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// Only a live control response supplies the switch identity. Missing/unreadable
// state never synthesizes main or a generation from process-state metadata.
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

func fixedMountStatus(status *mountcontrol.StatusResponse) (*mountstate.SwitchSnapshot, error) {
	if status == nil || !status.OK {
		return nil, errors.New("mount view status unavailable")
	}
	if !status.ViewSwitch {
		return nil, nil // Legacy mounts do not advertise a fixed-view journal.
	}
	if viewprotocol.ValidateOperationID(status.MountID) != nil || status.Generation == 0 || status.Binding == nil || status.Binding.Validate() != nil {
		return nil, errors.New("invalid mount view identity")
	}
	out := &mountstate.SwitchSnapshot{MountID: status.MountID, Generation: status.Generation, Binding: *status.Binding}
	if p := status.Pending; p != nil {
		if viewprotocol.ValidateOperationID(p.OperationID) != nil || p.State != mountstate.SwitchRecovering || p.Generation != status.Generation || p.FromGeneration == 0 || p.FromGeneration+1 != p.Generation || p.From.Validate() != nil || p.Target != *status.Binding {
			return nil, errors.New("invalid pending mount view decision")
		}
		copy := *p
		out.Pending = &copy
	}
	return out, nil
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
	}
	if _, err := fmt.Fprintf(w, "mount_id: %s\ngeneration: %d\nview: %s\nview_state: %s\n", view.MountID, view.Generation, layer, state); err != nil {
		return err
	}
	if op != "" {
		if _, err := fmt.Fprintf(w, "switch_operation: %s\n", op); err != nil {
			return err
		}
	}
	return nil
}
