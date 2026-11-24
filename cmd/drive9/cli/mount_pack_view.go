package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func refreshMountedPackView(state *mountstate.ProcessState, request func(context.Context, string, time.Duration) (*mountcontrol.StatusResponse, error), timeout time.Duration) error {
	if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE {
		return nil
	}
	if strings.TrimSpace(state.ControlSocket) == "" {
		return errors.New("mount control socket is missing; cannot confirm archive view")
	}
	if request == nil {
		request = mountcontrol.RequestStatus
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	status, err := request(ctx, state.ControlSocket, timeout)
	if err != nil {
		return fmt.Errorf("read mount view for archive: %w", err)
	}
	snap, err := fixedMountStatus(status)
	if err != nil {
		return fmt.Errorf("read mount view for archive: %w", err)
	}
	if status.Pending != nil || status.Recovering {
		return errors.New("mount view is recovering; retry the archive operation after recovery")
	}
	if snap != nil {
		state.LayerID = snap.Binding.LayerID
		state.LayerReadOnly = snap.Binding.LayerID != "" && snap.Binding.ReadOnly
	} else if status.ViewSwitch || status.Layer == nil || *status.Layer != state.LayerID {
		return errors.New("cannot identify the active mount view for the archive")
	}
	if strings.TrimSpace(status.RemoteRoot) != "" {
		state.RemoteRoot = status.RemoteRoot
	}
	return nil
}

func fixedMountStatus(status *mountcontrol.StatusResponse) (*mountstate.SwitchSnapshot, error) {
	if status == nil {
		return nil, errors.New("mount view status unavailable")
	}
	if !status.OK {
		if status.Error != "" {
			return nil, errors.New(status.Error)
		}
		return nil, errors.New("mount view status unavailable")
	}
	if status.MountID == "" && status.Generation == 0 && status.Binding == nil {
		return nil, nil
	}
	if viewprotocol.ValidateOperationID(status.MountID) != nil || status.Generation == 0 || status.Binding == nil || status.Binding.Validate() != nil {
		return nil, errors.New("invalid mount view identity")
	}
	out := &mountstate.SwitchSnapshot{
		MountID:    status.MountID,
		Generation: status.Generation,
		Binding:    *status.Binding,
		Recovering: status.Recovering,
	}
	if p := status.Pending; p != nil {
		if viewprotocol.ValidateOperationID(p.OperationID) != nil ||
			(p.State != mountstate.SwitchRecovering && p.State != mountstate.SwitchTargetRevoked) ||
			p.Generation != status.Generation || p.FromGeneration == 0 ||
			p.FromGeneration+1 != p.Generation || p.From.Validate() != nil || p.Target != *status.Binding {
			return nil, errors.New("invalid pending mount view decision")
		}
		cp := *p
		out.Pending = &cp
	}
	return out, nil
}
