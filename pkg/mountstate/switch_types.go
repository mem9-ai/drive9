package mountstate

import (
	"errors"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

const (
	SwitchRecovering    = "RECOVERING"
	SwitchTargetRevoked = "TARGET_REVOKED"
	SwitchDone          = "DONE"
	SwitchAborted       = "ABORTED"

	UnbindKindOp     = "op"
	UnbindKindFence  = "fence"
	UnbindKindExpire = "expire"

	MaxPendingUnbinds = 2
)

var (
	ErrSwitchUncertain     = errors.New("mount switch journal outcome uncertain; admission fail-closed")
	ErrSwitchCorrupt       = errors.New("mount switch journal corrupt")
	ErrSwitchConflict      = errors.New("mount switch request conflicts with journal state")
	ErrSwitchInvalid       = errors.New("invalid mount switch journal argument")
	ErrSwitchPending       = errors.New("mount switch decision already pending")
	ErrSwitchUnbindPending = errors.New("mount switch unbind cleanup pending")
	ErrSwitchNotRevoked    = errors.New("mount switch recovery requires TARGET_REVOKED state")
)

// PendingUnbind is one durable pending lease cleanup item in SwitchJournal
// ([layer_id, mount_id, kind, operation_id, fence], bounded at <= 2 items).
type PendingUnbind struct {
	LayerID     string `json:"layer_id"`
	MountID     string `json:"mount_id"`
	Kind        string `json:"kind"`
	OperationID string `json:"operation_id,omitempty"`
	Fence       int64  `json:"fence"`
}

func (u PendingUnbind) Validate() error {
	if err := viewprotocol.ValidateLayerID(u.LayerID); err != nil {
		return fmt.Errorf("%w: %v", ErrSwitchInvalid, err)
	}
	if err := viewprotocol.ValidateOperationID(u.MountID); err != nil {
		return fmt.Errorf("%w: mount_id: %v", ErrSwitchInvalid, err)
	}
	switch u.Kind {
	case UnbindKindOp:
		if err := viewprotocol.ValidateOperationID(u.OperationID); err != nil {
			return fmt.Errorf("%w: operation_id: %v", ErrSwitchInvalid, err)
		}
		if u.Fence < 0 || u.Fence%2 != 0 {
			return fmt.Errorf("%w: bind_intent observed fence must be non-negative even, got %d", ErrSwitchInvalid, u.Fence)
		}
	case UnbindKindFence:
		if u.OperationID != "" {
			return fmt.Errorf("%w: fence unbind must not carry operation_id", ErrSwitchInvalid)
		}
		if u.Fence <= 0 || u.Fence%2 == 0 {
			return fmt.Errorf("%w: fence unbind requires positive odd fence, got %d", ErrSwitchInvalid, u.Fence)
		}
	default:
		return fmt.Errorf("%w: unsupported unbind kind %q", ErrSwitchInvalid, u.Kind)
	}
	return nil
}

// SwitchReceipt describes a durable binding decision in SwitchJournal.
type SwitchReceipt struct {
	OperationID    string                     `json:"operation_id"`
	FromGeneration uint64                     `json:"from_generation"`
	Generation     uint64                     `json:"generation"`
	From           viewprotocol.SwitchBinding `json:"from"`
	Target         viewprotocol.SwitchBinding `json:"target"`
	State          string                     `json:"state"`
}

// SwitchSnapshot reports the durable state of a SwitchJournal.
type SwitchSnapshot struct {
	MountID        string                     `json:"mount_id"`
	Generation     uint64                     `json:"generation"`
	Binding        viewprotocol.SwitchBinding `json:"binding"`
	Pending        *SwitchReceipt             `json:"pending,omitempty"`
	PendingUnbinds []PendingUnbind            `json:"pending_unbinds,omitempty"`
}
