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
	ErrInvalidOperationID  = errors.New("invalid mount or operation id")
	ErrSwitchClosed        = errors.New("mount switch journal closed")
	ErrSwitchConflict      = errors.New("mount switch request conflicts with journal state")
	ErrSwitchInvalid       = errors.New("invalid mount switch journal argument")
	ErrSwitchPending       = errors.New("mount switch decision already pending")
	ErrSwitchUnbindPending = errors.New("mount switch unbind cleanup pending")
	ErrSwitchNotRevoked    = errors.New("mount switch recovery requires TARGET_REVOKED state")
)

// ValidateOperationID validates a local mount_id or operation_id token (1..64 ASCII bytes).
func ValidateOperationID(id string) error {
	if id == "" || len(id) > viewprotocol.MaxMountIDBytes {
		return fmt.Errorf("%w: %q", ErrInvalidOperationID, id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == ':'
		if !ok {
			return fmt.Errorf("%w: %q", ErrInvalidOperationID, id)
		}
	}
	return nil
}

// SwitchBinding describes a FUSE mount's active view binding in SwitchJournal
// and local control responses. LayerID is empty for main and MUST be a
// canonical server-generated layer_id (never a name or tag ref) otherwise.
type SwitchBinding struct {
	LayerID      string `json:"layer_id,omitempty"`
	Fence        int64  `json:"fence,omitempty"`
	BaseRootPath string `json:"base_root_path,omitempty"`
	ReadOnly     bool   `json:"read_only,omitempty"`
}

// Validate checks v36 §6.3/§6.5 invariants on a persisted or wire SwitchBinding.
func (b SwitchBinding) Validate() error {
	if b.LayerID == "" {
		if b.Fence != 0 || b.BaseRootPath != "" {
			return fmt.Errorf("%w: main binding must not carry layer fence or base_root_path", viewprotocol.ErrInvalidFence)
		}
		return nil
	}
	if err := viewprotocol.ValidateLayerID(b.LayerID); err != nil {
		return err
	}
	if b.BaseRootPath != "" {
		canon, err := viewprotocol.CanonicalizeViewPath(b.BaseRootPath)
		if err != nil || canon != b.BaseRootPath {
			return fmt.Errorf("invalid canonical base_root_path %q", b.BaseRootPath)
		}
	}
	if b.ReadOnly {
		if b.Fence != 0 {
			return fmt.Errorf("%w: read-only layer binding must have fence 0", viewprotocol.ErrInvalidFence)
		}
		return nil
	}
	if b.Fence <= 0 || b.Fence%2 == 0 {
		return fmt.Errorf("%w: writable layer binding requires positive odd fence, got %d", viewprotocol.ErrInvalidFence, b.Fence)
	}
	return nil
}

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
	if err := ValidateOperationID(u.MountID); err != nil {
		return fmt.Errorf("%w: mount_id: %v", ErrSwitchInvalid, err)
	}
	switch u.Kind {
	case UnbindKindOp:
		if err := ValidateOperationID(u.OperationID); err != nil {
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
	case UnbindKindExpire:
		if u.OperationID != "" || u.Fence != 0 {
			return fmt.Errorf("%w: expire unbind must not carry operation_id or fence", ErrSwitchInvalid)
		}
	default:
		return fmt.Errorf("%w: unsupported unbind kind %q", ErrSwitchInvalid, u.Kind)
	}
	return nil
}

// SwitchReceipt describes a durable binding decision in SwitchJournal.
type SwitchReceipt struct {
	// Recovery is reconstructed from the preceding abort_pending journal record.
	Recovery bool `json:"-"`
	// RequestedLayerRef records the original selector locally for retries after a layer is deleted.
	RequestedLayerRef string        `json:"-"`
	OperationID       string        `json:"operation_id"`
	FromGeneration    uint64        `json:"from_generation"`
	Generation        uint64        `json:"generation"`
	From              SwitchBinding `json:"from"`
	Target            SwitchBinding `json:"target"`
	State             string        `json:"state"`
}

// SwitchSnapshot reports the durable state of a SwitchJournal.
type SwitchSnapshot struct {
	MountID        string          `json:"mount_id"`
	Generation     uint64          `json:"generation"`
	Binding        SwitchBinding   `json:"binding"`
	Pending        *SwitchReceipt  `json:"pending,omitempty"`
	PendingUnbinds []PendingUnbind `json:"pending_unbinds,omitempty"`
	Recovering     bool            `json:"recovering,omitempty"`
}
