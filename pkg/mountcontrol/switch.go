package mountcontrol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

var (
	ErrSwitchUnsupported = errors.New("mount does not support view switching")
	ErrSwitchRecovering  = errors.New("mount switch is recovering")
)

// SwitchRequest binds one operation to a specific mount incarnation. Reuse all
// fields after an unknown outcome; a new operation ID could perform another
// switch. Target's zero value explicitly selects main.
type SwitchRequest struct {
	MountID            string               `json:"mount_id"`
	OperationID        string               `json:"operation_id"`
	ExpectedGeneration uint64               `json:"expected_generation"`
	Target             viewprotocol.Binding `json:"target"`
}

func (r SwitchRequest) Validate() error {
	if viewprotocol.ValidateOperationID(r.MountID) != nil || viewprotocol.ValidateOperationID(r.OperationID) != nil || r.ExpectedGeneration == 0 || r.ExpectedGeneration == math.MaxUint64 || r.Target.Validate() != nil {
		return fmt.Errorf("invalid mount switch request")
	}
	return nil
}

// SwitchResponse always echoes the mount and operation identity. Receipt is
// absent for a pre-decision refusal. RECOVERING includes the selected target
// even when installation or cache invalidation has not finished.
type SwitchResponse struct {
	MountID     string                    `json:"mount_id"`
	OperationID string                    `json:"operation_id"`
	Receipt     *mountstate.SwitchReceipt `json:"receipt,omitempty"`
	ErrorKind   string                    `json:"error_kind,omitempty"`
	Error       string                    `json:"error,omitempty"`
}

// SwitchError preserves the original identity even when the response was lost
// or malformed. Unknown means the caller must query/retry that operation, not
// assume the old binding or rerun an Agent callback. A failure before this
// attempt sends bytes still cannot resolve an earlier attempt with the same ID.
type SwitchError struct {
	MountID     string
	OperationID string
	Unknown     bool
	Cause       error
}

func (e *SwitchError) Error() string {
	return fmt.Sprintf("mount switch %s/%s: %v", e.MountID, e.OperationID, e.Cause)
}
func (e *SwitchError) Unwrap() error { return e.Cause }

// RequestSwitch sends exactly one switch after checking protocol support.
// It performs no automatic retry. A same-connection status check prevents old mount
// daemons (which treated unknown operations as drain) from receiving this op.
func RequestSwitch(ctx context.Context, socketPath string, req SwitchRequest) (*SwitchResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, switchError(req, false, err)
	}
	ctx, cancel := switchContext(ctx)
	defer cancel()
	return exchangeSwitch(ctx, socketPath, "switch", req)
}

// QuerySwitch queries the original operation with the original request identity.
// It cannot apply a switch. Keeping the complete request allows validation of
// the returned receipt, including the target and expected generation.
func QuerySwitch(ctx context.Context, socketPath string, req SwitchRequest) (*SwitchResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, switchError(req, false, err)
	}
	ctx, cancel := switchContext(ctx)
	defer cancel()
	return exchangeSwitch(ctx, socketPath, "switch-operation", req)
}

// Preserve an explicit caller budget; only callers without a deadline use
// the default. The remaining budget is also sent to the mount daemon.
func switchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, DefaultDrainTimeout)
}

func switchError(req SwitchRequest, unknown bool, err error) *SwitchError {
	return &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: unknown, Cause: err}
}

func exchangeSwitch(ctx context.Context, socketPath, op string, req SwitchRequest) (*SwitchResponse, error) {
	wire := struct {
		DrainRequest
		SwitchRequest
	}{DrainRequest: DrainRequest{Op: op}, SwitchRequest: req}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, switchError(req, true, err)
	}
	defer func() { _ = conn.Close() }()
	// Context cancellation must interrupt response reads even without a
	// caller deadline. stop joins no work; closing the connection is safe.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, switchError(req, true, err)
		}
	}
	// Keep negotiation and operation on this exact stream. Reconnecting
	// would let a replacement daemon receive an op it never advertised.
	reader := bufio.NewReaderSize(conn, 64<<10)
	if err := json.NewEncoder(conn).Encode(DrainRequest{Op: "ping"}); err != nil {
		return nil, switchError(req, true, err)
	}
	statusFrame, err := reader.ReadSlice('\n')
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, switchError(req, true, err)
	}
	var status StatusResponse
	if err := json.Unmarshal(statusFrame, &status); err != nil {
		return nil, switchError(req, true, err)
	}
	if !status.OK || !status.ViewSwitch {
		return nil, switchError(req, true, ErrSwitchUnsupported)
	}
	if status.MountID != req.MountID {
		return nil, switchError(req, true, errors.New("mount incarnation changed"))
	}
	if err := ctx.Err(); err != nil {
		return nil, switchError(req, true, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		// Round up sub-millisecond budgets so zero never selects the default.
		wire.TimeoutMS = int64(max(1, (time.Until(deadline)+time.Millisecond-1)/time.Millisecond))
	}
	if err := json.NewEncoder(conn).Encode(wire); err != nil {
		return nil, switchError(req, true, err)
	}
	// A switch peer closes the stream after its final response. Read the
	// complete bounded frame: a valid JSON prefix alone proves nothing
	// about a truncated, extended or still-incomplete response.
	data, err := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, switchError(req, true, err)
	}
	if len(data) > 64<<10 {
		return nil, switchError(req, true, errors.New("mount switch response exceeds limit"))
	}
	var resp SwitchResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, switchError(req, true, err)
	}
	if err := validateSwitchResponse(req, resp); err != nil {
		return nil, switchError(req, true, err)
	}
	if resp.ErrorKind != "" || resp.Error != "" {
		unknown := resp.Receipt != nil
		if resp.Receipt == nil {
			switch resp.ErrorKind {
			case "BUSY", "INVALID", "CONFLICT", "FORBIDDEN", "UNSUPPORTED":
				unknown = false
			default:
				unknown = true
			}
		}
		return &resp, switchError(req, unknown, fmt.Errorf("%s: %s", resp.ErrorKind, resp.Error))
	}
	if op == "switch" && resp.Receipt.State == mountstate.SwitchRecovering {
		return &resp, switchError(req, true, ErrSwitchRecovering)
	}
	return &resp, nil
}

func validateSwitchResponse(req SwitchRequest, resp SwitchResponse) error {
	if resp.MountID != req.MountID || resp.OperationID != req.OperationID {
		return errors.New("mount switch response identity mismatch")
	}
	r := resp.Receipt
	if r == nil {
		if resp.ErrorKind == "" {
			return errors.New("mount switch response missing receipt")
		}
		return nil
	}
	if r.OperationID != req.OperationID || r.FromGeneration != req.ExpectedGeneration || r.Generation == 0 || r.Generation != req.ExpectedGeneration+1 || r.Target != req.Target || r.From.Validate() != nil {
		return errors.New("mount switch receipt does not match request")
	}
	if r.State != mountstate.SwitchDone && r.State != mountstate.SwitchRecovering {
		return errors.New("invalid mount switch receipt state")
	}
	return nil
}
