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
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

var (
	ErrSwitchUnsupported = errors.New("mount does not support in-place view switching")
	ErrSwitchRecovering  = errors.New("mount switch is recovering")
)

// SwitchRequest binds one in-place view switch operation to a specific mount
// incarnation and expected generation (v36 §6.3 #2–#4).
type SwitchRequest struct {
	MountID            string `json:"mount_id"`
	OperationID        string `json:"operation_id"`
	ExpectedGeneration uint64 `json:"expected_generation"`
	Layer              string `json:"layer"`
	Recover            bool   `json:"recover,omitempty"`
}

func (r SwitchRequest) NormalizedTarget() (isMain bool, layerRef string, err error) {
	trimmed := strings.TrimSpace(r.Layer)
	if strings.EqualFold(trimmed, "main") {
		return true, "", nil
	}
	norm, nerr := viewprotocol.NormalizeLayerRef(trimmed)
	if nerr != nil {
		return false, "", nerr
	}
	return false, norm, nil
}

func (r SwitchRequest) Validate() error {
	if err := viewprotocol.ValidateOperationID(r.MountID); err != nil {
		return err
	}
	if err := viewprotocol.ValidateOperationID(r.OperationID); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.ExpectedGeneration == math.MaxUint64 {
		return fmt.Errorf("invalid expected_generation %d", r.ExpectedGeneration)
	}
	if _, _, err := r.NormalizedTarget(); err != nil {
		return err
	}
	return nil
}

// SwitchResponse echoes mount and operation identity together with either a
// committed/recovering/revoked receipt or a deterministic pre-decision refusal.
type SwitchResponse struct {
	MountID     string                    `json:"mount_id"`
	OperationID string                    `json:"operation_id"`
	Receipt     *mountstate.SwitchReceipt `json:"receipt,omitempty"`
	Code        string                    `json:"code,omitempty"`
	ErrorKind   string                    `json:"error_kind,omitempty"`
	Error       string                    `json:"error,omitempty"`
}

// SwitchError preserves mount/operation identity and machine code across local
// refusals and ambiguous transport outcomes.
type SwitchError struct {
	MountID     string
	OperationID string
	Code        string
	Unknown     bool
	Cause       error
}

func (e *SwitchError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("mount switch %s/%s (%s): %v", e.MountID, e.OperationID, e.Code, e.Cause)
	}
	return fmt.Sprintf("mount switch %s/%s: %v", e.MountID, e.OperationID, e.Cause)
}

func (e *SwitchError) Unwrap() error { return e.Cause }

func RequestSwitch(ctx context.Context, socketPath string, req SwitchRequest) (*SwitchResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Code: viewprotocol.CodeInvalidArgument, Unknown: false, Cause: err}
	}
	ctx, cancel := switchContext(ctx)
	defer cancel()
	return exchangeSwitch(ctx, socketPath, "switch", req)
}

func QuerySwitch(ctx context.Context, socketPath string, req SwitchRequest) (*SwitchResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Code: viewprotocol.CodeInvalidArgument, Unknown: false, Cause: err}
	}
	ctx, cancel := switchContext(ctx)
	defer cancel()
	return exchangeSwitch(ctx, socketPath, "switch-operation", req)
}

func switchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, DefaultDrainTimeout)
}

func exchangeSwitch(ctx context.Context, socketPath, op string, req SwitchRequest) (*SwitchResponse, error) {
	wire := struct {
		DrainRequest
		SwitchRequest
	}{DrainRequest: DrainRequest{Op: op}, SwitchRequest: req}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
		}
	}

	reader := bufio.NewReaderSize(conn, 64<<10)
	if err := json.NewEncoder(conn).Encode(DrainRequest{Op: "ping"}); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	statusFrame, err := reader.ReadSlice('\n')
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	var status StatusResponse
	if err := json.Unmarshal(statusFrame, &status); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	if !status.OK || !status.ViewSwitch {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Code: viewprotocol.CodeSwitchUnsupported, Unknown: false, Cause: ErrSwitchUnsupported}
	}
	if status.MountID != req.MountID {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Code: viewprotocol.CodeConflict, Unknown: false, Cause: errors.New("mount incarnation changed")}
	}
	if err := ctx.Err(); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: false, Cause: err}
	}
	if deadline, ok := ctx.Deadline(); ok {
		wire.TimeoutMS = int64(max(1, (time.Until(deadline)+time.Millisecond-1)/time.Millisecond))
	}
	if err := json.NewEncoder(conn).Encode(wire); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}

	data, err := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	if len(data) > 64<<10 {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: errors.New("mount switch response exceeds limit")}
	}
	var resp SwitchResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	if err := validateSwitchResponse(req, resp); err != nil {
		return nil, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Unknown: true, Cause: err}
	}
	if resp.Code != "" || resp.ErrorKind != "" || resp.Error != "" {
		unknown := isUnknownSwitchOutcome(resp)
		code := resp.Code
		if code == "" {
			code = resp.ErrorKind
		}
		return &resp, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Code: code, Unknown: unknown, Cause: fmt.Errorf("%s: %s", code, resp.Error)}
	}
	if op == "switch" && resp.Receipt != nil && resp.Receipt.State == mountstate.SwitchRecovering {
		return &resp, &SwitchError{MountID: req.MountID, OperationID: req.OperationID, Code: viewprotocol.CodeSwitchRecovering, Unknown: true, Cause: ErrSwitchRecovering}
	}
	return &resp, nil
}

func isUnknownSwitchOutcome(resp SwitchResponse) bool {
	if resp.Receipt != nil {
		return resp.Receipt.State == mountstate.SwitchRecovering
	}
	switch resp.ErrorKind {
	case "BUSY", "INVALID", "CONFLICT", "FORBIDDEN", "UNSUPPORTED", "NOT_FOUND", "PREPARE_FAILED", "CLEANUP_PENDING", "TARGET_REVOKED":
		return false
	default:
		return true
	}
}

func validateSwitchResponse(req SwitchRequest, resp SwitchResponse) error {
	if resp.MountID != req.MountID || resp.OperationID != req.OperationID {
		return errors.New("mount switch response identity mismatch")
	}
	r := resp.Receipt
	if r == nil {
		if resp.ErrorKind == "" && resp.Code == "" {
			return errors.New("mount switch response missing receipt")
		}
		return nil
	}
	if r.OperationID == "" || r.Generation == 0 || r.From.Validate() != nil || r.Target.Validate() != nil {
		return errors.New("mount switch receipt invalid")
	}
	if !req.Recover && (r.OperationID != req.OperationID || r.FromGeneration != req.ExpectedGeneration || r.Generation != req.ExpectedGeneration+1) {
		return errors.New("mount switch receipt does not match request")
	}
	switch r.State {
	case mountstate.SwitchDone, mountstate.SwitchRecovering, mountstate.SwitchTargetRevoked:
		return nil
	default:
		return errors.New("invalid mount switch receipt state")
	}
}
