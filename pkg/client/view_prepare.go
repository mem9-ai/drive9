package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// ErrViewLaunchUnavailable means the server did not grant the unique direct
// response right to execute a callback. Receipt recovery cannot grant that right.
var ErrViewLaunchUnavailable = errors.New("view work was already claimed; do not rerun the callback")

// FixedViewPrepareRequest uses caller-owned, durable operation IDs. Keep this
// request when an outcome is unknown; generating new IDs is not recovery.
type FixedViewPrepareRequest struct {
	Create                FixedViewCreate
	WorkOperationID       string
	SealOperationID       string
	ValidationOperationID string
	WorkTTL               time.Duration
	ValidationTTL         time.Duration
}

// FixedViewPreparation identifies the last attempted stage, including on error.
// Validated is a successful callback result, not permission to publish: final
// merge must still check authority, lease, fence, cancellation and dependencies.
type FixedViewPreparation struct {
	LayerID     string
	Stage       string
	OperationID string
	Work        ViewWork
	Seal        FixedViewOutcome
	Validation  ViewWork
	Validated   bool
}

// ViewWorkFunc must finish its requests and join its tool goroutines before
// returning. It can perform multiple edits or explicitly renew work through the
// bound client. Returning an error keeps the candidate for explicit inspection;
// it does not cancel, delete, retry, or publish it.
type ViewWorkFunc func(context.Context, *Client, ViewWork) error

// PrepareFixedView creates a fixed candidate, executes work once, seals it and
// runs readonly validation once. Each callback requires its own fresh Launch
// response. There is no automatic retry, polling, renewal or filesystem merge.
//
// A RUNNING seal stops preparation; query/replay its original operation to
// finish sealing, then explicitly claim validation. Recalling this whole method
// never resumes callbacks: the repeated work claim returns Launch=false.
// External side effects performed by callbacks are not rolled back.
func (c *Client) PrepareFixedView(ctx context.Context, req FixedViewPrepareRequest, work, validate ViewWorkFunc) (FixedViewPreparation, error) {
	out := FixedViewPreparation{LayerID: req.Create.LayerID, Stage: "create", OperationID: req.Create.OperationID}
	if err := validateFixedViewPreparation(req, work, validate); err != nil {
		return out, err
	}
	created, err := c.CreateFixedView(ctx, req.Create)
	if err != nil {
		return out, err
	}
	if created.LayerID != out.LayerID || created.Phase != "OPEN" || created.AdmissionClosed || created.Generation < 0 || created.Fence < 0 {
		return out, &ViewOperationError{LayerID: out.LayerID, OperationID: out.OperationID, Err: fmt.Errorf("invalid create receipt")}
	}

	out.Stage, out.OperationID = "work", req.WorkOperationID
	out.Work, err = c.runFixedViewCallback(ctx, out.LayerID, out.OperationID, req.WorkTTL, false, created.Fence, work)
	if err != nil {
		return out, err
	}

	out.Stage, out.OperationID = "seal", req.SealOperationID
	out.Seal, err = c.SealFixedView(ctx, out.Work, out.OperationID)
	if err != nil {
		return out, err
	}
	sealed := out.Seal.Layer
	if out.Seal.State != "DONE" || out.Seal.Code != "" || sealed.LayerID != out.LayerID || sealed.Phase != "READY" ||
		!sealed.AdmissionClosed || sealed.Fence != out.Work.Fence || sealed.Generation < created.Generation {
		return out, &ViewOperationError{LayerID: out.LayerID, OperationID: out.OperationID,
			Err: fmt.Errorf("seal did not confirm a ready candidate (state=%q code=%q)", out.Seal.State, out.Seal.Code)}
	}

	out.Stage, out.OperationID = "validation", req.ValidationOperationID
	out.Validation, err = c.runFixedViewCallback(ctx, out.LayerID, out.OperationID, req.ValidationTTL, true, out.Work.Fence, validate)
	if err != nil {
		return out, err
	}
	out.Validated = true
	return out, nil
}

func validateFixedViewPreparation(req FixedViewPrepareRequest, work, validate ViewWorkFunc) error {
	if work == nil || validate == nil {
		return fmt.Errorf("work and validation callbacks are required")
	}
	if _, err := fixedControlPath(req.Create.LayerID); err != nil {
		return err
	}
	if req.Create.ParentLayerID != "" {
		if _, err := fixedControlPath(req.Create.ParentLayerID); err != nil {
			return err
		}
	}
	seen := make(map[string]bool, 4)
	for _, id := range []string{req.Create.OperationID, req.WorkOperationID, req.SealOperationID, req.ValidationOperationID} {
		if err := viewprotocol.ValidateOperationID(id); err != nil {
			return err
		}
		if seen[id] {
			return fmt.Errorf("preparation stages require distinct operation IDs")
		}
		seen[id] = true
	}
	for _, ttl := range []time.Duration{req.WorkTTL, req.ValidationTTL} {
		if ttl < time.Millisecond || ttl > time.Hour {
			return fmt.Errorf("ttl must be between 1ms and 1h")
		}
	}
	return nil
}

func (c *Client) runFixedViewCallback(ctx context.Context, layerID, operationID string, ttl time.Duration, validation bool, previousFence int64, run ViewWorkFunc) (ViewWork, error) {
	claim, err := c.BeginViewWork(ctx, layerID, operationID, ttl, validation)
	if err != nil {
		return ViewWork{}, err
	}
	if !claim.Launch {
		return claim.Work, &ViewOperationError{LayerID: layerID, OperationID: operationID, Err: ErrViewLaunchUnavailable}
	}
	if claim.Work.Fence <= previousFence {
		return ViewWork{}, &ViewOperationError{LayerID: layerID, OperationID: operationID, Err: fmt.Errorf("work claim did not advance the fence")}
	}
	bound, err := c.WithView(claim.Work.Binding())
	if err != nil {
		return claim.Work, err
	}
	if err := ctx.Err(); err != nil {
		return claim.Work, err
	}
	callbackCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return claim.Work, run(callbackCtx, bound, claim.Work)
}
