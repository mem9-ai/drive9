package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// FixedViewState is a control result, not a permission to launch or replay work.
type FixedViewState struct {
	LayerID         string `json:"layer_id"`
	Phase           string `json:"phase"`
	Generation      int64  `json:"generation"`
	Fence           int64  `json:"fence"`
	AdmissionClosed bool   `json:"admission_closed"`
}

type FixedViewInfo struct {
	LayerID       string    `json:"layer_id"`
	Name          string    `json:"name"`
	ParentLayerID string    `json:"parent_layer_id,omitempty"`
	Phase         string    `json:"phase"`
	Generation    int64     `json:"generation"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type FixedViewPage struct {
	Layers    []FixedViewInfo `json:"layers"`
	NextAfter string          `json:"next_after,omitempty"`
}

type FixedViewCreate struct {
	OperationID   string `json:"operation_id"`
	LayerID       string `json:"layer_id"`
	ParentLayerID string `json:"parent_layer_id,omitempty"`
	Name          string `json:"name,omitempty"`
}

// ViewWork describes a server-owned generation. LeaseUntil is informative;
// extending it locally does not extend server eligibility.
type ViewWork struct {
	LayerID    string    `json:"layer_id"`
	ID         string    `json:"work_id"`
	Fence      int64     `json:"fence"`
	LeaseUntil time.Time `json:"lease_until"`
	Validation bool      `json:"validation"`
}

func (w ViewWork) Binding() ViewBinding {
	return ViewBinding{LayerID: w.LayerID, WorkID: w.ID, Fence: w.Fence, Validation: w.Validation}
}

type ViewWorkClaim struct {
	Work   ViewWork `json:"work"`
	Launch bool     `json:"launch"`
}

type FixedViewOutcome struct {
	State string         `json:"state"`
	Code  string         `json:"code,omitempty"`
	Layer FixedViewState `json:"layer"`
}

type FixedViewOperation struct {
	OperationID string          `json:"operation_id"`
	LayerID     string          `json:"layer_id"`
	Kind        string          `json:"kind"`
	State       string          `json:"state"`
	Code        string          `json:"code,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
}

// doViewControl always addresses the explicitly named view, independent of the
// receiver's file binding. It performs exactly one request: unknown outcomes
// must be reconciled by the caller with the original operation ID.
func (c *Client) doViewControl(ctx context.Context, method, path string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	control, err := c.WithView(ViewBinding{})
	if err != nil {
		return err
	}
	resp, err := control.doNoRedirect(req)
	if err != nil {
		return err
	}
	bodyReader := resp.Body
	defer func() { _ = bodyReader.Close() }()
	if resp.StatusCode != http.StatusOK {
		resp.Body = io.NopCloser(io.LimitReader(resp.Body, 64<<10))
		return readError(resp)
	}
	if err := decodeFixedResponse(resp.Body, out); err != nil {
		return fmt.Errorf("decode view control response: %w", err)
	}
	return nil
}

// Control requests use the caller's stable identity and never retry. The
// receiver may itself be bound to a different view, so do not derive error
// identity from c.view.
func (c *Client) doFixedControlMutation(ctx context.Context, layer, operation, method, path string, body, out any) error {
	if err := viewprotocol.ValidateOperationID(operation); err != nil {
		return err
	}
	if err := c.doViewControl(ctx, method, path, body, out); err != nil {
		return &ViewOperationError{LayerID: layer, OperationID: operation, Err: err}
	}
	return nil
}

func fixedControlPath(id string) (string, error) {
	if err := (ViewBinding{LayerID: id, WorkID: "control", Fence: 1}).Validate(); err != nil || id == "" {
		return "", fmt.Errorf("invalid layer id")
	}
	return "/v1/views/" + id, nil
}

func (c *Client) CreateFixedView(ctx context.Context, req FixedViewCreate) (FixedViewState, error) {
	var out FixedViewState
	if _, err := fixedControlPath(req.LayerID); err != nil {
		return out, err
	}
	if req.ParentLayerID != "" {
		if _, err := fixedControlPath(req.ParentLayerID); err != nil {
			return out, err
		}
	}
	err := c.doFixedControlMutation(ctx, req.LayerID, req.OperationID, "POST", "/v1/views", req, &out)
	return out, err
}
func (c *Client) ListFixedViews(ctx context.Context, after string, limit int) (FixedViewPage, error) {
	var out FixedViewPage
	if limit < 1 || limit > 256 {
		return out, fmt.Errorf("limit must be between 1 and 256")
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if after != "" {
		if _, err := fixedControlPath(after); err != nil {
			return out, err
		}
		q.Set("after", after)
	}
	err := c.doViewControl(ctx, "GET", "/v1/views?"+q.Encode(), nil, &out)
	return out, err
}
func (c *Client) GetFixedView(ctx context.Context, id string) (FixedViewInfo, error) {
	var out FixedViewInfo
	path, err := fixedControlPath(id)
	if err != nil {
		return out, err
	}
	err = c.doViewControl(ctx, "GET", path, nil, &out)
	return out, err
}
func (c *Client) BeginViewWork(ctx context.Context, id, operationID string, ttl time.Duration, validation bool) (ViewWorkClaim, error) {
	var out ViewWorkClaim
	path, err := fixedControlPath(id)
	if err != nil {
		return out, err
	}
	if ttl < time.Millisecond || ttl > time.Hour {
		return out, fmt.Errorf("ttl must be between 1ms and 1h")
	}
	req := struct {
		OperationID string `json:"operation_id"`
		TTLMillis   int64  `json:"ttl_ms"`
		Validation  bool   `json:"validation"`
	}{operationID, ttl.Milliseconds(), validation}
	var receipt struct {
		Work   ViewWork `json:"work"`
		Launch *bool    `json:"launch"`
	}
	if err = c.doFixedControlMutation(ctx, id, operationID, "POST", path+"/work", req, &receipt); err != nil {
		return ViewWorkClaim{}, err
	}
	if receipt.Launch == nil || receipt.Work.LayerID != id || receipt.Work.Validation != validation || receipt.Work.LeaseUntil.IsZero() || receipt.Work.Binding().Validate() != nil {
		return ViewWorkClaim{}, &ViewOperationError{LayerID: id, OperationID: operationID, Err: fmt.Errorf("invalid work claim receipt; query the original operation_id without launching work")}
	}
	return ViewWorkClaim{Work: receipt.Work, Launch: *receipt.Launch}, nil
}
func (c *Client) RenewViewWork(ctx context.Context, w ViewWork, ttl time.Duration) (ViewWork, error) {
	var out ViewWork
	path, err := fixedControlPath(w.LayerID)
	if err != nil {
		return out, err
	}
	if err = w.Binding().Validate(); err != nil {
		return out, err
	}
	if ttl < time.Millisecond || ttl > time.Hour {
		return out, fmt.Errorf("ttl must be between 1ms and 1h")
	}
	req := struct {
		Work      ViewWork `json:"work"`
		TTLMillis int64    `json:"ttl_ms"`
	}{w, ttl.Milliseconds()}
	if err = c.doViewControl(ctx, "POST", path+"/renew", req, &out); err != nil {
		return ViewWork{}, err
	}
	if out.LayerID != w.LayerID || out.ID != w.ID || out.Fence != w.Fence || out.Validation != w.Validation || out.LeaseUntil.IsZero() {
		return ViewWork{}, fmt.Errorf("renewal response does not identify the original work generation")
	}
	return out, nil
}
func (c *Client) SealFixedView(ctx context.Context, w ViewWork, operationID string) (FixedViewOutcome, error) {
	var out FixedViewOutcome
	path, err := fixedControlPath(w.LayerID)
	if err != nil {
		return out, err
	}
	if err = w.Binding().Validate(); err != nil {
		return out, err
	}
	req := struct {
		OperationID string   `json:"operation_id"`
		Work        ViewWork `json:"work"`
	}{operationID, w}
	err = c.doFixedControlMutation(ctx, w.LayerID, operationID, "POST", path+"/seal", req, &out)
	return out, err
}
func (c *Client) EndFixedView(ctx context.Context, id, operationID string, remove bool) (FixedViewOutcome, error) {
	var out FixedViewOutcome
	path, err := fixedControlPath(id)
	if err != nil {
		return out, err
	}
	method := "POST"
	if remove {
		method = "DELETE"
	} else {
		path += "/cancel"
	}
	err = c.doFixedControlMutation(ctx, id, operationID, method, path, struct {
		OperationID string `json:"operation_id"`
	}{operationID}, &out)
	return out, err
}
func (c *Client) GetFixedViewOperation(ctx context.Context, id, operationID string) (FixedViewOperation, error) {
	var out FixedViewOperation
	path, err := fixedControlPath(id)
	if err != nil {
		return out, err
	}
	if err := viewprotocol.ValidateOperationID(operationID); err != nil {
		return out, err
	}
	err = c.doViewControl(ctx, "GET", path+"/operations/"+url.PathEscape(operationID), nil, &out)
	if err == nil && (out.LayerID != id || out.OperationID != operationID || out.Kind == "" ||
		out.State != "RUNNING" && out.State != "DONE" && out.State != "FAILED") {
		err = fmt.Errorf("invalid operation receipt for the requested view and operation")
	}
	if err != nil {
		return FixedViewOperation{}, &ViewOperationError{LayerID: id, OperationID: operationID, Err: err}
	}
	return out, nil
}
