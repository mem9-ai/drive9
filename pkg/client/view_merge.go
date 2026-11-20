package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// MergeFixedView conditionally publishes a sealed, validated candidate to main.
// It sends one request. RUNNING is not completion. After an unknown outcome or
// ACCOUNTING_PENDING, retry this method with the original work and operation ID;
// a GET receipt marked DONE proves publication, not accounting completion.
func (c *Client) MergeFixedView(ctx context.Context, work ViewWork, operationID string) (FixedViewOutcome, error) {
	path, err := fixedControlPath(work.LayerID)
	if err != nil {
		return FixedViewOutcome{}, err
	}
	if err := work.Binding().Validate(); err != nil || !work.Validation {
		return FixedViewOutcome{}, fmt.Errorf("merge requires valid validation work")
	}
	if err := viewprotocol.ValidateOperationID(operationID); err != nil {
		return FixedViewOutcome{}, err
	}
	failure := func(err error) (FixedViewOutcome, error) {
		return FixedViewOutcome{}, &ViewOperationError{LayerID: work.LayerID, OperationID: operationID, Err: err}
	}
	data, err := json.Marshal(struct {
		OperationID string   `json:"operation_id"`
		Work        ViewWork `json:"work"`
	}{operationID, work})
	if err != nil {
		return failure(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path+"/merge", bytes.NewReader(data))
	if err != nil {
		return failure(err)
	}
	req.Header.Set("Content-Type", "application/json")
	control, err := c.WithView(ViewBinding{})
	if err != nil {
		return failure(err)
	}
	resp, err := control.doNoRedirect(req)
	if err != nil {
		return failure(err)
	}
	body := resp.Body
	defer func() { _ = body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		resp.Body = io.NopCloser(io.LimitReader(body, 64<<10))
		return failure(readError(resp))
	}
	var out FixedViewOutcome
	if err := decodeFixedResponse(body, &out); err != nil {
		return failure(err)
	}
	valid := out.Layer.LayerID == work.LayerID && resp.Header.Get(viewprotocol.OperationHeader) == operationID
	switch out.State {
	case "RUNNING":
		valid = valid && resp.StatusCode == http.StatusAccepted && out.Layer.Phase == "MERGING" && out.Layer.AdmissionClosed && out.Code == ""
	case "DONE":
		valid = valid && resp.StatusCode == http.StatusOK && out.Layer.Phase == "MERGED" && out.Layer.AdmissionClosed && out.Code == ""
	case "FAILED":
		valid = valid && resp.StatusCode == http.StatusOK && out.Code != ""
	default:
		valid = false
	}
	if !valid {
		return failure(fmt.Errorf("invalid merge receipt; retain the original operation_id"))
	}
	return out, nil
}
