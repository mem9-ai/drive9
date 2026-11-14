package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type FixedUploadOutcome struct {
	OperationID string `json:"operation_id"`
	UploadID    string `json:"upload_id"`
	Status      string `json:"status"`
}

// AbortFixedUpload withdraws an extent session's publication right. COMPLETED
// explicitly means the file was already published and remains present. A lost
// response retains operationID for reconciliation; this method never retries.
func (c *Client) AbortFixedUpload(ctx context.Context, operationID, uploadID string) (FixedUploadOutcome, error) {
	var out FixedUploadOutcome
	if c == nil || c.view.LayerID == "" || c.view.Validation || c.view.Validate() != nil {
		return out, viewprotocol.ErrBinding
	}
	if err := viewprotocol.ValidateOperationID(operationID); err != nil {
		return out, err
	}
	if err := viewprotocol.ValidateOperationID(uploadID); err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/uploads/"+uploadID+"/abort", nil)
	if err != nil {
		return out, err
	}
	req.Header.Set(viewprotocol.OperationHeader, operationID)
	resp, err := c.do(req)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := c.decodeViewMutation(req, resp, &out); err != nil {
		return FixedUploadOutcome{}, err
	}
	if resp.StatusCode != http.StatusOK || out.OperationID != operationID || out.UploadID != uploadID ||
		(out.Status != "ABORTED" && out.Status != "COMPLETED" && out.Status != "EXPIRED") {
		return FixedUploadOutcome{}, c.viewMutationError(req, fmt.Errorf("invalid fixed upload outcome"))
	}
	return out, nil
}
