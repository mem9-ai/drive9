package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// ChmodFixedFile changes only the identified file revision. Retain operationID
// and all inputs before calling; an unknown result is reconciled by replaying
// this request, never by looking up and adopting a newer file at the same path.
func (c *Client) ChmodFixedFile(ctx context.Context, operationID, path, resourceID string, revision int64, mode uint32) (int64, error) {
	if c == nil || c.view.LayerID == "" || c.view.Validation || c.view.Validate() != nil {
		return 0, viewprotocol.ErrBinding
	}
	if viewprotocol.ValidateOperationID(operationID) != nil || viewprotocol.ValidateOperationID(resourceID) != nil || revision <= 0 {
		return 0, fmt.Errorf("invalid fixed file mode identity")
	}
	ctx, err := WithViewOperationID(ctx, operationID)
	if err != nil {
		return 0, err
	}
	body, err := json.Marshal(struct {
		Mode       uint32 `json:"mode"`
		ResourceID string `json:"resource_id"`
		Revision   int64  `json:"expected_revision"`
	}{mode, resourceID, revision})
	if err != nil {
		return 0, err
	}
	req, err := c.newFSRequest(ctx, http.MethodPost, path, "?chmod", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return 0, markCommitAttempt(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Status      string         `json:"status"`
		OperationID string         `json:"operation_id"`
		View        FixedViewState `json:"view"`
		ResourceID  string         `json:"resource_id"`
		Revision    int64          `json:"revision"`
		Size        *int64         `json:"size_bytes"`
	}
	if err := c.decodeViewMutation(req, resp, &out); err != nil {
		return 0, markCommitAttempt(err)
	}
	if resp.StatusCode != http.StatusOK || out.Status != "ok" || out.OperationID != operationID || out.View.LayerID != c.view.LayerID || out.View.Fence != c.view.Fence || out.View.Generation < 1 || out.ResourceID != resourceID || out.Revision < revision || out.Revision-revision > 1 || out.Size == nil || *out.Size < 0 {
		return 0, markCommitAttempt(c.viewMutationError(req, fmt.Errorf("invalid fixed file mode receipt")))
	}
	return out.Revision, nil
}
