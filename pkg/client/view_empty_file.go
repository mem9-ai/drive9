package client

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// WriteFixedEmptyFile creates or truncates an empty file with an explicit
// replay identity. Save all inputs before calling. The returned file identity
// is the durable PUT receipt (Status is "ok"), not a later pathname lookup.
func (c *Client) WriteFixedEmptyFile(ctx context.Context, operationID, path string, expectedRevision int64) (FixedUploadCompletion, error) {
	if c == nil || c.view.LayerID == "" || c.view.Validation || c.view.Validate() != nil {
		return FixedUploadCompletion{}, viewprotocol.ErrBinding
	}
	if viewprotocol.ValidateOperationID(operationID) != nil || validateFSPath(path) != nil || path == "/" || strings.HasSuffix(path, "/") || expectedRevision < 0 {
		return FixedUploadCompletion{}, fmt.Errorf("invalid fixed empty file request")
	}
	ctx, err := WithViewOperationID(ctx, operationID)
	if err != nil {
		return FixedUploadCompletion{}, err
	}
	req, err := c.newFSRequest(ctx, http.MethodPut, path, "", bytes.NewReader(nil))
	if err != nil {
		return FixedUploadCompletion{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(expectedRevision, 10))
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return FixedUploadCompletion{}, markCommitAttempt(err)
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
		return FixedUploadCompletion{}, markCommitAttempt(err)
	}
	if resp.StatusCode != http.StatusOK || out.Status != "ok" || out.OperationID != operationID || out.View.LayerID != c.view.LayerID || out.View.Fence != c.view.Fence || out.View.Generation < 1 || viewprotocol.ValidateOperationID(out.ResourceID) != nil || out.Revision < 1 || out.Size == nil || *out.Size != 0 {
		return FixedUploadCompletion{}, markCommitAttempt(c.viewMutationError(req, fmt.Errorf("invalid fixed empty file receipt")))
	}
	return FixedUploadCompletion{Status: out.Status, OperationID: out.OperationID, View: out.View, ResourceID: out.ResourceID, Revision: out.Revision, Size: 0}, nil
}
