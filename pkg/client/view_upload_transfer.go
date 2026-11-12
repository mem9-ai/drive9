package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// FixedExtentUploadPlan is the original server-owned transfer plan. Replaying
// initiation can return an expired plan; it never extends upload authority.
type FixedExtentUploadPlan = uploadPlanV2

// FixedExtentPart identifies one sequential part by its SHA-256 address, not an
// object-store ETag. Size and offsets are determined by the saved upload plan.
type FixedExtentPart = completePart

type FixedUploadCompletion struct {
	Status      string         `json:"status"`
	OperationID string         `json:"operation_id"`
	View        FixedViewState `json:"view"`
	ResourceID  string         `json:"resource_id"`
	Revision    int64          `json:"revision"`
	Size        int64          `json:"size_bytes"`
}

// InitiateFixedExtentUpload sends one request using an explicit durable ID.
// expectedRevision=-1 permits create/overwrite, zero requires absence. The
// returned plan can be used with PresignFixedExtentPart. On unknown outcome,
// retry these identical inputs; do not replace operationID.
func (c *Client) InitiateFixedExtentUpload(ctx context.Context, operationID, path string, size, expectedRevision int64, resourceID string) (FixedExtentUploadPlan, error) {
	var out FixedExtentUploadPlan
	if c == nil || c.view.LayerID == "" || c.view.Validation || c.view.Validate() != nil {
		return out, viewprotocol.ErrBinding
	}
	if err := viewprotocol.ValidateOperationID(operationID); err != nil {
		return out, err
	}
	if err := validateFSPath(path); err != nil {
		return out, err
	}
	if path == "/" || strings.HasSuffix(path, "/") || size <= 0 || size > 256*(64<<20) || expectedRevision < -1 || resourceID != "" && viewprotocol.ValidateOperationID(resourceID) != nil {
		return out, fmt.Errorf("invalid fixed extent upload parameters")
	}
	body, err := json.Marshal(struct {
		Path             string `json:"path"`
		Size             int64  `json:"total_size"`
		ExpectedRevision *int64 `json:"expected_revision,omitempty"`
	}{path, size, expectedRevisionField(expectedRevision)})
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/uploads/initiate", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(viewprotocol.OperationHeader, operationID)
	if resourceID != "" {
		req.Header.Set("X-Dat9-Expected-Resource", resourceID)
	}
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := c.decodeViewMutation(req, resp, &out); err != nil {
		return FixedExtentUploadPlan{}, err
	}
	if resp.StatusCode != http.StatusAccepted || validateFixedUploadPlan(&out, size) != nil {
		return FixedExtentUploadPlan{}, c.viewMutationError(req, fmt.Errorf("invalid fixed extent upload plan"))
	}
	return out, nil
}

func validateFixedUploadPlan(p *uploadPlanV2, size int64) error {
	if p == nil || viewprotocol.ValidateOperationID(p.UploadID) != nil || p.ContentLayout != ContentLayoutExtent || p.Key != "" || p.Resumable || size <= 0 ||
		(p.PartSize != 4<<20 && p.PartSize != 64<<20) || p.TotalParts < 1 || p.TotalParts > 256 || int64(p.TotalParts) != (size-1)/p.PartSize+1 || !p.ChecksumContract.Required {
		return fmt.Errorf("invalid fixed extent plan")
	}
	if _, err := time.Parse(time.RFC3339Nano, p.ExpiresAt); err != nil {
		return err
	}
	for _, algorithm := range p.ChecksumContract.Supported {
		if strings.EqualFold(algorithm, "SHA-256") || strings.EqualFold(algorithm, "SHA256") {
			return nil
		}
	}
	return fmt.Errorf("fixed extent upload requires SHA-256")
}

// CompleteFixedExtentUpload publishes only the saved session's target. A 503
// may mean publication succeeded but reservation release is pending. Retry the
// same operationID, uploadID and parts, or query the original operation; this
// method never aborts, re-initiates, or automatically retries a request.
func (c *Client) CompleteFixedExtentUpload(ctx context.Context, operationID, uploadID string, parts []FixedExtentPart) (FixedUploadCompletion, error) {
	var out FixedUploadCompletion
	if c == nil || c.view.LayerID == "" || c.view.Validation || c.view.Validate() != nil {
		return out, viewprotocol.ErrBinding
	}
	if viewprotocol.ValidateOperationID(operationID) != nil || viewprotocol.ValidateOperationID(uploadID) != nil || len(parts) < 1 || len(parts) > 256 {
		return out, fmt.Errorf("invalid fixed upload completion")
	}
	seen := make(map[int]bool, len(parts))
	for _, part := range parts {
		key, err := hex.DecodeString(part.ETag)
		if err != nil || len(key) != 32 || part.Number < 1 || part.Number > len(parts) || seen[part.Number] {
			return out, fmt.Errorf("invalid fixed extent part")
		}
		seen[part.Number] = true
	}
	body, err := json.Marshal(struct {
		Parts []completePart `json:"parts"`
	}{parts})
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/uploads/"+uploadID+"/complete", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(viewprotocol.OperationHeader, operationID)
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return out, markCommitAttempt(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if err := c.decodeViewMutation(req, resp, &out); err != nil {
		return FixedUploadCompletion{}, markCommitAttempt(err)
	}
	if resp.StatusCode != http.StatusOK || out.Status != "completed" || out.OperationID != operationID || out.View.LayerID != c.view.LayerID || out.View.Fence != c.view.Fence ||
		out.View.Generation < 1 || out.Revision < 1 || out.Size <= 0 || viewprotocol.ValidateOperationID(out.ResourceID) != nil {
		return FixedUploadCompletion{}, markCommitAttempt(c.viewMutationError(req, fmt.Errorf("invalid fixed upload completion receipt")))
	}
	return out, nil
}
