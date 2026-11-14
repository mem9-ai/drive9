package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// PresignFixedExtentPart obtains a create-once capability for one sequential
// extent part. size is the caller's expected byte length, checked against the
// session's response. Repeating these same inputs rechecks live authority and
// reuses the authorization budget; this is not a file publication operation.
func (c *Client) PresignFixedExtentPart(ctx context.Context, uploadID string, number int, size int64, checksum string) (PartURL, error) {
	var out PartURL
	if c == nil || c.view.LayerID == "" || c.view.Validation || c.view.Validate() != nil {
		return out, viewprotocol.ErrBinding
	}
	if number > 256 {
		return out, fmt.Errorf("invalid fixed extent part")
	}
	return c.presignExtentPart(ctx, uploadID, number, size, checksum)
}

func (c *Client) presignExtentPart(ctx context.Context, uploadID string, number int, size int64, checksum string) (PartURL, error) {
	var out PartURL
	if err := viewprotocol.ValidateOperationID(uploadID); err != nil {
		return out, err
	}
	checksum = strings.ToLower(checksum)
	digest, err := hex.DecodeString(checksum)
	if err != nil || len(digest) != 32 || number < 1 || number > 10000 || size <= 0 || size > 64<<20 {
		return out, fmt.Errorf("invalid extent part")
	}
	var payload struct {
		Number   int `json:"part_number"`
		Checksum struct {
			Algorithm string `json:"algorithm"`
			Value     string `json:"value"`
		} `json:"checksum"`
	}
	payload.Number = number
	payload.Checksum.Algorithm, payload.Checksum.Value = "sha256", checksum
	body, err := json.Marshal(payload)
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/uploads/"+uploadID+"/presign", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return out, err
	}
	responseBody := resp.Body
	defer func() { _ = responseBody.Close() }()
	if resp.StatusCode != http.StatusOK {
		resp.Body = io.NopCloser(io.LimitReader(resp.Body, 64<<10))
		return out, readError(resp)
	}
	if err := decodeFixedResponse(resp.Body, &out); err != nil {
		return PartURL{}, err
	}
	expires, err := time.Parse(time.RFC3339Nano, out.ExpiresAt)
	if err != nil || !expires.After(time.Now()) || out.Number != number || out.Size != size || out.ChecksumSHA256 != checksum || out.ChecksumCRC32C != "" ||
		(out.SkipUpload && (out.URL != "" || len(out.Headers) != 0)) || (!out.SkipUpload && out.URL == "") {
		return PartURL{}, fmt.Errorf("invalid extent signing response")
	}
	return out, nil
}
