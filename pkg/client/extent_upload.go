package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func validateExtentUploadPlan(plan *uploadPlanV2, size int64) error {
	if plan == nil || plan.ContentLayout != ContentLayoutExtent || viewprotocol.ValidateOperationID(plan.UploadID) != nil || size <= 0 ||
		(plan.PartSize != 4<<20 && plan.PartSize != 64<<20) || plan.TotalParts < 1 || plan.TotalParts > 10000 ||
		int64(plan.TotalParts) != (size-1)/plan.PartSize+1 || plan.Key != "" || !plan.ChecksumContract.Required {
		return errors.New("invalid extent upload plan")
	}
	deadline, err := time.Parse(time.RFC3339Nano, plan.ExpiresAt)
	if err != nil || !deadline.After(time.Now()) {
		return errors.New("invalid extent upload deadline")
	}
	for _, algorithm := range plan.ChecksumContract.Supported {
		if strings.EqualFold(algorithm, "SHA-256") || strings.EqualFold(algorithm, "SHA256") {
			return nil
		}
	}
	return errors.New("extent upload requires SHA-256")
}

// writeExtentUpload uses the same bounded buffers as multipart, but each part
// is a create-once object identified by its bytes. Complete receives SHA-256
// addresses, never object-store ETags. Fixed views reuse transfer and buffer
// handling, then publish with their separate durable completion protocol.
func (c *Client) writeExtentUpload(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt, size int64, progress ProgressFunc, summary *UploadSummary, tags map[string]string, wholeChecksum string, preCompleteCheck func() error, initiateDuration time.Duration) (err error) {
	if c.view.LayerID != "" {
		id, _ := ctx.Value(viewOperationKey{}).(string)
		if viewprotocol.ValidateOperationID(id) != nil || validateFixedUploadPlan(plan, size) != nil || len(tags) != 0 || wholeChecksum != "" {
			return errors.New("invalid fixed extent upload preparation")
		}
	}
	if err := validateExtentUploadPlan(plan, size); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("extent upload %s: %w", plan.UploadID, err)
		}
	}()
	parallelism := boundedUploadParallelism(plan.PartSize, plan.TotalParts)
	if summary != nil {
		summary.Mode, summary.InitiateSeconds = "extent_v2", initiateDuration.Seconds()
		summary.PartSizeBytes, summary.TotalParts, summary.Parallelism = plan.PartSize, plan.TotalParts, parallelism
	}
	started := time.Now()
	parts := make([]completePart, plan.TotalParts)
	buffers := newUploadBufferPool(plan.PartSize, parallelism)
	group, uploadCtx := errgroup.WithContext(ctx)
	group.SetLimit(parallelism)
	for number := 1; number <= plan.TotalParts; number++ {
		if uploadCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			buffer, err := buffers.get(uploadCtx)
			if err != nil {
				return err
			}
			defer buffers.put(buffer)
			offset := int64(number-1) * plan.PartSize
			data := buffer[:min(plan.PartSize, size-offset)]
			n, err := ra.ReadAt(data, offset)
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("read extent part %d: %w", number, err)
			}
			if n != len(data) {
				return fmt.Errorf("read extent part %d: %w", number, io.ErrUnexpectedEOF)
			}
			digest := sha256.Sum256(data)
			checksum := hex.EncodeToString(digest[:])
			if err := c.uploadExtentPart(uploadCtx, plan.UploadID, number, data, checksum); err != nil {
				return fmt.Errorf("extent part %d: %w", number, err)
			}
			parts[number-1] = completePart{Number: number, ETag: checksum}
			if progress != nil {
				progress(number, plan.TotalParts, int64(len(data)))
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		return err
	}
	if err := runPreCompleteCheck(ctx, preCompleteCheck); err != nil {
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		return err
	}
	if summary != nil {
		summary.UploadedParts, summary.UploadSeconds = plan.TotalParts, time.Since(started).Seconds()
	}
	started = time.Now()
	completeErr := error(nil)
	if c.view.LayerID != "" {
		id, _ := ctx.Value(viewOperationKey{}).(string)
		var receipt FixedUploadCompletion
		receipt, completeErr = c.CompleteFixedExtentUpload(ctx, id, plan.UploadID, parts)
		if completeErr == nil && receipt.Size != size {
			completeErr = markCommitAttempt(&ViewOperationError{LayerID: c.view.LayerID, OperationID: id, Err: errors.New("fixed upload completion size mismatch")})
		}
	} else {
		completeErr = c.completeExtentUpload(ctx, plan.UploadID, parts, tags, wholeChecksum)
	}
	if completeErr != nil {
		// The server may have published despite a lost or malformed response.
		// Keep the session available for query; never initiate again or abort here.
		return completeErr
	}
	if summary != nil {
		summary.CompleteSeconds = time.Since(started).Seconds()
	}
	return nil
}

func (c *Client) uploadExtentPart(ctx context.Context, uploadID string, number int, data []byte, checksum string) error {
	for attempt := 0; attempt < 2; attempt++ {
		part, err := c.presignExtentPart(ctx, uploadID, number, int64(len(data)), checksum)
		if err != nil {
			return err
		}
		if part.SkipUpload {
			return nil
		}
		status, err := c.putExtentPart(ctx, part, data)
		if err != nil {
			return err
		}
		if status >= 200 && status < 300 {
			return nil
		}
		// A concurrent identical PUT can win the create-once race. Rechecking
		// the same checksum lets the server acknowledge that existing block.
		if attempt == 0 && (status == http.StatusForbidden || status == http.StatusPreconditionFailed) {
			continue
		}
		return fmt.Errorf("extent object PUT returned HTTP %d", status)
	}
	return errors.New("extent part retry exhausted")
}

func (c *Client) putExtentPart(ctx context.Context, part PartURL, data []byte) (int, error) {
	u, err := url.Parse(part.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return 0, errors.New("invalid extent object URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, part.URL, bytes.NewReader(data))
	if err != nil {
		return 0, errors.New("invalid extent PUT request")
	}
	for key, value := range part.Headers {
		lower := strings.ToLower(key)
		if isForbiddenPresignedHeader(key) || lower == "cookie" || lower == "proxy-authorization" || strings.HasPrefix(lower, "x-drive9-") || strings.HasPrefix(lower, "x-dat9-") {
			return 0, errors.New("extent signing response contains a forbidden header")
		}
		req.Header.Set(key, value)
	}
	hc := *c.httpClient
	hc.Jar = nil
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		return 0, fmt.Errorf("extent object PUT: %w", err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func (c *Client) completeExtentUpload(ctx context.Context, uploadID string, parts []completePart, tags map[string]string, checksum string) error {
	body, err := json.Marshal(struct {
		Parts    []completePart    `json:"parts"`
		Tags     map[string]string `json:"tags"`
		Checksum string            `json:"checksum_sha256,omitempty"`
	}{parts, tags, checksum})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/uploads/"+uploadID+"/complete", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return markCommitAttempt(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return markCommitAttempt(readError(resp))
	}
	var result struct {
		Status     string `json:"status"`
		Revision   int64  `json:"revision"`
		ResourceID string `json:"resource_id"`
	}
	if err := decodeFixedResponse(resp.Body, &result); err != nil {
		return markCommitAttempt(err)
	}
	if result.Status != "completed" || result.Revision < 1 || result.ResourceID == "" {
		return markCommitAttempt(errors.New("invalid extent completion receipt"))
	}
	return nil
}
