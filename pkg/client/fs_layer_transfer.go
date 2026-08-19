package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const fsLayerPresignBatchSize = 16

type fsLayerUploadInitiateRequest struct {
	Path         string `json:"path"`
	TotalSize    int64  `json:"total_size"`
	BaseRevision int64  `json:"base_revision,omitempty"`
	Mode         uint32 `json:"mode,omitempty"`
}

func (c *Client) initiateFSLayerUpload(ctx context.Context, layerID, path string, size, baseRevision int64, mode uint32, hasMode bool) (*uploadPlanV2, bool, error) {
	if baseRevision < 0 {
		baseRevision = 0
	}
	if !hasMode {
		mode = 0
	}
	body, err := json.Marshal(fsLayerUploadInitiateRequest{
		Path:         path,
		TotalSize:    size,
		BaseRevision: baseRevision,
		Mode:         mode & 0o777,
	})
	if err != nil {
		return nil, false, err
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/uploads/initiate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, false, fmt.Errorf("initiate fs layer upload: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return nil, true, nil
	}
	if resp.StatusCode != http.StatusAccepted {
		return nil, false, fmt.Errorf("initiate fs layer upload: %w", readError(resp))
	}
	var plan uploadPlanV2
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, false, fmt.Errorf("decode fs layer upload plan: %w", err)
	}
	if err := validateFSLayerUploadPlan(&plan, size); err != nil {
		if plan.UploadID != "" {
			c.abortFSLayerUploadBestEffort(ctx, layerID, plan.UploadID)
		}
		return nil, false, err
	}
	return &plan, false, nil
}

func validateFSLayerUploadPlan(plan *uploadPlanV2, size int64) error {
	if plan.UploadID == "" {
		return fmt.Errorf("invalid fs layer upload plan: upload_id is empty")
	}
	if plan.PartSize <= 0 {
		return fmt.Errorf("invalid fs layer upload plan: part_size must be positive")
	}
	wantParts := (size-1)/plan.PartSize + 1
	if plan.TotalParts <= 0 || int64(plan.TotalParts) != wantParts {
		return fmt.Errorf("invalid fs layer upload plan: total_parts=%d, want %d", plan.TotalParts, wantParts)
	}
	return nil
}

func (c *Client) uploadFSLayerFileDirect(ctx context.Context, layerID string, body io.Reader, size int64, plan *uploadPlanV2) (*FSLayerEntry, error) {
	parts := make([]completePart, 0, plan.TotalParts)
	remaining := size
	for start := 1; start <= plan.TotalParts; start += fsLayerPresignBatchSize {
		end := min(start+fsLayerPresignBatchSize-1, plan.TotalParts)
		expectedSizes := make([]int64, 0, end-start+1)
		batchRemaining := remaining
		for partNumber := start; partNumber <= end; partNumber++ {
			partSize := min(plan.PartSize, batchRemaining)
			expectedSizes = append(expectedSizes, partSize)
			batchRemaining -= partSize
		}
		presigned, err := c.presignFSLayerUploadParts(ctx, layerID, plan.UploadID, start, end, expectedSizes)
		if err != nil {
			c.abortFSLayerUploadBestEffort(ctx, layerID, plan.UploadID)
			return nil, err
		}
		for i, part := range presigned {
			partNumber := start + i
			partSize := expectedSizes[i]
			etag, err := c.uploadFSLayerPart(ctx, part, body, partSize)
			if err != nil {
				c.abortFSLayerUploadBestEffort(ctx, layerID, plan.UploadID)
				return nil, fmt.Errorf("upload fs layer part %d: %w", partNumber, err)
			}
			parts = append(parts, completePart{Number: partNumber, ETag: etag})
			remaining -= partSize
		}
	}
	return c.completeFSLayerUpload(ctx, layerID, plan.UploadID, parts)
}

func (c *Client) presignFSLayerUploadParts(ctx context.Context, layerID, uploadID string, start, end int, expectedSizes []int64) ([]presignedPart, error) {
	entries := make([]struct {
		PartNumber int `json:"part_number"`
	}, 0, end-start+1)
	for partNumber := start; partNumber <= end; partNumber++ {
		entries = append(entries, struct {
			PartNumber int `json:"part_number"`
		}{PartNumber: partNumber})
	}
	body, err := json.Marshal(struct {
		Parts []struct {
			PartNumber int `json:"part_number"`
		} `json:"parts"`
	}{Parts: entries})
	if err != nil {
		return nil, err
	}
	u := c.fsLayerUploadURL(layerID, uploadID, "presign-batch")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("presign fs layer upload parts %d-%d: %w", start, end, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("presign fs layer upload parts %d-%d: %w", start, end, readError(resp))
	}
	var out struct {
		Parts []presignedPart `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer presign response: %w", err)
	}
	if len(out.Parts) != len(entries) {
		return nil, fmt.Errorf("invalid fs layer presign response: got %d parts, want %d", len(out.Parts), len(entries))
	}
	for i, part := range out.Parts {
		partNumber := start + i
		if part.Number != partNumber || part.URL == "" || part.Size != expectedSizes[i] {
			return nil, fmt.Errorf("invalid fs layer presign response for part %d", partNumber)
		}
	}
	return out.Parts, nil
}

func (c *Client) uploadFSLayerPart(ctx context.Context, part presignedPart, body io.Reader, size int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, part.URL, io.LimitReader(body, size))
	if err != nil {
		return "", err
	}
	for key, value := range part.Headers {
		if strings.EqualFold(key, "host") {
			continue
		}
		req.Header.Set(key, value)
	}
	req.ContentLength = size
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusForbidden {
		return "", errPresignExpired
	}
	if resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(responseBody))
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("S3 upload response omitted ETag")
	}
	return etag, nil
}

func (c *Client) completeFSLayerUpload(ctx context.Context, layerID, uploadID string, parts []completePart) (*FSLayerEntry, error) {
	body, err := json.Marshal(struct {
		Parts []completePart `json:"parts"`
	}{Parts: parts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.fsLayerUploadURL(layerID, uploadID, "complete"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, markCommitAttempt(fmt.Errorf("complete fs layer upload: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, markCommitAttempt(fmt.Errorf("complete fs layer upload: %w", readError(resp)))
	}
	var entry FSLayerEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		return nil, markCommitAttempt(fmt.Errorf("decode completed fs layer entry: %w", err))
	}
	return &entry, nil
}

func (c *Client) abortFSLayerUploadBestEffort(parent context.Context, layerID, uploadID string) {
	timeout := c.multipartAbortTimeout
	if timeout <= 0 {
		timeout = defaultMultipartAbortTimeout
	}
	ctx, cancel := newMultipartAbortContext(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.fsLayerUploadURL(layerID, uploadID, "abort"), nil)
	if err != nil {
		return
	}
	resp, err := c.do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func (c *Client) fsLayerUploadURL(layerID, uploadID, action string) string {
	return c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/uploads/" + url.PathEscape(uploadID) + "/" + action
}
