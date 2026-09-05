package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

const (
	AppendLogCodeRebased     = "append_log_rebased"
	AppendLogCodeConflict    = "append_log_conflict"
	AppendLogCodeUnsupported = "append_log_unsupported"
	AppendLogCodeTooLarge    = "append_log_too_large"
)

// AppendLogResult is the committed metadata returned by POST ?append-log.
type AppendLogResult struct {
	Revision int64 `json:"revision"`
	Size     int64 `json:"size_bytes"`
}

const maxAppendLogInt64 = int64(1<<63 - 1)

// AppendLog streams an immutable tail to the server-owned append-log endpoint.
func (c *Client) AppendLog(ctx context.Context, path string, tail io.Reader, tailSize, expectedRevision, expectedSize int64) (AppendLogResult, error) {
	if tailSize < 0 || expectedRevision < 0 || expectedSize < 0 {
		return AppendLogResult{}, fmt.Errorf("append-log sizes and revision must be non-negative")
	}
	if tailSize > 0 && tail == nil {
		return AppendLogResult{}, fmt.Errorf("append-log tail is nil")
	}
	if expectedSize > maxAppendLogInt64-tailSize {
		return AppendLogResult{}, fmt.Errorf("append-log size overflows int64")
	}
	if tailSize == 0 {
		tail = http.NoBody
	} else if tail == nil {
		tail = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?append-log", tail)
	if err != nil {
		return AppendLogResult{}, err
	}
	req.ContentLength = tailSize
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(expectedRevision, 10))
	req.Header.Set("X-Dat9-Expected-Size", strconv.FormatInt(expectedSize, 10))

	resp, err := c.do(req)
	if err != nil {
		return AppendLogResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return AppendLogResult{}, readError(resp)
	}

	var result AppendLogResult
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&result); err != nil {
		return AppendLogResult{}, fmt.Errorf("decode append-log response: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return AppendLogResult{}, fmt.Errorf("decode append-log response trailing data: %w", err)
	}
	if result.Revision <= 0 {
		return AppendLogResult{}, fmt.Errorf("append-log response omitted committed revision")
	}
	if result.Size != expectedSize+tailSize {
		return AppendLogResult{}, fmt.Errorf("append-log response size %d, want %d", result.Size, expectedSize+tailSize)
	}
	return result, nil
}

// WriteServerStreamConditional sends one ordinary conditional PUT to the
// Drive9 server. It intentionally does not select any multipart upload plan.
func (c *Client) WriteServerStreamConditional(ctx context.Context, path string, body io.Reader, size, expectedRevision int64) (int64, error) {
	if size < 0 || expectedRevision < 0 {
		return 0, fmt.Errorf("server stream size and expected revision must be non-negative")
	}
	if size > 0 && body == nil {
		return 0, fmt.Errorf("server stream body is nil")
	}
	if size == 0 {
		body = http.NoBody
	} else if body == nil {
		body = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(path), body)
	if err != nil {
		return 0, err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(expectedRevision, 10))

	resp, err := c.do(req)
	if err != nil {
		return 0, markCommitAttempt(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return 0, markCommitAttempt(readError(resp))
	}
	var result struct {
		Revision int64 `json:"revision"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, markCommitAttempt(fmt.Errorf("decode server stream response: %w", err))
	}
	if result.Revision <= 0 {
		return 0, markCommitAttempt(fmt.Errorf("server stream response omitted committed revision"))
	}
	return result.Revision, nil
}
