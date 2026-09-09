package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

func (c *Client) GetSlices(ctx context.Context, path string) ([]SliceRow, int64, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path)+"?slices=1", nil)
	if err != nil {
		return nil, 0, 0, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, 0, 0, readError(resp)
	}
	var out struct {
		Revision   int64      `json:"revision"`
		Generation int64      `json:"generation"`
		Slices     []SliceRow `json:"slices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, 0, err
	}
	return out.Slices, out.Revision, out.Generation, nil
}

func (c *Client) PlanExtentRead(ctx context.Context, path string, off, length int64) (*ExtentReadPlan, error) {
	q := "?read-plan=1"
	if length > 0 || off > 0 {
		q += fmt.Sprintf("&off=%d&len=%d", off, length)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path)+q, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var plan ExtentReadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func (c *Client) CompactSlices(ctx context.Context, path string, chunk int64, snapshot []SliceRow, ops []SliceOp) error {
	body, err := json.Marshal(map[string]any{
		"chunk":    chunk,
		"snapshot": snapshot,
		"ops":      ops,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?compact-slices=1", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readExtentCommitError(resp)
	}
	return nil
}

const extentFetchConcurrency = 16

// ReadExtentRange fetches [off, off+length) from object storage using a
// read-plan. The control plane only issues presigned URLs.
func (c *Client) ReadExtentRange(ctx context.Context, path string, off, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}
	plan, err := c.PlanExtentRead(ctx, path, off, length)
	if err != nil {
		return nil, err
	}
	return c.FetchExtentPlan(ctx, plan, off, length)
}

// FetchExtentPlan GETs each presigned part (capped parallelism) and copies
// the requested window. Parts may extend beyond [off, off+length).
func (c *Client) FetchExtentPlan(ctx context.Context, plan *ExtentReadPlan, off, length int64) ([]byte, error) {
	if plan == nil || length <= 0 {
		return nil, nil
	}
	if off < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}
	end := off + length
	if plan.SizeBytes > 0 && end > plan.SizeBytes {
		end = plan.SizeBytes
		length = end - off
		if length <= 0 {
			return nil, nil
		}
	}
	out := make([]byte, length)
	type fetched struct {
		part ExtentReadPart
		data []byte
		err  error
	}
	parts := plan.Parts
	if len(parts) == 0 {
		// Holes and sparse gaps have no parts; the zeroed buffer is the
		// file contents for this window (not a short read).
		return out, nil
	}
	ch := make(chan fetched, len(parts))
	sem := make(chan struct{}, extentFetchConcurrency)
	var wg sync.WaitGroup
	for _, part := range parts {
		part := part
		if part.GetURL == "" || part.BlockKey == "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				ch <- fetched{part: part, err: ctx.Err()}
				return
			}
			data, err := c.GetPresignedRetry(ctx, part.GetURL, part.Headers)
			if err == nil {
				data, err = ExtentPartPayload(part, data)
			}
			ch <- fetched{part: part, data: data, err: err}
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for item := range ch {
		if item.err != nil {
			return nil, item.err
		}
		p0, p1 := item.part.FileOff, item.part.FileOff+item.part.Len
		if p1 <= off || p0 >= end {
			continue
		}
		from := off
		if p0 > from {
			from = p0
		}
		to := end
		if p1 < to {
			to = p1
		}
		srcOff := from - p0
		dstOff := from - off
		n := to - from
		if srcOff < 0 || dstOff < 0 || srcOff+n > int64(len(item.data)) || dstOff+n > int64(len(out)) {
			return nil, fmt.Errorf("extent part %s window overflow", item.part.BlockKey)
		}
		copy(out[dstOff:dstOff+n], item.data[srcOff:srcOff+n])
	}
	return out, nil
}

// ExtentPartPayload is JuiceFS chunk.ReadAt: the GET body is either the
// ranged window (len==part.Len) or the whole slice object. A 200 that ignored
// Range must be sliced at BlockOff or the first bytes of a compacted block
// are served as every page (btreeInitPage).
func ExtentPartPayload(part ExtentReadPart, data []byte) ([]byte, error) {
	if part.Len <= 0 {
		return nil, nil
	}
	if int64(len(data)) == part.Len {
		return data, nil
	}
	start := part.BlockOff
	if start < 0 {
		start = 0
	}
	end := start + part.Len
	if end > int64(len(data)) {
		return nil, fmt.Errorf("extent part %s body %d, want %d at block_off %d", part.BlockKey, len(data), part.Len, part.BlockOff)
	}
	return data[start:end], nil
}

func (c *Client) GetPresigned(ctx context.Context, rawURL string, headers map[string]string) ([]byte, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("empty presigned GET URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		if isForbiddenPresignedHeader(k) {
			continue
		}
		req.Header.Set(k, v)
	}
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("presigned GET HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("presigned GET HTTP %d: %s", resp.StatusCode, slurp)
	}
	return io.ReadAll(resp.Body)
}

func (c *Client) GetPresignedRetry(ctx context.Context, rawURL string, headers map[string]string) ([]byte, error) {
	var last error
	backoff := 20 * time.Millisecond
	for i := 0; i < 8; i++ {
		data, err := c.GetPresigned(ctx, rawURL, headers)
		if err == nil {
			return data, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !strings.Contains(err.Error(), "HTTP 404") && !strings.Contains(err.Error(), "HTTP 403") {
			return nil, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
	return nil, last
}
