package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
)

const extentReadPlanHeader = "X-Dat9-Extent-Read-Plan"

type extentReadPlan struct {
	Size       int64             `json:"size"`
	Blocks     []extentPlanBlock `json:"blocks"`
	NextOffset int64             `json:"next_offset,omitempty"`
}

type extentPlanBlock struct {
	FileOffset  int64  `json:"file_offset"`
	Length      int64  `json:"length"`
	BlockKey    string `json:"block_key,omitempty"`
	BlockOffset int64  `json:"block_offset"`
	URL         string `json:"url,omitempty"`
	Zero        bool   `json:"zero,omitempty"`
}

func (c *Client) isExtentPlanResponse(resp *http.Response) bool {
	// Fixed views use the server's protected byte stream, including for extent
	// files whose own content happens to be JSON. Never decode that as a plan.
	return c.view.LayerID == "" && resp.StatusCode == http.StatusPartialContent &&
		ContentLayout(resp.Header.Get("X-Dat9-Content-Layout")) == ContentLayoutExtent
}

func decodeExtentPlan(resp *http.Response, etag string) (extentReadPlan, error) {
	defer func() { _ = resp.Body.Close() }()
	if resp.Header.Get("ETag") != etag {
		return extentReadPlan{}, errors.New("extent file changed during read")
	}
	const maxPlanBytes = 4 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxPlanBytes+1))
	if err != nil {
		return extentReadPlan{}, fmt.Errorf("read extent plan: %w", err)
	}
	if len(raw) > maxPlanBytes {
		return extentReadPlan{}, errors.New("extent plan exceeds response limit")
	}
	var plan extentReadPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return extentReadPlan{}, fmt.Errorf("decode extent plan: %w", err)
	}
	return plan, nil
}

func validateExtentPlan(plan extentReadPlan, offset, end int64) error {
	if plan.Blocks == nil || plan.Size < 0 || offset < 0 || end < offset || end > plan.Size {
		return errors.New("invalid extent plan window")
	}
	cursor := offset
	for _, block := range plan.Blocks {
		if block.FileOffset != cursor || block.Length <= 0 || block.Length > end-cursor ||
			block.BlockOffset < 0 || block.BlockOffset > math.MaxInt64-block.Length {
			return errors.New("extent plan contains an invalid or discontinuous range")
		}
		if block.Zero {
			if block.URL != "" || block.BlockKey != "" || block.BlockOffset != 0 {
				return errors.New("extent zero range contains object metadata")
			}
		} else {
			u, err := url.Parse(block.URL)
			if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.Fragment != "" {
				return errors.New("extent plan contains an invalid object URL")
			}
		}
		cursor += block.Length
	}
	if cursor == end {
		if plan.NextOffset != 0 {
			return errors.New("completed extent plan has a continuation")
		}
	} else if cursor <= offset || plan.NextOffset != cursor {
		return errors.New("extent plan does not cover the window or advance its continuation")
	}
	return nil
}

func (c *Client) openExtentPlan(ctx context.Context, path string, resp *http.Response, offset, length int64) (io.ReadCloser, error) {
	etag := resp.Header.Get("ETag")
	if len(etag) < 2 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		_ = resp.Body.Close()
		return nil, errors.New("extent plan requires a strong file validator")
	}
	plan, err := decodeExtentPlan(resp, etag)
	if err != nil {
		return nil, err
	}
	end := plan.Size
	if length >= 0 && plan.Size >= offset && length < plan.Size-offset {
		end = offset + length
	}
	if err := validateExtentPlan(plan, offset, end); err != nil {
		return nil, err
	}
	readCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	stop := context.AfterFunc(readCtx, func() { _ = writer.CloseWithError(readCtx.Err()) })
	go func() {
		defer cancel()
		err := c.copyExtentPlans(readCtx, writer, path, plan, etag, end)
		stop()
		_ = writer.CloseWithError(err)
	}()
	return &extentPlanReader{PipeReader: reader, cancel: cancel}, nil
}

func (c *Client) copyExtentPlans(ctx context.Context, dst io.Writer, path string, plan extentReadPlan, etag string, end int64) error {
	for {
		for _, block := range plan.Blocks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if block.Zero {
				if _, err := io.CopyN(dst, extentZeroReader{}, block.Length); err != nil {
					return err
				}
			} else if err := c.copyExtentBlock(ctx, dst, block); err != nil {
				return err
			}
		}
		if plan.NextOffset == 0 {
			return nil
		}
		offset, size := plan.NextOffset, plan.Size
		resp, err := c.readFileResponse(ctx, path, fmt.Sprintf("bytes=%d-%d", offset, end-1), true, etag)
		if err != nil {
			return err
		}
		if !c.isExtentPlanResponse(resp) {
			_ = resp.Body.Close()
			return fmt.Errorf("extent continuation returned HTTP %d or changed layout", resp.StatusCode)
		}
		plan, err = decodeExtentPlan(resp, etag)
		if err != nil {
			return err
		}
		if plan.Size != size {
			return errors.New("extent file size changed during read")
		}
		if err := validateExtentPlan(plan, offset, end); err != nil {
			return err
		}
	}
}

func (c *Client) copyExtentBlock(ctx context.Context, dst io.Writer, block extentPlanBlock) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, block.URL, nil)
	if err != nil {
		return errors.New("invalid extent block request")
	}
	end := block.BlockOffset + block.Length - 1
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", block.BlockOffset, end))
	// Object URLs are capabilities. Never add Drive9 credentials, cookies,
	// actor/work headers, or follow an unexpected object-store redirect.
	hc := *c.httpClient
	hc.Jar = nil
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			err = requestErr.Err // the signed URL must not appear in error output
		}
		return fmt.Errorf("extent block download: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("extent block download returned HTTP %d", resp.StatusCode)
	}
	actualEnd, err := validatePartialContentRange(resp, block.BlockOffset, end)
	if err != nil || actualEnd != end || (resp.ContentLength >= 0 && resp.ContentLength != block.Length) {
		return errors.New("extent block response does not match the requested range")
	}
	_, err = io.CopyN(dst, resp.Body, block.Length)
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return err
}

type extentZeroReader struct{}

func (extentZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

type extentPlanReader struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (r *extentPlanReader) Close() error {
	r.cancel()
	return r.PipeReader.Close()
}
