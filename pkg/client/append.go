package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mem9-ai/dat9/pkg/s3client"
)

// AppendPlan mirrors the server's response for an append-initiate request.
type AppendPlan struct {
	BaseSize int64 `json:"base_size"`
	PatchPlan
}

// AppendStream appends bytes from r to path. Missing paths are created, small
// existing files fall back to read-modify-write, and large S3-backed files use
// the incremental append upload flow.
func (c *Client) AppendStream(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc) error {
	if size < 0 {
		return fmt.Errorf("append size must be non-negative")
	}
	if size == 0 {
		return nil
	}

	stat, err := c.StatCtx(ctx, path)
	if err != nil {
		if isClientNotFound(err) {
			return c.WriteStreamConditional(ctx, path, r, size, progress, 0)
		}
		return err
	}
	if stat.IsDir {
		return fmt.Errorf("is a directory: %s", path)
	}

	partSize := s3client.CalcAdaptivePartSize(stat.Size)
	plan, err := c.initiateAppend(ctx, path, size, partSize, stat.Revision)
	if err != nil {
		if shouldRewriteAppend(err) {
			return c.appendByRewrite(ctx, path, r, size, stat.Revision)
		}
		return err
	}

	remaining := size
	totalParts := len(plan.UploadParts) + len(plan.CopiedParts)
	for _, part := range plan.UploadParts {
		err := c.uploadPatchPart(ctx, part, func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			if int64(len(origData)) > partSize {
				return nil, fmt.Errorf("part %d original data length %d exceeds part size %d", partNumber, len(origData), partSize)
			}

			data := make([]byte, partSize)
			copy(data, origData)

			need := int(partSize) - len(origData)
			if need > 0 {
				if _, err := io.ReadFull(r, data[len(origData):]); err != nil {
					return nil, fmt.Errorf("read append bytes for part %d: %w", partNumber, err)
				}
				remaining -= int64(need)
			}
			return data, nil
		})
		if err != nil {
			return fmt.Errorf("part %d: %w", part.Number, err)
		}
		if progress != nil {
			progress(part.Number, totalParts, part.Size)
		}
	}
	if remaining != 0 {
		return fmt.Errorf("append source size mismatch: %d bytes remaining", remaining)
	}

	return c.completeUpload(ctx, plan.UploadID)
}

func (c *Client) initiateAppend(ctx context.Context, path string, appendSize int64, partSize int64, expectedRevision int64) (*AppendPlan, error) {
	body, err := json.Marshal(struct {
		AppendSize       int64  `json:"append_size"`
		PartSize         int64  `json:"part_size,omitempty"`
		ExpectedRevision *int64 `json:"expected_revision,omitempty"`
	}{
		AppendSize:       appendSize,
		PartSize:         partSize,
		ExpectedRevision: expectedRevisionField(expectedRevision),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal append request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?append", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		return nil, readError(resp)
	}

	var plan AppendPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode append plan: %w", err)
	}
	return &plan, nil
}

func (c *Client) appendByRewrite(ctx context.Context, path string, r io.Reader, appendSize int64, expectedRevision int64) error {
	existing, err := c.ReadCtx(ctx, path)
	if err != nil {
		return err
	}

	appendData, err := io.ReadAll(io.LimitReader(r, appendSize))
	if err != nil {
		return fmt.Errorf("read append data: %w", err)
	}
	if int64(len(appendData)) != appendSize {
		return fmt.Errorf("read append data: got %d bytes, want %d", len(appendData), appendSize)
	}

	final := make([]byte, 0, len(existing)+len(appendData))
	final = append(final, existing...)
	final = append(final, appendData...)
	return c.WriteCtxConditional(ctx, path, final, expectedRevision)
}

func isClientNotFound(err error) bool {
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusNotFound
	}
	return strings.HasPrefix(err.Error(), "not found: ")
}

func shouldRewriteAppend(err error) bool {
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	if statusErr.StatusCode != http.StatusBadRequest {
		return false
	}
	return strings.HasPrefix(statusErr.Message, "file is not S3-stored:")
}
