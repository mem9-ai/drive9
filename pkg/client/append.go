package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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
	return c.AppendStreamWithTags(ctx, path, r, size, progress, nil)
}

// AppendStreamWithTags appends bytes from r to path and applies tags on the
// resulting file revision.
func (c *Client) AppendStreamWithTags(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc, tags map[string]string) error {
	if size < 0 {
		return fmt.Errorf("append size must be non-negative")
	}

	stat, err := c.StatCtx(ctx, path)
	if err != nil {
		if isClientNotFound(err) {
			return c.writeStreamConditional(ctx, path, r, size, progress, 0, tags)
		}
		return err
	}
	if stat.IsDir {
		return fmt.Errorf("is a directory: %s", path)
	}
	if size == 0 {
		return nil
	}

	finalSize := stat.Size + size
	if finalSize < stat.Size {
		return fmt.Errorf("append size overflows file size")
	}

	partSize := s3client.CalcAdaptivePartSize(finalSize)
	plan, err := c.initiateAppend(ctx, path, size, partSize, stat.Revision)
	if err != nil {
		if shouldRewriteAppend(err) {
			return c.appendByRewrite(ctx, path, r, size, finalSize, stat.Revision, progress)
		}
		return err
	}
	for _, part := range plan.UploadParts {
		if appendPartOverlapsExisting(plan.BaseSize, plan.PartSize, part) && part.ReadURL == "" {
			return fmt.Errorf("append part %d overlaps existing data but is missing read_url", part.Number)
		}
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

	return c.completeUploadWithTags(ctx, plan.UploadID, tags)
}

func (c *Client) writeStreamConditional(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc, expectedRevision int64, tags map[string]string) error {
	_, err := c.writeStreamConditionalWithSummary(ctx, path, r, size, progress, expectedRevision, tags)
	return err
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

func (c *Client) appendByRewrite(ctx context.Context, path string, r io.Reader, appendSize int64, finalSize int64, expectedRevision int64, progress ProgressFunc) error {
	tmp, err := os.CreateTemp("", "drive9-append-*")
	if err != nil {
		return fmt.Errorf("create append temp file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	existing, err := c.ReadStream(ctx, path)
	if err != nil {
		return err
	}
	defer func() { _ = existing.Close() }()

	if _, err := io.Copy(tmp, existing); err != nil {
		return fmt.Errorf("copy existing file into temp file: %w", err)
	}

	written, err := io.Copy(tmp, io.LimitReader(r, appendSize))
	if err != nil {
		return fmt.Errorf("copy append data into temp file: %w", err)
	}
	if written != appendSize {
		return fmt.Errorf("copy append data into temp file: got %d bytes, want %d", written, appendSize)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind append temp file: %w", err)
	}

	return c.WriteStreamConditional(ctx, path, tmp, finalSize, progress, expectedRevision)
}

func appendPartOverlapsExisting(baseSize int64, partSize int64, part *PatchPartURL) bool {
	if baseSize <= 0 || partSize <= 0 || part == nil || part.Number <= 0 {
		return false
	}
	partStart := int64(part.Number-1) * partSize
	return partStart < baseSize
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
	return strings.HasPrefix(statusErr.Message, "file is not S3-stored:") ||
		strings.Contains(statusErr.Message, "S3 not configured") ||
		strings.Contains(statusErr.Message, "unknown POST action")
}
