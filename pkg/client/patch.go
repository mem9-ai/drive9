package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// PatchPlan mirrors the server's response for a PATCH request.
type PatchPlan struct {
	UploadID    string            `json:"upload_id"`
	PartSize    int64             `json:"part_size"`
	UploadParts []*PatchPartURL   `json:"upload_parts"`
	CopiedParts []int             `json:"copied_parts"`
}

// PatchPartURL describes one dirty part the client must upload.
type PatchPartURL struct {
	Number    int               `json:"number"`
	URL       string            `json:"url"`
	Size      int64             `json:"size"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt string            `json:"expires_at"`
	ReadURL   string            `json:"read_url,omitempty"`
}

// PatchFile performs a partial update of a large file using S3 UploadPartCopy
// for unchanged parts. Only the dirty parts are transferred over the network.
//
// Parameters:
//   - path: remote file path
//   - newSize: total size of the file after patching
//   - dirtyParts: 1-based part numbers that have been modified
//   - readPart: callback that returns the complete data for a given part number.
//     The callback receives the part number (1-based), the part's expected size,
//     and the original data for that part (downloaded from ReadURL; nil if the
//     part is entirely new beyond the original file). It must return the final
//     part data to upload.
func (c *Client) PatchFile(ctx context.Context, path string, newSize int64, dirtyParts []int, readPart func(partNumber int, partSize int64, origData []byte) ([]byte, error), progress ProgressFunc) error {
	// Step 1: Request patch plan from server
	reqBody, err := json.Marshal(map[string]any{
		"new_size":    newSize,
		"dirty_parts": dirtyParts,
	})
	if err != nil {
		return fmt.Errorf("marshal patch request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.url(path), bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return readError(resp)
	}

	var plan PatchPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return fmt.Errorf("decode patch plan: %w", err)
	}

	// Step 2: Upload dirty parts concurrently
	const maxConcurrency = 4
	sem := make(chan struct{}, maxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	totalParts := len(plan.UploadParts) + len(plan.CopiedParts)

	for _, part := range plan.UploadParts {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}

		select {
		case err := <-errCh:
			wg.Wait()
			return err
		default:
		}

		wg.Add(1)
		go func(p *PatchPartURL) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := c.uploadPatchPart(ctx, p, readPart); err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				return
			}

			if progress != nil {
				progress(p.Number, totalParts, p.Size)
			}
		}(part)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	// Step 3: Complete upload (same endpoint as normal multipart)
	return c.completeUpload(ctx, plan.UploadID)
}

// uploadPatchPart downloads original data (if available), calls the readPart
// callback to get the final data, and uploads it to the presigned URL.
func (c *Client) uploadPatchPart(ctx context.Context, part *PatchPartURL, readPart func(int, int64, []byte) ([]byte, error)) error {
	// Download original part data if a read URL is provided
	var origData []byte
	if part.ReadURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, part.ReadURL, nil)
		if err != nil {
			return fmt.Errorf("create read request: %w", err)
		}
		resp, err := c.httpClient.Do(req) // Direct to S3, no auth
		if err != nil {
			return fmt.Errorf("download original part: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("download original part: HTTP %d", resp.StatusCode)
		}
		origData, err = io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read original part body: %w", err)
		}
	}

	// Get the final part data from the callback
	data, err := readPart(part.Number, part.Size, origData)
	if err != nil {
		return fmt.Errorf("readPart callback: %w", err)
	}

	// Upload to presigned URL
	h := sha256.Sum256(data)
	checksum := base64.StdEncoding.EncodeToString(h[:])

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, part.URL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	for k, v := range part.Headers {
		if strings.EqualFold(k, "host") {
			continue
		}
		req.Header.Set(k, v)
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("x-amz-checksum-sha256", checksum)

	resp, err := c.httpClient.Do(req) // Direct to S3, no auth
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload part: HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
