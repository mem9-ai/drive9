package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// UploadPlan is the server's 202 response for large file uploads.
type UploadPlan struct {
	UploadID string       `json:"upload_id"`
	Parts    []PartURL    `json:"parts"`
}

// PartURL is a presigned URL for uploading one part.
type PartURL struct {
	Number    int    `json:"number"`
	URL       string `json:"url"`
	Size      int64  `json:"size"`
	ExpiresAt string `json:"expires_at"`
}

// CompletePart is sent to the server when completing an upload.
type CompletePart struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
}

// ProgressFunc is called after each part upload completes.
// partNumber is 1-based, totalParts is the total count.
type ProgressFunc func(partNumber, totalParts int, bytesUploaded int64)

// WriteStream uploads data from a reader. For small files (size < threshold),
// it does a direct PUT. For large files, the server returns 202 with presigned
// URLs, and the engine uploads parts concurrently.
func (c *Client) WriteStream(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", size))
	// No body — server decides based on Content-Length

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		// Small file: server expects body in a second request
		return c.writeSmall(ctx, path, r)

	case http.StatusAccepted:
		// Large file: server returned presigned URLs
		var plan UploadPlan
		if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
			return fmt.Errorf("decode upload plan: %w", err)
		}
		return c.uploadParts(ctx, plan, r, progress)

	default:
		return readError(resp)
	}
}

// writeSmall does a direct PUT with the body for small files.
func (c *Client) writeSmall(ctx context.Context, path string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read data: %w", err)
	}
	return c.Write(path, data)
}

// uploadParts concurrently uploads parts to presigned URLs.
func (c *Client) uploadParts(ctx context.Context, plan UploadPlan, r io.Reader, progress ProgressFunc) error {
	const maxConcurrency = 4

	type partResult struct {
		number int
		etag   string
		err    error
	}

	results := make([]CompletePart, len(plan.Parts))
	errCh := make(chan error, 1)
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, part := range plan.Parts {
		// Read the part data from the sequential reader
		partData := make([]byte, part.Size)
		n, err := io.ReadFull(r, partData)
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("read part %d: %w", part.Number, err)
		}
		partData = partData[:n]

		wg.Add(1)
		go func(idx int, p PartURL, data []byte) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				select {
				case errCh <- ctx.Err():
				default:
				}
				return
			}

			etag, err := c.uploadOnePart(ctx, p.URL, data)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				return
			}

			results[idx] = CompletePart{Number: p.Number, ETag: etag}
			if progress != nil {
				progress(p.Number, len(plan.Parts), int64(len(data)))
			}
		}(i, part, partData)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	return c.completeUpload(ctx, plan.UploadID, results)
}

// uploadOnePart PUTs data to a presigned URL and returns the ETag.
func (c *Client) uploadOnePart(ctx context.Context, url string, data []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(data))

	resp, err := c.httpClient.Do(req) // Direct to S3, no auth header
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return resp.Header.Get("ETag"), nil
}

// completeUpload notifies the server that all parts are uploaded.
func (c *Client) completeUpload(ctx context.Context, uploadID string, parts []CompletePart) error {
	body, err := json.Marshal(struct {
		Parts []CompletePart `json:"parts"`
	}{Parts: parts})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/uploads/"+uploadID+"/complete",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// ReadStream reads a file, following 302 redirects for large files.
func (c *Client) ReadStream(ctx context.Context, path string) (io.ReadCloser, error) {
	// Disable redirect following so we can detect 302
	noRedirectClient := *c.httpClient
	noRedirectClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path), nil)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := noRedirectClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch {
	case resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusTemporaryRedirect:
		// Large file: follow presigned URL
		resp.Body.Close()
		location := resp.Header.Get("Location")
		if location == "" {
			return nil, fmt.Errorf("302 without Location header")
		}
		req2, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
		if err != nil {
			return nil, err
		}
		resp2, err := c.httpClient.Do(req2) // Direct to S3, no auth
		if err != nil {
			return nil, err
		}
		if resp2.StatusCode >= 300 {
			defer resp2.Body.Close()
			return nil, readError(resp2)
		}
		return resp2.Body, nil

	case resp.StatusCode >= 300:
		defer resp.Body.Close()
		return nil, readError(resp)

	default:
		// Small file: return body directly
		return resp.Body, nil
	}
}

// UploadMeta is the server's response for querying active uploads.
type UploadMeta struct {
	UploadID   string `json:"upload_id"`
	PartsTotal int    `json:"parts_total"`
	Status     string `json:"status"`
	ExpiresAt  string `json:"expires_at"`
}

// ResumeUpload queries for an incomplete upload and resumes it.
// Two-step flow: GET query → POST resume (get missing part URLs) → upload → complete.
func (c *Client) ResumeUpload(ctx context.Context, path string, r io.ReaderAt, totalSize int64, progress ProgressFunc) error {
	// Step 1: Query for active upload (no side effects)
	meta, err := c.queryUpload(ctx, path)
	if err != nil {
		return err
	}

	// Step 2: Request resume — server returns presigned URLs for missing parts
	plan, err := c.requestResume(ctx, meta.UploadID)
	if err != nil {
		return err
	}

	if len(plan.Parts) == 0 {
		// All parts uploaded, just complete
		return c.completeUpload(ctx, plan.UploadID, nil)
	}

	// Step 3: Upload missing parts concurrently
	results, err := c.uploadMissingParts(ctx, plan.Parts, r, progress)
	if err != nil {
		return err
	}

	// Step 4: Complete
	return c.completeUpload(ctx, plan.UploadID, results)
}

// queryUpload finds an active upload for the given path.
func (c *Client) queryUpload(ctx context.Context, path string) (*UploadMeta, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/v1/uploads?path="+path+"&status=UPLOADING", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("no active upload for %s", path)
	}
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}

	var meta UploadMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decode upload meta: %w", err)
	}
	return &meta, nil
}

// requestResume asks the server to generate presigned URLs for missing parts.
func (c *Client) requestResume(ctx context.Context, uploadID string) (*UploadPlan, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/uploads/"+uploadID+"/resume", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusGone {
		return nil, fmt.Errorf("upload %s has expired", uploadID)
	}
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}

	var plan UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode resume plan: %w", err)
	}
	return &plan, nil
}

// uploadMissingParts uploads parts from a ReaderAt (random access for resume).
func (c *Client) uploadMissingParts(ctx context.Context, parts []PartURL, r io.ReaderAt, progress ProgressFunc) ([]CompletePart, error) {
	const maxConcurrency = 4
	sem := make(chan struct{}, maxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	results := make([]CompletePart, len(parts))

	for i, part := range parts {
		data := make([]byte, part.Size)
		offset := int64(part.Number-1) * part.Size
		n, err := r.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read part %d at offset %d: %w", part.Number, offset, err)
		}
		data = data[:n]

		wg.Add(1)
		go func(idx int, p PartURL, d []byte) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				select {
				case errCh <- ctx.Err():
				default:
				}
				return
			}

			etag, err := c.uploadOnePart(ctx, p.URL, d)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				return
			}
			results[idx] = CompletePart{Number: p.Number, ETag: etag}
			if progress != nil {
				progress(p.Number, len(parts), int64(len(d)))
			}
		}(i, part, data)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	return results, nil
}
