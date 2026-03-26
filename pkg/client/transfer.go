package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/mem9-ai/dat9/pkg/s3client"
)

// UploadPlan is the server's 202 response for large file uploads.
type UploadPlan struct {
	UploadID string    `json:"upload_id"`
	PartSize int64     `json:"part_size"` // standard part size (last part may be smaller)
	Parts    []PartURL `json:"parts"`
}

// PartURL is a presigned URL for uploading one part.
type PartURL struct {
	Number    int    `json:"number"`
	URL       string `json:"url"`
	Size      int64  `json:"size"`
	ExpiresAt string `json:"expires_at"`
}

// ProgressFunc is called after each part upload completes.
// partNumber is 1-based, totalParts is the total count.
type ProgressFunc func(partNumber, totalParts int, bytesUploaded int64)

// DefaultSmallFileThreshold matches the server's threshold for direct PUT vs multipart.
const DefaultSmallFileThreshold = 1 << 20 // 1MB

// WriteStream uploads data from a reader. For small files (size < threshold),
// it does a direct PUT with body. For large files, it sends a Content-Length-only
// PUT to get a 202 with presigned URLs, then uploads parts concurrently.
func (c *Client) WriteStream(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc) error {
	threshold := int64(DefaultSmallFileThreshold)
	if c.smallFileThreshold > 0 {
		threshold = c.smallFileThreshold
	}
	if size < threshold {
		// Small file: direct PUT with body
		data, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("read data: %w", err)
		}
		return c.Write(path, data)
	}

	// Large file: send total size via Content-Length, empty body — server returns 202.
	// Use http.NoBody + override ContentLength so Go sends the header without actual data.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(path), http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Dat9-Content-Length", fmt.Sprintf("%d", size))

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return readError(resp)
	}

	var plan UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return fmt.Errorf("decode upload plan: %w", err)
	}
	return c.uploadParts(ctx, plan, r, progress)
}

// uploadParts concurrently uploads parts to presigned URLs.
// Part data is read sequentially from r, but uploads run concurrently
// with at most maxConcurrency in-flight to bound memory usage.
func (c *Client) uploadParts(ctx context.Context, plan UploadPlan, r io.Reader, progress ProgressFunc) error {
	const maxConcurrency = 4

	errCh := make(chan error, 1)
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, part := range plan.Parts {
		// Acquire semaphore before reading so we hold at most maxConcurrency buffers
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}

		// Check for prior upload errors before reading more data
		select {
		case err := <-errCh:
			wg.Wait()
			return err
		default:
		}

		partData := make([]byte, part.Size)
		n, err := io.ReadFull(r, partData)
		if err != nil && err != io.ErrUnexpectedEOF {
			<-sem
			wg.Wait()
			return fmt.Errorf("read part %d: %w", part.Number, err)
		}
		partData = partData[:n]

		wg.Add(1)
		go func(p PartURL, data []byte) {
			defer wg.Done()
			defer func() { <-sem }()

			_, err := c.uploadOnePart(ctx, p.URL, data)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				return
			}

			if progress != nil {
				progress(p.Number, len(plan.Parts), int64(len(data)))
			}
		}(part, partData)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	return c.completeUpload(ctx, plan.UploadID)
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
// No body needed — server rebuilds the part list via S3 ListParts.
func (c *Client) completeUpload(ctx context.Context, uploadID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/uploads/"+uploadID+"/complete", nil)
	if err != nil {
		return err
	}

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
		return c.completeUpload(ctx, plan.UploadID)
	}

	// Step 3: Upload missing parts concurrently
	if err := c.uploadMissingParts(ctx, *plan, r, progress); err != nil {
		return err
	}

	// Step 4: Complete
	return c.completeUpload(ctx, plan.UploadID)
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

	var envelope struct {
		Uploads []UploadMeta `json:"uploads"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode upload meta: %w", err)
	}
	if len(envelope.Uploads) == 0 {
		return nil, fmt.Errorf("no active upload for %s", path)
	}
	return &envelope.Uploads[0], nil
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
func (c *Client) uploadMissingParts(ctx context.Context, plan UploadPlan, r io.ReaderAt, progress ProgressFunc) error {
	const maxConcurrency = 4
	sem := make(chan struct{}, maxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Use plan's part size for offset calculation; fall back to default
	stdPartSize := plan.PartSize
	if stdPartSize <= 0 {
		stdPartSize = s3client.PartSize
	}

	parts := plan.Parts

	for _, part := range parts {
		// Acquire semaphore before reading so we hold at most maxConcurrency buffers
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}

		// Check for prior upload errors before reading more data
		select {
		case err := <-errCh:
			wg.Wait()
			return err
		default:
		}

		data := make([]byte, part.Size)
		offset := int64(part.Number-1) * stdPartSize
		n, err := r.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			<-sem
			wg.Wait()
			return fmt.Errorf("read part %d at offset %d: %w", part.Number, offset, err)
		}
		data = data[:n]

		wg.Add(1)
		go func(p PartURL, d []byte) {
			defer wg.Done()
			defer func() { <-sem }()

			_, err := c.uploadOnePart(ctx, p.URL, d)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				return
			}
			if progress != nil {
				progress(p.Number, len(parts), int64(len(d)))
			}
		}(part, data)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	return nil
}
