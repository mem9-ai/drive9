package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

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
	Number         int               `json:"number"`
	URL            string            `json:"url"`
	Size           int64             `json:"size"`
	ChecksumSHA256 string            `json:"checksum_sha256,omitempty"`
	ChecksumCRC32C string            `json:"checksum_crc32c,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	ExpiresAt      string            `json:"expires_at"`
}

// --- v2 upload types (on-demand presign, no upfront checksum) ---

// uploadPlanV2 mirrors the server's UploadPlanV2 response.
type uploadPlanV2 struct {
	UploadID         string           `json:"upload_id"`
	Key              string           `json:"key"`
	PartSize         int64            `json:"part_size"`
	TotalParts       int              `json:"total_parts"`
	ExpiresAt        string           `json:"expires_at"`
	Resumable        bool             `json:"resumable"`
	ChecksumContract checksumContract `json:"checksum_contract"`
}

type checksumContract struct {
	Supported []string `json:"supported"`
	Required  bool     `json:"required"`
}

// presignedPart is a presigned URL received from the v2 presign-batch endpoint.
type presignedPart struct {
	Number         int               `json:"number"`
	URL            string            `json:"url"`
	Size           int64             `json:"size"`
	ChecksumSHA256 string            `json:"checksum_sha256,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	ExpiresAt      time.Time         `json:"expires_at"`
}

// completePart is sent to the v2 complete endpoint.
type completePart struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
}

// ProgressFunc is called after each part upload completes.
// partNumber is 1-based, totalParts is the total count.
type ProgressFunc func(partNumber, totalParts int, bytesUploaded int64)

// DefaultSmallFileThreshold matches the server's threshold for direct PUT vs multipart.
const DefaultSmallFileThreshold = 50_000 // 50,000 bytes — matches embedding model max input characters

// Upload concurrency limits, modeled after db9's memory-bounded approach:
//
//	parallelism = min(maxBufferBytes / partSize, maxConcurrency)
//
// This keeps total in-flight memory bounded regardless of part size.
const (
	uploadMaxConcurrency = 16
	uploadMaxBufferBytes = 256 * 1024 * 1024 // 256 MB
)

const (
	downloadParallelThreshold = 8 << 20
	downloadChunkSize         = 8 << 20
	downloadMaxConcurrency    = 8
)

// DownloadSummary exposes the coarse-grained large-file download metrics that
// the benchmark harness consumes from CLI stderr.
type DownloadSummary struct {
	Type           string    `json:"type"`
	Mode           string    `json:"mode"`
	Concurrency    int       `json:"concurrency"`
	ChunkSizeBytes int64     `json:"chunk_size_bytes"`
	RangeCount     int       `json:"range_count"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	RemotePath     string    `json:"remote_path"`
	LocalPath      string    `json:"local_path"`
}

type downloadRange struct {
	offset int64
	length int64
}

type readTarget struct {
	objectURL string
}

func uploadParallelism(partSize int64) int {
	if partSize <= 0 {
		partSize = s3client.PartSize
	}
	byMemory := int(uploadMaxBufferBytes / partSize)
	if byMemory < 1 {
		byMemory = 1
	}
	return min(byMemory, uploadMaxConcurrency)
}

func checksumParallelism(partSize int64, partCount int) int {
	if partSize <= 0 {
		partSize = s3client.PartSize
	}
	byMemory := int(uploadMaxBufferBytes / partSize)
	if byMemory < 1 {
		byMemory = 1
	}
	return min(runtime.GOMAXPROCS(0), partCount, byMemory)
}

// WriteStream uploads data from a reader. For small files (size < threshold),
// it does a direct PUT with body. For large files, it tries the v2 protocol
// (on-demand presign, no upfront checksum) first, falling back to v1 on 404.
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
	ra, ok := r.(io.ReaderAt)
	if !ok {
		return fmt.Errorf("large uploads require an io.ReaderAt (seekable source)")
	}

	// Try v2 protocol first (on-demand presign, no checksum pre-computation).
	err := c.writeStreamV2(ctx, path, ra, size, progress)
	if err == errV2NotAvailable {
		// Server doesn't support v2 — fall back to v1.
		return c.writeStreamV1(ctx, path, ra, size, progress)
	}
	return err
}

// errV2NotAvailable is a sentinel indicating the server returned 404 for
// the v2 initiate endpoint, so the caller should fall back to v1.
var errV2NotAvailable = fmt.Errorf("v2 upload API not available")

// writeStreamV1 is the original v1 upload path: pre-compute checksums → initiate → upload all.
func (c *Client) writeStreamV1(ctx context.Context, path string, ra io.ReaderAt, size int64, progress ProgressFunc) error {
	checksums, err := computePartChecksumsFromReaderAt(ra, size, s3client.PartSize)
	if err != nil {
		return fmt.Errorf("compute part checksums: %w", err)
	}
	plan, err := c.initiateUpload(ctx, path, size, checksums)
	if err != nil {
		return err
	}
	return c.uploadParts(ctx, plan, ra, progress)
}

// writeStreamV2 implements the v2 upload protocol:
// 1. Initiate (no checksums, server returns part_size + total_parts)
// 2. Pipelined presign: background goroutine fetches presigned URLs in batches
// 3. Upload goroutines consume presigned URLs from channel
// 4. Complete with part ETags, or abort on failure
func (c *Client) writeStreamV2(ctx context.Context, path string, ra io.ReaderAt, size int64, progress ProgressFunc) error {
	plan, err := c.initiateUploadV2(ctx, path, size)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Pipelined presign: feed presigned URLs into a buffered channel.
	parallelism := uploadParallelism(plan.PartSize)
	presignCh := make(chan presignedPart, parallelism)
	presignErrCh := make(chan error, 1)

	go c.presignPipeline(ctx, plan, parallelism, presignCh, presignErrCh)

	// Upload parts, collecting ETags for the complete call.
	parts, err := c.uploadPartsV2(ctx, plan, ra, presignCh, presignErrCh, progress)
	if err != nil {
		_ = c.abortUploadV2(context.Background(), plan.UploadID)
		return err
	}

	if err := c.completeUploadV2(ctx, plan.UploadID, parts); err != nil {
		// Complete failed (network error, 5xx, 409, 410) — best-effort abort
		// to avoid leaving orphaned multipart uploads / upload rows.
		_ = c.abortUploadV2(context.Background(), plan.UploadID)
		return err
	}
	return nil
}

type uploadInitiateRequest struct {
	Path          string   `json:"path"`
	TotalSize     int64    `json:"total_size"`
	PartChecksums []string `json:"part_checksums"`
}

type uploadResumeRequest struct {
	PartChecksums []string `json:"part_checksums"`
}

func (c *Client) initiateUpload(ctx context.Context, path string, size int64, checksums []string) (UploadPlan, error) {
	plan, resp, err := c.initiateUploadByBody(ctx, path, size, checksums)
	if err == nil {
		return plan, nil
	}
	if resp != nil {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
			return c.initiateUploadLegacy(ctx, path, size, checksums)
		}
		if resp.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(err.Error()), "unknown upload action") {
			return c.initiateUploadLegacy(ctx, path, size, checksums)
		}
		return UploadPlan{}, err
	}
	return UploadPlan{}, err
}

func (c *Client) initiateUploadByBody(ctx context.Context, path string, size int64, checksums []string) (UploadPlan, *http.Response, error) {
	body, err := json.Marshal(uploadInitiateRequest{Path: path, TotalSize: size, PartChecksums: checksums})
	if err != nil {
		return UploadPlan{}, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/uploads/initiate", bytes.NewReader(body))
	if err != nil {
		return UploadPlan{}, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return UploadPlan{}, nil, err
	}
	if resp.StatusCode != http.StatusAccepted {
		return UploadPlan{}, resp, readError(resp)
	}
	var plan UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		_ = resp.Body.Close()
		return UploadPlan{}, nil, fmt.Errorf("decode upload plan: %w", err)
	}
	_ = resp.Body.Close()
	return plan, nil, nil
}

func (c *Client) initiateUploadLegacy(ctx context.Context, path string, size int64, checksums []string) (UploadPlan, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(path), http.NoBody)
	if err != nil {
		return UploadPlan{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Dat9-Content-Length", fmt.Sprintf("%d", size))
	if len(checksums) > 0 {
		req.Header.Set("X-Dat9-Part-Checksums", strings.Join(checksums, ","))
	}

	resp, err := c.do(req)
	if err != nil {
		return UploadPlan{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		return UploadPlan{}, readError(resp)
	}

	var plan UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return UploadPlan{}, fmt.Errorf("decode upload plan: %w", err)
	}
	return plan, nil
}

// uploadParts concurrently uploads parts to presigned URLs.
// Each goroutine reads its part directly via ReaderAt (parallel disk reads),
// with at most maxConcurrency in-flight to bound memory usage.
func (c *Client) uploadParts(ctx context.Context, plan UploadPlan, ra io.ReaderAt, progress ProgressFunc) error {
	stdPartSize := plan.PartSize
	if stdPartSize <= 0 && len(plan.Parts) > 0 {
		stdPartSize = plan.Parts[0].Size
	}
	if stdPartSize <= 0 {
		stdPartSize = s3client.PartSize
	}
	maxConcurrency := uploadParallelism(stdPartSize)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, part := range plan.Parts {
		// Check for prior upload errors before spawning more work
		select {
		case err := <-errCh:
			cancel()
			wg.Wait()
			return err
		default:
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}

		wg.Add(1)
		go func(p PartURL) {
			defer wg.Done()
			defer func() { <-sem }()

			data := make([]byte, p.Size)
			offset := int64(p.Number-1) * stdPartSize
			n, err := ra.ReadAt(data, offset)
			if err != nil && err != io.EOF {
				select {
				case errCh <- fmt.Errorf("read part %d: %w", p.Number, err):
				default:
				}
				cancel()
				return
			}
			if int64(n) != p.Size {
				select {
				case errCh <- fmt.Errorf("short read for part %d: got %d want %d", p.Number, n, p.Size):
				default:
				}
				cancel()
				return
			}

			_, err = c.uploadOnePart(ctx, p, data)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				cancel()
				return
			}

			if progress != nil {
				progress(p.Number, len(plan.Parts), int64(len(data)))
			}
		}(part)
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
func (c *Client) uploadOnePart(ctx context.Context, part PartURL, data []byte) (string, error) {
	checksum := part.ChecksumCRC32C
	if checksum == "" {
		// No pre-computed checksum — compute CRC32C now.
		checksum = computeCRC32C(data)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, part.URL, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	for k, v := range part.Headers {
		if strings.EqualFold(k, "host") {
			continue
		}
		req.Header.Set(k, v)
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("x-amz-checksum-crc32c", checksum)

	resp, err := c.httpClient.Do(req) // Direct to S3, no auth header
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// --- v2 upload methods ---

// initiateUploadV2 calls POST /v2/uploads/initiate.
// Returns errV2NotAvailable if the server responds with 404.
func (c *Client) initiateUploadV2(ctx context.Context, path string, size int64) (*uploadPlanV2, error) {
	body, err := json.Marshal(struct {
		Path      string `json:"path"`
		TotalSize int64  `json:"total_size"`
	}{Path: path, TotalSize: size})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/uploads/initiate", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errV2NotAvailable
	}
	if resp.StatusCode != http.StatusAccepted {
		return nil, readError(resp)
	}
	var plan uploadPlanV2
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode v2 upload plan: %w", err)
	}
	return &plan, nil
}

// presignPipeline runs in a background goroutine. It fetches presigned URLs
// in batches via POST /v2/uploads/{id}/presign-batch and sends them to presignCh.
// The channel is closed when all parts have been presigned or an error occurs.
func (c *Client) presignPipeline(ctx context.Context, plan *uploadPlanV2, batchSize int, presignCh chan<- presignedPart, errCh chan<- error) {
	defer close(presignCh)

	for start := 1; start <= plan.TotalParts; start += batchSize {
		end := start + batchSize - 1
		if end > plan.TotalParts {
			end = plan.TotalParts
		}

		// Build batch request (no checksums in phase 1)
		entries := make([]struct {
			PartNumber int `json:"part_number"`
		}, 0, end-start+1)
		for i := start; i <= end; i++ {
			entries = append(entries, struct {
				PartNumber int `json:"part_number"`
			}{PartNumber: i})
		}

		body, err := json.Marshal(struct {
			Parts any `json:"parts"`
		}{Parts: entries})
		if err != nil {
			errCh <- fmt.Errorf("marshal presign batch: %w", err)
			return
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			c.baseURL+"/v2/uploads/"+plan.UploadID+"/presign-batch", bytes.NewReader(body))
		if err != nil {
			errCh <- fmt.Errorf("create presign batch request: %w", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.do(req)
		if err != nil {
			errCh <- fmt.Errorf("presign batch: %w", err)
			return
		}

		if resp.StatusCode >= 300 {
			err = readError(resp)
			_ = resp.Body.Close()
			errCh <- fmt.Errorf("presign batch HTTP %d: %w", resp.StatusCode, err)
			return
		}

		var result struct {
			Parts []presignedPart `json:"parts"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			_ = resp.Body.Close()
			errCh <- fmt.Errorf("decode presign batch: %w", err)
			return
		}
		_ = resp.Body.Close()

		for _, p := range result.Parts {
			select {
			case presignCh <- p:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}
}

// uploadPartsV2 reads presigned URLs from presignCh and uploads parts concurrently.
// Returns the completed parts (number + etag) in order.
func (c *Client) uploadPartsV2(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt,
	presignCh <-chan presignedPart, presignErrCh <-chan error, progress ProgressFunc) ([]completePart, error) {

	parallelism := uploadParallelism(plan.PartSize)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]completePart, plan.TotalParts)
	errCh := make(chan error, 1)
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for pp := range presignCh {
		// Check for presign pipeline errors
		select {
		case err := <-presignErrCh:
			cancel()
			wg.Wait()
			return nil, fmt.Errorf("presign pipeline: %w", err)
		default:
		}

		// Check for prior upload errors
		select {
		case err := <-errCh:
			cancel()
			wg.Wait()
			return nil, err
		default:
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return nil, ctx.Err()
		}

		wg.Add(1)
		go func(p presignedPart) {
			defer wg.Done()
			defer func() { <-sem }()

			data := make([]byte, p.Size)
			offset := int64(p.Number-1) * plan.PartSize
			n, err := ra.ReadAt(data, offset)
			if err != nil && err != io.EOF {
				select {
				case errCh <- fmt.Errorf("read part %d: %w", p.Number, err):
				default:
				}
				cancel()
				return
			}
			if int64(n) != p.Size {
				select {
				case errCh <- fmt.Errorf("short read for part %d: got %d want %d", p.Number, n, p.Size):
				default:
				}
				cancel()
				return
			}

			etag, err := c.uploadOnePartV2(ctx, p, data)
			if err == errPresignExpired {
				// Presigned URL expired — fetch a fresh one and retry once.
				fresh, presignErr := c.presignOnePart(ctx, plan.UploadID, p.Number)
				if presignErr != nil {
					select {
					case errCh <- fmt.Errorf("re-presign part %d: %w", p.Number, presignErr):
					default:
					}
					cancel()
					return
				}
				etag, err = c.uploadOnePartV2(ctx, *fresh, data)
			}
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				cancel()
				return
			}

			results[p.Number-1] = completePart{Number: p.Number, ETag: etag}

			if progress != nil {
				progress(p.Number, plan.TotalParts, int64(len(data)))
			}
		}(pp)
	}

	wg.Wait()

	// Check for any remaining errors
	select {
	case err := <-presignErrCh:
		return nil, fmt.Errorf("presign pipeline: %w", err)
	default:
	}
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	return results, nil
}

// errPresignExpired indicates S3 returned 403, likely due to an expired presigned URL.
var errPresignExpired = fmt.Errorf("presigned URL expired")

// uploadOnePartV2 PUTs data to a presigned URL and returns the ETag.
// Phase 1: no per-part checksum header — checksum negotiation deferred to #113/#114.
// Returns errPresignExpired on 403 so callers can re-presign and retry.
func (c *Client) uploadOnePartV2(ctx context.Context, part presignedPart, data []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, part.URL, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	for k, v := range part.Headers {
		if strings.EqualFold(k, "host") {
			continue
		}
		req.Header.Set(k, v)
	}
	req.ContentLength = int64(len(data))

	resp, err := c.httpClient.Do(req) // Direct to S3, no auth header
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		return "", errPresignExpired
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return resp.Header.Get("ETag"), nil
}

// presignOnePart fetches a fresh presigned URL for a single part via
// POST /v2/uploads/{id}/presign. Used to retry after presigned URL expiry.
func (c *Client) presignOnePart(ctx context.Context, uploadID string, partNumber int) (*presignedPart, error) {
	body, err := json.Marshal(struct {
		PartNumber int `json:"part_number"`
	}{PartNumber: partNumber})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v2/uploads/"+uploadID+"/presign", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var p presignedPart
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, fmt.Errorf("decode presign response: %w", err)
	}
	return &p, nil
}

// completeUploadV2 sends the part list to POST /v2/uploads/{id}/complete.
func (c *Client) completeUploadV2(ctx context.Context, uploadID string, parts []completePart) error {
	body, err := json.Marshal(struct {
		Parts []completePart `json:"parts"`
	}{Parts: parts})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v2/uploads/"+uploadID+"/complete", bytes.NewReader(body))
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
		return readError(resp)
	}
	return nil
}

// abortUploadV2 sends POST /v2/uploads/{id}/abort.
func (c *Client) abortUploadV2(ctx context.Context, uploadID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v2/uploads/"+uploadID+"/abort", nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// ReadStream reads a file, following 302 redirects for large files.
func (c *Client) ReadStream(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := c.readWithoutRedirect(ctx, path)
	if err != nil {
		return nil, err
	}

	switch {
	case resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusTemporaryRedirect:
		// Large file: follow presigned URL
		_ = resp.Body.Close()
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
			defer func() { _ = resp2.Body.Close() }()
			return nil, readError(resp2)
		}
		return resp2.Body, nil

	case resp.StatusCode >= 300:
		defer func() { _ = resp.Body.Close() }()
		return nil, readError(resp)

	default:
		// Small file: return body directly
		return resp.Body, nil
	}
}

func (c *Client) readWithoutRedirect(ctx context.Context, path string) (*http.Response, error) {
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

	return noRedirectClient.Do(req)
}

func (c *Client) resolveReadTarget(ctx context.Context, path string) (*readTarget, error) {
	resp, err := c.readWithoutRedirect(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusTemporaryRedirect:
		location := resp.Header.Get("Location")
		if location == "" {
			return nil, fmt.Errorf("302 without Location header")
		}
		return &readTarget{objectURL: location}, nil

	case resp.StatusCode >= 300:
		return nil, readError(resp)

	default:
		return nil, fmt.Errorf("expected redirect for large download path %q, got HTTP %d", path, resp.StatusCode)
	}
}

// ReadStreamRange reads a byte range from a remote file. For large files the
// server returns a 302 redirect to a presigned S3 URL; this method resolves
// that redirect and issues an HTTP Range request so only the requested bytes
// are transferred. For small files (no redirect) the full body is returned
// and the caller should read only what it needs.
func (c *Client) ReadStreamRange(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	resp, err := c.readWithoutRedirect(ctx, path)
	if err != nil {
		return nil, err
	}

	switch {
	case resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusTemporaryRedirect:
		_ = resp.Body.Close()
		location := resp.Header.Get("Location")
		if location == "" {
			return nil, fmt.Errorf("302 without Location header")
		}
		req2, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
		if err != nil {
			return nil, err
		}
		// Use HTTP Range header for efficient partial read.
		req2.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		resp2, err := c.httpClient.Do(req2)
		if err != nil {
			return nil, err
		}

		switch resp2.StatusCode {
		case http.StatusPartialContent:
			return resp2.Body, nil
		case http.StatusRequestedRangeNotSatisfiable:
			defer func() { _ = resp2.Body.Close() }()
			return io.NopCloser(bytes.NewReader(nil)), nil
		default:
			if resp2.StatusCode >= 300 {
				defer func() { _ = resp2.Body.Close() }()
				return nil, readError(resp2)
			}
			// 200: server returned full body (Range not honored).
			// Skip to offset and limit the read.
			return c.sliceBody(resp2.Body, offset, length)
		}

	case resp.StatusCode >= 300:
		defer func() { _ = resp.Body.Close() }()
		return nil, readError(resp)

	default:
		// Small file: full body returned. Skip to offset and limit.
		return c.sliceBody(resp.Body, offset, length)
	}
}

func (c *Client) readObjectRangeStrict(ctx context.Context, objectURL string, offset, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		return resp.Body, nil
	case http.StatusRequestedRangeNotSatisfiable:
		defer func() { _ = resp.Body.Close() }()
		return io.NopCloser(bytes.NewReader(nil)), nil
	case http.StatusOK:
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf("range request was not honored for %q at offset=%d length=%d", objectURL, offset, length)
	default:
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			return nil, readError(resp)
		}
		return nil, fmt.Errorf("unexpected status %d for range request %q", resp.StatusCode, objectURL)
	}
}

// sliceBody skips offset bytes from rc, then returns a reader limited to
// length bytes. The original rc is closed when the returned ReadCloser is closed.
func (c *Client) sliceBody(rc io.ReadCloser, offset, length int64) (io.ReadCloser, error) {
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			_ = rc.Close()
			if err == io.EOF {
				// Offset past end — return empty reader.
				return io.NopCloser(strings.NewReader("")), nil
			}
			return nil, fmt.Errorf("skip to offset: %w", err)
		}
	}
	return &limitedReadCloser{r: io.LimitReader(rc, length), c: rc}, nil
}

type limitedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedReadCloser) Close() error               { return l.c.Close() }

// DownloadToFile downloads a remote file into a local path.
func (c *Client) DownloadToFile(ctx context.Context, remotePath, localPath string, size int64) error {
	_, err := c.DownloadToFileWithSummary(ctx, remotePath, localPath, size)
	return err
}

// DownloadToFileWithSummary downloads a remote file into a local path and
// returns a coarse-grained summary when the large-file parallel path is used.
func (c *Client) DownloadToFileWithSummary(ctx context.Context, remotePath, localPath string, size int64) (*DownloadSummary, error) {
	out, err := os.Create(localPath)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", localPath, err)
	}
	defer func() { _ = out.Close() }()

	if size < downloadParallelThreshold {
		rc, err := c.ReadStream(ctx, remotePath)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rc.Close() }()
		_, err = io.Copy(out, rc)
		return nil, err
	}

	if err := out.Truncate(size); err != nil {
		return nil, fmt.Errorf("preallocate %s: %w", localPath, err)
	}

	summary, err := c.downloadLargeFileParallel(ctx, remotePath, localPath, out, size)
	if err != nil {
		return nil, err
	}
	if err := out.Sync(); err != nil {
		return nil, fmt.Errorf("sync %s: %w", localPath, err)
	}
	return summary, nil
}

func (c *Client) downloadLargeFileParallel(ctx context.Context, remotePath, localPath string, out *os.File, size int64) (*DownloadSummary, error) {
	target, err := c.resolveReadTarget(ctx, remotePath)
	if err != nil {
		return nil, err
	}

	rangeCount := int((size + downloadChunkSize - 1) / downloadChunkSize)
	concurrency := min(downloadMaxConcurrency, rangeCount)
	startedAt := time.Now()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasks := make(chan downloadRange, concurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	reportErr := func(err error) {
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}

				rc, err := c.readObjectRangeStrict(ctx, target.objectURL, task.offset, task.length)
				if err != nil {
					reportErr(err)
					return
				}

				data, readErr := io.ReadAll(rc)
				closeErr := rc.Close()
				if readErr != nil {
					reportErr(fmt.Errorf("read range offset=%d length=%d: %w", task.offset, task.length, readErr))
					return
				}
				if closeErr != nil {
					reportErr(fmt.Errorf("close range offset=%d length=%d: %w", task.offset, task.length, closeErr))
					return
				}
				if int64(len(data)) != task.length {
					reportErr(fmt.Errorf("short range read at offset=%d: got %d bytes, want %d", task.offset, len(data), task.length))
					return
				}
				if _, err := out.WriteAt(data, task.offset); err != nil {
					reportErr(fmt.Errorf("write range offset=%d length=%d: %w", task.offset, task.length, err))
					return
				}
			}
		}()
	}

enqueueLoop:
	for offset := int64(0); offset < size; offset += downloadChunkSize {
		length := min(size-offset, downloadChunkSize)
		task := downloadRange{offset: offset, length: length}
		select {
		case tasks <- task:
		case <-ctx.Done():
			break enqueueLoop
		}
	}
	close(tasks)
	wg.Wait()

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	finishedAt := time.Now()
	return &DownloadSummary{
		Type:           "download_summary",
		Mode:           "parallel_range_reuse_presigned_url",
		Concurrency:    concurrency,
		ChunkSizeBytes: downloadChunkSize,
		RangeCount:     rangeCount,
		StartedAt:      startedAt,
		FinishedAt:     finishedAt,
		ElapsedSeconds: finishedAt.Sub(startedAt).Seconds(),
		RemotePath:     remotePath,
		LocalPath:      localPath,
	}, nil
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
	checksums, err := computePartChecksumsFromReaderAt(r, totalSize, s3client.PartSize)
	if err != nil {
		return fmt.Errorf("compute part checksums: %w", err)
	}
	plan, err := c.requestResume(ctx, meta.UploadID, checksums)
	if err != nil {
		return err
	}

	if len(plan.Parts) == 0 {
		// All parts uploaded, just complete
		return c.completeUpload(ctx, plan.UploadID)
	}

	// Step 3: Upload missing parts concurrently
	if err := c.uploadMissingParts(ctx, *plan, r, meta.PartsTotal, progress); err != nil {
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
	defer func() { _ = resp.Body.Close() }()

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
func (c *Client) requestResume(ctx context.Context, uploadID string, checksums []string) (*UploadPlan, error) {
	plan, resp, err := c.requestResumeByBody(ctx, uploadID, checksums)
	if err == nil {
		return plan, nil
	}
	if resp != nil {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(err.Error()), "missing x-dat9-part-checksums header") {
			return c.requestResumeLegacy(ctx, uploadID, checksums)
		}
		return nil, err
	}
	return nil, err
}

func (c *Client) requestResumeByBody(ctx context.Context, uploadID string, checksums []string) (*UploadPlan, *http.Response, error) {
	body, err := json.Marshal(uploadResumeRequest{PartChecksums: checksums})
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/uploads/"+uploadID+"/resume", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode == http.StatusGone {
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("upload %s has expired", uploadID)
	}
	if resp.StatusCode >= 300 {
		return nil, resp, readError(resp)
	}

	var plan UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("decode resume plan: %w", err)
	}
	_ = resp.Body.Close()
	return &plan, nil, nil
}

func (c *Client) requestResumeLegacy(ctx context.Context, uploadID string, checksums []string) (*UploadPlan, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/uploads/"+uploadID+"/resume", nil)
	if err != nil {
		return nil, err
	}
	if len(checksums) > 0 {
		req.Header.Set("X-Dat9-Part-Checksums", strings.Join(checksums, ","))
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

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
func (c *Client) uploadMissingParts(ctx context.Context, plan UploadPlan, r io.ReaderAt, totalParts int, progress ProgressFunc) error {
	// Use plan's part size for offset calculation; fall back to default
	stdPartSize := plan.PartSize
	if stdPartSize <= 0 {
		stdPartSize = s3client.PartSize
	}
	maxConcurrency := uploadParallelism(stdPartSize)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, maxConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for _, part := range plan.Parts {
		select {
		case err := <-errCh:
			cancel()
			wg.Wait()
			return err
		default:
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}

		wg.Add(1)
		go func(p PartURL) {
			defer wg.Done()
			defer func() { <-sem }()

			data := make([]byte, p.Size)
			offset := int64(p.Number-1) * stdPartSize
			n, err := r.ReadAt(data, offset)
			if err != nil && err != io.EOF {
				select {
				case errCh <- fmt.Errorf("read part %d at offset %d: %w", p.Number, offset, err):
				default:
				}
				cancel()
				return
			}
			if int64(n) != p.Size {
				select {
				case errCh <- fmt.Errorf("short read for part %d at offset %d: got %d want %d", p.Number, offset, n, p.Size):
				default:
				}
				cancel()
				return
			}

			_, err = c.uploadOnePart(ctx, p, data)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("part %d: %w", p.Number, err):
				default:
				}
				cancel()
				return
			}
			if progress != nil {
				progress(p.Number, totalParts, int64(len(data)))
			}
		}(part)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	return nil
}

func computePartChecksumsFromReaderAt(r io.ReaderAt, totalSize int64, partSize int64) ([]string, error) {
	if totalSize <= 0 {
		return nil, nil
	}
	parts := s3client.CalcParts(totalSize, partSize)
	checksums := make([]string, len(parts))
	// Cap workers by CPU count, part count, and memory (workers × partSize ≤ 256MB).
	workers := checksumParallelism(partSize, len(parts))

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	partCh := make(chan int, len(parts))

	for i := range parts {
		partCh <- i
	}
	close(partCh)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, partSize)
			for i := range partCh {
				p := parts[i]
				data := buf[:p.Size]
				offset := int64(p.Number-1) * partSize
				n, err := r.ReadAt(data, offset)
				if err != nil && err != io.EOF {
					errOnce.Do(func() { firstErr = fmt.Errorf("read part %d: %w", p.Number, err) })
					return
				}
				if int64(n) != p.Size {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("short read for part %d: got %d want %d", p.Number, n, p.Size)
					})
					return
				}
				checksums[i] = computeCRC32C(data)
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return checksums, nil
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func computeCRC32C(data []byte) string {
	v := crc32.Checksum(data, crc32cTable)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return base64.StdEncoding.EncodeToString(b)
}
