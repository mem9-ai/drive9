package client

import (
	"context"
	"fmt"
	"sync"
)

// StreamWriter provides a streaming multipart upload API where individual
// parts can be submitted concurrently as they become available.
// It wraps the v2 upload protocol (initiate → presign → upload → complete),
// with fallback to v1 if the server doesn't support v2.
//
// Usage:
//  1. Call Client.NewStreamWriter() to create a StreamWriter (lazy — no network call yet).
//  2. Call WritePart() for each part as data becomes ready (concurrency-safe).
//  3. Call Complete() to upload the final part and finalize the upload.
//  4. Call Abort() on any error to clean up server-side state.
type StreamWriter struct {
	client   *Client
	path     string
	totalSize int64

	mu        sync.Mutex
	plan      *uploadPlanV2    // lazily initialized on first WritePart
	uploaded  map[int]completePart // partNumber → completed part
	inflight  sync.WaitGroup
	err       error            // first error from any goroutine
	started   bool
	completed bool

	sem chan struct{} // concurrency limiter
}

// NewStreamWriter creates a StreamWriter for streaming multipart upload.
// No network call is made until the first WritePart.
func (c *Client) NewStreamWriter(ctx context.Context, path string, totalSize int64) *StreamWriter {
	return &StreamWriter{
		client:    c,
		path:      path,
		totalSize: totalSize,
		uploaded:  make(map[int]completePart),
		sem:       make(chan struct{}, uploadMaxConcurrency),
	}
}

// Started reports whether the upload has been initiated.
func (sw *StreamWriter) Started() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.started
}

// initLocked initiates the multipart upload. Caller must hold sw.mu.
func (sw *StreamWriter) initLocked(ctx context.Context) error {
	if sw.started {
		return nil
	}
	plan, err := sw.client.initiateUploadV2(ctx, sw.path, sw.totalSize)
	if err == errV2NotAvailable {
		return fmt.Errorf("streaming upload requires v2 protocol: %w", err)
	}
	if err != nil {
		return fmt.Errorf("initiate stream upload: %w", err)
	}
	sw.plan = plan
	sw.started = true
	return nil
}

// WritePart uploads a single part in the background. partNum is 1-based.
// data is copied internally so the caller may reuse the buffer after return.
// This method is concurrency-safe.
func (sw *StreamWriter) WritePart(ctx context.Context, partNum int, data []byte) error {
	sw.mu.Lock()
	if sw.err != nil {
		err := sw.err
		sw.mu.Unlock()
		return err
	}
	if sw.completed {
		sw.mu.Unlock()
		return fmt.Errorf("stream writer already completed")
	}

	// Lazy init
	if err := sw.initLocked(ctx); err != nil {
		sw.err = err
		sw.mu.Unlock()
		return err
	}
	plan := sw.plan
	sw.mu.Unlock()

	// Copy data so caller can reuse buffer
	buf := make([]byte, len(data))
	copy(buf, data)

	// Acquire semaphore
	select {
	case sw.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	sw.inflight.Add(1)
	go func() {
		defer sw.inflight.Done()
		defer func() { <-sw.sem }()

		// Presign this part
		pp, err := sw.client.presignOnePart(ctx, plan.UploadID, partNum)
		if err != nil {
			sw.setError(fmt.Errorf("presign part %d: %w", partNum, err))
			return
		}

		// Upload
		etag, err := sw.client.uploadOnePartV2(ctx, *pp, buf)
		if err == errPresignExpired {
			// Retry with fresh presign
			pp2, err2 := sw.client.presignOnePart(ctx, plan.UploadID, partNum)
			if err2 != nil {
				sw.setError(fmt.Errorf("re-presign part %d: %w", partNum, err2))
				return
			}
			etag, err = sw.client.uploadOnePartV2(ctx, *pp2, buf)
		}
		if err != nil {
			sw.setError(fmt.Errorf("upload part %d: %w", partNum, err))
			return
		}

		sw.mu.Lock()
		sw.uploaded[partNum] = completePart{Number: partNum, ETag: etag}
		sw.mu.Unlock()
	}()

	return nil
}

// Complete waits for all inflight parts, uploads the final part if provided,
// and calls CompleteMultipartUpload.
// finalPartNum/finalPartData can be used for the last (possibly partial) part.
// If finalPartData is nil, no additional part is uploaded.
func (sw *StreamWriter) Complete(ctx context.Context, finalPartNum int, finalPartData []byte) error {
	// Upload final part synchronously if provided
	if finalPartData != nil && len(finalPartData) > 0 {
		sw.mu.Lock()
		if sw.err != nil {
			err := sw.err
			sw.mu.Unlock()
			return err
		}
		if err := sw.initLocked(ctx); err != nil {
			sw.err = err
			sw.mu.Unlock()
			return err
		}
		plan := sw.plan
		sw.mu.Unlock()

		pp, err := sw.client.presignOnePart(ctx, plan.UploadID, finalPartNum)
		if err != nil {
			return fmt.Errorf("presign final part %d: %w", finalPartNum, err)
		}
		etag, err := sw.client.uploadOnePartV2(ctx, *pp, finalPartData)
		if err != nil {
			return fmt.Errorf("upload final part %d: %w", finalPartNum, err)
		}
		sw.mu.Lock()
		sw.uploaded[finalPartNum] = completePart{Number: finalPartNum, ETag: etag}
		sw.mu.Unlock()
	}

	// Wait for all background parts
	sw.inflight.Wait()

	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.err != nil {
		return sw.err
	}

	if !sw.started || sw.plan == nil {
		return fmt.Errorf("stream writer was never started")
	}

	// Build ordered parts list from actually-uploaded parts.
	// We use sw.uploaded (not sw.plan.TotalParts) because the file may have
	// grown after initiation, making TotalParts stale.
	if len(sw.uploaded) == 0 {
		return fmt.Errorf("no parts uploaded in stream upload")
	}
	// Find the max part number to determine the range.
	maxPart := 0
	for pn := range sw.uploaded {
		if pn > maxPart {
			maxPart = pn
		}
	}
	parts := make([]completePart, 0, maxPart)
	for i := 1; i <= maxPart; i++ {
		cp, ok := sw.uploaded[i]
		if !ok {
			return fmt.Errorf("missing part %d in stream upload (have %d parts, max %d)", i, len(sw.uploaded), maxPart)
		}
		parts = append(parts, cp)
	}

	sw.completed = true
	return sw.client.completeUploadV2(ctx, sw.plan.UploadID, parts)
}

// Abort cancels the multipart upload and cleans up server-side state.
func (sw *StreamWriter) Abort(ctx context.Context) error {
	sw.inflight.Wait()

	sw.mu.Lock()
	defer sw.mu.Unlock()

	if !sw.started || sw.plan == nil {
		return nil
	}
	return sw.client.abortUploadV2(ctx, sw.plan.UploadID)
}

// setError records the first error encountered.
func (sw *StreamWriter) setError(err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.err == nil {
		sw.err = err
	}
}
