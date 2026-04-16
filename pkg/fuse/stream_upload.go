package fuse

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// StreamUploader manages multipart uploads for large writable handles.
//
// Two modes of operation:
//
//  1. Flush-time upload (UploadAll): Used for the current create/truncate paths
//     because the final size is unknown until close. All parts are uploaded in
//     parallel once the exact total size is available.
//
//  2. Streaming upload (SubmitPart + FinishStreaming): Reserved for callers
//     that know the exact final size before submitting parts. The current FUSE
//     create/truncate paths do not satisfy that constraint, so SubmitPart
//     intentionally defers work instead of initiating an invalid upload plan.
type StreamUploader struct {
	client           *client.Client
	path             string
	expectedRevision int64

	mu            sync.Mutex
	writer        *client.StreamWriter
	started       bool
	streamedParts map[int]bool // 1-based part numbers uploaded during streaming
	inflightWg    sync.WaitGroup
	streamErr     error // first error from streaming upload
}

// NewStreamUploader creates a StreamUploader for the given path.
// No network calls are made until UploadAll or SubmitPart is called.
func NewStreamUploader(c *client.Client, path string, expectedRevision int64) *StreamUploader {
	return &StreamUploader{
		client:           c,
		path:             path,
		expectedRevision: expectedRevision,
		streamedParts:    make(map[int]bool),
	}
}

// Started reports whether the upload has been initiated.
func (su *StreamUploader) Started() bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.started
}

// HasStreamedParts reports whether any parts were uploaded during streaming
// (i.e., during Write() calls, not at flush time).
func (su *StreamUploader) HasStreamedParts() bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return len(su.streamedParts) > 0
}

// SubmitPart uploads a single part in the background when the caller can start
// a valid streaming upload. For FUSE create/truncate handles the final file
// size is unknown during Write(), while the current multipart APIs require an
// exact total size at initiate time. In that case we deliberately defer the
// upload and let UploadAll handle the file at flush/close with the precise
// final size.
//
// This keeps the path correct and avoids issuing an initiate request with a
// bogus total size that the server will reject.
// partNum is 1-based. data is copied by the underlying StreamWriter.
// onDone is called after a successful upload with the 1-based part number.
func (su *StreamUploader) SubmitPart(ctx context.Context, partNum int, data []byte, onDone func(int)) error {
	return nil
}

// FinishStreaming completes a streaming upload:
//  1. Wait for all inflight streaming parts
//  2. Re-upload any dirty parts (parts that were back-written after eviction)
//  3. Upload the final (partial) part via Complete
//
// lastPartNum is 1-based, lastPartData is the data for the final part.
// dirtyParts is a map of 1-based partNum → data for back-written parts.
func (su *StreamUploader) FinishStreaming(ctx context.Context, totalSize int64,
	lastPartNum int, lastPartData []byte, dirtyParts map[int][]byte) error {

	// Wait for all inflight streaming uploads
	su.inflightWg.Wait()

	su.mu.Lock()
	if su.streamErr != nil {
		err := su.streamErr
		sw := su.writer
		su.mu.Unlock()
		if sw != nil {
			abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = sw.Abort(abortCtx)
			cancel()
		}
		return err
	}
	sw := su.writer
	if sw == nil {
		su.mu.Unlock()
		return nil
	}
	su.mu.Unlock()

	// Re-upload dirty (back-written) parts
	for pn, data := range dirtyParts {
		if err := sw.WritePart(ctx, pn, data); err != nil {
			abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = sw.Abort(abortCtx)
			cancel()
			return err
		}
	}

	// Complete with the last part
	err := sw.Complete(ctx, lastPartNum, lastPartData)
	if err != nil {
		abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sw.Abort(abortCtx)
		cancel()
		return err
	}
	return nil
}

// UploadAll initiates a v2 multipart upload and uploads all provided parts
// in parallel. This should be called at flush time with the final file size
// and all part data. partData is a map of 1-based partNum → data.
// The last part may be smaller than partSize.
func (su *StreamUploader) UploadAll(ctx context.Context, totalSize int64, partData map[int][]byte) error {
	if len(partData) == 0 {
		return nil
	}

	su.mu.Lock()
	su.writer = su.client.NewStreamWriterConditional(ctx, su.path, totalSize, su.expectedRevision)
	su.started = true
	sw := su.writer
	su.mu.Unlock()

	// Find the max part number to determine which is the last part
	maxPart := 0
	for pn := range partData {
		if pn > maxPart {
			maxPart = pn
		}
	}

	// Upload all parts except the last one in parallel via WritePart
	for pn, data := range partData {
		if pn == maxPart {
			continue // last part is handled by Complete
		}
		if err := sw.WritePart(ctx, pn, data); err != nil {
			abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = sw.Abort(abortCtx)
			cancel()
			return err
		}
	}

	// Complete with the last part
	lastPartData := partData[maxPart]
	err := sw.Complete(ctx, maxPart, lastPartData)
	if err != nil {
		abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sw.Abort(abortCtx)
		cancel()
		return err
	}
	return nil
}

// Abort cancels the upload and cleans up server-side state.
func (su *StreamUploader) Abort() {
	// Wait for inflight streaming parts before aborting
	su.inflightWg.Wait()

	su.mu.Lock()
	sw := su.writer
	su.mu.Unlock()

	if sw != nil {
		if err := sw.Abort(context.Background()); err != nil {
			log.Printf("stream upload abort failed for %s: %v", su.path, err)
		}
	}
}
