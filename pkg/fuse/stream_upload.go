package fuse

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// StreamUploader manages parallel part uploads both during Write() for
// sequential streaming and at flush/close time for non-sequential files.
//
// Two modes of operation:
//
//  1. Flush-time upload (UploadAll): Used for non-sequential writes or when
//     streaming was not triggered. All parts are uploaded in parallel at close.
//
//  2. Streaming upload (SubmitPart + FinishStreaming): Used for large sequential
//     writes. Parts are uploaded as they fill during Write(), and memory is
//     released after each upload completes. At close, FinishStreaming uploads
//     the final partial part and any dirty (back-written) parts, then completes.
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

// RefreshExpectedRevision updates the conditional revision used for future
// upload initiation if streaming has not started yet.
func (su *StreamUploader) RefreshExpectedRevision(revision int64) bool {
	if revision < 0 {
		return false
	}

	su.mu.Lock()
	defer su.mu.Unlock()
	if su.started {
		return false
	}
	su.expectedRevision = revision
	return true
}

// ExpectedRevision reports the CAS revision that will be used when the next
// upload starts or resumes.
func (su *StreamUploader) ExpectedRevision() int64 {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.expectedRevision
}

// ResetForNextWrite prepares the uploader for another flush cycle after a
// successful commit on the same open handle.
func (su *StreamUploader) ResetForNextWrite(revision int64) {
	su.mu.Lock()
	defer su.mu.Unlock()

	su.expectedRevision = revision
	su.writer = nil
	su.started = false
	su.streamErr = nil
	su.streamedParts = make(map[int]bool)
}

// HasStreamedParts reports whether any parts were uploaded during streaming
// (i.e., during Write() calls, not at flush time).
func (su *StreamUploader) HasStreamedParts() bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return len(su.streamedParts) > 0
}

// SubmitPart uploads a single part in the background (streaming mode).
// Lazily initiates the multipart upload on first call.
// partNum is 1-based. data is copied by the underlying StreamWriter.
// onDone is called after a successful upload with the 1-based part number.
func (su *StreamUploader) SubmitPart(ctx context.Context, partNum int, data []byte, onDone func(int)) error {
	su.mu.Lock()

	// Check for prior error
	if su.streamErr != nil {
		err := su.streamErr
		su.mu.Unlock()
		return err
	}

	// Lazy init — use a very large totalSize since we don't know final size yet.
	// This only affects server plan metadata, not actual upload correctness.
	if !su.started {
		su.writer = su.client.NewStreamWriterConditional(ctx, su.path, math.MaxInt64, su.expectedRevision)
		su.started = true
	}
	sw := su.writer

	su.inflightWg.Add(1)
	su.mu.Unlock()

	// Make a copy of data for the background upload
	buf := make([]byte, len(data))
	copy(buf, data)

	go func() {
		defer su.inflightWg.Done()

		// sw.WritePart queues the actual S3 upload in a background goroutine
		// inside StreamWriter. It copies buf internally, so buf is safe to
		// reference after WritePart returns. The actual upload completes when
		// sw.Complete() calls sw.inflight.Wait() inside FinishStreaming.
		//
		// We mark streamedParts and call onDone here (after WritePart returns)
		// rather than after the S3 upload finishes, because:
		// 1. WritePart has already copied the data — eviction is safe.
		// 2. If the S3 upload later fails, streamErr is set by StreamWriter,
		//    and FinishStreaming checks it before calling Complete.
		// 3. onDone (EvictPart) only releases memory — it does not affect
		//    upload correctness.
		//
		// IMPORTANT: flushHandle must release fh.mu before calling
		// FinishStreaming, because onDone acquires fh.mu. Otherwise deadlock:
		//   fh.Lock() → FinishStreaming → inflightWg.Wait() → onDone → fh.Lock()
		err := sw.WritePart(ctx, partNum, buf)
		if err != nil {
			su.mu.Lock()
			if su.streamErr == nil {
				su.streamErr = err
			}
			su.mu.Unlock()
			log.Printf("streaming upload part %d failed for %s: %v", partNum, su.path, err)
			return
		}

		su.mu.Lock()
		su.streamedParts[partNum] = true
		su.mu.Unlock()

		if onDone != nil {
			onDone(partNum)
		}
	}()

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
