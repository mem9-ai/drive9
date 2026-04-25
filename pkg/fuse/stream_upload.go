package fuse

import (
	"context"
	"log"
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
//     writes. Parts are buffered locally during Write() and memory is released
//     via onDone. At close, FinishStreaming initiates the server-side multipart
//     upload with the actual total size, uploads all buffered parts, and completes.
//
// Note: The server's v2 upload protocol treats totalSize as the exact final
// size (used for part count validation at confirm time), so we cannot initiate
// with an estimated upper bound. Instead, SubmitPart buffers parts locally and
// FinishStreaming initiates with the real size.
type StreamUploader struct {
	client           *client.Client
	path             string
	expectedRevision int64

	mu            sync.Mutex
	writer        *client.StreamWriter
	started       bool
	streamedParts map[int]bool   // 1-based part numbers submitted during streaming
	pendingParts  map[int][]byte // buffered part data awaiting server upload
	inflightWg    sync.WaitGroup
	streamErr     error // first error from streaming upload
}

// NewStreamUploader creates a StreamUploader for the given path.
// No network calls are made until UploadAll or FinishStreaming is called.
func NewStreamUploader(c *client.Client, path string, expectedRevision int64) *StreamUploader {
	return &StreamUploader{
		client:           c,
		path:             path,
		expectedRevision: expectedRevision,
		streamedParts:    make(map[int]bool),
		pendingParts:     make(map[int][]byte),
	}
}

// Started reports whether parts have been submitted for streaming.
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
	su.pendingParts = make(map[int][]byte)
}

// HasStreamedParts reports whether any parts were submitted during streaming
// (i.e., during Write() calls, not at flush time).
func (su *StreamUploader) HasStreamedParts() bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return len(su.streamedParts) > 0
}

// SubmitPart buffers a part for later upload during FinishStreaming.
// partNum is 1-based. data is copied internally.
// onDone is called immediately after buffering (allowing WriteBuffer eviction).
//
// The actual server-side multipart upload is deferred to FinishStreaming,
// which knows the final file size — required because the server validates
// totalSize as exact at confirm time.
func (su *StreamUploader) SubmitPart(ctx context.Context, partNum int, data []byte, onDone func(int)) error {
	su.mu.Lock()

	if su.streamErr != nil {
		err := su.streamErr
		su.mu.Unlock()
		return err
	}

	su.started = true

	// Copy data for deferred upload.
	buf := make([]byte, len(data))
	copy(buf, data)
	su.pendingParts[partNum] = buf
	su.streamedParts[partNum] = true

	su.mu.Unlock()

	// Signal that the data has been copied — caller can evict from WriteBuffer.
	if onDone != nil {
		onDone(partNum)
	}

	return nil
}

// FinishStreaming completes a streaming upload:
//  1. Initiate the server-side multipart upload with the actual totalSize
//  2. Upload all buffered parts + any dirty (back-written) parts
//  3. Upload the final (partial) part via Complete
//
// lastPartNum is 1-based, lastPartData is the data for the final part.
// dirtyParts is a map of 1-based partNum → data for back-written parts.
func (su *StreamUploader) FinishStreaming(ctx context.Context, totalSize int64,
	lastPartNum int, lastPartData []byte, dirtyParts map[int][]byte) error {

	su.mu.Lock()
	if su.streamErr != nil {
		err := su.streamErr
		su.mu.Unlock()
		return err
	}

	// Initiate the server-side upload now that we know the exact total size.
	su.writer = su.client.NewStreamWriterConditional(ctx, su.path, totalSize, su.expectedRevision)
	sw := su.writer

	// Collect all buffered parts.
	pending := su.pendingParts
	su.pendingParts = make(map[int][]byte)
	su.mu.Unlock()

	// Upload all buffered parts (from SubmitPart during Write).
	for pn, data := range pending {
		if pn == lastPartNum && lastPartData != nil {
			// Last part will be handled by Complete — skip if it's also
			// the final part. But if lastPartData differs (back-written),
			// the dirtyParts map handles that below.
			continue
		}
		if err := sw.WritePart(ctx, pn, data); err != nil {
			abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = sw.Abort(abortCtx)
			cancel()
			return err
		}
	}

	// Re-upload dirty (back-written) parts.
	for pn, data := range dirtyParts {
		if err := sw.WritePart(ctx, pn, data); err != nil {
			abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = sw.Abort(abortCtx)
			cancel()
			return err
		}
	}

	// Complete with the last part.
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
	su.pendingParts = make(map[int][]byte) // release buffered data
	su.mu.Unlock()

	if sw != nil {
		if err := sw.Abort(context.Background()); err != nil {
			log.Printf("stream upload abort failed for %s: %v", su.path, err)
		}
	}
}
