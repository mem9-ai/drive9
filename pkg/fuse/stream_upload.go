package fuse

import (
	"context"
	"log"
	"sync"

	"github.com/mem9-ai/dat9/pkg/client"
)

// StreamUploader manages background part uploads driven by WriteBuffer's
// OnPartReady callback. When a part is fully written (8MB), it is
// immediately uploaded in the background. On close/flush, only the
// last partial part + CompleteMultipartUpload are needed.
//
// SubmitPart returns an error so the caller knows if a submission failed.
// It tracks which part numbers have been submitted to avoid duplicate
// uploads (e.g. when OnPartReady fires for the same part on rewrite).
type StreamUploader struct {
	client    *client.Client
	path      string
	totalSize int64 // updated as file grows

	mu             sync.Mutex
	writer         *client.StreamWriter
	started        bool
	err            error        // first background error
	submittedParts map[int]bool // parts already submitted — prevents duplicates
}

// NewStreamUploader creates a StreamUploader for the given path.
// No network calls are made until the first part is submitted.
func NewStreamUploader(c *client.Client, path string) *StreamUploader {
	return &StreamUploader{
		client:         c,
		path:           path,
		submittedParts: make(map[int]bool),
	}
}

// Started reports whether any parts have been submitted.
func (su *StreamUploader) Started() bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.started
}

// SetTotalSize updates the expected total file size. Must be called
// before Finish so the StreamWriter knows the correct totalSize.
func (su *StreamUploader) SetTotalSize(size int64) {
	su.mu.Lock()
	defer su.mu.Unlock()
	su.totalSize = size
}

// SubmitPart uploads a part in the background. partNum is 1-based.
// data is owned by the caller — StreamUploader will copy it.
// Returns an error if the upload has already failed or the part couldn't
// be submitted. Duplicate submissions of the same partNum are silently
// ignored (idempotent).
func (su *StreamUploader) SubmitPart(partNum int, data []byte) error {
	su.mu.Lock()
	if su.err != nil {
		err := su.err
		su.mu.Unlock()
		return err
	}

	// Skip duplicate submissions (e.g. OnPartReady firing on rewrite)
	if su.submittedParts[partNum] {
		su.mu.Unlock()
		return nil
	}

	// Lazy init the StreamWriter
	if !su.started {
		su.writer = su.client.NewStreamWriter(context.Background(), su.path, su.totalSize)
		su.started = true
	}
	su.submittedParts[partNum] = true
	sw := su.writer
	su.mu.Unlock()

	if err := sw.WritePart(context.Background(), partNum, data); err != nil {
		su.mu.Lock()
		if su.err == nil {
			su.err = err
		}
		su.mu.Unlock()
		return err
	}
	return nil
}

// IsPartSubmitted reports whether a part has already been submitted.
func (su *StreamUploader) IsPartSubmitted(partNum int) bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.submittedParts[partNum]
}

// Finish uploads the last (possibly partial) part and completes the upload.
// lastPartNum and lastPartData describe the final part. If the last part
// was already submitted via SubmitPart (exact-multiple-of-partSize case),
// pass nil for lastPartData.
// Returns any error from background uploads or the complete call.
func (su *StreamUploader) Finish(totalSize int64, lastPartNum int, lastPartData []byte) error {
	su.mu.Lock()
	if su.err != nil {
		err := su.err
		sw := su.writer
		su.mu.Unlock()
		if sw != nil {
			_ = sw.Abort(context.Background())
		}
		return err
	}

	if !su.started {
		// Never started — shouldn't happen, but handle gracefully
		su.mu.Unlock()
		return nil
	}

	// If the last part was already submitted (file size is exact multiple
	// of partSize), don't upload it again — pass nil data to Complete.
	if su.submittedParts[lastPartNum] {
		lastPartData = nil
	}

	sw := su.writer
	su.mu.Unlock()

	err := sw.Complete(context.Background(), lastPartNum, lastPartData)
	if err != nil {
		_ = sw.Abort(context.Background())
		return err
	}
	return nil
}

// Abort cancels the upload and cleans up server-side state.
func (su *StreamUploader) Abort() {
	su.mu.Lock()
	sw := su.writer
	su.mu.Unlock()

	if sw != nil {
		if err := sw.Abort(context.Background()); err != nil {
			log.Printf("stream upload abort failed for %s: %v", su.path, err)
		}
	}
}

// Err returns the first background error, if any.
func (su *StreamUploader) Err() error {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.err
}
