package fuse

import (
	"context"
	"sync"

	"github.com/mem9-ai/dat9/pkg/client"
)

// StreamUploader manages background part uploads driven by WriteBuffer's
// OnPartReady callback. When a part is fully written (8MB), it is
// immediately uploaded in the background. On close/flush, only the
// last partial part + CompleteMultipartUpload are needed.
type StreamUploader struct {
	client    *client.Client
	path      string
	totalSize int64 // updated as file grows

	mu      sync.Mutex
	writer  *client.StreamWriter
	started bool
	err     error // first background error
}

// NewStreamUploader creates a StreamUploader for the given path.
// No network calls are made until the first part is submitted.
func NewStreamUploader(c *client.Client, path string) *StreamUploader {
	return &StreamUploader{
		client: c,
		path:   path,
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
func (su *StreamUploader) SubmitPart(partNum int, data []byte) {
	su.mu.Lock()
	if su.err != nil {
		su.mu.Unlock()
		return
	}

	// Lazy init the StreamWriter
	if !su.started {
		su.writer = su.client.NewStreamWriter(context.Background(), su.path, su.totalSize)
		su.started = true
	}
	sw := su.writer
	su.mu.Unlock()

	if err := sw.WritePart(context.Background(), partNum, data); err != nil {
		su.mu.Lock()
		if su.err == nil {
			su.err = err
		}
		su.mu.Unlock()
	}
}

// Finish uploads the last (possibly partial) part and completes the upload.
// totalSize must reflect the final file size. Returns any error from
// background uploads or the complete call.
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

	// Update total size on the StreamWriter by recreating if needed
	// The StreamWriter was created with potentially outdated totalSize.
	// Since the v2 initiate already happened with the initial size,
	// the server knows the total. We just need to complete with all parts.
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
		_ = sw.Abort(context.Background())
	}
}

// Err returns the first background error, if any.
func (su *StreamUploader) Err() error {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.err
}
