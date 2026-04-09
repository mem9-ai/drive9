package fuse

import (
	"context"
	"log"
	"sync"

	"github.com/mem9-ai/dat9/pkg/client"
)

// StreamUploader manages parallel part uploads at flush time.
//
// Unlike the previous design which uploaded parts eagerly during Write(),
// this uploader is only used at flush/close time. This is necessary because
// FUSE writers can revisit any offset before close (e.g. patching headers,
// updating checksums, writing footers), and the final file size is not known
// until close. Eagerly uploading parts during Write() would persist stale
// content for any rewritten parts.
//
// At flush time, flushHandle calls UploadAll with the final dirty parts,
// which initiates a v2 multipart upload and uploads all parts in parallel.
type StreamUploader struct {
	client *client.Client
	path   string

	mu      sync.Mutex
	writer  *client.StreamWriter
	started bool
}

// NewStreamUploader creates a StreamUploader for the given path.
// No network calls are made until UploadAll is called at flush time.
func NewStreamUploader(c *client.Client, path string) *StreamUploader {
	return &StreamUploader{
		client: c,
		path:   path,
	}
}

// Started reports whether the upload has been initiated.
func (su *StreamUploader) Started() bool {
	su.mu.Lock()
	defer su.mu.Unlock()
	return su.started
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
	su.writer = su.client.NewStreamWriter(ctx, su.path, totalSize)
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
			_ = sw.Abort(ctx)
			return err
		}
	}

	// Complete with the last part
	lastPartData := partData[maxPart]
	err := sw.Complete(ctx, maxPart, lastPartData)
	if err != nil {
		_ = sw.Abort(ctx)
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
