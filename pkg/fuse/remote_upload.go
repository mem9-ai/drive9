package fuse

import (
	"bytes"
	"context"
	"io"

	"github.com/mem9-ai/dat9/pkg/client"
)

// uploadBufferedRemoteFile keeps FUSE background uploads aligned with the
// foreground flush path: small files can still use direct PUT, while larger
// files must go through the multipart client flow so the server receives the
// required part metadata/checksum semantics.
func uploadBufferedRemoteFile(ctx context.Context, c *client.Client, remotePath string, data []byte, expectedRevision int64) error {
	return c.WriteStreamConditional(ctx, remotePath, bytes.NewReader(data), int64(len(data)), nil, expectedRevision)
}

// uploadFromShadow streams a shadow file to the server without loading the
// entire file into memory. Uses io.SectionReader to wrap the shadow store's
// ReadAt into an io.Reader for WriteStreamConditional.
func uploadFromShadow(ctx context.Context, c *client.Client, shadows *ShadowStore, remotePath string, expectedRevision int64) error {
	// Sync shadow to disk before uploading to ensure all data is durable.
	if err := shadows.Sync(remotePath); err != nil {
		return err
	}
	size := shadows.Size(remotePath)
	if size < 0 {
		return io.ErrUnexpectedEOF
	}
	if size == 0 {
		return c.WriteStreamConditional(ctx, remotePath, bytes.NewReader(nil), 0, nil, expectedRevision)
	}
	ra := &shadowReaderAt{store: shadows, path: remotePath}
	sr := io.NewSectionReader(ra, 0, size)
	return c.WriteStreamConditional(ctx, remotePath, sr, size, nil, expectedRevision)
}

// shadowReaderAt adapts ShadowStore.ReadAt into an io.ReaderAt.
type shadowReaderAt struct {
	store *ShadowStore
	path  string
}

func (s *shadowReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return s.store.ReadAt(s.path, off, p)
}
