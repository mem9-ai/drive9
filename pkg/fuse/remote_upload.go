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

// uploadFromShadowRemote streams a shadow file to the server without loading
// the entire file into memory. localPath identifies the shadow entry;
// remotePath is the API destination (may differ when using RemoteRoot).
func uploadFromShadowRemote(ctx context.Context, c *client.Client, shadows *ShadowStore, localPath, remotePath string, expectedRevision int64) error {
	_, err := uploadFromShadowRemoteWithRevision(ctx, c, shadows, localPath, remotePath, expectedRevision)
	return err
}

func uploadFromShadowRemoteWithRevision(ctx context.Context, c *client.Client, shadows *ShadowStore, localPath, remotePath string, expectedRevision int64) (int64, error) {
	// Sync shadow to disk before uploading to ensure all data is durable.
	if err := shadows.Sync(localPath); err != nil {
		return 0, err
	}
	size := shadows.Size(localPath)
	if size < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if size == 0 {
		return c.WriteCtxConditionalWithRevision(ctx, remotePath, nil, expectedRevision)
	}
	ra := &shadowReaderAt{store: shadows, path: localPath}
	sr := io.NewSectionReader(ra, 0, size)
	return 0, c.WriteMultipartStreamConditional(ctx, remotePath, sr, size, nil, expectedRevision)
}

// shadowReaderAt adapts ShadowStore.ReadAt into an io.ReaderAt.
type shadowReaderAt struct {
	store *ShadowStore
	path  string
}

func (s *shadowReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return s.store.ReadAt(s.path, off, p)
}
