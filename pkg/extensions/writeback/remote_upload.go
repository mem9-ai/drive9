package writeback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"io"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	// The zero value keeps the local barrier, including legacy recovery.
	shadowUploadLocalDurable shadowUploadDurability = iota
	// Foreground close-sync may rely on remote durability for an eligible source.
	shadowUploadRemoteDurable
)

// maxPathTruncateInMemoryBytes is the size threshold above which a
// clean-path truncate streams remotely instead of building the kept
// prefix in memory (v1 dat9fs). A var so tests can lower it.
var maxPathTruncateInMemoryBytes int64 = 64 << 20

var errInvalidShadowUpload = errors.New("invalid shadow upload configuration")

// shadowUploadDurability selects the local barrier before a remote commit.
type shadowUploadDurability uint8

// remoteTruncateReaderAt serves the truncated view of a remote file: bytes
// below the kept prefix are fetched on demand, the rest zero-filled.
type remoteTruncateReaderAt struct {
	ctx          context.Context
	client       *client.Client
	remotePath   string
	existingSize int64
	totalSize    int64
}

// ReadAt implements io.ReaderAt.
func (r *remoteTruncateReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.totalSize {
		return 0, io.EOF
	}
	if end := off + int64(len(p)); end > r.totalSize {
		p = p[:r.totalSize-off]
	}

	n := 0
	if off < r.existingSize {
		readLen := int64(len(p))
		if end := off + readLen; end > r.existingSize {
			readLen = r.existingSize - off
		}
		data, err := r.client.ReadAtCtx(r.ctx, r.remotePath, off, readLen)
		if err != nil {
			return 0, err
		}
		copy(p, data)
		n = len(data)
		if int64(n) != readLen {
			return n, io.ErrUnexpectedEOF
		}
	}
	clear(p[n:])
	return len(p), nil
}

// uploadBufferedRemoteFile keeps FUSE background uploads aligned with the
// foreground flush path: small files can still use direct PUT, while larger
// files must go through the multipart client flow so the server receives the
// required part metadata/checksum semantics.
func uploadBufferedRemoteFile(ctx context.Context, c *client.Client, remotePath string, data []byte, expectedRevision int64) error {
	return c.WriteStreamConditional(ctx, remotePath, bytes.NewReader(data), int64(len(data)), nil, expectedRevision)
}

func uploadBufferedRemoteFileWithRevision(ctx context.Context, c *client.Client, remotePath string, data []byte, expectedRevision int64) (int64, error) {
	if err := c.WriteStreamConditional(ctx, remotePath, bytes.NewReader(data), int64(len(data)), nil, expectedRevision); err != nil {
		return 0, err
	}
	return 0, nil
}

// isInlineShadowUpload is shared by the ordinary uploader and combined-mode
// eligibility. Empty files have their own direct-PUT path.
func isInlineShadowUpload(size, threshold int64) bool {
	return size > 0 && threshold > 0 && size < threshold
}

// uploadFromShadowRemote streams a shadow with generation fencing through remote
// acknowledgement. localPath identifies the shadow; remotePath is the API path.
// Remote-only durability requires an eligible generation-pinned foreground
// source; the caller must wait for the remote commit before ACK.
func uploadFromShadowRemote(ctx context.Context, c *client.Client, shadows *staging.ShadowStore, localPath, remotePath string, expectedRevision int64, expectedGen uint64, durability shadowUploadDurability) (int64, error) {
	switch durability {
	case shadowUploadLocalDurable:
		if err := shadows.SyncIfGeneration(localPath, expectedGen); err != nil {
			return 0, err
		}
	case shadowUploadRemoteDurable:
		// Guard future callers that might bypass foreground policy selection.
		if expectedGen == 0 {
			return 0, fmt.Errorf("%w: remote-durable shadow upload requires a generation", errInvalidShadowUpload)
		}
	default:
		return 0, fmt.Errorf("%w: durability %d", errInvalidShadowUpload, durability)
	}

	fd, size, release, err := shadows.OpenIfGeneration(localPath, expectedGen)
	if err != nil {
		return 0, err
	}
	defer release()
	defer func() { _ = fd.Close() }()
	if size == 0 {
		return c.WriteCtxConditionalWithRevision(ctx, remotePath, nil, expectedRevision)
	}
	threshold := c.CachedSmallFileThreshold()
	if isInlineShadowUpload(size, threshold) {
		data := make([]byte, size)
		if _, err := fd.ReadAt(data, 0); err != nil {
			return 0, err
		}
		return c.WriteCtxConditionalWithRevision(ctx, remotePath, data, expectedRevision)
	}
	sr := io.NewSectionReader(fd, 0, size)
	if err := c.WriteMultipartStreamConditional(ctx, remotePath, sr, size, nil, expectedRevision); err != nil {
		return 0, err
	}
	return 0, nil
}

// uploadRemoteTruncateFile commits a size change remotely without
// materializing the image (v1): a zero target writes an empty image; any
// other target streams a multipart upload whose source lazily reads the
// kept prefix and zero-extends to the new size.
func uploadRemoteTruncateFile(ctx context.Context, c *client.Client, remotePath string, oldSize, newSize int64, expectedRevision int64) error {
	if newSize == 0 {
		_, err := c.WriteCtxConditionalWithRevision(ctx, remotePath, nil, expectedRevision)
		return err
	}
	existingSize := oldSize
	if existingSize > newSize {
		existingSize = newSize
	}
	ra := &remoteTruncateReaderAt{
		ctx:          ctx,
		client:       c,
		remotePath:   remotePath,
		existingSize: existingSize,
		totalSize:    newSize,
	}
	return c.WriteMultipartStreamConditional(ctx, remotePath, ra, newSize, nil, expectedRevision)
}
