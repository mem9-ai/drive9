// A seekable remote-file adapter: the client's multipart upload path
// requires an io.ReaderAt, and streaming a remote source into a shadow or
// commit destination must not materialize the whole file. ReadAt serves
// each slice with one ranged fetch.
package layerfs

import (
	"context"
	"fmt"
	"io"

	"github.com/mem9-ai/drive9/pkg/client"
)

// rangedReaderAt adapts a remote file to io.ReaderAt/io.Seeker with
// per-slice ranged reads.
type rangedReaderAt struct {
	ctx  context.Context
	cl   *client.Client
	path string
	size int64
	off  int64
}

// newRangedReaderAt wraps a remote file of the given size.
func newRangedReaderAt(ctx context.Context, cl *client.Client, path string, size int64) (*rangedReaderAt, error) {
	return &rangedReaderAt{ctx: ctx, cl: cl, path: path, size: size}, nil
}

// ReadAt implements io.ReaderAt with one ranged request per call.
func (r *rangedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > r.size {
		want = r.size - off
	}
	if want <= 0 {
		return 0, io.EOF
	}
	rc, err := r.cl.ReadStreamRange(r.ctx, r.path, off, want)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rc.Close() }()
	n, err := io.ReadFull(rc, p[:want])
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return n, io.EOF
	}
	return n, err
}

// Read implements io.Reader sequentially.
func (r *rangedReaderAt) Read(p []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	n, err := r.ReadAt(p, r.off)
	r.off += int64(n)
	return n, err
}

// Seek implements io.Seeker.
func (r *rangedReaderAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// offset as-is
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, fmt.Errorf("ranged reader: invalid whence %d", whence)
	}
	if offset < 0 {
		return 0, fmt.Errorf("ranged reader: negative seek")
	}
	r.off = offset
	return offset, nil
}
