package extent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

// WrapS3 exposes a tenant-prefixed JuiceFS ObjectStorage over drive9's S3 client.
func WrapS3(s3 s3client.S3Client, prefix string) (object.ObjectStorage, error) {
	if s3 == nil {
		return nil, fmt.Errorf("nil s3 client")
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	st := &s3Wrap{s3: s3, prefix: prefix}
	return st, nil
}

type s3Wrap struct {
	object.DefaultObjectStorage
	s3     s3client.S3Client
	prefix string
}

func (s *s3Wrap) String() string { return "drive9-s3-wrap" }

func (s *s3Wrap) key(key string) string {
	if s.prefix == "" || strings.HasPrefix(key, s.prefix) {
		return key
	}
	return s.prefix + strings.TrimPrefix(key, "/")
}

func (s *s3Wrap) Get(ctx context.Context, key string, off, limit int64, _ ...object.AttrGetter) (io.ReadCloser, error) {
	rc, err := s.s3.GetObject(ctx, s.key(key))
	if err != nil {
		return nil, err
	}
	if off <= 0 && limit <= 0 {
		return rc, nil
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, err
	}
	if off > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	data = data[off:]
	if limit > 0 && int64(len(data)) > limit {
		data = data[:limit]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Put streams the payload to S3. object.ObjectStorage.Put carries no length,
// but s3client.PutObject needs Content-Length, so the length is recovered
// without buffering the payload: JuiceFS's chunk store always hands Put a
// seekable in-memory reader (pkg/chunk/cached_store.go put passes a
// *bytes.Reader), so measuring is a Seek to the end and back. Only a
// non-seekable reader — nothing in the compaction path produces one today — is
// spooled to a temp file. Either way a merged chunk (up to 64 MiB, which the
// old io.ReadAll copied into the Go heap on every upload) is streamed.
func (s *s3Wrap) Put(ctx context.Context, key string, in io.Reader, _ ...object.AttrGetter) error {
	body, size, cleanup, err := sizedBody(in)
	if err != nil {
		return err
	}
	defer cleanup()
	return s.s3.PutObject(ctx, s.key(key), body, size, s3client.EncryptionOpts{})
}

// sizedBody returns in together with its remaining length, measured without
// buffering. A non-seekable reader is copied to a temp file, which yields the
// length and also makes the body rewindable — the object client re-reads it
// when a plain-HTTP endpoint signs the payload.
func sizedBody(in io.Reader) (io.Reader, int64, func(), error) {
	if in == nil {
		return nil, 0, func() {}, fmt.Errorf("extent s3 put: nil body")
	}
	if seeker, ok := in.(io.Seeker); ok {
		cur, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, 0, func() {}, fmt.Errorf("extent s3 put: seek: %w", err)
		}
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, 0, func() {}, fmt.Errorf("extent s3 put: seek end: %w", err)
		}
		if _, err := seeker.Seek(cur, io.SeekStart); err != nil {
			return nil, 0, func() {}, fmt.Errorf("extent s3 put: rewind: %w", err)
		}
		return in, end - cur, func() {}, nil
	}
	f, err := os.CreateTemp("", "drive9-extent-put-*")
	if err != nil {
		return nil, 0, func() {}, fmt.Errorf("extent s3 put: temp file: %w", err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	if _, err := io.Copy(f, in); err != nil {
		cleanup()
		return nil, 0, func() {}, fmt.Errorf("extent s3 put: spool body: %w", err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		cleanup()
		return nil, 0, func() {}, fmt.Errorf("extent s3 put: measure spooled body: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, func() {}, fmt.Errorf("extent s3 put: rewind spooled body: %w", err)
	}
	return f, size, cleanup, nil
}

func (s *s3Wrap) Delete(ctx context.Context, key string, _ ...object.AttrGetter) error {
	return s.s3.DeleteObject(ctx, s.key(key))
}

func (s *s3Wrap) Head(ctx context.Context, key string) (object.Object, error) {
	rc, err := s.s3.GetObject(ctx, s.key(key))
	if err != nil {
		return nil, os.ErrNotExist
	}
	n, _ := io.Copy(io.Discard, rc)
	_ = rc.Close()
	return &blockObj{key: key, size: n, mtime: time.Now()}, nil
}

type blockObj struct {
	key   string
	size  int64
	mtime time.Time
}

func (o *blockObj) Key() string          { return o.key }
func (o *blockObj) Size() int64          { return o.size }
func (o *blockObj) Mtime() time.Time     { return o.mtime }
func (o *blockObj) IsDir() bool          { return false }
func (o *blockObj) IsSymlink() bool      { return false }
func (o *blockObj) StorageClass() string { return "" }
func (o *blockObj) Status() string       { return "" }
