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

func (s *s3Wrap) Put(ctx context.Context, key string, in io.Reader, _ ...object.AttrGetter) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	return s.s3.PutObject(ctx, s.key(key), bytes.NewReader(data), int64(len(data)), s3client.EncryptionOpts{})
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
