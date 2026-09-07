package fuse

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/mem9-ai/drive9/pkg/client"
)

type drive9BlockStore struct {
	object.DefaultObjectStorage
	client *client.Client
}

func (s *drive9BlockStore) String() string { return "drive9blocks" }

func (s *drive9BlockStore) Get(ctx context.Context, key string, off, limit int64, _ ...object.AttrGetter) (io.ReadCloser, error) {
	rc, err := s.client.GetExtentBlock(ctx, key)
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

func (s *drive9BlockStore) Put(ctx context.Context, key string, in io.Reader, _ ...object.AttrGetter) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	return s.client.PutExtentBlock(ctx, key, bytes.NewReader(data), int64(len(data)))
}

func (s *drive9BlockStore) Delete(ctx context.Context, key string, _ ...object.AttrGetter) error {
	return s.client.DeleteExtentBlock(ctx, key)
}

func (s *drive9BlockStore) Head(_ context.Context, key string) (object.Object, error) {
	return nil, os.ErrNotExist
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
