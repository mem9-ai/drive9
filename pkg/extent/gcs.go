package extent

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/juicedata/juicefs/pkg/object"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

// gcsExtentChunkSize matches JuiceFS's gs backend. Extent blocks are 4 MiB, so a
// slightly larger resumable-upload chunk keeps one block in one chunk instead of
// making the client buffer the whole object.
const gcsExtentChunkSize = 5 << 20

// gcsTokenStorage is the extent data plane over Google Cloud Storage.
//
// JuiceFS's own "gs" backend builds storage.NewClient(ctx) from Application
// Default Credentials and ignores the accessKey/secretKey/token arguments, so
// the server's downscoped, tenant-prefix-bound OAuth token cannot be delivered
// through it. This store presents that token as the bearer credential instead:
// the token, not the mount's own identity, is the tenant isolation boundary.
type gcsTokenStorage struct {
	object.DefaultObjectStorage
	client *storage.Client
	bucket string
}

// newGCSTokenStorage builds a bucket-scoped GCS store that authenticates with
// accessToken. opts are appended after the token source for tests that need to
// retarget the endpoint.
func newGCSTokenStorage(ctx context.Context, bucket, accessToken string, opts ...option.ClientOption) (*gcsTokenStorage, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, fmt.Errorf("gcs bucket is required")
	}
	if strings.TrimSpace(accessToken) == "" {
		return nil, fmt.Errorf("gcs access token is required")
	}
	allOpts := append([]option.ClientOption{
		option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})),
	}, opts...)
	client, err := storage.NewClient(ctx, allOpts...)
	if err != nil {
		return nil, fmt.Errorf("gcs client: %w", err)
	}
	return &gcsTokenStorage{client: client, bucket: bucket}, nil
}

func (g *gcsTokenStorage) String() string { return "gs://" + g.bucket + "/" }

// Create is a no-op: drive9-server owns bucket lifecycle, and a tenant-scoped
// credential is deliberately not allowed to create one.
func (g *gcsTokenStorage) Create(context.Context) error { return nil }

func (g *gcsTokenStorage) Get(ctx context.Context, key string, off, limit int64, _ ...object.AttrGetter) (io.ReadCloser, error) {
	return g.client.Bucket(g.bucket).Object(key).NewRangeReader(ctx, off, limit)
}

func (g *gcsTokenStorage) Put(ctx context.Context, key string, in io.Reader, _ ...object.AttrGetter) error {
	writer := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	writer.ChunkSize = gcsExtentChunkSize
	if _, err := io.Copy(writer, in); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

// Delete is idempotent, mirroring JuiceFS's gs backend: deleting an object that
// is already gone is a success.
func (g *gcsTokenStorage) Delete(ctx context.Context, key string, _ ...object.AttrGetter) error {
	if err := g.client.Bucket(g.bucket).Object(key).Delete(ctx); err != nil && err != storage.ErrObjectNotExist {
		return err
	}
	return nil
}

func (g *gcsTokenStorage) Head(ctx context.Context, key string) (object.Object, error) {
	attrs, err := g.client.Bucket(g.bucket).Object(key).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return &blockObj{key: key, size: attrs.Size, mtime: attrs.Updated}, nil
}

// Shutdown releases the client's connections; JuiceFS calls it through
// object.Shutdown when the vfs is closed.
func (g *gcsTokenStorage) Shutdown() {
	if g != nil && g.client != nil {
		_ = g.client.Close()
	}
}
