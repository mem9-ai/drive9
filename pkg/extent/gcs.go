package extent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
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
// through it. This store presents that token as the bearer credential instead.
//
// The store cannot verify the token's scope: it applies the tenant prefix
// client-side only. The requirement is on the producer — accessToken must be
// downscoped to projects/_/buckets/<bucket>/objects/<prefix>, because a
// bucket-wide token would let a mount reach every tenant's blocks.
type gcsTokenStorage struct {
	object.DefaultObjectStorage
	client *storage.Client
	bucket string
}

// newGCSTokenStorage builds a bucket-scoped GCS store that authenticates with
// accessToken. A non-empty endpoint is the Cloud Storage JSON API base path
// (e.g. https://storage.googleapis.com/storage/v1/, or an emulator's
// .../storage/v1/), not a bare host: it replaces the generated client's
// BasePath verbatim and receives the bearer token, so it is a trusted
// control-plane value. It must be https with a path; plaintext http is accepted
// only on loopback (a local emulator, where the credential never leaves the
// box). Reads use the JSON API so this base path is honoured.
func newGCSTokenStorage(ctx context.Context, bucket, accessToken, endpoint string) (*gcsTokenStorage, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, fmt.Errorf("gcs bucket is required")
	}
	if strings.TrimSpace(accessToken) == "" {
		return nil, fmt.Errorf("gcs access token is required")
	}
	opts := []option.ClientOption{
		option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})),
		// Read through the JSON API: the XML read path drops the configured
		// /storage/v1/ base path, so Head could succeed while Get failed.
		storage.WithJSONReads(),
	}
	if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || strings.Trim(u.Path, "/") == "" {
			return nil, fmt.Errorf("gcs endpoint %q must be an https JSON API base path, not a bare host", endpoint)
		}
		if u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
			return nil, fmt.Errorf("gcs endpoint %q is plaintext; a bearer credential requires https except on loopback", endpoint)
		}
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs client: %w", err)
	}
	return &gcsTokenStorage{client: client, bucket: bucket}, nil
}

// isLoopbackHost reports whether host is a loopback name or address, the only
// place a plaintext endpoint is tolerated: the credential never leaves the box.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (g *gcsTokenStorage) String() string { return "gs://" + g.bucket + "/" }

// Create is a no-op: drive9-server owns bucket lifecycle, and a tenant-scoped
// credential is deliberately not allowed to create one.
func (g *gcsTokenStorage) Create(context.Context) error { return nil }

func (g *gcsTokenStorage) Get(ctx context.Context, key string, off, limit int64, _ ...object.AttrGetter) (io.ReadCloser, error) {
	return g.client.Bucket(g.bucket).Object(key).NewRangeReader(ctx, off, limit)
}

func (g *gcsTokenStorage) Put(ctx context.Context, key string, in io.Reader, _ ...object.AttrGetter) error {
	// Cancel the upload context on failure: that aborts the resumable session,
	// whereas Close() would finalize a truncated object under the block key.
	// (CloseWithError is deprecated for exactly this reason.)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := g.writer(ctx, key)
	if _, err := io.Copy(writer, in); err != nil {
		cancel()
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

// writer builds the object writer used for every block. The chunk-size contract
// (one 4 MiB block per chunk, without buffering the whole object) is pinned
// through this seam by TestGCSWriterUsesExtentChunkSize.
func (g *gcsTokenStorage) writer(ctx context.Context, key string) *storage.Writer {
	writer := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	writer.ChunkSize = gcsExtentChunkSize
	return writer
}

// Delete is idempotent, mirroring JuiceFS's gs backend: deleting an object that
// is already gone is a success.
func (g *gcsTokenStorage) Delete(ctx context.Context, key string, _ ...object.AttrGetter) error {
	if err := g.client.Bucket(g.bucket).Object(key).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return err
	}
	return nil
}

func (g *gcsTokenStorage) Head(ctx context.Context, key string) (object.Object, error) {
	attrs, err := g.client.Bucket(g.bucket).Object(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return &blockObj{key: key, size: attrs.Size, mtime: attrs.Updated}, nil
}

// Shutdown releases the client. object.Shutdown reaches it through the prefix
// wrapper: CloseRuntime calls it at teardown, and the credential refresh closes
// the client it supersedes.
func (g *gcsTokenStorage) Shutdown() {
	if g != nil && g.client != nil {
		_ = g.client.Close()
	}
}
