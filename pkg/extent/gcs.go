package extent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/juicedata/juicefs/pkg/object"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
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

// gcsTokenSource renews the tenant's GCS bearer token in place. Because the
// storage client is created once over this source and never superseded, none of
// the per-generation store/client refcount machinery is needed.
type gcsTokenSource struct {
	src  CredentialSource
	lead time.Duration

	mu   sync.Mutex
	cred Credential
	tok  *oauth2.Token
}

func newGCSTokenSource(cred Credential, src CredentialSource) *gcsTokenSource {
	return &gcsTokenSource{
		src:  src,
		lead: credentialRefreshLead(credentialRefreshWindow, credentialRefreshJitter, rand.Int64N),
		cred: cred,
		tok:  &oauth2.Token{AccessToken: strings.TrimSpace(cred.AccessToken), Expiry: credentialExpiry(cred.ExpiresAt)},
	}
}

// Token returns the current bearer token, minting a replacement when the current
// one is within the refresh lead of expiry. A mint failure keeps serving the
// current token; an actually-expired token then surfaces as a 403 from the
// object store, with the refresh metric distinguishing it from a data-plane bug.
func (t *gcsTokenSource) Token() (*oauth2.Token, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// No source or no expiry means a static credential: never refresh.
	if t.src == nil || t.tok.Expiry.IsZero() {
		return t.tok, nil
	}
	if time.Until(t.tok.Expiry) > t.lead {
		return t.tok, nil
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), CredentialMintTimeout)
	defer cancel()
	next, err := t.src.GetDataCredential(ctx)
	if err != nil {
		logger.Warn(context.Background(), "extent_credential_refresh_failed",
			zap.String("prefix", t.cred.Prefix),
			zap.String("step", "mint"),
			zap.Error(err))
		metrics.RecordTenantOperation(t.tenantLabel(), "extent_storage", "refresh_credential", metrics.ResultForError(err), time.Since(start))
		return t.tok, nil
	}
	t.cred = *next
	t.tok = &oauth2.Token{AccessToken: strings.TrimSpace(next.AccessToken), Expiry: credentialExpiry(next.ExpiresAt)}
	metrics.RecordTenantOperation(t.tenantLabel(), "extent_storage", "refresh_credential", "ok", time.Since(start))
	return t.tok, nil
}

func (t *gcsTokenSource) tenantLabel() string {
	if t.cred.TenantID != "" {
		return t.cred.TenantID
	}
	return tenantFromPrefix(t.cred.Prefix)
}

// credentialExpiry parses an RFC3339 expiry; empty or malformed means "never
// refresh" (zero time).
func credentialExpiry(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	exp, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return exp
}

// newGCSTokenStorage builds a bucket-scoped GCS store that authenticates with
// tokenSource, which renews the server-minted tenant token in place so the
// client is created once and never superseded. A non-empty endpoint is the
// Cloud Storage JSON API root
// (https://storage.googleapis.com/storage/v1/, or an emulator's
// .../storage/v1/), not a bare host and not a path-prefixed proxy base: it
// replaces the generated client's BasePath and receives the bearer token, but
// reads honour BasePath while resumable uploads use an absolute
// /upload/storage/v1/... reference that discards it, so only the API root keeps
// both legs under the configured endpoint. It must be https; plaintext http is
// accepted only on loopback (a local emulator, where the credential never
// leaves the box). Reads use the JSON API so this base path is honoured.
func newGCSTokenStorage(ctx context.Context, bucket string, tokenSource oauth2.TokenSource, endpoint string) (*gcsTokenStorage, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, fmt.Errorf("gcs bucket is required")
	}
	if tokenSource == nil {
		return nil, fmt.Errorf("gcs token source is required")
	}
	// storage.NewClient diverts to its emulator branch whenever
	// STORAGE_EMULATOR_HOST is set, and that branch installs
	// option.WithoutAuthentication plus an unvalidated endpoint template. It
	// would silently relocate this credential-bearing store and drop the bearer
	// token, and with an explicit endpoint the (unauthenticated) request would
	// still go to that endpoint. Fail closed on either shape.
	if emu := strings.TrimSpace(os.Getenv("STORAGE_EMULATOR_HOST")); emu != "" {
		if strings.TrimSpace(endpoint) != "" {
			return nil, fmt.Errorf("STORAGE_EMULATOR_HOST %q cannot be combined with an explicit gcs endpoint", emu)
		}
		if !isLoopbackHost(emulatorHostname(emu)) {
			return nil, fmt.Errorf("STORAGE_EMULATOR_HOST %q must be loopback for the extent GCS store", emu)
		}
	}
	opts := []option.ClientOption{
		option.WithTokenSource(tokenSource),
		// Read through the JSON API: the XML read path drops the configured
		// /storage/v1/ base path, so Head could succeed while Get failed.
		storage.WithJSONReads(),
	}
	if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("gcs endpoint %q must be an https JSON API base path, not a bare host", endpoint)
		}
		if u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
			return nil, fmt.Errorf("gcs endpoint %q is plaintext; a bearer credential requires https except on loopback", endpoint)
		}
		// Reads merge this base path via ResolveRelative, but resumable uploads
		// use an absolute /upload/storage/v1/... reference that drops any path
		// prefix. Only the API root is safe: a prefixed endpoint would read fine
		// and then fail every block upload, so fail closed on anything else.
		if strings.Trim(u.Path, "/") != "storage/v1" {
			return nil, fmt.Errorf("gcs endpoint %q must be the JSON API root (.../storage/v1/); a path prefix is not honoured by uploads", endpoint)
		}
		endpoint = strings.TrimRight(endpoint, "/") + "/"
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs client: %w", err)
	}
	return &gcsTokenStorage{client: client, bucket: bucket}, nil
}

// emulatorHostname extracts the host from STORAGE_EMULATOR_HOST, which may be
// a bare host, host:port, or a full URL.
func emulatorHostname(raw string) string {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
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
		// Abort, don't Close: cancel() makes the storage package's monitorCancel
		// goroutine call CloseWithError(ctx.Err()), so the truncated object is
		// never finalized. A Close() here would race that goroutine and could
		// commit the partial body while returning the copy error.
		cancel()
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
