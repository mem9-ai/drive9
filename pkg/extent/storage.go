package extent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	SchemeFile = "file"
	SchemeS3   = "s3"
	SchemeGCS  = "gcs"

	// credentialRefreshWindow is how long before expiry the data plane renews
	// its STS session. The old session stays valid while the replacement is
	// minted, so renewing early costs nothing.
	credentialRefreshWindow = 2 * time.Minute
	// credentialRefreshJitter spreads renewals over the last part of the
	// refresh window: a mount renews between
	// credentialRefreshWindow-credentialRefreshJitter and
	// credentialRefreshWindow before expiry. Without it every mount of a
	// tenant that started in the same instant (rolling restart, fleet-wide
	// remount) calls AssumeRole in the same instant.
	credentialRefreshJitter = time.Minute
	// CredentialMintTimeout bounds a credential mint. It is exported so the
	// mount's runtime initialization uses the same policy.
	CredentialMintTimeout = 30 * time.Second
	// jfsS3VHostStyleEnv is JuiceFS's only path-style switch
	// (pkg/object/s3.go defaultPathStyle): unset/"0"/"false" selects path
	// style, anything else virtual-host style. JuiceFS reads it once per
	// storage construction.
	jfsS3VHostStyleEnv = "JFS_S3_VHOST_STYLE"
)

// Credential is the tenant-prefix object-store session from POST /v1/data-credential.
type Credential struct {
	Scheme string `json:"scheme"`
	// Endpoint is the scheme-specific base. For "file" it is the storage root,
	// for "s3" the S3 endpoint, and for "gcs" the Cloud Storage JSON API base
	// path (e.g. https://storage.googleapis.com/storage/v1/), not a bare host.
	// A "gcs" endpoint also receives the bearer access token, so it is a trusted
	// control-plane value; the store fails closed on a bare host.
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket,omitempty"`
	Prefix          string `json:"prefix"`
	TenantID        string `json:"tenant_id,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	// AccessToken is a short-lived, tenant-prefix-scoped OAuth token (scheme
	// "gcs"). The mount presents it to Cloud Storage directly, so it is a
	// bearer credential and must never be logged.
	AccessToken    string `json:"access_token,omitempty"`
	Region         string `json:"region,omitempty"`
	ForcePathStyle bool   `json:"force_path_style,omitempty"`
	// EncryptionMode/KeyID describe the deployment's resolved S3 object
	// encryption policy. The server only mints this credential when the extent
	// data plane can honour it (no per-object SSE), so the values are
	// informational here: JuiceFS's S3 backend sets no SSE headers.
	EncryptionMode  string `json:"encryption_mode,omitempty"`
	EncryptionKeyID string `json:"encryption_key_id,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

// CredentialSource refreshes STS sessions. *client.Client implements this.
type CredentialSource interface {
	GetDataCredential(ctx context.Context) (*Credential, error)
}

// errExtentStoreClosed is returned by every I/O entry point after Shutdown.
var errExtentStoreClosed = errors.New("extent storage is closed")

// canonicalExtentScheme maps a data-credential scheme to the one this package
// reports. Cloud Storage is minted as "gcs", while the object-backend/objectfs
// surfaces canonicalize to "gs"; both are accepted and reported as "gcs" so
// String() and metric labels stay stable.
func canonicalExtentScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "", SchemeFile:
		return SchemeFile
	case SchemeS3:
		return SchemeS3
	case "gs", SchemeGCS:
		return SchemeGCS
	default:
		return strings.ToLower(strings.TrimSpace(scheme))
	}
}

// OpenStorage builds a JuiceFS ObjectStorage from a data-credential response.
// file is the in-process mock used by unit tests. s3 is the production and
// local-MinIO data plane (STS or static keys). gcs is the Google Cloud Storage
// data plane, authenticated with the server's downscoped, tenant-prefix-bound
// OAuth token. Blocks are never proxied through drive9-server.
func OpenStorage(cred *Credential, src CredentialSource) (object.ObjectStorage, error) {
	if cred == nil {
		return nil, fmt.Errorf("nil data credential")
	}
	inner, err := openInner(cred, src)
	if err != nil {
		return nil, err
	}
	prefix := cred.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix != "" {
		inner = object.WithPrefix(inner, prefix)
	}
	// The GCS client renews its own token in place, so the wrapper never swaps
	// it; only the stateless S3/file arms re-open on refresh.
	wrapperSrc := src
	if canonicalExtentScheme(cred.Scheme) == SchemeGCS {
		wrapperSrc = nil
	}
	return &refreshingStore{
		src:    wrapperSrc,
		inner:  inner,
		cred:   *cred,
		scheme: canonicalExtentScheme(cred.Scheme),
		prefix: prefix,
		// One jittered lead per mount: mounts of the same tenant then renew
		// at spread-out instants instead of together.
		refreshLead: credentialRefreshLead(credentialRefreshWindow, credentialRefreshJitter, rand.Int64N),
	}, nil
}

// credentialRefreshLead returns the lead time before expiry at which a store
// renews its session: base minus up to jitter. The result never exceeds base,
// so the jitter can only renew earlier than the un-jittered window — renewing
// later could race credential expiry. int64n is rand.Int64N in production and
// a stub in tests.
func credentialRefreshLead(base, jitter time.Duration, int64n func(int64) int64) time.Duration {
	if base <= 0 || jitter <= 0 || int64n == nil {
		return base
	}
	if jitter > base {
		jitter = base
	}
	return base - time.Duration(int64n(int64(jitter)+1))
}

// vhostStyleValue maps the credential's force_path_style onto JuiceFS's
// environment switch: "0" keeps path style (the JuiceFS default, required by
// MinIO-style endpoints), "1" asks for virtual-host style.
func vhostStyleValue(forcePathStyle bool) string {
	if forcePathStyle {
		return "0"
	}
	return "1"
}

// s3StyleMu serializes JFS_S3_VHOST_STYLE mutation; see createS3Storage.
var s3StyleMu sync.Mutex

// createS3Storage builds the JuiceFS s3 storage for one credential. JuiceFS
// picks path style vs virtual-host style from JFS_S3_VHOST_STYLE alone and
// reads it once while constructing the SDK client, so the variable is set for
// exactly that window and restored afterwards. Honouring the credential keeps
// the data plane consistent with the control plane: a bucket the server
// reaches with virtual-host URLs is reached the same way from the mount.
func createS3Storage(endpoint, accessKey, secretKey, token string, forcePathStyle bool) (object.ObjectStorage, error) {
	return withS3StyleEnv(forcePathStyle, func() (object.ObjectStorage, error) {
		return object.CreateStorage("s3", endpoint, accessKey, secretKey, token)
	})
}

// withS3StyleEnv runs create with JFS_S3_VHOST_STYLE set to the value the
// credential asks for, then restores the previous value.
func withS3StyleEnv(forcePathStyle bool, create func() (object.ObjectStorage, error)) (object.ObjectStorage, error) {
	s3StyleMu.Lock()
	defer s3StyleMu.Unlock()
	prev, had := os.LookupEnv(jfsS3VHostStyleEnv)
	_ = os.Setenv(jfsS3VHostStyleEnv, vhostStyleValue(forcePathStyle))
	st, err := create()
	if had {
		_ = os.Setenv(jfsS3VHostStyleEnv, prev)
	} else {
		_ = os.Unsetenv(jfsS3VHostStyleEnv)
	}
	return st, err
}

func openInner(cred *Credential, src CredentialSource) (object.ObjectStorage, error) {
	switch canonicalExtentScheme(cred.Scheme) {
	case SchemeFile:
		root := cred.Endpoint
		if root == "" {
			return nil, fmt.Errorf("file storage endpoint is required")
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(root, "/") {
			root += "/"
		}
		st, err := object.CreateStorage("file", root, "", "", "")
		if err != nil {
			return nil, err
		}
		return st, st.Create(context.Background())
	case SchemeS3:
		ep := endpointWithBucket(cred.Endpoint, cred.Bucket)
		return createS3Storage(ep, cred.AccessKeyID, cred.SecretAccessKey, cred.SessionToken, cred.ForcePathStyle)
	case SchemeGCS:
		// canonicalExtentScheme folds the object-backend/objectfs "gs" spelling
		// onto the extent wire value "gcs". An endpoint is honoured for an
		// emulator or a private/regional JSON API base; the server's own mint
		// leaves it empty. The client renews its own token from src in place.
		if strings.TrimSpace(cred.AccessToken) == "" {
			return nil, fmt.Errorf("gcs access token is required")
		}
		return newGCSTokenStorage(context.Background(), cred.Bucket, newGCSTokenSource(*cred, src), cred.Endpoint)
	case "drive9":
		return nil, fmt.Errorf("extent data plane does not proxy blocks through drive9-server")
	default:
		return nil, fmt.Errorf("unsupported data-credential scheme %q", cred.Scheme)
	}
}

// endpointWithBucket appends the bucket to an S3 endpoint that does not
// already name one, which is the form JuiceFS's s3 backend expects for a
// path-style endpoint.
//
// The decision has to look at the parsed host and path, not at a substring of
// the URL: bucket "prod" with endpoint "https://prod-storage.example.com"
// contains the bucket name but does not name it, and the old substring check
// therefore handed JuiceFS an endpoint with no bucket at all.
func endpointWithBucket(endpoint, bucket string) string {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return endpoint
	}
	raw := strings.TrimSpace(endpoint)
	if raw == "" {
		return bucket
	}
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		if strings.Trim(u.Path, "/") != "" {
			return endpoint
		}
		u.Path = "/" + bucket
		return u.String()
	}
	trimmed := strings.TrimRight(raw, "/")
	if last := trimmed[strings.LastIndex(trimmed, "/")+1:]; last == bucket {
		return endpoint
	}
	return trimmed + "/" + bucket
}

// refreshingStore renews a stateless S3/file session by re-opening the inner
// store when the credential is near expiry. The GCS client is not handled here:
// it renews its own token in place (see gcsTokenSource), so no superseded store
// ever owns a resource that needs exactly-once teardown.
type refreshingStore struct {
	object.DefaultObjectStorage
	src    CredentialSource
	mu     sync.Mutex
	inner  object.ObjectStorage
	cred   Credential
	scheme string
	// prefix comes from the first credential and is stable across refreshes, so
	// the read-only accessors can serve it without taking mu (scheme is likewise
	// written once at construction).
	prefix string
	// closed is set under mu by Shutdown; store() refuses to serve or refresh
	// after teardown.
	closed bool
	// refreshLead is how long before expiry this store renews, jittered per
	// store instance (see credentialRefreshLead).
	refreshLead time.Duration
	puts        atomic.Int64
	gets        atomic.Int64
}

func (s *refreshingStore) String() string {
	return "drive9-" + s.scheme
}

func (s *refreshingStore) Scheme() string { return s.scheme }

func (s *refreshingStore) Prefix() string { return s.prefix }

// Shutdown releases the inner store's resources (for example the GCS client).
// It is idempotent.
func (s *refreshingStore) Shutdown() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	object.Shutdown(s.inner)
}

// store returns the inner store for one operation, refreshing the session first
// when needed. The returned store is safe to use without a hold: S3/file stores
// are stateless and the GCS client is never replaced.
func (s *refreshingStore) store(ctx context.Context) (object.ObjectStorage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errExtentStoreClosed
	}
	return s.innerStoreLocked(ctx)
}

func (s *refreshingStore) PutCount() int64 { return s.puts.Load() }

func (s *refreshingStore) GetCount() int64 { return s.gets.Load() }

// tenantLabel identifies the tenant for logs and metrics: the explicit
// credential field when the server sent one, else the tenant parsed out of the
// prefix ("t/<tenant>/") for older servers.
// tenantLabel is only called while s.mu is held, so it may read s.cred.
func (s *refreshingStore) tenantLabel() string {
	if s.cred.TenantID != "" {
		return s.cred.TenantID
	}
	return tenantFromPrefix(s.cred.Prefix)
}

func tenantFromPrefix(prefix string) string {
	rest, ok := strings.CutPrefix(strings.Trim(prefix, "/"), "t/")
	if !ok || rest == "" || strings.Contains(rest, "/") {
		return ""
	}
	return rest
}

// innerStoreLocked resolves the store for one operation and refreshes it when
// the credential is near expiry. The caller holds s.mu.
func (s *refreshingStore) innerStoreLocked(ctx context.Context) (object.ObjectStorage, error) {
	if s.src == nil || s.cred.ExpiresAt == "" {
		return s.inner, nil
	}
	exp, err := time.Parse(time.RFC3339, s.cred.ExpiresAt)
	if err != nil {
		return s.inner, nil
	}
	lead := s.refreshLead
	if lead <= 0 {
		// Defensive: a store built outside OpenStorage still renews early.
		lead = credentialRefreshWindow
	}
	if time.Until(exp) > lead {
		return s.inner, nil
	}
	start := time.Now()
	// Bound the mint: it runs under s.mu, so an unbounded call would also stall
	// Shutdown.
	mintCtx, mintCancel := context.WithTimeout(ctx, CredentialMintTimeout)
	defer mintCancel()
	next, err := s.src.GetDataCredential(mintCtx)
	if err != nil {
		// Keep serving the old session, but never silently: an expired
		// credential surfaces as a 403 from the object store, which is
		// otherwise indistinguishable from a data-plane bug.
		s.recordRefreshFailure(ctx, "mint", start, err)
		return s.inner, nil
	}
	inner, err := OpenStorage(next, s.src)
	if err != nil {
		s.recordRefreshFailure(ctx, "open", start, err)
		return nil, err
	}
	superseded := s.inner
	// OpenStorage always wraps its result in a *refreshingStore; adopt that
	// store's internals rather than re-deriving the scheme from the credential
	// here (a second source of truth for the scheme).
	rs, ok := inner.(*refreshingStore)
	if !ok {
		object.Shutdown(inner)
		return nil, fmt.Errorf("extent storage refresh: %T is not a refreshingStore", inner)
	}
	s.inner = rs.inner
	s.cred = rs.cred // refresh the expiry the next call checks
	// scheme/prefix are stable across refreshes and stay immutable, so the
	// read-only accessors never race with this write.
	// Only stateless S3/file stores reach here, so releasing the superseded one
	// is a no-op; GCS renews its own client in place.
	object.Shutdown(superseded)
	metrics.RecordTenantOperation(s.tenantLabel(), "extent_storage", "refresh_credential", "ok", time.Since(start))
	return s.inner, nil
}

func (s *refreshingStore) recordRefreshFailure(ctx context.Context, step string, start time.Time, err error) {
	tenant := s.tenantLabel()
	logger.Warn(ctx, "extent_credential_refresh_failed",
		zap.String("tenant", tenant),
		zap.String("prefix", s.prefix),
		zap.String("step", step),
		zap.Error(err))
	metrics.RecordTenantOperation(tenant, "extent_storage", "refresh_credential", metrics.ResultForError(err), time.Since(start))
}

func (s *refreshingStore) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	inner, err := s.store(ctx)
	if err != nil {
		return nil, err
	}
	s.gets.Add(1)
	return inner.Get(ctx, key, off, limit, getters...)
}

func (s *refreshingStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	inner, err := s.store(ctx)
	if err != nil {
		return err
	}
	s.puts.Add(1)
	return inner.Put(ctx, key, in, getters...)
}

func (s *refreshingStore) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	inner, err := s.store(ctx)
	if err != nil {
		return err
	}
	return inner.Delete(ctx, key, getters...)
}

func (s *refreshingStore) Head(ctx context.Context, key string) (object.Object, error) {
	inner, err := s.store(ctx)
	if err != nil {
		return nil, err
	}
	return inner.Head(ctx, key)
}
