// Package client provides the drive9 Go SDK.
// Strictly references agfs-sdk/go/client design patterns.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/tagutil"
)

// Client is the drive9 HTTP client.
type Client struct {
	baseURL            string
	apiKey             string
	actor              string // X-Dat9-Actor header value (per-mount ID)
	httpClient         *http.Client
	smallFileThreshold int64 // 0 means use DefaultSmallFileThreshold

	// statusFetchMu serializes /v1/status fetches across concurrent callers
	// so a transient warm failure can be retried but two callers never
	// double-fetch. Set statusFetched only on a successful HTTP 200, so a
	// timeout/5xx during warm never permanently caches "unknown" — the
	// next caller will retry.
	statusFetchMu sync.Mutex
	statusFetched atomic.Bool
	// statusMax / statusInline are atomic so warmup goroutines and hot-path
	// readers (commit queue, FUSE write decisions) coordinate without a
	// mutex. Race detector previously caught this when FUSE's async warm
	// raced with concurrent uploads.
	statusMax    atomic.Int64 // tenant max_upload_bytes from /v1/status; 0 if unavailable
	statusInline atomic.Int64 // server inline_threshold from /v1/status; 0 if unavailable
}

// tenantStatusResponse mirrors the server's TenantStatusResponse JSON shape.
// Forward-compatible: unknown fields decode-and-ignore cleanly.
type tenantStatusResponse struct {
	Status          string `json:"status"`
	MaxUploadBytes  int64  `json:"max_upload_bytes"`
	InlineThreshold int64  `json:"inline_threshold,omitempty"`
}

// ErrConflict reports an HTTP 409 write conflict from the server.
var ErrConflict = errors.New("conflict")

// StatusError preserves the HTTP status code for API errors.
type StatusError struct {
	StatusCode int
	Message    string
}

func (e *StatusError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("HTTP %d", e.StatusCode)
}

func (e *StatusError) Is(target error) bool {
	return target == ErrConflict && e.StatusCode == http.StatusConflict
}

// IsNotFound reports whether err is an HTTP 404 StatusError.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.StatusCode == http.StatusNotFound
}

// New creates a new drive9 client authenticated with an owner API key.
//
// Owner credentials reach the tenant management plane (CreateVaultSecret,
// IssueVaultGrant, audit, etc.) as well as the data plane. Use NewWithToken
// for delegated capability tokens; the two kinds are distinct in caller
// capability even though both are carried as `Authorization: Bearer` on the
// wire (server-side middleware disambiguates — see pkg/server/auth.go).
func New(baseURL, apiKey string) *Client {
	return newClient(baseURL, apiKey)
}

// NewWithToken creates a new drive9 client authenticated with a delegated
// capability token (JWT). Delegated callers can only reach read-path vault
// endpoints and the FUSE data-plane routes that capabilityAuthMiddleware
// resolves; admin-plane endpoints will 401 server-side. Choosing the
// constructor at the call site makes the principal kind explicit per Plan 9
// orthogonality — a "forget to check kind" bug class doesn't get a
// construction path.
func NewWithToken(baseURL, token string) *Client {
	return newClient(baseURL, token)
}

// newClient is the shared internal constructor for New and NewWithToken.
// Both credential kinds travel as `Authorization: Bearer` on the wire, so
// they share the same httpClient/transport wiring. Kind differentiation
// lives at the call site (constructor name) — there is no internal
// discriminator field. If a per-call-kind bug surfaces later, the minimal
// fix is a discriminator; meanwhile Invariant #7 keeps all authorization
// decisions server-side.
func newClient(baseURL, credential string) *Client {
	// Clone DefaultTransport to preserve Proxy, HTTP/2, dialer, and TLS defaults,
	// then tune connection pooling for concurrent multipart uploads, prefetch,
	// and repeated range reads against both drive9 and presigned S3 URLs.
	// Default MaxIdleConnsPerHost=2 forces new TLS handshakes for every
	// request beyond 2 in-flight, adding ~50-100ms per connection in WAN
	// deployments. Keep headroom above uploadMaxConcurrency so reads and
	// metadata calls do not evict upload connections.
	var transport *http.Transport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 64

	// Custom DialContext: fall back to public DNS (8.8.8.8, 1.1.1.1) when the
	// system resolver fails. Fixes DNS on Termux and minimal containers that
	// lack /etc/resolv.conf.
	baseDialer := &net.Dialer{Timeout: 10 * time.Second}
	fallbackResolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 5 * time.Second}
			// Try Google DNS first, then Cloudflare.
			conn, err := d.DialContext(ctx, "udp", "8.8.8.8:53")
			if err != nil {
				conn, err = d.DialContext(ctx, "udp", "1.1.1.1:53")
			}
			return conn, err
		},
	}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		// Try system resolver first.
		conn, err := baseDialer.DialContext(ctx, network, addr)
		if err == nil {
			return conn, nil
		}
		// Only fall back to public DNS on actual DNS resolution errors.
		// Non-DNS errors (connection refused, timeout, etc.) should not
		// leak internal hostnames to public resolvers.
		var dnsErr *net.DNSError
		if !errors.As(err, &dnsErr) {
			return nil, err
		}
		host, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, err // return original error
		}
		ips, resolveErr := fallbackResolver.LookupHost(ctx, host)
		if resolveErr != nil || len(ips) == 0 {
			return nil, err // return original dial error
		}
		return baseDialer.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  credential,
		httpClient: &http.Client{
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > 0 && req.URL.Host != via[0].URL.Host {
					req.Header.Del("Authorization")
					req.Header.Del("X-Dat9-Actor")
				}
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
	}
}

// FileInfo represents a file entry from a directory listing.
type FileInfo struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	IsDir      bool   `json:"isDir"`
	Mtime      int64  `json:"mtime,omitempty"`      // Unix seconds, 0 means unknown
	Revision   int64  `json:"revision,omitempty"`    // file revision from server; 0 means unknown (old server)
	Mode       uint32 `json:"mode,omitempty"`
	HasMode    bool   `json:"hasMode"`
	ResourceID string `json:"resource_id,omitempty"`
	Nlink      uint32 `json:"nlink,omitempty"`
}

// StatResult represents file metadata from HEAD.
type StatResult struct {
	Size       int64
	IsDir      bool
	Revision   int64
	Mtime      time.Time
	Mode       uint32
	HasMode    bool // true when the server returned a mode header (including 0)
	ResourceID string
	Nlink      uint32
}

// MaxBatchStatPaths is the maximum number of paths accepted by BatchStatCtx.
const MaxBatchStatPaths = 256

// MaxBatchReadSmallPaths is the maximum number of paths accepted by BatchReadSmallCtx.
const MaxBatchReadSmallPaths = 128

// BatchStatResult is one per-path result from BatchStatCtx.
//
// Status is the HTTP-like per-path status. A missing path returns Status 404
// in its own result instead of failing the whole batch.
type BatchStatResult struct {
	Path       string `json:"path"`
	Status     int    `json:"status"`
	Error      string `json:"error,omitempty"`
	Size       int64  `json:"size,omitempty"`
	IsDir      bool   `json:"isDir"`
	Revision   int64  `json:"revision,omitempty"`
	Mtime      int64  `json:"mtime,omitempty"` // Unix seconds, 0 means unknown
	Mode       uint32 `json:"mode,omitempty"`
	HasMode    bool   `json:"hasMode"`
	ResourceID string `json:"resource_id,omitempty"`
	Nlink      uint32 `json:"nlink,omitempty"`
}

// OK reports whether the per-path batch stat result is successful.
func (r BatchStatResult) OK() bool {
	return r.Status >= 200 && r.Status < 300 && r.Error == ""
}

// BatchReadSmallResult is one per-path result from BatchReadSmallCtx.
//
// Data is JSON base64-encoded on the wire. Missing, invalid, directory, and
// too-large paths are reported as per-path errors.
type BatchReadSmallResult struct {
	Path     string `json:"path"`
	Status   int    `json:"status"`
	Error    string `json:"error,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Revision int64  `json:"revision,omitempty"`
	Mtime    int64  `json:"mtime,omitempty"` // Unix seconds, 0 means unknown
}

// OK reports whether the per-path batch read-small result is successful.
func (r BatchReadSmallResult) OK() bool {
	return r.Status >= 200 && r.Status < 300 && r.Error == ""
}

// StatMetadataResult represents enriched metadata from GET /v1/fs/{path}?stat=1.
type StatMetadataResult struct {
	Size         int64             `json:"size"`
	IsDir        bool              `json:"isdir"`
	ResourceID   string            `json:"resource_id,omitempty"`
	Nlink        uint32            `json:"nlink,omitempty"`
	Revision     int64             `json:"revision"`
	Mtime        *int64            `json:"mtime,omitempty"` // Unix seconds when known
	ContentType  string            `json:"content_type"`
	SemanticText string            `json:"semantic_text"`
	Tags         map[string]string `json:"tags"`
	Degraded     bool              `json:"degraded,omitempty"`
}

var errStatMetadataCompatFallback = errors.New("stat metadata fallback to legacy HEAD")

func (c *Client) url(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.baseURL + "/v1/fs" + path
}

func (c *Client) RawGet(endpoint string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+endpoint, nil)
	if err != nil {
		return nil, err
	}
	return c.do(req)
}

func (c *Client) RawPost(endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, c.baseURL+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func (c *Client) RawDelete(endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, c.baseURL+endpoint, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req)
}

// SetActor sets the X-Dat9-Actor header value for all subsequent requests.
// Used by FUSE mounts to identify the per-mount actor for SSE self-filtering.
func (c *Client) SetActor(actor string) {
	c.actor = actor
}

// BaseURL returns the server base URL.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// APIKey returns the API key.
func (c *Client) APIKey() string {
	return c.apiKey
}

// MaxUploadBytes returns the tenant's effective single-upload size cap as
// reported by GET /v1/status. The result is cached for the lifetime of the
// client (one fetch per process). Returns 0 if the server is older and does
// not include the field, or if the lookup fails — callers should treat 0 as
// "unknown" and fall back to a conservative local default.
func (c *Client) MaxUploadBytes(ctx context.Context) int64 {
	c.ensureTenantStatus(ctx)
	return c.statusMax.Load()
}

// Warm proactively populates the /v1/status cache so subsequent hot-path
// reads (CachedSmallFileThreshold, uploadThreshold) see the server-
// advertised values instead of falling back to compiled-in defaults.
// Failures are silent — the cache simply stays at zero and callers fall
// back. Idempotent; safe to call once at CLI/FUSE startup.
func (c *Client) Warm(ctx context.Context) {
	c.ensureTenantStatus(ctx)
}

// SetSmallFileThresholdForTests pins the client's small-file cutoff
// without consulting the server. Hot paths (uploadThreshold,
// CachedSmallFileThreshold) treat this override as authoritative and
// skip the network fetch entirely.
//
// Production code must NOT call this — the server is the threshold
// authority and clients are supposed to negotiate via /v1/status. The
// helper exists for unit tests that build fake HTTP servers without a
// /v1/status route and would otherwise force every upload through V2
// multipart (the safe-default when no server value is observable).
func (c *Client) SetSmallFileThresholdForTests(threshold int64) {
	c.smallFileThreshold = threshold
}

// SmallFileThreshold returns the server's DB-inline vs S3 storage cutoff as
// reported by GET /v1/status. The result is cached for the lifetime of the
// client. Returns 0 when the server omits the field (older builds) or when
// the lookup fails; callers should fall back to DefaultSmallFileThreshold.
//
// An explicit per-Client override set via testing or configuration takes
// precedence: when c.smallFileThreshold > 0 it short-circuits the network
// fetch and is returned as-is. This keeps unit tests deterministic and lets
// operators pin a threshold when needed.
func (c *Client) SmallFileThreshold(ctx context.Context) int64 {
	if c.smallFileThreshold > 0 {
		return c.smallFileThreshold
	}
	c.ensureTenantStatus(ctx)
	return c.statusInline.Load()
}

// CachedSmallFileThreshold returns the server-advertised threshold without
// triggering a network fetch. Returns 0 when no value has been negotiated
// yet. Use this on hot paths (commit queue, FUSE write decisions) to avoid
// surprising side-effect requests; defer the initial fetch to a single
// SmallFileThreshold call from a startup or warmup site.
func (c *Client) CachedSmallFileThreshold() int64 {
	if c.smallFileThreshold > 0 {
		return c.smallFileThreshold
	}
	return c.statusInline.Load()
}

// ensureTenantStatus fetches and caches /v1/status fields once per Client.
// Failures cache as zero so callers fall back to local defaults instead of
// retrying every request.
func (c *Client) ensureTenantStatus(ctx context.Context) {
	if c.statusFetched.Load() {
		return
	}
	c.statusFetchMu.Lock()
	defer c.statusFetchMu.Unlock()
	if c.statusFetched.Load() {
		return
	}
	body, ok := c.fetchTenantStatus(ctx)
	if !ok {
		// Transient failure (timeout, 5xx, network). Don't mark fetched —
		// a future Warm/MaxUploadBytes/SmallFileThreshold call will retry.
		// Hot-path reads continue to fall back to compiled defaults until
		// then.
		return
	}
	if body.MaxUploadBytes > 0 {
		c.statusMax.Store(body.MaxUploadBytes)
	}
	if body.InlineThreshold > 0 {
		c.statusInline.Store(body.InlineThreshold)
	}
	// Mark fetched even when the server omits inline_threshold (older
	// build): that's an authoritative "no value", retrying won't help and
	// hot paths should stop attempting status fetches every call.
	c.statusFetched.Store(true)
}

func (c *Client) fetchTenantStatus(ctx context.Context) (tenantStatusResponse, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/status", nil)
	if err != nil {
		return tenantStatusResponse{}, false
	}
	resp, err := c.do(req)
	if err != nil {
		return tenantStatusResponse{}, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return tenantStatusResponse{}, false
	}
	var body tenantStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return tenantStatusResponse{}, false
	}
	return body, true
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if c.actor != "" {
		req.Header.Set("X-Dat9-Actor", c.actor)
	}
	return c.httpClient.Do(req)
}

// Write uploads data to a remote path.
func (c *Client) Write(path string, data []byte) error {
	return c.WriteCtx(context.Background(), path, data)
}

// WriteCtx uploads data to a remote path with context support.
func (c *Client) WriteCtx(ctx context.Context, path string, data []byte) error {
	return c.WriteCtxConditional(ctx, path, data, -1)
}

// WriteCtxConditional uploads data to a remote path and applies the write only
// when expectedRevision matches the current server revision.
// expectedRevision semantics:
// - negative: unconditional write
// - zero: path must not already exist
// - positive: file must exist at exactly that revision
func (c *Client) WriteCtxConditional(ctx context.Context, path string, data []byte, expectedRevision int64) error {
	return c.WriteCtxConditionalWithTags(ctx, path, data, expectedRevision, nil)
}

// WriteCtxConditionalWithTags uploads data to a remote path with optional
// compare-and-set semantics and optional file tags.
// This method issues a single PUT request and is therefore intended for direct
// write paths. When tags are provided for a large file, the server rejects the
// request because large-file uploads must send tags in the multipart complete
// request instead of X-Dat9-Tag headers. Callers that need tag-aware uploads
// for large files should use WriteStreamWithSummaryAndTags or
// ResumeUploadWithSummaryAndTags.
//
// expectedRevision semantics:
// - negative: unconditional write
// - zero: path must not already exist
// - positive: file must exist at exactly that revision
func (c *Client) WriteCtxConditionalWithTags(ctx context.Context, path string, data []byte, expectedRevision int64, tags map[string]string) error {
	return c.writeCtxConditionalWithTagsAndDescription(ctx, path, data, expectedRevision, tags, "")
}

// WriteCtxConditionalWithDescription is like WriteCtxConditional but also sends
// a description header for the file.
func (c *Client) WriteCtxConditionalWithDescription(ctx context.Context, path string, data []byte, expectedRevision int64, description string) error {
	return c.writeCtxConditionalWithTagsAndDescription(ctx, path, data, expectedRevision, nil, description)
}

func (c *Client) writeCtxConditionalWithTagsAndDescription(ctx context.Context, path string, data []byte, expectedRevision int64, tags map[string]string, description string) error {
	_, err := c.writeCtxConditionalFull(ctx, path, data, expectedRevision, tags, description)
	return err
}

// WriteCtxConditionalWithRevision is like WriteCtxConditional but also returns
// the committed revision from the server response.
func (c *Client) WriteCtxConditionalWithRevision(ctx context.Context, path string, data []byte, expectedRevision int64) (int64, error) {
	return c.writeCtxConditionalFull(ctx, path, data, expectedRevision, nil, "")
}

// CreateFile creates an empty file.
func (c *Client) CreateFile(path string) (int64, error) {
	return c.CreateFileCtx(context.Background(), path)
}

// CreateFileCtx creates an empty file with context support and returns the
// committed file revision when the server reports it.
func (c *Client) CreateFileCtx(ctx context.Context, path string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?create=1", nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return 0, readError(resp)
	}
	var result struct {
		Revision int64 `json:"revision"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, fmt.Errorf("decode create file response: %w", err)
	}
	return result.Revision, nil
}

// Symlink creates a symbolic link at linkPath pointing to target.
func (c *Client) Symlink(target, linkPath string) error {
	return c.SymlinkCtx(context.Background(), target, linkPath)
}

// SymlinkCtx creates a symbolic link with context support.
func (c *Client) SymlinkCtx(ctx context.Context, target, linkPath string) error {
	body, err := json.Marshal(map[string]string{"target": target})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(linkPath)+"?symlink=1", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// Hardlink creates a hard link at dstPath pointing at srcPath's file entity.
func (c *Client) Hardlink(srcPath, dstPath string) error {
	return c.HardlinkCtx(context.Background(), srcPath, dstPath)
}

// HardlinkCtx creates a hard link with context support.
func (c *Client) HardlinkCtx(ctx context.Context, srcPath, dstPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(dstPath)+"?hardlink=1", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Dat9-Hardlink-Source", srcPath)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

func (c *Client) writeCtxConditionalFull(ctx context.Context, path string, data []byte, expectedRevision int64, tags map[string]string, description string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(path), bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if expectedRevision >= 0 {
		req.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(expectedRevision, 10))
	}
	if err := setTagHeaders(req, tags); err != nil {
		return 0, err
	}
	if description != "" {
		req.Header.Set("X-Dat9-Description", description)
	}
	resp, err := c.do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return 0, readError(resp)
	}
	// Parse committed revision from response body.
	var result struct {
		Revision int64 `json:"revision"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If response doesn't contain revision (old server), return 0.
		return 0, nil
	}
	return result.Revision, nil
}

// Read downloads a file's content.
func (c *Client) Read(path string) ([]byte, error) {
	return c.ReadCtx(context.Background(), path)
}

// ReadCtx downloads a file's content with context support.
func (c *Client) ReadCtx(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	return io.ReadAll(resp.Body)
}

// ReadAt downloads at most length bytes starting at offset.
func (c *Client) ReadAt(path string, offset, length int64) ([]byte, error) {
	return c.ReadAtCtx(context.Background(), path, offset, length)
}

// ReadAtCtx downloads at most length bytes starting at offset with context support.
func (c *Client) ReadAtCtx(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	rc, err := c.ReadStreamRange(ctx, path, offset, length)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// List returns the entries in a directory.
func (c *Client) List(path string) ([]FileInfo, error) {
	return c.ListCtx(context.Background(), path)
}

// ListCtx returns the entries in a directory with context support.
func (c *Client) ListCtx(ctx context.Context, path string) ([]FileInfo, error) {
	// Use an explicit value to avoid intermediaries dropping bare "?list".
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path)+"?list=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result struct {
		Entries []FileInfo `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return result.Entries, nil
}

// BatchStatCtx returns lightweight metadata for up to MaxBatchStatPaths paths.
//
// Transport/request errors fail the method. Per-path stat errors are returned
// inside the corresponding BatchStatResult so one missing path does not fail
// the whole batch.
func (c *Client) BatchStatCtx(ctx context.Context, paths []string) ([]BatchStatResult, error) {
	if len(paths) == 0 {
		return []BatchStatResult{}, nil
	}
	if len(paths) > MaxBatchStatPaths {
		return nil, fmt.Errorf("batch stat: %d paths exceeds limit of %d", len(paths), MaxBatchStatPaths)
	}
	body, err := json.Marshal(struct {
		Paths []string `json:"paths"`
	}{Paths: paths})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/fs:batch-stat", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Results []BatchStatResult `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(out.Results) != len(paths) {
		return nil, fmt.Errorf("batch stat: got %d results for %d paths", len(out.Results), len(paths))
	}
	return out.Results, nil
}

// BatchReadSmallCtx reads up to MaxBatchReadSmallPaths small inline files.
//
// Transport/request errors fail the method. Per-path read errors are returned
// inside the corresponding BatchReadSmallResult so one missing or too-large
// path does not fail the whole batch.
func (c *Client) BatchReadSmallCtx(ctx context.Context, paths []string, maxBytes int64) ([]BatchReadSmallResult, error) {
	if len(paths) == 0 {
		return []BatchReadSmallResult{}, nil
	}
	if len(paths) > MaxBatchReadSmallPaths {
		return nil, fmt.Errorf("batch read-small: %d paths exceeds limit of %d", len(paths), MaxBatchReadSmallPaths)
	}
	body, err := json.Marshal(struct {
		Paths    []string `json:"paths"`
		MaxBytes int64    `json:"max_bytes,omitempty"`
	}{Paths: paths, MaxBytes: maxBytes})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/fs:batch-read-small", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Results []BatchReadSmallResult `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(out.Results) != len(paths) {
		return nil, fmt.Errorf("batch read-small: got %d results for %d paths", len(out.Results), len(paths))
	}
	for i := range out.Results {
		if out.Results[i].Path != paths[i] {
			return nil, fmt.Errorf("batch read-small: result[%d] path = %q, want %q", i, out.Results[i].Path, paths[i])
		}
	}
	return out.Results, nil
}

// Stat returns metadata for a path.
//
// Stat is the lightweight metadata interface based on HEAD. It is intended for
// callers that only need compact attributes (size/isdir/revision/mtime) and do
// not need enriched fields such as content_type, semantic_text, and tags.
func (c *Client) Stat(path string) (*StatResult, error) {
	return c.StatCtx(context.Background(), path)
}

// StatCtx returns metadata for a path with context support.
//
// StatCtx is the context-aware form of the lightweight HEAD-based Stat
// interface. Use StatMetadataCompatCtx when enriched metadata is required.
func (c *Client) StatCtx(ctx context.Context, path string) (*StatResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.url(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	s := &StatResult{
		IsDir: resp.Header.Get("X-Dat9-IsDir") == "true",
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		s.Size, _ = strconv.ParseInt(cl, 10, 64)
	}
	if rev := resp.Header.Get("X-Dat9-Revision"); rev != "" {
		s.Revision, _ = strconv.ParseInt(rev, 10, 64)
	}
	if mode := resp.Header.Get("X-Dat9-Mode"); mode != "" {
		s.HasMode = true
		if m, err := strconv.ParseUint(mode, 10, 32); err == nil {
			s.Mode = uint32(m)
		}
	}
	if mt := resp.Header.Get("X-Dat9-Mtime"); mt != "" {
		if sec, err := strconv.ParseInt(mt, 10, 64); err == nil {
			s.Mtime = time.Unix(sec, 0)
		}
	}
	s.ResourceID = resp.Header.Get("X-Dat9-Resource-ID")
	if nlink := resp.Header.Get("X-Dat9-Nlink"); nlink != "" {
		if n, err := strconv.ParseUint(nlink, 10, 32); err == nil {
			s.Nlink = uint32(n)
		}
	}
	return s, nil
}

// StatMetadata returns enriched metadata for a path, including mtime,
// content_type, semantic_text, and tags.
func (c *Client) StatMetadata(path string) (*StatMetadataResult, error) {
	return c.StatMetadataCtx(context.Background(), path)
}

// StatMetadataCtx returns enriched metadata for a path with context support.
func (c *Client) StatMetadataCtx(ctx context.Context, path string) (*StatMetadataResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path)+"?stat=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	if !strings.HasPrefix(contentType, "application/json") {
		return nil, fmt.Errorf("%w: unexpected Content-Type %q", errStatMetadataCompatFallback, resp.Header.Get("Content-Type"))
	}
	// Intentionally do not require X-Dat9-Mtime here yet. The stat-specific
	// marker check will become useful once the server rollout is complete, and
	// for now we only gate the compat fallback on obvious non-JSON responses.
	var out StatMetadataResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode stat metadata: %w", err)
	}
	if out.Tags == nil {
		out.Tags = map[string]string{}
	}
	return &out, nil
}

// StatMetadataCompat returns enriched metadata for a path and transparently
// falls back to legacy HEAD stat when the server does not support ?stat=1.
func (c *Client) StatMetadataCompat(path string) (*StatMetadataResult, error) {
	return c.StatMetadataCompatCtx(context.Background(), path)
}

// StatMetadataCompatCtx returns enriched metadata for a path with context
// support, and falls back to HEAD stat when ?stat=1 is unsupported by older
// servers.
func (c *Client) StatMetadataCompatCtx(ctx context.Context, path string) (*StatMetadataResult, error) {
	out, err := c.StatMetadataCtx(ctx, path)
	if err == nil {
		return out, nil
	}
	if !shouldFallbackStatMetadata(err) {
		return nil, err
	}
	statOut, statErr := c.StatCtx(ctx, path)
	if statErr != nil {
		return nil, statErr
	}
	var mtime *int64
	if !statOut.Mtime.IsZero() {
		unix := statOut.Mtime.Unix()
		mtime = &unix
	}
	return &StatMetadataResult{
		Size:         statOut.Size,
		IsDir:        statOut.IsDir,
		ResourceID:   statOut.ResourceID,
		Nlink:        statOut.Nlink,
		Revision:     statOut.Revision,
		Mtime:        mtime,
		ContentType:  "",
		SemanticText: "",
		Tags:         map[string]string{},
		Degraded:     true,
	}, nil
}

func shouldFallbackStatMetadata(err error) bool {
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		switch statusErr.StatusCode {
		case http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusNotImplemented:
			return true
		}
	}
	return errors.Is(err, errStatMetadataCompatFallback)
}

// Delete removes a file or directory.
func (c *Client) Delete(path string) error {
	return c.DeleteCtx(context.Background(), path)
}

// DeleteCtx removes a file or directory with context support.
func (c *Client) DeleteCtx(ctx context.Context, path string) error {
	return c.deleteCtx(ctx, path, false, "")
}

// DeleteFileCtx removes a file with a server-side type hint.
func (c *Client) DeleteFileCtx(ctx context.Context, path string) error {
	return c.deleteCtx(ctx, path, false, "file")
}

// DeleteDirCtx removes an empty directory with a server-side type hint.
func (c *Client) DeleteDirCtx(ctx context.Context, path string) error {
	return c.deleteCtx(ctx, path, false, "dir")
}

// RemoveAll removes a file or directory tree recursively.
// If path names a regular file, RemoveAll behaves like Delete.
// If path does not exist, RemoveAll returns the same 404 *StatusError as Delete
// instead of succeeding like os.RemoveAll.
func (c *Client) RemoveAll(path string) error {
	return c.RemoveAllCtx(context.Background(), path)
}

// RemoveAllCtx removes a file or directory tree recursively with context support.
// It forwards to deleteCtx with recursive=true, so regular files use Delete
// semantics and missing paths return the same 404 *StatusError as RemoveAll.
func (c *Client) RemoveAllCtx(ctx context.Context, path string) error {
	return c.deleteCtx(ctx, path, true, "")
}

func (c *Client) deleteCtx(ctx context.Context, path string, recursive bool, kind string) error {
	requestURL := c.url(path)
	if recursive {
		// Use an explicit value to avoid intermediaries dropping bare "?recursive".
		requestURL += "?recursive=1"
	} else if kind != "" {
		requestURL += "?kind=" + url.QueryEscape(kind)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// Copy performs a server-side zero-copy (same file_id, new path).
func (c *Client) Copy(srcPath, dstPath string) error {
	return c.CopyCtx(context.Background(), srcPath, dstPath)
}

// CopyCtx performs a server-side zero-copy with context support so
// callers can cancel mid-flight (e.g., Ctrl+C during a recursive
// tree copy). The non-Ctx Copy delegates here so both paths share
// the same request shape.
func (c *Client) CopyCtx(ctx context.Context, srcPath, dstPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(dstPath)+"?copy", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Dat9-Copy-Source", srcPath)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// Rename moves/renames a file or directory (metadata-only).
func (c *Client) Rename(oldPath, newPath string) error {
	return c.RenameCtx(context.Background(), oldPath, newPath)
}

// RenameCtx moves/renames a file or directory with context support.
func (c *Client) RenameCtx(ctx context.Context, oldPath, newPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(newPath)+"?rename", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Dat9-Rename-Source", oldPath)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// Mkdir creates a directory.
func (c *Client) Mkdir(path string) error {
	return c.MkdirCtx(context.Background(), path, 0o755)
}

// MkdirCtx creates a directory with context support.
func (c *Client) MkdirCtx(ctx context.Context, path string, mode uint32) error {
	urlStr := c.url(path) + "?mkdir"
	if mode != 0o755 {
		urlStr += "&mode=" + strconv.FormatUint(uint64(mode), 10)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// Chmod updates the permission bits of a file.
func (c *Client) Chmod(path string, mode uint32) error {
	return c.ChmodCtx(context.Background(), path, mode)
}

// ChmodCtx updates the permission bits of a file with context support.
func (c *Client) ChmodCtx(ctx context.Context, path string, mode uint32) error {
	body, err := json.Marshal(map[string]uint32{"mode": mode})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?chmod", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

func readError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	var errResp struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		return &StatusError{StatusCode: resp.StatusCode, Message: errResp.Error}
	}
	var nestedErr struct {
		Error struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &nestedErr) == nil && nestedErr.Error.Message != "" {
		return &StatusError{StatusCode: resp.StatusCode, Message: nestedErr.Error.Message}
	}
	return &StatusError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body))}
}

func (c *Client) SQL(query string) ([]map[string]interface{}, error) {
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/v1/sql", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var rows []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return rows, nil
}

type SearchResult struct {
	Path      string   `json:"path"`
	Name      string   `json:"name"`
	SizeBytes int64    `json:"size_bytes"`
	Score     *float64 `json:"score,omitempty"`
}

func (c *Client) Grep(query, pathPrefix string, limit int) ([]SearchResult, error) {
	return c.GrepWithLayer(query, pathPrefix, limit, "")
}

func (c *Client) GrepWithLayer(query, pathPrefix string, limit int, layerRef string) ([]SearchResult, error) {
	u := c.url(pathPrefix) + "?grep=" + url.QueryEscape(query)
	if limit > 0 {
		u += "&limit=" + strconv.Itoa(limit)
	}
	if strings.TrimSpace(layerRef) != "" {
		u += "&layer=" + url.QueryEscape(strings.TrimSpace(layerRef))
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var results []SearchResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}

func (c *Client) Find(pathPrefix string, params url.Values) ([]SearchResult, error) {
	params.Set("find", "")
	u := c.url(pathPrefix) + "?" + params.Encode()
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var results []SearchResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}

// validateTags applies the same client-side validation across direct PUT,
// multipart upload, and resume flows. Multipart tags are still sent only in
// the final complete request, but validating up front avoids uploading parts
// before discovering an invalid tag and prevents json.Marshal from silently
// replacing invalid UTF-8 in complete payloads.
func validateTags(tags map[string]string) error {
	if len(tags) == 0 {
		return nil
	}
	for k := range tags {
		if err := tagutil.ValidateEntry(k, tags[k]); err != nil {
			return err
		}
	}
	return nil
}

func setTagHeaders(req *http.Request, tags map[string]string) error {
	if len(tags) == 0 {
		return nil
	}
	if err := validateTags(tags); err != nil {
		return err
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		req.Header.Add("X-Dat9-Tag", k+"="+tags[k])
	}
	return nil
}
