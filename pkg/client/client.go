// Package client provides the dat9 Go SDK.
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
	"time"

	"github.com/mem9-ai/dat9/pkg/tagutil"
)

// Client is the dat9 HTTP client.
type Client struct {
	baseURL            string
	apiKey             string
	actor              string // X-Dat9-Actor header value (per-mount ID)
	httpClient         *http.Client
	smallFileThreshold int64 // 0 means use DefaultSmallFileThreshold
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

// New creates a new dat9 client authenticated with an owner API key.
//
// Owner credentials reach the tenant management plane (CreateVaultSecret,
// IssueVaultGrant, audit, etc.) as well as the data plane. Use NewWithToken
// for delegated capability tokens; the two kinds are distinct in caller
// capability even though both are carried as `Authorization: Bearer` on the
// wire (server-side middleware disambiguates — see pkg/server/auth.go).
func New(baseURL, apiKey string) *Client {
	return newClient(baseURL, apiKey)
}

// NewWithToken creates a new dat9 client authenticated with a delegated
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
	// then tune connection pooling for concurrent multipart uploads to S3.
	// Default MaxIdleConnsPerHost=2 forces new TLS handshakes for every
	// part beyond 2 in-flight, adding ~50-100ms per part. Setting it to
	// 16 lets the connection pool cover typical upload parallelism.
	var transport *http.Transport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 16

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
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	IsDir bool   `json:"isDir"`
	Mtime int64  `json:"mtime,omitempty"` // Unix seconds, 0 means unknown
}

// StatResult represents file metadata from HEAD.
type StatResult struct {
	Size     int64
	IsDir    bool
	Revision int64
	Mtime    time.Time
}

// StatMetadataResult represents enriched metadata from GET /v1/fs/{path}?stat=1.
type StatMetadataResult struct {
	Size         int64             `json:"size"`
	IsDir        bool              `json:"isdir"`
	Revision     int64             `json:"revision"`
	Mtime        int64             `json:"mtime,omitempty"` // Unix seconds, 0 means unknown
	ContentType  string            `json:"content_type"`
	SemanticText string            `json:"semantic_text"`
	Tags         map[string]string `json:"tags"`
}

func (c *Client) url(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.baseURL + "/v1/fs" + path
}

func (c *Client) RawPost(endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, c.baseURL+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
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
//
// expectedRevision semantics:
// - negative: unconditional write
// - zero: path must not already exist
// - positive: file must exist at exactly that revision
func (c *Client) WriteCtxConditionalWithTags(ctx context.Context, path string, data []byte, expectedRevision int64, tags map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(path), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if expectedRevision >= 0 {
		req.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(expectedRevision, 10))
	}
	if err := setTagHeaders(req, tags); err != nil {
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
	if mt := resp.Header.Get("X-Dat9-Mtime"); mt != "" {
		if sec, err := strconv.ParseInt(mt, 10, 64); err == nil {
			s.Mtime = time.Unix(sec, 0)
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
	var mtime int64
	if !statOut.Mtime.IsZero() {
		mtime = statOut.Mtime.Unix()
	}
	return &StatMetadataResult{
		Size:         statOut.Size,
		IsDir:        statOut.IsDir,
		Revision:     statOut.Revision,
		Mtime:        mtime,
		ContentType:  "",
		SemanticText: "",
		Tags:         map[string]string{},
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
	return strings.Contains(err.Error(), "decode stat metadata:")
}

// Delete removes a file or directory.
func (c *Client) Delete(path string) error {
	return c.DeleteCtx(context.Background(), path)
}

// DeleteCtx removes a file or directory with context support.
func (c *Client) DeleteCtx(ctx context.Context, path string) error {
	return c.deleteCtx(ctx, path, false)
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
	return c.deleteCtx(ctx, path, true)
}

func (c *Client) deleteCtx(ctx context.Context, path string, recursive bool) error {
	requestURL := c.url(path)
	if recursive {
		// Use an explicit value to avoid intermediaries dropping bare "?recursive".
		requestURL += "?recursive=1"
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
	req, err := http.NewRequest(http.MethodPost, c.url(dstPath)+"?copy", nil)
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
	return c.MkdirCtx(context.Background(), path)
}

// MkdirCtx creates a directory with context support.
func (c *Client) MkdirCtx(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?mkdir", nil)
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

func readError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	var errResp struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		return &StatusError{StatusCode: resp.StatusCode, Message: errResp.Error}
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
	u := c.url(pathPrefix) + "?grep=" + url.QueryEscape(query)
	if limit > 0 {
		u += "&limit=" + strconv.Itoa(limit)
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

func setTagHeaders(req *http.Request, tags map[string]string) error {
	if len(tags) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		if err := tagutil.ValidateEntry(k, tags[k]); err != nil {
			return err
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		req.Header.Add("X-Dat9-Tag", k+"="+tags[k])
	}
	return nil
}
