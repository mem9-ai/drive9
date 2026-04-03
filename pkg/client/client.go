// Package client provides the dat9 Go SDK.
// Strictly references agfs-sdk/go/client design patterns.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Client is the dat9 HTTP client.
type Client struct {
	baseURL            string
	apiKey             string
	httpClient         *http.Client
	smallFileThreshold int64 // 0 means use DefaultSmallFileThreshold
}

// New creates a new dat9 client.
func New(baseURL, apiKey string) *Client {
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
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > 0 && req.URL.Host != via[0].URL.Host {
					req.Header.Del("Authorization")
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
}

// StatResult represents file metadata from HEAD.
type StatResult struct {
	Size     int64
	IsDir    bool
	Revision int64
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

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	return c.httpClient.Do(req)
}

// Write uploads data to a remote path.
func (c *Client) Write(path string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, c.url(path), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
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
	req, err := http.NewRequest(http.MethodGet, c.url(path), nil)
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
	// Use an explicit value to avoid intermediaries dropping bare "?list".
	req, err := http.NewRequest(http.MethodGet, c.url(path)+"?list=1", nil)
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
func (c *Client) Stat(path string) (*StatResult, error) {
	req, err := http.NewRequest(http.MethodHead, c.url(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("not found: %s", path)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
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
	return s, nil
}

// Delete removes a file or directory.
func (c *Client) Delete(path string) error {
	req, err := http.NewRequest(http.MethodDelete, c.url(path), nil)
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
	req, err := http.NewRequest(http.MethodPost, c.url(newPath)+"?rename", nil)
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
	req, err := http.NewRequest(http.MethodPost, c.url(path)+"?mkdir", nil)
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
		return fmt.Errorf("%s", errResp.Error)
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
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
