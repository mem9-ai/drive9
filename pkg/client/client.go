// Package client provides the dat9 Go SDK.
// Strictly references agfs-sdk/go/client design patterns.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: http.DefaultClient,
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	return io.ReadAll(resp.Body)
}

// List returns the entries in a directory.
func (c *Client) List(path string) ([]FileInfo, error) {
	req, err := http.NewRequest(http.MethodGet, c.url(path)+"?list", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
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
	resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
