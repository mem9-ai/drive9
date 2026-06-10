package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type FSLayerCreateRequest struct {
	LayerID        string            `json:"layer_id,omitempty"`
	BaseRootPath   string            `json:"base_root_path"`
	Name           string            `json:"name,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	DurabilityMode string            `json:"durability_mode,omitempty"`
	ActorID        string            `json:"actor_id,omitempty"`
}

type FSLayer struct {
	LayerID        string            `json:"layer_id"`
	BaseRootPath   string            `json:"base_root_path"`
	Name           string            `json:"name"`
	Tags           map[string]string `json:"tags,omitempty"`
	State          string            `json:"state"`
	DurabilityMode string            `json:"durability_mode"`
	ActorID        string            `json:"actor_id"`
	DurableSeq     int64             `json:"durable_seq"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	SealedAt       *time.Time        `json:"sealed_at,omitempty"`
}

type FSLayerEntry struct {
	LayerID                string    `json:"layer_id"`
	Path                   string    `json:"path"`
	ParentPath             string    `json:"parent_path"`
	Name                   string    `json:"name"`
	Op                     string    `json:"op"`
	Kind                   string    `json:"kind"`
	BaseInodeID            string    `json:"base_inode_id"`
	BaseRevision           int64     `json:"base_revision"`
	StorageType            string    `json:"storage_type"`
	StorageRef             string    `json:"storage_ref"`
	StorageRefHash         string    `json:"storage_ref_hash"`
	StorageEncryptionMode  string    `json:"storage_encryption_mode"`
	StorageEncryptionKeyID string    `json:"storage_encryption_key_id"`
	ChecksumSHA256         string    `json:"checksum_sha256"`
	SizeBytes              int64     `json:"size_bytes"`
	Mode                   uint32    `json:"mode"`
	Content                []byte    `json:"content,omitempty"`
	ContentText            string    `json:"content_text,omitempty"`
	EntrySeq               int64     `json:"entry_seq"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

type FSLayerEntryRequest struct {
	Path                   string `json:"path"`
	Op                     string `json:"op,omitempty"`
	Kind                   string `json:"kind,omitempty"`
	BaseInodeID            string `json:"base_inode_id,omitempty"`
	BaseRevision           int64  `json:"base_revision,omitempty"`
	StorageType            string `json:"storage_type,omitempty"`
	StorageRef             string `json:"storage_ref,omitempty"`
	StorageRefHash         string `json:"storage_ref_hash,omitempty"`
	StorageEncryptionMode  string `json:"storage_encryption_mode,omitempty"`
	StorageEncryptionKeyID string `json:"storage_encryption_key_id,omitempty"`
	Content                []byte `json:"content,omitempty"`
	ContentType            string `json:"content_type,omitempty"`
	ContentText            string `json:"content_text,omitempty"`
	ChecksumSHA256         string `json:"checksum_sha256,omitempty"`
	SizeBytes              int64  `json:"size_bytes,omitempty"`
	Mode                   uint32 `json:"mode,omitempty"`
}

type FSLayerCommit struct {
	Status    string                  `json:"status"`
	LayerID   string                  `json:"layer_id"`
	Applied   int                     `json:"applied,omitempty"`
	Conflicts []FSLayerCommitConflict `json:"conflicts,omitempty"`
}

type FSLayerCommitConflict struct {
	Path         string `json:"path"`
	Reason       string `json:"reason"`
	BaseRevision int64  `json:"base_revision,omitempty"`
	WantRevision int64  `json:"want_revision,omitempty"`
}

type FSLayerCheckpointRequest struct {
	CheckpointID string `json:"checkpoint_id,omitempty"`
	Label        string `json:"label,omitempty"`
}

type FSLayerCheckpoint struct {
	CheckpointID string    `json:"checkpoint_id"`
	LayerID      string    `json:"layer_id"`
	DurableSeq   int64     `json:"durable_seq"`
	Label        string    `json:"label"`
	CreatedAt    time.Time `json:"created_at"`
}

type FSLayerEvent struct {
	EventID   string    `json:"event_id"`
	LayerID   string    `json:"layer_id"`
	Seq       int64     `json:"seq"`
	ActorID   string    `json:"actor_id"`
	Op        string    `json:"op"`
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"created_at"`
}

func (c *Client) CreateFSLayer(ctx context.Context, req FSLayerCreateRequest) (*FSLayer, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/layers", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out FSLayer
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer: %w", err)
	}
	return &out, nil
}

func (c *Client) ListFSLayers(ctx context.Context) ([]FSLayer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/layers", nil)
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
	var out struct {
		Layers []FSLayer `json:"layers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layers: %w", err)
	}
	return out.Layers, nil
}

func (c *Client) GetFSLayer(ctx context.Context, layerID string) (*FSLayer, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/layers/"+url.PathEscape(layerID), nil)
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
	var out FSLayer
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer: %w", err)
	}
	return &out, nil
}

func (c *Client) DiffFSLayer(ctx context.Context, layerID string) ([]FSLayerEntry, error) {
	return c.diffFSLayer(ctx, layerID, nil)
}

func (c *Client) DiffFSLayerAtSeq(ctx context.Context, layerID string, maxSeq int64) ([]FSLayerEntry, error) {
	if maxSeq < 0 {
		return nil, fmt.Errorf("maxSeq must be non-negative")
	}
	return c.diffFSLayer(ctx, layerID, &maxSeq)
}

func (c *Client) diffFSLayer(ctx context.Context, layerID string, maxSeq *int64) ([]FSLayerEntry, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/diff"
	if maxSeq != nil {
		u += "?max_seq=" + url.QueryEscape(fmt.Sprintf("%d", *maxSeq))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
	var out struct {
		Entries []FSLayerEntry `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer diff: %w", err)
	}
	return out.Entries, nil
}

func (c *Client) UpsertFSLayerEntry(ctx context.Context, layerID string, req FSLayerEntryRequest) (*FSLayerEntry, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/entries"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out FSLayerEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer entry: %w", err)
	}
	return &out, nil
}

func (c *Client) UploadFSLayerFile(ctx context.Context, layerID, path string, body io.Reader, size int64, baseRevision int64, mode uint32, hasMode bool) (*FSLayerEntry, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path must not be empty")
	}
	q := url.Values{}
	q.Set("path", path)
	q.Set("size", fmt.Sprintf("%d", size))
	if baseRevision > 0 {
		q.Set("base_revision", fmt.Sprintf("%d", baseRevision))
	}
	if hasMode {
		q.Set("mode", fmt.Sprintf("%o", mode&0o777))
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/objects?" + q.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return nil, err
	}
	httpReq.ContentLength = size
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out FSLayerEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer object entry: %w", err)
	}
	return &out, nil
}

func (c *Client) ReadFSLayerFile(ctx context.Context, layerID, path string, maxSeq *int64) ([]byte, error) {
	rc, err := c.ReadFSLayerFileStream(ctx, layerID, path, maxSeq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

func (c *Client) ReadFSLayerFileStream(ctx context.Context, layerID, path string, maxSeq *int64) (io.ReadCloser, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path must not be empty")
	}
	q := url.Values{}
	q.Set("path", path)
	if maxSeq != nil {
		q.Set("max_seq", fmt.Sprintf("%d", *maxSeq))
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/objects?" + q.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		return nil, readError(resp)
	}
	return resp.Body, nil
}

func (c *Client) GetFSLayerEntry(ctx context.Context, layerID, path string) (*FSLayerEntry, error) {
	return c.getFSLayerEntry(ctx, layerID, path, nil)
}

func (c *Client) GetFSLayerEntryAtSeq(ctx context.Context, layerID, path string, maxSeq int64) (*FSLayerEntry, error) {
	if maxSeq < 0 {
		return nil, fmt.Errorf("maxSeq must be non-negative")
	}
	return c.getFSLayerEntry(ctx, layerID, path, &maxSeq)
}

func (c *Client) getFSLayerEntry(ctx context.Context, layerID, path string, maxSeq *int64) (*FSLayerEntry, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path must not be empty")
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/entries?path=" + url.QueryEscape(path)
	if maxSeq != nil {
		u += "&max_seq=" + url.QueryEscape(fmt.Sprintf("%d", *maxSeq))
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out FSLayerEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer entry: %w", err)
	}
	return &out, nil
}

func (c *Client) CheckpointFSLayer(ctx context.Context, layerID string, req FSLayerCheckpointRequest) (*FSLayerCheckpoint, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/checkpoints"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out FSLayerCheckpoint
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer checkpoint: %w", err)
	}
	return &out, nil
}

func (c *Client) GetFSLayerCheckpoint(ctx context.Context, checkpointID string) (*FSLayerCheckpoint, error) {
	if strings.TrimSpace(checkpointID) == "" {
		return nil, fmt.Errorf("checkpointID must not be empty")
	}
	u := c.baseURL + "/v1/layer-checkpoints/" + url.PathEscape(checkpointID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out FSLayerCheckpoint
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer checkpoint: %w", err)
	}
	return &out, nil
}

func (c *Client) ListFSLayerEvents(ctx context.Context, layerID string, since int64) ([]FSLayerEvent, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	q := url.Values{}
	if since > 0 {
		q.Set("since", fmt.Sprintf("%d", since))
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/events"
	if encoded := q.Encode(); encoded != "" {
		u += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
	var out struct {
		Events []FSLayerEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer events: %w", err)
	}
	return out.Events, nil
}

func (c *Client) RollbackFSLayer(ctx context.Context, layerID string) error {
	return c.postFSLayerAction(ctx, layerID, "rollback")
}

func (c *Client) CommitFSLayer(ctx context.Context, layerID string) (*FSLayerCommit, error) {
	if strings.TrimSpace(layerID) == "" {
		return nil, fmt.Errorf("layerID must not be empty")
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/commit"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader([]byte("{}")))
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
		if resp.StatusCode == http.StatusConflict {
			body, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, &StatusError{
					StatusCode: resp.StatusCode,
					Message:    fmt.Sprintf("read fs layer commit conflict body: %v", readErr),
				}
			}
			var out FSLayerCommit
			if err := json.Unmarshal(body, &out); err == nil && (out.Status != "" || len(out.Conflicts) > 0) {
				return &out, &StatusError{StatusCode: resp.StatusCode, Message: "fs layer commit conflict"}
			}
			resp.Body = io.NopCloser(bytes.NewReader(body))
		}
		return nil, readError(resp)
	}
	var out FSLayerCommit
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode fs layer commit: %w", err)
	}
	return &out, nil
}

func (c *Client) postFSLayerAction(ctx context.Context, layerID, action string) error {
	if strings.TrimSpace(layerID) == "" {
		return fmt.Errorf("layerID must not be empty")
	}
	u := c.baseURL + "/v1/layers/" + url.PathEscape(layerID) + "/" + action
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader([]byte("{}")))
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
