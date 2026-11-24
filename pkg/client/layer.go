package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// LayerCreateRequest creates a new CoW Layer off main (server assigns canonical layer_id).
type LayerCreateRequest struct {
	Name         string            `json:"name,omitempty"`
	BaseRootPath string            `json:"base_root_path,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	ActorID      string            `json:"actor_id,omitempty"`
	TTLSeconds   int64             `json:"ttl_seconds,omitempty"`
}

// LayerForkRequest forks a child Layer from an existing active Layer.
type LayerForkRequest struct {
	Name       string            `json:"name,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	ActorID    string            `json:"actor_id,omitempty"`
	TTLSeconds int64             `json:"ttl_seconds,omitempty"`
}

// LayerInfo is the control-plane representation of a Layer (v36 §6.5 #2).
type LayerInfo struct {
	LayerID          string            `json:"layer_id"`
	Name             string            `json:"name"`
	BaseRootPath     string            `json:"base_root_path"`
	ParentLayerID    string            `json:"parent_layer_id"`
	ActorID          string            `json:"actor_id"`
	State            string            `json:"state"`
	SnapGen          int64             `json:"snap_gen"`
	Fence            int64             `json:"fence"`
	ExclusiveMountID string            `json:"exclusive_mount_id"`
	MountLeaseUntil  *time.Time        `json:"mount_lease_until"`
	MountLeaseTTLMS  int64             `json:"mount_lease_ttl_ms"`
	MountExpired     bool              `json:"mount_expired"`
	Tags             map[string]string `json:"tags,omitempty"`
	ExpiresAt        *time.Time        `json:"expires_at"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

type LayerListOptions struct {
	Limit int
	After string
}

type LayerListPage struct {
	Layers    []LayerInfo `json:"layers"`
	NextAfter string      `json:"next_after,omitempty"`
}

type LayerMergeResult struct {
	LayerID      string `json:"layer_id"`
	Status       string `json:"status"`
	AppliedPaths int    `json:"applied_paths"`
}

type LayerDeleteResult struct {
	LayerID string `json:"layer_id"`
	Status  string `json:"status"`
}

func (c *Client) CreateLayer(ctx context.Context, req LayerCreateRequest) (*LayerInfo, error) {
	var out LayerInfo
	if err := c.doLayerJSON(ctx, http.MethodPost, "/v1/layers", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListLayers(ctx context.Context, opts LayerListOptions) (*LayerListPage, error) {
	q := url.Values{}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.After != "" {
		q.Set("after", opts.After)
	}
	endpoint := "/v1/layers"
	if enc := q.Encode(); enc != "" {
		endpoint += "?" + enc
	}
	var out LayerListPage
	if err := c.doLayerJSON(ctx, http.MethodGet, endpoint, nil, &out); err != nil {
		return nil, err
	}
	if out.Layers == nil {
		out.Layers = []LayerInfo{}
	}
	return &out, nil
}

func (c *Client) ListAllLayers(ctx context.Context) ([]LayerInfo, error) {
	var all []LayerInfo
	after := ""
	for {
		page, err := c.ListLayers(ctx, LayerListOptions{Limit: 200, After: after})
		if err != nil {
			return nil, err
		}
		all = append(all, page.Layers...)
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
	}
	if all == nil {
		all = []LayerInfo{}
	}
	return all, nil
}

func (c *Client) GetLayer(ctx context.Context, ref string) (*LayerInfo, error) {
	norm, err := viewprotocol.NormalizeLayerRef(ref)
	if err != nil {
		return nil, err
	}
	var out LayerInfo
	if err := c.doLayerJSON(ctx, http.MethodGet, "/v1/layers/"+url.PathEscape(norm), nil, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.LayerID) == "" {
		return nil, fmt.Errorf("resolve layer %q: server returned an empty layer ID", ref)
	}
	return &out, nil
}

func (c *Client) doLayerJSON(ctx context.Context, method, endpoint string, reqBody, dst any) error {
	var bodyReader *bytes.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	} else {
		bodyReader = bytes.NewReader(nil)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, bodyReader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	if dst == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}
