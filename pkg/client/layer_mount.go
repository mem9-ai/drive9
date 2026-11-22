package client

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type LayerMountBindRequest struct {
	MountID       string `json:"mount_id"`
	OperationID   string `json:"operation_id"`
	ExpectedFence int64  `json:"expected_fence,omitempty"`
	Force         bool   `json:"force,omitempty"`
}

type LayerMountLease struct {
	LayerID          string    `json:"layer_id"`
	MountID          string    `json:"mount_id"`
	OperationID      string    `json:"operation_id"`
	Fence            int64     `json:"fence"`
	MountLeaseUntil  time.Time `json:"mount_lease_until"`
	MountLeaseTTLMS  int64     `json:"mount_lease_ttl_ms"`
}

type LayerMountRenewRequest struct {
	MountID string `json:"mount_id"`
	Fence   int64  `json:"fence"`
}

type LayerMountUnbindRequest struct {
	MountID     string `json:"mount_id,omitempty"`
	Kind        string `json:"kind,omitempty"`
	OperationID string `json:"operation_id,omitempty"`
	Fence       int64  `json:"fence,omitempty"`
	Force       bool   `json:"force,omitempty"`
}

type LayerMountUnbindResult struct {
	LayerID string `json:"layer_id"`
	Fence   int64  `json:"fence"`
	Unbound bool   `json:"unbound"`
}

func (c *Client) ForkLayer(ctx context.Context, ref string, req LayerForkRequest) (*LayerInfo, error) {
	norm, err := viewprotocol.NormalizeLayerRef(ref)
	if err != nil {
		return nil, err
	}
	var out LayerInfo
	if err := c.doLayerJSON(ctx, http.MethodPost, "/v1/layers/"+url.PathEscape(norm)+"/fork", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) MergeLayer(ctx context.Context, ref string) (*LayerMergeResult, error) {
	norm, err := viewprotocol.NormalizeLayerRef(ref)
	if err != nil {
		return nil, err
	}
	var out LayerMergeResult
	if err := c.doLayerJSON(ctx, http.MethodPost, "/v1/layers/"+url.PathEscape(norm)+"/merge", map[string]any{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) DeleteLayer(ctx context.Context, ref string, force bool) (*LayerDeleteResult, error) {
	norm, err := viewprotocol.NormalizeLayerRef(ref)
	if err != nil {
		return nil, err
	}
	endpoint := "/v1/layers/" + url.PathEscape(norm)
	if force {
		endpoint += "?force=true"
	}
	var out LayerDeleteResult
	if err := c.doLayerJSON(ctx, http.MethodDelete, endpoint, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) BindLayerMount(ctx context.Context, layerID string, req LayerMountBindRequest) (*LayerMountLease, error) {
	if err := viewprotocol.ValidateLayerID(layerID); err != nil {
		return nil, err
	}
	var out LayerMountLease
	if err := c.doLayerJSON(ctx, http.MethodPost, "/v1/layers/"+layerID+"/bind-mount", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) RenewLayerMount(ctx context.Context, layerID string, req LayerMountRenewRequest) (*LayerMountLease, error) {
	if err := viewprotocol.ValidateLayerID(layerID); err != nil {
		return nil, err
	}
	var out LayerMountLease
	if err := c.doLayerJSON(ctx, http.MethodPost, "/v1/layers/"+layerID+"/renew-mount", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) UnbindLayerMount(ctx context.Context, layerID string, req LayerMountUnbindRequest) (*LayerMountUnbindResult, error) {
	if err := viewprotocol.ValidateLayerID(layerID); err != nil {
		return nil, err
	}
	var out LayerMountUnbindResult
	if err := c.doLayerJSON(ctx, http.MethodPost, "/v1/layers/"+layerID+"/unbind-mount", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
