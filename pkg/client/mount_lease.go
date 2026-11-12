package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// MountLease is one live FUSE mount registration. The server expires a lease
// after TTL unless the mount refreshes it, which lets the migration controller
// distinguish a crashed mount from an idle but mounted filesystem.
type MountLease struct {
	TenantID                 string `json:"tenant_id"`
	MountID                  string `json:"mount_id"`
	MountPoint               string `json:"mount_point"`
	Host                     string `json:"host,omitempty"`
	PID                      int64  `json:"pid,omitempty"`
	Mode                     string `json:"mode"`
	StartedAt                string `json:"started_at"`
	LastSeenAt               string `json:"last_seen_at"`
	ExpiresAt                string `json:"expires_at"`
	HeartbeatIntervalSeconds int    `json:"heartbeat_interval_seconds"`
}

// MountLeaseRequest registers one mount. TTLSeconds is advisory; the server
// clamps it to its supported range.
type MountLeaseRequest struct {
	MountID    string `json:"mount_id"`
	MountPoint string `json:"mount_point"`
	Host       string `json:"host,omitempty"`
	PID        int64  `json:"pid,omitempty"`
	Mode       string `json:"mode,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

type mountLeaseHeartbeatRequest struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// RegisterMountLease registers or refreshes this mount's lease.
func (c *Client) RegisterMountLease(ctx context.Context, req MountLeaseRequest) (*MountLease, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal mount lease request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/mounts", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("create mount lease request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return c.doMountLease(httpReq, "register")
}

// HeartbeatMountLease extends a live lease. A 404 means the lease expired (or
// the server restarted), and the caller should re-register.
func (c *Client) HeartbeatMountLease(ctx context.Context, mountID string, ttlSeconds int) (*MountLease, error) {
	raw, err := json.Marshal(mountLeaseHeartbeatRequest{TTLSeconds: ttlSeconds})
	if err != nil {
		return nil, fmt.Errorf("marshal mount lease heartbeat: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/mounts/"+url.PathEscape(mountID)+"/heartbeat", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("create mount lease heartbeat request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return c.doMountLease(httpReq, "heartbeat")
}

// DeleteMountLease releases a lease. Deletion is idempotent.
func (c *Client) DeleteMountLease(ctx context.Context, mountID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/mounts/"+url.PathEscape(mountID), nil)
	if err != nil {
		return fmt.Errorf("create mount lease delete request: %w", err)
	}
	resp, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("mount lease delete request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

func (c *Client) doMountLease(httpReq *http.Request, action string) (*MountLease, error) {
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("mount lease %s request: %w", action, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out MountLease
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode mount lease %s response: %w", action, err)
	}
	return &out, nil
}
