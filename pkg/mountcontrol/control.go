// Package mountcontrol defines the JSON protocol used by drive9 mount
// control sockets.
package mountcontrol

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

const DefaultDrainTimeout = 30 * time.Second

type DrainRequest struct {
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

func (r DrainRequest) Timeout() time.Duration {
	if r.TimeoutMS <= 0 {
		return DefaultDrainTimeout
	}
	return time.Duration(r.TimeoutMS) * time.Millisecond
}

type DrainResponse struct {
	OK                    bool         `json:"ok"`
	MountPoint            string       `json:"mount_point,omitempty"`
	StartedAt             time.Time    `json:"started_at"`
	CompletedAt           time.Time    `json:"completed_at"`
	DurationMS            int64        `json:"duration_ms"`
	Error                 string       `json:"error,omitempty"`
	ErrorKind             string       `json:"error_kind,omitempty"`
	FailedPath            string       `json:"failed_path,omitempty"`
	FUSEProtocolMajor     uint32       `json:"fuse_protocol_major,omitempty"`
	FUSEProtocolMinor     uint32       `json:"fuse_protocol_minor,omitempty"`
	NativeSyncFSSupported bool         `json:"native_syncfs_supported"`
	Pending               DrainPending `json:"pending"`
	Phases                []DrainPhase `json:"phases,omitempty"`
}

type DrainPending struct {
	OpenHandles          int   `json:"open_handles"`
	DirtyHandles         int   `json:"dirty_handles"`
	CommitQueuePending   int   `json:"commit_queue_pending"`
	CommitQueueBytes     int64 `json:"commit_queue_bytes"`
	CommitQueueInFlight  int   `json:"commit_queue_in_flight"`
	CommitQueueDelayed   int   `json:"commit_queue_delayed"`
	CommitQueueConflicts int   `json:"commit_queue_conflicts"`
	UploaderQueued       int   `json:"uploader_queued"`
	UploaderInFlight     int   `json:"uploader_in_flight"`
	UploaderCached       int   `json:"uploader_cached"`
	UploaderCachedBytes  int64 `json:"uploader_cached_bytes"`
}

type DrainPhase struct {
	Name       string `json:"name"`
	DurationMS int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

func NewDrainResponse(mountPoint string, startedAt time.Time) DrainResponse {
	return DrainResponse{
		OK:         true,
		MountPoint: mountPoint,
		StartedAt:  startedAt,
	}
}

func (r *DrainResponse) Finish(now time.Time) {
	r.CompletedAt = now
	r.DurationMS = now.Sub(r.StartedAt).Milliseconds()
}

func (r *DrainResponse) Fail(kind, path string, err error) {
	r.OK = false
	r.ErrorKind = kind
	r.FailedPath = path
	if err != nil {
		r.Error = err.Error()
	}
}

func RequestDrain(ctx context.Context, socketPath string, timeout time.Duration) (*DrainResponse, error) {
	if socketPath == "" {
		return nil, fmt.Errorf("missing mount control socket")
	}
	if timeout <= 0 {
		timeout = DefaultDrainTimeout
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial mount control socket %s: %w", socketPath, err)
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	req := DrainRequest{TimeoutMS: timeout.Milliseconds()}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("write drain request: %w", err)
	}
	var resp DrainResponse
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read drain response: %w", err)
	}
	return &resp, nil
}
