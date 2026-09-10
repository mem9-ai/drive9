package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// fileTasksMarkerHeader is set by servers that implement
// GET /v1/fs/{path}?tasks. Servers that predate the endpoint silently fall
// through to a plain read (which may redirect to object storage or serve an
// application/json file), so the client refuses to decode a response that does
// not carry this marker.
const fileTasksMarkerHeader = "X-Dat9-Tasks"

// ErrFileTasksUnsupported reports that the server does not implement
// GET /v1/fs/{path}?tasks (the `fs tasks` command). Callers can detect it with
// errors.Is.
var ErrFileTasksUnsupported = errors.New("file tasks: server does not support fs tasks (?tasks)")

// FileTask is one durable extract/embed task for a file's current revision.
// LastError is empty unless the task failed, and is bounded by the server.
type FileTask struct {
	TaskType  string `json:"task_type"`
	Status    string `json:"status"`
	LastError string `json:"last_error,omitempty"`
}

// FileTasksResult is the response of GET /v1/fs/{path}?tasks.
type FileTasksResult struct {
	Path  string     `json:"path"`
	Tasks []FileTask `json:"tasks"`
}

// FileTasks returns the extract/embed task status for a file's current
// revision, including the failure reason when a task failed.
func (c *Client) FileTasks(path string) (*FileTasksResult, error) {
	return c.FileTasksCtx(context.Background(), path)
}

// FileTasksCtx returns the extract/embed task status for a file's current
// revision with context support.
//
// The request deliberately does not follow redirects: on a server without
// ?tasks, the GET falls through to a plain read that may redirect to object
// storage. A response is only decoded as a task list when the server sets the
// X-Dat9-Tasks marker; a redirect, a file body, or any other unmarked 2xx
// returns an error matching ErrFileTasksUnsupported.
func (c *Client) FileTasksCtx(ctx context.Context, path string) (*FileTasksResult, error) {
	req, err := c.newFSRequest(ctx, http.MethodGet, path, "?tasks=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		// A cross-host Location (an object-store redirect) is the strongest
		// signal that the server predates ?tasks; name the host without ever
		// dereferencing it.
		if host := redirectHost(resp); host != "" {
			return nil, fmt.Errorf("%w (HTTP %d redirect to %s)", ErrFileTasksUnsupported, resp.StatusCode, host)
		}
		return nil, fmt.Errorf("%w (HTTP %d redirect)", ErrFileTasksUnsupported, resp.StatusCode)
	case resp.StatusCode >= 400:
		return nil, readError(resp)
	}
	if resp.Header.Get(fileTasksMarkerHeader) != "1" {
		return nil, fmt.Errorf("%w (HTTP %d)", ErrFileTasksUnsupported, resp.StatusCode)
	}
	var out FileTasksResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode file tasks: %w", err)
	}
	if out.Tasks == nil {
		out.Tasks = []FileTask{}
	}
	return &out, nil
}

// redirectHost returns the host of a response's Location header, or "" when the
// header is missing, relative, or malformed. It never follows the redirect.
func redirectHost(resp *http.Response) string {
	u, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		return ""
	}
	return u.Host
}
