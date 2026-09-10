package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
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

// errFileTasksUnexpectedRedirect reports a redirect the client refused to
// follow that is not evidence the server predates ?tasks: a same-host redirect
// (same hostname and effective port, scheme ignored) such as an ingress scheme
// upgrade. A cross-host redirect (a different hostname and/or explicit port —
// the object-store fall-through) reports ErrFileTasksUnsupported instead.
var errFileTasksUnexpectedRedirect = errors.New("file tasks: unexpected redirect")

// IsFileTasksUnexpectedRedirect reports whether err is a same-host redirect
// (same hostname and effective port, scheme ignored) that the client refused to
// follow.
func IsFileTasksUnexpectedRedirect(err error) bool {
	return errors.Is(err, errFileTasksUnexpectedRedirect)
}

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
// X-Dat9-Tasks marker; an unmarked 2xx returns an error matching
// ErrFileTasksUnsupported. A redirect to a different host or effective port
// (the object-store case) also matches ErrFileTasksUnsupported, while a
// same-host redirect (for example a scheme upgrade) matches
// IsFileTasksUnexpectedRedirect.
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
		// A different host or effective port (an object-store redirect) is the
		// strongest signal that the server predates ?tasks; name the host without
		// ever dereferencing it. A redirect that is local to this host (for
		// example an http->https upgrade) is not that signal, so it must not
		// claim the capability is missing.
		host := redirectHost(resp)
		if host != "" && !sameHostAndEffectivePort(c.baseURL, host) {
			return nil, fmt.Errorf("%w (HTTP %d redirect to %s)", ErrFileTasksUnsupported, resp.StatusCode, host)
		}
		if host == "" {
			host = "relative Location"
		}
		return nil, fmt.Errorf("%w (HTTP %d to %s)", errFileTasksUnexpectedRedirect, resp.StatusCode, host)
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

// sameHostAndEffectivePort reports whether host names the same host and
// effective port as baseURL: the hostname compared case-insensitively and the
// effective port (an explicit port, or the scheme default when one side omits
// it). The scheme is deliberately ignored — a same-host http->https upgrade is
// not evidence the server predates ?tasks. A different hostname or a genuinely
// different explicit port stays cross-host, so an object-store redirect still
// maps to ErrFileTasksUnsupported.
func sameHostAndEffectivePort(baseURL, host string) bool {
	base, err := url.Parse(baseURL)
	if err != nil || base.Hostname() == "" {
		return false
	}
	other, err := url.Parse("//" + host)
	if err != nil || other.Hostname() == "" {
		return false
	}
	if !strings.EqualFold(base.Hostname(), other.Hostname()) {
		return false
	}
	return effectivePort(base, base.Scheme) == effectivePort(other, base.Scheme)
}

// effectivePort returns u's explicit port, or the default port for fallback (the
// base URL's scheme, used when the redirect Location is protocol-relative and
// carries no scheme). An unknown scheme with no port yields "".
func effectivePort(u *url.URL, fallback string) string {
	if port := u.Port(); port != "" {
		return port
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = fallback
	}
	switch strings.ToLower(scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}
