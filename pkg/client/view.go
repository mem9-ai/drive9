package client

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// ViewBinding identifies the server-issued work generation for ordinary file
// APIs. Zero means main. It does not create or launch work, nor grant permission.
type ViewBinding = viewprotocol.Binding

// WithView returns an independent client fixed to one view. Existing requests,
// extent state and handles on c retain their original binding. The caller must
// separately drain and invalidate a mount before replacing its client.
func (c *Client) WithView(view ViewBinding) (*Client, error) {
	if c == nil {
		return nil, viewprotocol.ErrBinding
	}
	if err := view.Validate(); err != nil {
		return nil, err
	}
	hc := *c.httpClient

	return &Client{baseURL: c.baseURL, apiKey: c.apiKey, actor: c.actor, httpClient: &hc,
		smallFileThreshold: c.smallFileThreshold, multipartAbortTimeout: c.multipartAbortTimeout, view: view}, nil
}

// boundRequest uses a separate route namespace, so an old server which ignores
// new headers cannot accidentally execute a Layer write against main. Servers
// resolve this prefix to the same file APIs with a validated ViewContext.
func (c *Client) boundRequest(req *http.Request) (*http.Request, error) {
	if c.view.LayerID == "" {
		return req, nil
	}
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	if req.URL.Scheme != base.Scheme || req.URL.Host != base.Host {
		return nil, fmt.Errorf("view request targets another server: %w", viewprotocol.ErrBinding)
	}
	prefix := strings.TrimRight(base.EscapedPath(), "/")
	escaped := req.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix+"/v1/") && !strings.HasPrefix(escaped, prefix+"/v2/") {
		return nil, fmt.Errorf("view request is not a filesystem API: %w", viewprotocol.ErrBinding)
	}
	escaped = prefix + viewprotocol.RoutePrefix + c.view.LayerID + strings.TrimPrefix(escaped, prefix)
	decoded, err := url.PathUnescape(escaped)
	if err != nil {
		return nil, err
	}
	out := req.Clone(req.Context())
	out.URL.Path, out.URL.RawPath = decoded, escaped
	if err := c.view.Apply(out.Header); err != nil {
		return nil, err
	}
	return out, nil
}
