package client

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// MaxLayerViewBatchWriteItems caps items per BatchWriteCtx inside a Layer view (v36 §6.5 #3).
const MaxLayerViewBatchWriteItems = 100

// ErrInvalidViewRequest reports a malformed or cross-origin request on a view-bound Client.
var ErrInvalidViewRequest = errors.New("invalid layer view request")

// ViewBinding immutably binds a Client to either main (LayerID == "", Fence == 0)
// or a canonical server-generated Layer ID with optional odd mount fence > 0.
type ViewBinding struct {
	LayerID    string
	Fence      int64
	WriteGuard func() error
}

// ViewBinding returns the Client's immutable Layer view binding.
func (c *Client) ViewBinding() ViewBinding {
	if c == nil {
		return ViewBinding{}
	}
	return c.view
}

// WithView returns a shallow clone of c bound to vb.
// When vb.LayerID == "", vb.Fence must be 0 (main view).
// When vb.LayerID != "", vb.LayerID must satisfy ^layer_[0-9a-hjkmnp-tv-z]{26}$
// and vb.Fence must be 0 (unmounted) or a positive odd integer (bound mount).
func (c *Client) WithView(vb ViewBinding) (*Client, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidViewRequest)
	}
	if vb.LayerID == "" {
		if vb.Fence != 0 {
			return nil, fmt.Errorf("%w: main view cannot carry non-zero fence %d", viewprotocol.ErrInvalidFence, vb.Fence)
		}
		cp := c.cloneShallow()
		cp.view = ViewBinding{}
		cp.baseOrigin = nil
		cp.viewDownload = nil
		return cp, nil
	}
	if err := viewprotocol.ValidateLayerID(vb.LayerID); err != nil {
		return nil, err
	}
	if vb.Fence < 0 || (vb.Fence > 0 && vb.Fence%2 == 0) {
		return nil, fmt.Errorf("%w: %d", viewprotocol.ErrInvalidFence, vb.Fence)
	}
	origin, err := parseCanonicalBaseOrigin(c.baseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidViewRequest, err)
	}
	var plainTransport *http.Transport
	if c.httpClient != nil {
		plainTransport, _ = c.httpClient.Transport.(*http.Transport)
	}
	cp := c.cloneShallow()
	cp.view = vb
	cp.baseOrigin = origin
	cp.viewDownload = viewprotocol.NewViewDownloadHTTPClientWithOrigin(c.httpClient, origin, plainTransport)
	return cp, nil
}

func (c *Client) cloneShallow() *Client {
	cp := &Client{
		baseURL:               c.baseURL,
		apiKey:                c.apiKey,
		actor:                 c.actor,
		httpClient:            c.httpClient,
		smallFileThreshold:    c.smallFileThreshold,
		multipartAbortTimeout: c.multipartAbortTimeout,
	}
	if maxB := c.statusMax.Load(); maxB > 0 {
		cp.statusMax.Store(maxB)
	}
	if inl := c.statusInline.Load(); inl > 0 {
		cp.statusInline.Store(inl)
	}
	if body := c.statusBody.Load(); body != nil {
		cp.statusBody.Store(body)
	}
	return cp
}

func parseCanonicalBaseOrigin(rawBase string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(rawBase), "/"))
	if err != nil || u == nil || !u.IsAbs() || u.Host == "" || u.User != nil {
		return nil, errors.New("invalid client base URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, errors.New("unsupported client base URL scheme")
	}
	return u, nil
}

func sameCanonicalOrigin(base, target *url.URL, hostOverride string) bool {
	if base == nil || target == nil || target.User != nil || !target.IsAbs() {
		return false
	}
	bs, bh, bp, ok1 := splitOrigin(base.Scheme, base.Host)
	ts, th, tp, ok2 := splitOrigin(target.Scheme, target.Host)
	if !ok1 || !ok2 || bs != ts || bh != th || bp != tp {
		return false
	}
	if hostOverride != "" {
		_, oh, op, ok3 := splitOrigin(ts, hostOverride)
		if !ok3 || oh != bh || op != bp {
			return false
		}
	}
	return true
}

func splitOrigin(rawScheme, rawHost string) (scheme, host, port string, ok bool) {
	scheme = strings.ToLower(strings.TrimSpace(rawScheme))
	if scheme != "http" && scheme != "https" {
		return "", "", "", false
	}
	h, p, err := net.SplitHostPort(rawHost)
	if err != nil {
		h = rawHost
		if scheme == "https" {
			p = "443"
		} else {
			p = "80"
		}
	}
	h = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(h)), ".")
	if h == "" || p == "" {
		return "", "", "", false
	}
	if _, err := strconv.Atoi(p); err != nil {
		return "", "", "", false
	}
	return scheme, h, p, true
}

func stripBaseOriginPrefix(baseOrigin *url.URL, escapedPath string) (basePrefix, innerEscaped string, ok bool) {
	if baseOrigin != nil && strings.Trim(baseOrigin.Path, "/") != "" {
		basePrefix = strings.TrimSuffix(path.Clean("/"+strings.TrimLeft(baseOrigin.Path, "/")), "/")
		if basePrefix != "" && basePrefix != "/" {
			if !strings.HasPrefix(escapedPath, basePrefix+"/") {
				return "", "", false
			}
			return basePrefix, strings.TrimPrefix(escapedPath, basePrefix), true
		}
	}
	return "", escapedPath, strings.HasPrefix(escapedPath, "/")
}
