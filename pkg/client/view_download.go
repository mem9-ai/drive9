package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func (c *Client) rewriteRequestForView(req *http.Request) (*http.Request, error) {
	if c == nil || c.view.LayerID == "" {
		return req, nil
	}
	if req == nil || req.URL == nil || c.baseOrigin == nil {
		return nil, ErrInvalidViewRequest
	}
	if !sameCanonicalOrigin(c.baseOrigin, req.URL, req.Host) {
		return nil, fmt.Errorf("%w: request does not match client base origin", ErrInvalidViewRequest)
	}
	basePrefix, innerEscaped, ok := stripBaseOriginPrefix(c.baseOrigin, req.URL.EscapedPath())
	if !ok {
		return nil, fmt.Errorf("%w: request path outside base prefix", ErrInvalidViewRequest)
	}

	switch viewprotocol.ClassifyClientRoute(req.Method, innerEscaped, req.URL.Query()) {
	case viewprotocol.RoutePassThrough:
		out := req.Clone(req.Context())
		out.Header.Del(viewprotocol.FenceHeader)
		return out, nil

	case viewprotocol.RouteRewriteView:
		if viewRouteRequiresWriteGuard(req.Method, innerEscaped) && c.view.WriteGuard != nil {
			if err := c.view.WriteGuard(); err != nil {
				return nil, err
			}
		}
		if req.Header.Get("X-Dat9-Description") != "" {
			return nil, viewprotocol.ErrUnsupportedInLayerView
		}
		viewEscaped, err := viewprotocol.FormatViewPath(c.view.LayerID, innerEscaped)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidViewRequest, err)
		}
		fullEscaped := basePrefix + viewEscaped
		fullDecoded, err := url.PathUnescape(fullEscaped)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidViewRequest, err)
		}
		out := req.Clone(req.Context())
		uCopy := *req.URL
		uCopy.Path = fullDecoded
		uCopy.RawPath = fullEscaped
		out.URL = &uCopy
		if c.view.Fence > 0 {
			out.Header.Set(viewprotocol.FenceHeader, strconv.FormatInt(c.view.Fence, 10))
		} else {
			out.Header.Del(viewprotocol.FenceHeader)
		}
		return out, nil

	case viewprotocol.RouteRejectInvalid:
		return nil, fmt.Errorf("%w: %v", ErrInvalidViewRequest, viewprotocol.ErrInvalidViewRoute)

	default:
		return nil, viewprotocol.ErrUnsupportedInLayerView
	}
}

func viewRouteRequiresWriteGuard(method, innerEscaped string) bool {
	if method != http.MethodGet && method != http.MethodHead {
		return true
	}
	return method == http.MethodGet &&
		strings.HasPrefix(innerEscaped, "/v1/uploads/") &&
		strings.HasSuffix(innerEscaped, "/resume")
}

func (c *Client) doObjectLocation(ctx context.Context, sourceURL *url.URL, rawLocation string, hdr http.Header) (*http.Response, error) {
	if c.view.LayerID != "" {
		if c.viewDownload == nil {
			return nil, viewprotocol.ErrInvalidViewRedirect
		}
		return c.viewDownload.DoRedirectLocation(ctx, sourceURL, rawLocation, hdr)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawLocation, nil)
	if err != nil {
		return nil, err
	}
	for k, vals := range hdr {
		req.Header[k] = append([]string(nil), vals...)
	}
	return c.doPresigned(req)
}

func (c *Client) resolveObjectRedirectLocation(sourceURL *url.URL, rawLocation string) (string, error) {
	if c.view.LayerID != "" {
		if c.viewDownload == nil {
			return "", viewprotocol.ErrInvalidViewRedirect
		}
		resolved, err := c.viewDownload.ResolveRedirectLocation(sourceURL, rawLocation)
		if err != nil {
			return "", err
		}
		return resolved.String(), nil
	}
	return rawLocation, nil
}

func (c *Client) doObjectTargetURL(ctx context.Context, rawObjectURL string, hdr http.Header) (*http.Response, error) {
	if c.view.LayerID != "" {
		if c.viewDownload == nil || c.baseOrigin == nil {
			return nil, viewprotocol.ErrInvalidViewRedirect
		}
		parsed, err := url.Parse(rawObjectURL)
		if err != nil {
			return nil, viewprotocol.ErrInvalidViewRedirect
		}
		if !parsed.IsAbs() {
			return c.viewDownload.DoRedirectLocation(ctx, c.baseOrigin, rawObjectURL, hdr)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
		if err != nil {
			return nil, err
		}
		for k, vals := range hdr {
			req.Header[k] = append([]string(nil), vals...)
		}
		return c.viewDownload.DoRedirectTarget(req)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawObjectURL, nil)
	if err != nil {
		return nil, err
	}
	for k, vals := range hdr {
		req.Header[k] = append([]string(nil), vals...)
	}
	return c.doPresigned(req)
}
