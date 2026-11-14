package viewprotocol

import (
	"net/http"
	"net/url"
	"strings"
)

const RoutePrefix = "/v1/views/"

// ParseRoute verifies the explicit Layer route and header binding together,
// then returns an owned request for the ordinary versioned API. Dispatch must
// pass Binding into the filesystem operation; it must never drop that context
// and call main. Unsupported view operations must fail before ordinary dispatch.
func ParseRoute(r *http.Request) (*http.Request, Binding, error) {
	b, err := Parse(r.Header)
	if err != nil || b.LayerID == "" {
		return nil, Binding{}, ErrBinding
	}
	escaped := r.URL.EscapedPath()
	prefix := RoutePrefix + b.LayerID
	if !strings.HasPrefix(escaped, prefix+"/v1/") && !strings.HasPrefix(escaped, prefix+"/v2/") {
		return nil, Binding{}, ErrBinding
	}
	endpoint := strings.TrimPrefix(escaped, prefix)
	if strings.HasPrefix(endpoint, RoutePrefix) {
		return nil, Binding{}, ErrBinding
	}
	decoded, err := url.PathUnescape(endpoint)
	if err != nil {
		return nil, Binding{}, ErrBinding
	}
	out := r.Clone(r.Context())
	out.URL.Path, out.URL.RawPath = decoded, endpoint
	// RequestURI is a server-only field; keep it consistent for auditing and
	// dispatch that use it instead of URL. RawQuery is preserved verbatim.
	out.RequestURI = out.URL.RequestURI()
	return out, b, nil
}
