package viewprotocol

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
)

// MaxViewDownloadRedirects caps redirect hops when streaming or downloading
// objects inside a Layer view.
const MaxViewDownloadRedirects = 10

type viewRedirectHopKey struct{}

// IsViewRedirectHop reports whether ctx marks a redirected hop initiated inside
// NewViewDownloadCheckRedirect.
func IsViewRedirectHop(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(viewRedirectHopKey{}).(bool)
	return v
}

// isAllowedRedirectDownloadHeader defines the minimal safe request-header
// allowlist forwarded on redirected Layer view object downloads (presigned
// S3/GCS/Azure/object GETs). Using an allowlist rather than a denylist blocks
// Authorization, Cookie, X-API-Key, X-FS-Layer-Fence, X-TiDB-Cloud-Authorization,
// X-TiDBCloud-Public-Key, X-TiDBCloud-Private-Key, and any future internal or
// custom credential headers by construction while preserving byte-range and
// conditional read headers.
func isAllowedRedirectDownloadHeader(canonicalKey string) bool {
	switch http.CanonicalHeaderKey(canonicalKey) {
	case "Range",
		"If-Range",
		"If-Match",
		"If-None-Match",
		"If-Modified-Since",
		"If-Unmodified-Since",
		"Accept",
		"Accept-Encoding",
		"User-Agent":
		return true
	default:
		return false
	}
}

// StripSensitiveRedirectHeaders prunes a redirected download request's headers
// down to the minimal safe object-read allowlist (Range, If-*, Accept,
// Accept-Encoding, User-Agent), removing all credentials, cookies, internal
// auth headers (including X-TiDB-Cloud-Authorization and X-TiDBCloud-* keys),
// and Layer fence headers.
func StripSensitiveRedirectHeaders(req *http.Request) {
	if req == nil || req.Header == nil {
		return
	}
	for k := range req.Header {
		if !isAllowedRedirectDownloadHeader(k) {
			delete(req.Header, k)
		}
	}
}

func sanitizeRedirectHTTPTransport(plain *http.Transport) *http.Transport {
	var cloned *http.Transport
	if plain != nil {
		cloned = plain.Clone()
	} else if def, ok := http.DefaultTransport.(*http.Transport); ok && def != nil {
		cloned = def.Clone()
	} else {
		cloned = &http.Transport{}
	}
	if cloned.TLSClientConfig != nil {
		cloned.TLSClientConfig.Certificates = nil
		cloned.TLSClientConfig.GetClientCertificate = nil
		cloned.TLSClientConfig.ClientSessionCache = nil
		cloned.TLSClientConfig.ServerName = ""
		cloned.TLSClientConfig.InsecureSkipVerify = false
		cloned.TLSClientConfig.VerifyPeerCertificate = nil
		cloned.TLSClientConfig.VerifyConnection = nil
	}
	cloned.DialTLSContext = nil
	cloned.DialTLS = nil //nolint:staticcheck // clear deprecated DialTLS hook so legacy callers cannot bypass redirect TLS isolation
	cloned.ProxyConnectHeader = nil
	cloned.GetProxyConnectHeader = nil
	if cloned.Proxy != nil {
		baseProxy := cloned.Proxy
		cloned.Proxy = func(req *http.Request) (*url.URL, error) {
			u, err := baseProxy(req)
			if err != nil || u == nil {
				return u, err
			}
			if u.User != nil {
				return nil, fmt.Errorf("%w: proxy URL contains user credentials on view redirect transport", ErrInvalidViewRedirect)
			}
			return u, nil
		}
	}
	return cloned
}

// ValidateViewDownloadOriginRequest verifies that an initial (Hop-0) request
// sent through a dedicated Layer view download client uses GET or HEAD,
// strictly matches baseOrigin's (scheme, host, port) and optional deployment
// path prefix, and carries no conflicting req.Host override before any
// authenticated origin RoundTripper can run.
func ValidateViewDownloadOriginRequest(baseOrigin *url.URL, req *http.Request) error {
	if baseOrigin == nil || req == nil || req.URL == nil {
		return ErrInvalidViewRedirect
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet && method != http.MethodHead {
		return fmt.Errorf("%w: unsupported view download method %q", ErrInvalidViewRedirect, method)
	}
	baseScheme, baseHost, basePort, err := canonicalHTTPOrigin(baseOrigin)
	if err != nil {
		return err
	}
	reqScheme, reqHost, reqPort, err := canonicalHTTPOrigin(req.URL)
	if err != nil {
		return err
	}
	if reqScheme != baseScheme || reqHost != baseHost || reqPort != basePort {
		return fmt.Errorf("%w: initial view download request %q does not match base origin", ErrInvalidViewRedirect, SanitizeURLForError(req.URL))
	}
	if req.Host != "" {
		_, overrideHost, overridePort, hErr := canonicalHTTPOrigin(&url.URL{Scheme: reqScheme, Host: req.Host})
		if hErr != nil || overrideHost != baseHost || overridePort != basePort {
			return fmt.Errorf("%w: request Host override does not match base origin", ErrInvalidViewRedirect)
		}
	}
	if strings.Trim(baseOrigin.Path, "/") != "" {
		basePrefix := strings.TrimSuffix(path.Clean("/"+strings.TrimLeft(baseOrigin.Path, "/")), "/")
		if basePrefix != "" && basePrefix != "/" {
			esc := req.URL.EscapedPath()
			lowerEsc := strings.ToLower(esc)
			if strings.Contains(lowerEsc, "%2f") || strings.Contains(lowerEsc, "%5c") {
				return fmt.Errorf("%w: encoded separator in %q", ErrInvalidViewRedirect, SanitizeURLForError(req.URL))
			}
			decoded, uErr := url.PathUnescape(esc)
			if uErr != nil || strings.ContainsRune(decoded, '\\') || strings.ContainsRune(decoded, 0) {
				return fmt.Errorf("%w: invalid path in %q", ErrInvalidViewRedirect, SanitizeURLForError(req.URL))
			}
			cleaned := path.Clean("/" + strings.TrimLeft(decoded, "/"))
			if cleaned != basePrefix && !strings.HasPrefix(cleaned, basePrefix+"/") {
				return fmt.Errorf("%w: initial view download request %q outside base path prefix %q", ErrInvalidViewRedirect, SanitizeURLForError(req.URL), basePrefix)
			}
		}
	}
	return nil
}

type viewDownloadRoundTripper struct {
	baseOrigin *url.URL
	origin     http.RoundTripper
	redirect   *http.Transport
}

func (rt viewDownloadRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, ErrInvalidViewRedirect
	}
	isRedirect := IsViewRedirectHop(req.Context())
	var (
		resp *http.Response
		err  error
	)
	if isRedirect {
		// 1. Client.send attaches cookies from Client.Jar AFTER CheckRedirect
		//    returns; pruning to the safe allowlist here removes any Jar cookie
		//    and all custom credential headers right at the wire boundary.
		// 2. Redirected hops NEVER delegate to rt.origin (which could be an
		//    auth-injecting RoundTripper decorator or mTLS transport); they
		//    execute strictly on the credential-free sanitized *http.Transport.
		out := req.Clone(req.Context())
		out.Host = ""
		StripSensitiveRedirectHeaders(out)
		plain := rt.redirect
		if plain == nil {
			plain = sanitizeRedirectHTTPTransport(nil)
		}
		resp, err = plain.RoundTrip(out)
	} else {
		// Hop 0 MUST match baseOrigin before touching rt.origin so passing an
		// off-origin presigned URL directly to this client can never invoke an
		// auth-injecting or mTLS origin transport.
		if err := ValidateViewDownloadOriginRequest(rt.baseOrigin, req); err != nil {
			return nil, err
		}
		origin := rt.origin
		if origin == nil {
			if rt.redirect != nil {
				origin = rt.redirect
			} else {
				origin = sanitizeRedirectHTTPTransport(nil)
			}
		}
		resp, err = origin.RoundTrip(req)
	}
	if err != nil || resp == nil {
		return resp, err
	}
	if isRedirect || (resp.StatusCode >= 300 && resp.StatusCode < 400) {
		if resp.Header != nil {
			resp.Header.Del("Set-Cookie")
			resp.Header.Del("Set-Cookie2")
		}
	}
	switch resp.StatusCode {
	case http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect:
		if resp.Header != nil {
			if loc := resp.Header.Get("Location"); loc != "" {
				if strings.ContainsAny(loc, "\r\n\x00") || req.URL == nil {
					if resp.Body != nil {
						_ = resp.Body.Close()
					}
					return nil, fmt.Errorf("%w: malformed redirect Location header", ErrInvalidViewRedirect)
				}
				if _, pErr := req.URL.Parse(loc); pErr != nil {
					if resp.Body != nil {
						_ = resp.Body.Close()
					}
					return nil, fmt.Errorf("%w: malformed redirect Location header", ErrInvalidViewRedirect)
				}
			}
		}
	}
	return resp, nil
}

// NewViewDownloadTransport returns a RoundTripper whose Hop-0 requests are
// confined to baseOrigin (using plainTransport when non-nil) and whose
// redirected object-store hops run on a cloned *http.Transport scrubbed of
// client TLS certificates, TLS session caches, custom TLS dialers, and proxy
// credentials.
func NewViewDownloadTransport(baseOrigin *url.URL, plainTransport *http.Transport) http.RoundTripper {
	var originRT http.RoundTripper
	if plainTransport != nil {
		originRT = plainTransport.Clone()
	}
	sanitizedRedirect := sanitizeRedirectHTTPTransport(plainTransport)
	if originRT == nil {
		originRT = sanitizedRedirect
	}
	return viewDownloadRoundTripper{
		baseOrigin: baseOrigin,
		origin:     originRT,
		redirect:   sanitizedRedirect,
	}
}

// NewViewDownloadSplitTransport confines Hop-0 requests to baseOrigin via
// originTransport, while routing all redirected object-download hops strictly
// through a credential-scrubbed concrete *http.Transport (bypassing any auth
// wrapper, mTLS client certificate, or proxy auth on originTransport) after
// allowlist-pruning request headers.
func NewViewDownloadSplitTransport(baseOrigin *url.URL, originTransport http.RoundTripper, plainRedirectTransport *http.Transport) http.RoundTripper {
	if existing, ok := originTransport.(viewDownloadRoundTripper); ok {
		originTransport = existing.origin
		if plainRedirectTransport == nil {
			plainRedirectTransport = existing.redirect
		}
	}
	if plainRedirectTransport == nil {
		if plain, ok := originTransport.(*http.Transport); ok {
			plainRedirectTransport = plain
		}
	}
	return viewDownloadRoundTripper{
		baseOrigin: baseOrigin,
		origin:     originTransport,
		redirect:   sanitizeRedirectHTTPTransport(plainRedirectTransport),
	}
}

// ViewDownloadClient is a dedicated Layer view download client that enforces
// Hop-0 origin confinement, isolates redirected object hops onto a
// credential-scrubbed *http.Transport, disables CookieJar, and scrubs presigned
// query signatures from all returned errors (including outer *url.Error values).
type ViewDownloadClient struct {
	baseOrigin *url.URL
	httpClient *http.Client
}

// Do executes a Hop-0 Drive9 Layer view download request (following validated
// 3xx redirects over the credential-free redirect transport) and returns
// deterministic query-scrubbed diagnostics on error.
func (c *ViewDownloadClient) Do(req *http.Request) (*http.Response, error) {
	if c == nil || c.httpClient == nil {
		return nil, ErrInvalidViewRedirect
	}
	op := "Get"
	if req != nil && req.Method != "" {
		op = req.Method[:1] + strings.ToLower(req.Method[1:])
	}
	var reqURL *url.URL
	if req != nil {
		reqURL = req.URL
	}
	if err := ValidateViewDownloadOriginRequest(c.baseOrigin, req); err != nil {
		return nil, &url.Error{Op: op, URL: SanitizeURLForError(reqURL), Err: err}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return resp, sanitizeClientBoundaryError(op, reqURL, err)
	}
	return resp, nil
}

// ResolveRedirectLocation resolves a raw HTTP 3xx Location header value against
// the Hop-0 sourceURL per RFC 9110 / RFC 3986 (after verifying sourceURL
// belongs to c.baseOrigin) and validates the resulting absolute target URL with
// ValidateViewDownloadRedirect.
func (c *ViewDownloadClient) ResolveRedirectLocation(sourceURL *url.URL, rawLocation string) (*url.URL, error) {
	if c == nil || c.baseOrigin == nil || sourceURL == nil {
		return nil, ErrInvalidViewRedirect
	}
	if err := ValidateViewDownloadOriginRequest(c.baseOrigin, &http.Request{Method: http.MethodGet, URL: sourceURL}); err != nil {
		return nil, err
	}
	if rawLocation == "" || strings.ContainsAny(rawLocation, "\r\n\x00") {
		return nil, fmt.Errorf("%w: malformed redirect Location header", ErrInvalidViewRedirect)
	}
	parsedLoc, err := url.Parse(rawLocation)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed redirect Location header", ErrInvalidViewRedirect)
	}
	resolved := sourceURL.ResolveReference(parsedLoc)
	if err := ValidateViewDownloadRedirect(http.MethodGet, c.baseOrigin, resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

// DoRedirectLocation resolves rawLocation against Hop-0 sourceURL per RFC 9110,
// validates the target URL, prunes headers to the safe read allowlist, and
// executes the GET directly over the credential-scrubbed redirect transport.
func (c *ViewDownloadClient) DoRedirectLocation(ctx context.Context, sourceURL *url.URL, rawLocation string, header http.Header) (*http.Response, error) {
	resolved, err := c.ResolveRedirectLocation(sourceURL, rawLocation)
	if err != nil {
		return nil, &url.Error{Op: "Get", URL: SanitizeURLForError(sourceURL), Err: err}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolved.String(), nil)
	if err != nil {
		return nil, &url.Error{Op: "Get", URL: SanitizeURLForError(resolved), Err: ErrInvalidViewRedirect}
	}
	for k, vals := range header {
		cp := make([]string, len(vals))
		copy(cp, vals)
		req.Header[k] = cp
	}
	return c.DoRedirectTarget(req)
}

// DoRedirectTarget executes an already-resolved absolute http(s) redirect
// target or presigned object GET directly over the credential-free redirect
// transport (bypassing any origin auth wrapper or mTLS client certificate)
// after rejecting relative URLs, validating the target with
// ValidateViewDownloadRedirect, and pruning headers to the safe read allowlist.
// Callers holding a raw 3xx Location header that may be relative MUST resolve
// it against the Hop-0 source request URL via DoRedirectLocation or
// ResolveRedirectLocation first.
func (c *ViewDownloadClient) DoRedirectTarget(req *http.Request) (*http.Response, error) {
	if c == nil || c.httpClient == nil || c.baseOrigin == nil || req == nil || req.URL == nil {
		return nil, ErrInvalidViewRedirect
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	op := method[:1] + strings.ToLower(method[1:])
	if !req.URL.IsAbs() || req.URL.Host == "" {
		return nil, &url.Error{
			Op:  op,
			URL: SanitizeRawURLForError(req.URL.String()),
			Err: fmt.Errorf("%w: DoRedirectTarget requires an absolute http(s) URL", ErrInvalidViewRedirect),
		}
	}
	if req.Host != "" {
		reqScheme, reqHost, reqPort, uErr := canonicalHTTPOrigin(req.URL)
		if uErr != nil {
			return nil, &url.Error{Op: op, URL: SanitizeURLForError(req.URL), Err: uErr}
		}
		_, overrideHost, overridePort, hErr := canonicalHTTPOrigin(&url.URL{Scheme: reqScheme, Host: req.Host})
		if hErr != nil || overrideHost != reqHost || overridePort != reqPort {
			return nil, &url.Error{
				Op:  op,
				URL: SanitizeURLForError(req.URL),
				Err: fmt.Errorf("%w: request Host override does not match redirect target", ErrInvalidViewRedirect),
			}
		}
	}
	if err := ValidateViewDownloadRedirect(method, c.baseOrigin, req.URL); err != nil {
		return nil, &url.Error{Op: op, URL: SanitizeURLForError(req.URL), Err: err}
	}
	out := req.Clone(context.WithValue(req.Context(), viewRedirectHopKey{}, true))
	out.Method = method
	out.RequestURI = ""
	out.Host = ""
	StripSensitiveRedirectHeaders(out)
	resp, err := c.httpClient.Do(out)
	if err != nil {
		return resp, sanitizeClientBoundaryError(op, out.URL, err)
	}
	return resp, nil
}

// NewViewDownloadHTTPClient returns a dedicated *ViewDownloadClient for Layer
// view downloads with Jar forced to nil, Hop-0 origin confinement to baseOrigin,
// redirected hops isolated on a credential-scrubbed *http.Transport,
// CheckRedirect set to NewViewDownloadCheckRedirect, and query-redacted errors.
func NewViewDownloadHTTPClient(baseOrigin *url.URL, plainTransport *http.Transport) *ViewDownloadClient {
	return &ViewDownloadClient{
		baseOrigin: baseOrigin,
		httpClient: &http.Client{
			Jar:           nil,
			Transport:     NewViewDownloadTransport(baseOrigin, plainTransport),
			CheckRedirect: NewViewDownloadCheckRedirect(baseOrigin),
		},
	}
}

// NewViewDownloadHTTPClientWithOrigin builds a dedicated *ViewDownloadClient for
// Layer view downloads from an existing SDK originClient: Jar is forced to nil,
// Hop 0 is strictly confined to baseOrigin before running originClient.Transport,
// redirected hops bypass custom RoundTripper wrappers, mTLS client certificates,
// origin ServerName pinning, and proxy credentials, and returned errors scrub
// any presigned query signatures.
func NewViewDownloadHTTPClientWithOrigin(originClient *http.Client, baseOrigin *url.URL, plainRedirectTransport *http.Transport) *ViewDownloadClient {
	var (
		originRT http.RoundTripper
		cl       http.Client
	)
	if originClient != nil {
		originRT = originClient.Transport
		cl.Timeout = originClient.Timeout
	}
	cl.Jar = nil
	cl.Transport = NewViewDownloadSplitTransport(baseOrigin, originRT, plainRedirectTransport)
	cl.CheckRedirect = NewViewDownloadCheckRedirect(baseOrigin)
	return &ViewDownloadClient{
		baseOrigin: baseOrigin,
		httpClient: &cl,
	}
}

func scrubRequestURLQueryInPlace(req *http.Request) {
	if req == nil || req.URL == nil {
		return
	}
	clean := *req.URL
	clean.User = nil
	clean.RawQuery = ""
	clean.ForceQuery = false
	clean.Fragment = ""
	clean.RawFragment = ""
	req.URL = &clean
}

// NewViewDownloadCheckRedirect returns an http.Client.CheckRedirect policy for
// Layer view downloads that enforces ValidateViewDownloadRedirect on every hop,
// marks the request context as a redirect hop for NewViewDownloadTransport, and
// strips all sensitive authentication, cookie, and fence headers before
// following the redirect.
func NewViewDownloadCheckRedirect(baseOrigin *url.URL) func(req *http.Request, via []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) == 0 {
			scrubRequestURLQueryInPlace(req)
			return ErrInvalidViewRedirect
		}
		if len(via) > MaxViewDownloadRedirects {
			scrubRequestURLQueryInPlace(req)
			return fmt.Errorf("%w: exceeded %d redirects", ErrInvalidViewRedirect, MaxViewDownloadRedirects)
		}
		if err := ValidateViewDownloadRedirect(via[0].Method, baseOrigin, req.URL); err != nil {
			scrubRequestURLQueryInPlace(req)
			return err
		}
		*req = *req.WithContext(context.WithValue(req.Context(), viewRedirectHopKey{}, true))
		req.Host = ""
		StripSensitiveRedirectHeaders(req)
		return nil
	}
}

// ValidateViewDownloadRedirect verifies a 3xx Location returned inside a Layer
// view: only GET requests may follow redirects, both URLs must use http/https
// without userinfo, and any target on the same host (regardless of port or
// http<->https scheme) or any subdomain of baseOrigin (matching Go net/http's
// shouldCopyHeaderOnRedirect domain trust boundary) MUST NOT resolve to any /v1
// or /v2 Drive9 API route (including under baseOrigin.Path prefixes). Callers
// executing redirects with net/http MUST install NewViewDownloadCheckRedirect
// so sensitive headers are also stripped on every hop.
func ValidateViewDownloadRedirect(method string, baseOrigin *url.URL, location *url.URL) error {
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet {
		return fmt.Errorf("%w: redirect forbidden for method %q", ErrInvalidViewRedirect, method)
	}
	if baseOrigin == nil || location == nil {
		return ErrInvalidViewRedirect
	}
	if !location.IsAbs() {
		if location.Host != "" || location.Opaque != "" || location.User != nil || !strings.HasPrefix(location.Path, "/") {
			return fmt.Errorf("%w: relative redirect location requires source request URL resolution", ErrInvalidViewRedirect)
		}
	}
	_, baseHost, _, err := canonicalHTTPOrigin(baseOrigin)
	if err != nil {
		return err
	}
	resolved := baseOrigin.ResolveReference(location)
	_, resHost, _, err := canonicalHTTPOrigin(resolved)
	if err != nil {
		return err
	}
	if isSameOrSubdomainHost(resHost, baseHost) {
		esc := resolved.EscapedPath()
		lowerEsc := strings.ToLower(esc)
		if strings.Contains(lowerEsc, "%2f") || strings.Contains(lowerEsc, "%5c") {
			return fmt.Errorf("%w: encoded separator in %q", ErrInvalidViewRedirect, SanitizeURLForError(resolved))
		}
		decoded, uErr := url.PathUnescape(esc)
		if uErr != nil || strings.ContainsRune(decoded, '\\') || strings.ContainsRune(decoded, 0) {
			return fmt.Errorf("%w: invalid redirect path in %q", ErrInvalidViewRedirect, SanitizeURLForError(resolved))
		}
		lowerBasePrefix := ""
		if strings.Trim(baseOrigin.Path, "/") != "" {
			cleanedPrefix := strings.TrimSuffix(path.Clean("/"+strings.TrimLeft(baseOrigin.Path, "/")), "/")
			if cleanedPrefix != "" && cleanedPrefix != "/" {
				lowerBasePrefix = strings.ToLower(cleanedPrefix)
			}
		}
		candidates := [...]string{
			esc,
			"/" + strings.TrimLeft(esc, "/"),
			decoded,
			"/" + strings.TrimLeft(decoded, "/"),
			path.Clean("/" + strings.TrimLeft(decoded, "/")),
		}
		for _, p := range candidates {
			lp := strings.ToLower(p)
			if isDrive9APIRoutePath(lp) {
				return fmt.Errorf("%w: %q", ErrInvalidViewRedirect, SanitizeURLForError(resolved))
			}
			if lowerBasePrefix != "" && strings.HasPrefix(lp, lowerBasePrefix+"/") {
				stripped := strings.TrimPrefix(lp, lowerBasePrefix)
				if isDrive9APIRoutePath(stripped) || isDrive9APIRoutePath("/"+strings.TrimLeft(stripped, "/")) {
					return fmt.Errorf("%w: %q", ErrInvalidViewRedirect, SanitizeURLForError(resolved))
				}
			}
		}
	}
	return nil
}

func isSameOrSubdomainHost(host, baseHost string) bool {
	if host == "" || baseHost == "" {
		return false
	}
	return host == baseHost || strings.HasSuffix(host, "."+baseHost)
}

func isDrive9APIRoutePath(lowerPath string) bool {
	return lowerPath == "/v1" || strings.HasPrefix(lowerPath, "/v1/") ||
		lowerPath == "/v2" || strings.HasPrefix(lowerPath, "/v2/")
}

func canonicalHTTPOrigin(u *url.URL) (scheme, host, port string, err error) {
	if u == nil || u.Opaque != "" || u.User != nil {
		return "", "", "", ErrInvalidViewRedirect
	}
	scheme = strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", "", fmt.Errorf("%w: unsupported scheme %q", ErrInvalidViewRedirect, u.Scheme)
	}
	if u.Host == "" {
		return "", "", "", ErrInvalidViewRedirect
	}
	rawHost := u.Host
	if strings.Contains(rawHost, ":") && !strings.HasSuffix(rawHost, "]") {
		if _, p, splitErr := net.SplitHostPort(rawHost); splitErr != nil {
			return "", "", "", ErrInvalidViewRedirect
		} else if p != "" {
			for i := 0; i < len(p); i++ {
				if p[i] < '0' || p[i] > '9' {
					return "", "", "", ErrInvalidViewRedirect
				}
			}
		}
	}
	host = strings.TrimRight(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return "", "", "", ErrInvalidViewRedirect
	}
	port = u.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	return scheme, host, port, nil
}
