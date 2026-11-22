package viewprotocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// RouteAction classifies how a Layer-bound client or server view gateway must
// treat an HTTP request.
type RouteAction uint8

const (
	// RoutePassThrough sends the request directly to the root control plane
	// without "/v1/views/{layer_id}" and without X-FS-Layer-Fence (Set A).
	RoutePassThrough RouteAction = iota
	// RouteRewriteView routes the request through "/v1/views/{layer_id}" and
	// attaches X-FS-Layer-Fence when Fence > 0 (Set B).
	RouteRewriteView
	// RouteRejectUnsupported rejects the operation as 501 UNSUPPORTED_IN_LAYER_VIEW (Set C).
	RouteRejectUnsupported
	// RouteRejectInvalid rejects a malformed or nested view request as 400 INVALID_ARGUMENT (Set C).
	RouteRejectInvalid
)

// FormatViewPath prepends "/v1/views/{layer_id}" onto an escaped inner path.
func FormatViewPath(layerID, innerEscapedPath string) (string, error) {
	if err := ValidateLayerID(layerID); err != nil {
		return "", err
	}
	if strings.ContainsAny(innerEscapedPath, "?#") ||
		(!strings.HasPrefix(innerEscapedPath, "/v1/") && !strings.HasPrefix(innerEscapedPath, "/v2/")) ||
		innerEscapedPath == "/v1/views" || strings.HasPrefix(innerEscapedPath, RoutePrefix) {
		return "", fmt.Errorf("%w: %q", ErrInvalidViewRoute, innerEscapedPath)
	}
	return RoutePrefix + layerID + innerEscapedPath, nil
}

// ClassifyClientRoute applies the exhaustive 3-set routing table (v35 §6.5 #3)
// for a Client bound to a non-empty ViewBinding.LayerID.
func ClassifyClientRoute(method, escapedPath string, query url.Values) RouteAction {
	if escapedPath == "/v1/views" || strings.HasPrefix(escapedPath, RoutePrefix) {
		return RouteRejectInvalid
	}
	if isPassThroughControlRoute(method, escapedPath, query) {
		return RoutePassThrough
	}
	return ClassifyViewDataRoute(method, escapedPath, query)
}

// ClassifyViewDataRoute validates inner requests targeting "/v1/views/{layer_id}".
// Only Set B returns RouteRewriteView; Set A control routes are NOT allowed inside "/v1/views/*".
func ClassifyViewDataRoute(method, escapedPath string, query url.Values) RouteAction {
	if escapedPath == "/v1/views" || strings.HasPrefix(escapedPath, RoutePrefix) {
		return RouteRejectInvalid
	}
	switch escapedPath {
	case "/v1/fs:batch-stat", "/v1/fs:batch-read-small", "/v1/fs:batch-write":
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	if escapedPath == "/v1/fs" || strings.HasPrefix(escapedPath, "/v1/fs/") {
		return classifyFSViewRoute(method, query)
	}
	if escapedPath == "/v1/uploads" || strings.HasPrefix(escapedPath, "/v1/uploads/") {
		return classifyV1UploadViewRoute(method, escapedPath, query)
	}
	if strings.HasPrefix(escapedPath, "/v2/uploads/") {
		return classifyV2UploadViewRoute(method, escapedPath, query)
	}
	return RouteRejectUnsupported
}

func isPassThroughControlRoute(method, escapedPath string, query url.Values) bool {
	if escapedPath == "/v1/status" {
		return method == http.MethodGet && len(query) == 0
	}
	if escapedPath == "/v1/layers" {
		if method == http.MethodPost {
			return len(query) == 0
		}
		if method == http.MethodGet {
			return queryKeysSubsetOf(query, "limit", "after")
		}
		return false
	}
	if strings.HasPrefix(escapedPath, "/v1/layers/") {
		rest := strings.TrimPrefix(escapedPath, "/v1/layers/")
		if rest == "" || strings.HasSuffix(rest, "/") {
			return false
		}
		ref, action, hasSlash := strings.Cut(rest, "/")
		if ref == "" || strings.Contains(action, "/") {
			return false
		}
		if !hasSlash {
			if method == http.MethodGet {
				return len(query) == 0
			}
			if method == http.MethodDelete {
				return queryKeysSubsetOf(query, "force")
			}
			return false
		}
		if method != http.MethodPost || len(query) != 0 {
			return false
		}
		switch action {
		case "fork", "merge", "bind-mount", "renew-mount", "unbind-mount":
			return true
		default:
			return false
		}
	}
	if escapedPath == "/v1/mounts" {
		return method == http.MethodPost && len(query) == 0
	}
	if strings.HasPrefix(escapedPath, "/v1/mounts/") {
		if len(query) != 0 {
			return false
		}
		rest := strings.TrimPrefix(escapedPath, "/v1/mounts/")
		if rest == "" || strings.HasSuffix(rest, "/") {
			return false
		}
		if strings.HasSuffix(rest, "/heartbeat") {
			id := strings.TrimSuffix(rest, "/heartbeat")
			return id != "" && !strings.Contains(id, "/") && method == http.MethodPost
		}
		return !strings.Contains(rest, "/") && method == http.MethodDelete
	}
	return false
}

func classifyFSViewRoute(method string, query url.Values) RouteAction {
	switch method {
	case http.MethodHead, http.MethodPut, http.MethodPatch:
		if len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	case http.MethodDelete:
		if queryKeysSubsetOf(query, "kind", "recursive") {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	case http.MethodGet:
		if query.Has("grep") || query.Has("find") || query.Has("tasks") {
			return RouteRejectUnsupported
		}
		switch {
		case query.Has("stat"):
			if queryKeysSubsetOf(query, "stat") {
				return RouteRewriteView
			}
			return RouteRejectUnsupported
		case query.Has("list"):
			if queryKeysSubsetOf(query, "list", "limit", "cursor") {
				return RouteRewriteView
			}
			return RouteRejectUnsupported
		case len(query) == 0:
			return RouteRewriteView
		default:
			return RouteRejectUnsupported
		}
	case http.MethodPost:
		return classifyFSPostViewQuery(query)
	default:
		return RouteRejectUnsupported
	}
}

func classifyFSPostViewQuery(query url.Values) RouteAction {
	selectors := [...]string{
		"append-log", "append", "copy", "rename", "mkdir",
		"chmod", "setmeta", "create", "symlink", "hardlink",
	}
	count := 0
	selected := ""
	for _, k := range selectors {
		if query.Has(k) {
			count++
			selected = k
		}
	}
	if count != 1 {
		if count == 0 && len(query) > 0 {
			return RouteRejectUnsupported
		}
		return RouteRejectInvalid
	}
	switch selected {
	case "append", "append-log":
		return RouteRejectUnsupported
	case "create":
		if queryKeysSubsetOf(query, "create", "layout") {
			return RouteRewriteView
		}
	case "mkdir":
		if queryKeysSubsetOf(query, "mkdir", "mode") {
			return RouteRewriteView
		}
	case "chmod", "rename", "copy", "hardlink", "symlink", "setmeta":
		if queryKeysSubsetOf(query, selected) {
			return RouteRewriteView
		}
	}
	return RouteRejectInvalid
}

func classifyV1UploadViewRoute(method, escapedPath string, query url.Values) RouteAction {
	if escapedPath == "/v1/uploads" {
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
		if method == http.MethodGet && queryKeysSubsetOf(query, "path", "status") {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	if escapedPath == "/v1/uploads/initiate" {
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	rest := strings.TrimPrefix(escapedPath, "/v1/uploads/")
	if rest == "" || strings.HasSuffix(rest, "/") {
		return RouteRejectUnsupported
	}
	id, action, hasSlash := strings.Cut(rest, "/")
	if id == "" || strings.Contains(action, "/") {
		return RouteRejectUnsupported
	}
	if !hasSlash {
		if method == http.MethodDelete && len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	switch action {
	case "complete":
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
	case "resume":
		if (method == http.MethodGet || method == http.MethodPost) && len(query) == 0 {
			return RouteRewriteView
		}
	}
	return RouteRejectUnsupported
}

func classifyV2UploadViewRoute(method, escapedPath string, query url.Values) RouteAction {
	if method != http.MethodPost || len(query) != 0 {
		return RouteRejectUnsupported
	}
	rest := strings.TrimPrefix(escapedPath, "/v2/uploads/")
	if rest == "" || strings.HasSuffix(rest, "/") {
		return RouteRejectUnsupported
	}
	if rest == "initiate" {
		return RouteRewriteView
	}
	id, action, ok := strings.Cut(rest, "/")
	if !ok || id == "" || id == "initiate" || strings.Contains(action, "/") {
		return RouteRejectUnsupported
	}
	switch action {
	case "presign", "presign-batch", "complete", "abort":
		return RouteRewriteView
	default:
		return RouteRejectUnsupported
	}
}

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

// SanitizeURLForError strips userinfo credentials, query parameters (such as
// presigned X-Amz-Signature / token queries), and fragments from u so error
// messages and logs retain scheme://host/path diagnostics without leaking
// signed query material.
func SanitizeURLForError(u *url.URL) string {
	if u == nil {
		return ""
	}
	clean := *u
	clean.User = nil
	clean.RawQuery = ""
	clean.ForceQuery = false
	clean.Fragment = ""
	clean.RawFragment = ""
	return clean.String()
}

// SanitizeRawURLForError strips query strings, fragments, and userinfo
// credentials from a raw URL string.
func SanitizeRawURLForError(raw string) string {
	if raw == "" {
		return ""
	}
	cut := raw
	if idx := strings.IndexAny(cut, "?#"); idx >= 0 {
		cut = cut[:idx]
	}
	if parsed, err := url.Parse(cut); err == nil && parsed != nil && parsed.Scheme != "" {
		return SanitizeURLForError(parsed)
	}
	if schemeIdx := strings.Index(cut, "://"); schemeIdx >= 0 {
		rest := cut[schemeIdx+3:]
		slashIdx := strings.IndexByte(rest, '/')
		authority := rest
		suffix := ""
		if slashIdx >= 0 {
			authority = rest[:slashIdx]
			suffix = rest[slashIdx:]
		}
		if atIdx := strings.LastIndexByte(authority, '@'); atIdx >= 0 {
			authority = authority[atIdx+1:]
		}
		return cut[:schemeIdx+3] + authority + suffix
	}
	return cut
}

// ScrubSensitiveURLText removes query strings, fragments, and userinfo
// credentials from HTTP(S) URLs (including bracketed IPv6 host literals) and
// quoted URL/path tokens embedded inside an error message (such as Go
// net/http's Location parse failure messages).
func ScrubSensitiveURLText(msg string) string {
	if msg == "" || !strings.ContainsAny(msg, "?#@") {
		return msg
	}
	var b strings.Builder
	b.Grow(len(msg))
	for i := 0; i < len(msg); {
		if msg[i] == '"' || msg[i] == '\'' {
			quote := msg[i]
			endQuote := findClosingUnescapedQuoteByte(msg, i+1, quote)
			if endQuote >= 0 {
				inner := msg[i+1 : endQuote]
				if looksLikeURLOrQueryPath(inner) {
					b.WriteByte(quote)
					b.WriteString(SanitizeRawURLForError(inner))
					b.WriteByte(quote)
					i = endQuote + 1
					continue
				}
			}
		}
		if strings.HasPrefix(msg[i:], "http://") || strings.HasPrefix(msg[i:], "https://") {
			end := scanHTTPURLTokenEnd(msg, i)
			b.WriteString(SanitizeRawURLForError(msg[i:end]))
			i = end
			continue
		}
		b.WriteByte(msg[i])
		i++
	}
	return b.String()
}

func scanHTTPURLTokenEnd(msg string, start int) int {
	schemeSep := strings.Index(msg[start:], "://")
	if schemeSep < 0 {
		return start
	}
	j := start + schemeSep + 3
	inAuthority := true
	inIPv6Host := false
	for j < len(msg) {
		ch := msg[j]
		if ch == '\\' && j+1 < len(msg) {
			j += 2
			continue
		}
		if inIPv6Host {
			if ch == ']' {
				inIPv6Host = false
				j++
				continue
			}
			if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '"' || ch == '\'' {
				break
			}
			j++
			continue
		}
		if inAuthority {
			if ch == '[' {
				inIPv6Host = true
				j++
				continue
			}
			if ch == '/' || ch == '?' || ch == '#' {
				inAuthority = false
			}
		}
		if isErrorURLTokenDelimiter(ch) {
			break
		}
		j++
	}
	return j
}

func findClosingUnescapedQuoteByte(s string, from int, quote byte) int {
	for j := from; j < len(s); j++ {
		if s[j] == '\\' && j+1 < len(s) {
			j++
			continue
		}
		if s[j] == quote {
			return j
		}
	}
	return -1
}

func isErrorURLTokenDelimiter(ch byte) bool {
	switch ch {
	case ' ', '\t', '\r', '\n', '"', '\'', '>', ')', ']':
		return true
	default:
		return false
	}
}

func looksLikeURLOrQueryPath(s string) bool {
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return true
	}
	if !strings.ContainsAny(s, "?#") {
		return false
	}
	return strings.Contains(s, "/") || strings.Contains(s, "=")
}

type redactedDownloadError struct {
	msg          string
	cause        error
	sanitizedURL *url.Error
}

func (e *redactedDownloadError) Error() string { return e.msg }

func (e *redactedDownloadError) Is(target error) bool {
	return errors.Is(e.cause, target)
}

func (e *redactedDownloadError) As(target any) bool {
	if uTarget, ok := target.(**url.Error); ok && e.sanitizedURL != nil {
		*uTarget = e.sanitizedURL
		return true
	}
	return errors.As(e.cause, target)
}

// SanitizeViewDownloadError scrubs outer and wrapped *url.Error values as well
// as inner error messages (including malformed Location parse errors generated
// before CheckRedirect) so neither Error() text nor *url.Error.URL ever retains
// presigned query signatures, fragments, or userinfo credentials.
func SanitizeViewDownloadError(err error) error {
	if err == nil {
		return nil
	}
	if uErr, ok := err.(*url.Error); ok && uErr != nil {
		return sanitizeURLErrorValue(uErr)
	}
	var nestedUErr *url.Error
	var cleanUErr *url.Error
	if errors.As(err, &nestedUErr) && nestedUErr != nil {
		cleanUErr = sanitizeURLErrorValue(nestedUErr)
	}
	origMsg := err.Error()
	cleanMsg := ScrubSensitiveURLText(origMsg)
	if cleanMsg != origMsg || cleanUErr != nil {
		return &redactedDownloadError{
			msg:          cleanMsg,
			cause:        err,
			sanitizedURL: cleanUErr,
		}
	}
	return err
}

func sanitizeURLErrorValue(uErr *url.Error) *url.Error {
	if uErr == nil {
		return nil
	}
	clean := *uErr
	clean.URL = SanitizeRawURLForError(uErr.URL)
	clean.Err = SanitizeViewDownloadError(uErr.Err)
	return &clean
}

type controlledTransportError struct {
	msg     string
	timeout bool
}

func (e *controlledTransportError) Error() string {
	if e == nil || e.msg == "" {
		return "view download transport failure"
	}
	return e.msg
}

func (e *controlledTransportError) Timeout() bool   { return e != nil && e.timeout }
func (e *controlledTransportError) Temporary() bool { return e != nil && e.timeout }

// buildControlledBoundaryCause separates cause classification from diagnostic
// text construction: it NEVER retains or formats cause.Error(), returning only
// exact known sentinel values or fixed diagnostic messages while preserving
// cancellation, redirect-policy, EOF, and timeout semantics.
func buildControlledBoundaryCause(cause error, isTimeout bool) error {
	switch {
	case errors.Is(cause, ErrInvalidViewRedirect):
		return ErrInvalidViewRedirect
	case errors.Is(cause, ErrInvalidViewRoute):
		return ErrInvalidViewRoute
	case errors.Is(cause, ErrInvalidLayerID):
		return ErrInvalidLayerID
	case errors.Is(cause, ErrInvalidFence):
		return ErrInvalidFence
	case errors.Is(cause, context.Canceled):
		return context.Canceled
	case errors.Is(cause, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(cause, io.EOF):
		return io.EOF
	case errors.Is(cause, io.ErrUnexpectedEOF):
		return io.ErrUnexpectedEOF
	case errors.Is(cause, http.ErrUseLastResponse):
		return http.ErrUseLastResponse
	}
	var certVerifyErr *tls.CertificateVerificationError
	var unknownAuthErr x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var certInvalidErr x509.CertificateInvalidError
	var sysRootsErr x509.SystemRootsError
	var recordHdrErr tls.RecordHeaderError
	var alertErr tls.AlertError
	if errors.As(cause, &certVerifyErr) ||
		errors.As(cause, &unknownAuthErr) ||
		errors.As(cause, &hostErr) ||
		errors.As(cause, &certInvalidErr) ||
		errors.As(cause, &sysRootsErr) ||
		errors.As(cause, &recordHdrErr) ||
		errors.As(cause, &alertErr) {
		return &controlledTransportError{msg: "tls verification or handshake failure", timeout: isTimeout}
	}
	var dnsErr *net.DNSError
	var netOpErr *net.OpError
	if errors.As(cause, &dnsErr) || errors.As(cause, &netOpErr) {
		return &controlledTransportError{msg: "network transport failure", timeout: isTimeout}
	}
	return &controlledTransportError{msg: "view download transport failure", timeout: isTimeout}
}

func sanitizeClientBoundaryURL(candidate string, fallbackURL *url.URL) string {
	if candidate != "" {
		cut := candidate
		if idx := strings.IndexAny(cut, "?#"); idx >= 0 {
			cut = cut[:idx]
		}
		if parsed, err := url.Parse(cut); err == nil && parsed != nil && parsed.Opaque == "" && parsed.Host != "" {
			scheme := strings.ToLower(parsed.Scheme)
			if scheme == "http" || scheme == "https" {
				return SanitizeURLForError(parsed)
			}
		}
	}
	return SanitizeURLForError(fallbackURL)
}

func sanitizeClientBoundaryError(defaultOp string, fallbackURL *url.URL, err error) error {
	if err == nil {
		return nil
	}
	op := "Get"
	if strings.EqualFold(defaultOp, "head") {
		op = "Head"
	}
	cleanURL := SanitizeURLForError(fallbackURL)
	cause := err
	var uErr *url.Error
	if errors.As(err, &uErr) && uErr != nil {
		cleanURL = sanitizeClientBoundaryURL(uErr.URL, fallbackURL)
		if uErr.Err != nil {
			cause = uErr.Err
		}
	}
	var netErr net.Error
	isTimeout := errors.As(err, &netErr) && netErr.Timeout()
	return &url.Error{
		Op:  op,
		URL: cleanURL,
		Err: buildControlledBoundaryCause(cause, isTimeout),
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

// CanonicalizeViewPath normalizes an absolute Layer filesystem path using the
// exact byte, UTF-8, NFC, dot-segment, and preserved-whitespace semantics of
// pkg/pathutil, while rejecting empty or relative paths without a leading '/'.
func CanonicalizeViewPath(raw string) (string, error) {
	if raw == "" || !strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("%w: path must start with '/'", ErrInvalidViewRoute)
	}
	if raw == "/" {
		return "/", nil
	}
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b == 0x00 {
			return "", fmt.Errorf("%w: path contains NUL", ErrInvalidViewRoute)
		}
		if b == '\n' || b == '\t' || b == '\r' {
			continue
		}
		if b >= 0x01 && b <= 0x1f {
			return "", fmt.Errorf("%w: path contains control byte 0x%02x", ErrInvalidViewRoute, b)
		}
	}
	if strings.ContainsRune(raw, '\\') {
		return "", fmt.Errorf("%w: path contains backslash", ErrInvalidViewRoute)
	}
	for _, seg := range strings.Split(strings.Trim(raw, "/"), "/") {
		if seg == "." || seg == ".." {
			return "", fmt.Errorf("%w: dot segment %q", ErrInvalidViewRoute, seg)
		}
	}
	cleaned := path.Clean(raw)
	if cleaned == "." || cleaned == "/" {
		return "/", nil
	}
	cleaned = strings.TrimSuffix(cleaned, "/")
	if !utf8.ValidString(cleaned) {
		return "", fmt.Errorf("%w: invalid UTF-8", ErrInvalidViewRoute)
	}
	return norm.NFC.String(cleaned), nil
}

// PathWithinBaseRoot reports whether targetPath resides at or strictly inside
// baseRootPath under canonical Drive9 path semantics (preserving spaces in names,
// NFC-normalizing UTF-8, and failing closed on empty, relative, or dot paths).
func PathWithinBaseRoot(baseRootPath, targetPath string) bool {
	base, err := CanonicalizeViewPath(baseRootPath)
	if err != nil {
		return false
	}
	target, err := CanonicalizeViewPath(targetPath)
	if err != nil {
		return false
	}
	if base == "/" {
		return true
	}
	return target == base || strings.HasPrefix(target, base+"/")
}

func queryKeysSubsetOf(q url.Values, allowed ...string) bool {
	for k := range q {
		found := false
		for _, a := range allowed {
			if k == a {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
