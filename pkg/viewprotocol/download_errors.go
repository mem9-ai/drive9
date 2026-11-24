package viewprotocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

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
