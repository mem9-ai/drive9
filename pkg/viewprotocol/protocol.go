package viewprotocol

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// IsCanonicalLayerID reports whether id matches the server-generated ULID form.
func IsCanonicalLayerID(id string) bool {
	return canonicalLayerIDRE.MatchString(id)
}

// ValidateLayerID enforces the canonical server-generated Layer ID syntax.
func ValidateLayerID(id string) error {
	if !IsCanonicalLayerID(id) {
		return fmt.Errorf("%w: %q", ErrInvalidLayerID, id)
	}
	return nil
}

// NormalizeLayerName trims and validates an optional human-facing layer name.
// Names stay disjoint from "main", "tag:" selectors, and "layer_" IDs.
func NormalizeLayerName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", nil
	}
	if len(name) > MaxLayerNameBytes || strings.EqualFold(name, "main") || name == "." || name == ".." {
		return "", fmt.Errorf("%w: %q", ErrInvalidLayerName, raw)
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "tag:") || strings.HasPrefix(lower, "layer_") {
		return "", fmt.Errorf("%w: %q uses reserved prefix", ErrInvalidLayerName, raw)
	}
	if !validLayerText(name) || strings.ContainsRune(name, '/') {
		return "", fmt.Errorf("%w: forbidden character", ErrInvalidLayerName)
	}
	return name, nil
}

// NormalizeLayerRef trims and validates a control-plane {ref} parameter.
func NormalizeLayerRef(raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if ref == "" || len(ref) > MaxLayerRefBytes || strings.EqualFold(ref, "main") {
		return "", fmt.Errorf("%w: %q", ErrInvalidLayerRef, raw)
	}
	if !validLayerText(ref) {
		return "", fmt.Errorf("%w: forbidden character", ErrInvalidLayerRef)
	}
	return ref, nil
}

func validLayerText(s string) bool {
	return utf8.ValidString(s) && strings.IndexFunc(s, func(r rune) bool {
		return unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r)
	}) < 0
}

// ParseTagSelector parses "tag:<key>" or "tag:<key>=<value>".
func ParseTagSelector(ref string) (key, value string, hasValue, isTag bool, err error) {
	const prefix = "tag:"
	if len(ref) < len(prefix) || !strings.EqualFold(ref[:len(prefix)], prefix) {
		return "", "", false, false, nil
	}
	rest := ref[len(prefix):]
	if rest == "" {
		return "", "", false, true, fmt.Errorf("%w: empty tag selector", ErrInvalidLayerRef)
	}
	k, v, eq := strings.Cut(rest, "=")
	if k == "" || len(k) > MaxLayerNameBytes || len(v) > MaxLayerNameBytes {
		return "", "", false, true, fmt.Errorf("%w: invalid tag selector", ErrInvalidLayerRef)
	}
	return k, v, eq, true, nil
}

// ParseFenceHeader parses X-FS-Layer-Fence. When present it must be a positive
// odd base-10 int64 without whitespace or leading plus/zero padding.
func ParseFenceHeader(h http.Header) (fence int64, present bool, err error) {
	vals := h.Values(FenceHeader)
	if len(vals) == 0 {
		return 0, false, nil
	}
	if len(vals) > 1 {
		return 0, true, fmt.Errorf("%w: duplicate %s header", ErrInvalidFence, FenceHeader)
	}
	raw := vals[0]
	if raw == "" || raw[0] < '1' || raw[0] > '9' {
		return 0, true, fmt.Errorf("%w: %q", ErrInvalidFence, raw)
	}
	n, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil || n <= 0 || n%2 == 0 {
		return 0, true, fmt.Errorf("%w: must be positive odd integer", ErrInvalidFence)
	}
	return n, true, nil
}

// ParseViewRoute strips "/v1/views/{layer_id}" from r and returns a cloned
// request targeting the inner /v1/* or /v2/* endpoint along with layer_id.
func ParseViewRoute(r *http.Request) (*http.Request, string, error) {
	if r == nil || r.URL == nil {
		return nil, "", ErrInvalidViewRoute
	}
	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, RoutePrefix) {
		return nil, "", ErrInvalidViewRoute
	}
	rest := strings.TrimPrefix(escaped, RoutePrefix)
	rawLayerID, innerEscaped, ok := strings.Cut(rest, "/")
	if !ok || rawLayerID == "" {
		return nil, "", ErrInvalidViewRoute
	}
	layerID, err := url.PathUnescape(rawLayerID)
	if err != nil || layerID != rawLayerID {
		return nil, "", ErrInvalidLayerID
	}
	if err := ValidateLayerID(layerID); err != nil {
		return nil, "", err
	}
	innerEscaped = "/" + innerEscaped
	if (!strings.HasPrefix(innerEscaped, "/v1/") && !strings.HasPrefix(innerEscaped, "/v2/")) ||
		innerEscaped == "/v1/views" || strings.HasPrefix(innerEscaped, RoutePrefix) {
		return nil, "", ErrInvalidViewRoute
	}
	innerDecoded, err := url.PathUnescape(innerEscaped)
	if err != nil {
		return nil, "", ErrInvalidViewRoute
	}
	out := r.Clone(r.Context())
	out.URL.Path = innerDecoded
	out.URL.RawPath = innerEscaped
	out.RequestURI = out.URL.RequestURI()
	return out, layerID, nil
}
