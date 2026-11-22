package viewprotocol

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

// ValidateOperationID validates a local mount_id or operation_id token (1..64 ASCII bytes).
func ValidateOperationID(id string) error {
	if id == "" || len(id) > MaxMountIDBytes {
		return fmt.Errorf("%w: %q", ErrInvalidOperationID, id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == ':'
		if !ok {
			return fmt.Errorf("%w: %q", ErrInvalidOperationID, id)
		}
	}
	return nil
}

// SwitchBinding describes a FUSE mount's active view binding in SwitchJournal
// and local control responses. LayerID is empty for main and MUST be a
// canonical server-generated layer_id (never a name or tag ref) otherwise.
type SwitchBinding struct {
	LayerID      string `json:"layer_id,omitempty"`
	Fence        int64  `json:"fence,omitempty"`
	BaseRootPath string `json:"base_root_path,omitempty"`
	ReadOnly     bool   `json:"read_only,omitempty"`
}

// Validate checks v36 §6.3/§6.5 invariants on a persisted or wire SwitchBinding.
func (b SwitchBinding) Validate() error {
	if b.LayerID == "" {
		if b.Fence != 0 || b.BaseRootPath != "" {
			return fmt.Errorf("%w: main binding must not carry layer fence or base_root_path", ErrInvalidFence)
		}
		return nil
	}
	if err := ValidateLayerID(b.LayerID); err != nil {
		return err
	}
	if b.BaseRootPath != "" {
		canon, err := CanonicalizeViewPath(b.BaseRootPath)
		if err != nil || canon != b.BaseRootPath {
			return fmt.Errorf("invalid canonical base_root_path %q", b.BaseRootPath)
		}
	}
	if b.ReadOnly {
		if b.Fence != 0 {
			return fmt.Errorf("%w: read-only layer binding must have fence 0", ErrInvalidFence)
		}
		return nil
	}
	if b.Fence <= 0 || b.Fence%2 == 0 {
		return fmt.Errorf("%w: writable layer binding requires positive odd fence, got %d", ErrInvalidFence, b.Fence)
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
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c < 0x20 || c == 0x7f || c == '/' {
			return "", fmt.Errorf("%w: %q contains forbidden character", ErrInvalidLayerName, raw)
		}
	}
	return name, nil
}

// NormalizeLayerRef trims and validates a control-plane {ref} parameter.
func NormalizeLayerRef(raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if ref == "" || len(ref) > MaxLayerRefBytes || strings.EqualFold(ref, "main") {
		return "", fmt.Errorf("%w: %q", ErrInvalidLayerRef, raw)
	}
	for i := 0; i < len(ref); i++ {
		if ref[i] < 0x20 || ref[i] == 0x7f {
			return "", fmt.Errorf("%w: control character in ref", ErrInvalidLayerRef)
		}
	}
	return ref, nil
}

// ParseTagSelector parses "tag:<key>" or "tag:<key>=<value>".
func ParseTagSelector(ref string) (key, value string, hasValue, isTag bool, err error) {
	if !strings.HasPrefix(ref, "tag:") {
		return "", "", false, false, nil
	}
	rest := strings.TrimPrefix(ref, "tag:")
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
