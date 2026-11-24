package viewprotocol

import (
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

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
