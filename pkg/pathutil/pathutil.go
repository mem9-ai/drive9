// Package pathutil provides path canonicalization and validation for dat9.
// Paths follow the inode model convention: directories end with "/", files do not.
package pathutil

import (
	"fmt"
	pathpkg "path"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Canonicalize normalizes and validates a file path (no trailing slash).
func Canonicalize(raw string) (string, error) {
	return canonicalize(raw, false)
}

// CanonicalizeDir normalizes and validates a directory path (trailing slash).
func CanonicalizeDir(raw string) (string, error) {
	return canonicalize(raw, true)
}

func canonicalize(raw string, isDir bool) (string, error) {
	if raw == "" || raw == "/" {
		return "/", nil
	}

	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b == 0x00 {
			return "", fmt.Errorf("path contains NUL character")
		}
		if b >= 0x01 && b <= 0x1f {
			return "", fmt.Errorf("path contains control character 0x%02x", b)
		}
	}

	if strings.ContainsRune(raw, '\\') {
		return "", fmt.Errorf("path contains backslash")
	}

	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}

	cleaned := pathpkg.Clean(raw)
	if cleaned == "." {
		return "/", nil
	}

	segments := strings.Split(strings.Trim(raw, "/"), "/")
	for _, seg := range segments {
		if seg == "." || seg == ".." {
			return "", fmt.Errorf("path contains %q segment", seg)
		}
	}

	cleaned = strings.TrimSuffix(cleaned, "/")

	if !utf8.ValidString(cleaned) {
		return "", fmt.Errorf("path contains invalid UTF-8")
	}
	cleaned = norm.NFC.String(cleaned)

	if isDir && cleaned != "/" {
		cleaned += "/"
	}

	return cleaned, nil
}

// ParentPath returns the parent directory path (always ends with "/").
func ParentPath(p string) string {
	if p == "/" {
		return "/"
	}
	p = strings.TrimSuffix(p, "/")
	dir := pathpkg.Dir(p)
	if dir == "." {
		return "/"
	}
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	return dir
}

// BaseName returns the last element of the path (without trailing slash).
func BaseName(p string) string {
	if p == "/" {
		return "/"
	}
	p = strings.TrimSuffix(p, "/")
	return pathpkg.Base(p)
}

// IsDir returns true if the path represents a directory (ends with "/").
func IsDir(p string) bool {
	return p == "/" || strings.HasSuffix(p, "/")
}

// Ext returns the file extension (e.g. ".txt").
func Ext(p string) string {
	return pathpkg.Ext(strings.TrimSuffix(p, "/"))
}
