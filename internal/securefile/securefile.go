// Package securefile validates and reads process-owned private files.
package securefile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ReadOwnerOnlySingleLine reads one non-empty line from a private regular file
// owned by the current process user in a private process-owned directory.
func ReadOwnerOnlySingleLine(path string, maxBytes int64) (string, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("file must be a clean absolute path")
	}
	if maxBytes <= 0 {
		return "", fmt.Errorf("maximum file size must be positive")
	}
	if err := ValidatePrivateOwnedDirectory(filepath.Dir(path)); err != nil {
		return "", fmt.Errorf("file parent: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("file must be a regular file")
	}
	if !ownedByEffectiveUser(info) {
		return "", fmt.Errorf("file must be owned by the process user")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("file must not be accessible by group or other")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !os.SameFile(info, openedInfo) {
		return "", fmt.Errorf("file changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(raw)) > maxBytes {
		return "", fmt.Errorf("file is too large")
	}
	value := strings.TrimSpace(string(raw))
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("file must contain exactly one non-empty value")
	}
	return value, nil
}

// ValidatePrivateOwnedDirectory rejects symlinked, foreign-owned, or
// group/other-writable directories.
func ValidatePrivateOwnedDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("must be a real directory")
	}
	if !ownedByEffectiveUser(info) {
		return fmt.Errorf("must be owned by the process user")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("must not be writable by group or other")
	}
	return nil
}
