//go:build windows

package main

import (
	"fmt"
	"os"
)

// readLocalDevKeyFilePlatform reads the key file, refusing a symlink.
//
// Go reports every writable file as 0666 on Windows (0444 when read-only) and
// Chmod(0600) only toggles the read-only flag, so Unix-style mode bits carry no
// meaning here; ownership lives in an ACL and the per-user config directory
// already scopes the file.
func readLocalDevKeyFilePlatform(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("local dev key path %s is a symlink; refusing to use it", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("local dev key file %s is not a regular file", path)
	}
	return os.ReadFile(path)
}

// restrictLocalDevKeyDir is a no-op on Windows: permissions are ACL-based.
func restrictLocalDevKeyDir(dir string) error {
	_ = dir
	return nil
}
