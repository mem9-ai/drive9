//go:build windows

package main

import "os"

// checkLocalDevKeyPermissions is a no-op on Windows.
//
// Go reports every writable file as 0666 there (0444 when read-only) and
// Chmod(0600) does not create Unix-style owner-only bits, so a mode check would
// reject the key file on every restart. Ownership lives in an ACL rather than in
// stat, and the per-user config directory already scopes the file.
func checkLocalDevKeyPermissions(path string, info os.FileInfo) error {
	_ = path
	_ = info
	return nil
}

// restrictLocalDevKeyDir is a no-op on Windows for the same reason.
func restrictLocalDevKeyDir(dir string) error {
	_ = dir
	return nil
}
