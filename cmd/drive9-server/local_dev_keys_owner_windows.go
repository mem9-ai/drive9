//go:build windows

package main

import "os"

// checkLocalDevKeyFileOwner is a no-op on Windows: ownership lives in an ACL
// rather than the stat mode bits, and the per-user config directory already
// scopes the file.
func checkLocalDevKeyFileOwner(path string, info os.FileInfo) error {
	_ = path
	_ = info
	return nil
}
