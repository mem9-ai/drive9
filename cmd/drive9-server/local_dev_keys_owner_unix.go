//go:build !windows

package main

import (
	"fmt"
	"os"
	"syscall"
)

// checkLocalDevKeyFileOwner refuses a key file that belongs to another user.
//
// The mode check alone is not enough on a shared temp directory: another local
// user can pre-create the path with 0600 and keys of their choosing, and a
// world-unreadable file owned by them would still hand them this installation's
// signing and master keys. Unix ownership is the only reliable signal.
func checkLocalDevKeyFileOwner(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot determine the owner of local dev key file %s", path)
	}
	if uid := os.Getuid(); int(stat.Uid) != uid {
		return fmt.Errorf(
			"local dev key file %s is owned by uid %d, not the current user (uid %d); refusing to trust it",
			path, stat.Uid, uid)
	}
	return nil
}
