//go:build !windows

package main

import (
	"fmt"
	"os"
	"syscall"
)

// checkLocalDevKeyPermissions refuses a key file that other users could read or
// that belongs to another user.
//
// The mode check alone is not enough on a shared temp directory: another local
// user can pre-create the path with 0600 and keys of their choosing, and a
// world-unreadable file owned by them would still hand them this installation's
// signing and master keys. Unix ownership is the only reliable signal.
func checkLocalDevKeyPermissions(path string, info os.FileInfo) error {
	perm := info.Mode().Perm()
	if perm&0o077 != 0 {
		return fmt.Errorf(
			"local dev key file %s has mode %04o; it must not be readable by group or other users (chmod 600, or delete it to generate a new pair)",
			path, perm)
	}
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

// restrictLocalDevKeyDir keeps the key directory out of reach of other users.
func restrictLocalDevKeyDir(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("stat local dev key dir %s: %w", dir, err)
	}
	if info.Mode().Perm()&0o077 == 0 {
		return nil
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("local dev key dir %s must not be accessible to other users: %w", dir, err)
	}
	return nil
}
