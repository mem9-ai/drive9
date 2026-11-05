//go:build !windows

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// The key file lives in a path an unprivileged local user may be able to
// pre-create (the temp-dir fallback), so every access opens with O_NOFOLLOW and
// validates the inode it actually opened. Checking the path with Lstat and then
// reading or chmod-ing it would race: os.Chmod follows a symlink, so a swapped
// link could retarget the change at an unrelated directory.

// readLocalDevKeyFilePlatform reads the key file only when the file itself is
// safe: a regular file, owned by this user, with no group or other permission
// bits.
func readLocalDevKeyFilePlatform(path string) ([]byte, error) {
	file, err := openLocalDevNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat local dev key file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("local dev key file %s is not a regular file", path)
	}
	if permErr := checkLocalDevKeyPermissions(path, info); permErr != nil {
		return nil, permErr
	}
	return io.ReadAll(file)
}

// checkLocalDevKeyDir verifies that the key directory cannot be modified by other
// users, without changing its permissions.
//
// The directory is opened with O_NOFOLLOW, so a symlink planted at the path is
// rejected outright instead of being followed, and the checks run on the inode
// that was opened. An existing directory is never re-permissioned:
// DRIVE9_LOCAL_DEV_KEY_FILE may legitimately live inside a directory this process
// does not own the layout of, and chmod-ing it to 0700 would break everything
// else that traverses it.
//
// Write access is the part that matters. A group or other member who can write
// into the directory can delete the key file, and the next start treats a missing
// file as a rotation request: it generates a new pair and everything encrypted
// under the deleted one becomes unreadable. That is data loss, not just a denial
// of service, so a writable parent is refused rather than repaired. Read-only
// access (0755) is fine — the file itself is 0600, opened with O_NOFOLLOW, and
// rejected when another uid owns it.
func checkLocalDevKeyDir(dir string) error {
	file, err := openLocalDevNoFollow(dir)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat local dev key dir %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local dev key dir %s is not a directory", dir)
	}
	if permErr := checkLocalDevKeyOwner(dir, info); permErr != nil {
		return permErr
	}
	if perm := info.Mode().Perm(); perm&0o022 != 0 {
		return fmt.Errorf(
			"local dev key dir %s is mode %04o, so other users can delete the key file and force a rotation; run 'chmod go-w %s' or point %s at a directory only this user can write",
			dir, perm, dir, EnvLocalDevKeyFile)
	}
	return nil
}

// openLocalDevNoFollow opens path without following a final symlink.
func openLocalDevNoFollow(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EMLINK) {
			return nil, fmt.Errorf("local dev key path %s is a symlink; refusing to use it", path)
		}
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// checkLocalDevKeyPermissions refuses keys that other users could read or that
// belong to another user. A readable file is not enough of a signal on a shared
// temp directory: another local user can create the path with 0600 and keys of
// their choosing, so ownership is checked too.
func checkLocalDevKeyPermissions(path string, info os.FileInfo) error {
	perm := info.Mode().Perm()
	if perm&0o077 != 0 {
		return fmt.Errorf(
			"local dev key file %s has mode %04o; it must not be readable by group or other users (chmod 600, or delete it to generate a new pair)",
			path, perm)
	}
	return checkLocalDevKeyOwner(path, info)
}

func checkLocalDevKeyOwner(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot determine the owner of local dev key path %s", path)
	}
	if uid := os.Getuid(); int(stat.Uid) != uid {
		return fmt.Errorf(
			"local dev key path %s is owned by uid %d, not the current user (uid %d); refusing to trust it",
			path, stat.Uid, uid)
	}
	return nil
}
