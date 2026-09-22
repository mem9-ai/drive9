//go:build !windows

package fuse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func acquirePromotionRootLock(localRoot string) (*os.File, error) {
	dir := filepath.Join(localRoot, ".drive9", "promotion")
	lockPath := filepath.Join(dir, "local-root.lock")
	if err := ensurePromotionDirDurable(dir, 0o700); err != nil {
		return nil, fmt.Errorf("prepare local-root lock %s: %w", lockPath, err)
	}
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open local-root lock %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("local root is already in use by another mount (lock=%s): %w", lockPath, err)
	}
	if err := validatePromotionRootLock(file); err != nil {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("validate local-root lock %s: %w", lockPath, err)
	}
	return file, nil
}

func validatePromotionRootLock(file *os.File) error {
	if file == nil {
		return errors.New("local-root lock is not held")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fmt.Errorf("stat local-root lock: %w", err)
	}
	if stat.Nlink == 0 {
		return errors.New("local-root lock was unlinked")
	}
	return nil
}

func releasePromotionRootLock(file *os.File) {
	if file == nil {
		return
	}
	_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
	_ = file.Close()
}
