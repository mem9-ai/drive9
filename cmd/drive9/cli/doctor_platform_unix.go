//go:build !windows

package cli

import (
	"os"
	"path/filepath"
	"syscall"
)

func doctorAccess(path string, mode uint32) error {
	return syscall.Access(path, mode)
}

func isMountpoint(path string) (bool, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return false, err
	}
	parent := filepath.Dir(abs)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, nil
	}
	parentStat, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, nil
	}
	return stat.Dev != parentStat.Dev || stat.Ino == parentStat.Ino, nil
}
