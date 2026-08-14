//go:build linux

package migration

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func platformStatFields(info os.FileInfo) (uint64, uint64, int64, uint64, int64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, 0, 0, false
	}
	ctime := stat.Ctim.Sec*1_000_000_000 + stat.Ctim.Nsec
	return uint64(stat.Dev), stat.Ino, ctime, uint64(stat.Nlink), stat.Blocks, true
}

func platformVolumeSerial(root string) (string, bool, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return "", false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", false, nil
	}
	base := fmt.Sprintf("/sys/dev/block/%d:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)))
	for _, suffix := range []string{"device/serial", "../device/serial", "../../device/serial"} {
		body, readErr := os.ReadFile(base + "/" + suffix)
		if readErr == nil {
			return string(body), true, nil
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			return "", false, readErr
		}
	}
	entries, readErr := os.ReadDir("/dev/disk/by-id")
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return "", false, readErr
	}
	for _, entry := range entries {
		if !strings.Contains(strings.ToLower(entry.Name()), "elastic_block_store") {
			continue
		}
		deviceInfo, statErr := os.Stat(filepath.Join("/dev/disk/by-id", entry.Name()))
		if statErr != nil {
			continue
		}
		deviceStat, ok := deviceInfo.Sys().(*syscall.Stat_t)
		if ok && uint64(deviceStat.Rdev) == uint64(stat.Dev) {
			return entry.Name(), true, nil
		}
	}
	return "", false, nil
}
