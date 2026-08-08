//go:build darwin

package migration

import (
	"os"
	"syscall"
)

func platformStatFields(info os.FileInfo) (uint64, uint64, int64, uint64, int64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, 0, 0, false
	}
	ctime := stat.Ctimespec.Sec*1_000_000_000 + stat.Ctimespec.Nsec
	return uint64(stat.Dev), stat.Ino, ctime, uint64(stat.Nlink), stat.Blocks, true
}

func platformVolumeSerial(string) (string, bool, error) {
	return "", false, nil
}
