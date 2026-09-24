//go:build !windows

package fuse

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

func validatePromotionPlatformMetadata(abs string, _ fs.FileInfo) error {
	size, err := unix.Listxattr(abs, nil)
	if err != nil {
		return fmt.Errorf("list xattr %s: %w", abs, err)
	}
	if size > 0 {
		buf := make([]byte, size)
		read, err := unix.Listxattr(abs, buf)
		if err != nil {
			return fmt.Errorf("read xattr list %s: %w", abs, err)
		}
		for _, raw := range bytes.Split(buf[:read], []byte{0}) {
			name := string(raw)
			if name == "" || (runtime.GOOS == "darwin" && name == "com.apple.provenance") {
				continue
			}
			return fmt.Errorf("xattr %s: %s", name, abs)
		}
	}
	return validatePromotionPlatformACL(abs)
}

func validatePromotionLocalMetadata(abs string, info fs.FileInfo, uid, gid uint32) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("missing stat metadata: %s", abs)
	}
	if stat.Uid != uid || stat.Gid != gid {
		return fmt.Errorf("foreign ownership: %s", abs)
	}
	if !info.IsDir() && stat.Nlink != 1 {
		return fmt.Errorf("hard link: %s", abs)
	}
	if info.Mode().IsRegular() && info.Size() > 0 && stat.Blocks*512 < info.Size() {
		return fmt.Errorf("sparse file: %s", abs)
	}
	return validatePromotionPlatformMetadata(abs, info)
}

func validatePromotionSameFilesystem(source, quarantineRoot string) error {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return err
	}
	quarantineInfo, err := os.Stat(quarantineRoot)
	if err != nil {
		return err
	}
	sourceStat, sourceOK := sourceInfo.Sys().(*syscall.Stat_t)
	quarantineStat, quarantineOK := quarantineInfo.Sys().(*syscall.Stat_t)
	if !sourceOK || !quarantineOK || sourceStat.Dev != quarantineStat.Dev {
		return errors.New("promotion source and quarantine are on different filesystems")
	}
	return nil
}

func samePromotionFile(before, after fs.FileInfo) bool {
	if before == nil || after == nil || before.Size() != after.Size() || before.ModTime() != after.ModTime() || before.Mode() != after.Mode() {
		return false
	}
	beforeStat, beforeOK := before.Sys().(*syscall.Stat_t)
	afterStat, afterOK := after.Sys().(*syscall.Stat_t)
	return beforeOK && afterOK && beforeStat.Ino == afterStat.Ino
}
