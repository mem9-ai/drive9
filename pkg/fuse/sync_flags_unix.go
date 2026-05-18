//go:build !windows

package fuse

import "syscall"

func hasSyncOpenFlag(flags uint32) bool {
	syncFlags := uint32(syscall.O_SYNC | syscall.O_DSYNC)
	return flags&syncFlags != 0
}
