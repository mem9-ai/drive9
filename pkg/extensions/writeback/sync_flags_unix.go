//go:build !windows

package writeback

import "syscall"

// hasSyncOpenFlag reports O_SYNC/O_DSYNC opens, which force per-Write
// commits regardless of the mount policy.
func hasSyncOpenFlag(flags uint32) bool {
	syncFlags := uint32(syscall.O_SYNC | syscall.O_DSYNC)
	return flags&syncFlags != 0
}
