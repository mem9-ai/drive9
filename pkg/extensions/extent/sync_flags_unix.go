//go:build !windows

package extent

import "syscall"

// hasSyncOpenFlag reports O_SYNC/O_DSYNC opens: those demand per-write
// durable commits, so the buffered-image path never claims them.
func hasSyncOpenFlag(flags uint32) bool {
	syncFlags := uint32(syscall.O_SYNC | syscall.O_DSYNC)
	return flags&syncFlags != 0
}
