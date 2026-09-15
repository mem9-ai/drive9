//go:build !linux

package fuse

import (
	"errors"
)

var errUnmountSyscallUnsupported = errors.New("syscall unmount not supported on this platform")

// kernelMountTableHas: platforms without /proc/self/mountinfo expose no
// authoritative kernel-table file, so fall back to the bounded stat probe.
// darwin's umount path is non-lazy, where the stat probe is equivalent.
func kernelMountTableHas(mountPoint string) (bool, error) {
	return activeMountPointBounded(mountPoint)
}

// unmountSyscall keeps the umount2 escalation a reported no-op outside
// Linux; the binary-based unmount ladder still applies.
func unmountSyscall(mountPoint string, lazy bool) error {
	_ = lazy
	return errUnmountSyscallUnsupported
}
