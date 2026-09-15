//go:build !linux

package fuse

import (
	"errors"
	"os"
)

var errUnmountSyscallUnsupported = errors.New("syscall unmount not supported on this platform")

// kernelMountTableHas: platforms without /proc/self/mountinfo expose no
// authoritative kernel-table file, so fall back to the bounded stat probe.
// darwin's umount path is non-lazy, where the stat probe is equivalent.
func kernelMountTableHas(mountPoint string) (bool, error) {
	return activeMountPointBounded(mountPoint)
}

// mountTableProbeFailed resolves kernelMountTableHas errors on platforms
// without a kernel-table file, where the probe stats the mountpoint itself:
// a missing path is genuinely not mounted; other failures fall back to the
// bounded stat probe again and stay conservative on unknowns.
func mountTableProbeFailed(mountPoint string, err error) bool {
	if os.IsNotExist(err) {
		return false
	}
	active, aerr := activeMountPointBounded(mountPoint)
	if aerr != nil {
		return !os.IsNotExist(aerr)
	}
	return active
}

// unmountSyscall keeps the umount2 escalation a reported no-op outside
// Linux; the binary-based unmount ladder still applies.
func unmountSyscall(mountPoint string, lazy bool) error {
	_ = lazy
	return errUnmountSyscallUnsupported
}
