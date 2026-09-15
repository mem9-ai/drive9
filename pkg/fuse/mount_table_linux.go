//go:build linux

package fuse

import (
	"os"
	"syscall"
)

// kernelMountTableHas reports whether mountPoint still has an entry in the
// kernel mount table, read from /proc/self/mountinfo. This is the
// authoritative "is the unmount complete yet" check: after a lazy
// (MNT_DETACH) unmount the /etc/mtab entry is gone and path resolution falls
// through to the underlying directory, but the kernel keeps the entry listed
// until the mount's last reference is reaped.
func kernelMountTableHas(mountPoint string) (bool, error) {
	candidates := mountTableCandidates(mountPoint)
	if len(candidates) == 0 {
		return false, syscall.EINVAL
	}
	data, err := os.ReadFile(procMountInfoPath)
	if err != nil {
		return false, err
	}
	for _, mp := range parseMountInfoMountPoints(data) {
		if candidates[mp] {
			return true, nil
		}
	}
	return false, nil
}

// mountTableProbeFailed resolves kernelMountTableHas errors on Linux. The
// kernel mount table is the only authoritative source here, and it never
// stats the mountpoint itself — so any read failure (missing /proc, chroot,
// permission) is indeterminate and must fail closed: report the mount as
// still listed so callers never forgive an unreadable table as "cleared".
func mountTableProbeFailed(string, error) bool {
	return true
}

// unmountSyscall detaches mountPoint with umount2(2), bypassing fusermount's
// /etc/mtab bookkeeping. Once a lazy detach removed the mtab entry,
// fusermount refuses to touch the leftover kernel entry ("entry for ...
// not found in /etc/mtab"), while umount2 still works for the mount owner.
func unmountSyscall(mountPoint string, lazy bool) error {
	flags := 0
	if lazy {
		flags |= syscall.MNT_DETACH
	}
	return syscall.Unmount(mountPoint, flags)
}
