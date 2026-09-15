//go:build !windows

package cli

import (
	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
)

func probeMountPointReadyCLI(mountPoint string) bool {
	return drive9fuse.ProbeMountPointReady(mountPoint) == nil
}

func mountPointStillActiveImpl(mountPoint string) bool {
	// Authoritative kernel mount-table check: the previous stat-based probe
	// reported lazily (MNT_DETACH) detached mounts as inactive while their
	// kernel entry lingered until the last reference was reaped, letting
	// umount return success for a still-listed mount (#928).
	return drive9fuse.KernelMountTableHas(mountPoint)
}

func forceUnmountMountPointCLI(mountPoint string) {
	drive9fuse.ForceUnmountLazy(mountPoint)
}

func unmountSyscallCLI(mountPoint string) error {
	return drive9fuse.UnmountSyscall(mountPoint, false)
}

func ensureCleanMountPointCLI(mountPoint string) (bool, error) {
	return drive9fuse.EnsureCleanMountpoint(mountPoint)
}
