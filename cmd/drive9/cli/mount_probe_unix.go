//go:build !windows

package cli

import (
	"os"

	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
)

func probeMountPointReadyCLI(mountPoint string) bool {
	return drive9fuse.ProbeMountPointReady(mountPoint) == nil
}

func mountPointStillActiveImpl(mountPoint string) bool {
	active, err := drive9fuse.ActiveMountPoint(mountPoint)
	if err != nil {
		// Missing path → not mounted. Broken FUSE (ENOTCONN etc.) is still
		// present and needs force-unmount cleanup, so treat as active.
		if os.IsNotExist(err) {
			return false
		}
		if drive9fuse.IsTransportBroken(err) {
			return true
		}
		return false
	}
	return active
}

func forceUnmountMountPointCLI(mountPoint string) {
	drive9fuse.ForceUnmountLazy(mountPoint)
}

func ensureCleanMountPointCLI(mountPoint string) (bool, error) {
	return drive9fuse.EnsureCleanMountpoint(mountPoint)
}
