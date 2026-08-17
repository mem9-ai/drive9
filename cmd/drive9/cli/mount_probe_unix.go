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
	active, err := drive9fuse.ActiveMountPointBounded(mountPoint)
	if err != nil {
		// Missing path → not mounted.
		if os.IsNotExist(err) {
			return false
		}
		// Timeout / ENOTCONN / unknown: treat as still active so umount does
		// not hang forever or forgive a failed fusermount as success.
		return true
	}
	return active
}

func forceUnmountMountPointCLI(mountPoint string) {
	drive9fuse.ForceUnmountLazy(mountPoint)
}

func ensureCleanMountPointCLI(mountPoint string) (bool, error) {
	return drive9fuse.EnsureCleanMountpoint(mountPoint)
}
