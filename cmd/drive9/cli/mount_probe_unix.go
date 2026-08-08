//go:build !windows

package cli

import drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"

func probeMountPointReadyCLI(mountPoint string) bool {
	return drive9fuse.ProbeMountPointReady(mountPoint) == nil
}

func mountPointStillActiveImpl(mountPoint string) bool {
	active, err := drive9fuse.ActiveMountPoint(mountPoint)
	if err != nil {
		// Stat failures (including ENOTCONN/missing) → treat as not active.
		return false
	}
	return active
}
