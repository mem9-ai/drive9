//go:build linux

package fuse

import "syscall"

func readOnlyDirectMountFlags() uintptr {
	// DirectMountFlags replaces go-fuse's MS_NODEV|MS_NOSUID defaults.
	return syscall.MS_NODEV | syscall.MS_NOSUID | syscall.MS_RDONLY
}
