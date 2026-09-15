//go:build windows

package cli

import "errors"

var errUnmountSyscallUnsupportedCLI = errors.New("syscall unmount not supported on this platform")

func probeMountPointReadyCLI(mountPoint string) bool {
	return false
}

func mountPointStillActiveImpl(mountPoint string) bool {
	return false
}

func forceUnmountMountPointCLI(mountPoint string) {}

func unmountSyscallCLI(mountPoint string) error {
	_ = mountPoint
	return errUnmountSyscallUnsupportedCLI
}

func ensureCleanMountPointCLI(mountPoint string) (bool, error) {
	return false, nil
}
