//go:build windows

package cli

func probeMountPointReadyCLI(mountPoint string) bool {
	return false
}

func mountPointStillActiveImpl(mountPoint string) bool {
	return false
}

func forceUnmountMountPointCLI(mountPoint string) {}

func ensureCleanMountPointCLI(mountPoint string) (bool, error) {
	return false, nil
}
