//go:build windows

package cli

func probeMountPointReadyCLI(mountPoint string) bool {
	return false
}

func mountPointStillActiveImpl(mountPoint string) bool {
	return false
}
