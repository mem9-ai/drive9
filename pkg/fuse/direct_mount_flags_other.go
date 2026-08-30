//go:build !linux

package fuse

func readOnlyDirectMountFlags() uintptr {
	return 0
}
