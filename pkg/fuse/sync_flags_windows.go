//go:build windows

package fuse

func hasSyncOpenFlag(uint32) bool {
	return false
}
