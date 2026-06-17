//go:build windows

package fuse

func readContinuousPerfProcessStats() continuousPerfProcessStats {
	return continuousPerfProcessStats{}
}
