//go:build windows

package fuse

func activeMountPoint(path string) (bool, error) {
	_ = path
	return false, nil
}
