//go:build windows

package cli

func doctorAccess(path string, mode uint32) error {
	_ = path
	_ = mode
	return nil
}

func isMountpoint(path string) (bool, error) {
	_ = path
	return false, nil
}
