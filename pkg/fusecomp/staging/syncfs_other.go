//go:build !linux

package staging

func syncFilesystemDir(_ string) error {
	return errSyncfsUnsupported
}
