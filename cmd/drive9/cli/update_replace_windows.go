//go:build windows

package cli

import "errors"

func replaceExecutableFile(newPath, targetPath string) error {
	return errors.New("windows self-update cannot replace the running drive9.exe; download the latest release and replace it from a separate process")
}
