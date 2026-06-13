//go:build !windows

package cli

import "os"

func replaceExecutableFile(newPath, targetPath string) error {
	return os.Rename(newPath, targetPath)
}
