//go:build !windows

package cli

import "os"

func preflightReplaceExecutableFile() error {
	return nil
}

func replaceExecutableFile(newPath, targetPath string) error {
	return os.Rename(newPath, targetPath)
}

func updatePlatformUsageNote() string {
	return ""
}
