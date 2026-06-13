//go:build windows

package cli

import "golang.org/x/sys/windows"

func replaceExecutableFile(newPath, targetPath string) error {
	from, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
