//go:build windows

package cli

import "errors"

var ErrWindowsSelfUpdateUnsupported = errors.New("windows self-update cannot replace the running drive9.exe; download the latest release and replace it from a separate process")

func preflightReplaceExecutableFile() error {
	return ErrWindowsSelfUpdateUnsupported
}

func replaceExecutableFile(newPath, targetPath string) error {
	return ErrWindowsSelfUpdateUnsupported
}

func updatePlatformUsageNote() string {
	return "\n  note: Windows self-update is not supported; download the latest release manually."
}
