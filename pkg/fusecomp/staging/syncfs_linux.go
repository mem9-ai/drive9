//go:build linux

package staging

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncFilesystemDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = unix.Syncfs(int(d.Fd()))
	_ = d.Close()
	if err != nil {
		return errSyncfsUnsupported
	}
	return nil
}
