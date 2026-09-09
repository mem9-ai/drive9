//go:build linux

package cli

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncMountFileSystem(mountPoint string) error {
	f, err := os.OpenFile(mountPoint, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return unix.Syncfs(int(f.Fd()))
}
