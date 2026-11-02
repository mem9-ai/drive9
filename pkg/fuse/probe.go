package fuse

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// probeMountPointReadyTimeout bounds the readiness probe so a half-broken serve
// loop cannot hang startup forever on the fallback path.
const probeMountPointReadyTimeout = 5 * time.Second

// probeMountPointReady confirms the mountpoint is an active, usable mount after
// a Linux permission-like WaitMount error.
func probeMountPointReady(mountPoint string) error {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return fmt.Errorf("empty mountpoint")
	}

	done := make(chan error, 1)
	go func() {
		done <- probeMountPointReadyOnce(mountPoint)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(probeMountPointReadyTimeout):
		return fmt.Errorf("mountpoint readiness probe timed out after %s", probeMountPointReadyTimeout)
	}
}

func probeMountPointReadyOnce(mountPoint string) error {
	return probeMountPointReadyOnceWithMountCheck(mountPoint, activeMountPoint)
}

func probeMountPointReadyOnceWithMountCheck(mountPoint string, isActiveMountPoint func(string) (bool, error)) error {
	info, err := os.Stat(mountPoint)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("mountpoint is not a directory")
	}
	mounted, err := isActiveMountPoint(mountPoint)
	if err != nil {
		return err
	}
	if !mounted {
		return fmt.Errorf("mountpoint is not an active mount")
	}
	dir, err := os.Open(mountPoint)
	if err != nil {
		return err
	}
	var readErr error
	if _, err := dir.Readdirnames(1); err != nil && !errors.Is(err, io.EOF) {
		readErr = err
	}
	if err := dir.Close(); err != nil {
		return err
	}
	return readErr
}
