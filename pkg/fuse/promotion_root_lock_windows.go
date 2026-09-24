//go:build windows

package fuse

import (
	"errors"
	"os"
)

func acquirePromotionRootLock(_ string) (*os.File, error) {
	return nil, errors.New("synchronous promotion is not supported on Windows")
}

func validatePromotionRootLock(_ *os.File) error {
	return errors.New("synchronous promotion is not supported on Windows")
}

func releasePromotionRootLock(_ *os.File) {}
