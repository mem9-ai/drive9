//go:build windows

package fuse

import (
	"errors"
	"io/fs"
)

func validatePromotionPlatformMetadata(_ string, _ fs.FileInfo) error {
	return errors.New("synchronous promotion is not supported on Windows")
}

func validatePromotionLocalMetadata(_ string, _ fs.FileInfo, _, _ uint32) error {
	return errors.New("synchronous promotion is not supported on Windows")
}

func validatePromotionSameFilesystem(_, _ string) error {
	return errors.New("synchronous promotion is not supported on Windows")
}

func samePromotionFile(_, _ fs.FileInfo) bool {
	return false
}
