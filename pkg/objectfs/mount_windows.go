//go:build windows

package objectfs

import "fmt"

// Mount is not available on Windows; object mounts use go-fuse.
func Mount(opts Options) error {
	_ = opts
	return fmt.Errorf("object-store mounts are not supported on Windows")
}
