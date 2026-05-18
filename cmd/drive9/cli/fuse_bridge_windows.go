//go:build windows

package cli

import "fmt"

func mountFuseImpl(opts *mountFuseOptions) error {
	_ = opts
	return fmt.Errorf("drive9 mount: FUSE mounts are not supported on Windows; use `drive9 mount` without `--mode=fuse` on Windows (the default path uses WebDAV), or run drive9 mount on Linux or macOS")
}

func mountVaultImpl(opts *vaultMountOptions) error {
	_ = opts
	return fmt.Errorf("drive9 mount vault: FUSE mounts are not supported on Windows; use drive9 vault commands or the vault API on Windows, or use a Linux or macOS host for vault mounts")
}
