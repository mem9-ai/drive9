//go:build windows

package cli

import "fmt"

func parseFuseSyncModeImpl(s string) (fuseSyncMode, error) {
	switch s {
	case "", string(fuseSyncModeAuto):
		return fuseSyncModeAuto, nil
	case string(fuseSyncModeInteractive):
		return fuseSyncModeInteractive, nil
	case string(fuseSyncModeStrict):
		return fuseSyncModeStrict, nil
	default:
		return "", fmt.Errorf("unknown sync mode %q (valid: auto, interactive, strict)", s)
	}
}

func parseFuseWritePolicyImpl(s string) (fuseWritePolicy, error) {
	switch s {
	case "", string(fuseWritePolicyWriteBack):
		return fuseWritePolicyWriteBack, nil
	case string(fuseWritePolicyCloseSync):
		return fuseWritePolicyCloseSync, nil
	case string(fuseWritePolicyWriteSync):
		return fuseWritePolicyWriteSync, nil
	default:
		return "", fmt.Errorf("unknown write policy %q (valid: writeback, close-sync, write-sync)", s)
	}
}

func mountFuseImpl(opts *mountFuseOptions) error {
	_ = opts
	return fmt.Errorf("drive9 mount: FUSE mounts are not supported on Windows; use `drive9 mount` without `--mode=fuse` on Windows (the default path uses WebDAV), or run drive9 mount on Linux or macOS")
}

func mountVaultImpl(opts *vaultMountOptions) error {
	_ = opts
	return fmt.Errorf("drive9 mount vault: FUSE mounts are not supported on Windows; use drive9 vault commands or the vault API on Windows, or use a Linux or macOS host for vault mounts")
}
