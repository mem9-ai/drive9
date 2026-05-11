//go:build !windows

package cli

import (
	"fmt"

	drive9fuse "github.com/mem9-ai/dat9/pkg/fuse"
)

func parseFuseSyncModeImpl(s string) (fuseSyncMode, error) {
	mode, err := drive9fuse.ParseSyncMode(s)
	if err != nil {
		return "", err
	}
	return fromDrive9FuseSyncMode(mode), nil
}

func mountFuseImpl(opts *mountFuseOptions) error {
	mode, err := toDrive9FuseSyncMode(opts.SyncMode)
	if err != nil {
		return err
	}

	return drive9fuse.Mount(&drive9fuse.MountOptions{
		Server:                opts.Server,
		APIKey:                opts.APIKey,
		Token:                 opts.Token,
		MountPoint:            opts.MountPoint,
		RemoteRoot:            opts.RemoteRoot,
		CacheDir:              opts.CacheDir,
		CacheSize:             opts.CacheSize,
		DirTTL:                opts.DirTTL,
		AttrTTL:               opts.AttrTTL,
		EntryTTL:              opts.EntryTTL,
		FlushDebounce:         opts.FlushDebounce,
		LookupRetryCount:      opts.LookupRetryCount,
		LookupRetryTimeout:    opts.LookupRetryTimeout,
		LegacyDirStatFallback: opts.LegacyDirStatFallback,
		ReadDirPrefetch:       opts.ReadDirPrefetch,
		PrefetchMaxFiles:      opts.PrefetchMaxFiles,
		PrefetchMaxFileBytes:  opts.PrefetchMaxFileBytes,
		PrefetchMaxBytes:      opts.PrefetchMaxBytes,
		PrefetchTimeout:       opts.PrefetchTimeout,
		SyncMode:              mode,
		Profile:               opts.Profile,
		AllowOther:            opts.AllowOther,
		ReadOnly:              opts.ReadOnly,
		Debug:                 opts.Debug,
		PerfCounters:          opts.PerfCounters,
	})
}

func mountVaultImpl(opts *vaultMountOptions) error {
	return drive9fuse.MountVault(&drive9fuse.VaultMountOptions{
		Server:     opts.Server,
		APIKey:     opts.APIKey,
		Token:      opts.Token,
		MountPoint: opts.MountPoint,
		DirTTL:     opts.DirTTL,
		AllowOther: opts.AllowOther,
		Debug:      opts.Debug,
	})
}

func fromDrive9FuseSyncMode(mode drive9fuse.SyncMode) fuseSyncMode {
	switch mode {
	case drive9fuse.SyncInteractive:
		return fuseSyncModeInteractive
	case drive9fuse.SyncStrict:
		return fuseSyncModeStrict
	default:
		return fuseSyncModeAuto
	}
}

func toDrive9FuseSyncMode(mode fuseSyncMode) (drive9fuse.SyncMode, error) {
	switch mode {
	case fuseSyncModeAuto:
		return drive9fuse.SyncAuto, nil
	case fuseSyncModeInteractive:
		return drive9fuse.SyncInteractive, nil
	case fuseSyncModeStrict:
		return drive9fuse.SyncStrict, nil
	default:
		return drive9fuse.SyncAuto, fmt.Errorf("unknown sync mode %q", mode)
	}
}
