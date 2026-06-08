//go:build !windows

package cli

import (
	"fmt"

	drive9fuse "github.com/mem9-ai/dat9/pkg/fuse"
)

func mountFuseImpl(opts *mountFuseOptions) error {
	mode, err := toDrive9FuseSyncMode(opts.SyncMode)
	if err != nil {
		return err
	}
	writePolicy, err := toDrive9FuseWritePolicy(opts.WritePolicy)
	if err != nil {
		return err
	}

	return drive9fuse.Mount(&drive9fuse.MountOptions{
		Server:                  opts.Server,
		APIKey:                  opts.APIKey,
		Token:                   opts.Token,
		MountPoint:              opts.MountPoint,
		RemoteRoot:              opts.RemoteRoot,
		CacheDir:                opts.CacheDir,
		CacheSize:               opts.CacheSize,
		ReadCacheMaxFileBytes:   opts.ReadCacheMaxFileBytes,
		DiskReadCacheSize:       opts.DiskReadCacheSize,
		DiskReadCacheFreeRatio:  opts.DiskReadCacheFreeRatio,
		DirTTL:                  opts.DirTTL,
		AttrTTL:                 opts.AttrTTL,
		EntryTTL:                opts.EntryTTL,
		FlushDebounce:           opts.FlushDebounce,
		LookupRetryCount:        opts.LookupRetryCount,
		LookupRetryTimeout:      opts.LookupRetryTimeout,
		LegacyDirStatFallback:   opts.LegacyDirStatFallback,
		ReadDirPrefetch:         opts.ReadDirPrefetch,
		PrefetchMaxFiles:        opts.PrefetchMaxFiles,
		PrefetchMaxFileBytes:    opts.PrefetchMaxFileBytes,
		PrefetchMaxBytes:        opts.PrefetchMaxBytes,
		PrefetchTimeout:         opts.PrefetchTimeout,
		TrustLocalEvents:        opts.TrustLocalEvents,
		SyncMode:                mode,
		WritePolicy:             writePolicy,
		Profile:                 opts.Profile,
		LocalRoot:               opts.LocalRoot,
		LocalOnlyPatterns:       opts.LocalOnlyPatterns,
		RemoteOnlyPatterns:      opts.RemoteOnlyPatterns,
		PackPaths:               opts.PackPaths,
		UploadConcurrency:       opts.UploadConcurrency,
		ReadConcurrency:         opts.ReadConcurrency,
		ParallelReadConcurrency: opts.ParallelReadConcurrency,
		ParallelReadBlockSize:   opts.ParallelReadBlockSize,
		SyncRead:                opts.SyncRead,
		AllowOther:              opts.AllowOther,
		ReadOnly:                opts.ReadOnly,
		Debug:                   opts.Debug,
		PerfCounters:            opts.PerfCounters,
		EnableGitWorkspaces:     opts.LocalRoot != "",
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

func toDrive9FuseWritePolicy(policy fuseWritePolicy) (drive9fuse.WritePolicy, error) {
	switch policy {
	case fuseWritePolicyWriteBack:
		return drive9fuse.WritePolicyWriteBack, nil
	case fuseWritePolicyCloseSync:
		return drive9fuse.WritePolicyCloseSync, nil
	case fuseWritePolicyWriteSync:
		return drive9fuse.WritePolicyWriteSync, nil
	default:
		return drive9fuse.WritePolicyWriteBack, fmt.Errorf("unknown write policy %q", policy)
	}
}
