package cli

import (
	"fmt"
	"time"
)

type fuseSyncMode string
type fuseWritePolicy string
type fuseDurability string

const (
	fuseSyncModeAuto        fuseSyncMode = "auto"
	fuseSyncModeInteractive fuseSyncMode = "interactive"
	fuseSyncModeStrict      fuseSyncMode = "strict"

	fuseWritePolicyWriteBack fuseWritePolicy = "writeback"
	fuseWritePolicyCloseSync fuseWritePolicy = "close-sync"
	fuseWritePolicyWriteSync fuseWritePolicy = "write-sync"

	fuseDurabilityAuto        fuseDurability = "auto"
	fuseDurabilityInteractive fuseDurability = "interactive"
	fuseDurabilityFsync       fuseDurability = "fsync"
	fuseDurabilityCloseSync   fuseDurability = "close-sync"
	fuseDurabilityWriteSync   fuseDurability = "write-sync"
)

type mountFuseOptions struct {
	Server                string
	APIKey                string
	Token                 string
	MountPoint            string
	RemoteRoot            string
	CacheDir              string
	CacheSize             int64
	ReadCacheMaxFileBytes int64
	DirTTL                time.Duration
	AttrTTL               time.Duration
	EntryTTL              time.Duration
	FlushDebounce         time.Duration
	LookupRetryCount      int
	LookupRetryTimeout    time.Duration
	LegacyDirStatFallback bool
	ReadDirPrefetch       bool
	PrefetchMaxFiles      int
	PrefetchMaxFileBytes  int64
	PrefetchMaxBytes      int64
	PrefetchTimeout       time.Duration
	SyncMode              fuseSyncMode
	WritePolicy           fuseWritePolicy
	Profile               string
	UploadConcurrency     int
	ReadConcurrency       int
	SyncRead              bool
	AllowOther            bool
	ReadOnly              bool
	Debug                 bool
	PerfCounters          bool
	ProfileCPU            string
	ProfileHeap           string
	ProfileDir            string
	ProfileHeapInterval   time.Duration
	PprofAddr             string
	PerfSamplesPath       string
	PerfSampleInterval    time.Duration
	PerfMaxSamples        int
}

type vaultMountOptions struct {
	Server     string
	APIKey     string
	Token      string
	MountPoint string
	DirTTL     time.Duration
	AllowOther bool
	Debug      bool
}

var mountFuse = mountFuseImpl

var mountVault = mountVaultImpl

func parseFuseDurability(s string) (fuseSyncMode, fuseWritePolicy, error) {
	switch fuseDurability(s) {
	case fuseDurabilityAuto:
		return fuseSyncModeAuto, fuseWritePolicyWriteBack, nil
	case fuseDurabilityInteractive:
		return fuseSyncModeInteractive, fuseWritePolicyWriteBack, nil
	case fuseDurabilityFsync:
		return fuseSyncModeStrict, fuseWritePolicyWriteBack, nil
	case fuseDurabilityCloseSync:
		return fuseSyncModeStrict, fuseWritePolicyCloseSync, nil
	case fuseDurabilityWriteSync:
		return fuseSyncModeStrict, fuseWritePolicyWriteSync, nil
	default:
		return "", "", fmt.Errorf("unknown durability %q (valid: auto, interactive, fsync, close-sync, write-sync)", s)
	}
}
