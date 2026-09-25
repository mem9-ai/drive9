package cli

import (
	"fmt"
	"strings"
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
	Server                       string
	APIKey                       string
	Token                        string
	MountPoint                   string
	RemoteRoot                   string
	CacheDir                     string
	CacheSize                    int64
	ReadCacheMaxFileBytes        int64
	ReadCacheTTL                 time.Duration
	DiskReadCacheSize            int64
	DiskReadCacheFreeRatio       float64
	DirTTL                       time.Duration
	AttrTTL                      time.Duration
	EntryTTL                     time.Duration
	FlushDebounce                time.Duration
	LookupRetryCount             int
	LookupRetryTimeout           time.Duration
	LegacyDirStatFallback        bool
	ReadDirPrefetch              bool
	PrefetchMaxFiles             int
	PrefetchMaxFileBytes         int64
	PrefetchMaxBytes             int64
	PrefetchTimeout              time.Duration
	TrustLocalEvents             bool
	SyncMode                     fuseSyncMode
	WritePolicy                  fuseWritePolicy
	WritebackLazyStaging         bool
	WritebackSyncWindow          time.Duration
	Profile                      string
	LayerRef                     string
	CheckpointRef                string
	LocalRoot                    string
	LocalOnlyPatterns            []string
	LocalGitignoreAwarePatterns  []string
	RemoteOnlyPatterns           []string
	AppendLogPatterns            []string
	PackPaths                    []string
	ExtentPaths                  []string
	UploadConcurrency            int
	DirCacheMaxEntries           int
	CommitQueueMaxPending        int
	DeferredUnlink               bool
	WriteBackBatchWindow         time.Duration
	WriteBackBatchMaxFiles       int
	WriteBackBatchMaxBytes       int64
	WriteCacheFreeRatio          float64
	WriteCacheSizeMB             int64
	ReadConcurrency              int
	ParallelReadConcurrency      int
	ParallelReadBlockSize        int64
	SyncRead                     bool
	DirectMountStrict            bool
	GVisorCompat                 bool
	LegacyInterruptibleMutations bool
	AllowOther                   bool
	ReadOnly                     bool
	Debug                        bool
	PerfCounters                 bool
	ProfileCPUDuration           time.Duration
	ProfileCPUInterval           time.Duration
	ProfileHeap                  string
	ProfileDir                   string
	ProfileHeapInterval          time.Duration
	PprofAddr                    string
	PerfSamplesPath              string
	PerfSampleInterval           time.Duration
	PerfMaxSamples               int
	PerfMaxSampleFiles           int
	PerfMaxProfileFiles          int
	Supervised                   bool
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

// parseWritebackSyncWindow parses --writeback-sync-window (issue #964):
// "close" -> legacy fsync-at-close staging; "0"/"off" -> lazy with no
// background syncer; otherwise a duration -> lazy with a bounded syncer.
func parseWritebackSyncWindow(v string) (lazy bool, window time.Duration, err error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "close":
		return false, 0, nil
	case "0", "off", "none":
		return true, 0, nil
	default:
		d, perr := time.ParseDuration(v)
		if perr != nil || d < 0 {
			return false, 0, fmt.Errorf("invalid --writeback-sync-window %q (use a duration, \"0\"/\"off\", or \"close\")", v)
		}
		return true, d, nil
	}
}

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
