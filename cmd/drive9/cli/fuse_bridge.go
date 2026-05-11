package cli

import "time"

type fuseSyncMode string

const (
	fuseSyncModeAuto        fuseSyncMode = "auto"
	fuseSyncModeInteractive fuseSyncMode = "interactive"
	fuseSyncModeStrict      fuseSyncMode = "strict"
)

type mountFuseOptions struct {
	Server                string
	APIKey                string
	Token                 string
	MountPoint            string
	RemoteRoot            string
	CacheDir              string
	CacheSize             int64
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
	Profile               string
	AllowOther            bool
	ReadOnly              bool
	Debug                 bool
	PerfCounters          bool
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

func parseFuseSyncMode(s string) (fuseSyncMode, error) {
	return parseFuseSyncModeImpl(s)
}
