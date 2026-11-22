package fuse

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// ProfilingOptions is the profiling knob bundle (perf.ProfilingOptions),
// kept as a named type for the stable MountOptions API.
type ProfilingOptions = perf.ProfilingOptions

// MountConfig carries the mount inputs the fuse stack accepts.
type MountConfig struct {
	Server     string
	APIKey     string
	Token      string
	MountPoint          string
	RemoteRoot          string
	RemoteRootDefaulted bool

	CacheDir              string
	CacheSize             int64
	ReadCacheMaxFileBytes int64
	ReadCacheTTL          time.Duration
	DiskReadCacheSize     int64
	// ExtentCacheSize is the extent plane's disk cache budget in bytes.
	// The three disk budgets are independent:
	//
	//   DiskReadCacheSize  classic read cache
	//   WriteCacheSizeMB   classic writeback staging
	//   ExtentCacheSize    extent plane (read cache + writeback staging)
	//
	// JuiceFS accounts its read cache and its writeback staging in ONE
	// CacheSize, so the extent plane takes a single number: the engine's
	// total retained disk bytes never exceed it, while the split between
	// cached reads and staged writes stays JuiceFS's own LRU decision.
	ExtentCacheSize        int64
	DiskReadCacheFreeRatio float64

	DirTTL           time.Duration
	AttrTTL          time.Duration
	EntryTTL         time.Duration
	NegativeEntryTTL time.Duration

	FlushDebounce         time.Duration
	LookupRetryCount      int
	LookupRetryTimeout    time.Duration
	LegacyDirStatFallback bool
	ReadDirPrefetch       bool
	PrefetchMaxFiles      int
	PrefetchMaxFileBytes  int64
	PrefetchMaxBytes      int64
	PrefetchTimeout       time.Duration

	TrustLocalEvents bool
	SyncMode         string
	WritePolicy      string

	WritebackLazyStaging bool
	WritebackSyncWindow  time.Duration

	Profile  string
	LayerRef string

	LocalRoot                   string
	LocalOnlyPatterns           []string
	LocalGitignoreAwarePatterns []string
	RemoteOnlyPatterns          []string
	// LocalizeQuotaBytes/LocalizeFreeRatio bound the local-only overlay's
	// host-disk usage (0 disables either bound).
	LocalizeQuotaBytes int64
	LocalizeFreeRatio  float64

	AppendLogPatterns []string
	PackPaths         []string
	// ExtentPaths are path globs whose fresh creates ride the JuiceFS
	// engine plane from byte zero (content_layout=extent); the builtin
	// "extent" profile sets ["*"]. Default mounts stay hybrid — creation
	// on the classic plane, large files promoted to extent at the inline
	// cutoff.
	ExtentPaths []string

	UploadConcurrency     int
	DirCacheMaxEntries    int
	CommitQueueMaxPending int

	WriteBackBatchWindow   time.Duration
	WriteBackBatchMaxFiles int
	WriteBackBatchMaxBytes int64
	WriteCacheFreeRatio    float64
	WriteCacheSizeMB       int64
	// MemoryBudget caps the process-wide in-memory footprint in bytes (see
	// MountOptions.MemoryBudget): 0 = auto (cgroup-derived), negative =
	// disabled, positive = explicit bytes.
	MemoryBudget     int64
	MemoryBudgetMode string

	ReadConcurrency         int
	ParallelReadConcurrency int
	ParallelReadBlockSize   int64
	SyncRead                bool

	DirectMountStrict            bool
	GVisorCompat                 bool
	LegacyInterruptibleMutations bool

	AllowOther bool
	ReadOnly   bool
	Debug      bool

	// EnableGitWorkspaces gates the git extension's workspace discovery
	// (v1 default: off; overlay mounts turn it on).
	EnableGitWorkspaces bool
	// DisableBuiltinOverlayDefaults stops the profile's built-in local-only
	// defaults from being merged into the pattern lists (v1 parity).
	DisableBuiltinOverlayDefaults bool
	// RemoteCommitWaitTimeout bounds metadata-op waits for background commits
	// (default 5s; 0 uses default; negative disables the bound).
	RemoteCommitWaitTimeout time.Duration
	// SkipProcessState skips the mount pid/process state file (independent of
	// Supervised — v1 had both knobs).
	SkipProcessState bool
	// OmitProcessStateCredentials keeps API keys, delegated tokens, and the
	// potentially credential-bearing server endpoint out of the process-state
	// file while retaining the PID and control socket needed by drain and
	// unmount. Callers using this mode cannot use credential-backed post-unmount
	// helpers such as auto-pack.
	OmitProcessStateCredentials bool
	// DefaultOwner, when set, is the uid/gid reported for files that carry
	// no stored owner (normally the mounting process). A sandbox uses it to
	// present the workspace as owned by its unprivileged application user.
	DefaultOwner *Owner

	PerfCounters bool

	ProfileCPUDuration  time.Duration
	ProfileCPUInterval  time.Duration
	ProfileHeap         string
	ProfileDir          string
	ProfileHeapInterval time.Duration
	PprofAddr           string
	PerfSamplesPath     string
	PerfSampleInterval  time.Duration
	PerfMaxSamples      int
	PerfMaxSampleFiles  int
	PerfMaxProfileFiles int

	Supervised bool

	// Extensions lists the extensions to wire in build-up order: core
	// first, each subsequent entry wrapping the ones before it — the chain
	// assembly order is the reverse, ending with core as the terminal (the
	// caller guarantees core is present). Nil/empty wires the default full
	// set, byte-identical to the pre-flag behavior.
	Extensions []string
}

func (cfg MountConfig) toProfilingOptions() perf.ProfilingOptions {
	return perf.ProfilingOptions{
		CPUProfileDuration:  cfg.ProfileCPUDuration,
		CPUProfileInterval:  cfg.ProfileCPUInterval,
		HeapProfilePath:     cfg.ProfileHeap,
		ProfileDir:          cfg.ProfileDir,
		HeapProfileInterval: cfg.ProfileHeapInterval,
		PprofAddr:           cfg.PprofAddr,
		PerfSamplesPath:     cfg.PerfSamplesPath,
		PerfSampleInterval:  cfg.PerfSampleInterval,
		PerfMaxSamples:      cfg.PerfMaxSamples,
		PerfMaxSampleFiles:  cfg.PerfMaxSampleFiles,
		PerfMaxProfileFiles: cfg.PerfMaxProfileFiles,
	}
}

// defaultExtensionOrder is the default --extensions value, in build-up
// order: core first, each subsequent entry wrapping the ones before it, so
// the chain assembly order (outermost first) is the reverse of this list.
// It is the exact reverse of the historical wired chain, so the default
// wires byte-identically to the pre-flag code.
var defaultExtensionOrder = []string{
	"core", "sse", "writeback", "xattr", "specials", "extent",
	"appendlog", "git", "localize", "renameguard", "cache",
}

// knownExtensionNames guards --extensions against typos (an unknown name
// would otherwise silently not wire).
var knownExtensionNames = map[string]bool{
	"core": true, "sse": true, "extent": true, "writeback": true,
	"xattr": true, "specials": true, "appendlog": true,
	"git": true, "localize": true, "renameguard": true,
	"cache": true,
}

// parseExtensionOrder validates the --extensions list and returns it in
// flag order (build-up; the chain is assembled as its reverse, with core as
// the terminal — the caller guarantees core is present). Empty input means
// the default full set.
func parseExtensionOrder(list []string) ([]string, error) {
	if len(list) == 0 {
		return defaultExtensionOrder, nil
	}
	out := make([]string, 0, len(list))
	seen := map[string]bool{}
	for _, raw := range list {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if !knownExtensionNames[name] {
			return nil, fmt.Errorf("fuse: unknown extension %q (valid: %s)", name, strings.Join(defaultExtensionOrder, ","))
		}
		if seen[name] {
			return nil, fmt.Errorf("fuse: duplicate extension %q", name)
		}
		seen[name] = true
		out = append(out, name)
	}
	if len(out) == 0 {
		return defaultExtensionOrder, nil
	}
	// Reverse assembly requires core as the FIRST build-up entry so it
	// becomes the terminal handler; a list like "cache,core" assembles
	// [core, cache] and core never calls next — the cache is silently
	// unreachable.
	if out[0] != "core" {
		return nil, fmt.Errorf("fuse: --extensions must start with core (the chain terminal), got %q first", out[0])
	}
	return out, nil
}

// extentPromotionCutoff decides the write plane's promotion line: with
// explicit [extent] patterns the layout is pattern-owned (a staged image
// promotes only when its PATH matches — see extentRoute; the cutoff is
// then 0 so size alone never promotes); pattern-less hybrid mounts
// promote at the server's inline cutoff.
func extentPromotionCutoff(patternConfigured bool, cl *client.Client) int64 {
	if patternConfigured {
		return 0
	}
	return extentInlineThreshold(cl)
}

// extentHybridCutoff resolves write-plane size promotion: pattern-less hybrid
// mounts and the default coding-agent profile (whose SQLite globs only pull
// database files into extent at size 0) still promote large non-matching files
// at the server's inline cutoff instead of falling back to multipart. Explicit
// non-coding-agent pattern mounts preserve pattern-only promotion via
// extentPromotionCutoff.
func extentHybridCutoff(profile string, patternConfigured bool, cl *client.Client) int64 {
	if patternConfigured && profile != "coding-agent" {
		return extentPromotionCutoff(true, cl)
	}
	return extentPromotionCutoff(false, cl)
}

// extentInlineThreshold returns the server's inline cutoff (the old
// multipart upgrade line): staged images at or above it become extent.
func extentInlineThreshold(cl *client.Client) int64 {
	if t := cl.CachedSmallFileThreshold(); t > 0 {
		return t
	}
	// The status cache may not be warm at construction time; ask once with
	// a short budget. Fallback: the documented default inline cutoff.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cl.Warm(ctx)
	if t := cl.CachedSmallFileThreshold(); t > 0 {
		return t
	}
	return 50_000
}

// SyncMode controls when Fsync is considered durable.
type SyncMode int

const (
	// SyncAuto measures RTT at mount time. RTT > 10ms → interactive, else strict.
	SyncAuto SyncMode = iota
	// SyncInteractive: Fsync = local journal durable only. Remote commit is async.
	SyncInteractive
	// SyncStrict: Fsync = remote durable. Blocks until remote PUT succeeds.
	SyncStrict
)

// WritePolicy controls when ordinary writes become remote-durable.
type WritePolicy string

const (
	// WritePolicyWriteBack preserves the existing behavior: writes are staged
	// locally and remote upload may happen asynchronously after close.
	WritePolicyWriteBack WritePolicy = "writeback"
	// WritePolicyCloseSync makes close remote-durable by forcing cloud upload
	// from Flush, the FUSE operation whose status propagates to close(2).
	WritePolicyCloseSync WritePolicy = "close-sync"
	// WritePolicyWriteSync makes each Write operation remote-durable before
	// returning success. This is intentionally expensive.
	WritePolicyWriteSync WritePolicy = "write-sync"
)

// String returns the write policy name for CLI/logging.
func (p WritePolicy) String() string {
	switch p {
	case WritePolicyWriteBack:
		return string(WritePolicyWriteBack)
	case WritePolicyCloseSync:
		return string(WritePolicyCloseSync)
	case WritePolicyWriteSync:
		return string(WritePolicyWriteSync)
	default:
		return string(WritePolicyWriteBack)
	}
}

// MountOptions is the stable mount API (the pre-extensions pkg/fuse shape,
// byte-for-byte compatible with main), plus Extensions for the chain's
// build-up ordering. Mount converts it to the internal MountConfig — the
// wire-level semantics are unchanged.
type MountOptions struct {
	Server                 string        // drive9 server URL
	APIKey                 string        // owner API key (mutually exclusive with Token)
	Token                  string        // delegated capability JWT (mutually exclusive with APIKey)
	MountPoint             string        // local mount point
	RemoteRoot             string        // remote subtree root (default "/" or layer BaseRootPath)
	RemoteRootDefaulted    bool          // true when CLI did not receive an explicit :/remote argument
	CacheDir               string        // write-back cache directory (default ~/.cache/drive9)
	CacheSize              int64         // ReadCache max size in bytes (default 128MB)
	ReadCacheMaxFileBytes  int64         // largest single file admitted to ReadCache (default 4MiB)
	ReadCacheTTL           time.Duration // ReadCache TTL (default 30s)
	DiskReadCacheSize      int64         // disk read cache max size in bytes (default 1GiB)
	DiskReadCacheFreeRatio float64       // min free-space ratio before disk read cache evicts (default 0.10)
	DirTTL                 time.Duration // DirCache TTL (default 10s)
	AttrTTL                time.Duration // kernel attr cache TTL (default 60s)
	EntryTTL               time.Duration // kernel entry cache TTL (default 60s)
	NegativeEntryTTL       time.Duration // kernel negative entry cache TTL (default 1s)
	FlushDebounce          time.Duration // debounce window for small-file flush coalescing (default 2s; -1 uses default)
	SyncMode               SyncMode      // interactive, strict, or auto (default auto)
	WritePolicy            WritePolicy   // writeback, close-sync, or write-sync (default writeback)
	Profile                string        // mount profile name
	LayerRef               string        // writable or read-only fs layer ref
	LocalRoot              string        // local-only overlay root
	// LocalizeQuotaBytes caps the bytes the local-only overlay may occupy on
	// the host disk (0 disables); LocalizeFreeRatio is the host free-space
	// ratio below which local writes fail with ENOSPC (0 disables).
	LocalizeQuotaBytes          int64
	LocalizeFreeRatio           float64
	LocalOnlyPatterns           []string // local-only path patterns
	LocalGitignoreAwarePatterns []string // local-only patterns gated by Git ignore rules
	RemoteOnlyPatterns          []string // remote-persistent override patterns
	// DisableBuiltinOverlayDefaults stops the built-in local-only defaults for
	// Profile from being added on top of the given patterns. The CLI resolves
	// the effective profile itself and sets this; direct library callers leave
	// it false and keep the defaults.
	DisableBuiltinOverlayDefaults bool
	AppendLogPatterns             []string // remote-persistent files eligible for append-log synchronization
	PackPaths                     []string // local overlay paths auto-packed after unmount
	ExtentPaths                   []string // path globs created as content_layout=extent (--extent / [extent])
	CommitQueueMaxPending         int      // CommitQueue backpressure bound (default 100; 0 default)
	WriteBackBatchWindow          time.Duration
	WriteBackBatchMaxFiles        int
	WriteBackBatchMaxBytes        int64
	WriteCacheFreeRatio           float64
	WriteCacheSizeMB              int64
	// MemoryBudget caps the process-wide in-memory footprint (read cache,
	// write-back data cache, per-handle write buffers, inode table) in bytes.
	// 0 means AUTO: 60% of the container's cgroup memory limit, and disabled on
	// a bare host (no limit to derive). A negative value disables the budget.
	// MemoryBudgetMode selects what an over-budget allocation means: "" or
	// "enforce" refuses it (caches degrade, writes fail with ENOMEM), "warn"
	// admits and logs it.
	MemoryBudget     int64
	MemoryBudgetMode string
	// ExtentCacheSize is the extent plane's disk cache budget (bytes;
	// read cache and writeback staging share it).
	ExtentCacheSize              int64
	UploadConcurrency            int           // background upload workers (default 16)
	ReadConcurrency              int           // concurrent backend reads (default 24)
	ParallelReadConcurrency      int           // concurrent block reads per large FUSE read (default 4)
	ParallelReadBlockSize        int64         // block size for parallel reads (default 1MiB)
	SyncRead                     bool          // disable kernel async read dispatch
	DirectMountStrict            bool          // Linux only: mount(2) without fusermount fallback
	GVisorCompat                 bool          // gVisor FUSE compatibility behavior
	LegacyInterruptibleMutations bool          // FUSE interrupt cancels in-flight remote commits (legacy)
	LookupRetryCount             int           // detached retries after transient stat failures (default 2)
	LookupRetryTimeout           time.Duration // timeout per detached stat retry (default 250ms)
	LegacyDirStatFallback        bool          // list parent when directory stat 404s (legacy servers)
	ReadDirPrefetch              bool
	PrefetchMaxFiles             int
	PrefetchMaxFileBytes         int64
	PrefetchMaxBytes             int64
	PrefetchTimeout              time.Duration
	DirCacheMaxEntries           int
	TrustLocalEvents             bool
	AllowOther                   bool
	ReadOnly                     bool
	Debug                        bool
	PerfCounters                 bool
	EnableGitWorkspaces          bool // enable git workspace overlay discovery (default off)
	Profiling                    ProfilingOptions
	WritebackLazyStaging         bool
	WritebackSyncWindow          time.Duration
	// RemoteCommitWaitTimeout bounds how long a FUSE write/flush handler waits
	// for a background commit before proceeding anyway (default 5s; 0 uses
	// default; negative disables the timeout).
	RemoteCommitWaitTimeout time.Duration
	// SkipProcessState skips writing the mount pid/process state file.
	SkipProcessState bool
	// OmitProcessStateCredentials blanks secrets from the process state and
	// exit records; DefaultOwner overrides the mount's reported uid/gid.
	OmitProcessStateCredentials bool
	DefaultOwner                *Owner
	// Supervised marks this worker as managed by an external supervisor.
	Supervised bool
	// Extensions lists the extensions to wire in build-up order (core first).
	// Nil/empty wires the default full set.
	Extensions []string
}

// syncModeString maps the typed SyncMode onto the internal config value.
func (m SyncMode) configString() string {
	switch m {
	case SyncInteractive:
		return "interactive"
	case SyncStrict:
		return "strict"
	default:
		return "auto"
	}
}

// Mount assembles the fuse stack from the stable MountOptions and serves
// until unmounted. It only converts options into the internal MountConfig
// (see MountWithConfig) — behavior is byte-identical.
func Mount(opts *MountOptions) error {
	if opts == nil {
		return fmt.Errorf("fuse: nil MountOptions")
	}
	return MountWithConfig(mountConfigFromOptions(opts))
}

// mountConfigFromOptions is the public-options to internal-config
// conversion; kept separate so tests pin new option wiring.
func mountConfigFromOptions(opts *MountOptions) MountConfig {
	return MountConfig{
		Server:                        opts.Server,
		APIKey:                        opts.APIKey,
		Token:                         opts.Token,
		MountPoint:                    opts.MountPoint,
		RemoteRoot:                    opts.RemoteRoot,
		RemoteRootDefaulted:           opts.RemoteRootDefaulted,
		CacheDir:                      opts.CacheDir,
		CacheSize:                     opts.CacheSize,
		ReadCacheMaxFileBytes:         opts.ReadCacheMaxFileBytes,
		ReadCacheTTL:                  opts.ReadCacheTTL,
		DiskReadCacheSize:             opts.DiskReadCacheSize,
		ExtentCacheSize:               opts.ExtentCacheSize,
		DiskReadCacheFreeRatio:        opts.DiskReadCacheFreeRatio,
		DirTTL:                        opts.DirTTL,
		AttrTTL:                       opts.AttrTTL,
		EntryTTL:                      opts.EntryTTL,
		NegativeEntryTTL:              opts.NegativeEntryTTL,
		FlushDebounce:                 opts.FlushDebounce,
		LookupRetryCount:              opts.LookupRetryCount,
		LookupRetryTimeout:            opts.LookupRetryTimeout,
		LegacyDirStatFallback:         opts.LegacyDirStatFallback,
		ReadDirPrefetch:               opts.ReadDirPrefetch,
		PrefetchMaxFiles:              opts.PrefetchMaxFiles,
		PrefetchMaxFileBytes:          opts.PrefetchMaxFileBytes,
		PrefetchMaxBytes:              opts.PrefetchMaxBytes,
		PrefetchTimeout:               opts.PrefetchTimeout,
		TrustLocalEvents:              opts.TrustLocalEvents,
		SyncMode:                      opts.SyncMode.configString(),
		WritePolicy:                   opts.WritePolicy.String(),
		WritebackLazyStaging:          opts.WritebackLazyStaging,
		WritebackSyncWindow:           opts.WritebackSyncWindow,
		Profile:                       opts.Profile,
		LayerRef:                      opts.LayerRef,
		LocalRoot:                     opts.LocalRoot,
		LocalizeQuotaBytes:            opts.LocalizeQuotaBytes,
		LocalizeFreeRatio:             opts.LocalizeFreeRatio,
		LocalOnlyPatterns:             opts.LocalOnlyPatterns,
		LocalGitignoreAwarePatterns:   opts.LocalGitignoreAwarePatterns,
		RemoteOnlyPatterns:            opts.RemoteOnlyPatterns,
		DisableBuiltinOverlayDefaults: opts.DisableBuiltinOverlayDefaults,
		AppendLogPatterns:             opts.AppendLogPatterns,
		PackPaths:                     opts.PackPaths,
		ExtentPaths:                   opts.ExtentPaths,
		UploadConcurrency:             opts.UploadConcurrency,
		DirCacheMaxEntries:            opts.DirCacheMaxEntries,
		CommitQueueMaxPending:         opts.CommitQueueMaxPending,
		WriteBackBatchWindow:          opts.WriteBackBatchWindow,
		WriteBackBatchMaxFiles:        opts.WriteBackBatchMaxFiles,
		WriteBackBatchMaxBytes:        opts.WriteBackBatchMaxBytes,
		WriteCacheFreeRatio:           opts.WriteCacheFreeRatio,
		WriteCacheSizeMB:              opts.WriteCacheSizeMB,
		MemoryBudget:                  opts.MemoryBudget,
		MemoryBudgetMode:              opts.MemoryBudgetMode,
		ReadConcurrency:               opts.ReadConcurrency,
		ParallelReadConcurrency:       opts.ParallelReadConcurrency,
		ParallelReadBlockSize:         opts.ParallelReadBlockSize,
		SyncRead:                      opts.SyncRead,
		DirectMountStrict:             opts.DirectMountStrict,
		GVisorCompat:                  opts.GVisorCompat,
		LegacyInterruptibleMutations:  opts.LegacyInterruptibleMutations,
		AllowOther:                    opts.AllowOther,
		ReadOnly:                      opts.ReadOnly,
		Debug:                         opts.Debug,
		PerfCounters:                  opts.PerfCounters,
		EnableGitWorkspaces:           opts.EnableGitWorkspaces,
		ProfileCPUDuration:            opts.Profiling.CPUProfileDuration,
		ProfileCPUInterval:            opts.Profiling.CPUProfileInterval,
		ProfileHeap:                   opts.Profiling.HeapProfilePath,
		ProfileDir:                    opts.Profiling.ProfileDir,
		ProfileHeapInterval:           opts.Profiling.HeapProfileInterval,
		PprofAddr:                     opts.Profiling.PprofAddr,
		PerfSamplesPath:               opts.Profiling.PerfSamplesPath,
		PerfSampleInterval:            opts.Profiling.PerfSampleInterval,
		PerfMaxSamples:                opts.Profiling.PerfMaxSamples,
		PerfMaxSampleFiles:            opts.Profiling.PerfMaxSampleFiles,
		PerfMaxProfileFiles:           opts.Profiling.PerfMaxProfileFiles,
		RemoteCommitWaitTimeout:       opts.RemoteCommitWaitTimeout,
		SkipProcessState:              opts.SkipProcessState,
		OmitProcessStateCredentials:   opts.OmitProcessStateCredentials,
		DefaultOwner:                  opts.DefaultOwner,
		Supervised:                    opts.Supervised,
		Extensions:                    opts.Extensions,
	}
}

// resolveKernelTTLDefaults ports v1 setDefaults' profile-dependent kernel
// cache TTLs (kept as a named function so white-box tests pin the profile
// rows): 60s attr/entry, 30s attr on coding-agent, interactive drops
// attr/entry to 1s and listings to 2s; explicit (positive) values win.
func resolveKernelTTLDefaults(profile string, attrTTL, entryTTL, dirTTL time.Duration) (time.Duration, time.Duration, time.Duration) {
	if attrTTL <= 0 {
		attrTTL = 60 * time.Second
		if profile == "coding-agent" {
			attrTTL = 30 * time.Second
		}
		if profile == "interactive" {
			attrTTL = 1 * time.Second
		}
	}
	if entryTTL <= 0 {
		entryTTL = 60 * time.Second
		if profile == "interactive" {
			entryTTL = 1 * time.Second
		}
	}
	if dirTTL <= 0 {
		dirTTL = 10 * time.Second
		if profile == "interactive" {
			dirTTL = 2 * time.Second
		}
	}
	return attrTTL, entryTTL, dirTTL
}

// resolveWriteCacheSizeMB ports the v1 write-cache quota knob semantics:
// zero uses the 1024 MiB default, negative disables the byte quota (0).
func resolveWriteCacheSizeMB(sizeMB int64) int64 {
	if sizeMB == 0 {
		return 1024
	}
	if sizeMB < 0 {
		return 0
	}
	return sizeMB
}

// MountWithConfig assembles the fuse stack from the raw internal config and
// serves until unmounted. Mount(MountOptions) is the stable entry; this form
// exists for the CLI bridge and white-box tests that already hold a config.
func MountWithConfig(cfg MountConfig) error {
	err := mountWithConfig(cfg)
	if err != nil && cfg.OmitProcessStateCredentials {
		err = redactCredentialError(err, cfg)
	}
	return err
}

// buildMountProcessState assembles the pidfile record, blanking the
// credential-bearing fields when the caller asked them to stay out of the
// process state.
func buildMountProcessState(remoteRoot, profile, localRoot, server, apiKey, token string,
	packPaths []string, prof ProfilingOptions, supervised, omit bool,
	mountPoint string, creationTime uint64, credentialKind, controlSock string, startedAt time.Time) mountstate.ProcessState {
	if omit {
		server, apiKey, token = "", "", ""
	}
	return mountstate.ProcessState{
		PID:                 os.Getpid(),
		CreationTime:        creationTime,
		Component:           "drive9-fuse",
		MountKind:           mountstate.MountKindFUSE,
		MountPoint:          mountPoint,
		RemoteRoot:          remoteRoot,
		Profile:             profile,
		LocalRoot:           localRoot,
		Server:              server,
		PackPaths:           append([]string(nil), packPaths...),
		CredentialKind:      credentialKind,
		APIKey:              apiKey,
		Token:               token,
		ProfileDir:          prof.ProfileDir,
		PerfSamplesPath:     prof.PerfSamplesPath,
		PerfInterval:        prof.PerfSampleInterval.String(),
		PerfMaxSamples:      prof.PerfMaxSamples,
		PerfMaxSampleFiles:  prof.PerfMaxSampleFiles,
		PerfMaxProfileFiles: prof.PerfMaxProfileFiles,
		PprofAddr:           prof.PprofAddr,
		StartedAt:           startedAt.Format(time.RFC3339Nano),
		HeapProfilePath:     prof.HeapProfilePath,
		ControlSocket:       controlSock,
		Role:                mountstate.RoleWorker,
		Supervise:           supervised,
	}
}

// newMountProcessState is the MountOptions form of the builder (the mount
// entry and its white-box tests use it).
func newMountProcessState(opts *MountOptions, mountPoint string, creationTime uint64, credentialKind, controlSock string, startedAt time.Time) mountstate.ProcessState {
	return buildMountProcessState(opts.RemoteRoot, opts.Profile, opts.LocalRoot, opts.Server, opts.APIKey, opts.Token,
		opts.PackPaths, opts.Profiling, opts.Supervised, opts.OmitProcessStateCredentials,
		mountPoint, creationTime, credentialKind, controlSock, startedAt)
}

// readyMessage renders the mounted line; a non-persistent credential mount
// may run with a credential-bearing endpoint, so it omits the server.
func readyMessage(mountPoint, server, actorID string, readonly bool, policy, cacheBase, shadowDir string, omit bool) string {
	if omit {
		return fmt.Sprintf("drive9: mounted on %s (actor: %s, readonly: %v, write_policy: %s, cache: %s, shadow: %s)\n",
			mountPoint, actorID, readonly, policy, cacheBase, shadowDir)
	}
	return fmt.Sprintf("drive9: mounted on %s (server: %s, actor: %s, readonly: %v, write_policy: %s, cache: %s, shadow: %s)\n",
		mountPoint, server, actorID, readonly, policy, cacheBase, shadowDir)
}

// mountReadyMessage is the MountOptions form (used by tests).
func mountReadyMessage(opts *MountOptions, actorID, cacheBase, shadowDir string) string {
	return readyMessage(opts.MountPoint, opts.Server, actorID, opts.ReadOnly, opts.WritePolicy.String(), cacheBase, shadowDir, opts.OmitProcessStateCredentials)
}

// policyString renders the write policy for the ready line (the enum's
// zero value is writeback).
func policyString(policy writeback.Policy) string {
	switch policy {
	case writeback.PolicyWriteSync:
		return "write-sync"
	case writeback.PolicyCloseSync:
		return "close-sync"
	default:
		return "writeback"
	}
}

// redactCredentialError strips the endpoint/key/token from an error chain,
// preserving the MountExitError classification (exit code/reason survive).
func redactCredentialError(err error, cfg MountConfig) error {
	if err == nil {
		return nil
	}
	message := redactCredentialText(err.Error(), cfg)
	if message == err.Error() {
		return err
	}
	var exitErr *MountExitError
	if errors.As(err, &exitErr) {
		redacted := *exitErr
		redacted.Detail = redactCredentialText(exitErr.Detail, cfg)
		if exitErr.Cause != nil {
			redacted.Cause = redactedMountCause{
				message: redactCredentialText(exitErr.Cause.Error(), cfg),
				cause:   exitErr.Cause,
			}
		}
		return &redacted
	}
	return redactedMountCause{message: message, cause: err}
}

// redactMountCredentialError is the MountOptions form (used by the mount
// entry's white-box tests and by callers holding the public options).
func redactMountCredentialError(err error, opts *MountOptions) error {
	return redactCredentialError(err, MountConfig{Server: opts.Server, APIKey: opts.APIKey, Token: opts.Token})
}

// writeMountExitReason persists an exit record, redacting the detail when
// credentials must not leave the process.
func writeMountExitReason(opts *MountOptions, mountPoint string, rec mountstate.ExitReason) error {
	if opts != nil && opts.OmitProcessStateCredentials {
		rec.Detail = redactCredentialText(rec.Detail, MountConfig{Server: opts.Server, APIKey: opts.APIKey, Token: opts.Token})
	}
	return mountstate.WriteExitReason(mountPoint, rec)
}

// redactCredentialText replaces every secret the mount carries.
func redactCredentialText(text string, cfg MountConfig) string {
	for _, secret := range []string{cfg.Server, cfg.APIKey, cfg.Token} {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "<redacted>")
		}
	}
	return text
}

type redactedMountCause struct {
	message string
	cause   error
}

func (e redactedMountCause) Error() string { return e.message }
func (e redactedMountCause) Unwrap() error { return e.cause }

// applyExtentCapabilityGate clears explicit extent patterns when the server
// cannot attribute a published object to the creating descriptor (missing
// completion_identity_v1). Local and legacy deployments fall back to the
// classic plane instead of failing the first extent commit with EIO.
func applyExtentCapabilityGate(cfg *MountConfig, cl *client.Client, wantExt map[string]bool) {
	if len(cfg.ExtentPaths) == 0 || cl.CachedCompletionIdentitySupported() {
		return
	}
	logger.Warn(context.Background(), "extent_patterns_disabled",
		zap.String("server", cfg.Server),
		zap.Strings("patterns", cfg.ExtentPaths),
		zap.String("detail", "server does not advertise completion_identity_v1; falling back to the classic plane"))
	cfg.ExtentPaths = nil
	delete(wantExt, "extent")
}

func mountWithConfig(cfg MountConfig) error {
	if cfg.APIKey != "" && cfg.Token != "" {
		return fmt.Errorf("fuse: APIKey and Token are mutually exclusive")
	}
	if cfg.APIKey == "" && cfg.Token == "" {
		return fmt.Errorf("fuse: either APIKey or Token is required")
	}

	var baseClient *client.Client
	if cfg.Token != "" {
		baseClient = client.NewWithToken(cfg.Server, cfg.Token)
	} else {
		baseClient = client.New(cfg.Server, cfg.APIKey)
	}
	baseClient.Warm(context.Background())
	// v1: one actor id tags this mount's own writes AND filters its SSE
	// echo — separate ids would make every local commit look like a remote
	// change and trigger needless kernel invalidations.
	actorID := generateMountID()
	baseClient.SetActor(actorID)

	mv, err := prepareMountView(context.Background(), cfg, baseClient, actorID)
	if err != nil {
		return err
	}
	defer func() { _ = mv.close(context.Background()) }()

	cfg.RemoteRoot = mv.remoteRoot
	rt := mv.switcher.currentRuntime()
	f := rt.fs
	fuseOpts := newGoFuseMountOptions(cfg)

	profiler, err := perf.StartProfiler(cfg.toProfilingOptions())
	if err != nil {
		return fmt.Errorf("fuse: profiler: %w", err)
	}
	defer profiler.Stop()

	perfRecorder, err := perf.StartContinuousPerf(cfg.toProfilingOptions())
	if err != nil {
		return fmt.Errorf("fuse: continuous perf: %w", err)
	}
	perfRecorder.SetEnv(&perf.ContinuousPerfEnv{
		MountPoint: f.mountPoint(), RemoteRoot: f.remoteRootTag(), Server: f.serverTag(),
		SyncMode: rt.syncMode, WritePolicy: cfg.WritePolicy, Profile: cfg.Profile,
	})
	perfRecorder.SetPerf(f.perf)
	perfRecorder.SetQueueStats(func() *perf.QueueStats {
		cur := mv.switcher.currentRuntime()
		if cur == nil || cur.fs == nil {
			return nil
		}
		stats := &perf.QueueStats{
			OpenFileHandles:      cur.fs.HandleCount(),
			OpenDirectoryHandles: cur.fs.DirCount(),
		}
		if cur.writeback != nil {
			if q := cur.writeback.Queue(); q != nil {
				stats.CommitPending = q.Pending()
				_, stats.CommitPendingBytes = q.PendingStats()
			}
			stats.UploaderQueued, stats.UploaderInFlight = cur.writeback.UploaderSnapshot()
			stats.DirtyInodes = cur.writeback.DirtyHandleCount()
		}
		return stats
	})
	defer perfRecorder.Stop()

	ctrl, cerr := startMountViewControlServer(cfg.MountPoint, mv.switcher)
	if cerr != nil {
		fmt.Fprintf(os.Stderr, "drive9: mount control server failed: %v (continuing)\n", cerr)
	} else if ctrl != nil {
		defer ctrl.Close()
	}
	if !cfg.SkipProcessState {
		stateMountPoint := cfg.MountPoint
		if absMountPoint, absErr := filepath.Abs(stateMountPoint); absErr == nil {
			stateMountPoint = absMountPoint
		}
		if resolvedMountPoint, resolveErr := filepath.EvalSymlinks(stateMountPoint); resolveErr == nil {
			stateMountPoint = resolvedMountPoint
		}
		credentialKind := mountstate.CredentialKindAPIKey
		if cfg.Token != "" {
			credentialKind = mountstate.CredentialKindToken
		}
		prof := cfg.toProfilingOptions()
		creationTime, _ := mountstate.ProcessCreationTime(os.Getpid())
		controlSock := ""
		if ctrl != nil {
			controlSock = ctrl.SocketPath()
		}
		pidFile, pidErr := mountstate.WriteProcessState(cfg.MountPoint, buildMountProcessState(
			cfg.RemoteRoot, cfg.Profile, cfg.LocalRoot, cfg.Server, cfg.APIKey, cfg.Token,
			cfg.PackPaths, prof, cfg.Supervised, cfg.OmitProcessStateCredentials,
			stateMountPoint, creationTime, credentialKind, controlSock, time.Now().UTC(),
		))
		if pidErr != nil {
			return fmt.Errorf("fuse: write mount pid file: %w", pidErr)
		}
		defer func() {
			if removeErr := os.Remove(pidFile); removeErr != nil && !os.IsNotExist(removeErr) {
				fmt.Fprintf(os.Stderr, "drive9: remove mount pid file %s: %v\n", pidFile, removeErr)
			}
		}()
	}

	if err := os.MkdirAll(cfg.MountPoint, 0o755); err != nil {
		return fmt.Errorf("fuse: create mount point %s: %w", cfg.MountPoint, err)
	}
	server, err := gofuse.NewServer(mv.switcher.adapter, cfg.MountPoint, fuseOpts)
	if err != nil {
		if shouldForceUnmountAfterNewServerError(err) {
			fmt.Fprintf(os.Stderr, "drive9: mount startup failed during fuse server initialization failure at %s: %v\n", cfg.MountPoint, err)
			forceUnmount(cfg.MountPoint)
		}
		return ExitStartupTransientErr("fuse mount", err)
	}
	mv.server.Store(server)
	mv.ready.Store(true)
	defer mv.ready.Store(false)

	startCtx, startCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer startCancel()
	f.Init2(startCtx)

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	if err := f.StartErr(); err != nil {
		fmt.Fprintf(os.Stderr, "drive9: mount startup failed during extension start at %s: %v\n", cfg.MountPoint, err)
		if uerr := server.Unmount(); uerr != nil {
			fmt.Fprintf(os.Stderr, "drive9: cleanup after extension start failure: unmount failed: %v\n", uerr)
			forceUnmount(cfg.MountPoint)
		}
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
		}
		return ExitStartupTransientErr("extension start", err)
	}
	if err := server.WaitMount(); err != nil {
		if ok, probeErr := shouldContinueAfterWaitMountPermissionError(err, cfg.MountPoint, probeMountPointReady); ok {
			fmt.Fprintf(os.Stderr, "drive9: fuse wait mount permission error ignored after readiness probe passed at %s: %v\n", cfg.MountPoint, err)
		} else {
			if probeErr != nil {
				fmt.Fprintf(os.Stderr, "drive9: fuse wait mount permission fallback probe failed at %s: %v\n", cfg.MountPoint, probeErr)
			}
			fmt.Fprintf(os.Stderr, "drive9: mount startup failed during fuse wait mount failure at %s: %v\n", cfg.MountPoint, err)
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = mv.drainShutdownResponse(cleanupCtx)
			cleanupCancel()
			if uerr := server.Unmount(); uerr != nil {
				fmt.Fprintf(os.Stderr, "drive9: cleanup after fuse wait mount failure: unmount failed: %v\n", uerr)
				forceUnmount(cfg.MountPoint)
			}
			select {
			case <-serveDone:
			case <-time.After(10 * time.Second):
			}
			return ExitStartupTransientErr("fuse wait mount", err)
		}
	}
	cacheDesc, shadowDesc := "none", "none"
	if cfg.CacheDir != "" {
		cacheDesc = cfg.CacheDir + "/read"
		if rt.policy == writeback.PolicyWriteBack {
			shadowDesc = cfg.CacheDir + "/staging/shadow"
		}
	}
	if mv.memoryBudget != nil {
		fmt.Fprintf(os.Stderr, "drive9: memory budget: %s\n", mv.memoryBudget.Snapshot().String())
	}
	fmt.Fprint(os.Stderr, readyMessage(cfg.MountPoint, cfg.Server, actorID, cfg.ReadOnly, policyString(rt.policy), cacheDesc, shadowDesc, cfg.OmitProcessStateCredentials))

	lease := startMountLease(context.Background(), baseClient, actorID, cfg.MountPoint, cfg.ReadOnly)
	defer lease.Close()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	var shutdownTriggered atomic.Bool
	shutdownOutcome := newShutdownResult()
	go func() {
		defer shutdownOutcome.finish()
		<-sigCh
		shutdownTriggered.Store(true)
		fmt.Fprintf(os.Stderr, "\ndrive9: unmounting %s...\n", cfg.MountPoint)
		go func() {
			<-sigCh
			fmt.Fprintf(os.Stderr, "drive9: force-quit\n")
			ForceUnmount(cfg.MountPoint)
		}()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
		defer cancel()
		resp := mv.drainShutdownResponse(shutdownCtx)
		if !resp.OK {
			err := recordDrainFailure(cfg, resp)
			shutdownOutcome.set(err)
			fmt.Fprintf(os.Stderr, "drive9: shutdown drain failed at %s: %v\n", cfg.MountPoint, err)
		}
		for attempt := 0; attempt < 5; attempt++ {
			if err := server.Unmount(); err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		ForceUnmount(cfg.MountPoint)
	}()
	<-serveDone
	if shutdownTriggered.Load() {
		shutdownOutcome.wait()
		return shutdownOutcome.err()
	}
	return classifyServeExit(cfg.MountPoint, unmountedKernel)
}

// shutdownDrainTimeout bounds the shutdown drain. It matches the mount
// control server's drain budget (mountcontrol.DefaultDrainTimeout) plus room
// for the unmount retries the worker still has to run.
const shutdownDrainTimeout = 30 * time.Second

// shutdownResult carries the shutdown drain's verdict from the signal
// goroutine to the serve-loop exit path. The two are ordered by the kernel
// unmount, not by a Go synchronization edge, so the hand-off needs a mutex and
// a completion channel: on a forced unmount serveDone can close while the
// drain is still running, and returning before the verdict exists would report
// a clean stop that never happened.
type shutdownResult struct {
	mu      sync.Mutex
	outcome error
	done    chan struct{}
	once    sync.Once
}

func newShutdownResult() *shutdownResult {
	return &shutdownResult{done: make(chan struct{})}
}

func (s *shutdownResult) set(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	s.outcome = errors.Join(s.outcome, err)
	s.mu.Unlock()
}

// finish marks the verdict final; the shutdown goroutine defers it.
func (s *shutdownResult) finish() {
	if s == nil {
		return
	}
	s.once.Do(func() { close(s.done) })
}

// wait blocks until the shutdown goroutine recorded its verdict. The drain
// carries its own deadline, so the wait is bounded.
func (s *shutdownResult) wait() {
	if s == nil {
		return
	}
	<-s.done
}

func (s *shutdownResult) err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outcome
}

// recordDrainFailure persists the drain_failed exit record (the supervisor and
// `drive9 umount` read it to tell a clean stop from one that lost writes) and
// returns the error the worker exits with.
func recordDrainFailure(cfg MountConfig, resp mountcontrol.DrainResponse) error {
	pending := resp.Pending
	detail := fmt.Sprintf("drain failed at %s", cfg.MountPoint)
	reason := resp.ErrorKind
	if reason == "" {
		reason = resp.Error
	} else if resp.Error != "" {
		reason += ": " + resp.Error
	}
	if reason != "" {
		detail += ": " + reason
	}
	detail += fmt.Sprintf(" (open_handles=%d dirty_handles=%d commit_queue=%d commit_bytes=%d uploader_queued=%d uploader_in_flight=%d uploader_cached_bytes=%d)",
		pending.OpenHandles,
		pending.DirtyHandles,
		pending.CommitQueuePending,
		pending.CommitQueueBytes,
		pending.UploaderQueued,
		pending.UploaderInFlight,
		pending.UploaderCachedBytes,
	)
	if cfg.OmitProcessStateCredentials {
		detail = redactCredentialText(detail, cfg)
	}
	record := mountstate.ExitReason{
		PID:    os.Getpid(),
		Code:   ExitDrainFailed,
		Reason: string(ExitReasonDrainFailed),
		Detail: detail,
		// Files the caller can look for in the preserved recovery state.
		PendingFiles: pending.DirtyHandles + pending.CommitQueuePending + pending.UploaderQueued,
		PendingBytes: pending.CommitQueueBytes + pending.UploaderCachedBytes,
	}
	if err := mountstate.WriteExitReason(cfg.MountPoint, record); err != nil {
		fmt.Fprintf(os.Stderr, "drive9: record drain failure at %s: %v\n", cfg.MountPoint, err)
	}
	return ExitDrainFailedErr(detail)
}

// classifyServeExit classifies a serve-loop exit by the KERNEL mount row
// alone (mountStillPresent reports whether the mountinfo entry survives).
// The go-fuse fork's OnUnmount callback fires unconditionally — clean
// umount(2) and abnormal /dev/fuse disconnect alike — so no callback-side
// signal participates here. A row that survives the settle window marks
// a hung mount: the supervisor's ExitServeAbnormal recovery path.
func classifyServeExit(mountPoint string, mountStillPresent func(string) bool) error {
	if !mountStillPresent(mountPoint) {
		return nil
	}
	for deadline := time.Now().Add(300 * time.Millisecond); time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
		if !mountStillPresent(mountPoint) {
			return nil
		}
	}
	return ExitServeAbnormalErr(fmt.Sprintf("serve loop ended at %s with the kernel mount still present", mountPoint))
}

// unmountedKernel reports whether the mountpoint is no longer a FUSE mount.
func unmountedKernel(mountPoint string) bool {
	mnts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		// Non-Linux or unreadable: fall back to the handshake flag only.
		return true
	}
	for _, line := range strings.Split(string(mnts), "\n") {
		if strings.Contains(line, " "+mountPoint+" ") && strings.Contains(line, "fuse") {
			return false
		}
	}
	return true
}

// validateRemoteRoot ports v1's startup preflight: the remote source must
// exist (and be a directory) before the FUSE mount is established, so a typo
// in the source path fails fast instead of wedging the mountpoint. Scoped
// tokens that grant list-without-read fall back to List.
func validateRemoteRoot(ctx context.Context, c *client.Client, remoteRoot string) error {
	probeOpts := client.ListOptions{Limit: 1, SinglePage: true}
	if remoteRoot == "/" {
		if _, err := c.ListWithOptionsCtx(ctx, remoteRoot, probeOpts); err != nil {
			return remoteRootValidationError(remoteRoot, err)
		}
		return nil
	}

	stat, err := c.StatCtx(ctx, remoteRoot)
	if err == nil {
		if !stat.IsDir {
			return ExitStartupPermanentErr(fmt.Sprintf("remote root %q is not a directory", remoteRoot), nil)
		}
		return nil
	}
	if client.IsNotFound(err) {
		return ExitStartupPermanentErr(fmt.Sprintf("remote source %q does not exist", remoteRoot), err)
	}
	if !shouldFallbackRemoteRootList(err) {
		return remoteRootValidationError(remoteRoot, err)
	}
	// Stat may fail when directory stat is unsupported or when a scoped token
	// grants list without read. Fall back to List and classify its result as the
	// authoritative non-root mount validation.
	if _, listErr := c.ListWithOptionsCtx(ctx, remoteRoot, probeOpts); listErr != nil {
		if client.IsNotFound(listErr) {
			return ExitStartupPermanentErr(fmt.Sprintf("remote source %q does not exist", remoteRoot), listErr)
		}
		return remoteRootValidationError(remoteRoot, listErr)
	}
	return nil
}

func shouldFallbackRemoteRootList(err error) bool {
	if client.IsForbidden(err) {
		return true
	}
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	switch statusErr.StatusCode {
	case http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func remoteRootValidationError(remoteRoot string, err error) *MountExitError {
	if client.IsUnauthorized(err) || client.IsForbidden(err) {
		return ExitStartupPermanentErr(fmt.Sprintf("remote root %q authorization denied", remoteRoot), err)
	}
	if remoteRoot == "/" {
		return ExitStartupTransientErr("cannot reach drive9 server", err)
	}
	return ExitStartupTransientErr(fmt.Sprintf("remote root %q", remoteRoot), err)
}

// diskCacheOpts builds the on-disk read-cache options; an empty cache
// directory disables the disk layer (memory-only, v1 default).
// extentCacheSizeForTest mirrors the engine wiring's extent budget
// mapping: the configured bytes pass through verbatim, 0 included.
func extentCacheSizeForTest(bytes int64) uint64 {
	if bytes <= 0 {
		return 0
	}
	return uint64(bytes)
}

func diskCacheOpts(dir string, sizeMB int64, freeRatio float64) cache.DiskReadCacheOptions {
	if dir == "" {
		return cache.DiskReadCacheOptions{}
	}
	return cache.DiskReadCacheOptions{
		Dir:       dir,
		MaxSize:   sizeMB,
		FreeRatio: freeRatio,
	}
}

// cacheScopedDir joins a named cache subtree under the mount-scoped
// namespace; an unavailable cache base disables the subtree (empty).
func cacheScopedDir(cacheBase, hash, name string) string {
	if cacheBase == "" {
		return ""
	}
	return filepath.Join(cacheBase, hash, name)
}

// appendLogSnapshotDir resolves the append-log snapshot spill directory,
// created eagerly so the spill (which does not create parents) finds it.
func appendLogSnapshotDir(cacheBase, mountHash string) string {
	if cacheBase == "" {
		return ""
	}
	dir := filepath.Join(cacheBase, mountHash, "append-log-snapshots")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "drive9: append-log snapshot dir init failed: %v (continuing without)\n", err)
		return ""
	}
	return dir
}

// mountCredentialKind names the credential type backing the mount.
func mountCredentialKind(cfg *MountConfig) string {
	if cfg.Token != "" {
		return "token"
	}
	return "api_key"
}

// mountCredentialSecret returns the credential secret backing the mount.
func mountCredentialSecret(cfg *MountConfig) string {
	if cfg.Token != "" {
		return cfg.Token
	}
	return cfg.APIKey
}

// MountHash computes the writeback-side mount namespace.
func MountHash(serverURL, mountPoint string, remoteRoot ...string) string {
	root := "/"
	if len(remoteRoot) > 0 && remoteRoot[0] != "" {
		root = remoteRoot[0]
	}
	var input string
	if root == "/" {
		// Backward-compatible: same hash as pre-subtree-mount versions.
		input = serverURL + "\x00" + mountPoint
	} else {
		input = serverURL + "\x00" + mountPoint + "\x00" + root
	}
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8]) // 16 hex chars
}

// MountLayerHash folds the canonical layer ID into the mount
// namespace: a layer mount's writeback state is not interchangeable with
// the base mount's.
func MountLayerHash(serverURL, mountPoint, remoteRoot, layerID string) string {
	base := MountHash(serverURL, mountPoint, remoteRoot)
	input := base + "\x00layer\x00" + strings.TrimSpace(layerID)
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8])
}

// MountReadCacheHash computes the persistent read-cache namespace. Unlike
// the write-back cache namespace, it includes a credential digest because
// read-cache entries contain remote file bytes and can survive remounts.
func MountReadCacheHash(serverURL, mountPoint, remoteRoot, credentialKind, credential string) string {
	return MountReadCacheHashWithLayer(serverURL, mountPoint, remoteRoot, credentialKind, credential, "")
}

// MountReadCacheHashWithLayer folds an optional canonical layer ID into the read-cache namespace.
func MountReadCacheHashWithLayer(serverURL, mountPoint, remoteRoot, credentialKind, credential, layerID string) string {
	if remoteRoot == "" {
		remoteRoot = "/"
	}
	credentialDigest := sha256.Sum256([]byte(credentialKind + "\x00" + credential))
	input := serverURL + "\x00" + mountPoint + "\x00" + remoteRoot + "\x00" + credentialKind + "\x00" + hex.EncodeToString(credentialDigest[:])
	if trimmed := strings.TrimSpace(layerID); trimmed != "" {
		input += "\x00layer\x00" + trimmed
	}
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8])
}

// resolveSyncModeByRTT ports v1 MeasureRTT + ResolveMode: rttThreshold of
// 10ms, probe failure assumes WAN (interactive).
func resolveSyncModeByRTT(serverURL string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, serverURL+"/", nil)
	if err != nil {
		return "interactive"
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	rtt := time.Since(start)
	if err != nil {
		return "interactive"
	}
	_ = resp.Body.Close()
	if rtt > 10*time.Millisecond {
		return "interactive"
	}
	return "strict"
}

func generateMountID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "mount-unknown"
	}
	return hex.EncodeToString(b[:])
}

// transientOverlayRoot anchors the mount-local SQLite sidecar plane in the
// read-cache namespace, in a fresh per-mount directory.
func transientOverlayRoot(cacheBase, readCacheHash string) string {
	return filepath.Join(cacheBase, readCacheHash, "transient", transientOverlayMountID())
}

// transientOverlayMountID identifies one mount's transient plane. The
// fallback keeps pid uniqueness when the system entropy source fails.
func transientOverlayMountID() string {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err == nil {
		return fmt.Sprintf("%d-%s", os.Getpid(), hex.EncodeToString(suffix[:]))
	}
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
}

// shouldContinueAfterWaitMountPermissionError ports v1: on Linux the pollHack
// open can fail with EACCES/EPERM for a mount that is otherwise serving fine;
// the readiness probe decides whether to keep serving.
func shouldContinueAfterWaitMountPermissionError(err error, mountPoint string, probe func(string) error) (bool, error) {
	if err == nil || !isWaitMountPermissionError(err) {
		return false, nil
	}
	if runtime.GOOS != "linux" {
		return false, nil
	}
	if probe == nil {
		return false, fmt.Errorf("no readiness probe configured")
	}
	if probeErr := probe(mountPoint); probeErr != nil {
		return false, probeErr
	}
	return true, nil
}

func isWaitMountPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
		return true
	}
	// Some go-fuse WaitMount paths surface the kernel errno as a plain text
	// error that does not unwrap to a syscall.Errno, so fall back to matching
	// the canonical EACCES/EPERM strings.
	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "permission denied") || strings.Contains(errText, "operation not permitted")
}

// shouldForceUnmountAfterNewServerError targets go-fuse's post-mount INIT
// failure: NewServer calls mount(), then handleInit(), and returns
// fmt.Errorf("init: %s", code) if INIT fails after closing the mount fd. That
// path has no Server handle for Unmount, so the mountpoint must be detached
// directly (v1 comment: re-check when go-fuse is upgraded or replaced).
func shouldForceUnmountAfterNewServerError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "init:")
}

func forceUnmount(mountpoint string) {
	if runtime.GOOS == "darwin" {
		if err := runUnmountCmd(5*time.Second, "diskutil", "unmount", "force", mountpoint); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: force unmount failed: %v\n", err)
		}
		return
	}
	// Linux: prefer fusermount, always fall through to umount -l when the
	// endpoint remains (fusermount often fails with EACCES after daemon death).
	if _, err := exec.LookPath("fusermount3"); err == nil {
		if runUnmountCmd(5*time.Second, "fusermount3", "-u", mountpoint) == nil && !mountStillNeedsClean(mountpoint) {
			return
		}
	} else if _, err := exec.LookPath("fusermount"); err == nil {
		if runUnmountCmd(5*time.Second, "fusermount", "-u", mountpoint) == nil && !mountStillNeedsClean(mountpoint) {
			return
		}
	}
	if _, err := exec.LookPath("umount"); err == nil {
		if err := runUnmountCmd(5*time.Second, "umount", "-l", mountpoint); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: force unmount (umount -l) failed: %v\n", err)
		}
		return
	}
	fmt.Fprintf(os.Stderr, "drive9: force unmount: no fusermount/umount available\n")
}

// newGoFuseMountOptions renders the kernel mount options from the config
// (v1 newGoFuseMountOptions). Allow-other mounts bind default_permissions on both
// Linux and macOS — the ext4-parity EACCES contract from #1006. Without it macFUSE
// forwards every authorization to this daemon as an ACCESS message instead of
// checking the mode bits in the kernel.
func newGoFuseMountOptions(cfg MountConfig) *gofuse.MountOptions {
	return newGoFuseMountOptionsFor(runtime.GOOS, cfg)
}

// newGoFuseMountOptionsFor is the platform-parameterised form, so both branches are
// testable on any host.
func newGoFuseMountOptionsFor(goos string, cfg MountConfig) *gofuse.MountOptions {
	opts := &gofuse.MountOptions{
		FsName:             "drive9",
		Name:               "drive9",
		MaxReadAhead:       8 * 1024 * 1024,
		MaxWrite:           128 * 1024,
		MaxBackground:      32,
		EnableLocks:        true,
		Debug:              cfg.Debug,
		AllowOther:         cfg.AllowOther,
		EnableDirectIoMmap: true,
		SyncRead:           cfg.SyncRead,
		DirectMountStrict:  cfg.DirectMountStrict,
	}
	if goos == "linux" {
		opts.MaxWrite = 1024 * 1024
		if cfg.AllowOther {
			opts.Options = append(opts.Options, "default_permissions")
		}
	}
	if goos == "darwin" {
		// macFUSE checks mode bits in the kernel only when the mount says
		// default_permissions; otherwise every authorization becomes an ACCESS
		// message to this daemon. That is why an --allow-other mount needs the
		// option: this branch passes no defer_permissions, so without
		// default_permissions the only check would be FS.Access, whose group class
		// reads the caller's supplementary groups from /proc/<pid>/status — a file
		// macOS does not have — and it cannot distinguish a member from a
		// non-member. A private mount stays on defer_permissions: the kernel already
		// restricts it to the mounting user, and the option keeps the per-operation
		// ACCESS upcall (with its chain stat) off the hot path. "local" carries the
		// volume semantics on both branches.
		if cfg.AllowOther {
			opts.Options = append(opts.Options, "default_permissions", "local")
		} else {
			opts.Options = append(opts.Options, "defer_permissions", "local")
		}
	}
	if cfg.ReadOnly {
		opts.Options = append(opts.Options, "ro")
	}
	return opts
}
