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
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/appendlog"
	"github.com/mem9-ai/drive9/pkg/extensions/bulkfile"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/extensions/git"
	"github.com/mem9-ai/drive9/pkg/extensions/layer"
	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/extensions/renameguard"
	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/extensions/sse"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/extensions/xattr"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mountpath"
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
	MountPoint string
	RemoteRoot string

	CacheDir               string
	CacheSize              int64
	ReadCacheMaxFileBytes  int64
	ReadCacheTTL           time.Duration
	DiskReadCacheSize      int64
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

	Profile       string
	LayerRef      string
	CheckpointRef string

	LocalRoot                   string
	LocalOnlyPatterns           []string
	LocalGitignoreAwarePatterns []string
	RemoteOnlyPatterns          []string

	AppendLogPatterns []string
	PackPaths         []string
	ExtentPaths       []string

	UploadConcurrency     int
	DirCacheMaxEntries    int
	CommitQueueMaxPending int

	WriteBackBatchWindow   time.Duration
	WriteBackBatchMaxFiles int
	WriteBackBatchMaxBytes int64
	WriteCacheFreeRatio    float64
	WriteCacheSizeMB       int64

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
// order: core first, each subsequent entry wrapping the ones before it,
// so the chain assembly order (outermost first) is the reverse of this
// list. git wraps everything — it must see a complete filesystem (the
// read caches included) below itself to transform workspace paths
// efficiently, so it sits outside cache, the v1 outermost.
var defaultExtensionOrder = []string{
	"core", "sse", "writeback", "xattr", "specials", "bulkfile",
	"appendlog", "extent", "layer", "localize", "renameguard", "cache", "git",
}

// knownExtensionNames guards --extensions against typos (an unknown name
// would otherwise silently not wire).
var knownExtensionNames = map[string]bool{
	"core": true, "sse": true, "writeback": true, "xattr": true,
	"specials": true, "bulkfile": true, "appendlog": true, "extent": true,
	"git": true, "layer": true, "localize": true, "renameguard": true,
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
	Server                      string        // drive9 server URL
	APIKey                      string        // owner API key (mutually exclusive with Token)
	Token                       string        // delegated capability JWT (mutually exclusive with APIKey)
	MountPoint                  string        // local mount point
	RemoteRoot                  string        // remote subtree root (default "/")
	CacheDir                    string        // write-back cache directory (default ~/.cache/drive9)
	CacheSize                   int64         // ReadCache max size in bytes (default 128MB)
	ReadCacheMaxFileBytes       int64         // largest single file admitted to ReadCache (default 4MiB)
	ReadCacheTTL                time.Duration // ReadCache TTL (default 30s)
	DiskReadCacheSize           int64         // disk read cache max size in bytes (default 1GiB)
	DiskReadCacheFreeRatio      float64       // min free-space ratio before disk read cache evicts (default 0.10)
	DirTTL                      time.Duration // DirCache TTL (default 10s)
	AttrTTL                     time.Duration // kernel attr cache TTL (default 60s)
	EntryTTL                    time.Duration // kernel entry cache TTL (default 60s)
	NegativeEntryTTL            time.Duration // kernel negative entry cache TTL (default 1s)
	FlushDebounce               time.Duration // debounce window for small-file flush coalescing (default 2s; -1 uses default)
	SyncMode                    SyncMode      // interactive, strict, or auto (default auto)
	WritePolicy                 WritePolicy   // writeback, close-sync, or write-sync (default writeback)
	Profile                     string        // mount profile name
	LayerRef                    string        // writable fs layer ref
	CheckpointRef               string        // checkpoint ref for the layer view baseline
	LocalRoot                   string        // local-only overlay root
	LocalOnlyPatterns           []string      // local-only path patterns
	LocalGitignoreAwarePatterns []string      // local-only patterns gated by Git ignore rules
	RemoteOnlyPatterns          []string      // remote-persistent override patterns
	// DisableBuiltinOverlayDefaults stops the built-in local-only defaults for
	// Profile from being added on top of the given patterns. The CLI resolves
	// the effective profile itself and sets this; direct library callers leave
	// it false and keep the defaults.
	DisableBuiltinOverlayDefaults bool
	AppendLogPatterns             []string // remote-persistent files eligible for append-log synchronization
	PackPaths                     []string // local overlay paths auto-packed after unmount
	ExtentPaths                   []string // path globs created as content_layout=extent
	CommitQueueMaxPending         int      // CommitQueue backpressure bound (default 100; 0 default)
	WriteBackBatchWindow          time.Duration
	WriteBackBatchMaxFiles        int
	WriteBackBatchMaxBytes        int64
	WriteCacheFreeRatio           float64
	WriteCacheSizeMB              int64
	UploadConcurrency             int           // background upload workers (default 16)
	ReadConcurrency               int           // concurrent backend reads (default 24)
	ParallelReadConcurrency       int           // concurrent block reads per large FUSE read (default 4)
	ParallelReadBlockSize         int64         // block size for parallel reads (default 1MiB)
	SyncRead                      bool          // disable kernel async read dispatch
	DirectMountStrict             bool          // Linux only: mount(2) without fusermount fallback
	GVisorCompat                  bool          // gVisor FUSE compatibility behavior
	LegacyInterruptibleMutations  bool          // FUSE interrupt cancels in-flight remote commits (legacy)
	LookupRetryCount              int           // detached retries after transient stat failures (default 2)
	LookupRetryTimeout            time.Duration // timeout per detached stat retry (default 250ms)
	LegacyDirStatFallback         bool          // list parent when directory stat 404s (legacy servers)
	ReadDirPrefetch               bool
	PrefetchMaxFiles              int
	PrefetchMaxFileBytes          int64
	PrefetchMaxBytes              int64
	PrefetchTimeout               time.Duration
	DirCacheMaxEntries            int
	TrustLocalEvents              bool
	AllowOther                    bool
	ReadOnly                      bool
	Debug                         bool
	PerfCounters                  bool
	EnableGitWorkspaces           bool // enable git workspace overlay discovery (default off)
	Profiling                     ProfilingOptions
	WritebackLazyStaging          bool
	WritebackSyncWindow           time.Duration
	// RemoteCommitWaitTimeout bounds how long a FUSE write/flush handler waits
	// for a background commit before proceeding anyway (default 5s; 0 uses
	// default; negative disables the timeout).
	RemoteCommitWaitTimeout time.Duration
	// SkipProcessState skips writing the mount pid/process state file.
	SkipProcessState bool
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
	return MountWithConfig(MountConfig{
		Server:                        opts.Server,
		APIKey:                        opts.APIKey,
		Token:                         opts.Token,
		MountPoint:                    opts.MountPoint,
		RemoteRoot:                    opts.RemoteRoot,
		CacheDir:                      opts.CacheDir,
		CacheSize:                     opts.CacheSize,
		ReadCacheMaxFileBytes:         opts.ReadCacheMaxFileBytes,
		ReadCacheTTL:                  opts.ReadCacheTTL,
		DiskReadCacheSize:             opts.DiskReadCacheSize,
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
		CheckpointRef:                 opts.CheckpointRef,
		LocalRoot:                     opts.LocalRoot,
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
		Supervised:                    opts.Supervised,
		Extensions:                    opts.Extensions,
	})
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
	if cfg.APIKey != "" && cfg.Token != "" {
		return fmt.Errorf("fuse: APIKey and Token are mutually exclusive")
	}
	if cfg.APIKey == "" && cfg.Token == "" {
		return fmt.Errorf("fuse: either APIKey or Token is required")
	}
	remoteRoot, err := mountpath.NormalizeRoot(cfg.RemoteRoot)
	if err != nil {
		return fmt.Errorf("fuse: normalize remote root: %w", err)
	}

	var cl *client.Client
	if cfg.Token != "" {
		cl = client.NewWithToken(cfg.Server, cfg.Token)
	} else {
		cl = client.New(cfg.Server, cfg.APIKey)
	}
	cl.Warm(context.Background())
	// v1: one actor id tags this mount's own writes AND filters its SSE
	// echo — separate ids would make every local commit look like a remote
	// change and trigger needless kernel invalidations.
	actorID := generateMountID()
	cl.SetActor(actorID)

	// v1 validateRemoteRoot: refuse to mount a remote source that does not
	// exist (startup-permanent) before any kernel mount is established, and
	// surface connectivity/auth trouble as startup-transient for the
	// supervisor's retry ladder.
	if err := validateRemoteRoot(context.Background(), cl, remoteRoot); err != nil {
		return err
	}
	// A checkpoint without a layer is meaningless (the layer plane carries
	// the checkpoint view) and previously passed silently: the layer
	// extension was skipped, the checkpoint ignored, and an ordinary
	// writable filesystem mounted instead.
	if strings.TrimSpace(cfg.CheckpointRef) != "" && strings.TrimSpace(cfg.LayerRef) == "" {
		return fmt.Errorf("fuse: checkpoint %q requires --layer (the layer plane owns the checkpoint view)", cfg.CheckpointRef)
	}

	extOrder, err := parseExtensionOrder(cfg.Extensions)
	if err != nil {
		return err
	}
	// sharedCounters exists before the extension chain: the git
	// extension's refresh accounting needs the same instance the FS
	// summary prints.
	sharedCounters := perf.NewCounters(cfg.PerfCounters || cfg.PerfSamplesPath != "")
	wantExt := make(map[string]bool, len(extOrder))
	for _, name := range extOrder {
		wantExt[name] = true
	}
	// Each extension constructs independently into a registry; assembly
	// walks the flag order in reverse below.
	built := map[string]fs.Handler{}

	// v1 cache base: an explicit cache dir, else the per-user default, so
	// cache-backed components stay enabled without explicit configuration.
	cacheBase := cfg.CacheDir
	if cacheBase == "" {
		if home, herr := os.UserHomeDir(); herr == nil {
			cacheBase = filepath.Join(home, ".cache", "drive9")
		}
	}
	// v1 mount-scoped cache namespaces: writeback-side state keys on the
	// mount identity (layer mounts fold their refs into it), while the
	// read-side caches also key on the credential because their entries
	// hold remote bytes that survive remounts.
	mountHash := ""
	readCacheHash := ""
	if cacheBase != "" {
		mountHash = MountHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot)
		if cfg.LayerRef != "" {
			mountHash = MountLayerHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot, cfg.LayerRef, cfg.CheckpointRef)
		}
		readCacheHash = MountReadCacheHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot, mountCredentialKind(&cfg), mountCredentialSecret(&cfg))
	}

	// core constructs into the same registry as every other extension; the
	// flag order places it (first entry = innermost terminal).
	var coreExt *core.Core
	if wantExt["core"] {
		coreExt, err = core.New(cl, core.Options{
			RemoteRoot:            remoteRoot,
			StatRetryCount:        cfg.LookupRetryCount,
			StatRetryTimeout:      cfg.LookupRetryTimeout,
			ReadConcurrency:       cfg.ReadConcurrency,
			LegacyDirStatFallback: cfg.LegacyDirStatFallback,
			// gvisor-compat takes precedence and always keeps commits
			// detached (v1).
			LegacyInterruptibleMutations: cfg.LegacyInterruptibleMutations && !cfg.GVisorCompat,
		})
		if err != nil {
			return fmt.Errorf("fuse: core terminal: %w", err)
		}
		built["core"] = coreExt
	}
	bus := fs.NewEventBus()
	var readCache *cache.Cache
	if wantExt["cache"] {
		// v1 always ran the in-memory read cache — it does not need a cache
		// directory; the on-disk block cache rides --cache-dir. Zero limits
		// fall back to the ported defaults (128MB / 30s / 4MB per file).
		// The cache extension owns everything under the cache directory
		// (landlord); for now that is the read cache; the staging ledger
		// joins when writeback lands.
		rc, cerr := cache.New(cache.Options{
			Events:             bus,
			MaxSize:            cfg.CacheSize,
			TTL:                cfg.ReadCacheTTL,
			SequentialPrefetch: true,
			PrefetchClient:     cl,
			RemoteRoot:         remoteRoot,
			// v1 --parallel-read-concurrency/block-size (defaults 4 x 1MiB).
			ParallelReadConcurrency: cfg.ParallelReadConcurrency,
			ParallelReadBlockSize:   cfg.ParallelReadBlockSize,
			// v1 readdir content prefetch: --readdir-prefetch only (default
			// off — the max-files knob alone does not enable it).
			DirContentPrefetch:   cfg.ReadDirPrefetch,
			PrefetchMaxFiles:     cfg.PrefetchMaxFiles,
			PrefetchMaxFileBytes: cfg.PrefetchMaxFileBytes,
			PrefetchMaxBytes:     cfg.PrefetchMaxBytes,
			PrefetchTimeout:      cfg.PrefetchTimeout,
			Disk:                 diskCacheOpts(cacheScopedDir(cacheBase, readCacheHash, "read"), cfg.DiskReadCacheSize, cfg.DiskReadCacheFreeRatio),
		})
		if cerr != nil {
			return fmt.Errorf("fuse: cache extension: %w", cerr)
		}
		readCache = rc
		built["cache"] = readCache
	}

	var policy writeback.Policy
	switch cfg.WritePolicy {
	case "write-sync":
		policy = writeback.PolicyWriteSync
	case "writeback", "":
		policy = writeback.PolicyWriteBack
	case "close-sync":
		policy = writeback.PolicyCloseSync
	default:
		return fmt.Errorf("fuse: unknown write policy %q", cfg.WritePolicy)
	}

	// The v1 small-file writeback track (.dat/.meta + 16-worker uploader)
	// rides the mount-scoped cache namespace under its own name.
	writeCacheDir := cacheScopedDir(cacheBase, mountHash, "writecache")
	// The staging ledger + commit queue serve the async writeback policy
	// (shadow/pending/journal machinery); close-sync ALSO gets the ledger
	// for its v1 shadow-backed large-file close uploads (bounded-memory
	// staging + streamed landing). Write-sync commits inline per Write and
	// needs none of it. The cache plugin stays regardless — its READ caches
	// serve every policy; only its staging role is policy-bound.
	var stagingLedger *staging.Staging
	if (policy == writeback.PolicyWriteBack || policy == writeback.PolicyCloseSync) && cacheBase != "" && wantExt["writeback"] {
		// wantExt gate: a core-only stack (--extensions core) keeps the
		// default writeback policy but builds no writeback handler; the
		// ledger would still be created, recovered, and compacted — a
		// corrupt or unwritable cache dir failing a mount that never
		// uses it.
		// v1 write-cache quota (defaults freeRatio 0.10, 1024 MiB): the
		// shadow store refuses writes past the budget with ENOSPC.
		// v1 knobs: zero uses the default, negative disables the check.
		freeRatio := cfg.WriteCacheFreeRatio
		if freeRatio == 0 {
			freeRatio = 0.10
		} else if freeRatio < 0 {
			freeRatio = 0
		}
		cacheSizeMB := resolveWriteCacheSizeMB(cfg.WriteCacheSizeMB)
		// The ledger's internal layout (shadow/, pending/, journal.wal) is
		// the v1 mount-scoped one: root it at the mount hash directly.
		stagingLedger, err = staging.NewStagingWithQuota(filepath.Join(cacheBase, mountHash), freeRatio, cacheSizeMB)
		if err != nil {
			return fmt.Errorf("fuse: staging ledger: %w", err)
		}
		defer stagingLedger.Close()
		if err := stagingLedger.Recover(); err != nil {
			return fmt.Errorf("fuse: staging recovery: %w", err)
		}
		_ = stagingLedger.Compact()
	}

	// v1 sync-mode resolution: auto probes the server RTT (HEAD /, 5s
	// bound); >10ms (or an unreachable probe) resolves interactive — fsync
	// served by the local staging ledger — otherwise strict.
	syncMode := strings.TrimSpace(cfg.SyncMode)
	switch syncMode {
	case "", "auto":
		syncMode = resolveSyncModeByRTT(cfg.Server)
	case "strict", "interactive":
	default:
		return fmt.Errorf("fuse: unknown sync mode %q", cfg.SyncMode)
	}
	fmt.Fprintf(os.Stderr, "drive9: sync mode: %s\n", syncMode)

	// The POSIX rename preflight sits early so its attribute lookups see
	// the view handlers below it (lazy-create overlays answer for
	// not-yet-committed files).
	var renameGuard *renameguard.RenameGuard
	if wantExt["renameguard"] {
		renameGuard, err = renameguard.New(renameguard.Options{
			Uid: uint32(os.Getuid()),
			Gid: uint32(os.Getgid()),
		})
		if err != nil {
			return fmt.Errorf("fuse: renameguard extension: %w", err)
		}
		built["renameguard"] = renameGuard
	}

	// v1 EnableGitWorkspaces (default off): the git extension's workspace
	// discovery is opt-in — overlay mounts (LocalRoot) arm it via the CLI.
	// Constructed before localize so its declarative routing (a workspace's
	// .git stays local, v1 gitDirShouldBeLocalOverlay) wires into the
	// overlay router.
	var gitExt *git.Git
	if wantExt["git"] && cfg.EnableGitWorkspaces {
		var gerr error
		// FS-native: the extension carries no client; its store view is
		// the chain below itself, attached after assembly.
		gitExt, gerr = git.New(git.Options{Events: bus, LocalRoot: cfg.LocalRoot, RemoteRoot: remoteRoot,
			OnWorkspaceRefresh: sharedCounters.RecordGitWorkspaceRefresh, Logger: logger.L()})
		if gerr != nil {
			return fmt.Errorf("fuse: git extension: %w", gerr)
		}
		built["git"] = gitExt
	}

	// localize routes declared local-only paths to the local overlay
	// (v1 local_overlay/local_policy semantics).
	var localizeExt *localize.Localize
	if wantExt["localize"] && cfg.LocalRoot != "" {
		localizeOpts := localize.Options{
			LocalRoot:                   cfg.LocalRoot,
			LocalOnlyPatterns:           cfg.LocalOnlyPatterns,
			LocalGitignoreAwarePatterns: cfg.LocalGitignoreAwarePatterns,
			RemoteOnlyPatterns:          cfg.RemoteOnlyPatterns,
			Profile:                     cfg.Profile,
			ApplyBuiltinDefaults:        !cfg.DisableBuiltinOverlayDefaults,
		}
		if gitExt != nil {
			// v1 gitDirShouldBeLocalOverlay: a discovered workspace's .git
			// routes to the local overlay (the restored .git state backs
			// git's own file traffic, not the remote).
			localizeOpts.DeclarativeLocal = gitExt.RoutesLocal
		}
		if cacheBase != "" && !cfg.ReadOnly {
			// SQLite WAL-index sidecars (-shm) are mount-local scratch:
			// they live in the read-cache namespace in a fresh per-mount
			// directory, so a crashed mount's leftovers are never reused.
			// Creation failure only disables that plane (sidecars then
			// stay on their classified route).
			transientRoot := transientOverlayRoot(cacheBase, readCacheHash)
			if err := os.MkdirAll(transientRoot, 0o700); err != nil {
				fmt.Fprintf(os.Stderr, "drive9: transient local overlay init failed: %v (continuing without)\n", err)
			} else {
				localizeOpts.TransientRoot = transientRoot
			}
		}
		var lerr error
		localizeExt, lerr = localize.New(localizeOpts)
		if lerr != nil {
			return fmt.Errorf("fuse: localize extension: %w", lerr)
		}
		built["localize"] = localizeExt
	}

	// The layer view sits over the base (v1: layer over base); git sits
	// outside everything below (see defaultExtensionOrder).
	var layerExt *layer.Layer
	if wantExt["layer"] && strings.TrimSpace(cfg.LayerRef) != "" {
		layerExt, err = layer.New(layer.Options{
			Policy:        cfg.WritePolicy,
			LayerRef:      cfg.LayerRef,
			CheckpointRef: cfg.CheckpointRef,
			RemoteRoot:    remoteRoot,
			Client:        cl,
			Events:        bus,
		})
		if err != nil {
			return fmt.Errorf("fuse: layer extension: %w", err)
		}
		built["layer"] = layerExt
	}
	// The extent plane claims ahead of the classic data path (cache and
	// writeback never see extent files). Gated on the --extent paths /
	// profile [extent] section only: the cache dir falls back to the
	// per-user default like every other cache-backed component, so it is
	// never the enabling condition.
	var extentExt *extent.Extent
	// Build the extent reader whenever the stack requests it, even with
	// no local ExtentPaths: files created by another extent-enabled mount
	// still carry ExtentIno in listings/stats, and the server answers
	// their reads with 422 unless the JuiceFS-backed reader routes them.
	// An empty ExtentPaths only skips the JuiceFS runtime discovery.
	if wantExt["extent"] {
		extentExt, err = extent.New(extent.Options{
			ExtentPaths: cfg.ExtentPaths,
			Client:      cl,
			RemoteRoot:  remoteRoot,
			CacheDir:    cacheScopedDir(cacheBase, mountHash, "jfs"),
			// v1: write-sync mounts pay a per-Write VFS flush on the extent
			// plane too.
			Policy: cfg.WritePolicy,
		})
		if err != nil {
			return fmt.Errorf("fuse: extent extension: %w", err)
		}
		built["extent"] = extentExt
	}

	var appendExt *appendlog.AppendLog
	if wantExt["appendlog"] && len(cfg.AppendLogPatterns) > 0 && strings.TrimSpace(cfg.LayerRef) == "" {
		appendExt, err = appendlog.New(appendlog.Options{Patterns: cfg.AppendLogPatterns, Client: cl, RemoteRoot: remoteRoot, Events: bus, TempDir: appendLogSnapshotDir(cacheBase, mountHash)})
		if err != nil {
			return fmt.Errorf("fuse: appendlog extension: %w", err)
		}
		built["appendlog"] = appendExt
	}

	// The bulk data plane claims fresh truncate-writers (S3 multipart for
	// large images) only after the view/routing handlers have had their
	// claim, and before the buffered write path. A write-sync mount commits
	// every write inline below — the buffered-image claim stays out.
	if wantExt["bulkfile"] {
		bulkExt, berr := bulkfile.New(bulkfile.Options{Client: cl, RemoteRoot: remoteRoot, Events: bus, Policy: cfg.WritePolicy})
		if berr != nil {
			return fmt.Errorf("fuse: bulkfile extension: %w", berr)
		}
		built["bulkfile"] = bulkExt
	}

	if wantExt["specials"] {
		specialsExt, sperr := specials.New(specials.Options{Events: bus})
		if sperr != nil {
			return fmt.Errorf("fuse: specials extension: %w", sperr)
		}
		built["specials"] = specialsExt
	}

	if wantExt["xattr"] {
		xattrExt, xerr := xattr.New(xattr.Options{Events: bus})
		if xerr != nil {
			return fmt.Errorf("fuse: xattr extension: %w", xerr)
		}
		built["xattr"] = xattrExt
	}

	var wb *writeback.Writeback
	if wantExt["writeback"] {
		flushDebounce := cfg.FlushDebounce
		if cfg.Profile == "interactive" {
			// v1 ApplyInteractiveProfile disables the debouncer outright
			// ("replaced by shadow writes").
			flushDebounce = 0
		}
		var werr error
		wb, werr = writeback.New(writeback.Options{
			Policy:                policy,
			SyncMode:              syncMode,
			WriteCacheDir:         writeCacheDir,
			Client:                cl,
			RemoteRoot:            remoteRoot,
			Events:                bus,
			Staging:               stagingLedger,
			FlushDebounce:         flushDebounce,
			UploadConcurrency:     cfg.UploadConcurrency,
			CommitQueueMaxPending: cfg.CommitQueueMaxPending,
			BatchWindow:           cfg.WriteBackBatchWindow,
			BatchMaxFiles:         cfg.WriteBackBatchMaxFiles,
			BatchMaxBytes:         cfg.WriteBackBatchMaxBytes,
			// gvisor-compat takes precedence and always keeps commits
			// detached (v1).
			InterruptibleCommits:    cfg.LegacyInterruptibleMutations && !cfg.GVisorCompat,
			RemoteCommitWaitTimeout: cfg.RemoteCommitWaitTimeout,
			// v1 --writeback-lazy-staging / --writeback-sync-window.
			LazyStaging: cfg.WritebackLazyStaging,
			SyncWindow:  cfg.WritebackSyncWindow,
		})
		if werr != nil {
			return fmt.Errorf("fuse: writeback extension: %w", werr)
		}
		built["writeback"] = wb
	}

	if readCache != nil && wb != nil {
		// v1 seeded the read cache with the committed image at every
		// inline-commit settle, and read its write-side base preloads
		// through the same cache: reads racing backend replica lag then see
		// this mount's own writes (read-your-writes), and preloads cannot
		// 404 inside the lag window.
		wb.SetCommitSeeder(readCache.SeedCommit)
		wb.SetPendingSeeder(readCache.SeedPending)
		wb.SetBaseReader(func(path string) ([]byte, bool) {
			return readCache.ReadCached(path, 0)
		})
	}

	var sseExt *sse.SSE
	if wantExt["sse"] {
		var serr error
		sseExt, serr = sse.New(sse.Options{Client: cl, Actor: actorID, RemoteRoot: remoteRoot, Events: bus})
		if serr != nil {
			return fmt.Errorf("fuse: sse extension: %w", serr)
		}
		built["sse"] = sseExt
	}

	// v1 setDefaults: kernel attr/entry caches default to 60s (30s attr on
	// the coding-agent profile), negative entries 1s, listings 10s. The
	// interactive profile (v1 ApplyInteractiveProfile) drops attr/entry to
	// 1s, listings to 2s, and disables the flush debounce — zero values
	// only, so explicit flags still win.
	attrTTL, entryTTL, dirTTL := resolveKernelTTLDefaults(cfg.Profile, cfg.AttrTTL, cfg.EntryTTL, cfg.DirTTL)
	// View extensions that own directories (localize overlays, layer
	// view) must see dir GetAttrs go through the chain; every other stack
	// serves root/dir attrs from the inode table (v1).
	localDirAttrs := cfg.LocalRoot == "" && strings.TrimSpace(cfg.LayerRef) == ""
	// The kernel enforces permissions when default_permissions is on;
	// otherwise the daemon's open gate carries the mode contract.
	defaultPermissions := cfg.AllowOther
	// Assemble the chain as the REVERSE of the flag order: the flag lists
	// extensions in build-up order (core first, each subsequent entry
	// wrapping the previous ones), so the last flag entry is the outermost
	// handler and core is the terminal. Entries whose config prerequisites
	// are absent (cache without --cache-dir, layer without --layer, ...)
	// construct nothing and are skipped.
	var chainHandlers []fs.Handler
	for i := len(extOrder) - 1; i >= 0; i-- {
		if h := built[extOrder[i]]; h != nil {
			chainHandlers = append(chainHandlers, h)
		}
	}
	// The git extension's store view is the chain below itself: the full
	// handler list minus the git handler, dispatched as a Filesystem. It
	// must be wired before the facade starts serving (git's discovery,
	// view rewrites, and checkpoints all read/write through it — with the
	// read caches below, which is the point of placing git outermost).
	if gitExt != nil {
		// The store view's terminal must speak tenant-absolute paths: the
		// ordinary core prefixes RemoteRoot, so on a subtree mount (:/sub)
		// the store would read/write /sub/.drive9/... while the CLI
		// registers at the tenant root and discovery would never match.
		storeCore, serr := core.New(cl, core.Options{})
		if serr != nil {
			return fmt.Errorf("fuse: git store terminal: %w", serr)
		}
		tail := make([]fs.Handler, 0, len(chainHandlers))
		for _, h := range chainHandlers {
			if h == fs.Handler(gitExt) || h == fs.Handler(coreExt) {
				continue
			}
			tail = append(tail, h)
		}
		tail = append(tail, storeCore)
		tailChain, terr := fs.NewChain(tail...)
		if terr != nil {
			return fmt.Errorf("fuse: git store chain: %w", terr)
		}
		gitExt.AttachStore(fs.HandlerFilesystem(tailChain.Dispatch))
	}
	f, err := New(Options{
		Extensions:         chainHandlers,
		Events:             bus,
		AttrTTL:            attrTTL,
		EntryTTL:           entryTTL,
		NegEntryTTL:        cfg.NegativeEntryTTL,
		DirTTL:             dirTTL,
		DirCacheMaxEntries: cfg.DirCacheMaxEntries,
		LocalDirAttrs:      localDirAttrs,
		DefaultPermissions: defaultPermissions,
		Perf:               cfg.PerfCounters || cfg.PerfSamplesPath != "",
		PerfCounters:       sharedCounters,
		Debug:              cfg.Debug,
		GVisorCompat:       cfg.GVisorCompat,
	})
	if err != nil {
		return fmt.Errorf("fuse: assemble facade: %w", err)
	}
	// The POSIX rename preflight compares sticky ownership against the
	// facade's inode table (mount-local chown authority, v1), not the chain's
	// stat view.
	if renameGuard != nil {
		renameGuard.SetOwnerLookup(f.OwnerOf)
	}
	// Hand the facade-owned namespace cache to the terminal: fresh listings
	// answer Lookups/GetAttrs without a remote stat (v1 lookupFromDirCache).
	if coreExt != nil {
		if dc, ok := f.Ctx.DirCache.(core.NamespaceCache); ok {
			coreExt.Attach(dc)
		}
	}
	f.mountPt, f.rootTag, f.srvTag = cfg.MountPoint, remoteRoot, cfg.Server

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
		SyncMode: syncMode, WritePolicy: cfg.WritePolicy, Profile: cfg.Profile,
	})
	perfRecorder.SetPerf(f.perf)
	if wb != nil {
		perfRecorder.SetQueueStats(func() *perf.QueueStats {
			stats := &perf.QueueStats{
				OpenFileHandles:      f.HandleCount(),
				OpenDirectoryHandles: f.DirCount(),
			}
			if q := wb.Queue(); q != nil {
				stats.CommitPending = q.Pending()
				_, stats.CommitPendingBytes = q.PendingStats()
			}
			stats.UploaderQueued, stats.UploaderInFlight = wb.UploaderSnapshot()
			stats.DirtyInodes = wb.DirtyHandleCount()
			return stats
		})
	}
	defer perfRecorder.Stop()
	if sseExt != nil {
		sseExt.Attach(f.Ctx.Inodes, f.Ctx.Notify)
	}
	if gitExt != nil {
		// Remote store changes (overlay writes from other mounts, CLI
		// registration) knock the kernel's view of the mapped workspace
		// paths.
		gitExt.Attach(f.Ctx.Inodes, f.Ctx.Notify)
	}
	if layerExt != nil {
		// The layer event watcher resets the kernel view after a remote
		// refresh (v1 resetMountView on fs_layer_events).
		layerExt.Attach(f.Ctx.Notify)
		// A layer rollback fences the write plane: the commit queue halts
		// and later inline commits fail with the rolled-back error.
		if wb != nil {
			layerExt.OnRollback(wb.AbandonLayerForRollback)
		}
	}

	// Control server + mount process state (v1 mount.go) run BEFORE the
	// FUSE mount: the pidfile's EvalSymlinks stats the mountpoint, which
	// after NewServer is served by a serve loop that does not exist yet —
	// the daemon would deadlock on its own mount. The pidfile is how
	// `drive9 mount drain/umount/status` find this worker and its control
	// socket; a supervised worker skips it — the supervisor owns the state
	// file (v1 SkipProcessState: opts.Supervised).
	ctrl, cerr := startMountControlServer(cfg.MountPoint, f)
	if cerr != nil {
		fmt.Fprintf(os.Stderr, "drive9: mount control server failed: %v (continuing)\n", cerr)
	} else {
		defer ctrl.Close()
	}
	// v1 had SkipProcessState as an independent knob (the supervisor passes
	// it explicitly); Supervised alone is only a logging marker.
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
		pidFile, pidErr := mountstate.WriteProcessState(cfg.MountPoint, mountstate.ProcessState{
			PID:                 os.Getpid(),
			CreationTime:        creationTime,
			Component:           "drive9-fuse",
			MountKind:           mountstate.MountKindFUSE,
			MountPoint:          stateMountPoint,
			RemoteRoot:          cfg.RemoteRoot,
			Profile:             cfg.Profile,
			LocalRoot:           cfg.LocalRoot,
			Server:              cfg.Server,
			PackPaths:           append([]string(nil), cfg.PackPaths...),
			CredentialKind:      credentialKind,
			APIKey:              cfg.APIKey,
			Token:               cfg.Token,
			ProfileDir:          prof.ProfileDir,
			PerfSamplesPath:     prof.PerfSamplesPath,
			PerfInterval:        prof.PerfSampleInterval.String(),
			PerfMaxSamples:      prof.PerfMaxSamples,
			PerfMaxSampleFiles:  prof.PerfMaxSampleFiles,
			PerfMaxProfileFiles: prof.PerfMaxProfileFiles,
			PprofAddr:           prof.PprofAddr,
			StartedAt:           time.Now().UTC().Format(time.RFC3339Nano),
			HeapProfilePath:     prof.HeapProfilePath,
			ControlSocket:       controlSock,
			Role:                mountstate.RoleWorker,
			Supervise:           cfg.Supervised,
		})
		if pidErr != nil {
			return fmt.Errorf("fuse: write mount pid file: %w", pidErr)
		}
		defer func() {
			if removeErr := os.Remove(pidFile); removeErr != nil && !os.IsNotExist(removeErr) {
				fmt.Fprintf(os.Stderr, "drive9: remove mount pid file %s: %v\n", pidFile, removeErr)
			}
		}()
	}

	fuseOpts := newGoFuseMountOptions(cfg)

	server, err := gofuse.NewServer(f, cfg.MountPoint, fuseOpts)
	if err != nil {
		// v1 cleanupNewServerFailure: NewServer can fail after the kernel
		// mount came up (post-mount INIT failure) with no Server handle to
		// unmount through — detach the mountpoint ourselves so the failed
		// attempt does not leave a dead endpoint behind.
		if shouldForceUnmountAfterNewServerError(err) {
			fmt.Fprintf(os.Stderr, "drive9: mount startup failed during fuse server initialization failure at %s: %v\n", cfg.MountPoint, err)
			forceUnmount(cfg.MountPoint)
		}
		return ExitStartupTransientErr("fuse mount", err)
	}
	// Bound plugin Start (go-fuse runs Init inside the serve loop; a slow
	// plugin must never stall the mount).
	startCtx, startCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer startCancel()
	f.Init2(startCtx)

	// Serve must be reading /dev/fuse before WaitMount's pollHack opens the
	// mountpoint: that open is a FUSE OPENDIR and would deadlock against a
	// daemon that has not entered its read loop (Linux).
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	// A plugin that failed its Start leaves the stack degraded (a layer
	// checkpoint that could not restore serves an empty/read-only view):
	// the mount must fail instead of reporting success (review: Init2's
	// startErr was saved but never checked).
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
		// v1 shouldContinueAfterWaitMountPermissionError: Linux pollHack can
		// surface EACCES/EPERM for a mount that is actually serving; the
		// readiness probe decides whether to keep serving.
		if ok, probeErr := shouldContinueAfterWaitMountPermissionError(err, cfg.MountPoint, probeMountPointReady); ok {
			fmt.Fprintf(os.Stderr, "drive9: fuse wait mount permission error ignored after readiness probe passed at %s: %v\n", cfg.MountPoint, err)
		} else {
			if probeErr != nil {
				fmt.Fprintf(os.Stderr, "drive9: fuse wait mount permission fallback probe failed at %s: %v\n", cfg.MountPoint, probeErr)
			}
			// v1 cleanupMountStartFailure: NewServer succeeded and Serve is
			// running, so drain the write plane before detaching the kernel
			// mount; an unmount failure falls back to a forced detach so the
			// mountpoint is never left wedged (ENOTCONN) behind a failed
			// startup.
			fmt.Fprintf(os.Stderr, "drive9: mount startup failed during fuse wait mount failure at %s: %v\n", cfg.MountPoint, err)
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = f.flushOpenHandles(cleanupCtx)
			_ = f.chain.Drain(cleanupCtx)
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
		if policy == writeback.PolicyWriteBack {
			shadowDesc = cfg.CacheDir + "/staging/shadow"
		}
	}
	fmt.Fprintf(os.Stderr, "drive9: mounted on %s (server: %s, actor: %s, readonly: %v, write_policy: %s, cache: %s, shadow: %s)\n",
		cfg.MountPoint, cfg.Server, actorID, cfg.ReadOnly, policy, cacheDesc, shadowDesc)

	// Graceful shutdown (v1 mount.go): on the first SIGINT/SIGTERM flush
	// open handles, drain the chain, and Unmount with retries (EBUSY is
	// transient); a second signal force-unmounts. Without this the worker
	// waits for the kernel to drop the connection and the supervisor's
	// stop window expires into a kill.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\ndrive9: unmounting %s...\n", cfg.MountPoint)
		go func() {
			<-sigCh
			fmt.Fprintf(os.Stderr, "drive9: force-quit\n")
			ForceUnmount(cfg.MountPoint)
		}()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = f.flushOpenHandles(shutdownCtx)
		_ = f.chain.Drain(shutdownCtx)
		for attempt := 0; attempt < 5; attempt++ {
			if err := server.Unmount(); err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		ForceUnmount(cfg.MountPoint)
	}()
	<-serveDone
	// Classify the serve end: a clean unmount completed the OnUnmount
	// handshake (or our signal handler already detached the kernel mount);
	// anything else is an abnormal /dev/fuse disconnect that the
	// supervisor's ExitServeAbnormal recovery exists for. Reporting it as
	// success suppressed restarts.
	if !f.unmounted.Load() && !unmountedKernel(cfg.MountPoint) {
		return ExitServeAbnormalErr(fmt.Sprintf("serve loop ended at %s without a clean unmount", cfg.MountPoint))
	}
	return nil
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
	if remoteRoot == "/" {
		if _, err := c.ListCtx(ctx, remoteRoot); err != nil {
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
	if _, listErr := c.ListCtx(ctx, remoteRoot); listErr != nil {
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

// MountLayerHash folds the layer and checkpoint refs into the mount
// namespace: a layer mount's writeback state is not interchangeable with
// the base mount's.
func MountLayerHash(serverURL, mountPoint, remoteRoot, layerRef, checkpointRef string) string {
	base := MountHash(serverURL, mountPoint, remoteRoot)
	input := base + "\x00layer\x00" + strings.TrimSpace(layerRef) + "\x00checkpoint\x00" + strings.TrimSpace(checkpointRef)
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8])
}

// MountReadCacheHash computes the persistent read-cache namespace. Unlike
// the write-back cache namespace, it includes a credential digest because
// read-cache entries contain remote file bytes and can survive remounts.
func MountReadCacheHash(serverURL, mountPoint, remoteRoot, credentialKind, credential string) string {
	if remoteRoot == "" {
		remoteRoot = "/"
	}
	credentialDigest := sha256.Sum256([]byte(credentialKind + "\x00" + credential))
	input := serverURL + "\x00" + mountPoint + "\x00" + remoteRoot + "\x00" + credentialKind + "\x00" + hex.EncodeToString(credentialDigest[:])
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
// (v1 newGoFuseMountOptions). Linux allow-other mounts bind
// default_permissions — the ext4-parity EACCES contract from #1006.
func newGoFuseMountOptions(cfg MountConfig) *gofuse.MountOptions {
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
	if runtime.GOOS == "linux" {
		opts.MaxWrite = 1024 * 1024
		if cfg.AllowOther {
			opts.Options = append(opts.Options, "default_permissions")
		}
	}
	if runtime.GOOS == "darwin" {
		opts.Options = append(opts.Options, "defer_permissions", "local")
	}
	if cfg.ReadOnly {
		opts.Options = append(opts.Options, "ro")
	}
	return opts
}
