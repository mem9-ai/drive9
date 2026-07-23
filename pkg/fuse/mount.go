package fuse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// MountOptions configures the FUSE mount.
//
// Credential kind: APIKey and Token are mutually exclusive. APIKey is an
// owner tenant API key (full management + data plane); Token is a delegated
// capability JWT (read-path only). Exactly one must be non-empty at
// Mount() time — the running mount is bound to that single credential for
// its entire lifetime (Invariant #3). To change credentials, umount and
// remount; there is no in-process rebind.
type MountOptions struct {
	Server                  string        // drive9 server URL
	APIKey                  string        // owner API key (mutually exclusive with Token)
	Token                   string        // delegated capability JWT (mutually exclusive with APIKey)
	MountPoint              string        // local mount point
	RemoteRoot              string        // remote subtree root (default "/"); set via "drive9 mount :/path /local"
	CacheDir                string        // write-back cache directory (default ~/.cache/drive9); empty string uses default
	CacheSize               int64         // ReadCache max size in bytes (default 128MB)
	ReadCacheMaxFileBytes   int64         // largest single file admitted to ReadCache and fetched whole-file in one request (default 4MiB)
	ReadCacheTTL            time.Duration // ReadCache TTL (default 30s; negative disables time-based expiry)
	DiskReadCacheSize       int64         // disk-backed read cache max size in bytes (default 1GiB)
	DiskReadCacheFreeRatio  float64       // minimum filesystem free-space ratio before disk read cache evicts (default 0.10)
	DirTTL                  time.Duration // DirCache TTL (default 10s)
	AttrTTL                 time.Duration // kernel attr cache TTL (default 60s)
	EntryTTL                time.Duration // kernel entry cache TTL (default 60s)
	NegativeEntryTTL        time.Duration // kernel negative entry cache TTL (default 1s)
	FlushDebounce           time.Duration // debounce window for small-file flush coalescing (default 2s, 0 disables); set to -1 to use default
	SyncMode                SyncMode      // interactive, strict, or auto (default auto)
	WritePolicy             WritePolicy   // writeback, close-sync, or write-sync (default writeback)
	Profile                 string        // mount profile: "interactive", "coding-agent", "none", or a custom profile name
	LayerRef                string        // optional writable fs layer ref (layer_id, name, or tag ref)
	CheckpointRef           string        // optional checkpoint ref to restore as the layer view baseline
	LocalRoot               string        // local-only overlay root for overlay-profile mounts
	LocalOnlyPatterns       []string      // additional local-only path patterns for overlay-profile mounts
	RemoteOnlyPatterns      []string      // remote-persistent override path patterns for overlay-profile mounts
	PackPaths               []string      // local overlay paths auto-packed after unmount
	CommitQueueMaxPending   int           // maximum pending entries in CommitQueue before backpressure (default 100); 0 uses default
	WriteBackBatchWindow    time.Duration // writeback-only small-file batch window (default 0 disabled)
	WriteBackBatchMaxFiles  int           // maximum files in one writeback batch (default 64 when enabled)
	WriteBackBatchMaxBytes  int64         // maximum bytes in one writeback batch (default 4MiB when enabled)
	WriteCacheFreeRatio     float64       // minimum free-space ratio on cache-dir partition before write-back refuses writes (default 0.10); negative disables
	WriteCacheSizeMB        int64         // shadow cache byte quota in MB (default 1024 = 1GB); negative disables; shadow writes exceeding this return ENOSPC
	UploadConcurrency       int           // number of background upload workers (default 4)
	ReadConcurrency         int           // maximum concurrent backend reads issued by FUSE (default 24)
	ParallelReadConcurrency int           // maximum concurrent block reads for one large FUSE read (default 4)
	ParallelReadBlockSize   int64         // block size for parallel large-file reads in bytes (default 1MiB)
	SyncRead                bool          // disable kernel async read dispatch; at most one read in flight per file handle
	DirectMountStrict       bool          // Linux only: mount with mount(2) and do not fall back to fusermount
	LookupRetryCount        int           // detached retries after transient Lookup/GetAttr stat failures (default 2)
	LookupRetryTimeout      time.Duration // timeout per detached stat retry after interrupt/transient errors (default 250ms)
	LegacyDirStatFallback   bool          // on Lookup stat 404, list parent to support legacy servers without directory stat
	ReadDirPrefetch         bool          // prefetch small files after readdir into ReadCache (default false)
	PrefetchMaxFiles        int           // maximum files prefetched per directory read (default 32 when enabled)
	PrefetchMaxFileBytes    int64         // maximum individual file size prefetched (default 50KB)
	PrefetchMaxBytes        int64         // maximum aggregate bytes prefetched per directory read (default 1MB)
	PrefetchTimeout         time.Duration // timeout for one readdir prefetch batch (default 1s)
	DirCacheMaxEntries      int           // maximum entries per directory in DirCache (default 200000); directories exceeding this limit are not cached as complete
	TrustLocalEvents        bool          // allow revision-bound GetAttr hits from DirCache using process-local SSE freshness; safe only for single-server/sticky or cluster-wide event streams
	AllowOther              bool          // allow other users to access mount
	ReadOnly                bool          // mount as read-only
	Debug                   bool          // enable FUSE debug logging
	PerfCounters            bool          // print low-overhead FUSE perf counter summary on shutdown
	EnableGitWorkspaces     bool          // enable fast-clone git workspace overlay discovery
	Profiling               ProfilingOptions
	// RemoteCommitWaitTimeout bounds how long a FUSE write/flush handler waits
	// for a background commit to finish before proceeding anyway. This prevents
	// the FUSE daemon from hanging indefinitely when a backend upload is slow.
	// Default 5s; 0 uses default; negative disables the timeout (legacy behavior).
	RemoteCommitWaitTimeout time.Duration
}

const defaultUploadConcurrency = 16

func (o *MountOptions) setDefaults() {
	// Apply profile defaults before generic defaults so profile-specific
	// zero-value options can take effect while explicit non-zero values win.
	if o.Profile == MountProfileInteractive {
		ApplyInteractiveProfile(o)
	}
	if o.CacheSize <= 0 {
		o.CacheSize = defaultReadCacheMaxSize
	}
	if o.ReadCacheMaxFileBytes <= 0 {
		o.ReadCacheMaxFileBytes = defaultReadCacheMaxFileSize
	}
	if o.ReadCacheTTL == 0 {
		o.ReadCacheTTL = defaultReadCacheTTL
	}
	if o.DiskReadCacheSize <= 0 {
		o.DiskReadCacheSize = defaultDiskReadCacheMaxSize
	}
	if o.DiskReadCacheFreeRatio <= 0 {
		o.DiskReadCacheFreeRatio = defaultDiskReadCacheFreeRatio
	}
	if o.DirTTL <= 0 {
		o.DirTTL = defaultDirCacheTTL
	}
	if o.AttrTTL <= 0 {
		o.AttrTTL = defaultPositiveKernelCacheTTL
	}
	if o.EntryTTL <= 0 {
		o.EntryTTL = defaultPositiveKernelCacheTTL
	}
	if o.NegativeEntryTTL <= 0 {
		o.NegativeEntryTTL = time.Second
	}
	// FlushDebounce: 0 means disabled, negative means unset (use default).
	if o.FlushDebounce < 0 {
		o.FlushDebounce = defaultFlushDebounce
	}
	if o.UploadConcurrency <= 0 {
		o.UploadConcurrency = defaultUploadConcurrency
	}
	if o.ReadConcurrency <= 0 {
		o.ReadConcurrency = defaultRemoteReadConcurrency
	}
	if o.ParallelReadConcurrency <= 0 {
		o.ParallelReadConcurrency = defaultParallelReadConcurrency
	}
	if o.ParallelReadBlockSize <= 0 {
		o.ParallelReadBlockSize = defaultParallelReadBlockSize
	}
	if o.WriteCacheFreeRatio < 0 {
		// Negative values explicitly disable the write-back free-ratio guard.
		o.WriteCacheFreeRatio = 0
	} else if o.WriteCacheFreeRatio == 0 {
		o.WriteCacheFreeRatio = defaultWriteCacheFreeRatio
	}
	if o.WriteCacheSizeMB < 0 {
		// Negative values explicitly disable the write-back byte quota.
		o.WriteCacheSizeMB = 0
	} else if o.WriteCacheSizeMB == 0 {
		o.WriteCacheSizeMB = defaultWriteCacheSizeMB
	}
	if o.LookupRetryCount < 0 {
		// Negative values are CLI-internal sentinels meaning retries were
		// explicitly disabled by the operator.
		o.LookupRetryCount = 0
	} else if o.LookupRetryCount == 0 {
		o.LookupRetryCount = lookupTransientRetryCount
	}
	if o.LookupRetryTimeout <= 0 {
		o.LookupRetryTimeout = lookupTransientRetryTimeout
	}
	if o.PrefetchMaxFiles <= 0 {
		o.PrefetchMaxFiles = defaultReadDirPrefetchMaxFiles
	}
	if o.PrefetchMaxFileBytes <= 0 || o.PrefetchMaxFileBytes > defaultSmallFileThreshold {
		o.PrefetchMaxFileBytes = defaultSmallFileThreshold
	}
	if o.PrefetchMaxBytes <= 0 {
		o.PrefetchMaxBytes = defaultReadDirPrefetchMaxBytes
	}
	if o.PrefetchTimeout <= 0 {
		o.PrefetchTimeout = defaultReadDirPrefetchTimeout
	}
	if o.Profiling.PerfSamplesPath != "" && o.Profiling.PerfSampleInterval <= 0 {
		o.Profiling.PerfSampleInterval = 10 * time.Second
	}
	if o.Profiling.PerfSamplesPath != "" && o.Profiling.PerfMaxSamples <= 0 {
		o.Profiling.PerfMaxSamples = defaultPerfMaxSamples
	}
	if o.Profiling.PerfSamplesPath != "" && o.Profiling.PerfMaxSampleFiles <= 0 {
		o.Profiling.PerfMaxSampleFiles = defaultPerfMaxSampleFiles
	}
	if o.Profiling.ProfileDir != "" && o.Profiling.CPUProfileDuration <= 0 {
		o.Profiling.CPUProfileDuration = defaultCPUProfileDuration
	}
	if o.Profiling.ProfileDir != "" && o.Profiling.CPUProfileInterval <= 0 {
		o.Profiling.CPUProfileInterval = defaultCPUProfileInterval
	}
	if o.Profiling.ProfileDir != "" && o.Profiling.HeapProfileInterval <= 0 {
		o.Profiling.HeapProfileInterval = defaultHeapProfileInterval
	}
	if o.Profiling.ProfileDir != "" && o.Profiling.PerfMaxProfileFiles <= 0 {
		o.Profiling.PerfMaxProfileFiles = defaultPerfMaxProfileFiles
	}
	if o.RemoteCommitWaitTimeout == 0 {
		o.RemoteCommitWaitTimeout = 5 * time.Second
	}
}

// Mount creates and serves a FUSE mount. It blocks until the filesystem
// is unmounted or a signal (SIGINT, SIGTERM) is received.
func Mount(opts *MountOptions) error {
	if opts == nil {
		return fmt.Errorf("mount: options are required")
	}
	opts.setDefaults()
	if err := validateMountOptionsProfile(opts); err != nil {
		return err
	}
	if localOverlay := NewLocalOverlay(opts.LocalRoot); localOverlay != nil {
		if err := localOverlay.EnsureRoot(); err != nil {
			return fmt.Errorf("mount: prepare LocalRoot: %w", err)
		}
	}

	if err := os.MkdirAll(opts.MountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	// Validate credential inputs. MountOptions.APIKey and MountOptions.Token
	// are mutually exclusive (Invariant #3 — one mount, one principal).
	// Both empty is caller error; both non-empty would let a silent
	// priority rule override what the caller wrote, which we refuse.
	if opts.APIKey != "" && opts.Token != "" {
		return fmt.Errorf("mount: APIKey and Token are mutually exclusive (choose one principal kind at mount time)")
	}
	if opts.APIKey == "" && opts.Token == "" {
		return fmt.Errorf("mount: either APIKey (owner) or Token (delegated) is required")
	}
	opts.LayerRef = strings.TrimSpace(opts.LayerRef)
	opts.CheckpointRef = strings.TrimSpace(opts.CheckpointRef)
	if opts.CheckpointRef != "" && opts.LayerRef == "" {
		return fmt.Errorf("mount: CheckpointRef requires LayerRef")
	}

	// Generate per-mount actor ID for SSE self-filtering.
	actorID := generateMountID()

	// Create client and verify connectivity. The constructor choice binds
	// the mount's principal kind for its entire lifetime (see Invariant #3
	// and Invariant #6 — running mount credential change requires umount
	// and remount).
	var c *client.Client
	if opts.Token != "" {
		c = client.NewWithToken(opts.Server, opts.Token)
	} else {
		c = client.New(opts.Server, opts.APIKey)
	}
	c.SetActor(actorID)

	// Validate remote root (or server connectivity for root mounts).
	remoteRoot, err := mountpath.NormalizeRoot(opts.RemoteRoot)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	opts.RemoteRoot = remoteRoot
	if remoteRoot == "/" {
		if _, err := c.List("/"); err != nil {
			return fmt.Errorf("cannot reach drive9 server: %w", err)
		}
	} else {
		stat, err := c.Stat(remoteRoot)
		if err != nil {
			// If Stat explicitly says "not found", trust it — don't fall back
			// to List which may return empty success for non-existent paths.
			if client.IsNotFound(err) {
				return fmt.Errorf("drive9 mount: remote source %q does not exist\n\n  To create it first:\n    drive9 fs mkdir :%s\n  Then retry:\n    drive9 mount :%s <mountpoint>", remoteRoot, remoteRoot, remoteRoot)
			}
			// Stat may fail on backends where directory stat is unsupported
			// (non-404 error). Fall back to List to verify existence.
			if _, listErr := c.List(remoteRoot); listErr != nil {
				if client.IsNotFound(listErr) {
					return fmt.Errorf("drive9 mount: remote source %q does not exist\n\n  To create it first:\n    drive9 fs mkdir :%s\n  Then retry:\n    drive9 mount :%s <mountpoint>", remoteRoot, remoteRoot, remoteRoot)
				}
				return fmt.Errorf("remote root %q: %w", remoteRoot, listErr)
			}
		} else if !stat.IsDir {
			return fmt.Errorf("remote root %q is not a directory", remoteRoot)
		}
	}
	if opts.LayerRef != "" {
		layer, err := c.GetFSLayer(context.Background(), opts.LayerRef)
		if err != nil {
			return fmt.Errorf("mount: resolve fs layer %q: %w", opts.LayerRef, err)
		}
		if layer.State != "active" {
			return fmt.Errorf("mount: fs layer %q is %s, want active", opts.LayerRef, layer.State)
		}
		opts.LayerRef = layer.LayerID
		fmt.Fprintf(os.Stderr, "drive9: fs layer: %s\n", layer.LayerID)
	}

	// Build FUSE filesystem
	dat9fs := NewDat9FS(c, opts)
	layerEventWatcherStop := func() {}

	profiler, err := StartProfiler(opts.Profiling)
	if err != nil {
		return fmt.Errorf("start profiler: %w", err)
	}
	opts.Profiling.PprofAddr = profiler.PprofAddr()
	defer profiler.Stop()

	// Resolve sync mode (auto-detect RTT if needed).
	resolved := ResolveMode(context.Background(), opts.SyncMode, opts.Server)
	dat9fs.syncMode = resolved
	fmt.Fprintf(os.Stderr, "drive9: sync mode: %s\n", resolved)
	perfRecorder, err := StartContinuousPerf(opts.Profiling, dat9fs)
	if err != nil {
		return fmt.Errorf("start continuous perf: %w", err)
	}
	defer perfRecorder.Stop()

	cacheBase := opts.CacheDir
	if cacheBase == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			cacheBase = filepath.Join(home, ".cache", "drive9")
		}
	}
	mountHash := ""
	if cacheBase != "" {
		mountHash = MountHash(opts.Server, opts.MountPoint, opts.RemoteRoot)
		if opts.LayerRef != "" {
			mountHash = MountLayerHash(opts.Server, opts.MountPoint, opts.RemoteRoot, opts.LayerRef, opts.CheckpointRef)
		}
		readCacheHash := MountReadCacheHash(opts.Server, opts.MountPoint, opts.RemoteRoot, mountCredentialKind(opts), mountCredentialSecret(opts))
		readCacheDir := filepath.Join(cacheBase, readCacheHash, "read")
		diskReadCache, err := NewDiskReadCache(DiskReadCacheOptions{
			Dir:       readCacheDir,
			MaxSize:   opts.DiskReadCacheSize,
			FreeRatio: opts.DiskReadCacheFreeRatio,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "drive9: disk read cache init failed: %v (continuing without)\n", err)
		} else {
			dat9fs.diskReadCache = diskReadCache
		}
		if !opts.ReadOnly {
			transientRoot := transientOverlayRoot(cacheBase, readCacheHash)
			if err := os.MkdirAll(transientRoot, 0o700); err != nil {
				fmt.Fprintf(os.Stderr, "drive9: transient local overlay init failed: %v (continuing without)\n", err)
			} else {
				transientOverlay := NewLocalOverlay(transientRoot)
				if err := transientOverlay.EnsureRoot(); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: transient local overlay init failed: %v (continuing without)\n", err)
				} else {
					dat9fs.transientLocalOverlay = transientOverlay
				}
			}
		}
	}

	// Initialize write-back cache, shadow store, and pending index.
	var shadowDir string
	if !opts.ReadOnly {
		if cacheBase != "" {
			pendingDir := filepath.Join(cacheBase, mountHash, "pending")
			shadowDir = filepath.Join(cacheBase, mountHash, "shadow")

			// Initialize PendingIndex (in-memory authoritative metadata).
			pendingIdx, err := NewPendingIndex(pendingDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "drive9: pending index init failed: %v (continuing without)\n", err)
			} else {
				if err := pendingIdx.RecoverFromDisk(); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: pending index recovery: %v\n", err)
				}
				dat9fs.pendingIndex = pendingIdx
			}

			// Initialize ShadowStore with write-back disk protection.
			var writeCacheMaxBytes int64
			if opts.WriteCacheSizeMB > 0 {
				writeCacheMaxBytes = opts.WriteCacheSizeMB << 20
			}
			shadowStore, err := NewShadowStoreWithQuota(shadowDir, opts.WriteCacheFreeRatio, writeCacheMaxBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "drive9: shadow store init failed: %v (continuing without)\n", err)
			} else {
				dat9fs.shadowStore = shadowStore
			}

			// Initialize Journal WAL.
			journalPath := filepath.Join(cacheBase, mountHash, "journal.wal")
			journal, err := NewJournal(journalPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "drive9: journal init failed: %v (continuing without)\n", err)
			} else {
				dat9fs.journal = journal
				// Replay journal for crash recovery. Preserve the original kind
				// and base revision so CommitQueue.RecoverPending can re-enqueue.
				if err := replayJournalIntoPending(journal, pendingIdx); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: journal replay: %v\n", err)
				}
				// Drop frames already covered by commit markers so the WAL does
				// not grow unboundedly across mounts. Safe here: mount init is
				// single-threaded, no concurrent Append yet.
				if err := journal.Compact(); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: journal compact: %v\n", err)
				}
			}

			// Initialize WriteBackCache before CommitQueue so that legacy
			// pending entries can be migrated to shadow files. Without this,
			// RecoverPending would prune them as orphans (shadow missing).
			wbCache, err := NewWriteBackCache(pendingDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "drive9: write-back cache init failed: %v (continuing without)\n", err)
			}

			// Migrate legacy writeBack entries to shadow store so
			// CommitQueue.RecoverPending sees them and doesn't prune.
			if wbCache != nil && shadowStore != nil {
				for _, pe := range wbCache.ListPending() {
					if !shadowStore.Has(pe.Meta.Path) {
						if err := shadowStore.WriteFull(pe.Meta.Path, pe.Data, pe.Meta.BaseRev); err != nil {
							fmt.Fprintf(os.Stderr, "drive9: migrate legacy entry %s to shadow: %v\n", pe.Meta.Path, err)
						}
					}
				}
			}

			// Initialize CommitQueue for background remote commits.
			if shadowStore != nil && pendingIdx != nil {
				cqMaxPending := maxCommitQueuePending
				if opts.CommitQueueMaxPending > 0 {
					cqMaxPending = opts.CommitQueueMaxPending
				}
				cq := NewCommitQueue(c, shadowStore, pendingIdx, journal, opts.UploadConcurrency, cqMaxPending, opts.RemoteRoot)
				cq.SetLayerRef(opts.LayerRef)
				cq.SetPerfCounters(dat9fs.perf)
				cq.OnSuccess = dat9fs.onCommitQueueSuccess
				cq.OnCleanup = dat9fs.onCommitQueueCleanup
				cq.PathLock = dat9fs.lockRemoteCommitPath
				if opts.WritePolicy == WritePolicyWriteBack && opts.WriteBackBatchWindow > 0 {
					cq.ConfigureBatchWrite(opts.WriteBackBatchWindow, opts.WriteBackBatchMaxFiles, opts.WriteBackBatchMaxBytes)
				}
				cq.RecoverPending()
				shadowStore.RecoverPendingBytes()
				if opts.LayerRef != "" {
					if err := restoreLayerEntries(context.Background(), c, opts, shadowStore, pendingIdx, dat9fs); err != nil {
						return fmt.Errorf("mount: restore fs layer entries: %w", err)
					}
					layerEventWatcherStop = StartLayerEventWatcher(dat9fs, c, opts, shadowStore, pendingIdx)
				}
				dat9fs.commitQueue = cq
			}

			if wbCache != nil {
				uploader := NewWriteBackUploader(c, wbCache, opts.UploadConcurrency, opts.RemoteRoot)
				uploader.SetPerfCounters(dat9fs.perf)
				uploader.OnSuccess = dat9fs.onWriteBackUploadSuccess
				uploader.SnapshotStagingGens = dat9fs.snapshotStagingGens
				dat9fs.SetWriteBack(wbCache, uploader)
				// Recover pending uploads only when the newer commit queue is
				// unavailable. Otherwise commitQueue owns shadow-backed recovery.
				if dat9fs.commitQueue == nil {
					uploader.RecoverPending()
				}
			}
		}
	}

	// Configure FUSE mount options
	fuseOpts := newGoFuseMountOptions(opts)

	// Reexec child path: if this process was spawned by a parent's Reexec(),
	// receive the FUSE fd via the handshake instead of mounting a new one.
	if IsReexecChild() {
		return mountReexecChild(dat9fs, opts, fuseOpts, c, actorID, layerEventWatcherStop)
	}

	// Create FUSE server
	server, err := gofuse.NewServer(dat9fs, opts.MountPoint, fuseOpts)
	if err != nil {
		cleanupNewServerFailure(opts.MountPoint, err, layerEventWatcherStop, dat9fs.FlushAll, forceUnmount, nil)
		return fmt.Errorf("fuse mount: %w", err)
	}

	// Start SSE watcher after WaitMount. The initial stream reset invalidates
	// kernel/user-space caches; if it races go-fuse's WaitMount pollHack open
	// on Linux, the kernel can reject .go-fuse-epoll-hack with EACCES/EPERM.
	//
	// The layer event watcher (started earlier) is deliberately left running:
	// it does no initial blanket cache invalidation and its first poll only
	// fires after a 1s tick on real remote events, so it cannot race pollHack.
	var sseWatcher *SSEWatcher
	dat9fs.markStatCacheUnverified()
	stopWatchers := func() {
		if sseWatcher != nil {
			sseWatcher.Stop()
		}
		layerEventWatcherStop()
	}

	err = serveWaitMountThenStartWatchers(server.Serve, server.WaitMount, func() {
		sseWatcher = StartSSEWatcher(dat9fs, c, actorID)
	}, func(err error) error {
		ok, probeErr := shouldContinueAfterWaitMountPermissionError(err, opts.MountPoint, probeMountPointReady)
		if ok {
			fmt.Fprintf(os.Stderr, "drive9: fuse wait mount permission error ignored after readiness probe passed at %s: %v\n", opts.MountPoint, err)
			return nil
		}
		if probeErr != nil {
			fmt.Fprintf(os.Stderr, "drive9: fuse wait mount permission fallback probe failed at %s: %v\n", opts.MountPoint, probeErr)
		}
		// NewServer succeeded and Serve is running, so cleanup can drain Drive9-side
		// workers before asking go-fuse to detach any partially mounted kernel state.
		cleanupMountStartFailure(mountStartCleanup{
			reason:       "fuse wait mount failure",
			mountPoint:   opts.MountPoint,
			cause:        err,
			stopWatchers: stopWatchers,
			flushAll:     dat9fs.FlushAll,
			unmount:      server.Unmount,
			forceUnmount: forceUnmount,
		})
		return fmt.Errorf("fuse wait mount: %w", err)
	})
	if err != nil {
		return err
	}
	controlServer, err := startMountControlServer(opts.MountPoint, dat9fs)
	if err != nil {
		cleanupMountStartFailure(mountStartCleanup{
			reason:       "mount control socket failure",
			mountPoint:   opts.MountPoint,
			cause:        err,
			stopWatchers: stopWatchers,
			flushAll:     dat9fs.FlushAll,
			unmount:      server.Unmount,
			forceUnmount: forceUnmount,
		})
		return fmt.Errorf("start mount control socket: %w", err)
	}
	if controlServer != nil {
		defer controlServer.Close()
	}
	stateMountPoint := opts.MountPoint
	if absMountPoint, absErr := filepath.Abs(stateMountPoint); absErr == nil {
		stateMountPoint = absMountPoint
	}
	if resolvedMountPoint, resolveErr := filepath.EvalSymlinks(stateMountPoint); resolveErr == nil {
		stateMountPoint = resolvedMountPoint
	}
	credentialKind := mountstate.CredentialKindAPIKey
	if opts.Token != "" {
		credentialKind = mountstate.CredentialKindToken
	}
	pidFile, err := mountstate.WriteProcessState(opts.MountPoint, mountstate.ProcessState{
		PID:                 os.Getpid(),
		Component:           "drive9-fuse",
		MountKind:           mountstate.MountKindFUSE,
		MountPoint:          stateMountPoint,
		RemoteRoot:          opts.RemoteRoot,
		Profile:             opts.Profile,
		LocalRoot:           opts.LocalRoot,
		Server:              opts.Server,
		PackPaths:           append([]string(nil), opts.PackPaths...),
		CredentialKind:      credentialKind,
		APIKey:              opts.APIKey,
		Token:               opts.Token,
		ProfileDir:          opts.Profiling.ProfileDir,
		PerfSamplesPath:     opts.Profiling.PerfSamplesPath,
		PerfInterval:        opts.Profiling.PerfSampleInterval.String(),
		PerfMaxSamples:      opts.Profiling.PerfMaxSamples,
		PerfMaxSampleFiles:  opts.Profiling.PerfMaxSampleFiles,
		PerfMaxProfileFiles: opts.Profiling.PerfMaxProfileFiles,
		PprofAddr:           opts.Profiling.PprofAddr,
		StartedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		HeapProfilePath:     opts.Profiling.HeapProfilePath,
		ControlSocket:       controlServer.SocketPath(),
	})
	if err != nil {
		if controlServer != nil {
			controlServer.Close()
		}
		cleanupMountStartFailure(mountStartCleanup{
			reason:       "mount process state failure",
			mountPoint:   opts.MountPoint,
			cause:        err,
			stopWatchers: stopWatchers,
			flushAll:     dat9fs.FlushAll,
			unmount:      server.Unmount,
			forceUnmount: forceUnmount,
		})
		return fmt.Errorf("write mount pid file: %w", err)
	}
	defer func() {
		if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "drive9: remove mount pid file %s: %v\n", pidFile, err)
		}
	}()

	shutdown := newMountShutdown(stopWatchers, dat9fs.FlushAll)

	// Signal handling for graceful shutdown.
	//
	// First SIGINT/SIGTERM:
	//   1. Start a progress reporter goroutine that prints commit-queue
	//      depth every 2s so the user knows we're not hung.
	//   2. Call shutdown() — flushes open fds, drains commit queue.
	//   3. Retry server.Unmount() up to 5 times (EBUSY is transient).
	//   4. Fall back to forceUnmount on retry exhaustion.
	//
	// Second SIGINT/SIGTERM during step 1-3:
	//   - Tell the user how much work is being abandoned (count + bytes).
	//   - Surface where local state is preserved so they can recover or
	//     inspect it. Do NOT promise automatic resume — recovery is
	//     best-effort: ShadowSpill (large) files take the terminal-failure
	//     branch on conflict (commit_queue.go:716-722) to avoid OOMing
	//     during full-file byte comparison.
	//   - forceUnmount the mountpoint so re-mount doesn't fail with
	//     "Permission denied" (the FUSE endpoint must be released).
	//   - Exit with code 1.
	//
	// Buffer size 2: outer goroutine consumes the first signal, inner
	// goroutine the second. Buffer ≥ 2 ensures signals delivered between
	// the outer's <-sigCh return and the inner goroutine being scheduled
	// don't get dropped by signal.Notify.
	//
	// Headless/daemon limitation: if shutdown() blocks indefinitely on a
	// stuck commit-queue worker (no context cancellation), an interactive
	// user can press Ctrl+C twice; daemon operators must send a second
	// SIGTERM (e.g. via systemd's TimeoutStopSec) to trigger force-quit.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// SIGHUP triggers a clean-state binary reexec: drain → quiesce →
	// export fd → spawn child → hand off → exit. If reexec succeeds, the
	// old process exits without unmounting; the new process inherits the
	// FUSE connection. If reexec fails, the old process resumes serving.
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	go func() {
		for range sighupCh {
			fmt.Fprintf(os.Stderr, "drive9: SIGHUP received, starting reexec...\n")
			result := dat9fs.Reexec(ReexecConfig{
				MountPoint: opts.MountPoint,
			})
			if result.Accepted {
				fmt.Fprintf(os.Stderr, "drive9: reexec succeeded, new process is serving. Exiting.\n")
				// Stop watchers and control socket before exit.
				// Do NOT call server.Unmount() — the new process owns the fd.
				stopWatchers()
				if controlServer != nil {
					controlServer.Close()
				}
				if pidFile != "" {
					_ = os.Remove(pidFile)
				}
				os.Exit(0)
			}
			fmt.Fprintf(os.Stderr, "drive9: reexec failed: %v (resuming normal serving)\n", result.Err)
		}
	}()

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\ndrive9: unmounting %s...\n", opts.MountPoint)

		// Periodic progress reporter — stops when progressDone is closed.
		// Distinguishes commit-queue uploads (where we have stats) from
		// other drain phases (debouncer flush, per-fd flush, write-back
		// drain) so users don't see "still uploading 0 files" and assume
		// the process is hung.
		progressDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					var n int
					var b int64
					if dat9fs.commitQueue != nil {
						n, b = dat9fs.commitQueue.PendingStats()
					}
					if n > 0 {
						fmt.Fprintf(os.Stderr,
							"drive9: still uploading %d files (%s) — Ctrl+C again to force-quit\n",
							n, humanizeBytes(b))
					} else {
						fmt.Fprintf(os.Stderr,
							"drive9: still draining — Ctrl+C again to force-quit\n")
					}
				}
			}
		}()

		// Forced-exit handler for the second signal.
		go func() {
			<-sigCh
			n, b := 0, int64(0)
			if dat9fs.commitQueue != nil {
				n, b = dat9fs.commitQueue.PendingStats()
			}
			if n > 0 {
				stateLoc := cacheBase
				if stateLoc == "" {
					stateLoc = "<cache disabled>"
				}
				fmt.Fprintf(os.Stderr,
					"drive9: force-quit — abandoning %d files (%s); local state preserved in %s, but recovery is best-effort\n",
					n, humanizeBytes(b), stateLoc)
			} else {
				fmt.Fprintf(os.Stderr, "drive9: force-quit\n")
			}
			forceUnmount(opts.MountPoint)
			if controlServer != nil {
				controlServer.Close()
			}
			if pidFile != "" {
				_ = os.Remove(pidFile)
			}
			os.Exit(1)
		}()

		shutdown()
		close(progressDone)

		// Retry unmount up to 5 times — EBUSY is transient (Spotlight, Finder).
		const maxRetries = 5
		var unmountErr error
		for i := 0; i < maxRetries; i++ {
			if unmountErr = server.Unmount(); unmountErr == nil {
				return
			}
			fmt.Fprintf(os.Stderr, "drive9: unmount attempt %d/%d failed: %v\n", i+1, maxRetries, unmountErr)
			if i < maxRetries-1 {
				time.Sleep(500 * time.Millisecond)
			}
		}

		// All retries exhausted — force unmount via OS tool.
		fmt.Fprintf(os.Stderr, "drive9: retries exhausted, force-unmounting %s\n", opts.MountPoint)
		forceUnmount(opts.MountPoint)
	}()

	fmt.Fprintf(os.Stderr, "drive9: mounted on %s (server: %s, actor: %s, readonly: %v, write_policy: %s, cache: %s, shadow: %s)\n",
		opts.MountPoint, opts.Server, actorID, opts.ReadOnly, opts.WritePolicy, cacheBase, shadowDir)
	server.Wait()
	shutdown()
	return nil
}

// mountReexecChild handles the child side of a clean-state binary reexec.
// It is called from Mount() when IsReexecChild() returns true. Instead of
// creating a new FUSE server via NewServer, it receives the fd from the
// parent's Reexec() handshake and imports it.
func mountReexecChild(dat9fs *Dat9FS, opts *MountOptions, fuseOpts *gofuse.MountOptions, c *client.Client, actorID string, layerEventWatcherStop func()) error {
	cfg, err := ParseReexecChildEnv()
	if err != nil {
		return fmt.Errorf("reexec child: %w", err)
	}

	fd, parentConn, err := ReexecChildHandshake(cfg)
	if err != nil {
		return fmt.Errorf("reexec child: %w", err)
	}
	defer func() { _ = parentConn.Close() }()

	fmt.Fprintf(os.Stderr, "drive9: reexec child: fd received, importing...\n")

	server, err := ReexecChildImportAndServe(fd, cfg.MountPoint, dat9fs, fuseOpts)
	if err != nil {
		return fmt.Errorf("reexec child: %w", err)
	}

	// From here on, the child is serving. Set up watchers, control socket,
	// and signal handling as in the normal mount path.
	dat9fs.server = server

	sseWatcher := StartSSEWatcher(dat9fs, c, actorID)
	stopWatchers := func() {
		if sseWatcher != nil {
			sseWatcher.Stop()
		}
		layerEventWatcherStop()
	}

	controlServer, err := startMountControlServer(opts.MountPoint, dat9fs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drive9: reexec child: control socket failed: %v\n", err)
	}
	if controlServer != nil {
		defer controlServer.Close()
	}

	pidFile, err := mountstate.WriteProcessState(opts.MountPoint, mountstate.ProcessState{
		PID:            os.Getpid(),
		Component:      "drive9-fuse",
		MountKind:      mountstate.MountKindFUSE,
		MountPoint:     opts.MountPoint,
		RemoteRoot:     opts.RemoteRoot,
		Server:         opts.Server,
		StartedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		ControlSocket:  controlServer.SocketPath(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "drive9: reexec child: pid file failed: %v\n", err)
	}
	if pidFile != "" {
		defer func() {
			if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "drive9: remove mount pid file %s: %v\n", pidFile, err)
			}
		}()
	}

	// Send accept AFTER control socket and pidfile are established.
	// This ensures the parent cannot race-delete the child's resources
	// during its post-accept cleanup.
	if err := SendReexecAccept(parentConn); err != nil {
		return fmt.Errorf("reexec child: %w", err)
	}
	fmt.Fprintf(os.Stderr, "drive9: reexec child: accept sent, parent will exit\n")

	shutdown := newMountShutdown(stopWatchers, dat9fs.FlushAll)

	// Signal handling (same as normal mount).
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// SIGHUP for chained reexec.
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	go func() {
		for range sighupCh {
			fmt.Fprintf(os.Stderr, "drive9: SIGHUP received, starting reexec...\n")
			result := dat9fs.Reexec(ReexecConfig{MountPoint: opts.MountPoint})
			if result.Accepted {
				fmt.Fprintf(os.Stderr, "drive9: reexec succeeded, new process is serving. Exiting.\n")
				stopWatchers()
				if controlServer != nil {
					controlServer.Close()
				}
				if pidFile != "" {
					_ = os.Remove(pidFile)
				}
				os.Exit(0)
			}
			fmt.Fprintf(os.Stderr, "drive9: reexec failed: %v (resuming normal serving)\n", result.Err)
		}
	}()

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\ndrive9: unmounting %s...\n", opts.MountPoint)
		go func() {
			<-sigCh
			fmt.Fprintf(os.Stderr, "drive9: force-quit\n")
			forceUnmount(opts.MountPoint)
			os.Exit(1)
		}()
		shutdown()
		if unmountErr := server.Unmount(); unmountErr != nil {
			fmt.Fprintf(os.Stderr, "drive9: unmount failed: %v, force-unmounting\n", unmountErr)
			forceUnmount(opts.MountPoint)
		}
	}()

	fmt.Fprintf(os.Stderr, "drive9: reexec child serving on %s (server: %s)\n", opts.MountPoint, opts.Server)
	server.Wait()
	shutdown()
	return nil
}

type mountStartCleanup struct {
	reason       string
	mountPoint   string
	cause        error
	stopWatchers func()
	flushAll     func()
	unmount      func() error
	forceUnmount func(string)
	// forceUnmountWithoutServer is for the go-fuse NewServer post-mount/pre-server
	// INIT failure path, where no Server exists to call Unmount on.
	forceUnmountWithoutServer bool
	logf                      func(string, ...any)
}

func cleanupMountStartFailure(cleanup mountStartCleanup) {
	reason := strings.TrimSpace(cleanup.reason)
	if reason == "" {
		reason = "mount start failure"
	}
	mountPoint := strings.TrimSpace(cleanup.mountPoint)
	mountPointLabel := mountPoint
	if mountPointLabel == "" {
		mountPointLabel = "<unknown>"
	}
	logf := cleanup.logf
	if logf == nil {
		logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}
	if cleanup.cause != nil {
		logf("drive9: mount startup failed during %s at %s: %v\n", reason, mountPointLabel, cleanup.cause)
	} else {
		logf("drive9: mount startup failed during %s at %s\n", reason, mountPointLabel)
	}
	if cleanup.stopWatchers != nil {
		cleanup.stopWatchers()
	}
	if cleanup.flushAll != nil {
		cleanup.flushAll()
	}
	if cleanup.unmount == nil {
		if cleanup.forceUnmountWithoutServer && cleanup.forceUnmount != nil && mountPoint != "" {
			logf("drive9: cleanup after %s: forcing unmount of %s after partial init failure\n", reason, mountPoint)
			cleanup.forceUnmount(mountPoint)
		}
		return
	}
	if err := cleanup.unmount(); err != nil {
		logf("drive9: cleanup after %s: unmount failed: %v\n", reason, err)
		if cleanup.forceUnmount != nil && mountPoint != "" {
			cleanup.forceUnmount(mountPoint)
		}
		return
	}
	if mountPoint != "" {
		logf("drive9: cleanup after %s: unmounted %s\n", reason, mountPoint)
	} else {
		logf("drive9: cleanup after %s: unmounted mountpoint\n", reason)
	}
}

func cleanupNewServerFailure(
	mountPoint string,
	cause error,
	stopWatchers func(),
	flushAll func(),
	forceUnmount func(string),
	logf func(string, ...any),
) {
	cleanupMountStartFailure(mountStartCleanup{
		reason:                    "fuse server initialization failure",
		mountPoint:                mountPoint,
		cause:                     cause,
		stopWatchers:              stopWatchers,
		flushAll:                  flushAll,
		forceUnmount:              forceUnmount,
		forceUnmountWithoutServer: shouldForceUnmountAfterNewServerError(cause),
		logf:                      logf,
	})
}

// shouldForceUnmountAfterNewServerError targets go-fuse's post-mount INIT
// failure. In github.com/mornyx/go-fuse/v2, NewServer calls mount(), then
// handleInit(), and returns fmt.Errorf("init: %s", code) if INIT fails after
// closing the mount fd. That path has no Server handle for Unmount, so Drive9
// must detach the mountpoint itself. Re-check this predicate when go-fuse is
// upgraded or replaced.
func shouldForceUnmountAfterNewServerError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "init:")
}

func serveWaitMountThenStartWatchers(
	serve func(),
	waitMount func() error,
	startWatchers func(),
	handleWaitMountError func(error) error,
) error {
	// Serve must be running before WaitMount can proceed. On macOS,
	// mount_macfuse waits for STATFS before signalling ready, and that STATFS
	// is handled by the serve loop.
	//
	// WaitMount then runs go-fuse's pollHack, which opens
	// .go-fuse-epoll-hack inside the mountpoint to trigger _OP_POLL so
	// go-fuse can reply ENOSYS. Without this, macOS may later send _OP_POLL
	// and deadlock the Go runtime. Start watchers only after this readiness
	// probe finishes so an initial remote reset cannot invalidate the mount
	// while go-fuse is still proving readiness.
	go serve()
	if err := waitMount(); err != nil {
		if handleWaitMountError == nil {
			return err
		}
		if err := handleWaitMountError(err); err != nil {
			return err
		}
	}
	if startWatchers != nil {
		startWatchers()
	}
	return nil
}

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
	// the canonical EACCES/EPERM strings. Keep this even though errors.Is above
	// covers the wrapped case.
	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "permission denied") || strings.Contains(errText, "operation not permitted")
}

// probeMountPointReadyTimeout bounds the readiness probe so a half-broken serve
// loop cannot hang startup forever on the fallback path.
const probeMountPointReadyTimeout = 5 * time.Second

// probeMountPointReady confirms the mountpoint is an active, usable mount after
// a Linux permission-like WaitMount error. The active-mount check prevents the
// probe from accepting the plain directory that Mount creates before go-fuse
// mounts over it; once the mount is active, stat/open/readdir are served through
// go-fuse and a passing probe means the kernel<->serve-loop path works despite
// WaitMount's pollHack hitting EACCES/EPERM.
// The stat/open/readdir below can block on a wedged serve loop, so the work runs
// under a timeout (see probeMountPointReadyTimeout).
func probeMountPointReady(mountPoint string) error {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return fmt.Errorf("empty mountpoint")
	}

	done := make(chan error, 1)
	go func() {
		done <- probeMountPointReadyOnce(mountPoint)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(probeMountPointReadyTimeout):
		return fmt.Errorf("mountpoint readiness probe timed out after %s", probeMountPointReadyTimeout)
	}
}

func probeMountPointReadyOnce(mountPoint string) error {
	return probeMountPointReadyOnceWithMountCheck(mountPoint, activeMountPoint)
}

func probeMountPointReadyOnceWithMountCheck(mountPoint string, isActiveMountPoint func(string) (bool, error)) error {
	info, err := os.Stat(mountPoint)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("mountpoint is not a directory")
	}
	mounted, err := isActiveMountPoint(mountPoint)
	if err != nil {
		return err
	}
	if !mounted {
		return fmt.Errorf("mountpoint is not an active mount")
	}
	dir, err := os.Open(mountPoint)
	if err != nil {
		return err
	}
	var readErr error
	if _, err := dir.Readdirnames(1); err != nil && !errors.Is(err, io.EOF) {
		readErr = err
	}
	if err := dir.Close(); err != nil {
		return err
	}
	return readErr
}

func validateMountOptionsProfile(opts *MountOptions) error {
	if !validMountProfile(opts.Profile) {
		return fmt.Errorf("mount: unknown profile %q", opts.Profile)
	}
	opts.LocalRoot = strings.TrimSpace(opts.LocalRoot)
	if opts.EnableGitWorkspaces && opts.LocalRoot == "" {
		return fmt.Errorf("mount: EnableGitWorkspaces requires LocalRoot")
	}
	if opts.WriteBackBatchWindow < 0 {
		return fmt.Errorf("mount: WriteBackBatchWindow must be >= 0")
	}
	if opts.WriteBackBatchMaxFiles < 0 {
		return fmt.Errorf("mount: WriteBackBatchMaxFiles must be >= 0")
	}
	if opts.WriteBackBatchMaxBytes < 0 {
		return fmt.Errorf("mount: WriteBackBatchMaxBytes must be >= 0")
	}
	if opts.WriteBackBatchWindow > 0 && opts.WritePolicy != WritePolicyWriteBack {
		return fmt.Errorf("mount: WriteBackBatchWindow requires writeback policy")
	}
	hasOverlayOptions := opts.LocalRoot != "" || len(opts.LocalOnlyPatterns) > 0 || len(opts.RemoteOnlyPatterns) > 0 || len(opts.PackPaths) > 0
	if !profileAllowsLocalPolicy(opts.Profile) {
		if hasOverlayOptions {
			return fmt.Errorf("mount: overlay options require an overlay profile")
		}
		return nil
	}
	if opts.LocalRoot == "" {
		return fmt.Errorf("mount: profile %q requires LocalRoot", opts.Profile)
	}
	if !filepath.IsAbs(opts.LocalRoot) {
		return fmt.Errorf("mount: LocalRoot must be an absolute path")
	}
	if err := validateLocalPolicyPatterns(opts.LocalOnlyPatterns, opts.RemoteOnlyPatterns); err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	return nil
}

func restoreLayerEntries(ctx context.Context, c *client.Client, opts *MountOptions, shadows *ShadowStore, pending *PendingIndex, fs *Dat9FS) error {
	if c == nil || opts == nil || shadows == nil || pending == nil || strings.TrimSpace(opts.LayerRef) == "" {
		return nil
	}
	var maxSeq int64
	hasCheckpoint := false
	if strings.TrimSpace(opts.CheckpointRef) != "" {
		checkpoint, err := c.GetFSLayerCheckpoint(ctx, opts.CheckpointRef)
		if err != nil {
			return fmt.Errorf("read fs layer checkpoint %s: %w", opts.CheckpointRef, err)
		}
		if checkpoint.LayerID != opts.LayerRef {
			return fmt.Errorf("checkpoint %s belongs to layer %s, want %s", opts.CheckpointRef, checkpoint.LayerID, opts.LayerRef)
		}
		maxSeq = checkpoint.DurableSeq
		hasCheckpoint = true
	}
	var entries []client.FSLayerEntry
	var err error
	if hasCheckpoint {
		entries, err = c.ReplayFSLayerAtSeq(ctx, opts.LayerRef, maxSeq)
	} else {
		entries, err = c.ReplayFSLayer(ctx, opts.LayerRef)
	}
	if err != nil {
		return err
	}
	restoredUpserts := make(map[string]struct{})
	for _, entry := range entries {
		localPath, ok := mountpath.ToLocal(opts.RemoteRoot, entry.Path)
		if !ok {
			continue
		}
		switch entry.Op {
		case "whiteout":
			if fs != nil {
				fs.markLayerWhiteout(localPath)
			}
			continue
		case "mkdir":
			if fs != nil {
				fs.markLayerDir(localPath, entry.Mode)
			}
			continue
		case "chmod":
			if pending != nil {
				if err := pending.UpdateMode(localPath, entry.Mode); err != nil {
					return fmt.Errorf("restore fs layer chmod pending %s: %w", localPath, err)
				}
			}
			if fs != nil {
				switch entry.Kind {
				case "file":
					fs.markLayerFileMode(localPath, entry.Mode)
				case "dir":
					fs.markLayerDir(localPath, entry.Mode)
				case "symlink":
					if target, existingMode, ok := fs.layerSymlink(localPath); ok {
						nextMode := (existingMode &^ uint32(0o777)) | (entry.Mode & 0o777)
						if nextMode&uint32(syscall.S_IFMT) == 0 {
							nextMode |= uint32(syscall.S_IFLNK)
						}
						fs.markLayerSymlink(localPath, target, nextMode)
					}
				}
			}
			continue
		case "rename":
			if err := restoreLayerRenameEntry(ctx, c, opts, shadows, pending, fs, localPath, &entry, layerEntryFetchMaxSeq(&entry, hasCheckpoint, maxSeq)); err != nil {
				return err
			}
			continue
		case "symlink":
			fullEntry := &entry
			if strings.TrimSpace(fullEntry.ContentText) == "" && len(fullEntry.Content) == 0 {
				fetched, err := getLayerEntryForRestore(ctx, c, opts.LayerRef, entry.Path, layerEntryFetchMaxSeq(&entry, hasCheckpoint, maxSeq))
				if err != nil {
					return fmt.Errorf("restore fs layer symlink entry %s: %w", entry.Path, err)
				}
				fullEntry = fetched
			}
			target := strings.TrimSpace(fullEntry.ContentText)
			if target == "" && len(fullEntry.Content) > 0 {
				target = string(fullEntry.Content)
			}
			if target == "" {
				return fmt.Errorf("restore fs layer symlink entry %s: missing target", entry.Path)
			}
			if fs != nil {
				fs.markLayerSymlink(localPath, target, entry.Mode)
			}
			continue
		}
		if entry.Op != "upsert" || entry.Kind != "file" {
			continue
		}
		if _, ok := pending.GetMeta(localPath); ok {
			if _, restored := restoredUpserts[localPath]; !restored {
				continue
			}
		}
		entryMaxSeq := layerEntryFetchMaxSeq(&entry, hasCheckpoint, maxSeq)
		fullEntry, err := getLayerEntryForRestore(ctx, c, opts.LayerRef, entry.Path, entryMaxSeq)
		if err != nil {
			return fmt.Errorf("restore fs layer entry %s: %w", entry.Path, err)
		}
		var sizeBytes int64
		if fullEntry.StorageRef != "" || fullEntry.StorageType == "s3" {
			rc, err := c.ReadFSLayerFileStream(ctx, opts.LayerRef, entry.Path, entryMaxSeq)
			if err != nil {
				return fmt.Errorf("restore fs layer object %s: %w", entry.Path, err)
			}
			n, writeErr := shadows.WriteStream(localPath, rc, fullEntry.BaseRevision)
			closeErr := rc.Close()
			if writeErr != nil {
				return fmt.Errorf("restore fs layer shadow %s: %w", localPath, writeErr)
			}
			if closeErr != nil {
				return fmt.Errorf("restore fs layer object %s: %w", entry.Path, closeErr)
			}
			sizeBytes = n
		} else {
			content := fullEntry.Content
			if err := shadows.WriteFull(localPath, content, fullEntry.BaseRevision); err != nil {
				return fmt.Errorf("restore fs layer shadow %s: %w", localPath, err)
			}
			sizeBytes = int64(len(content))
		}
		if fullEntry.SizeBytes > 0 && sizeBytes != fullEntry.SizeBytes {
			return fmt.Errorf("restore fs layer object %s: copied %d bytes, want %d", entry.Path, sizeBytes, fullEntry.SizeBytes)
		}
		if _, err := pending.PutWithBaseRevAndMode(localPath, sizeBytes, PendingOverwrite, fullEntry.BaseRevision, fullEntry.Mode, fullEntry.Mode != 0); err != nil {
			return fmt.Errorf("restore fs layer pending %s: %w", localPath, err)
		}
		if fs != nil {
			if fullEntry.Mode != 0 {
				fs.markLayerFileMode(localPath, fullEntry.Mode)
			} else {
				fs.markLayerFile(localPath)
			}
		}
		restoredUpserts[localPath] = struct{}{}
	}
	return nil
}

func layerEntryFetchMaxSeq(entry *client.FSLayerEntry, hasCheckpoint bool, checkpointMaxSeq int64) *int64 {
	if entry != nil && entry.EntrySeq > 0 {
		seq := entry.EntrySeq
		return &seq
	}
	if hasCheckpoint {
		seq := checkpointMaxSeq
		return &seq
	}
	return nil
}

func getLayerEntryForRestore(ctx context.Context, c *client.Client, layerID, path string, maxSeq *int64) (*client.FSLayerEntry, error) {
	if maxSeq != nil {
		return c.GetFSLayerEntryAtSeq(ctx, layerID, path, *maxSeq)
	}
	return c.GetFSLayerEntry(ctx, layerID, path)
}

func restoreLayerRenameEntry(ctx context.Context, c *client.Client, opts *MountOptions, shadows *ShadowStore, pending *PendingIndex, fs *Dat9FS, oldLocalPath string, entry *client.FSLayerEntry, maxSeq *int64) error {
	if entry == nil {
		return nil
	}
	fullEntry := entry
	if strings.TrimSpace(fullEntry.ContentText) == "" && len(fullEntry.Content) == 0 {
		fetched, err := getLayerEntryForRestore(ctx, c, opts.LayerRef, entry.Path, maxSeq)
		if err != nil {
			return fmt.Errorf("restore fs layer rename entry %s: %w", entry.Path, err)
		}
		fullEntry = fetched
	}
	targetRemote := strings.TrimSpace(fullEntry.ContentText)
	if targetRemote == "" && len(fullEntry.Content) > 0 {
		targetRemote = strings.TrimSpace(string(fullEntry.Content))
	}
	if targetRemote == "" {
		return fmt.Errorf("restore fs layer rename entry %s: missing target", entry.Path)
	}
	if fs != nil {
		fs.markLayerWhiteout(oldLocalPath)
	}
	newLocalPath, ok := mountpath.ToLocal(opts.RemoteRoot, targetRemote)
	if !ok {
		return nil
	}
	movedShadow := shadows.Rename(oldLocalPath, newLocalPath)
	movedPending := pending.RenamePending(oldLocalPath, newLocalPath)
	if movedShadow || movedPending {
		return nil
	}
	if pending.HasPending(newLocalPath) {
		return nil
	}
	data, err := c.ReadCtx(ctx, fullEntry.Path)
	if err != nil {
		return fmt.Errorf("restore fs layer renamed source %s: %w", fullEntry.Path, err)
	}
	if err := shadows.WriteFull(newLocalPath, data, 0); err != nil {
		return fmt.Errorf("restore fs layer renamed shadow %s: %w", newLocalPath, err)
	}
	if _, err := pending.PutWithBaseRevAndMode(newLocalPath, int64(len(data)), PendingOverwrite, 0, fullEntry.Mode, fullEntry.Mode != 0); err != nil {
		return fmt.Errorf("restore fs layer renamed pending %s: %w", newLocalPath, err)
	}
	return nil
}

func mountCredentialKind(opts *MountOptions) string {
	if opts.Token != "" {
		return "token"
	}
	return "api_key"
}

func mountCredentialSecret(opts *MountOptions) string {
	if opts.Token != "" {
		return opts.Token
	}
	return opts.APIKey
}

func transientOverlayRoot(cacheBase, readCacheHash string) string {
	return filepath.Join(cacheBase, readCacheHash, "transient", transientOverlayMountID())
}

func transientOverlayMountID() string {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err == nil {
		return fmt.Sprintf("%d-%s", os.Getpid(), hex.EncodeToString(suffix[:]))
	}
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
}

func newGoFuseMountOptions(opts *MountOptions) *gofuse.MountOptions {
	fuseOpts := &gofuse.MountOptions{
		FsName:             "drive9",
		Name:               "drive9",
		MaxReadAhead:       8 * 1024 * 1024, // 8MB — larger readahead reduces FUSE kernel↔userspace switches
		MaxWrite:           128 * 1024,      // 128KB per write request (default 64KB)
		MaxBackground:      32,              // concurrent background FUSE requests (default 12)
		SyncRead:           opts.SyncRead,   // disables FUSE_CAP_ASYNC_READ; one read in flight per file handle
		DirectMountStrict:  opts.DirectMountStrict,
		EnableLocks:        true,
		Debug:              opts.Debug,
		AllowOther:         opts.AllowOther,
		EnableDirectIoMmap: true, // allow mmap on FOPEN_DIRECT_IO handles (e.g. SQLite *.db with mmap_size>0); no-op on kernels without CAP_DIRECT_IO_ALLOW_MMAP
	}
	if runtime.GOOS == "linux" {
		fuseOpts.MaxWrite = 1024 * 1024 // 1MiB — Linux FUSE supports this natively
		if opts.AllowOther {
			fuseOpts.Options = append(fuseOpts.Options, "default_permissions")
		}
	}
	if runtime.GOOS == "darwin" {
		// macFUSE can reject open/readdir before requests reach the daemon if
		// it performs local permission checks or treats the volume as a
		// privacy-gated network volume. drive9 authorization is remote, so
		// defer permission decisions to the filesystem handlers and present the
		// mount as local to ordinary CLI tools.
		fuseOpts.Options = append(fuseOpts.Options, "defer_permissions", "local")
	}
	if opts.ReadOnly {
		fuseOpts.Options = append(fuseOpts.Options, "ro")
	}
	return fuseOpts
}

func newMountShutdown(stopWatcher func(), flushAll func()) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			stopWatcher()
			flushAll()
		})
	}
}

// forceUnmount shells out to OS-specific tools to force-unmount a FUSE mount.
// Uses a 5-second timeout so that the forced-exit path can't itself hang
// on a wedged FUSE endpoint.
func forceUnmount(mountpoint string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.CommandContext(ctx, "diskutil", "unmount", "force", mountpoint)
	} else {
		// Linux: prefer fusermount3 (fuse3, default on Ubuntu 22.04+ /
		// Debian 12+), then fusermount, then umount -l.
		if _, err := exec.LookPath("fusermount3"); err == nil {
			cmd = exec.CommandContext(ctx, "fusermount3", "-u", mountpoint)
		} else if _, err := exec.LookPath("fusermount"); err == nil {
			cmd = exec.CommandContext(ctx, "fusermount", "-u", mountpoint)
		} else {
			cmd = exec.CommandContext(ctx, "umount", "-l", mountpoint)
		}
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Fprintf(os.Stderr, "drive9: force unmount timed out after 5s\n")
		} else {
			fmt.Fprintf(os.Stderr, "drive9: force unmount failed: %v\n", err)
		}
	}
}

// humanizeBytes formats a byte count as a human-readable string (e.g. "1.5 MB").
// Uses 1024-base (matches du -h, df -h, kernel reporting).
func humanizeBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// generateMountID creates a random 16-byte hex ID for this mount instance.
func generateMountID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp if crypto/rand fails (shouldn't happen).
		return fmt.Sprintf("mount-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
