package fuse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
	"github.com/mem9-ai/dat9/pkg/mountstate"
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
	Server                string        // drive9 server URL
	APIKey                string        // owner API key (mutually exclusive with Token)
	Token                 string        // delegated capability JWT (mutually exclusive with APIKey)
	MountPoint            string        // local mount point
	RemoteRoot            string        // remote subtree root (default "/"); set via "drive9 mount :/path /local"
	CacheDir              string        // write-back cache directory (default ~/.cache/drive9); empty string uses default
	CacheSize             int64         // ReadCache max size in bytes (default 128MB)
	ReadCacheMaxFileBytes int64         // largest single file admitted to ReadCache (default 1MiB)
	DirTTL                time.Duration // DirCache TTL (default 10s)
	AttrTTL               time.Duration // kernel attr cache TTL (default 60s)
	EntryTTL              time.Duration // kernel entry cache TTL (default 60s)
	NegativeEntryTTL      time.Duration // kernel negative entry cache TTL (default 10s)
	FlushDebounce         time.Duration // debounce window for small-file flush coalescing (default 2s, 0 disables); set to -1 to use default
	SyncMode              SyncMode      // interactive, strict, or auto (default auto)
	WritePolicy           WritePolicy   // writeback, close-sync, or write-sync (default writeback)
	Profile               string        // mount profile: "interactive", "" (default)
	UploadConcurrency     int           // number of background upload workers (default 4)
	ReadConcurrency       int           // maximum concurrent backend reads issued by FUSE (default 24)
	SyncRead              bool          // disable kernel async read dispatch; at most one read in flight per file handle
	LookupRetryCount      int           // detached retries after transient Lookup/GetAttr stat failures (default 2)
	LookupRetryTimeout    time.Duration // timeout per detached stat retry after interrupt/transient errors (default 250ms)
	LegacyDirStatFallback bool          // on Lookup stat 404, list parent to support legacy servers without directory stat
	ReadDirPrefetch       bool          // prefetch small files after readdir into ReadCache (default false)
	PrefetchMaxFiles      int           // maximum files prefetched per directory read (default 32 when enabled)
	PrefetchMaxFileBytes  int64         // maximum individual file size prefetched (default 50KB)
	PrefetchMaxBytes      int64         // maximum aggregate bytes prefetched per directory read (default 1MB)
	PrefetchTimeout       time.Duration // timeout for one readdir prefetch batch (default 1s)
	AllowOther            bool          // allow other users to access mount
	ReadOnly              bool          // mount as read-only
	Debug                 bool          // enable FUSE debug logging
	PerfCounters          bool          // print low-overhead FUSE perf counter summary on shutdown
	Profiling             ProfilingOptions
}

func (o *MountOptions) setDefaults() {
	// Apply profile defaults before generic defaults so profile-specific
	// zero-value options can take effect while explicit non-zero values win.
	if o.Profile == "interactive" {
		ApplyInteractiveProfile(o)
	}
	if o.CacheSize <= 0 {
		o.CacheSize = defaultReadCacheMaxSize
	}
	if o.ReadCacheMaxFileBytes <= 0 {
		o.ReadCacheMaxFileBytes = defaultReadCacheMaxFileSize
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
		o.NegativeEntryTTL = 10 * time.Second
	}
	// FlushDebounce: 0 means disabled, negative means unset (use default).
	if o.FlushDebounce < 0 {
		o.FlushDebounce = defaultFlushDebounce
	}
	if o.UploadConcurrency <= 0 {
		o.UploadConcurrency = 4
	}
	if o.ReadConcurrency <= 0 {
		o.ReadConcurrency = defaultRemoteReadConcurrency
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
}

// Mount creates and serves a FUSE mount. It blocks until the filesystem
// is unmounted or a signal (SIGINT, SIGTERM) is received.
func Mount(opts *MountOptions) error {
	opts.setDefaults()

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

	// Build FUSE filesystem
	dat9fs := NewDat9FS(c, opts)
	opts.Profiling.MountSync = dat9fs.SyncAll

	profiler, err := StartProfiler(opts.Profiling)
	if err != nil {
		return fmt.Errorf("start profiler: %w", err)
	}
	opts.Profiling.PprofAddr = profiler.PprofAddr()
	defer profiler.Stop()

	// Resolve sync mode (auto-detect RTT if needed).
	resolved := ResolveMode(context.Background(), opts.SyncMode, opts.Server)
	opts.SyncMode = resolved
	dat9fs.syncMode = resolved
	fmt.Fprintf(os.Stderr, "drive9: sync mode: %s\n", resolved)
	perfRecorder, err := StartContinuousPerf(opts.Profiling, dat9fs)
	if err != nil {
		return fmt.Errorf("start continuous perf: %w", err)
	}
	defer perfRecorder.Stop()

	// Initialize write-back cache, shadow store, and pending index.
	var cacheBase, shadowDir string
	if !opts.ReadOnly {
		cacheBase = opts.CacheDir
		if cacheBase == "" {
			home, err := os.UserHomeDir()
			if err == nil {
				cacheBase = filepath.Join(home, ".cache", "drive9")
			}
		}
		if cacheBase != "" {
			mh := MountHash(opts.Server, opts.MountPoint, opts.RemoteRoot)
			pendingDir := filepath.Join(cacheBase, mh, "pending")
			shadowDir = filepath.Join(cacheBase, mh, "shadow")

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

			// Initialize ShadowStore.
			shadowStore, err := NewShadowStore(shadowDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "drive9: shadow store init failed: %v (continuing without)\n", err)
			} else {
				dat9fs.shadowStore = shadowStore
			}

			// Initialize Journal WAL.
			journalPath := filepath.Join(cacheBase, mh, "journal.wal")
			journal, err := NewJournal(journalPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "drive9: journal init failed: %v (continuing without)\n", err)
			} else {
				dat9fs.journal = journal
				// Replay journal for crash recovery. Preserve the original kind
				// and base revision so CommitQueue.RecoverPending can re-enqueue.
				_ = journal.Replay(func(e JournalEntry) {
					if pendingIdx != nil && e.Op != JournalCommit && e.Op != JournalUnlink {
						if !pendingIdx.HasPending(e.Path) {
							kind := PendingOverwrite
							if e.BaseRev == 0 {
								kind = PendingNew
							}
							_, _ = pendingIdx.PutWithBaseRev(e.Path, e.Length, kind, e.BaseRev)
						}
					}
				})
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
				cq := NewCommitQueue(c, shadowStore, pendingIdx, journal, opts.UploadConcurrency, maxCommitQueuePending, opts.RemoteRoot)
				cq.SetPerfCounters(dat9fs.perf)
				cq.OnSuccess = dat9fs.onCommitQueueSuccess
				cq.OnCleanup = dat9fs.onCommitQueueCleanup
				cq.RecoverPending()
				dat9fs.commitQueue = cq
			}

			if wbCache != nil {
				uploader := NewWriteBackUploader(c, wbCache, opts.UploadConcurrency, opts.RemoteRoot)
				uploader.SetPerfCounters(dat9fs.perf)
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

	// Create FUSE server
	server, err := gofuse.NewServer(dat9fs, opts.MountPoint, fuseOpts)
	if err != nil {
		return fmt.Errorf("fuse mount: %w", err)
	}

	// Start SSE watcher for remote change notifications.
	sseWatcher := StartSSEWatcher(dat9fs, c, actorID)

	// Start serving in a background goroutine so WaitMount can proceed.
	// On macOS, Serve() must be running before WaitMount() returns because
	// mount_macfuse waits for STATFS (handled in the serve loop) before
	// signalling ready, and WaitMount then runs pollHack which triggers
	// a LOOKUP+OPEN+POLL through the mount point.
	go server.Serve()

	// WaitMount blocks until mount_macfuse exits (INIT+STATFS done) and
	// then runs pollHack, which opens .go-fuse-epoll-hack inside the mount
	// to trigger _OP_POLL so go-fuse can reply ENOSYS. Without this, macOS
	// may later send _OP_POLL and deadlock the Go runtime (the netpoller
	// thread consumes the last GOMAXPROCS slot, leaving no thread to handle
	// the POLL request from the kernel).
	if err := server.WaitMount(); err != nil {
		return fmt.Errorf("fuse wait mount: %w", err)
	}
	pidFile, err := mountstate.WriteProcessState(opts.MountPoint, mountstate.ProcessState{
		PID:             os.Getpid(),
		Component:       "drive9-fuse",
		MountPoint:      opts.MountPoint,
		RemoteRoot:      opts.RemoteRoot,
		Server:          opts.Server,
		ProfileDir:      opts.Profiling.ProfileDir,
		PerfJSONL:       opts.Profiling.PerfSamplesPath,
		PerfInterval:    opts.Profiling.PerfSampleInterval.String(),
		PerfMaxSamples:  opts.Profiling.PerfMaxSamples,
		PprofAddr:       opts.Profiling.PprofAddr,
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		CPUProfilePath:  opts.Profiling.CPUProfilePath,
		HeapProfilePath: opts.Profiling.HeapProfilePath,
	})
	if err != nil {
		sseWatcher.Stop()
		dat9fs.FlushAll()
		_ = server.Unmount()
		return fmt.Errorf("write mount pid file: %w", err)
	}
	defer func() {
		if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "drive9: remove mount pid file %s: %v\n", pidFile, err)
		}
	}()

	shutdown := newMountShutdown(sseWatcher.Stop, dat9fs.FlushAll)

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

func newGoFuseMountOptions(opts *MountOptions) *gofuse.MountOptions {
	fuseOpts := &gofuse.MountOptions{
		FsName:        "drive9",
		Name:          "drive9",
		MaxReadAhead:  8 * 1024 * 1024, // 8MB — larger readahead reduces FUSE kernel↔userspace switches
		MaxWrite:      128 * 1024,      // 128KB per write request (default 64KB)
		MaxBackground: 32,              // concurrent background FUSE requests (default 12)
		SyncRead:      opts.SyncRead,   // disables FUSE_CAP_ASYNC_READ; one read in flight per file handle
		Debug:         opts.Debug,
		AllowOther:    opts.AllowOther,
	}
	if runtime.GOOS == "linux" {
		fuseOpts.MaxWrite = 1024 * 1024 // 1MiB — Linux FUSE supports this natively
		if os.Geteuid() == 0 {
			fuseOpts.DirectMountStrict = true
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
