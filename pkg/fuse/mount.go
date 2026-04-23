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
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
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
	Server            string        // dat9 server URL
	APIKey            string        // owner API key (mutually exclusive with Token)
	Token             string        // delegated capability JWT (mutually exclusive with APIKey)
	MountPoint        string        // local mount point
	CacheDir          string        // write-back cache directory (default ~/.cache/drive9); empty string uses default
	CacheSize         int64         // ReadCache max size in bytes (default 128MB)
	DirTTL            time.Duration // DirCache TTL (default 5s)
	AttrTTL           time.Duration // kernel attr cache TTL (default 1s)
	EntryTTL          time.Duration // kernel entry cache TTL (default 1s)
	NegativeEntryTTL  time.Duration // kernel negative entry cache TTL (default 1s)
	FlushDebounce     time.Duration // debounce window for small-file flush coalescing (default 2s, 0 disables); set to -1 to use default
	SyncMode          SyncMode      // interactive, strict, or auto (default auto)
	Profile           string        // mount profile: "interactive", "" (default)
	UploadConcurrency int           // number of background upload workers (default 4)
	AllowOther        bool          // allow other users to access mount
	ReadOnly          bool          // mount as read-only
	Debug             bool          // enable FUSE debug logging
}

func (o *MountOptions) setDefaults() {
	if o.CacheSize <= 0 {
		o.CacheSize = defaultReadCacheMaxSize
	}
	if o.DirTTL <= 0 {
		o.DirTTL = defaultDirCacheTTL
	}
	if o.AttrTTL <= 0 {
		o.AttrTTL = 10 * time.Second
	}
	if o.EntryTTL <= 0 {
		o.EntryTTL = 10 * time.Second
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
	// Apply interactive profile if requested.
	if o.Profile == "interactive" {
		ApplyInteractiveProfile(o)
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
	if _, err := c.List("/"); err != nil {
		return fmt.Errorf("cannot reach dat9 server: %w", err)
	}

	// Build FUSE filesystem
	dat9fs := NewDat9FS(c, opts)

	// Resolve sync mode (auto-detect RTT if needed).
	resolved := ResolveMode(context.Background(), opts.SyncMode, opts.Server)
	dat9fs.syncMode = resolved
	fmt.Fprintf(os.Stderr, "dat9: sync mode: %s\n", resolved)

	// Initialize write-back cache, shadow store, and pending index.
	if !opts.ReadOnly {
		cacheBase := opts.CacheDir
		if cacheBase == "" {
			home, err := os.UserHomeDir()
			if err == nil {
				cacheBase = filepath.Join(home, ".cache", "drive9")
			}
		}
		if cacheBase != "" {
			mh := MountHash(opts.Server, opts.MountPoint)
			pendingDir := filepath.Join(cacheBase, mh, "pending")
			shadowDir := filepath.Join(cacheBase, mh, "shadow")

			// Initialize PendingIndex (in-memory authoritative metadata).
			pendingIdx, err := NewPendingIndex(pendingDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "dat9: pending index init failed: %v (continuing without)\n", err)
			} else {
				if err := pendingIdx.RecoverFromDisk(); err != nil {
					fmt.Fprintf(os.Stderr, "dat9: pending index recovery: %v\n", err)
				}
				dat9fs.pendingIndex = pendingIdx
			}

			// Initialize ShadowStore.
			shadowStore, err := NewShadowStore(shadowDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "dat9: shadow store init failed: %v (continuing without)\n", err)
			} else {
				dat9fs.shadowStore = shadowStore
			}

			// Initialize Journal WAL.
			journalPath := filepath.Join(cacheBase, mh, "journal.wal")
			journal, err := NewJournal(journalPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "dat9: journal init failed: %v (continuing without)\n", err)
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
				fmt.Fprintf(os.Stderr, "dat9: write-back cache init failed: %v (continuing without)\n", err)
			}

			// Migrate legacy writeBack entries to shadow store so
			// CommitQueue.RecoverPending sees them and doesn't prune.
			if wbCache != nil && shadowStore != nil {
				for _, pe := range wbCache.ListPending() {
					if !shadowStore.Has(pe.Meta.Path) {
						if err := shadowStore.WriteFull(pe.Meta.Path, pe.Data, pe.Meta.BaseRev); err != nil {
							fmt.Fprintf(os.Stderr, "dat9: migrate legacy entry %s to shadow: %v\n", pe.Meta.Path, err)
						}
					}
				}
			}

			// Initialize CommitQueue for background remote commits.
			if shadowStore != nil && pendingIdx != nil {
				cq := NewCommitQueue(c, shadowStore, pendingIdx, journal, opts.UploadConcurrency, maxCommitQueuePending)
				dat9fs.SetCommitQueue(cq)
				cq.RecoverPending()
			}

			if wbCache != nil {
				uploader := NewWriteBackUploader(c, wbCache, opts.UploadConcurrency)
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
	fuseOpts := &gofuse.MountOptions{
		FsName:        "dat9",
		Name:          "dat9",
		MaxReadAhead:  8 * 1024 * 1024, // 8MB — larger readahead reduces FUSE kernel↔userspace switches
		MaxWrite:      128 * 1024,      // 128KB per write request (default 64KB)
		MaxBackground: 32,              // concurrent background FUSE requests (default 12)
		Debug:         opts.Debug,
		AllowOther:    opts.AllowOther,
	}
	if runtime.GOOS == "linux" {
		fuseOpts.MaxWrite = 1024 * 1024 // 1MiB — Linux FUSE supports this natively
	}
	if opts.ReadOnly {
		fuseOpts.Options = append(fuseOpts.Options, "ro")
	}

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

	// Signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\ndat9: unmounting %s...\n", opts.MountPoint)

		// Second Ctrl+C during unmount exits immediately.
		go func() {
			<-sigCh
			fmt.Fprintf(os.Stderr, "dat9: forced exit\n")
			os.Exit(1)
		}()

		sseWatcher.Stop()
		dat9fs.FlushAll()

		// Retry unmount up to 5 times — EBUSY is transient (Spotlight, Finder).
		const maxRetries = 5
		var unmountErr error
		for i := 0; i < maxRetries; i++ {
			if unmountErr = server.Unmount(); unmountErr == nil {
				return
			}
			fmt.Fprintf(os.Stderr, "dat9: unmount attempt %d/%d failed: %v\n", i+1, maxRetries, unmountErr)
			if i < maxRetries-1 {
				time.Sleep(500 * time.Millisecond)
			}
		}

		// All retries exhausted — force unmount via OS tool.
		fmt.Fprintf(os.Stderr, "dat9: retries exhausted, force-unmounting %s\n", opts.MountPoint)
		forceUnmount(opts.MountPoint)
	}()

	fmt.Fprintf(os.Stderr, "dat9: mounted on %s (server: %s, actor: %s)\n", opts.MountPoint, opts.Server, actorID)
	server.Wait()
	sseWatcher.Stop()
	return nil
}

// forceUnmount shells out to OS-specific tools to force-unmount a FUSE mount.
func forceUnmount(mountpoint string) {
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.Command("diskutil", "unmount", "force", mountpoint)
	} else {
		// Linux: try fusermount first, fall back to umount -l.
		if _, err := exec.LookPath("fusermount"); err == nil {
			cmd = exec.Command("fusermount", "-u", mountpoint)
		} else {
			cmd = exec.Command("umount", "-l", mountpoint)
		}
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "dat9: force unmount failed: %v\n", err)
	}
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
