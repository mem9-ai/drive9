package fuse

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

// MountOptions configures the FUSE mount.
type MountOptions struct {
	Server        string        // dat9 server URL
	APIKey        string        // dat9 API key
	MountPoint    string        // local mount point
	CacheSize     int64         // ReadCache max size in bytes (default 128MB)
	DirTTL        time.Duration // DirCache TTL (default 5s)
	AttrTTL       time.Duration // kernel attr cache TTL (default 1s)
	EntryTTL      time.Duration // kernel entry cache TTL (default 1s)
	FlushDebounce time.Duration // debounce window for small-file flush coalescing (default 2s, 0 disables); set to -1 to use default
	AllowOther    bool          // allow other users to access mount
	ReadOnly      bool          // mount as read-only
	Debug         bool          // enable FUSE debug logging
}

func (o *MountOptions) setDefaults() {
	if o.CacheSize <= 0 {
		o.CacheSize = defaultReadCacheMaxSize
	}
	if o.DirTTL <= 0 {
		o.DirTTL = defaultDirCacheTTL
	}
	if o.AttrTTL <= 0 {
		o.AttrTTL = 60 * time.Second
	}
	if o.EntryTTL <= 0 {
		o.EntryTTL = 60 * time.Second
	}
	// FlushDebounce: 0 means disabled, negative means unset (use default).
	if o.FlushDebounce < 0 {
		o.FlushDebounce = defaultFlushDebounce
	}
}

// Mount creates and serves a FUSE mount. It blocks until the filesystem
// is unmounted or a signal (SIGINT, SIGTERM) is received.
func Mount(opts *MountOptions) error {
	opts.setDefaults()

	if err := os.MkdirAll(opts.MountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	// Create client and verify connectivity
	c := client.New(opts.Server, opts.APIKey)
	if _, err := c.List("/"); err != nil {
		return fmt.Errorf("cannot reach dat9 server: %w", err)
	}

	// Build FUSE filesystem
	dat9fs := NewDat9FS(c, opts)

	// Configure FUSE mount options
	fuseOpts := &gofuse.MountOptions{
		FsName:       "dat9",
		Name:         "dat9",
		MaxReadAhead: 8 * 1024 * 1024, // 8MB — larger readahead reduces FUSE kernel↔userspace switches
		Debug:        opts.Debug,
		AllowOther:   opts.AllowOther,
	}
	if opts.ReadOnly {
		fuseOpts.Options = append(fuseOpts.Options, "ro")
	}

	// Create FUSE server
	server, err := gofuse.NewServer(dat9fs, opts.MountPoint, fuseOpts)
	if err != nil {
		return fmt.Errorf("fuse mount: %w", err)
	}

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
		dat9fs.FlushAll()
		if err := server.Unmount(); err != nil {
			fmt.Fprintf(os.Stderr, "dat9: unmount error: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "dat9: mounted on %s (server: %s)\n", opts.MountPoint, opts.Server)
	server.Wait()
	return nil
}
