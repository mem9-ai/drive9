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
	Server     string        // dat9 server URL
	APIKey     string        // dat9 API key
	MountPoint string        // local mount point
	CacheSize  int64         // ReadCache max size in bytes (default 128MB)
	DirTTL     time.Duration // DirCache TTL (default 5s)
	AttrTTL    time.Duration // kernel attr cache TTL (default 1s)
	EntryTTL   time.Duration // kernel entry cache TTL (default 1s)
	AllowOther bool          // allow other users to access mount
	ReadOnly   bool          // mount as read-only
	Debug      bool          // enable FUSE debug logging
}

func (o *MountOptions) setDefaults() {
	if o.CacheSize <= 0 {
		o.CacheSize = defaultReadCacheMaxSize
	}
	if o.DirTTL <= 0 {
		o.DirTTL = defaultDirCacheTTL
	}
	if o.AttrTTL <= 0 {
		o.AttrTTL = 1 * time.Second
	}
	if o.EntryTTL <= 0 {
		o.EntryTTL = 1 * time.Second
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
	if _, err := c.Stat("/"); err != nil {
		return fmt.Errorf("cannot reach dat9 server: %w", err)
	}

	// Build FUSE filesystem
	dat9fs := NewDat9FS(c, opts)

	// Configure FUSE mount options
	fuseOpts := &gofuse.MountOptions{
		FsName:       "dat9",
		Name:         "dat9",
		MaxReadAhead: 128 * 1024, // 128KB
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
	server.Serve()
	return nil
}
