package cli

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"time"

	drive9fuse "github.com/mem9-ai/dat9/pkg/fuse"
)

// MountCmd handles the "drive9 mount" command.
func MountCmd(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	server := fs.String("server", os.Getenv("DRIVE9_SERVER"), "drive9 server URL")
	apiKey := fs.String("api-key", os.Getenv("DRIVE9_API_KEY"), "API key")
	cacheSize := fs.Int("cache-size", 128, "read cache size in MB")
	dirTTL := fs.Duration("dir-ttl", 5*time.Second, "directory cache TTL")
	attrTTL := fs.Duration("attr-ttl", 1*time.Second, "kernel attr cache TTL")
	entryTTL := fs.Duration("entry-ttl", 1*time.Second, "kernel entry cache TTL")
	flushDebounce := fs.Duration("flush-debounce", -1, "debounce window for small-file flush coalescing (default 2s, 0 disables)")
	syncMode := fs.String("sync-mode", "auto", "sync mode: auto, interactive, or strict")
	profile := fs.String("profile", "", "mount profile: interactive (empty for default)")
	allowOther := fs.Bool("allow-other", false, "allow other users to access mount")
	readOnly := fs.Bool("read-only", false, "mount as read-only")
	debug := fs.Bool("debug", false, "enable FUSE debug logging")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 mount [flags] <mountpoint>\n\nflags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}

	mountPoint := fs.Arg(0)

	// Fill server/api-key from config if not set via flags/env
	if *server == "" || *apiKey == "" {
		cfg := loadConfig()
		if *server == "" {
			*server = cfg.ResolveServer()
		}
		if *apiKey == "" {
			*apiKey = cfg.CurrentAPIKey()
		}
	}

	if *server == "" {
		return fmt.Errorf("drive9 server URL required (--server or $DRIVE9_SERVER)")
	}
	if *apiKey == "" {
		return fmt.Errorf("API key required (--api-key or $DRIVE9_API_KEY)")
	}

	syncModeVal, err := drive9fuse.ParseSyncMode(*syncMode)
	if err != nil {
		return err
	}

	opts := &drive9fuse.MountOptions{
		Server:        *server,
		APIKey:        *apiKey,
		MountPoint:    mountPoint,
		CacheSize:     int64(*cacheSize) << 20,
		DirTTL:        *dirTTL,
		AttrTTL:       *attrTTL,
		EntryTTL:      *entryTTL,
		FlushDebounce: *flushDebounce,
		SyncMode:      syncModeVal,
		Profile:       *profile,
		AllowOther:    *allowOther,
		ReadOnly:      *readOnly,
		Debug:         *debug,
	}

	return drive9fuse.Mount(opts)
}

// UmountCmd handles the "drive9 umount" command.
func UmountCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: drive9 umount <mountpoint>")
	}
	mountPoint := args[0]

	argv, err := umountArgv(runtime.GOOS, exec.LookPath, mountPoint)
	if err != nil {
		return err
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func umountArgv(goos string, lookPath func(string) (string, error), mountPoint string) ([]string, error) {
	if goos == "darwin" {
		return []string{"umount", mountPoint}, nil
	}
	if _, err := lookPath("fusermount3"); err == nil {
		return []string{"fusermount3", "-u", mountPoint}, nil
	}
	if _, err := lookPath("fusermount"); err == nil {
		return []string{"fusermount", "-u", mountPoint}, nil
	}
	if _, err := lookPath("umount"); err == nil {
		return []string{"umount", mountPoint}, nil
	}
	return nil, fmt.Errorf("umount: no supported unmount binary found (tried fusermount3, fusermount, umount)")
}
