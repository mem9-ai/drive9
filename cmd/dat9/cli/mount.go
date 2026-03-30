package cli

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"time"

	dat9fuse "github.com/mem9-ai/dat9/pkg/fuse"
)

// MountCmd handles the "dat9 mount" command.
func MountCmd(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	server := fs.String("server", os.Getenv("DAT9_SERVER"), "dat9 server URL")
	apiKey := fs.String("api-key", os.Getenv("DAT9_API_KEY"), "API key")
	cacheSize := fs.Int("cache-size", 128, "read cache size in MB")
	dirTTL := fs.Duration("dir-ttl", 5*time.Second, "directory cache TTL")
	attrTTL := fs.Duration("attr-ttl", 1*time.Second, "kernel attr cache TTL")
	entryTTL := fs.Duration("entry-ttl", 1*time.Second, "kernel entry cache TTL")
	allowOther := fs.Bool("allow-other", false, "allow other users to access mount")
	readOnly := fs.Bool("read-only", false, "mount as read-only")
	debug := fs.Bool("debug", false, "enable FUSE debug logging")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: dat9 mount [flags] <mountpoint>\n\nflags:\n")
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
		return fmt.Errorf("dat9 server URL required (--server or $DAT9_SERVER)")
	}
	if *apiKey == "" {
		return fmt.Errorf("API key required (--api-key or $DAT9_API_KEY)")
	}

	opts := &dat9fuse.MountOptions{
		Server:     *server,
		APIKey:     *apiKey,
		MountPoint: mountPoint,
		CacheSize:  int64(*cacheSize) << 20,
		DirTTL:     *dirTTL,
		AttrTTL:    *attrTTL,
		EntryTTL:   *entryTTL,
		AllowOther: *allowOther,
		ReadOnly:   *readOnly,
		Debug:      *debug,
	}

	return dat9fuse.Mount(opts)
}

// UmountCmd handles the "dat9 umount" command.
func UmountCmd(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: dat9 umount <mountpoint>")
	}
	mountPoint := args[0]

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("umount", mountPoint)
	default:
		cmd = exec.Command("fusermount", "-u", mountPoint)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
