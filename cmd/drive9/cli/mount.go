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
//
// Credential precedence matches spec §14.2: explicit --server / --api-key flag
// > DRIVE9_SERVER / DRIVE9_API_KEY env > active config context. The flag
// defaults are empty strings so we can distinguish "unset" from "explicit
// empty"; the latter is rejected (see rejectEmptyFlag).
//
// drive9fuse.Mount runs in-process (no fork/exec); credentials flow through
// MountOptions{Server, APIKey}, not through the child's environment. This
// makes the resolver's Unsetenv-after-read mitigation safe for mount.
func MountCmd(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	server := fs.String("server", "", "drive9 server URL (overrides $DRIVE9_SERVER and config)")
	apiKey := fs.String("api-key", "", "API key (overrides $DRIVE9_API_KEY and config)")
	cacheSize := fs.Int("cache-size", 128, "read cache size in MB")
	dirTTL := fs.Duration("dir-ttl", 10*time.Second, "directory cache TTL")
	attrTTL := fs.Duration("attr-ttl", 10*time.Second, "kernel attr cache TTL")
	entryTTL := fs.Duration("entry-ttl", 10*time.Second, "kernel entry cache TTL")
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

	serverGiven, apiKeyGiven := flagProvided(fs, "server"), flagProvided(fs, "api-key")
	if err := rejectEmptyFlag("server", *server, serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("api-key", *apiKey, apiKeyGiven); err != nil {
		return err
	}

	r := ResolveCredentials()
	if *server == "" {
		*server = r.Server
	}
	if *apiKey == "" {
		if r.Kind == CredentialOwner {
			*apiKey = r.APIKey
		}
	}

	if *server == "" {
		return fmt.Errorf("drive9 server URL required (--server, $%s, or `drive9 ctx`)", EnvServer)
	}
	if *apiKey == "" {
		return fmt.Errorf("owner API key required (--api-key, $%s, or `drive9 ctx`)", EnvAPIKey)
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
