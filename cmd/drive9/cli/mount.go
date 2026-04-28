package cli

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	drive9fuse "github.com/mem9-ai/dat9/pkg/fuse"
	drive9webdav "github.com/mem9-ai/dat9/pkg/webdav"
)

// MountCmd handles the "drive9 mount" command.
//
// Dispatch fork (Row A, V2e): the first positional argument selects the
// mount flavour.
//
//   - `drive9 mount vault <path>`   → read-only vault FUSE filesystem
//   - `drive9 mount [flags] <path>` → legacy writable fs mount (no
//     subcommand keyword; first positional is the mount point)
//
// We MUST peek at the first arg before flag.Parse because the vault flag
// set is smaller (no cache-size / write-path knobs), and a single flag
// set would quietly accept write-path flags for a vault mount — that
// would violate Row C (read-only) in a subtle, mount-time-visible way.
//
// Only the CURRENT supported backend keyword ("vault") is reserved here.
// Every other first positional falls through to the legacy parser, which
// enforces "exactly one mountpoint" so `drive9 mount kv /mnt/x` fails as
// a positional-arity error rather than by pre-reserving backend-shaped
// words that do not exist yet.
func MountCmd(args []string) error {
	if len(args) > 0 {
		if args[0] == "vault" {
			return VaultMountCmd(args[1:])
		}
	}
	return fsMountCmd(args)
}

// fsMountCmd is the pre-V2e writable fs mount entry point.
//
// Credential precedence matches spec §14.2: explicit --server / --api-key flag
// > DRIVE9_SERVER / DRIVE9_API_KEY / DRIVE9_VAULT_TOKEN env > active config
// context. The flag defaults are empty strings so we can distinguish "unset"
// from "explicit empty"; the latter is rejected (see rejectEmptyFlag).
//
// A mount is bound to exactly one principal at mount time (Invariant #3).
// If the resolver returns a delegated credential (owner JWT / `ctx use <alice>`),
// Mount is created via client.NewWithToken and bound to that capability for
// the mount's lifetime. If the active principal changes later (`ctx use` to
// another context), the running mount keeps its original binding — changing
// a running mount's credential requires umount + remount (Invariant #6).
// `vault reauth` is not part of M1; see docs/specs/vault-interaction-end-state.md
// §17.
//
// drive9fuse.Mount runs in-process (no fork/exec); credentials flow through
// MountOptions{Server, APIKey, Token}, not through the child's environment.
// This makes the resolver's Unsetenv-after-read mitigation safe for mount.
func fsMountCmd(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	server := fs.String("server", "", "drive9 server URL (overrides $DRIVE9_SERVER and config)")
	apiKey := fs.String("api-key", "", "owner API key (overrides $DRIVE9_API_KEY and config)")
	mode := fs.String("mode", "auto", "mount mode: auto, fuse, or webdav")
	cacheDir := fs.String("cache-dir", "", "write-back cache directory (default ~/.cache/drive9)")
	cacheSize := fs.Int("cache-size", 128, "read cache size in MB")
	dirTTL := fs.Duration("dir-ttl", 10*time.Second, "directory cache TTL")
	attrTTL := fs.Duration("attr-ttl", 10*time.Second, "kernel attr cache TTL")
	entryTTL := fs.Duration("entry-ttl", 10*time.Second, "kernel entry cache TTL")
	flushDebounce := fs.Duration("flush-debounce", -1, "debounce window for small-file flush coalescing (default 2s, 0 disables)")
	lookupRetryCount := fs.Int("lookup-retry-count", 2, "detached retries after transient Lookup/GetAttr stat failures (default 2, set 0 to disable)")
	lookupRetryTimeout := fs.Duration("lookup-retry-timeout", 250*time.Millisecond, "timeout per detached Lookup/GetAttr stat retry (default 250ms, must be > 0)")
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
	if fs.NArg() != 1 {
		return fmt.Errorf("drive9 mount: exactly one mountpoint required")
	}

	mountMode, err := ParseMountMode(*mode)
	if err != nil {
		return err
	}

	if err := validateLookupRetryFlags(*lookupRetryCount, *lookupRetryTimeout); err != nil {
		return err
	}
	normalizedLookupRetryCount := normalizeLookupRetryCount(*lookupRetryCount)

	mountPoint := fs.Arg(0)

	serverGiven, apiKeyGiven := flagProvided(fs, "server"), flagProvided(fs, "api-key")
	if err := rejectEmptyFlag("server", *server, serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("api-key", *apiKey, apiKeyGiven); err != nil {
		return err
	}

	serverVal, apiKeyVal, tokenVal, err := resolveMountCredentials(ResolveCredentials(), *server, *apiKey)
	if err != nil {
		return err
	}
	*server, *apiKey = serverVal, apiKeyVal
	token := tokenVal

	// Resolve auto mode to a concrete backend.
	resolved := ResolveMountMode(mountMode, runtime.GOOS, exec.LookPath)
	fmt.Fprintf(os.Stderr, "dat9: mount mode: %s\n", resolved)

	// WebDAV path: create client, start local WebDAV server, invoke mount_webdav.
	if resolved == MountModeWebDAV {
		var c *client.Client
		if token != "" {
			c = client.NewWithToken(*server, token)
		} else {
			c = client.New(*server, *apiKey)
		}
		if _, err := c.List("/"); err != nil {
			return fmt.Errorf("cannot reach dat9 server: %w", err)
		}

		return webdavMount(c, mountPoint)
	}

	// FUSE path (existing behavior).
	syncModeVal, err := drive9fuse.ParseSyncMode(*syncMode)
	if err != nil {
		return err
	}

	opts := &drive9fuse.MountOptions{
		Server:             *server,
		APIKey:             *apiKey,
		Token:              token,
		MountPoint:         mountPoint,
		CacheDir:           *cacheDir,
		CacheSize:          int64(*cacheSize) << 20,
		DirTTL:             *dirTTL,
		AttrTTL:            *attrTTL,
		EntryTTL:           *entryTTL,
		FlushDebounce:      *flushDebounce,
		LookupRetryCount:   normalizedLookupRetryCount,
		LookupRetryTimeout: *lookupRetryTimeout,
		SyncMode:           syncModeVal,
		Profile:            *profile,
		AllowOther:         *allowOther,
		ReadOnly:           *readOnly,
		Debug:              *debug,
	}

	return drive9fuse.Mount(opts)
}

// newWebDAVHandler creates an http.Handler that serves drive9 content over WebDAV.
func newWebDAVHandler(c *client.Client, prefix string) (http.Handler, error) {
	return drive9webdav.NewHandler(c, drive9webdav.Options{Prefix: prefix}), nil
}

func validateLookupRetryFlags(count int, timeout time.Duration) error {
	if count < 0 {
		return fmt.Errorf("drive9 mount: --lookup-retry-count must be >= 0")
	}
	if timeout <= 0 {
		return fmt.Errorf("drive9 mount: --lookup-retry-timeout must be > 0")
	}
	return nil
}

func normalizeLookupRetryCount(count int) int {
	if count == 0 {
		// Use negative sentinel so MountOptions.setDefaults can distinguish
		// explicit CLI disable from plain zero-value "unset" options.
		return -1
	}
	return count
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

// resolveMountCredentials selects the (server, apiKey, token) triple that a
// fresh mount will be bound to. It locks the principal kind at mount time
// per Invariant #3 — once this function returns, the chosen credential is
// fixed for the mount's lifetime. An explicit --api-key flag always means
// owner; delegated JWTs only reach this layer through the active context
// or DRIVE9_VAULT_TOKEN (resolver output).
//
// Returned token is non-empty iff the resolver produced a delegated
// credential and no --api-key flag was passed. apiKey and token are
// mutually exclusive; exactly one is non-empty on success.
func resolveMountCredentials(r ResolvedCredentials, flagServer, flagAPIKey string) (server, apiKey, token string, err error) {
	server = flagServer
	if server == "" {
		server = r.Server
	}

	apiKey = flagAPIKey
	if apiKey == "" {
		switch r.Kind {
		case CredentialOwner:
			apiKey = r.APIKey
		case CredentialDelegated:
			token = r.Token
		}
	}

	if server == "" {
		return "", "", "", fmt.Errorf("drive9 server URL required (--server, $%s, or `drive9 ctx`)", EnvServer)
	}
	if apiKey == "" && token == "" {
		return "", "", "", fmt.Errorf("owner API key or delegated token required (--api-key, $%s, $%s, or `drive9 ctx`)", EnvAPIKey, EnvVaultToken)
	}
	return server, apiKey, token, nil
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
