package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/dat9/pkg/buildinfo"
	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
	"github.com/mem9-ai/dat9/pkg/mountstate"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	drive9webdav "github.com/mem9-ai/dat9/pkg/webdav"
)

const (
	defaultFuseLookupRetryCount   = 3
	defaultFuseLookupRetryTimeout = 2 * time.Second
	defaultFuseReadConcurrency    = 24
)

var (
	errMountProcessStateStale  = errors.New("drive9 umount: stale mount process state")
	errMountProcessStateUnsafe = errors.New("drive9 umount: unsafe mount process state")
)

// MountCmd handles the "drive9 mount" command.
//
// Dispatch fork (Row A, V2e): the first positional argument selects the
// mount flavour.
//
//   - `drive9 mount vault <path>`   -> read-only vault FUSE filesystem
//   - `drive9 mount [flags] <path>` -> legacy writable fs mount (no
//     subcommand keyword; first positional is the mount point)
//
// We MUST peek at the first arg before flag.Parse because the vault flag
// set is smaller (no cache-size / write-path knobs), and a single flag
// set would quietly accept write-path flags for a vault mount - that
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
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, mountUsage())
		return nil
	}
	fmt.Fprint(os.Stderr, buildinfo.String("drive9 mount"))
	return fsMountCmd(args)
}

func mountUsage() string { return "usage: drive9 mount [flags] [:/remote] <mountpoint>" }

// fsMountCmd is the pre-V2e writable fs mount entry point.
//
// Credential precedence matches the unified resolver: explicit --server /
// --api-key flag overrides all resolver sources; env credentials still beat
// config credentials, but the active context's server beats DRIVE9_SERVER.
// The flag defaults are empty strings so we can distinguish "unset" from
// "explicit empty"; the latter is rejected (see rejectEmptyFlag).
//
// A mount is bound to exactly one principal at mount time (Invariant #3).
// If the resolver returns a delegated credential (owner JWT / `ctx use <alice>`),
// Mount is created via client.NewWithToken and bound to that capability for
// the mount's lifetime. If the active principal changes later (`ctx use` to
// another context), the running mount keeps its original binding - changing
// a running mount's credential requires umount + remount (Invariant #6).
// `vault reauth` is not part of M1; see docs/specs/vault-interaction-end-state.md
// section 17.
//
// drive9fuse.Mount runs in-process (no fork/exec); credentials flow through
// MountOptions{Server, APIKey, Token}, not through the child's environment.
// This makes the resolver's Unsetenv-after-read mitigation safe for mount.
func fsMountCmd(args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, mountUsage())
		return nil
	}
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	server := fs.String("server", "", "drive9 server URL (overrides $DRIVE9_SERVER and config)")
	apiKey := fs.String("api-key", "", "owner API key (overrides $DRIVE9_API_KEY and config)")
	mode := fs.String("mode", "auto", "mount mode: auto, fuse, or webdav")
	cacheDir := fs.String("cache-dir", "", "write-back cache directory (default ~/.cache/drive9)")
	cacheSize := fs.Int("cache-size", 128, "read cache size in MB")
	readCacheMaxFile := fs.Int64("read-cache-max-file-mb", 1, "maximum single file size admitted to read cache in MB")
	dirTTL := fs.Duration("dir-ttl", 10*time.Second, "directory cache TTL")
	attrTTL := fs.Duration("attr-ttl", 10*time.Second, "kernel attr cache TTL")
	entryTTL := fs.Duration("entry-ttl", 10*time.Second, "kernel entry cache TTL")
	flushDebounce := fs.Duration("flush-debounce", -1, "debounce window for small-file flush coalescing (default 2s, 0 disables)")
	lookupRetryCount := fs.Int("lookup-retry-count", defaultFuseLookupRetryCount, "detached retries after transient Lookup/GetAttr stat failures (set 0 to disable)")
	lookupRetryTimeout := fs.Duration("lookup-retry-timeout", defaultFuseLookupRetryTimeout, "timeout per detached Lookup/GetAttr stat retry (must be > 0 when set)")
	readConcurrency := fs.Int("read-concurrency", defaultFuseReadConcurrency, "maximum concurrent backend reads issued by FUSE")
	syncRead := fs.Bool("fuse-sync-read", false, "disable kernel async read dispatch; at most one read in flight per file handle")
	legacyDirStatFallback := fs.Bool("legacy-dir-stat-fallback", false, "on Lookup stat 404, list parent to support legacy servers without directory stat")
	readDirPrefetch := fs.Bool("readdir-prefetch", false, "prefetch small files after directory reads into the read cache")
	prefetchMaxFiles := fs.Int("readdir-prefetch-max-files", 32, "maximum small files prefetched per directory read")
	prefetchMaxFileBytes := fs.Int64("readdir-prefetch-max-file-bytes", 50_000, "maximum individual file size prefetched by readdir prefetch")
	prefetchMaxBytes := fs.Int64("readdir-prefetch-max-bytes", 1<<20, "maximum aggregate bytes prefetched per directory read")
	prefetchTimeout := fs.Duration("readdir-prefetch-timeout", time.Second, "timeout for one readdir prefetch batch")
	trustProcessLocalEvents := fs.Bool("trust-process-local-events", false, "allow revision-bound GetAttr dir-cache hits using process-local SSE freshness; only safe for single-server/sticky routing or cluster-wide event streams")
	durability := fs.String("durability", string(fuseDurabilityAuto), "write durability: auto, interactive, fsync, close-sync, or write-sync")
	profile := fs.String("profile", "", "mount profile: interactive, coding-agent (empty for default)")
	localRoot := fs.String("local-root", "", "local-only overlay storage root for --profile=coding-agent")
	var localOnlyPatterns stringListFlag
	var remoteOnlyPatterns stringListFlag
	fs.Var(&localOnlyPatterns, "local-only", "additional local-only path pattern for coding-agent overlay routing (repeatable, e.g. **/node_modules/**)")
	fs.Var(&remoteOnlyPatterns, "remote-only", "remote-persistent override path pattern for coding-agent overlay routing (repeatable)")
	uploadConcurrency := fs.Int("upload-concurrency", 16, "maximum concurrent background uploads issued by FUSE")
	allowOther := fs.Bool("allow-other", false, "allow other users to access mount")
	readOnly := fs.Bool("read-only", false, "mount as read-only")
	debug := fs.Bool("debug", false, "enable FUSE debug logging")
	perfCounters := fs.Bool("perf-counters", false, "print FUSE perf counter summary on unmount")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n\nflags:\n", mountUsage())
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse positional args: 1-arg = mountpoint; 2-arg = :/remote mountpoint.
	var remoteRoot, mountPoint string
	switch fs.NArg() {
	case 1:
		remoteRoot = "/"
		mountPoint = fs.Arg(0)
	case 2:
		rp, ok := ParseRemote(fs.Arg(0))
		if !ok {
			return fmt.Errorf("drive9 mount: first positional argument must be a remote source (e.g. :/path), got %q", fs.Arg(0))
		}
		if rp.Context != "" {
			return fmt.Errorf("drive9 mount: context-scoped remote sources (e.g. %s:/path) are not yet supported", rp.Context)
		}
		remoteRoot = rp.Path
		mountPoint = fs.Arg(1)
	default:
		fs.Usage()
		os.Exit(2)
	}

	var err error
	remoteRoot, err = mountpath.NormalizeRoot(remoteRoot)
	if err != nil {
		return fmt.Errorf("drive9 mount: %w", err)
	}

	mountMode, err := ParseMountMode(*mode)
	if err != nil {
		return err
	}

	lookupRetryCountGiven := flagProvided(fs, "lookup-retry-count")
	lookupRetryTimeoutGiven := flagProvided(fs, "lookup-retry-timeout")
	trustProcessLocalEventsGiven := flagProvided(fs, "trust-process-local-events")
	if err := validateLookupRetryFlags(*lookupRetryCount, *lookupRetryTimeout, lookupRetryCountGiven, lookupRetryTimeoutGiven); err != nil {
		return err
	}
	if *readConcurrency <= 0 {
		return fmt.Errorf("drive9 mount: --read-concurrency must be > 0")
	}
	if *uploadConcurrency <= 0 {
		return fmt.Errorf("drive9 mount: --upload-concurrency must be > 0")
	}
	if err := validateReadDirPrefetchFlags(*prefetchMaxFiles, *prefetchMaxFileBytes, *prefetchMaxBytes, *prefetchTimeout); err != nil {
		return err
	}
	normalizedLocalRoot := strings.TrimSpace(*localRoot)
	if err := validateMountProfileFlags(*profile, normalizedLocalRoot, localOnlyPatterns, remoteOnlyPatterns); err != nil {
		return err
	}
	syncModeVal, writePolicyVal, err := parseFuseDurability(*durability)
	if err != nil {
		return err
	}
	if *readCacheMaxFile <= 0 {
		return fmt.Errorf("drive9 mount: --read-cache-max-file-mb must be > 0")
	}
	normalizedLookupRetryCount := lookupRetryCountFlagValue(lookupRetryCountGiven, *lookupRetryCount)
	normalizedLookupRetryTimeout := durationFlagValue(fs, "lookup-retry-timeout", *lookupRetryTimeout)
	normalizedDirTTL := durationFlagValue(fs, "dir-ttl", *dirTTL)
	normalizedAttrTTL := durationFlagValue(fs, "attr-ttl", *attrTTL)
	normalizedEntryTTL := durationFlagValue(fs, "entry-ttl", *entryTTL)

	serverGiven, apiKeyGiven := flagProvided(fs, "server"), flagProvided(fs, "api-key")
	if err := rejectEmptyFlag("server", *server, serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("api-key", *apiKey, apiKeyGiven); err != nil {
		return err
	}

	// Resolve auto mode to a concrete backend.
	resolved := ResolveMountMode(mountMode, runtime.GOOS, exec.LookPath)
	fmt.Fprintf(os.Stderr, "drive9: mount mode: %s\n", resolved)
	if *profile == "coding-agent" && resolved != MountModeFUSE {
		return fmt.Errorf("drive9 mount: --profile=coding-agent requires --mode=fuse")
	}

	if resolved == MountModeWebDAV && *readOnly {
		return fmt.Errorf("drive9 mount: --read-only is not supported with WebDAV mode")
	}
	if resolved == MountModeWebDAV && trustProcessLocalEventsGiven {
		return fmt.Errorf("drive9 mount: --trust-process-local-events is only supported with --mode=fuse")
	}
	if runtime.GOOS == "windows" && resolved == MountModeFUSE {
		return mountFuse(&mountFuseOptions{
			MountPoint:         mountPoint,
			RemoteRoot:         remoteRoot,
			Profile:            *profile,
			LocalRoot:          normalizedLocalRoot,
			LocalOnlyPatterns:  append([]string(nil), localOnlyPatterns...),
			RemoteOnlyPatterns: append([]string(nil), remoteOnlyPatterns...),
			ReadOnly:           *readOnly,
			Debug:              *debug,
		})
	}

	serverVal, apiKeyVal, tokenVal, err := resolveMountCredentials(ResolveCredentials(), *server, *apiKey)
	if err != nil {
		return err
	}
	*server, *apiKey = serverVal, apiKeyVal
	token := tokenVal

	// WebDAV path: create client, start local WebDAV server, invoke mount_webdav.
	if resolved == MountModeWebDAV {
		if *durability != string(fuseDurabilityAuto) {
			return fmt.Errorf("--durability is only supported with --mode=fuse; WebDAV mounts always use their native write behavior")
		}
		var c *client.Client
		if token != "" {
			c = client.NewWithToken(*server, token)
		} else {
			c = client.New(*server, *apiKey)
		}
		warmCtx, warmCancel := context.WithTimeout(context.Background(), fsClientWarmTimeout)
		c.Warm(warmCtx)
		warmCancel()

		// Validate remote root exists and is a directory.
		if err := validateRemoteRoot(c, remoteRoot); err != nil {
			return err
		}

		return webdavMount(c, mountPoint, remoteRoot)
	}

	// FUSE path (existing behavior).
	opts := &mountFuseOptions{
		Server:                *server,
		APIKey:                *apiKey,
		Token:                 token,
		MountPoint:            mountPoint,
		RemoteRoot:            remoteRoot,
		CacheDir:              *cacheDir,
		CacheSize:             int64(*cacheSize) << 20,
		ReadCacheMaxFileBytes: *readCacheMaxFile << 20,
		DirTTL:                normalizedDirTTL,
		AttrTTL:               normalizedAttrTTL,
		EntryTTL:              normalizedEntryTTL,
		FlushDebounce:         *flushDebounce,
		LookupRetryCount:      normalizedLookupRetryCount,
		LookupRetryTimeout:    normalizedLookupRetryTimeout,
		LegacyDirStatFallback: *legacyDirStatFallback,
		ReadDirPrefetch:       *readDirPrefetch,
		PrefetchMaxFiles:      *prefetchMaxFiles,
		PrefetchMaxFileBytes:  *prefetchMaxFileBytes,
		PrefetchMaxBytes:      *prefetchMaxBytes,
		PrefetchTimeout:       *prefetchTimeout,
		TrustLocalEvents:      *trustProcessLocalEvents,
		SyncMode:              syncModeVal,
		WritePolicy:           writePolicyVal,
		Profile:               *profile,
		LocalRoot:             normalizedLocalRoot,
		LocalOnlyPatterns:     append([]string(nil), localOnlyPatterns...),
		RemoteOnlyPatterns:    append([]string(nil), remoteOnlyPatterns...),
		UploadConcurrency:     *uploadConcurrency,
		ReadConcurrency:       *readConcurrency,
		SyncRead:              *syncRead,
		AllowOther:            *allowOther,
		ReadOnly:              *readOnly,
		Debug:                 *debug,
		PerfCounters:          *perfCounters,
	}

	return mountFuse(opts)
}

// newWebDAVHandler creates an http.Handler that serves drive9 content over WebDAV.
func newWebDAVHandler(c *client.Client, prefix string, remoteRoot string) (http.Handler, error) {
	return drive9webdav.NewHandler(c, drive9webdav.Options{Prefix: prefix, RemoteRoot: remoteRoot}), nil
}

// validateRemoteRoot checks that the remote root path exists and is a directory.
// Mirrors the FUSE path's Stat->List fallback so both mount modes behave
// identically on backends where directory stat is unsupported.
func validateRemoteRoot(c *client.Client, remoteRoot string) error {
	if remoteRoot == "/" {
		// Root always exists; verify server connectivity via List.
		if _, err := c.List("/"); err != nil {
			return fmt.Errorf("cannot reach drive9 server: %w", err)
		}
		return nil
	}
	stat, err := c.Stat(remoteRoot)
	if err != nil {
		// If Stat explicitly says "not found", trust it - don't fall back
		// to List which may return empty success for non-existent paths.
		if client.IsNotFound(err) {
			return remoteRootError(remoteRoot, err)
		}
		// Stat may fail on backends where directory stat is unsupported
		// (non-404 error). Fall back to List to verify the remote root
		// exists and is listable.
		if _, listErr := c.List(remoteRoot); listErr != nil {
			return remoteRootError(remoteRoot, listErr)
		}
		return nil
	}
	if !stat.IsDir {
		return fmt.Errorf("remote root %q is not a directory", remoteRoot)
	}
	return nil
}

// remoteRootError wraps a remote-root stat/list error with actionable guidance
// when the path does not exist (HTTP 404).
func remoteRootError(remoteRoot string, err error) error {
	if client.IsNotFound(err) {
		return fmt.Errorf("drive9 mount: remote source %q does not exist\n\n  To create it first:\n    drive9 fs mkdir :%s\n  Then retry:\n    drive9 mount :%s <mountpoint>", remoteRoot, remoteRoot, remoteRoot)
	}
	return fmt.Errorf("remote root %q: %w", remoteRoot, err)
}

func validateLookupRetryFlags(count int, timeout time.Duration, countGiven bool, timeoutGiven bool) error {
	if countGiven && count < 0 {
		return fmt.Errorf("drive9 mount: --lookup-retry-count must be >= 0")
	}
	if timeoutGiven && timeout <= 0 {
		return fmt.Errorf("drive9 mount: --lookup-retry-timeout must be > 0")
	}
	return nil
}

func validateReadDirPrefetchFlags(maxFiles int, maxFileBytes int64, maxBytes int64, timeout time.Duration) error {
	if maxFiles <= 0 {
		return fmt.Errorf("drive9 mount: --readdir-prefetch-max-files must be > 0")
	}
	if maxFileBytes <= 0 {
		return fmt.Errorf("drive9 mount: --readdir-prefetch-max-file-bytes must be > 0")
	}
	if maxBytes <= 0 {
		return fmt.Errorf("drive9 mount: --readdir-prefetch-max-bytes must be > 0")
	}
	if timeout <= 0 {
		return fmt.Errorf("drive9 mount: --readdir-prefetch-timeout must be > 0")
	}
	return nil
}

func validateMountProfileFlags(profile string, localRoot string, localOnlyPatterns []string, remoteOnlyPatterns []string) error {
	switch profile {
	case "", "interactive", "coding-agent":
	default:
		return fmt.Errorf("drive9 mount: unknown --profile %q (valid: interactive, coding-agent)", profile)
	}
	hasPolicyFlags := localRoot != "" || len(localOnlyPatterns) > 0 || len(remoteOnlyPatterns) > 0
	if profile != "coding-agent" {
		if hasPolicyFlags {
			return fmt.Errorf("drive9 mount: --local-root, --local-only, and --remote-only require --profile=coding-agent")
		}
		return nil
	}
	if strings.TrimSpace(localRoot) == "" {
		return fmt.Errorf("drive9 mount: --profile=coding-agent requires --local-root")
	}
	if !filepath.IsAbs(localRoot) {
		return fmt.Errorf("drive9 mount: --local-root must be an absolute path")
	}
	if err := validateMountPolicyPatterns(localOnlyPatterns, remoteOnlyPatterns); err != nil {
		return err
	}
	return nil
}

func validateMountPolicyPatterns(patternGroups ...[]string) error {
	for _, patterns := range patternGroups {
		for _, pattern := range patterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			if _, err := pathutil.Canonicalize(pattern); err != nil {
				return fmt.Errorf("drive9 mount: invalid local policy pattern %q: %w", pattern, err)
			}
		}
	}
	return nil
}

type stringListFlag []string

func (values *stringListFlag) String() string {
	if values == nil {
		return ""
	}
	return strings.Join(*values, ",")
}

func (values *stringListFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("empty path pattern")
	}
	*values = append(*values, value)
	return nil
}

func durationFlagValue(fs *flag.FlagSet, name string, value time.Duration) time.Duration {
	if flagProvided(fs, name) {
		return value
	}
	return 0
}

func lookupRetryCountFlagValue(given bool, count int) int {
	if !given {
		return 0
	}
	return normalizeLookupRetryCount(count)
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
	return runUmount(args, defaultUmountDeps())
}

func umountUsage() string { return "usage: drive9 umount [--timeout duration] <mountpoint>" }

type umountDeps struct {
	goos             string
	lookPath         func(string) (string, error)
	run              func([]string) error
	readPID          func(string) (int, string, error)
	readProcessState func(string) (mountstate.ProcessState, string, error)
	terminate        func(int, time.Duration) error
	terminateState   func(mountstate.ProcessState, time.Duration) error
	remove           func(string) error
	pidAlive         func(int) bool
	now              func() time.Time
	sleep            func(time.Duration)
	printErrf        func(string, ...any)
}

func defaultUmountDeps() umountDeps {
	return umountDeps{
		goos:     runtime.GOOS,
		lookPath: exec.LookPath,
		run: func(argv []string) error {
			cmd := exec.Command(argv[0], argv[1:]...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
		},
		readPID:          mountstate.ReadPID,
		readProcessState: mountstate.ReadProcessState,
		terminate:        terminateProcess,
		terminateState:   terminateMountProcess,
		remove:           os.Remove,
		pidAlive:         processAlive,
		now:              time.Now,
		sleep:            time.Sleep,
		printErrf:        func(format string, args ...any) { fmt.Fprintf(os.Stderr, format, args...) },
	}
}

func runUmount(args []string, deps umountDeps) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, umountUsage())
		return nil
	}
	fs := flag.NewFlagSet("umount", flag.ContinueOnError)
	waitTimeout := fs.Duration("timeout", 60*time.Second, "time to wait for the drive9 mount process to exit after unmount; 0 disables waiting")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n\nflags:\n", umountUsage())
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("%s", umountUsage())
	}
	mountPoint := fs.Arg(0)
	stateMountPoint := mountPoint
	var err error
	if deps.goos == "windows" {
		stateMountPoint, err = webdavMountStatePoint(deps.goos, mountPoint)
		if err != nil {
			return err
		}
	}

	argv, err := umountArgv(deps.goos, deps.lookPath, mountPoint)
	if err != nil {
		return err
	}
	runErr := deps.run(argv)
	if deps.goos != "windows" && runErr != nil {
		return runErr
	}
	if deps.goos != "windows" && *waitTimeout == 0 {
		return nil
	}
	if deps.goos == "windows" {
		var (
			state mountstate.ProcessState
			path  string
		)
		if deps.readProcessState != nil {
			state, path, err = deps.readProcessState(stateMountPoint)
		} else {
			var pid int
			pid, path, err = deps.readPID(stateMountPoint)
			state = mountstate.ProcessState{PID: pid}
		}
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return runErr
			}
			if runErr != nil {
				return runErr
			}
			return err
		}

		if deps.terminateState == nil && deps.terminate == nil {
			if runErr != nil {
				return runErr
			}
			return fmt.Errorf("drive9 umount: no process terminator configured for Windows WebDAV mount")
		}

		if deps.terminateState != nil {
			err = deps.terminateState(state, *waitTimeout)
		} else {
			err = deps.terminate(state.PID, *waitTimeout)
		}
		if err != nil {
			if errors.Is(err, errMountProcessStateStale) || errors.Is(err, errMountProcessStateUnsafe) {
				if path != "" && deps.remove != nil {
					if removeErr := deps.remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) && runErr == nil {
						return fmt.Errorf("drive9 umount: remove mount pid file %s: %w", path, removeErr)
					}
				}
				if errors.Is(err, errMountProcessStateStale) {
					if deps.printErrf != nil {
						deps.printErrf("drive9 umount: removed stale mount pid file %s without terminating any process\n", path)
					}
					return runErr
				}
			}
			if runErr != nil {
				return runErr
			}
			return fmt.Errorf("%w (pid file: %s)", err, path)
		}
		if path != "" && deps.remove != nil {
			if err := deps.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				if runErr != nil {
					return runErr
				}
				return fmt.Errorf("drive9 umount: remove mount pid file %s: %w", path, err)
			}
		}
		return runErr
	}

	pid, path, err := deps.readPID(stateMountPoint)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return runErr
		}
		if runErr != nil {
			return runErr
		}
		return err
	}
	if *waitTimeout == 0 {
		return nil
	}
	if err := waitForPIDExit(pid, *waitTimeout, deps); err != nil {
		return fmt.Errorf("%w (pid file: %s)", err, path)
	}
	return nil
}

func waitForPIDExit(pid int, timeout time.Duration, deps umountDeps) error {
	if pid <= 0 {
		return fmt.Errorf("invalid mount process pid %d", pid)
	}
	deadline := deps.now().Add(timeout)
	for {
		if !deps.pidAlive(pid) {
			return nil
		}
		if !deps.now().Before(deadline) {
			if deps.printErrf != nil {
				deps.printErrf("drive9 umount: mount process pid %d still running after %s\n", pid, timeout)
			}
			return fmt.Errorf("drive9 umount: mount process pid %d still running after %s", pid, timeout)
		}
		deps.sleep(100 * time.Millisecond)
	}
}

func processAlive(pid int) bool {
	return processAliveImpl(pid)
}

var waitForProcessExit = waitForProcessExitByPID

func terminateProcess(pid int, waitTimeout time.Duration) error {
	if pid <= 0 {
		return fmt.Errorf("invalid mount process pid %d", pid)
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	err = process.Kill()
	if err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			err = nil
		} else {
			return err
		}
	}
	if waitTimeout > 0 {
		return waitForProcessExit(pid, waitTimeout)
	}
	if err != nil {
		return err
	}
	return nil
}

// resolveMountCredentials selects the (server, apiKey, token) triple that a
// fresh mount will be bound to. It locks the principal kind at mount time
// per Invariant #3 - once this function returns, the chosen credential is
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
		case CredentialOwner, CredentialFSScoped:
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
	if goos == "windows" {
		normalizedMountPoint, err := normalizeWebDAVMountPoint(goos, mountPoint)
		if err != nil {
			return nil, err
		}
		return []string{"net", "use", normalizedMountPoint, "/delete", "/y"}, nil
	}
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
