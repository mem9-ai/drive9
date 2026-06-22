package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/mem9-ai/drive9/pkg/buildinfo"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	drive9webdav "github.com/mem9-ai/drive9/pkg/webdav"
)

const (
	defaultFuseLookupRetryCount        = 3
	defaultFuseLookupRetryTimeout      = 2 * time.Second
	defaultFuseReadConcurrency         = 24
	defaultFuseParallelReadConcurrency = 4
	defaultFuseParallelReadBlockSizeMB = 1
	defaultMountBackgroundReadyTimeout = 30 * time.Second
	defaultMountPerfPprofAddr          = "127.0.0.1:0"
	defaultMountPerfCPUDuration        = 30 * time.Second
	defaultMountPerfCPUInterval        = 10 * time.Minute
	defaultMountPerfHeapInterval       = 10 * time.Minute
)

var (
	errMountProcessStateStale  = errors.New("drive9 umount: stale mount process state")
	errMountProcessStateUnsafe = errors.New("drive9 umount: unsafe mount process state")
)

type mountBackgroundRequest struct {
	Args       []string
	MountPoint string
	Server     string
	APIKey     string
	Token      string
}

var startMountBackground = startMountBackgroundImpl

// MountCmd handles the "drive9 mount" command.
//
// Dispatch fork (Row A, V2e): the first positional argument selects the
// mount flavour.
//
//   - `drive9 mount vault <path>`       -> read-only vault FUSE filesystem
//   - `drive9 mount drain <mountpoint>` -> drain pending writes for a live FUSE mount
//   - `drive9 mount [flags] <path>`     -> legacy writable fs mount (no
//     subcommand keyword; first positional is the mount point)
//
// We MUST peek at the first arg before flag.Parse because the vault flag
// set is smaller (no cache-size / write-path knobs), and a single flag
// set would quietly accept write-path flags for a vault mount - that
// would violate Row C (read-only) in a subtle, mount-time-visible way.
//
// Only the CURRENT supported subcommand/backend keywords ("vault", "drain") are
// reserved here. Every other first positional falls through to the legacy parser,
// which enforces "exactly one mountpoint" so `drive9 mount kv /mnt/x` fails as a
// positional-arity error rather than by pre-reserving backend-shaped words that do
// not exist yet.
func MountCmd(args []string) error {
	fmt.Fprint(os.Stderr, buildinfo.String("drive9 mount"))
	if len(args) > 0 {
		if args[0] == "vault" {
			return vaultMountCmd(args[1:], true)
		}
		if args[0] == "drain" {
			return MountDrainCmd(args[1:])
		}
	}
	return fsMountCmdWithBackground(args, true)
}

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
// In foreground mode, drive9fuse.Mount runs in-process and credentials flow
// through MountOptions{Server, APIKey, Token}. In default background mode,
// this command starts a fresh `drive9 mount --foreground` child and passes the
// resolved credential snapshot through that child's environment; the child
// consumes and unsets those env vars through the normal resolver path.
func fsMountCmd(args []string) error {
	return fsMountCmdWithBackground(args, false)
}

func fsMountCmdWithBackground(args []string, background bool) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	server := fs.String("server", "", "drive9 server URL (overrides $DRIVE9_SERVER and config)")
	apiKey := fs.String("api-key", "", "owner API key (overrides $DRIVE9_API_KEY and config)")
	mode := fs.String("mode", "auto", "mount mode: auto, fuse, or webdav")
	foreground := fs.Bool("foreground", false, "run in the foreground and block until unmounted")
	cacheDir := fs.String("cache-dir", "", "write-back cache directory (default ~/.cache/drive9)")
	cacheSize := fs.Int("cache-size", 128, "read cache size in MB")
	readCacheMaxFile := fs.Int64("read-cache-max-file-mb", 4, "maximum single file size admitted to read cache in MB; files at or below this size are fetched with a single whole-file request")
	readCacheTTL := fs.Duration("read-cache-ttl", 30*time.Second, "read cache TTL; 0 disables time-based expiry")
	diskReadCacheSize := fs.Int64("disk-read-cache-size-mb", 1024, "disk-backed read cache size in MB")
	diskReadCacheFreeRatio := fs.Float64("disk-read-cache-free-ratio", 0.10, "minimum filesystem free-space ratio before disk read cache evicts")
	dirTTL := fs.Duration("dir-ttl", 10*time.Second, "directory cache TTL")
	attrTTL := fs.Duration("attr-ttl", 10*time.Second, "kernel attr cache TTL")
	entryTTL := fs.Duration("entry-ttl", 10*time.Second, "kernel entry cache TTL")
	flushDebounce := fs.Duration("flush-debounce", -1, "debounce window for small-file flush coalescing (default 2s, 0 disables)")
	lookupRetryCount := fs.Int("lookup-retry-count", defaultFuseLookupRetryCount, "detached retries after transient Lookup/GetAttr stat failures (set 0 to disable)")
	lookupRetryTimeout := fs.Duration("lookup-retry-timeout", defaultFuseLookupRetryTimeout, "timeout per detached Lookup/GetAttr stat retry (must be > 0 when set)")
	readConcurrency := fs.Int("read-concurrency", defaultFuseReadConcurrency, "maximum concurrent backend reads issued by FUSE")
	parallelReadConcurrency := fs.Int("parallel-read-concurrency", defaultFuseParallelReadConcurrency, "maximum concurrent block reads for one large FUSE read")
	parallelReadBlockSize := fs.Int64("parallel-read-block-size-mb", defaultFuseParallelReadBlockSizeMB, "block size in MB for parallel large-file reads")
	syncRead := fs.Bool("fuse-sync-read", false, "disable kernel async read dispatch; at most one read in flight per file handle")
	legacyDirStatFallback := fs.Bool("legacy-dir-stat-fallback", false, "on Lookup stat 404, list parent to support legacy servers without directory stat")
	readDirPrefetch := fs.Bool("readdir-prefetch", false, "prefetch small files after directory reads into the read cache")
	prefetchMaxFiles := fs.Int("readdir-prefetch-max-files", 32, "maximum small files prefetched per directory read")
	prefetchMaxFileBytes := fs.Int64("readdir-prefetch-max-file-bytes", 50_000, "maximum individual file size prefetched by readdir prefetch")
	prefetchMaxBytes := fs.Int64("readdir-prefetch-max-bytes", 1<<20, "maximum aggregate bytes prefetched per directory read")
	prefetchTimeout := fs.Duration("readdir-prefetch-timeout", time.Second, "timeout for one readdir prefetch batch")
	trustProcessLocalEvents := fs.Bool("trust-process-local-events", false, "allow revision-bound GetAttr dir-cache hits using process-local SSE freshness; only safe for single-server/sticky routing or cluster-wide event streams")
	durability := fs.String("durability", string(fuseDurabilityAuto), "write durability: auto, interactive, fsync, close-sync, or write-sync")
	layerRef := fs.String("layer", "", "mount through writable fs layer (layer id, name, or tag ref)")
	checkpointRef := fs.String("checkpoint", "", "restore fs layer checkpoint before mounting")
	profile := fs.String("profile", "", "mount profile: coding-agent (default), portable, none, interactive, or a ~/.drive9/profiles/<name> file")
	localRoot := fs.String("local-root", "", "local-only overlay storage root (auto-generated for overlay profiles)")
	var localOnlyPatterns stringListFlag
	var remoteOnlyPatterns stringListFlag
	var unpackArchives stringListFlag
	noAutoUnpack := fs.Bool("no-auto-unpack", false, "disable automatic profile pack restore before mounting")
	fs.Var(&localOnlyPatterns, "local-only", "additional local-only path pattern for overlay routing (repeatable, e.g. **/node_modules/**)")
	fs.Var(&remoteOnlyPatterns, "remote-only", "remote-persistent override path pattern for overlay routing (repeatable)")
	fs.Var(&unpackArchives, "unpack", "restore a drive9 pack archive into --local-root before mounting (repeatable)")
	uploadConcurrency := fs.Int("upload-concurrency", 16, "maximum concurrent background uploads issued by FUSE")
	dirCacheMaxEntries := fs.Int("dir-cache-max-entries", 100000, "maximum entries per directory in namespace cache before complete marking is disabled")
	commitQueueMaxPending := fs.Int("commit-queue-max-pending", 100, "maximum pending entries in CommitQueue before backpressure")
	allowOther := fs.Bool("allow-other", false, "allow other users to access mount")
	readOnly := fs.Bool("read-only", false, "mount as read-only")
	debug := fs.Bool("debug", false, "enable FUSE debug logging")
	perfDir := fs.String("perf-dir", "", "enable standard FUSE profiling outputs in this directory")
	perfInterval := fs.Duration("perf-interval", 0, "continuous performance sample interval (default 10s when --perf-dir is set)")
	perfMaxSamples := fs.Int("perf-max-samples", 0, "maximum samples per continuous perf JSONL segment (default 7200 when --perf-dir is set)")
	perfMaxSampleFiles := fs.Int("perf-max-sample-files", 0, "maximum retained continuous perf sample files (default 2 when --perf-dir is set)")
	perfMaxProfileFiles := fs.Int("perf-max-profile-files", 0, "maximum retained CPU and heap profile files per type (default 48 when --perf-dir is set)")
	perfCPUDuration := fs.Duration("perf-cpu-duration", 0, "CPU profile capture window duration (default 30s when --perf-dir is set)")
	perfCPUInterval := fs.Duration("perf-cpu-interval", 0, "periodically capture CPU profiles at this interval; one capture starts immediately when profiling begins (default 10m when --perf-dir is set)")
	perfHeapInterval := fs.Duration("perf-heap-interval", 0, "periodically write heap profiles at this interval (default 10m when --perf-dir is set)")
	perfAddr := fs.String("perf-addr", "", "serve live pprof on this address, e.g. 127.0.0.1:6060")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 mount [flags] [:/remote] <mountpoint>\n\nflags:\n")
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
	readCacheTTLGiven := flagProvided(fs, "read-cache-ttl")
	trustProcessLocalEventsGiven := flagProvided(fs, "trust-process-local-events")
	perfDirGiven := flagProvided(fs, "perf-dir")
	perfIntervalGiven := flagProvided(fs, "perf-interval")
	perfMaxSamplesGiven := flagProvided(fs, "perf-max-samples")
	perfMaxSampleFilesGiven := flagProvided(fs, "perf-max-sample-files")
	perfMaxProfileFilesGiven := flagProvided(fs, "perf-max-profile-files")
	perfCPUDurationGiven := flagProvided(fs, "perf-cpu-duration")
	perfCPUIntervalGiven := flagProvided(fs, "perf-cpu-interval")
	perfHeapIntervalGiven := flagProvided(fs, "perf-heap-interval")
	perfAddrGiven := flagProvided(fs, "perf-addr")
	if err := validateLookupRetryFlags(*lookupRetryCount, *lookupRetryTimeout, lookupRetryCountGiven, lookupRetryTimeoutGiven); err != nil {
		return err
	}
	if *readConcurrency <= 0 {
		return fmt.Errorf("drive9 mount: --read-concurrency must be > 0")
	}
	if *parallelReadConcurrency <= 0 {
		return fmt.Errorf("drive9 mount: --parallel-read-concurrency must be > 0")
	}
	if *parallelReadBlockSize <= 0 {
		return fmt.Errorf("drive9 mount: --parallel-read-block-size-mb must be > 0")
	}
	if *uploadConcurrency <= 0 {
		return fmt.Errorf("drive9 mount: --upload-concurrency must be > 0")
	}
	if *dirCacheMaxEntries <= 0 {
		return fmt.Errorf("drive9 mount: --dir-cache-max-entries must be > 0")
	}
	if *commitQueueMaxPending <= 0 {
		return fmt.Errorf("drive9 mount: --commit-queue-max-pending must be > 0")
	}
	if err := validateReadDirPrefetchFlags(*prefetchMaxFiles, *prefetchMaxFileBytes, *prefetchMaxBytes, *prefetchTimeout); err != nil {
		return err
	}
	if perfDirGiven && strings.TrimSpace(*perfDir) == "" {
		return fmt.Errorf("drive9 mount: --perf-dir must not be empty")
	}
	if perfAddrGiven && strings.TrimSpace(*perfAddr) == "" {
		return fmt.Errorf("drive9 mount: --perf-addr must not be empty")
	}
	effectivePerfCPUDuration := *perfCPUDuration
	effectivePerfCPUInterval := *perfCPUInterval
	effectivePerfHeapInterval := *perfHeapInterval
	if perfDirGiven {
		if !perfCPUDurationGiven {
			effectivePerfCPUDuration = defaultMountPerfCPUDuration
		}
		if !perfCPUIntervalGiven {
			effectivePerfCPUInterval = defaultMountPerfCPUInterval
		}
		if !perfHeapIntervalGiven {
			effectivePerfHeapInterval = defaultMountPerfHeapInterval
		}
	}
	if err := validateMountPerfFlags(mountPerfFlagValidation{
		perfDirGiven:             perfDirGiven,
		perfInterval:             *perfInterval,
		perfMaxSamples:           *perfMaxSamples,
		perfMaxSampleFiles:       *perfMaxSampleFiles,
		perfMaxProfileFiles:      *perfMaxProfileFiles,
		perfCPUDuration:          effectivePerfCPUDuration,
		perfCPUInterval:          effectivePerfCPUInterval,
		perfHeapInterval:         effectivePerfHeapInterval,
		perfIntervalGiven:        perfIntervalGiven,
		perfMaxSamplesGiven:      perfMaxSamplesGiven,
		perfMaxSampleFilesGiven:  perfMaxSampleFilesGiven,
		perfMaxProfileFilesGiven: perfMaxProfileFilesGiven,
		perfCPUDurationGiven:     perfCPUDurationGiven,
		perfCPUIntervalGiven:     perfCPUIntervalGiven,
		perfHeapIntervalGiven:    perfHeapIntervalGiven,
		perfAddrGiven:            perfAddrGiven,
	}); err != nil {
		return err
	}
	var profileHeap, profileDir, perfJSONL, pprofAddr string
	applyMountPerfDirDefaults(*perfDir, *perfAddr, &profileHeap, &profileDir, &perfJSONL, &pprofAddr)
	profileGiven := flagProvided(fs, "profile")
	resolved := ResolveMountMode(mountMode, runtime.GOOS, exec.LookPath)
	fmt.Fprintf(os.Stderr, "drive9: mount mode: %s\n", resolved)
	var profileCfg profileConfig
	if !profileGiven && resolved == MountModeWebDAV {
		profileCfg = builtinNoneProfile()
	} else {
		var err error
		profileCfg, err = loadProfileConfig(*profile)
		if err != nil {
			return err
		}
	}
	*profile = profileCfg.Name
	effectiveLocalOnlyPatterns := mergeProfileValues(profileCfg.LocalOnlyPatterns, localOnlyPatterns)
	effectiveRemoteOnlyPatterns := mergeProfileValues(profileCfg.RemoteOnlyPatterns, remoteOnlyPatterns)
	effectivePackPaths := mergeProfileValues(profileCfg.PackPaths)
	normalizedLocalRoot := strings.TrimSpace(*localRoot)
	syncModeVal, writePolicyVal, err := parseFuseDurability(*durability)
	if err != nil {
		return err
	}
	if *readCacheMaxFile <= 0 {
		return fmt.Errorf("drive9 mount: --read-cache-max-file-mb must be > 0")
	}
	normalizedReadCacheTTL, err := readCacheTTLFlagValue(readCacheTTLGiven, *readCacheTTL)
	if err != nil {
		return err
	}
	if *diskReadCacheSize <= 0 {
		return fmt.Errorf("drive9 mount: --disk-read-cache-size-mb must be > 0")
	}
	if *diskReadCacheFreeRatio <= 0 || *diskReadCacheFreeRatio >= 1 {
		return fmt.Errorf("drive9 mount: --disk-read-cache-free-ratio must be > 0 and < 1")
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

	overlayProfile := profileAllowsOverlay(profileCfg.Name)
	if overlayProfile && resolved != MountModeFUSE {
		return fmt.Errorf("drive9 mount: --profile=%s requires --mode=fuse", profileCfg.Name)
	}
	if resolved == MountModeWebDAV && *readOnly {
		return fmt.Errorf("drive9 mount: --read-only is not supported with WebDAV mode")
	}
	if resolved == MountModeWebDAV && trustProcessLocalEventsGiven {
		return fmt.Errorf("drive9 mount: --trust-process-local-events is only supported with --mode=fuse")
	}
	if resolved == MountModeWebDAV && readCacheTTLGiven {
		return fmt.Errorf("drive9 mount: --read-cache-ttl is only supported with --mode=fuse")
	}
	if resolved == MountModeWebDAV && *durability != string(fuseDurabilityAuto) {
		return fmt.Errorf("--durability is only supported with --mode=fuse; WebDAV mounts always use their native write behavior")
	}
	if resolved == MountModeWebDAV && perfDirGiven {
		return fmt.Errorf("--perf-dir is only supported with --mode=fuse")
	}
	if resolved == MountModeWebDAV && strings.TrimSpace(*layerRef) != "" {
		return fmt.Errorf("drive9 mount: --layer is only supported with --mode=fuse")
	}
	if resolved == MountModeWebDAV && strings.TrimSpace(*checkpointRef) != "" {
		return fmt.Errorf("drive9 mount: --checkpoint is only supported with --mode=fuse")
	}
	if strings.TrimSpace(*checkpointRef) != "" && strings.TrimSpace(*layerRef) == "" {
		return fmt.Errorf("drive9 mount: --checkpoint requires --layer")
	}

	serverVal, apiKeyVal, tokenVal, err := resolveMountCredentials(ResolveCredentials(), *server, *apiKey)
	if err != nil {
		return err
	}
	*server, *apiKey = serverVal, apiKeyVal
	token := tokenVal

	if len(unpackArchives) > 0 {
		if resolved != MountModeFUSE {
			return fmt.Errorf("drive9 mount: --unpack is only supported with --mode=fuse")
		}
		if !overlayProfile {
			return fmt.Errorf("drive9 mount: --unpack requires an overlay profile")
		}
	}
	if normalizedLocalRoot == "" && overlayProfile && resolved == MountModeFUSE {
		normalizedLocalRoot, err = defaultMountLocalRoot(*server, remoteRoot, mountCredentialCacheKey(apiKeyVal, tokenVal))
		if err != nil {
			return err
		}
	}
	if err := validateMountProfileFlags(profileCfg.Name, normalizedLocalRoot, effectiveLocalOnlyPatterns, effectiveRemoteOnlyPatterns, effectivePackPaths); err != nil {
		return err
	}

	autoUnpack := overlayProfile && len(effectivePackPaths) > 0 && !*noAutoUnpack
	if runtime.GOOS == "windows" && resolved == MountModeFUSE && len(unpackArchives) == 0 && !autoUnpack {
		return mountFuse(&mountFuseOptions{
			MountPoint:         mountPoint,
			RemoteRoot:         remoteRoot,
			Profile:            profileCfg.Name,
			LayerRef:           strings.TrimSpace(*layerRef),
			CheckpointRef:      strings.TrimSpace(*checkpointRef),
			LocalRoot:          normalizedLocalRoot,
			LocalOnlyPatterns:  append([]string(nil), effectiveLocalOnlyPatterns...),
			RemoteOnlyPatterns: append([]string(nil), effectiveRemoteOnlyPatterns...),
			PackPaths:          append([]string(nil), effectivePackPaths...),
			ReadOnly:           *readOnly,
			Debug:              *debug,
		})
	}

	if background && !*foreground {
		return startMountBackground(mountBackgroundRequest{
			Args:       append([]string(nil), args...),
			MountPoint: mountPoint,
			Server:     serverVal,
			APIKey:     apiKeyVal,
			Token:      tokenVal,
		})
	}

	// WebDAV path: create client, start local WebDAV server, invoke mount_webdav.
	if resolved == MountModeWebDAV {
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

	if autoUnpack || len(unpackArchives) > 0 {
		var c *client.Client
		if token != "" {
			c = client.NewWithToken(*server, token)
		} else {
			c = client.New(*server, *apiKey)
		}
		if autoUnpack {
			archivePath, err := defaultPackArchivePath(remoteRoot, profileCfg.Name)
			if err != nil {
				return err
			}
			if _, err := unpackRemoteArchiveIfExists(context.Background(), c, archivePath, unpackOptions{
				LocalRoot: normalizedLocalRoot,
				Replace:   true,
			}); err != nil {
				return fmt.Errorf("drive9 mount: auto-unpack :%s: %w", archivePath, err)
			}
		}
		for _, archiveArg := range unpackArchives {
			archiveClient, archivePath, err := clientForRemoteArchiveArg(c, archiveArg)
			if err != nil {
				return err
			}
			if err := unpackRemoteArchive(context.Background(), archiveClient, archivePath, unpackOptions{
				LocalRoot: normalizedLocalRoot,
				Replace:   true,
			}); err != nil {
				return fmt.Errorf("drive9 mount: unpack %s: %w", archiveArg, err)
			}
		}
	}

	if runtime.GOOS == "windows" && resolved == MountModeFUSE {
		return mountFuse(&mountFuseOptions{
			MountPoint:         mountPoint,
			RemoteRoot:         remoteRoot,
			Profile:            profileCfg.Name,
			LayerRef:           strings.TrimSpace(*layerRef),
			CheckpointRef:      strings.TrimSpace(*checkpointRef),
			LocalRoot:          normalizedLocalRoot,
			LocalOnlyPatterns:  append([]string(nil), effectiveLocalOnlyPatterns...),
			RemoteOnlyPatterns: append([]string(nil), effectiveRemoteOnlyPatterns...),
			PackPaths:          append([]string(nil), effectivePackPaths...),
			ReadOnly:           *readOnly,
			Debug:              *debug,
		})
	}

	// FUSE path (existing behavior).
	opts := &mountFuseOptions{
		Server:                  *server,
		APIKey:                  *apiKey,
		Token:                   token,
		MountPoint:              mountPoint,
		RemoteRoot:              remoteRoot,
		CacheDir:                *cacheDir,
		CacheSize:               int64(*cacheSize) << 20,
		ReadCacheMaxFileBytes:   *readCacheMaxFile << 20,
		ReadCacheTTL:            normalizedReadCacheTTL,
		DiskReadCacheSize:       *diskReadCacheSize << 20,
		DiskReadCacheFreeRatio:  *diskReadCacheFreeRatio,
		DirTTL:                  normalizedDirTTL,
		AttrTTL:                 normalizedAttrTTL,
		EntryTTL:                normalizedEntryTTL,
		FlushDebounce:           *flushDebounce,
		LookupRetryCount:        normalizedLookupRetryCount,
		LookupRetryTimeout:      normalizedLookupRetryTimeout,
		LegacyDirStatFallback:   *legacyDirStatFallback,
		ReadDirPrefetch:         *readDirPrefetch,
		PrefetchMaxFiles:        *prefetchMaxFiles,
		PrefetchMaxFileBytes:    *prefetchMaxFileBytes,
		PrefetchMaxBytes:        *prefetchMaxBytes,
		PrefetchTimeout:         *prefetchTimeout,
		TrustLocalEvents:        *trustProcessLocalEvents,
		SyncMode:                syncModeVal,
		WritePolicy:             writePolicyVal,
		Profile:                 profileCfg.Name,
		LayerRef:                strings.TrimSpace(*layerRef),
		CheckpointRef:           strings.TrimSpace(*checkpointRef),
		LocalRoot:               normalizedLocalRoot,
		LocalOnlyPatterns:       append([]string(nil), effectiveLocalOnlyPatterns...),
		RemoteOnlyPatterns:      append([]string(nil), effectiveRemoteOnlyPatterns...),
		PackPaths:               append([]string(nil), effectivePackPaths...),
		UploadConcurrency:       *uploadConcurrency,
		DirCacheMaxEntries:      *dirCacheMaxEntries,
		CommitQueueMaxPending:   *commitQueueMaxPending,
		ReadConcurrency:         *readConcurrency,
		ParallelReadConcurrency: *parallelReadConcurrency,
		ParallelReadBlockSize:   *parallelReadBlockSize << 20,
		SyncRead:                *syncRead,
		AllowOther:              *allowOther,
		ReadOnly:                *readOnly,
		Debug:                   *debug,
		ProfileCPUDuration:      effectivePerfCPUDuration,
		ProfileCPUInterval:      effectivePerfCPUInterval,
		ProfileHeap:             profileHeap,
		ProfileDir:              profileDir,
		ProfileHeapInterval:     effectivePerfHeapInterval,
		PprofAddr:               pprofAddr,
		PerfSamplesPath:         perfJSONL,
		PerfSampleInterval:      *perfInterval,
		PerfMaxSamples:          *perfMaxSamples,
		PerfMaxSampleFiles:      *perfMaxSampleFiles,
		PerfMaxProfileFiles:     *perfMaxProfileFiles,
	}

	return mountFuse(opts)
}

func startMountBackgroundImpl(req mountBackgroundRequest) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("drive9 mount: locate executable: %w", err)
	}
	logPath, logFile, err := openMountBackgroundLog(req.MountPoint)
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return fmt.Errorf("drive9 mount: open %s: %w", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()

	cmd := exec.Command(exe, foregroundMountCommandArgs(req.Args)...)
	cmd.Env = mountBackgroundEnv(os.Environ(), req)
	cmd.Stdin = devNull
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	configureMountBackgroundCommand(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("drive9 mount: start background mount: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	if err := waitForBackgroundMountReady(req.MountPoint, cmd.Process.Pid, waitCh, logPath, defaultMountBackgroundReadyTimeout); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "drive9: mount running in background (pid: %d, log: %s)\n", cmd.Process.Pid, logPath)
	fmt.Fprintf(os.Stderr, "drive9: unmount with `drive9 umount %s`\n", req.MountPoint)
	return nil
}

func foregroundMountCommandArgs(args []string) []string {
	out := []string{"mount"}
	if len(args) > 0 && args[0] == "vault" {
		out = append(out, "vault", "--foreground")
		out = append(out, stripBackgroundOnlyCredentialArgs(args[1:])...)
		return out
	}
	out = append(out, "--foreground")
	out = append(out, stripBackgroundOnlyCredentialArgs(args)...)
	return out
}

func stripBackgroundOnlyCredentialArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--api-key" {
			if i+1 < len(args) {
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "--api-key=") {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func mountBackgroundEnv(environ []string, req mountBackgroundRequest) []string {
	out := make([]string, 0, len(environ)+2)
	for _, kv := range environ {
		if strings.HasPrefix(kv, EnvServer+"=") ||
			strings.HasPrefix(kv, EnvAPIKey+"=") ||
			strings.HasPrefix(kv, EnvVaultToken+"=") ||
			strings.HasPrefix(kv, EnvTiDBCloudPublicKey+"=") ||
			strings.HasPrefix(kv, EnvTiDBCloudPrivateKey+"=") {
			continue
		}
		out = append(out, kv)
	}
	if req.Server != "" {
		out = append(out, EnvServer+"="+req.Server)
	}
	if req.Token != "" {
		out = append(out, EnvVaultToken+"="+req.Token)
	} else if req.APIKey != "" {
		out = append(out, EnvAPIKey+"="+req.APIKey)
	}
	return out
}

func openMountBackgroundLog(mountPoint string) (string, *os.File, error) {
	path, err := mountBackgroundLogPath(mountPoint)
	if err != nil {
		return "", nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return "", nil, fmt.Errorf("drive9 mount: open background log %s: %w", path, err)
	}
	_, _ = fmt.Fprintf(f, "\n--- drive9 mount background start %s ---\n", time.Now().Format(time.RFC3339))
	return path, f, nil
}

func mountBackgroundLogPath(mountPoint string) (string, error) {
	cacheRoot, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(cacheRoot) == "" {
		cacheRoot = os.TempDir()
	}
	dir := filepath.Join(cacheRoot, "drive9", "mount-logs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("drive9 mount: create background log directory: %w", err)
	}
	canonical := filepath.Clean(mountPoint)
	if abs, err := filepath.Abs(canonical); err == nil {
		canonical = abs
	}
	if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
		canonical = resolved
	}
	sum := sha256.Sum256([]byte(canonical))
	return filepath.Join(dir, "mount-"+hex.EncodeToString(sum[:8])+".log"), nil
}

func waitForBackgroundMountReady(mountPoint string, pid int, waitCh <-chan error, logPath string, timeout time.Duration) error {
	return waitForBackgroundMountReadyWithDeps(mountPoint, pid, waitCh, logPath, timeout, backgroundMountReadyDeps{
		readProcessState: mountstate.ReadProcessState,
		terminate:        terminateProcess,
		now:              time.Now,
		sleep:            time.Sleep,
	})
}

type backgroundMountReadyDeps struct {
	readProcessState func(string) (mountstate.ProcessState, string, error)
	terminate        func(int, time.Duration) error
	now              func() time.Time
	sleep            func(time.Duration)
}

func waitForBackgroundMountReadyWithDeps(mountPoint string, pid int, waitCh <-chan error, logPath string, timeout time.Duration, deps backgroundMountReadyDeps) error {
	if deps.readProcessState == nil {
		deps.readProcessState = mountstate.ReadProcessState
	}
	if deps.terminate == nil {
		deps.terminate = terminateProcess
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.sleep == nil {
		deps.sleep = time.Sleep
	}
	stateMountPoint := backgroundMountStatePoint(mountPoint)
	deadline := deps.now().Add(timeout)
	for {
		select {
		case err := <-waitCh:
			if err != nil {
				return fmt.Errorf("drive9 mount: background mount exited before becoming ready: %w (log: %s)", err, logPath)
			}
			return fmt.Errorf("drive9 mount: background mount exited before becoming ready (log: %s)", logPath)
		default:
		}

		state, _, err := deps.readProcessState(stateMountPoint)
		if err == nil {
			if state.PID == pid {
				return nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			if stopErr := stopBackgroundMountProcess(pid, waitCh, deps, 5*time.Second); stopErr != nil {
				return fmt.Errorf("drive9 mount: read mount process state: %w; failed to stop background mount pid %d: %v", err, pid, stopErr)
			}
			return fmt.Errorf("drive9 mount: read mount process state: %w", err)
		}

		if !deps.now().Before(deadline) {
			_ = stopBackgroundMountProcess(pid, waitCh, deps, 5*time.Second)
			return fmt.Errorf("drive9 mount: timed out waiting for background mount to become ready after %s (log: %s)", timeout, logPath)
		}
		deps.sleep(50 * time.Millisecond)
	}
}

func stopBackgroundMountProcess(pid int, waitCh <-chan error, deps backgroundMountReadyDeps, timeout time.Duration) error {
	var stopErr error
	if deps.terminate != nil {
		stopErr = deps.terminate(pid, timeout)
	}
	if waitCh == nil {
		return stopErr
	}
	deadline := deps.now().Add(timeout)
	for {
		select {
		case <-waitCh:
			return stopErr
		default:
		}
		if timeout <= 0 || !deps.now().Before(deadline) {
			if stopErr != nil {
				return stopErr
			}
			return fmt.Errorf("background mount pid %d did not exit after stop request", pid)
		}
		deps.sleep(10 * time.Millisecond)
	}
}

func backgroundMountStatePoint(mountPoint string) string {
	if runtime.GOOS == "windows" {
		if stateMountPoint, err := webdavMountStatePoint(runtime.GOOS, mountPoint); err == nil {
			return stateMountPoint
		}
	}
	return mountPoint
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

type mountPerfFlagValidation struct {
	perfDirGiven             bool
	perfInterval             time.Duration
	perfMaxSamples           int
	perfMaxSampleFiles       int
	perfMaxProfileFiles      int
	perfCPUDuration          time.Duration
	perfCPUInterval          time.Duration
	perfHeapInterval         time.Duration
	perfIntervalGiven        bool
	perfMaxSamplesGiven      bool
	perfMaxSampleFilesGiven  bool
	perfMaxProfileFilesGiven bool
	perfCPUDurationGiven     bool
	perfCPUIntervalGiven     bool
	perfHeapIntervalGiven    bool
	perfAddrGiven            bool
}

func validateMountPerfFlags(v mountPerfFlagValidation) error {
	if v.perfIntervalGiven && v.perfInterval <= 0 {
		return fmt.Errorf("drive9 mount: --perf-interval must be > 0")
	}
	if v.perfMaxSamplesGiven && v.perfMaxSamples <= 0 {
		return fmt.Errorf("drive9 mount: --perf-max-samples must be > 0")
	}
	if v.perfMaxSampleFilesGiven && v.perfMaxSampleFiles <= 0 {
		return fmt.Errorf("drive9 mount: --perf-max-sample-files must be > 0")
	}
	if v.perfMaxProfileFilesGiven && v.perfMaxProfileFiles <= 0 {
		return fmt.Errorf("drive9 mount: --perf-max-profile-files must be > 0")
	}
	if v.perfCPUDurationGiven && v.perfCPUDuration <= 0 {
		return fmt.Errorf("drive9 mount: --perf-cpu-duration must be > 0")
	}
	if v.perfCPUIntervalGiven && v.perfCPUInterval <= 0 {
		return fmt.Errorf("drive9 mount: --perf-cpu-interval must be > 0")
	}
	if v.perfHeapIntervalGiven && v.perfHeapInterval <= 0 {
		return fmt.Errorf("drive9 mount: --perf-heap-interval must be > 0")
	}
	if v.perfDirGiven && v.perfCPUDuration >= v.perfCPUInterval {
		return fmt.Errorf("drive9 mount: --perf-cpu-duration must be less than --perf-cpu-interval")
	}
	for _, name := range []struct {
		flag  string
		given bool
	}{
		{"--perf-interval", v.perfIntervalGiven},
		{"--perf-max-samples", v.perfMaxSamplesGiven},
		{"--perf-max-sample-files", v.perfMaxSampleFilesGiven},
		{"--perf-max-profile-files", v.perfMaxProfileFilesGiven},
		{"--perf-cpu-duration", v.perfCPUDurationGiven},
		{"--perf-cpu-interval", v.perfCPUIntervalGiven},
		{"--perf-heap-interval", v.perfHeapIntervalGiven},
		{"--perf-addr", v.perfAddrGiven},
	} {
		if name.given && !v.perfDirGiven {
			return fmt.Errorf("drive9 mount: %s requires --perf-dir", name.flag)
		}
	}
	return nil
}

func applyMountPerfDirDefaults(perfDir string, perfAddr string, profileHeap, profileDir, perfJSONL, pprofAddr *string) {
	perfDir = strings.TrimSpace(perfDir)
	if perfDir == "" {
		return
	}
	*profileHeap = filepath.Join(perfDir, "heap-final.pprof")
	*profileDir = perfDir
	*perfJSONL = filepath.Join(perfDir, "perf.jsonl")
	*pprofAddr = defaultMountPerfPprofAddr
	if addr := strings.TrimSpace(perfAddr); addr != "" {
		*pprofAddr = addr
	}
}

func validateMountProfileFlags(profile string, localRoot string, localOnlyPatterns []string, remoteOnlyPatterns []string, packPaths []string) error {
	if err := validateProfileName(profile); err != nil {
		return err
	}
	hasPolicyFlags := localRoot != "" || len(localOnlyPatterns) > 0 || len(remoteOnlyPatterns) > 0 || len(packPaths) > 0
	if !profileAllowsOverlay(profile) {
		if hasPolicyFlags {
			return fmt.Errorf("drive9 mount: --local-root, --local-only, --remote-only, and pack paths require an overlay profile")
		}
		return nil
	}
	if strings.TrimSpace(localRoot) == "" {
		return fmt.Errorf("drive9 mount: --profile=%s requires --local-root", profile)
	}
	if !filepath.IsAbs(localRoot) {
		return fmt.Errorf("drive9 mount: --local-root must be an absolute path")
	}
	if err := validateMountPolicyPatterns(localOnlyPatterns, remoteOnlyPatterns); err != nil {
		return err
	}
	return nil
}

func profileAllowsOverlay(profile string) bool {
	profile = strings.TrimSpace(profile)
	return profile != "" && profile != "interactive" && profile != noneMountProfile
}

func defaultMountLocalRoot(server string, remoteRoot string, credentialKey string) (string, error) {
	cacheRoot, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(cacheRoot) == "" {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			if err != nil {
				return "", fmt.Errorf("drive9 mount: cannot determine cache directory: %w", err)
			}
			return "", fmt.Errorf("drive9 mount: cannot determine cache directory: %w", homeErr)
		}
		cacheRoot = filepath.Join(home, ".cache")
	}
	remoteRoot, err = mountpath.NormalizeRoot(remoteRoot)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(server) + "\n" + remoteRoot + "\n" + credentialKey))
	hash := hex.EncodeToString(sum[:8])
	label := safePackArchiveLabel(pathBase(remoteRoot))
	if label == "" {
		label = "root"
	}
	return filepath.Join(cacheRoot, "drive9", "mounts", label+"-"+hash), nil
}

func mountCredentialCacheKey(apiKey, token string) string {
	credential := strings.TrimSpace(apiKey)
	if credential == "" {
		credential = strings.TrimSpace(token)
	}
	if credential == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(sum[:8])
}

func pathBase(value string) string {
	value = strings.TrimSuffix(value, "/")
	if value == "" {
		return "root"
	}
	idx := strings.LastIndex(value, "/")
	if idx >= 0 {
		value = value[idx+1:]
	}
	return value
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

func readCacheTTLFlagValue(given bool, value time.Duration) (time.Duration, error) {
	if !given {
		return 0, nil
	}
	if value < 0 {
		return 0, fmt.Errorf("drive9 mount: --read-cache-ttl must be >= 0")
	}
	if value == 0 {
		return -time.Nanosecond, nil
	}
	return value, nil
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
	packAfterUnmount func(context.Context, mountstate.ProcessState, []string, []string) error
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
		packAfterUnmount: defaultPackAfterUnmount,
		now:              time.Now,
		sleep:            time.Sleep,
		printErrf:        func(format string, args ...any) { fmt.Fprintf(os.Stderr, format, args...) },
	}
}

func runUmount(args []string, deps umountDeps) error {
	fs := flag.NewFlagSet("umount", flag.ContinueOnError)
	waitTimeout := fs.Duration("timeout", 60*time.Second, "time to wait for the drive9 mount process to exit after unmount; 0 disables waiting")
	noAutoPack := fs.Bool("no-auto-pack", false, "disable automatic profile pack upload after unmount")
	var packArchives stringListFlag
	var packPaths stringListFlag
	fs.Var(&packArchives, "pack", "also pack profile local overlay paths to this remote archive after unmount (repeatable)")
	fs.Var(&packPaths, "pack-path", "local overlay path to include in the automatic/default pack (repeatable; relative paths resolve under the mount remote root)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 umount [--timeout duration] [--pack :/archive.tar.gz] [--pack-path path]... [--no-auto-pack] <mountpoint>\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("usage: drive9 umount [--timeout duration] [--pack :/archive.tar.gz] [--pack-path path]... [--no-auto-pack] <mountpoint>")
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

	var packState mountstate.ProcessState
	packStateOK := false
	needPackState := len(packArchives) > 0 || len(packPaths) > 0 || !*noAutoPack
	if needPackState {
		if deps.readProcessState == nil {
			if len(packArchives) > 0 || len(packPaths) > 0 {
				return fmt.Errorf("drive9 umount: pack requires mount process metadata")
			}
		} else {
			var err error
			packState, _, err = deps.readProcessState(stateMountPoint)
			if err != nil {
				if len(packArchives) > 0 || len(packPaths) > 0 {
					return fmt.Errorf("drive9 umount: read mount state for pack: %w", err)
				}
			} else {
				packStateOK = true
			}
		}
	}

	packArchiveArgs := append([]string(nil), packArchives...)
	if packStateOK {
		if len(packArchives) > 0 || len(packPaths) > 0 {
			if err := validateUmountPackState(packState); err != nil {
				return err
			}
		}
		if !*noAutoPack && (len(packState.PackPaths) > 0 || len(packPaths) > 0) {
			if err := validateUmountPackState(packState); err != nil {
				return err
			}
			defaultArchive, err := defaultPackArchivePath(packState.RemoteRoot, packState.Profile)
			if err != nil {
				return err
			}
			packArchiveArgs = prependPackArchiveArg(packArchiveArgs, ":"+defaultArchive)
		}
	}
	if len(packPaths) > 0 && len(packArchiveArgs) == 0 {
		return fmt.Errorf("drive9 umount: --pack-path requires an auto-pack mount or --pack")
	}

	argv, err := umountArgv(deps.goos, deps.lookPath, mountPoint)
	if err != nil {
		return err
	}
	runErr := deps.run(argv)
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
		if runErr == nil {
			if err := runPackAfterUnmountIfRequested(deps, packState, packArchiveArgs, packPaths); err != nil {
				return err
			}
		}
		return runErr
	}

	state, path, stateOK, err := readUnmountProcessState(deps, stateMountPoint)
	if err != nil {
		if runErr != nil {
			return runErr
		}
		return err
	}
	if stateOK && state.MountKind == mountstate.MountKindWebDAV {
		if deps.terminateState != nil {
			err = deps.terminateState(state, *waitTimeout)
		} else if deps.terminate != nil {
			err = deps.terminate(state.PID, *waitTimeout)
		} else {
			err = fmt.Errorf("drive9 umount: no process terminator configured for WebDAV mount")
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
		if runErr != nil {
			return runErr
		}
		return runPackAfterUnmountIfRequested(deps, packState, packArchiveArgs, packPaths)
	}
	if runErr != nil {
		return runErr
	}
	if *waitTimeout == 0 {
		return runPackAfterUnmountIfRequested(deps, packState, packArchiveArgs, packPaths)
	}
	if !stateOK {
		return runPackAfterUnmountIfRequested(deps, packState, packArchiveArgs, packPaths)
	}
	if err := waitForPIDExit(state.PID, *waitTimeout, deps); err != nil {
		return fmt.Errorf("%w (pid file: %s)", err, path)
	}
	return runPackAfterUnmountIfRequested(deps, packState, packArchiveArgs, packPaths)
}

func readUnmountProcessState(deps umountDeps, mountPoint string) (mountstate.ProcessState, string, bool, error) {
	if deps.readProcessState != nil {
		state, path, err := deps.readProcessState(mountPoint)
		if err == nil {
			return state, path, true, nil
		}
		if errors.Is(err, os.ErrNotExist) {
			return mountstate.ProcessState{}, path, false, nil
		}
		return mountstate.ProcessState{}, path, false, err
	}
	pid, path, err := deps.readPID(mountPoint)
	if err == nil {
		return mountstate.ProcessState{PID: pid}, path, true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return mountstate.ProcessState{}, path, false, nil
	}
	return mountstate.ProcessState{}, path, false, err
}

func validateUmountPackState(state mountstate.ProcessState) error {
	if strings.TrimSpace(state.LocalRoot) == "" {
		return fmt.Errorf("drive9 umount: --pack requires mount metadata with local_root")
	}
	if !filepath.IsAbs(state.LocalRoot) {
		return fmt.Errorf("drive9 umount: mount metadata local_root must be absolute, got %q", state.LocalRoot)
	}
	if strings.TrimSpace(state.RemoteRoot) == "" {
		return fmt.Errorf("drive9 umount: --pack requires mount metadata with remote_root")
	}
	return nil
}

func runPackAfterUnmountIfRequested(deps umountDeps, state mountstate.ProcessState, archives []string, paths []string) error {
	if len(archives) == 0 {
		return nil
	}
	if deps.packAfterUnmount == nil {
		return fmt.Errorf("drive9 umount: --pack is not available")
	}
	return deps.packAfterUnmount(context.Background(), state, append([]string(nil), archives...), append([]string(nil), paths...))
}

func prependPackArchiveArg(existing []string, archive string) []string {
	out := []string{archive}
	for _, value := range existing {
		if value == archive {
			continue
		}
		out = append(out, value)
	}
	return out
}

func defaultPackAfterUnmount(ctx context.Context, state mountstate.ProcessState, archives []string, paths []string) error {
	c, err := packClientFromMountState(state)
	if err != nil {
		return err
	}
	warmCtx, cancel := context.WithTimeout(ctx, fsClientWarmTimeout)
	c.Warm(warmCtx)
	cancel()
	opts := packOptions{
		LocalRoot:        state.LocalRoot,
		RemoteRoot:       state.RemoteRoot,
		Profile:          state.Profile,
		Paths:            append([]string(nil), paths...),
		ProfilePackPaths: append([]string(nil), state.PackPaths...),
	}
	for _, archiveArg := range archives {
		archiveClient, archivePath, err := clientForRemoteArchiveArg(c, archiveArg)
		if err != nil {
			return err
		}
		if err := packRemoteArchive(ctx, archiveClient, archivePath, opts); err != nil {
			return fmt.Errorf("drive9 umount: pack %s: %w", archiveArg, err)
		}
	}
	return nil
}

type mountPackAuth struct {
	Server string
	APIKey string
	Token  string
}

func packClientFromMountState(state mountstate.ProcessState) (*client.Client, error) {
	auth, err := packAuthFromMountState(state)
	if err != nil {
		return nil, err
	}
	if auth.Token != "" {
		return client.NewWithToken(auth.Server, auth.Token), nil
	}
	return client.New(auth.Server, auth.APIKey), nil
}

func packAuthFromMountState(state mountstate.ProcessState) (mountPackAuth, error) {
	server := strings.TrimSpace(state.Server)
	kind := strings.TrimSpace(state.CredentialKind)
	apiKey := strings.TrimSpace(state.APIKey)
	token := strings.TrimSpace(state.Token)
	switch kind {
	case mountstate.CredentialKindAPIKey:
		if apiKey == "" {
			return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing API key")
		}
		if server == "" {
			return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing server")
		}
		return mountPackAuth{Server: server, APIKey: apiKey}, nil
	case mountstate.CredentialKindToken:
		if token == "" {
			return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing delegated token")
		}
		if server == "" {
			return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing server")
		}
		return mountPackAuth{Server: server, Token: token}, nil
	case "":
		if apiKey != "" || token != "" {
			if server == "" {
				return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing server")
			}
			return mountPackAuth{Server: server, APIKey: apiKey, Token: token}, nil
		}
	default:
		return mountPackAuth{}, fmt.Errorf("drive9 umount: unsupported mount credential kind %q", kind)
	}

	// Compatibility for pid files written before mount credential snapshots.
	r := ResolveCredentials()
	if server == "" {
		server = r.Server
	}
	switch r.Kind {
	case CredentialOwner, CredentialFSScoped:
		apiKey = r.APIKey
	case CredentialDelegated:
		token = r.Token
	}
	if server == "" {
		return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing server")
	}
	if apiKey == "" && token == "" {
		return mountPackAuth{}, fmt.Errorf("drive9 umount: mount metadata is missing credentials")
	}
	return mountPackAuth{Server: server, APIKey: apiKey, Token: token}, nil
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
