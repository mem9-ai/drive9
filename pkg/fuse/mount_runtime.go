package fuse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/appendlog"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/extensions/git"
	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/extensions/renameguard"
	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/extensions/sse"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/extensions/xattr"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

type mountRuntime struct {
	binding    mountstate.SwitchBinding
	layerLease *layerMountBinding
	fs         *FS
	writeback  *writeback.Writeback
	staging    *staging.Staging
	syncMode   string
	policy     writeback.Policy
	stopped    bool
}

func (r *mountRuntime) closeStaging() {
	if r != nil && r.staging != nil {
		r.staging.Close()
		r.staging = nil
	}
}

func (r *mountRuntime) stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.layerLease != nil {
		r.layerLease.StopRenew()
	}
	if r.stopped {
		return nil
	}
	if r.fs != nil {
		if err := r.fs.chain.Stop(ctx); err != nil {
			return err
		}
		if err := r.fs.notifications.wait(ctx); err != nil {
			return err
		}
		r.fs.inodes.ReleaseMemoryBudget()
	}
	r.closeStaging()
	r.stopped = true
	return nil
}

func buildMountMemoryBudget(cfg MountConfig) (*membudget.Budget, error) {
	const memoryBudgetFloor = 16 << 20
	budgetLimit := cfg.MemoryBudget
	budgetSource := "configured"
	budgetAuto := false
	switch {
	case budgetLimit < 0:
		budgetLimit = 0
		budgetSource = "disabled with -1"
	case budgetLimit == 0:
		budgetLimit, budgetSource = membudget.AutoLimit()
		budgetAuto = budgetLimit > 0
		if budgetAuto {
			budgetSource = "auto: " + budgetSource
		}
	}
	if budgetLimit > 0 {
		mode, merr := membudget.ParseMode(strings.TrimSpace(cfg.MemoryBudgetMode))
		if merr != nil {
			return nil, fmt.Errorf("fuse: %w", merr)
		}
		if budgetAuto && budgetLimit < memoryBudgetFloor {
			mode = membudget.ModeWarn
			fmt.Fprintf(os.Stderr, "drive9: memory budget: auto limit %d MiB is below the %d MiB floor; accounting only (warn mode)\n",
				budgetLimit>>20, memoryBudgetFloor>>20)
		}
		mb := membudget.New(budgetLimit, mode, logger.L())
		fmt.Fprintf(os.Stderr, "drive9: memory budget limit=%d MiB (%s, mode=%s)\n", budgetLimit>>20, budgetSource, mode)
		return mb, nil
	}
	if budgetSource == "disabled with -1" {
		fmt.Fprintf(os.Stderr, "drive9: memory budget disabled by --memory-budget-mb -1\n")
	} else {
		fmt.Fprintf(os.Stderr, "drive9: memory budget disabled (auto: no cgroup memory limit found; set --memory-budget-mb to bound RSS)\n")
	}
	return nil, nil
}
func buildViewRuntime(
	cfg MountConfig,
	cl *client.Client,
	remoteRoot, actorID string,
	binding mountstate.SwitchBinding,
	layerLease *layerMountBinding,
	memoryBudget *membudget.Budget,
	sharedCounters *perf.Counters,
	sharedExtent **extent.Engine,
) (_ *mountRuntime, retErr error) {
	extOrder, err := parseExtensionOrder(cfg.Extensions)
	if err != nil {
		return nil, err
	}
	if sharedCounters == nil {
		sharedCounters = perf.NewCounters(cfg.PerfCounters || cfg.PerfSamplesPath != "")
	}
	wantExt := make(map[string]bool, len(extOrder))
	for _, name := range extOrder {
		wantExt[name] = true
	}
	localCfg := cfg
	localCfg.RemoteRoot = remoteRoot
	applyExtentCapabilityGate(&localCfg, cl, wantExt)
	built := map[string]fs.Handler{}

	cacheBase := localCfg.CacheDir
	if cacheBase == "" {
		if home, herr := os.UserHomeDir(); herr == nil {
			cacheBase = filepath.Join(home, ".cache", "drive9")
		}
	}
	mountHash, readCacheHash := "", ""
	if cacheBase != "" {
		mountHash, readCacheHash, err = mountRuntimeCacheHashes(localCfg, remoteRoot, binding)
		if err != nil {
			return nil, err
		}
	}
	viewLocalRoot, err := mountstate.ViewLocalRoot(localCfg.LocalRoot, binding)
	if err != nil {
		return nil, err
	}

	var coreExt *core.Core
	if wantExt["core"] {
		coreExt, err = core.New(cl, core.Options{
			RemoteRoot:                   remoteRoot,
			StatRetryCount:               localCfg.LookupRetryCount,
			StatRetryTimeout:             localCfg.LookupRetryTimeout,
			ReadConcurrency:              localCfg.ReadConcurrency,
			LegacyDirStatFallback:        localCfg.LegacyDirStatFallback,
			TrustLocalEvents:             localCfg.TrustLocalEvents,
			LegacyInterruptibleMutations: localCfg.LegacyInterruptibleMutations && !localCfg.GVisorCompat,
		})
		if err != nil {
			return nil, fmt.Errorf("fuse: core terminal: %w", err)
		}
		built["core"] = coreExt
	}
	bus := fs.NewEventBus()
	if coreExt != nil {
		coreExt.AttachEvents(bus)
	}

	var readCache *cache.Cache
	if wantExt["cache"] {
		rc, cerr := cache.New(cache.Options{
			Events:                  bus,
			MaxSize:                 localCfg.CacheSize,
			TTL:                     localCfg.ReadCacheTTL,
			MaxFileSize:             localCfg.ReadCacheMaxFileBytes,
			MemoryBudget:            memoryBudget,
			SequentialPrefetch:      true,
			PrefetchClient:          cl,
			RemoteRoot:              remoteRoot,
			ParallelReadConcurrency: localCfg.ParallelReadConcurrency,
			ParallelReadBlockSize:   localCfg.ParallelReadBlockSize,
			DirContentPrefetch:      localCfg.ReadDirPrefetch,
			PrefetchMaxFiles:        localCfg.PrefetchMaxFiles,
			PrefetchMaxFileBytes:    localCfg.PrefetchMaxFileBytes,
			PrefetchMaxBytes:        localCfg.PrefetchMaxBytes,
			PrefetchTimeout:         localCfg.PrefetchTimeout,
			Disk: func() cache.DiskReadCacheOptions {
				dir := ""
				if localCfg.DiskReadCacheSize > 0 {
					dir = cacheScopedDir(cacheBase, readCacheHash, "read")
				}
				return diskCacheOpts(dir, localCfg.DiskReadCacheSize, localCfg.DiskReadCacheFreeRatio)
			}(),
		})
		if cerr != nil {
			return nil, fmt.Errorf("fuse: cache extension: %w", cerr)
		}
		readCache = rc
		built["cache"] = readCache
	}

	var policy writeback.Policy
	switch localCfg.WritePolicy {
	case "write-sync":
		policy = writeback.PolicyWriteSync
	case "writeback", "":
		policy = writeback.PolicyWriteBack
	case "close-sync":
		policy = writeback.PolicyCloseSync
	default:
		return nil, fmt.Errorf("fuse: unknown write policy %q", localCfg.WritePolicy)
	}

	writeCacheDir := cacheScopedDir(cacheBase, mountHash, "writecache")
	var stagingLedger *staging.Staging
	defer func() {
		if retErr != nil && stagingLedger != nil {
			stagingLedger.Close()
		}
	}()
	if !localCfg.ReadOnly && (policy == writeback.PolicyWriteBack || policy == writeback.PolicyCloseSync) && cacheBase != "" && wantExt["writeback"] {
		freeRatio := localCfg.WriteCacheFreeRatio
		if freeRatio == 0 {
			freeRatio = 0.10
		} else if freeRatio < 0 {
			freeRatio = 0
		}
		stagingLedger, err = staging.NewStagingWithQuota(filepath.Join(cacheBase, mountHash), freeRatio, resolveWriteCacheSizeMB(localCfg.WriteCacheSizeMB))
		if err != nil {
			return nil, fmt.Errorf("fuse: staging ledger: %w", err)
		}
		if err := stagingLedger.Recover(); err != nil {
			return nil, fmt.Errorf("fuse: staging recovery: %w", err)
		}
		_ = stagingLedger.Compact()
	}

	syncMode := strings.TrimSpace(localCfg.SyncMode)
	switch syncMode {
	case "", "auto":
		syncMode = resolveSyncModeByRTT(localCfg.Server)
	case "strict", "interactive":
	default:
		return nil, fmt.Errorf("fuse: unknown sync mode %q", localCfg.SyncMode)
	}

	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if localCfg.DefaultOwner != nil {
		uid, gid = localCfg.DefaultOwner.UID, localCfg.DefaultOwner.GID
	}

	var renameGuard *renameguard.RenameGuard
	if wantExt["renameguard"] {
		renameGuard, err = renameguard.New(renameguard.Options{Uid: uid, Gid: gid})
		if err != nil {
			return nil, fmt.Errorf("fuse: renameguard extension: %w", err)
		}
		built["renameguard"] = renameGuard
	}

	var gitExt *git.Git
	if wantExt["git"] && localCfg.EnableGitWorkspaces && binding.LayerID == "" {
		gitExt, err = git.New(git.Options{
			Client:             cl,
			Events:             bus,
			LocalRoot:          viewLocalRoot,
			RemoteRoot:         remoteRoot,
			SyncPolicy:         string(localCfg.WritePolicy),
			OnWorkspaceRefresh: sharedCounters.RecordGitWorkspaceRefresh,
		})
		if err != nil {
			return nil, fmt.Errorf("fuse: git extension: %w", err)
		}
		built["git"] = gitExt
	}

	if wantExt["localize"] && viewLocalRoot != "" {
		localizeOpts := localize.Options{
			LocalRoot:                   viewLocalRoot,
			QuotaBytes:                  localCfg.LocalizeQuotaBytes,
			FreeRatio:                   localCfg.LocalizeFreeRatio,
			LocalOnlyPatterns:           localCfg.LocalOnlyPatterns,
			LocalGitignoreAwarePatterns: localCfg.LocalGitignoreAwarePatterns,
			RemoteOnlyPatterns:          localCfg.RemoteOnlyPatterns,
			Profile:                     localCfg.Profile,
			ApplyBuiltinDefaults:        !localCfg.DisableBuiltinOverlayDefaults,
		}
		if gitExt != nil {
			localizeOpts.DeclarativeLocal = gitExt.RoutesLocalCtx
		}
		if cacheBase != "" && !localCfg.ReadOnly {
			transientRoot := transientOverlayRoot(cacheBase, readCacheHash)
			if mkErr := os.MkdirAll(transientRoot, 0o700); mkErr != nil {
				fmt.Fprintf(os.Stderr, "drive9: transient local overlay init failed: %v (continuing without)\n", mkErr)
			} else {
				localizeOpts.TransientRoot = transientRoot
			}
		}
		localizeExt, lerr := localize.New(localizeOpts)
		if lerr != nil {
			return nil, fmt.Errorf("fuse: localize extension: %w", lerr)
		}
		built["localize"] = localizeExt
	}

	var extentPlane *extent.Handler
	if wantExt["extent"] {
		var engine *extent.Engine
		if sharedExtent != nil && *sharedExtent != nil {
			engine = *sharedExtent
			engine.ResetForGeneration(cl)
		} else {
			baseExtentHash := MountHash(localCfg.Server, localCfg.MountPoint, remoteRoot)
			engineConf := extent.EngineConfig{
				Client:         cl,
				RemoteRoot:     remoteRoot,
				CacheDir:       cacheScopedDir(cacheBase, baseExtentHash, "extent"),
				ReadOnly:       localCfg.ReadOnly,
				FreeSpaceRatio: float32(localCfg.DiskReadCacheFreeRatio),
				Writeback:      false,
			}
			if localCfg.ExtentCacheSize >= 0 {
				engineConf.CacheSize = uint64(localCfg.ExtentCacheSize)
			}
			engine = extent.NewEngine(engineConf)
			if sharedExtent != nil {
				*sharedExtent = engine
			}
		}
		extentPlane = extent.New(extent.Options{
			Engine:    engine,
			Threshold: 1,
			Patterns:  localCfg.ExtentPaths,
			Events:    bus,
			WriteSync: localCfg.WritePolicy == string(WritePolicyWriteSync),
		})
		built["extent"] = extentPlane
		if syncMode != "strict" {
			logger.Warn(context.Background(), "extent_durability_override",
				zap.String("durability_profile", localCfg.Profile),
				zap.String("write_policy", string(localCfg.WritePolicy)),
				zap.String("sync_mode", syncMode),
				zap.Strings("extent_patterns", localCfg.ExtentPaths),
				zap.String("detail", "extent-served paths have no local-only fsync staging; fsync publishes remotely"))
		}
	}

	if wantExt["appendlog"] && len(localCfg.AppendLogPatterns) > 0 && binding.LayerID == "" {
		appendExt, aerr := appendlog.New(appendlog.Options{
			Patterns:   localCfg.AppendLogPatterns,
			Client:     cl,
			RemoteRoot: remoteRoot,
			Events:     bus,
			TempDir:    appendLogSnapshotDir(cacheBase, mountHash),
			SyncPolicy: string(localCfg.WritePolicy),
		})
		if aerr != nil {
			return nil, fmt.Errorf("fuse: appendlog extension: %w", aerr)
		}
		built["appendlog"] = appendExt
	}

	if wantExt["specials"] {
		specialsExt, sperr := specials.New(specials.Options{Events: bus})
		if sperr != nil {
			return nil, fmt.Errorf("fuse: specials extension: %w", sperr)
		}
		built["specials"] = specialsExt
	}
	if wantExt["xattr"] {
		xattrExt, xerr := xattr.New(xattr.Options{Events: bus})
		if xerr != nil {
			return nil, fmt.Errorf("fuse: xattr extension: %w", xerr)
		}
		built["xattr"] = xattrExt
	}

	var wb *writeback.Writeback
	if wantExt["writeback"] && !localCfg.ReadOnly {
		flushDebounce := localCfg.FlushDebounce
		if localCfg.Profile == "interactive" {
			flushDebounce = 0
		}
		wb, err = writeback.New(writeback.Options{
			Policy:                  policy,
			SyncMode:                syncMode,
			WriteCacheDir:           writeCacheDir,
			WriteCacheSizeMB:        resolveWriteCacheSizeMB(localCfg.WriteCacheSizeMB),
			WriteCacheFreeRatio:     localCfg.WriteCacheFreeRatio,
			Client:                  cl,
			RemoteRoot:              remoteRoot,
			Events:                  bus,
			Staging:                 stagingLedger,
			FlushDebounce:           flushDebounce,
			UploadConcurrency:       localCfg.UploadConcurrency,
			CommitQueueMaxPending:   localCfg.CommitQueueMaxPending,
			BatchWindow:             localCfg.WriteBackBatchWindow,
			BatchMaxFiles:           localCfg.WriteBackBatchMaxFiles,
			BatchMaxBytes:           localCfg.WriteBackBatchMaxBytes,
			ExtentTakeover:          wantExt["extent"],
			ExtentMinSize:           extentHybridCutoff(localCfg.Profile, len(localCfg.ExtentPaths) > 0, cl),
			ExtentPatterns:          localCfg.ExtentPaths,
			ExtentThreshold:         int64(extentInlineThreshold(cl)),
			ExtentOpener:            extentPlane,
			InterruptibleCommits:    localCfg.LegacyInterruptibleMutations && !localCfg.GVisorCompat,
			RemoteCommitWaitTimeout: localCfg.RemoteCommitWaitTimeout,
			LazyStaging:             localCfg.WritebackLazyStaging,
			SyncWindow:              localCfg.WritebackSyncWindow,
		})
		if err != nil {
			return nil, fmt.Errorf("fuse: writeback extension: %w", err)
		}
		built["writeback"] = wb
	}
	if readCache != nil && wb != nil {
		wb.SetCommitSeeder(readCache.SeedCommit)
		wb.SetCommitSeedLimit(readCache.MaxFileSize())
		wb.SetPendingSeeder(readCache.SeedPending)
		wb.SetBaseReader(func(path string) ([]byte, bool) { return readCache.ReadCached(path, 0) })
	}
	var sseExt *sse.SSE
	if wantExt["sse"] && binding.LayerID == "" {
		sseExt, err = sse.New(sse.Options{Client: cl, Actor: actorID, RemoteRoot: remoteRoot, Events: bus})
		if err != nil {
			return nil, fmt.Errorf("fuse: sse extension: %w", err)
		}
		built["sse"] = sseExt
	}

	attrTTL, entryTTL, dirTTL := resolveKernelTTLDefaults(localCfg.Profile, localCfg.AttrTTL, localCfg.EntryTTL, localCfg.DirTTL)
	localDirAttrs := viewLocalRoot == "" && binding.LayerID == ""
	fuseOpts := newGoFuseMountOptions(localCfg)
	defaultPermissions := slices.Contains(fuseOpts.Options, "default_permissions")
	var chainHandlers []fs.Handler
	for i := len(extOrder) - 1; i >= 0; i-- {
		if h := built[extOrder[i]]; h != nil {
			chainHandlers = append(chainHandlers, h)
		}
	}
	f, err := New(Options{
		OwnerSet:           localCfg.DefaultOwner != nil,
		Extensions:         chainHandlers,
		Events:             bus,
		AttrTTL:            attrTTL,
		EntryTTL:           entryTTL,
		NegEntryTTL:        localCfg.NegativeEntryTTL,
		DirTTL:             dirTTL,
		DirCacheMaxEntries: localCfg.DirCacheMaxEntries,
		LocalDirAttrs:      localDirAttrs,
		DefaultPermissions: defaultPermissions,
		Perf:               localCfg.PerfCounters || localCfg.PerfSamplesPath != "",
		PerfCounters:       sharedCounters,
		Debug:              localCfg.Debug,
		GVisorCompat:       localCfg.GVisorCompat,
		Uid:                uid,
		Gid:                gid,
		MemoryBudget:       memoryBudget,
	})
	if err != nil {
		return nil, fmt.Errorf("fuse: assemble facade: %w", err)
	}
	if renameGuard != nil {
		renameGuard.SetOwnerLookup(f.OwnerOf)
	}
	if coreExt != nil {
		if dc, ok := f.Ctx.DirCache.(core.NamespaceCache); ok {
			coreExt.Attach(dc)
		}
	}
	f.mountPt, f.rootTag, f.srvTag = localCfg.MountPoint, remoteRoot, localCfg.Server
	f.viewLocalState = localCfg.LocalRoot != ""
	if sseExt != nil {
		sseExt.Attach(f.Ctx.Inodes, f.Ctx.Notify)
	}
	if layerLease != nil && layerLease.LayerID != "" {
		f.layerFence = layerLease
		if wb != nil {
			layerLease.OnSelfFence(wb.AbandonLayerForRollback)
		}
	}
	return &mountRuntime{
		binding:    binding,
		layerLease: layerLease,
		fs:         f,
		writeback:  wb,
		staging:    stagingLedger,
		syncMode:   syncMode,
		policy:     policy,
	}, nil
}
