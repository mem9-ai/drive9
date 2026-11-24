//go:build !windows

package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type mountView struct {
	switcher     *mountViewSwitch
	ready        atomic.Bool
	server       atomic.Pointer[gofuse.Server]
	sharedExtent *extent.Engine
	memoryBudget *membudget.Budget
	remoteRoot   string
}

func resolveCanonicalStateMountPoint(raw string) string {
	mp := raw
	if abs, err := filepath.Abs(mp); err == nil {
		mp = abs
	}
	if eval, err := filepath.EvalSymlinks(mp); err == nil {
		mp = eval
	}
	return mp
}

func prepareMountView(ctx context.Context, cfg MountConfig, baseClient *client.Client, actorID string) (_ *mountView, retErr error) {
	if err := mountstate.EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("fuse: ensure mount state dir: %w", err)
	}
	timing := defaultLayerLeaseTiming()
	layerRef := strings.TrimSpace(cfg.LayerRef)
	var targetInfo *client.LayerInfo
	var baseRoot string
	var remoteRoot string
	if layerRef == "" {
		rootInput := cfg.RemoteRoot
		if strings.TrimSpace(rootInput) == "" {
			rootInput = "/"
		}
		var rerr error
		remoteRoot, rerr = mountpath.NormalizeRoot(rootInput)
		if rerr != nil {
			return nil, fmt.Errorf("fuse: normalize remote root: %w", rerr)
		}
	} else {
		info, gerr := baseClient.GetLayer(ctx, layerRef)
		if gerr != nil {
			return nil, ClassifyLayerStartupError(fmt.Sprintf("resolve layer %q", layerRef), gerr)
		}
		if berr := viewprotocol.ValidateLayerID(info.LayerID); berr != nil {
			return nil, ExitStartupPermanentErr("invalid layer identity returned by server", berr)
		}
		var berr error
		baseRoot, remoteRoot, berr = mountpath.LayerRoots(info.BaseRootPath, cfg.RemoteRoot, cfg.RemoteRootDefaulted)
		if berr != nil {
			return nil, ExitStartupPermanentErr(fmt.Sprintf("resolve root for layer %s: %v", info.LayerID, berr), berr)
		}
		targetInfo = info
	}

	stateMP := resolveCanonicalStateMountPoint(cfg.MountPoint)
	credScope := mountCredentialKind(&cfg) + ":" + sha256HexString(mountCredentialSecret(&cfg))
	scope, err := mountstate.SwitchScope(cfg.Server, credScope, remoteRoot, stateMP)
	if err != nil {
		return nil, fmt.Errorf("fuse: switch scope: %w", err)
	}
	jPath, err := mountstate.SwitchScopedJournalPath(scope, actorID)
	if err != nil {
		return nil, fmt.Errorf("fuse: switch journal path: %w", err)
	}
	journal, err := mountstate.OpenSwitchJournal(jPath, actorID, scope, viewprotocol.SwitchBinding{ReadOnly: cfg.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("fuse: open switch journal: %w", err)
	}
	skipFailCleanup := false
	var initialLease *layerMountBinding
	unbindFn := func(uctx context.Context, item mountstate.PendingUnbind) error {
		cctx, cancel := context.WithTimeout(uctx, timing.callTimeout)
		defer cancel()
		_, uerr := baseClient.UnbindLayerMount(cctx, item.LayerID, client.LayerMountUnbindRequest{
			MountID: item.MountID, Kind: item.Kind, OperationID: item.OperationID, Fence: item.Fence,
		})
		if client.IsNotFound(uerr) {
			return nil
		}
		return uerr
	}
	defer func() {
		if retErr == nil {
			return
		}
		if !skipFailCleanup {
			if initialLease != nil {
				initialLease.StopRenew()
			}
			if !journal.Uncertain() {
				if qerr := journal.QueueCurrentUnbindForUnmount(); qerr != nil {
					retErr = errors.Join(retErr, qerr)
				} else if derr := journal.DrainPendingUnbinds(context.Background(), unbindFn); derr != nil {
					retErr = errors.Join(retErr, derr)
				}
			} else if initialLease != nil {
				initialLease.Unbind(context.Background())
			}
			snap := journal.Snapshot()
			if !journal.Uncertain() && snap.Pending == nil && len(snap.PendingUnbinds) == 0 && (snap.Binding.LayerID == "" || snap.Binding.ReadOnly) {
				_ = mountstate.RemoveSwitchJournal(jPath)
			}
		}
		if cerr := journal.Close(); cerr != nil {
			retErr = errors.Join(retErr, cerr)
		}
	}()

	var postPublishCrashErr error
	failpoint.InjectCall("prepareMountViewAfterJournalOpen", &postPublishCrashErr)
	if postPublishCrashErr != nil {
		skipFailCleanup = true
		return nil, postPublishCrashErr
	}

	if derr := journal.DrainPendingUnbinds(ctx, unbindFn); derr != nil {
		return nil, ClassifyLayerStartupError("drain pending layer unbinds", derr)
	}
	drainedPrior, perr := mountstate.DrainPriorScopeJournals(ctx, jPath, scope, unbindFn)
	if perr != nil {
		return nil, ClassifyLayerStartupError("drain prior mount switch journal", perr)
	}
	if targetInfo != nil && drainedPrior {
		refreshed, gerr := baseClient.GetLayer(ctx, targetInfo.LayerID)
		if gerr != nil {
			return nil, ClassifyLayerStartupError(fmt.Sprintf("refresh layer %q after draining prior journal", targetInfo.LayerID), gerr)
		}
		targetInfo = refreshed
	}

	viewClient := baseClient
	initialBinding := viewprotocol.SwitchBinding{ReadOnly: cfg.ReadOnly}
	if targetInfo != nil {
		initialBinding.LayerID = targetInfo.LayerID
		initialBinding.BaseRootPath = baseRoot
		if cfg.ReadOnly {
			vc, verr := baseClient.WithView(client.ViewBinding{LayerID: targetInfo.LayerID, Fence: 0})
			if verr != nil {
				return nil, ExitStartupPermanentErr(fmt.Sprintf("bind read-only layer view %s", targetInfo.LayerID), verr)
			}
			viewClient = vc
			initialLease = &layerMountBinding{
				BaseClient: baseClient, ViewClient: vc, LayerID: targetInfo.LayerID,
				BaseRootPath: baseRoot, RemoteRoot: remoteRoot, MountID: actorID, ReadOnly: true,
			}
			if berr := journal.BindInitial(initialBinding, ""); berr != nil {
				return nil, fmt.Errorf("fuse: journal bind initial read-only layer: %w", berr)
			}
		} else {
			if targetInfo.Fence%2 == 1 && !targetInfo.MountExpired {
				return nil, newMountExit(ExitLayerLeaseBusy, ExitReasonLayerLeaseBusy,
					fmt.Sprintf("layer %s already has an active exclusive mount lease", targetInfo.LayerID), client.ErrConflict)
			}
			expectedFence := targetInfo.Fence
			if expectedFence%2 == 1 && targetInfo.MountExpired {
				unb, uerr := baseClient.UnbindLayerMount(ctx, targetInfo.LayerID, client.LayerMountUnbindRequest{
					MountID: actorID, Kind: mountstate.UnbindKindExpire,
				})
				if uerr != nil {
					return nil, ClassifyLayerStartupError(fmt.Sprintf("expire stale mount lease on layer %s", targetInfo.LayerID), uerr)
				}
				if unb == nil || unb.Fence < 0 || unb.Fence%2 != 0 {
					return nil, ExitStartupTransientErr(
						fmt.Sprintf("expire stale mount lease on layer %s returned invalid fence", targetInfo.LayerID), nil)
				}
				expectedFence = unb.Fence
			}
			if expectedFence < 0 || expectedFence%2 != 0 {
				return nil, ExitStartupPermanentErr(
					fmt.Sprintf("layer %s reported invalid fence %d", targetInfo.LayerID, expectedFence), nil)
			}
			opID := "bind_" + generateMountID()
			if ierr := journal.AppendBindIntent(mountstate.PendingUnbind{
				LayerID: targetInfo.LayerID, MountID: actorID, Kind: mountstate.UnbindKindOp, OperationID: opID, Fence: expectedFence,
			}); ierr != nil {
				return nil, fmt.Errorf("fuse: persist initial bind_intent: %w", ierr)
			}
			var preBindCrashErr error
			failpoint.InjectCall("prepareMountViewBeforeInitialServerBind", &preBindCrashErr)
			if preBindCrashErr != nil {
				skipFailCleanup = true
				return nil, preBindCrashErr
			}
			bindStart := time.Now()
			leaseResp, berr := baseClient.BindLayerMount(ctx, targetInfo.LayerID, client.LayerMountBindRequest{
				MountID: actorID, OperationID: opID, ExpectedFence: expectedFence,
			})
			if berr != nil {
				return nil, ClassifyLayerStartupError(fmt.Sprintf("bind exclusive mount lease on layer %s", targetInfo.LayerID), berr)
			}
			var crashErr error
			failpoint.InjectCall("prepareMountViewAfterInitialServerBind", &crashErr)
			if crashErr != nil {
				skipFailCleanup = true
				return nil, crashErr
			}
			initialBinding.Fence = leaseResp.Fence
			initialLease = &layerMountBinding{
				BaseClient: baseClient, LayerID: targetInfo.LayerID, BaseRootPath: baseRoot,
				RemoteRoot: remoteRoot, MountID: actorID, OperationID: opID, Fence: leaseResp.Fence,
			}
			vc, verr := baseClient.WithView(client.ViewBinding{
				LayerID: targetInfo.LayerID, Fence: leaseResp.Fence, WriteGuard: initialLease.checkWriteAllowed, ReadUnfenced: initialLease.fenced.Load,
			})
			if verr != nil {
				return nil, ExitStartupPermanentErr(fmt.Sprintf("bind writable layer view %s", targetInfo.LayerID), verr)
			}
			if jerr := journal.BindInitial(initialBinding, opID); jerr != nil {
				return nil, fmt.Errorf("fuse: commit initial layer binding: %w", jerr)
			}
			viewClient = vc
			initialLease.ViewClient = vc
			initialLease.startRenewLoop(bindStart, leaseResp.MountLeaseTTLMS, timing)
		}
	}

	if err := validateRemoteRoot(ctx, viewClient, remoteRoot); err != nil {
		return nil, err
	}
	memoryBudget, err := buildMountMemoryBudget(cfg)
	if err != nil {
		return nil, err
	}
	sharedCounters := perf.NewCounters(cfg.PerfCounters || cfg.PerfSamplesPath != "")
	var sharedExtent *extent.Engine
	gen1 := mountRuntimeGeneration{MountID: actorID, Generation: 1}
	rt, err := buildViewRuntime(cfg, viewClient, remoteRoot, actorID, initialBinding, initialLease, gen1, memoryBudget, sharedCounters, &sharedExtent)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "drive9: sync mode: %s\n", rt.syncMode)

	adapter := &viewRequestFS{inner: rt.fs}
	adapter.selected.Store(&viewRequestTarget{fs: rt.fs, generation: 1})
	mv := &mountView{
		sharedExtent: sharedExtent,
		memoryBudget: memoryBudget,
		remoteRoot:   remoteRoot,
	}
	sw := &mountViewSwitch{
		goos:        runtime.GOOS,
		cfg:         cfg,
		baseClient:  baseClient,
		remoteRoot:  remoteRoot,
		actorID:     actorID,
		journalPath: jPath,
		journal:     journal,
		adapter:     adapter,
		current:     rt,
		notifier:    mv,
		fence:       func(fctx context.Context) error { return fenceViewRootUpdates(fctx, mv) },
		timing:      timing,
	}
	sw.buildRuntime = func(bctx context.Context, target viewprotocol.SwitchBinding, lease *layerMountBinding, gen mountRuntimeGeneration) (*mountRuntime, error) {
		if err := bctx.Err(); err != nil {
			return nil, err
		}
		srv := mv.server.Load()
		if !mv.ready.Load() || srv == nil {
			return nil, errors.New("mount connection ended")
		}
		vc := baseClient
		if target.LayerID != "" && lease != nil && lease.ViewClient != nil {
			vc = lease.ViewClient
		}
		next, berr := buildViewRuntime(cfg, vc, remoteRoot, actorID, target, lease, gen, memoryBudget, sharedCounters, &mv.sharedExtent)
		if berr != nil {
			return nil, berr
		}
		next.fs.mu.Lock()
		next.fs.server = srv
		next.fs.mu.Unlock()
		return next, nil
	}
	mv.switcher = sw
	return mv, nil
}

func sha256HexString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (v *mountView) InodeNotify(node uint64, off, length int64) gofuse.Status {
	if srv := v.server.Load(); srv != nil {
		return srv.InodeNotify(node, off, length)
	}
	return gofuse.ENODEV
}

func (v *mountView) EntryNotify(parent uint64, name string) gofuse.Status {
	if srv := v.server.Load(); srv != nil {
		return srv.EntryNotify(parent, name)
	}
	return gofuse.ENODEV
}

func (v *mountView) drainShutdownResponse(ctx context.Context) mountcontrol.DrainResponse {
	v.ready.Store(false)
	v.switcher.mu.Lock()
	defer v.switcher.mu.Unlock()

	mntPt := ""
	if v.switcher.current != nil && v.switcher.current.fs != nil {
		mntPt = v.switcher.current.fs.mountPt
		v.switcher.current.fs.shutdownRequested.Store(true)
	}
	if v.switcher.journal.Uncertain() {
		resp := mountcontrol.NewDrainResponse(mntPt, time.Now().UTC())
		resp.Fail("RECOVERING", mntPt, mountstate.ErrSwitchUncertain)
		resp.Finish(time.Now().UTC())
		return resp
	}

	snap := v.switcher.journal.Snapshot()
	if snap.Pending != nil {
		// Prior to Decide, the outgoing runtime was 100% drained and ordinary
		// FUSE admission stayed closed with zero new writes (v36 §6.3 #4).
		if v.switcher.pendingLease != nil {
			v.switcher.pendingLease.StopRenew()
			v.switcher.pendingLease = nil
		}
		if v.switcher.current != nil && v.switcher.current.layerLease != nil {
			v.switcher.current.layerLease.StopRenew()
		}
		_ = v.switcher.journal.AbortPendingForUnmount()
		_ = v.switcher.drainPendingUnbindsLocked(ctx)
		resp := mountcontrol.NewDrainResponse(mntPt, time.Now().UTC())
		resp.Finish(time.Now().UTC())
		return resp
	}
	if v.switcher.recoveringPaused {
		if v.switcher.current != nil && v.switcher.current.layerLease != nil {
			v.switcher.current.layerLease.StopRenew()
		}
		_ = v.switcher.drainPendingUnbindsLocked(ctx)
		resp := mountcontrol.NewDrainResponse(mntPt, time.Now().UTC())
		resp.Finish(time.Now().UTC())
		return resp
	}

	pause, perr := v.switcher.adapter.admission.pause()
	if perr != nil && !errors.Is(perr, errViewAdmissionPaused) {
		resp := mountcontrol.NewDrainResponse(mntPt, time.Now().UTC())
		resp.Fail(classifyDrainErr(perr), mntPt, perr)
		v.switcher.current.fs.snapshotDrainPending(&resp)
		resp.Finish(time.Now().UTC())
		return resp
	}
	if pause != nil {
		if werr := pause.wait(ctx); werr != nil {
			resp := mountcontrol.NewDrainResponse(mntPt, time.Now().UTC())
			resp.Fail(classifyDrainErr(werr), mntPt, werr)
			v.switcher.current.fs.snapshotDrainPending(&resp)
			resp.Finish(time.Now().UTC())
			return resp
		}
	}
	resp := v.switcher.current.fs.Drain(ctx)
	if v.switcher.current != nil && v.switcher.current.layerLease != nil {
		v.switcher.current.layerLease.StopRenew()
	}
	_ = v.switcher.journal.QueueCurrentUnbindForUnmount()
	_ = v.switcher.drainPendingUnbindsLocked(ctx)
	return resp
}

func (v *mountView) close(ctx context.Context) error {
	if v == nil || v.switcher == nil {
		return nil
	}
	v.ready.Store(false)
	v.switcher.mu.Lock()
	defer v.switcher.mu.Unlock()
	if v.switcher.pendingLease != nil {
		v.switcher.pendingLease.StopRenew()
		v.switcher.pendingLease = nil
	}
	var err error
	if v.switcher.current != nil {
		err = v.switcher.current.stop(ctx)
	}
	if v.switcher.pendingTarget != nil && v.switcher.pendingTarget != v.switcher.current {
		err = errors.Join(err, v.switcher.pendingTarget.stop(ctx))
	}
	if v.switcher.journal != nil {
		if !v.switcher.journal.Uncertain() {
			_ = v.switcher.journal.QueueCurrentUnbindForUnmount()
			_ = v.switcher.drainPendingUnbindsLocked(ctx)
			snap := v.switcher.journal.Snapshot()
			if !v.switcher.journal.Uncertain() && snap.Pending == nil && len(snap.PendingUnbinds) == 0 && (snap.Binding.LayerID == "" || snap.Binding.ReadOnly) && v.switcher.journalPath != "" {
				_ = mountstate.RemoveSwitchJournal(v.switcher.journalPath)
			}
		}
		err = errors.Join(err, v.switcher.journal.Close())
	}
	return err
}
