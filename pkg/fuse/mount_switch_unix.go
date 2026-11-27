//go:build !windows

package fuse

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type mountViewSwitch struct {
	mu           sync.Mutex
	goos         string
	cfg          MountConfig
	baseClient   *client.Client
	remoteRoot   string
	actorID      string
	journalPath  string
	journal      *mountstate.SwitchJournal
	adapter      *viewRequestFS
	current      *mountRuntime
	notifier     viewCacheNotifier
	fence        func(context.Context) error
	timing       layerLeaseTiming
	buildRuntime func(context.Context, mountstate.SwitchBinding, *layerMountBinding) (*mountRuntime, error)
	onSwitchDone func(mountstate.SwitchBinding)

	pendingPause     *viewPause
	pendingPlan      viewCacheInvalidation
	pendingLease     *layerMountBinding
	pendingTarget    *mountRuntime
	pendingInstalled bool
	outgoingStopped  bool
	recoveringPaused atomic.Bool

	asyncUnbindMu      sync.Mutex
	asyncUnbindRunning bool
	asyncUnbindStopped bool
	asyncUnbindStop    chan struct{}
	asyncUnbindWg      sync.WaitGroup
}

func (s *mountViewSwitch) supported() bool {
	if s == nil {
		return false
	}
	goos := s.goos
	if goos == "" {
		goos = runtime.GOOS
	}
	return goos == "linux"
}

func (s *mountViewSwitch) currentRuntime() *mountRuntime {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

func (s *mountViewSwitch) executeUnbind(ctx context.Context, item mountstate.PendingUnbind) error {
	callCtx, cancel := context.WithTimeout(ctx, s.leaseCallTimeout())
	defer cancel()
	_, err := s.baseClient.UnbindLayerMount(callCtx, item.LayerID, client.LayerMountUnbindRequest{
		MountID:     item.MountID,
		Kind:        item.Kind,
		OperationID: item.OperationID,
		Fence:       item.Fence,
	})
	if client.IsNotFound(err) {
		return nil
	}
	return err
}

func (s *mountViewSwitch) leaseCallTimeout() time.Duration {
	if s.timing.callTimeout > 0 {
		return s.timing.callTimeout
	}
	return defaultLayerLeaseCallTimeout
}

func (s *mountViewSwitch) drainPendingUnbindsLocked(ctx context.Context) error {
	return s.journal.DrainPendingUnbinds(ctx, s.executeUnbind)
}

func (s *mountViewSwitch) stopAsyncUnbinds() {
	s.asyncUnbindMu.Lock()
	if !s.asyncUnbindStopped {
		s.asyncUnbindStopped = true
		if s.asyncUnbindStop == nil {
			s.asyncUnbindStop = make(chan struct{})
		}
		close(s.asyncUnbindStop)
	}
	s.asyncUnbindMu.Unlock()
	s.asyncUnbindWg.Wait()
}

func (s *mountViewSwitch) kickAsyncDrainUnbinds() {
	s.asyncUnbindMu.Lock()
	if s.asyncUnbindStopped || s.asyncUnbindRunning {
		s.asyncUnbindMu.Unlock()
		return
	}
	if s.asyncUnbindStop == nil {
		s.asyncUnbindStop = make(chan struct{})
	}
	stopCh := s.asyncUnbindStop
	s.asyncUnbindRunning = true
	s.asyncUnbindWg.Add(1)
	s.asyncUnbindMu.Unlock()

	go func() {
		defer s.asyncUnbindWg.Done()
		defer func() {
			s.asyncUnbindMu.Lock()
			s.asyncUnbindRunning = false
			s.asyncUnbindMu.Unlock()
		}()
		for attempt := 0; attempt < 5; attempt++ {
			select {
			case <-stopCh:
				return
			default:
			}
			s.mu.Lock()
			if s.journal == nil || s.journal.Uncertain() {
				s.mu.Unlock()
				return
			}
			items := s.journal.Snapshot().PendingUnbinds
			s.mu.Unlock()
			if len(items) == 0 {
				return
			}
			allDone := true
			for _, item := range items {
				callCtx, cancel := context.WithTimeout(context.Background(), s.leaseCallTimeout())
				watchDone := make(chan struct{})
				go func() {
					select {
					case <-stopCh:
						cancel()
					case <-watchDone:
					}
				}()
				uerr := s.executeUnbind(callCtx, item)
				close(watchDone)
				cancel()
				if uerr != nil {
					allDone = false
					continue
				}
				s.mu.Lock()
				if s.journal == nil || s.journal.Uncertain() {
					s.mu.Unlock()
					return
				}
				if aerr := s.journal.AppendUnbindDone(item); aerr != nil {
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()
			}
			if allDone {
				s.mu.Lock()
				empty := s.journal == nil || len(s.journal.Snapshot().PendingUnbinds) == 0
				s.mu.Unlock()
				if empty {
					return
				}
			}
			select {
			case <-stopCh:
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()
}

func (s *mountViewSwitch) query(req mountcontrol.SwitchRequest) mountcontrol.SwitchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := req.Validate(); err != nil {
		return switchErrResp(req, viewprotocol.CodeInvalidArgument, "INVALID", err)
	}
	if s.journal.Uncertain() {
		return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", mountstate.ErrSwitchUncertain)
	}
	snap := s.journal.Snapshot()
	if req.MountID != snap.MountID {
		return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", errors.New("mount incarnation mismatch"))
	}
	prior, ok := s.journal.Operation(req.OperationID)
	if !ok {
		return switchErrResp(req, viewprotocol.CodeLayerNotFound, "NOT_FOUND", fmt.Errorf("switch operation %q not found", req.OperationID))
	}
	if prior.State == mountstate.SwitchTargetRevoked {
		return mountcontrol.SwitchResponse{
			MountID:     snap.MountID,
			OperationID: req.OperationID,
			Receipt:     &prior,
			Code:        mountcontrol.CodeSwitchTargetRevoked,
			ErrorKind:   "TARGET_REVOKED",
			Error:       "target layer or lease was revoked",
		}
	}
	return mountcontrol.SwitchResponse{
		MountID:     snap.MountID,
		OperationID: req.OperationID,
		Receipt:     &prior,
	}
}

func (s *mountViewSwitch) drain(ctx context.Context) mountcontrol.DrainResponse {
	resp := mountcontrol.NewDrainResponse("", time.Now().UTC())
	if !s.mu.TryLock() {
		resp.Fail("BUSY", "", errors.New("mount switch in progress"))
		resp.Finish(time.Now().UTC())
		return resp
	}
	defer s.mu.Unlock()
	if s.journal.Uncertain() {
		resp.Fail("RECOVERING", "", mountstate.ErrSwitchUncertain)
		resp.Finish(time.Now().UTC())
		return resp
	}
	snap := s.journal.Snapshot()
	if snap.Pending != nil {
		if snap.Pending.State == mountstate.SwitchTargetRevoked {
			// Outgoing view was 100% drained prior to Decide and ordinary FUSE
			// stayed closed with zero new writes (v36 §6.3 #4 umount path).
			resp.Finish(time.Now().UTC())
			return resp
		}
		resp.Fail("RECOVERING", "", mountstate.ErrSwitchPending)
		resp.Finish(time.Now().UTC())
		return resp
	}
	if s.recoveringPaused.Load() {
		resp.Finish(time.Now().UTC())
		return resp
	}
	return s.current.fs.Drain(ctx)
}

func (s *mountViewSwitch) apply(ctx context.Context, req mountcontrol.SwitchRequest) mountcontrol.SwitchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := req.Validate(); err != nil {
		return switchErrResp(req, viewprotocol.CodeInvalidArgument, "INVALID", err)
	}
	if s.journal.Uncertain() {
		return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", mountstate.ErrSwitchUncertain)
	}
	snap := s.journal.Snapshot()
	if req.MountID != snap.MountID {
		return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", errors.New("mount incarnation mismatch"))
	}
	if !s.supported() {
		return switchErrResp(req, mountcontrol.CodeSwitchUnsupported, "UNSUPPORTED", mountcontrol.ErrSwitchUnsupported)
	}
	isMain, layerRef, _ := req.NormalizedTarget()

	if prior, ok := s.journal.Operation(req.OperationID); ok {
		if prior.FromGeneration != req.ExpectedGeneration || req.Recover != prior.Recovery {
			return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", mountstate.ErrSwitchConflict)
		}
		matched, matchErr := s.targetMatchesReplay(ctx, isMain, layerRef, prior)
		if matchErr != nil {
			return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", matchErr)
		}
		if !matched {
			return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", mountstate.ErrSwitchConflict)
		}
		if prior.State == mountstate.SwitchTargetRevoked {
			return mountcontrol.SwitchResponse{MountID: snap.MountID, OperationID: req.OperationID, Receipt: &prior, Code: mountcontrol.CodeSwitchTargetRevoked, ErrorKind: "TARGET_REVOKED", Error: "target layer or lease was revoked"}
		}
		if prior.State == mountstate.SwitchRecovering && snap.Pending != nil && snap.Pending.OperationID == req.OperationID {
			return s.finishPendingLocked(ctx, req.OperationID)
		}
		if prior.State == mountstate.SwitchDone {
			return mountcontrol.SwitchResponse{MountID: snap.MountID, OperationID: req.OperationID, Receipt: &prior}
		}
		return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", mountstate.ErrSwitchConflict)
	}

	recoveringFromRevoked := s.recoveringPaused.Load()
	if req.Recover {
		if snap.Pending != nil {
			if snap.Pending.State != mountstate.SwitchTargetRevoked {
				return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", mountstate.ErrSwitchNotRevoked)
			}
			if req.ExpectedGeneration != snap.Generation {
				return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", fmt.Errorf("expected_generation %d != current %d", req.ExpectedGeneration, snap.Generation))
			}
			if s.pendingPause == nil {
				p, perr := s.adapter.admission.pause()
				if perr != nil {
					return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", perr)
				}
				s.pendingPause = p
			}
			if s.pendingLease != nil {
				s.pendingLease.StopRenew()
				s.pendingLease = nil
			}
			if s.current != nil && s.current.layerLease != nil {
				s.current.layerLease.StopRenew()
			}
			if s.pendingTarget != nil {
				if s.pendingInstalled {
					s.current = s.pendingTarget
					s.pendingTarget = nil
					s.pendingInstalled = false
					s.outgoingStopped = false
				} else {
					if serr := s.pendingTarget.stop(ctx); serr != nil {
						return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", serr)
					}
					s.pendingTarget = nil
				}
			}
			s.pendingInstalled = false
			if _, aerr := s.journal.AbortPendingForRecover(snap.Pending.OperationID); aerr != nil {
				if errors.Is(aerr, mountstate.ErrSwitchUncertain) {
					return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", aerr)
				}
				return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", aerr)
			}
			s.recoveringPaused.Store(true)
			recoveringFromRevoked = true
			snap = s.journal.Snapshot()
		} else if !recoveringFromRevoked {
			return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", mountstate.ErrSwitchNotRevoked)
		}
	} else if snap.Pending != nil || recoveringFromRevoked {
		if recoveringFromRevoked || snap.Pending != nil && snap.Pending.State == mountstate.SwitchTargetRevoked {
			return switchErrResp(req, mountcontrol.CodeSwitchTargetRevoked, "TARGET_REVOKED",
				errors.New("pending switch target was revoked; use --recover or unmount"))
		}
		return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING",
			errors.New("another switch operation is recovering"))
	}

	if req.ExpectedGeneration != snap.Generation {
		return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", fmt.Errorf("expected_generation %d != current %d", req.ExpectedGeneration, snap.Generation))
	}
	if err := s.drainPendingUnbindsLocked(ctx); err != nil || len(s.journal.Snapshot().PendingUnbinds) > 0 {
		s.kickAsyncDrainUnbinds()
		return switchErrResp(req, mountcontrol.CodeSwitchUnbindCleanupPending, "CLEANUP_PENDING", mountstate.ErrSwitchUnbindPending)
	}
	return s.executeSwitchPrecheckedLocked(ctx, req, snap, isMain, layerRef, recoveringFromRevoked)
}

func (s *mountViewSwitch) targetMatchesReplay(ctx context.Context, isMain bool, layerRef string, prior mountstate.SwitchReceipt) (bool, error) {
	target := prior.Target
	if isMain {
		return target.LayerID == "", nil
	}
	if target.LayerID == "" {
		return false, nil
	}
	if viewprotocol.IsCanonicalLayerID(layerRef) {
		return target.LayerID == layerRef, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, s.leaseCallTimeout())
	info, err := s.baseClient.GetLayer(callCtx, layerRef)
	cancel()
	if err != nil {
		if client.IsNotFound(err) {
			return layerRef == prior.RequestedLayerRef, nil
		}
		return false, err
	}
	return info != nil && info.LayerID == target.LayerID, nil
}

func (s *mountViewSwitch) executeSwitchPrecheckedLocked(
	ctx context.Context,
	req mountcontrol.SwitchRequest,
	snap mountstate.SwitchSnapshot,
	isMain bool,
	layerRef string,
	recoveringFromRevoked bool,
) mountcontrol.SwitchResponse {
	var targetInfo *client.LayerInfo
	var baseRoot string
	var observedEvenFence int64

	if isMain {
		if !recoveringFromRevoked && snap.Binding.LayerID == "" {
			return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", errors.New("mount is already serving main"))
		}
		if err := validateRemoteRoot(ctx, s.baseClient, s.remoteRoot); err != nil {
			return classifyPreDecisionError(req, err)
		}
	} else {
		info, err := s.baseClient.GetLayer(ctx, layerRef)
		if err != nil {
			return classifyPreDecisionError(req, err)
		}
		if !recoveringFromRevoked && info.LayerID == snap.Binding.LayerID {
			return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", fmt.Errorf("mount is already serving layer %s", info.LayerID))
		}
		canonBase, cerr := viewprotocol.CanonicalizeViewPath(info.BaseRootPath)
		if cerr != nil {
			return switchErrResp(req, viewprotocol.CodeInvalidArgument, "INVALID", cerr)
		}
		if !viewprotocol.PathWithinBaseRoot(canonBase, s.remoteRoot) {
			return switchErrResp(req, viewprotocol.CodeForbiddenOutsideBase, "FORBIDDEN",
				fmt.Errorf("mount remote root %q is outside layer %s base_root_path %q", s.remoteRoot, info.LayerID, canonBase))
		}
		baseRoot = canonBase
		targetInfo = info

		if !s.cfg.ReadOnly {
			if info.Fence%2 == 1 && !info.MountExpired {
				return switchErrResp(req, viewprotocol.CodeLayerMountConflict, "CONFLICT",
					fmt.Errorf("layer %s already has an active exclusive mount lease", info.LayerID))
			}
			observedEvenFence = info.Fence
			if observedEvenFence%2 == 1 && info.MountExpired {
				unb, uerr := s.baseClient.UnbindLayerMount(ctx, info.LayerID, client.LayerMountUnbindRequest{
					MountID: snap.MountID,
					Kind:    mountstate.UnbindKindExpire,
				})
				if uerr != nil {
					return classifyPreDecisionError(req, uerr)
				}
				if unb != nil {
					observedEvenFence = unb.Fence
				}
			}
			if observedEvenFence < 0 || observedEvenFence%2 != 0 {
				return switchErrResp(req, viewprotocol.CodeLayerMountConflict, "CONFLICT",
					fmt.Errorf("layer %s did not settle to a non-negative even fence (got %d)", info.LayerID, observedEvenFence))
			}
		} else {
			vc, verr := s.baseClient.WithView(client.ViewBinding{LayerID: info.LayerID, Fence: 0})
			if verr != nil {
				return switchErrResp(req, viewprotocol.CodeInvalidArgument, "INVALID", verr)
			}
			if err := validateRemoteRoot(ctx, vc, s.remoteRoot); err != nil {
				return classifyPreDecisionError(req, err)
			}
		}
	}

	var pause *viewPause
	var plan viewCacheInvalidation
	if recoveringFromRevoked {
		pause = s.pendingPause
		if pause == nil {
			p, perr := s.adapter.admission.pause()
			if perr != nil {
				return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", perr)
			}
			pause = p
			s.pendingPause = p
		}
		plan = s.pendingPlan
		if len(plan.inodes) == 0 && s.current != nil && s.current.fs != nil {
			var perr error
			plan, perr = prepareViewCacheInvalidation(s.current.fs.inodes.Snapshot())
			if perr != nil {
				return switchErrResp(req, mountcontrol.CodeSwitchPrepareFailed, "PREPARE_FAILED", perr)
			}
		}
	} else {
		var perr error
		pause, plan, perr = s.adapter.prepareViewSwitch(ctx, s.current.fs, s.notifier, s.fence)
		if perr != nil {
			return classifyPreDecisionError(req, perr)
		}
	}

	rollbackPreDecision := func(cause error) mountcontrol.SwitchResponse {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.leaseCallTimeout())
		derr := s.drainPendingUnbindsLocked(cleanupCtx)
		cancel()
		stillPending := len(s.journal.Snapshot().PendingUnbinds) > 0
		if !recoveringFromRevoked && !s.journal.Uncertain() {
			_ = pause.resume()
		}
		if s.journal.Uncertain() {
			s.pendingPause = pause
			return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", mountstate.ErrSwitchUncertain)
		}
		if derr != nil || stillPending {
			s.kickAsyncDrainUnbinds()
			return switchErrResp(req, mountcontrol.CodeSwitchBindCleanupPending, "CLEANUP_PENDING",
				fmt.Errorf("rollback unbind pending after %v", cause))
		}
		return classifyPreDecisionError(req, cause)
	}

	targetBinding := mountstate.SwitchBinding{ReadOnly: s.cfg.ReadOnly}
	var newLease *layerMountBinding
	if !isMain {
		targetBinding.LayerID = targetInfo.LayerID
		targetBinding.BaseRootPath = baseRoot
		if !s.cfg.ReadOnly {
			if ierr := s.journal.AppendBindIntent(mountstate.PendingUnbind{
				LayerID:     targetInfo.LayerID,
				MountID:     snap.MountID,
				Kind:        mountstate.UnbindKindOp,
				OperationID: req.OperationID,
				Fence:       observedEvenFence,
			}); ierr != nil {
				if errors.Is(ierr, mountstate.ErrSwitchUncertain) {
					s.pendingPause = pause
					return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", ierr)
				}
				if !recoveringFromRevoked {
					_ = pause.resume()
				}
				return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", ierr)
			}
			bindStart := time.Now()
			leaseResp, berr := s.baseClient.BindLayerMount(ctx, targetInfo.LayerID, client.LayerMountBindRequest{
				MountID:       snap.MountID,
				OperationID:   req.OperationID,
				ExpectedFence: observedEvenFence,
			})
			if berr != nil {
				return rollbackPreDecision(berr)
			}
			targetBinding.Fence = leaseResp.Fence
			newLease = &layerMountBinding{
				BaseClient:   s.baseClient,
				LayerID:      targetInfo.LayerID,
				BaseRootPath: baseRoot,
				RemoteRoot:   s.remoteRoot,
				MountID:      snap.MountID,
				OperationID:  req.OperationID,
				Fence:        leaseResp.Fence,
			}
			vc, verr := s.baseClient.WithView(client.ViewBinding{
				LayerID:    targetInfo.LayerID,
				Fence:      leaseResp.Fence,
				WriteGuard: newLease.checkWriteAllowed, ReadUnfenced: newLease.fenced.Load,
			})
			if verr != nil {
				return rollbackPreDecision(verr)
			}
			if rerr := validateRemoteRoot(ctx, vc, s.remoteRoot); rerr != nil {
				return rollbackPreDecision(rerr)
			}
			newLease.ViewClient = vc
			newLease.startRenewLoop(bindStart, leaseResp.MountLeaseTTLMS, s.timing)
		}
	}

	var decideErr error
	idleErr := pause.atIdle(ctx, func() error {
		if !recoveringFromRevoked {
			if cerr := s.adapter.checkViewSwitch(s.current.fs); cerr != nil {
				return cerr
			}
		}
		_, _, decideErr = s.journal.DecideWithLayerRef(req.OperationID, snap.Generation, targetBinding, layerRef)
		return decideErr
	})
	if idleErr != nil {
		if newLease != nil {
			newLease.StopRenew()
		}
		if errors.Is(idleErr, mountstate.ErrSwitchUncertain) || s.journal.Uncertain() {
			s.pendingPause = pause
			return switchErrResp(req, mountcontrol.CodeSwitchRecovering, "RECOVERING", idleErr)
		}
		return rollbackPreDecision(idleErr)
	}

	s.pendingPause = pause
	s.pendingPlan = plan
	s.pendingLease = newLease
	if s.pendingTarget != nil {
		_ = s.pendingTarget.stop(context.Background())
		s.pendingTarget = nil
	}
	s.pendingInstalled = false
	if !recoveringFromRevoked {
		s.outgoingStopped = false
	}
	return s.finishPendingLocked(ctx, req.OperationID)
}
