//go:build !windows

package fuse

import (
	"context"
	"errors"
	"net/http"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func switchErrResp(req mountcontrol.SwitchRequest, code, kind string, err error) mountcontrol.SwitchResponse {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return mountcontrol.SwitchResponse{
		MountID:     req.MountID,
		OperationID: req.OperationID,
		Code:        code,
		ErrorKind:   kind,
		Error:       msg,
	}
}

func (s *mountViewSwitch) finishPendingLocked(ctx context.Context, opID string) mountcontrol.SwitchResponse {
	snap := s.journal.Snapshot()
	req := mountcontrol.SwitchRequest{MountID: snap.MountID, OperationID: opID}
	if s.journal.Uncertain() {
		return switchErrResp(req, viewprotocol.CodeSwitchRecovering, "RECOVERING", mountstate.ErrSwitchUncertain)
	}
	if snap.Pending == nil || snap.Pending.OperationID != opID {
		if done, ok := s.journal.Operation(opID); ok && done.State == mountstate.SwitchDone {
			return mountcontrol.SwitchResponse{MountID: snap.MountID, OperationID: opID, Receipt: &done}
		}
		return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", mountstate.ErrSwitchConflict)
	}
	dec := *snap.Pending
	if dec.State == mountstate.SwitchTargetRevoked {
		return mountcontrol.SwitchResponse{
			MountID:     snap.MountID,
			OperationID: opID,
			Receipt:     &dec,
			Code:        viewprotocol.CodeSwitchTargetRevoked,
			ErrorKind:   "TARGET_REVOKED",
			Error:       "target layer or lease was revoked",
		}
	}

	pause := s.pendingPause
	if pause == nil {
		var perr error
		pause, perr = s.adapter.admission.pause()
		if perr != nil {
			return s.recoveringResp(dec, perr)
		}
		s.pendingPause = pause
	}
	if len(s.pendingPlan.inodes) == 0 && s.current != nil && s.current.fs != nil {
		if plan, perr := prepareViewCacheInvalidation(s.current.fs.inodes.Snapshot()); perr == nil {
			s.pendingPlan = plan
		}
	}

	// Re-verify target before stopping outgoing runtime or installing target (v36 §6.3 #4).
	var targetLease *layerMountBinding
	if dec.Target.LayerID != "" {
		if dec.Target.ReadOnly {
			if _, gerr := s.baseClient.GetLayer(ctx, dec.Target.LayerID); gerr != nil {
				if isRevokedTargetError(gerr) {
					return s.markRevokedLocked(dec, gerr)
				}
				return s.recoveringResp(dec, gerr)
			}
			vc, verr := s.baseClient.WithView(client.ViewBinding{LayerID: dec.Target.LayerID, Fence: 0})
			if verr != nil {
				return s.recoveringResp(dec, verr)
			}
			targetLease = &layerMountBinding{
				BaseClient: s.baseClient, ViewClient: vc, LayerID: dec.Target.LayerID,
				BaseRootPath: dec.Target.BaseRootPath, RemoteRoot: s.remoteRoot, MountID: snap.MountID, ReadOnly: true,
			}
		} else {
			callStart := time.Now()
			renewed, rerr := s.baseClient.RenewLayerMount(ctx, dec.Target.LayerID, client.LayerMountRenewRequest{
				MountID: snap.MountID,
				Fence:   dec.Target.Fence,
			})
			if rerr != nil {
				if isRevokedTargetError(rerr) {
					return s.markRevokedLocked(dec, rerr)
				}
				return s.recoveringResp(dec, rerr)
			}
			targetLease = s.pendingLease
			if targetLease == nil || targetLease.LayerID != dec.Target.LayerID || targetLease.Fence != dec.Target.Fence {
				targetLease = &layerMountBinding{
					BaseClient: s.baseClient, LayerID: dec.Target.LayerID, BaseRootPath: dec.Target.BaseRootPath,
					RemoteRoot: s.remoteRoot, MountID: snap.MountID, OperationID: dec.OperationID, Fence: dec.Target.Fence,
				}
				vc, verr := s.baseClient.WithView(client.ViewBinding{
					LayerID: dec.Target.LayerID, Fence: dec.Target.Fence, WriteGuard: targetLease.checkWriteAllowed, ReadUnfenced: targetLease.fenced.Load,
				})
				if verr != nil {
					return s.recoveringResp(dec, verr)
				}
				targetLease.ViewClient = vc
				targetLease.startRenewLoop(callStart, renewed.MountLeaseTTLMS, s.timing)
				s.pendingLease = targetLease
			}
		}
	}

	if s.current != nil {
		if err := s.current.stop(ctx); err != nil {
			return s.recoveringResp(dec, err)
		}
	}
	if s.pendingTarget == nil {
		gen := mountRuntimeGeneration{MountID: snap.MountID, Generation: dec.Generation}
		rt, berr := s.buildRuntime(ctx, dec.Target, targetLease, gen)
		if berr != nil {
			return s.recoveringResp(dec, berr)
		}
		startCtx, startCancel := context.WithTimeout(ctx, 20*time.Second)
		rt.fs.Init2(startCtx)
		startCancel()
		if serr := rt.fs.StartErr(); serr != nil {
			_ = rt.stop(context.Background())
			return s.recoveringResp(dec, serr)
		}
		s.pendingTarget = rt
	}

	if err := s.fence(ctx); err != nil {
		return s.recoveringResp(dec, err)
	}
	if !s.pendingInstalled {
		curGen := s.adapter.currentTarget().generation
		if curGen != dec.Generation {
			if err := s.adapter.installTargetAtGeneration(ctx, pause, curGen, dec.Generation, s.pendingTarget.fs); err != nil {
				return s.recoveringResp(dec, err)
			}
		}
		s.pendingInstalled = true
	}
	if err := s.pendingPlan.invalidate(ctx, s.notifier); err != nil {
		return s.recoveringResp(dec, err)
	}

	var completed mountstate.SwitchReceipt
	err := pause.resumeAtIdle(ctx, func() error {
		if s.adapter.unmounted.Load() {
			return errViewUnmounted
		}
		var cerr error
		completed, cerr = s.journal.Complete(dec.OperationID)
		return cerr
	})
	if err != nil {
		return s.recoveringResp(dec, err)
	}
	s.current = s.pendingTarget
	s.pendingPause = nil
	s.pendingPlan = viewCacheInvalidation{}
	s.pendingLease = nil
	s.pendingTarget = nil
	s.pendingInstalled = false
	s.recoveringPaused = false
	if len(s.journal.Snapshot().PendingUnbinds) > 0 {
		s.kickAsyncDrainUnbinds()
	}
	return mountcontrol.SwitchResponse{
		MountID:     snap.MountID,
		OperationID: dec.OperationID,
		Receipt:     &completed,
	}
}

func (s *mountViewSwitch) markRevokedLocked(dec mountstate.SwitchReceipt, cause error) mountcontrol.SwitchResponse {
	if s.pendingLease != nil {
		s.pendingLease.StopRenew()
		s.pendingLease = nil
	}
	rev, merr := s.journal.MarkTargetRevoked(dec.OperationID)
	if merr != nil {
		return s.recoveringResp(dec, errors.Join(cause, merr))
	}
	return mountcontrol.SwitchResponse{
		MountID:     s.journal.Snapshot().MountID,
		OperationID: dec.OperationID,
		Receipt:     &rev,
		Code:        viewprotocol.CodeSwitchTargetRevoked,
		ErrorKind:   "TARGET_REVOKED",
		Error:       cause.Error(),
	}
}

func (s *mountViewSwitch) recoveringResp(dec mountstate.SwitchReceipt, cause error) mountcontrol.SwitchResponse {
	snap := s.journal.Snapshot()
	rcpt := dec
	if r, ok := s.journal.Operation(dec.OperationID); ok {
		rcpt = r
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return mountcontrol.SwitchResponse{
		MountID:     snap.MountID,
		OperationID: dec.OperationID,
		Receipt:     &rcpt,
		Code:        viewprotocol.CodeSwitchRecovering,
		ErrorKind:   "RECOVERING",
		Error:       msg,
	}
}

func isRevokedTargetError(err error) bool {
	if client.IsNotFound(err) || errors.Is(err, client.ErrConflict) || client.IsForbidden(err) {
		return true
	}
	var st *client.StatusError
	if errors.As(err, &st) {
		switch st.StatusCode {
		case http.StatusNotFound, http.StatusConflict, http.StatusForbidden, http.StatusBadRequest:
			return true
		}
	}
	return false
}

func classifyPreDecisionError(req mountcontrol.SwitchRequest, err error) mountcontrol.SwitchResponse {
	if errors.Is(err, errViewPrepareFailed) {
		return switchErrResp(req, viewprotocol.CodeSwitchPrepareFailed, "PREPARE_FAILED", err)
	}
	if errors.Is(err, syscall.EBUSY) {
		return switchErrResp(req, viewprotocol.CodeSwitchBusy, "BUSY", err)
	}
	var st *client.StatusError
	if errors.As(err, &st) {
		code := st.Code
		switch st.StatusCode {
		case http.StatusNotFound:
			if code == "" {
				code = viewprotocol.CodeLayerNotFound
			}
			return switchErrResp(req, code, "NOT_FOUND", err)
		case http.StatusForbidden:
			if code == "" {
				code = viewprotocol.CodeForbidden
			}
			return switchErrResp(req, code, "FORBIDDEN", err)
		case http.StatusConflict:
			if code == "" {
				code = viewprotocol.CodeConflict
			}
			return switchErrResp(req, code, "CONFLICT", err)
		case http.StatusBadRequest:
			if code == "" {
				code = viewprotocol.CodeInvalidArgument
			}
			return switchErrResp(req, code, "INVALID", err)
		case http.StatusNotImplemented:
			if code == "" {
				code = viewprotocol.CodeSwitchUnsupported
			}
			return switchErrResp(req, code, "UNSUPPORTED", err)
		}
	}
	if client.IsNotFound(err) {
		return switchErrResp(req, viewprotocol.CodeLayerNotFound, "NOT_FOUND", err)
	}
	if client.IsForbidden(err) {
		return switchErrResp(req, viewprotocol.CodeForbidden, "FORBIDDEN", err)
	}
	if errors.Is(err, client.ErrConflict) {
		return switchErrResp(req, viewprotocol.CodeConflict, "CONFLICT", err)
	}
	return switchErrResp(req, viewprotocol.CodeSwitchPrepareFailed, "PREPARE_FAILED", err)
}
