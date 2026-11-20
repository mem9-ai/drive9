//go:build !windows

package fuse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// mountViewSwitch coordinates one live mount. Construction and kernel ownership
// remain with the mount entry: notifier/fence must name that same connection,
// validate must check the server-issued target qualification, and build must
// return an unstarted runtime bound to that connection and exact generation.
// A failed build must clean up its partial runtime. No callback may run work.
// This coordinator does not reopen journals or transparently recover a dead
// FUSE connection. A reopened pending journal requires explicit mount recovery.
type mountViewSwitch struct {
	mu       sync.Mutex
	journal  *mountstate.SwitchJournal
	adapter  *viewRequestFS
	current  *mountRuntime
	notifier viewCacheNotifier
	fence    func(context.Context) error
	validate func(context.Context, viewprotocol.Binding) error
	build    func(context.Context, viewprotocol.Binding, mountRuntimeGeneration) (*mountRuntime, error)
	pending  *mountViewPending
}

type mountViewPending struct {
	pause        *viewPause
	plan         viewCacheInvalidation
	oldStopped   bool
	target       *mountRuntime
	installation *viewInstallation
}

func (s *mountViewSwitch) query(req mountcontrol.SwitchRequest) mountcontrol.SwitchResponse {
	resp := mountcontrol.SwitchResponse{MountID: req.MountID, OperationID: req.OperationID}
	if err := req.Validate(); err != nil {
		return switchFailure(resp, "INVALID", err)
	}
	snapshot, err := s.journal.Snapshot()
	if err != nil {
		return switchFailure(resp, "UNKNOWN", err)
	}
	if snapshot.MountID != req.MountID {
		return switchFailure(resp, "CONFLICT", mountstate.ErrSwitchConflict)
	}
	r, found, err := s.journal.Lookup(req.OperationID)
	if err != nil {
		return switchFailure(resp, "UNKNOWN", err)
	}
	if !found {
		return switchFailure(resp, "NOT_FOUND", errors.New("mount switch operation not recorded"))
	}
	if r.FromGeneration != req.ExpectedGeneration || r.Target != req.Target {
		return switchFailure(resp, "CONFLICT", mountstate.ErrSwitchConflict)
	}
	resp.Receipt = &r
	return resp
}

func (s *mountViewSwitch) apply(ctx context.Context, req mountcontrol.SwitchRequest) mountcontrol.SwitchResponse {
	resp := mountcontrol.SwitchResponse{MountID: req.MountID, OperationID: req.OperationID}
	if !s.mu.TryLock() {
		// Another attempt may already have decided this very operation. Do
		// not call a concurrent same-ID retry a definite pre-decision refusal.
		return switchFailure(resp, "UNKNOWN", errors.New("mount switch already executing; query original operation"))
	}
	defer s.mu.Unlock()
	resp = s.query(req)
	if resp.Receipt != nil {
		if resp.Receipt.State == mountstate.SwitchDone {
			return resp // Replay never changes the current binding.
		}
		if s.pending == nil {
			return switchFailure(resp, "RECOVERING", errors.New("mount runtime recovery required"))
		}
		return s.finish(ctx, resp)
	}
	if resp.ErrorKind != "NOT_FOUND" {
		return resp
	}
	resp.ErrorKind, resp.Error = "", ""
	snapshot, err := s.journal.Snapshot()
	if err != nil {
		return switchFailure(resp, "UNKNOWN", err)
	}
	if snapshot.Pending != nil || snapshot.Generation != req.ExpectedGeneration || s.pending != nil {
		return switchFailure(resp, "CONFLICT", mountstate.ErrSwitchConflict)
	}
	if s.validate == nil || s.build == nil || s.adapter == nil || s.current == nil || s.current.fs == nil {
		return switchFailure(resp, "UNSUPPORTED", errors.New("mount switch runtime not configured"))
	}
	if s.adapter.currentTarget().generation != snapshot.Generation || s.current.binding != snapshot.Binding {
		return switchFailure(resp, "CONFLICT", mountstate.ErrSwitchConflict)
	}
	if err := s.validate(ctx, req.Target); err != nil {
		return switchFailure(resp, "TARGET_UNAVAILABLE", err)
	}
	pause, plan, err := s.adapter.prepareViewSwitch(ctx, s.current.fs, s.notifier, s.fence)
	if err != nil {
		kind := "PREPARE_FAILED"
		if errors.Is(err, syscall.EBUSY) {
			kind = "BUSY"
		}
		return switchFailure(resp, kind, err)
	}
	var receipt mountstate.SwitchReceipt
	err = pause.atIdle(ctx, func() error {
		if err := s.adapter.checkViewSwitch(s.current.fs); err != nil {
			return err
		}
		receipt, err = s.journal.Decide(req.OperationID, req.ExpectedGeneration, req.Target)
		return err
	})
	if err != nil {
		if errors.Is(err, mountstate.ErrSwitchUncertain) {
			// Even no receipt is not evidence of abort. Keep admission closed;
			// the failed journal cannot grant another attempt a fresh decision.
			return switchFailure(resp, "UNKNOWN", err)
		}
		return switchFailure(resp, "PREPARE_FAILED", errors.Join(err, pause.resume()))
	}
	s.pending = &mountViewPending{pause: pause, plan: plan}
	resp.Receipt = &receipt
	return s.finish(ctx, resp)
}

func (s *mountViewSwitch) finish(ctx context.Context, resp mountcontrol.SwitchResponse) mountcontrol.SwitchResponse {
	p := s.pending
	fail := func(err error) mountcontrol.SwitchResponse { return switchFailure(resp, "RECOVERING", err) }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if s.adapter.unmounted.Load() {
		return fail(errViewUnmounted)
	}
	if !p.oldStopped {
		// No new ordinary callback can construct a runtime or enqueue a write.
		// Stop must report actual completion, not just request cancellation.
		if err := s.current.stop(ctx); err != nil {
			return fail(fmt.Errorf("stop old mount view: %w", err))
		}
		p.oldStopped = true
	}
	if p.target == nil {
		if err := s.validate(ctx, resp.Receipt.Target); err != nil {
			return fail(err)
		}
		target, err := s.build(ctx, resp.Receipt.Target, mountRuntimeGeneration{MountID: resp.MountID, Generation: resp.Receipt.Generation})
		if err != nil {
			return fail(err)
		}
		if target == nil || target.fs == nil || target.fs == s.current.fs || target.binding != resp.Receipt.Target ||
			target.generation != (mountRuntimeGeneration{MountID: resp.MountID, Generation: resp.Receipt.Generation}) {
			return fail(errors.New("invalid target mount runtime"))
		}
		p.target = target
	}
	// Start is attempted exactly once for each constructed runtime. If it
	// fails, keep the failed runtime until its real cleanup completes. Only
	// then can a retry build a replacement in this decided generation.
	p.target.fs.Init2(ctx)
	if startErr := p.target.fs.StartErr(); startErr != nil {
		if stopErr := p.target.stop(ctx); stopErr != nil {
			return fail(errors.Join(startErr, stopErr))
		}
		p.target = nil
		return fail(startErr)
	}

	if p.installation == nil {
		installation, err := newViewInstallation(s.journal, s.adapter, p.pause, p.target.fs, p.plan, s.notifier, s.fence)
		if err != nil {
			return fail(err)
		}
		p.installation = installation
	}
	receipt, err := p.installation.finish(ctx)
	resp.Receipt = receipt
	if err != nil {
		if receipt == nil {
			return switchFailure(resp, "UNKNOWN", err)
		}
		return fail(err)
	}
	s.current = p.target
	s.pending = nil
	return resp
}

func switchFailure(resp mountcontrol.SwitchResponse, kind string, err error) mountcontrol.SwitchResponse {
	resp.ErrorKind, resp.Error = kind, err.Error()
	return resp
}
