package fuse

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"
)

// Kernel invalidation queues FORGET separately from its notification reply.
// Give those callbacks a bounded opportunity to retire idle lookup references;
// references still held by a cwd, fd or mapping remain BUSY after this budget.
const viewForgetWait = time.Second

// prepareViewSwitch pauses ordinary callbacks, drains the current runtime and
// evicts reclaimable kernel references before checking what remains pinned.
// The caller must have validated the target and kernel support first. No switch
// decision or target installation happens here. Failure reopens the old view;
// success returns the owned pause and detached plan for the eventual decision.
//
// A successful observation is not permanent: the owner must call checkViewSwitch
// inside pause.atIdle again when recording the durable decision. It must also
// serialize control operations and stop old background publishers after deciding.
func (v *viewRequestFS) prepareViewSwitch(ctx context.Context, current *FS, notifier viewCacheNotifier, fenceRoot func(context.Context) error) (pause *viewPause, plan viewCacheInvalidation, err error) {
	if current == nil || notifier == nil || fenceRoot == nil || v.currentTarget().fs != current {
		return nil, plan, errViewGenerationConflict
	}
	pause, err = v.admission.pause()
	if err != nil {
		return nil, plan, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, pause.resume())
			pause = nil
		}
	}()
	if err = pause.wait(ctx); err != nil {
		return pause, plan, err
	}
	if v.unmounted.Load() || current.unmounted.Load() {
		return pause, plan, errViewUnmounted
	}
	// Avoid invalidating data while an unpreserved open handle/mapping or
	// lock can still use it. Idle cache lookup references are checked after
	// notifications give the kernel a chance to drop them.
	if current.HandleCount() != 0 || current.DirCount() != 0 || current.heldLockCount() != 0 {
		return pause, plan, fmt.Errorf("mount view has open handles or locks: %w", syscall.EBUSY)
	}
	if drained := current.Drain(ctx); !drained.OK {
		return pause, plan, fmt.Errorf("drain old mount view (%s): %s", drained.ErrorKind, drained.Error)
	}
	for _, refusal := range current.ReexecPreflight().Refusals {
		if refusal.Gate != "inode_nlookup" {
			return pause, plan, fmt.Errorf("mount view %s: %s: %w", refusal.Gate, refusal.Reason, syscall.EBUSY)
		}
	}
	plan, err = prepareViewCacheInvalidation(current.inodes.Snapshot())
	if err != nil {
		return pause, plan, err
	}
	if err = plan.invalidate(ctx, notifier); err != nil {
		return pause, plan, err
	}
	if err = fenceRoot(ctx); err != nil {
		return pause, plan, err
	}
	forgetCtx, cancel := context.WithTimeout(ctx, viewForgetWait)
	defer cancel()
	err = pause.waitForCleanup(forgetCtx, func() bool {
		return v.unmounted.Load() || current.unmounted.Load() || current.nonRootKernelLookupCount() == 0
	})
	if err != nil {
		if ctx.Err() != nil {
			return pause, plan, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return pause, plan, fmt.Errorf("mount view retains kernel lookup references: %w", syscall.EBUSY)
		}
		return pause, plan, err
	}
	err = pause.atIdle(ctx, func() error { return v.checkViewSwitch(current) })
	return pause, plan, err
}

// checkViewSwitch must run inside the owned pause's idle section. Any reference
// that remains pinned is BUSY: this implementation does not rebind old handles
// or silently turn an old non-root cwd into a path in the selected target.
func (v *viewRequestFS) checkViewSwitch(current *FS) error {
	if v.unmounted.Load() || current.unmounted.Load() {
		return errViewUnmounted
	}
	if v.currentTarget().fs != current {
		return errViewGenerationConflict
	}
	if result := current.ReexecPreflight(); !result.OK {
		return fmt.Errorf("mount view retains references or private extension state (%v): %w", result.Refusals, syscall.EBUSY)
	}
	return nil
}
