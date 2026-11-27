package fuse

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"
)

const viewForgetWait = time.Second

// errViewPrepareFailed distinguishes Gate 4 Drain failures (PREPARE_FAILED,
// preserving all dirty/uncommitted writes in current runtime) from EBUSY.
var errViewPrepareFailed = errors.New("mount view drain prepare failed")

// prepareViewSwitch executes the 5 local preflight gates (v36 §6.3 #3):
//
//	1-3. Open file handles (incl. closed-fd mmap), dir handles, held FUSE locks -> EBUSY.
//	4.   current.Drain(ctx) -> PREPARE_FAILED (dirty data retained in current).
//	5.   Extension ViewSwitchGater/ReexecGater + EntryNotify/InodeNotify/root fence
//	     + 1s waitForCleanup until nonRootKernelLookupCount == 0 + atIdle recheck -> EBUSY.
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
	if current.HandleCount() != 0 || current.DirCount() != 0 || current.heldLockCount() != 0 {
		return pause, plan, fmt.Errorf("mount view has open handles or locks: %w", syscall.EBUSY)
	}
	if drained := current.Drain(ctx); !drained.OK {
		return pause, plan, fmt.Errorf("%w (%s): %s", errViewPrepareFailed, drained.ErrorKind, drained.Error)
	}
	for _, refusal := range current.viewSwitchPreflight().Refusals {
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

func (v *viewRequestFS) checkViewSwitch(current *FS) error {
	if v.unmounted.Load() || current.unmounted.Load() {
		return errViewUnmounted
	}
	if v.currentTarget().fs != current {
		return errViewGenerationConflict
	}
	if result := current.viewSwitchPreflight(); !result.OK {
		return fmt.Errorf("mount view retains references or private extension state (%v): %w", result.Refusals, syscall.EBUSY)
	}
	return nil
}
