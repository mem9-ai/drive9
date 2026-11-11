//go:build !windows

package fuse

import (
	"context"
	"errors"
	"sync"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// viewInstallation finishes one already-durable decision in the same live
// mount. It owns no authorization or runtime construction. Before constructing
// it, the switch owner must stop old background publishers, initialize target,
// and prove old references safe (or reject them before deciding). A recovered
// process must establish its kernel/runtime identity separately; this object
// does not transparently resume a dead FUSE mount.
type viewInstallation struct {
	mu        sync.Mutex
	journal   *mountstate.SwitchJournal
	adapter   *viewRequestFS
	pause     *viewPause
	decision  mountstate.SwitchReceipt
	target    gofuse.RawFileSystem
	plan      viewCacheInvalidation
	notifier  viewCacheNotifier
	fenceRoot func(context.Context) error // bound to the same live kernel connection
	installed bool
	completed *mountstate.SwitchReceipt
}

func newViewInstallation(journal *mountstate.SwitchJournal, adapter *viewRequestFS, pause *viewPause, target gofuse.RawFileSystem, plan viewCacheInvalidation, notifier viewCacheNotifier, fenceRoot func(context.Context) error) (*viewInstallation, error) {
	if journal == nil || adapter == nil || pause == nil || pause.gate != &adapter.admission || target == nil || notifier == nil || fenceRoot == nil {
		return nil, errors.New("incomplete mount view installation")
	}
	snapshot, err := journal.Snapshot()
	if err != nil {
		return nil, err
	}
	if snapshot.Pending == nil || adapter.currentTarget().generation != snapshot.Pending.FromGeneration {
		return nil, mountstate.ErrSwitchConflict
	}
	if len(plan.inodes) == 0 || plan.inodes[0] != gofuse.FUSE_ROOT_ID {
		return nil, errors.New("mount view installation missing root invalidation")
	}
	// Own the plan: retries must not use a changed or caller-mutated snapshot.
	plan.inodes = append([]uint64(nil), plan.inodes...)
	plan.entries = append([]viewCacheEntry(nil), plan.entries...)
	return &viewInstallation{journal: journal, adapter: adapter, pause: pause, decision: *snapshot.Pending,
		target: target, plan: plan, notifier: notifier, fenceRoot: fenceRoot}, nil
}

// finish keeps admission closed on every failure. Once installed, retries
// invalidate the same plan without installing another generation. Only a
// confirmed completion record permits reopening; old successful replays have
// no side effects. A nil receipt on error means the journal could not confirm
// this attempt's result, not that the decision was undone.
func (i *viewInstallation) finish(ctx context.Context) (*mountstate.SwitchReceipt, error) {
	if !i.mu.TryLock() {
		return nil, mountstate.ErrSwitchPending
	}
	defer i.mu.Unlock()
	if i.completed != nil {
		r := *i.completed
		return &r, nil
	}
	if err := ctx.Err(); err != nil {
		return i.pendingResult(err)
	}
	if err := i.fenceRoot(ctx); err != nil {
		return i.pendingResult(err)
	}
	if !i.installed {
		if err := i.adapter.installTarget(ctx, i.pause, i.decision.FromGeneration, i.decision.Generation, i.target); err != nil {
			return i.pendingResult(err)
		}
		i.installed = true
	}
	if err := i.plan.invalidate(ctx, i.notifier); err != nil {
		return i.pendingResult(err)
	}
	// Cleanup or OnUnmount may have started during notifications. Commit the
	// local completion and reopen under the same idle gate, so no callback
	// can interleave between these two steps. No kernel notification here.
	err := i.pause.resumeAtIdle(ctx, func() error {
		if i.adapter.unmounted.Load() {
			return errViewUnmounted
		}
		r, err := i.journal.Complete(i.decision.OperationID)
		if err == nil {
			i.completed = &r
		}
		return err
	})
	if err != nil {
		return i.pendingResult(err)
	}
	r := *i.completed
	return &r, nil
}

func (i *viewInstallation) pendingResult(cause error) (*mountstate.SwitchReceipt, error) {
	r, found, err := i.journal.Lookup(i.decision.OperationID)
	if err != nil {
		return nil, errors.Join(cause, err)
	}
	if !found {
		return nil, errors.Join(cause, mountstate.ErrSwitchConflict)
	}
	return &r, cause
}
