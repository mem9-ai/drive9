package fuse

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/pingcap/failpoint"
)

func (fs *Dat9FS) applyPendingModeForCommitLocked(ctx context.Context, fh *FileHandle, state ...*handleLockState) error {
	if len(state) == 0 {
		return fs.applyPendingModeWithTimeoutLocked(fh)
	}
	return fs.applyPendingModeBudgetedLocked(ctx, fh, state[0])
}

// Keep the owner until both remote metadata and matching sibling generations
// are acknowledged. A failed relock leaves state.held=false for the caller.
func (fs *Dat9FS) applyPendingModeBudgetedLocked(ctx context.Context, fh *FileHandle, state *handleLockState) error {
	mode, hasMode := fs.modeForPendingHandle(fh)
	if !hasMode || fh.Unlinked {
		return nil
	}
	path, ino, generation := fh.Path, fh.Ino, fh.PendingModeGen
	pending := fh.HasPendingMode
	previous, hasPrevious, previousKnown := fh.PreviousMode, fh.HasPreviousMode, fh.PreviousModeKnown
	if fs.ftruncateParticipates(fh) || fh.deferredRelease.Load() != 0 {
		if !fs.ftruncateAliasLinked(fh) {
			return syscall.EAGAIN
		}
		for {
			err := fs.fenceFtruncateLocked(fh)
			if !errors.Is(err, errFtruncateBusy) {
				if err != nil {
					return err
				}
				break
			}
			unlockHandleForIO(fh, []*handleLockState{state})
			failpoint.InjectCall("ftruncateModeWait", fs, fh)
			if err := waitHandleRetry(ctx, state.deadline); err != nil {
				return err
			}
			if err := state.lock(ctx, fh); err != nil {
				return err
			}
			if fh.Path != path || fh.Ino != ino || fh.Unlinked {
				return syscall.EAGAIN
			}
			if pending && !pendingModeMatchesLocked(fh, mode, generation) {
				return nil
			}
		}
	}
	unlock := func() {}
	if fh.RemoteCommitUnlock != nil {
		unlock = fs.takeHandleRemoteCommitPathLocked(fh)
	}
	apply := shouldApplyRemoteMode(fs.pendingKindForHandle(fh), hasMode, mode)
	unlockHandleForIO(fh, []*handleLockState{state})
	var err error
	if apply {
		err = retryPostUploadMode(ctx, func() error { return fs.applyRemoteMode(ctx, path, mode) })
	}
	unlock()
	failpoint.InjectCall("ftruncateModeBeforeRelock", fs, fh)

	if lockErr := state.lock(ctx, fh); lockErr != nil {
		return lockErr
	}
	if err != nil {
		if !pendingModeMatchesLocked(fh, mode, generation) {
			return nil
		}
		if !state.participating && hasPrevious {
			fs.inodes.SetModeState(ino, previous, previousKnown)
		}
		return err
	}
	if fh.Path != path || fh.Ino != ino || fh.Unlinked {
		return syscall.EAGAIN
	}
	return fs.finishPendingModeForHandleLocked(ctx, fh, mode, generation, state)
}

func (fs *Dat9FS) clearPendingModeGenerationWithContext(ctx context.Context, ino uint64, skip *FileHandle, mode uint32, generation uint64, deadline time.Time) error {
	for _, sibling := range fs.fileHandlesForInode(ino) {
		if sibling == skip {
			continue
		}
		if err := lockHandleWithContext(ctx, sibling, deadline); err != nil {
			return err
		}
		if pendingModeMatchesLocked(sibling, mode, generation) {
			clearPendingModeLocked(sibling)
		}
		sibling.Unlock()
	}
	return nil
}
