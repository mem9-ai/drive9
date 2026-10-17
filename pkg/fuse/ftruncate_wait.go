package fuse

import (
	"context"
	"syscall"
	"time"
)

func waitHandleRetry(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	interval := samePathDirtyWaitInterval
	if !deadline.IsZero() {
		if remaining := time.Until(deadline); remaining <= 0 {
			return syscall.EAGAIN
		} else if remaining < interval {
			interval = remaining
		}
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// On failure the caller owns no handle lock. The same deadline survives retries.
func lockHandleWithContext(ctx context.Context, fh *FileHandle, deadline time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return syscall.EAGAIN
		}
		if fh.TryLock() {
			if err := ctx.Err(); err != nil {
				fh.Unlock()
				return err
			}
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				fh.Unlock()
				return syscall.EAGAIN
			}
			return nil
		}
		if err := waitHandleRetry(ctx, deadline); err != nil {
			return err
		}
	}
}

// Per-call lock state crosses existing upload helpers; it owns no new mutex.
// Legacy Release keeps its old wait policy until a truncate obligation is seen.
type handleLockState struct {
	held, truncateOnly, participating bool
	deadline                          time.Time
}

func (s *handleLockState) lock(ctx context.Context, fh *FileHandle) error {
	for {
		s.participating = s.participating || fh.pendingFtruncate.Load() != nil || fh.deferredRelease.Load() != 0
		if !s.truncateOnly || s.participating {
			err := lockHandleWithContext(ctx, fh, s.deadline)
			s.held = err == nil
			return err
		}
		if fh.TryLock() {
			s.held = true
			return nil
		}
		_ = waitHandleRetry(context.Background(), time.Time{})
	}
}

func unlockHandleForIO(fh *FileHandle, state []*handleLockState) {
	if len(state) != 0 {
		state[0].held = false
	}
	fh.Unlock()
}

func relockHandleAfterIO(ctx context.Context, fh *FileHandle, state []*handleLockState) error {
	if len(state) != 0 {
		return state[0].lock(ctx, fh)
	}
	fh.Lock()
	return nil
}
