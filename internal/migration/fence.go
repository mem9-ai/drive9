package migration

import (
	"context"
	"errors"
	"fmt"
)

func (w *Worker) PrepareCutover(ctx context.Context) (Checkpoint, error) {
	if err := w.controlGate.Acquire(ctx); err != nil {
		return Checkpoint{}, err
	}
	defer w.controlGate.Release()
	return w.prepareCutoverLocked(ctx)
}

func (w *Worker) prepareCutoverLocked(ctx context.Context) (Checkpoint, error) {
	if w.fenceIntent.Load() {
		return w.completeFenceLocked(ctx)
	}
	snapshot := w.state.Snapshot()
	if w.recovery == nil || snapshot.Phase != PhaseDualWriteRepairing || !snapshot.Conditions.CurrentConverged || snapshot.Conditions.Attention || snapshot.Verification.Status != "passed" {
		return Checkpoint{}, ErrIllegalAction
	}
	w.writesFenced.Store(true)
	next := w.recovery.Record.Checkpoint
	next.FenceIntent = true
	record, err := w.checkpoint.Update(ctx, w.recovery.Record, next)
	if err != nil {
		observed, loadErr := w.checkpoint.Load(ctx, next.JobID)
		if loadErr == nil && sameCheckpointIdentity(observed.Checkpoint, next) && observed.Checkpoint.FenceComplete {
			return w.adoptCompletedFence(observed)
		}
		if loadErr == nil && sameCheckpointIdentity(observed.Checkpoint, next) && observed.Checkpoint.FenceIntent {
			w.recovery.Record, w.recovery.WritesAllowed = observed, false
			w.fenceIntent.Store(true)
		} else if loadErr == nil {
			w.writesFenced.Store(false)
		}
		return Checkpoint{}, err
	}
	w.recovery.Record, w.recovery.WritesAllowed = record, false
	w.fenceIntent.Store(true)
	return w.completeFenceLocked(ctx)
}

func (w *Worker) completeFenceLocked(ctx context.Context) (Checkpoint, error) {
	if w.fenceComplete.Load() {
		return w.recovery.Record.Checkpoint, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		next := w.recovery.Record.Checkpoint
		if !next.FenceIntent {
			return Checkpoint{}, errors.New("fence recovery lacks durable intent")
		}
		next.FenceComplete, next.HighestPhase = true, PhaseCutoverReady
		record, err := w.checkpoint.Update(ctx, w.recovery.Record, next)
		if err == nil {
			return w.adoptCompletedFence(record)
		}
		observed, loadErr := w.checkpoint.Load(ctx, next.JobID)
		if loadErr != nil {
			return Checkpoint{}, err
		}
		if !sameCheckpointIdentity(observed.Checkpoint, next) || !observed.Checkpoint.FenceIntent {
			return Checkpoint{}, fmt.Errorf("%w: fence completion identity", ErrCheckpointMismatch)
		}
		w.recovery.Record, w.recovery.WritesAllowed = observed, false
		w.fenceIntent.Store(true)
		if observed.Checkpoint.FenceComplete {
			return w.adoptCompletedFence(observed)
		}
		if attempt == 1 {
			return Checkpoint{}, err
		}
	}
	return Checkpoint{}, ErrCheckpointConflict
}

func (w *Worker) adoptCompletedFence(record CheckpointRecord) (Checkpoint, error) {
	w.recovery.Record = record
	w.recovery.WritesAllowed = false
	w.writesFenced.Store(true)
	w.fenceIntent.Store(true)
	w.fenceComplete.Store(true)
	w.state.mu.Lock()
	w.state.phase = PhaseCutoverReady
	w.state.recomputeLocked()
	w.state.mu.Unlock()
	return record.Checkpoint, nil
}
