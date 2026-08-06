package migration

import (
	"context"
	"errors"
)

func (w *Worker) PrepareCutover(ctx context.Context) (Checkpoint, error) {
	if w.fenceIntent.Load() {
		return w.completeFence(ctx)
	}
	snapshot := w.state.Snapshot()
	if w.recovery == nil || snapshot.Phase != PhaseDualWriteRepairing || !snapshot.Conditions.CurrentConverged || snapshot.Conditions.Attention || snapshot.Verification.Status != "passed" || snapshot.Current.CompletedAt.After(snapshot.Verification.CompletedAt) {
		return Checkpoint{}, ErrIllegalAction
	}
	w.writesFenced.Store(true)
	next := w.recovery.Record.Checkpoint
	next.FenceIntent = true
	record, err := w.checkpoint.Update(ctx, w.recovery.Record, next)
	if err != nil {
		observed, loadErr := w.checkpoint.Load(ctx, next.JobID)
		if loadErr == nil && observed.Checkpoint.FenceIntent {
			w.recovery.Record, w.recovery.WritesAllowed = observed, false
			w.fenceIntent.Store(true)
		} else if loadErr == nil {
			w.writesFenced.Store(false)
		}
		return Checkpoint{}, err
	}
	w.recovery.Record, w.recovery.WritesAllowed = record, false
	w.fenceIntent.Store(true)
	return w.completeFence(ctx)
}

func (w *Worker) completeFence(ctx context.Context) (Checkpoint, error) {
	if w.fenceComplete.Load() {
		return w.recovery.Record.Checkpoint, nil
	}
	next := w.recovery.Record.Checkpoint
	if !next.FenceIntent {
		return Checkpoint{}, errors.New("fence recovery lacks durable intent")
	}
	next.FenceComplete, next.HighestPhase = true, PhaseCutoverReady
	record, err := w.checkpoint.Update(ctx, w.recovery.Record, next)
	if err != nil {
		return Checkpoint{}, err
	}
	w.recovery.Record = record
	w.fenceComplete.Store(true)
	w.state.mu.Lock()
	w.state.phase = PhaseCutoverReady
	w.state.recomputeLocked()
	w.state.mu.Unlock()
	return record.Checkpoint, nil
}
