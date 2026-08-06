package migration

import (
	"context"
	"errors"
	"time"
)

var ErrVerificationFailed = errors.New("full verification failed")

func (w *Worker) VerifyFull(ctx context.Context) (VerificationState, error) {
	w.state.mu.Lock()
	current := w.state.verification
	if w.state.phase != PhaseDualWriteRepairing || !w.state.recoveryComplete || current.Status == "pending" || current.Status == "running" {
		w.state.mu.Unlock()
		return current, ErrIllegalAction
	}
	if current.Status == "passed" || current.Status == "failed" {
		w.state.mu.Unlock()
		if current.Status == "failed" {
			return current, ErrVerificationFailed
		}
		return current, nil
	}
	current = VerificationState{Status: "pending", RequestedAt: time.Now()}
	w.state.verification = current
	w.state.verification.Status = "running"
	w.state.mu.Unlock()
	err := w.Round(ctx, RoundModeVerification)
	snapshot := w.state.Snapshot()
	current.Status, current.CompletedAt = "passed", time.Now()
	if snapshot.LastComplete != nil {
		current.SourceCount = int64(len(snapshot.LastComplete.Source))
		for _, finding := range snapshot.LastComplete.Findings {
			if finding.Severity == SeverityBlocker {
				current.MismatchCount++
			}
		}
	}
	if err != nil || !snapshot.Current.Converged || snapshot.Conditions.Attention || len(snapshot.Grace)+len(snapshot.Retry) > 0 {
		current.Status = "failed"
	}
	w.state.mu.Lock()
	w.state.verification = current
	w.state.mu.Unlock()
	if err != nil {
		return current, err
	}
	if current.Status == "failed" {
		return current, ErrVerificationFailed
	}
	return current, nil
}
