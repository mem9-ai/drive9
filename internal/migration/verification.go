package migration

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrVerificationFailed  = errors.New("full verification failed")
	errVerificationPending = errors.New("full verification has pending repair work")
)

func (w *Worker) VerifyFull(ctx context.Context) (VerificationState, error) {
	if err := w.controlGate.Acquire(ctx); err != nil {
		return VerificationState{}, err
	}
	defer w.controlGate.Release()
	return w.verifyFullLocked(ctx)
}

func (w *Worker) verifyFullLocked(ctx context.Context) (VerificationState, error) {
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
	current = VerificationState{Status: "pending", RequestedAt: w.clock()}
	w.state.verification = current
	w.state.verification.Status = "running"
	w.state.mu.Unlock()

	attempt := 0
	var blockedAt time.Time
	for {
		err := w.Round(ctx, RoundModeVerification)
		snapshot := w.state.Snapshot()
		if err == nil && snapshot.Current.ScanComplete && len(snapshot.Grace)+len(snapshot.Retry) == 0 {
			return w.finishVerification(current, snapshot)
		}
		if err == nil {
			err = errVerificationPending
		}
		if ctx.Err() != nil {
			return w.resetVerification(), ctx.Err()
		}
		if isAuthError(err) {
			if refreshErr := w.refreshClientLocked(ctx); refreshErr != nil {
				w.state.SetAttention(true)
				return w.resetVerification(), fmt.Errorf("refresh verification credential: %w", refreshErr)
			}
		} else if !verificationRetryable(err) {
			w.state.SetAttention(true)
			return w.resetVerification(), err
		}
		if errors.Is(err, errVerificationPending) {
			blockedAt = time.Time{}
		} else {
			if blockedAt.IsZero() {
				blockedAt = w.clock()
			} else if w.clock().Sub(blockedAt) >= attentionAfter {
				w.state.SetAttention(true)
			}
		}
		if err := w.waitForRetry(ctx, retryDelay(attempt, maxRetryDelay)); err != nil {
			return w.resetVerification(), err
		}
		attempt++
	}
}

func (w *Worker) finishVerification(current VerificationState, snapshot StateSnapshot) (VerificationState, error) {
	current.Status, current.CompletedAt = "passed", w.clock()
	if snapshot.LastComplete != nil {
		current.SourceCount = int64(len(snapshot.LastComplete.Source))
		for _, finding := range snapshot.LastComplete.Findings {
			if finding.Severity == SeverityBlocker {
				current.MismatchCount++
			}
		}
	}
	if !snapshot.Current.Converged || snapshot.Conditions.Attention {
		current.Status = "failed"
	}
	w.state.mu.Lock()
	w.state.verification = current
	w.state.mu.Unlock()
	if current.Status == "failed" {
		return current, ErrVerificationFailed
	}
	return current, nil
}

func (w *Worker) resetVerification() VerificationState {
	w.state.mu.Lock()
	w.state.verification = VerificationState{}
	w.state.mu.Unlock()
	return VerificationState{}
}

func verificationRetryable(err error) bool {
	return errors.Is(err, errVerificationPending) || retryableWorkerError(err)
}
