package backend

import (
	"context"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const expirySweepInterval = 5 * time.Minute

// ExpirySweepStore defines the operations needed by the expiry sweep worker.
// Implemented by *meta.Store.
type ExpirySweepStore interface {
	ExpireActiveReservations(ctx context.Context) (int64, error)
}

// ExpirySweepWorker periodically scans for upload reservations that have passed
// their expires_at deadline. For each expired reservation it releases the
// reserved_bytes counter and marks the reservation as aborted.
type ExpirySweepWorker struct {
	store  ExpirySweepStore
	cancel context.CancelFunc
	done   chan struct{}
}

// StartExpirySweepWorker starts the background expiry sweep loop.
// Returns nil if store is nil (central quota not wired).
func StartExpirySweepWorker(store ExpirySweepStore) *ExpirySweepWorker {
	if store == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &ExpirySweepWorker{
		store:  store,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go w.run(ctx)
	return w
}

// Stop gracefully shuts down the expiry sweep worker.
func (w *ExpirySweepWorker) Stop() {
	if w == nil {
		return
	}
	w.cancel()
	<-w.done
}

func (w *ExpirySweepWorker) run(ctx context.Context) {
	defer close(w.done)

	logger.Info(ctx, "expiry_sweep_worker_started")
	ticker := time.NewTicker(expirySweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "expiry_sweep_worker_stopped")
			return
		case <-ticker.C:
			if w.sweep(ctx) {
				logger.Info(ctx, "expiry_sweep_worker_stopped_fatal")
				return
			}
		}
	}
}

// sweep runs one expiry sweep cycle. Returns true if a fatal error occurred
// (e.g. database closed) and the loop should stop.
func (w *ExpirySweepWorker) sweep(ctx context.Context) (fatal bool) {
	start := time.Now()
	released, err := w.store.ExpireActiveReservations(ctx)
	elapsed := time.Since(start)
	if err != nil {
		if strings.Contains(err.Error(), "database is closed") || strings.Contains(err.Error(), "connection refused") {
			logger.Info(ctx, "expiry_sweep_worker_db_closed")
			return true
		}
		logger.Error(ctx, "expiry_sweep_error", zap.Error(err), zap.Duration("elapsed", elapsed))
		metrics.RecordOperation("expiry_sweep", "sweep", "error", elapsed)
		return false
	}
	if released > 0 {
		logger.Info(ctx, "expiry_sweep_released",
			zap.Int64("released_bytes", released),
			zap.Duration("elapsed", elapsed))
	}
	metrics.RecordOperation("expiry_sweep", "sweep", "ok", elapsed)
	return false
}
