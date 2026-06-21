package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
)

const (
	startupMetaRetryTotalTimeout   = 2 * time.Minute
	startupMetaRetryAttemptTimeout = 10 * time.Second
	startupMetaRetryInitialBackoff = time.Second
	startupMetaRetryMaxBackoff     = 15 * time.Second
)

type startupRetryOptions struct {
	TotalTimeout   time.Duration
	AttemptTimeout time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Sleep          func(context.Context, time.Duration) error
}

func defaultStartupRetryOptions() startupRetryOptions {
	return startupRetryOptions{
		TotalTimeout:   startupMetaRetryTotalTimeout,
		AttemptTimeout: startupMetaRetryAttemptTimeout,
		InitialBackoff: startupMetaRetryInitialBackoff,
		MaxBackoff:     startupMetaRetryMaxBackoff,
		Sleep:          sleepContext,
	}
}

func (o startupRetryOptions) withDefaults() startupRetryOptions {
	if o.TotalTimeout <= 0 {
		o.TotalTimeout = startupMetaRetryTotalTimeout
	}
	if o.AttemptTimeout <= 0 {
		o.AttemptTimeout = startupMetaRetryAttemptTimeout
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = startupMetaRetryInitialBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = startupMetaRetryMaxBackoff
	}
	if o.MaxBackoff < o.InitialBackoff {
		o.MaxBackoff = o.InitialBackoff
	}
	if o.Sleep == nil {
		o.Sleep = sleepContext
	}
	return o
}

func openControlPlaneStoreWithRetry(ctx context.Context, dsn string, opts startupRetryOptions) (*meta.Store, error) {
	var store *meta.Store
	err := retryStartupOperation(ctx, "open_control_plane_store", opts, func(attemptCtx context.Context) error {
		opened, err := meta.OpenContext(attemptCtx, dsn)
		if err != nil {
			return err
		}
		store = opened
		return nil
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func pingControlPlaneDBWithRetry(ctx context.Context, store *meta.Store, opts startupRetryOptions) error {
	return retryStartupOperation(ctx, "ping_control_plane_db", opts, func(attemptCtx context.Context) error {
		return store.DB().PingContext(attemptCtx)
	})
}

func retryStartupOperation(ctx context.Context, name string, opts startupRetryOptions, op func(context.Context) error) error {
	opts = opts.withDefaults()
	deadlineCtx, cancel := context.WithTimeout(ctx, opts.TotalTimeout)
	defer cancel()

	backoff := opts.InitialBackoff
	var lastErr error
	for attempt := 1; ; attempt++ {
		attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, opts.AttemptTimeout)
		err := op(attemptCtx)
		attemptCancel()
		if err == nil {
			if attempt > 1 {
				logger.Info(context.Background(), "startup_dependency_available",
					zap.String("dependency", name),
					zap.Int("attempt", attempt))
			}
			return nil
		}
		lastErr = err
		if deadlineCtx.Err() != nil {
			return fmt.Errorf("%s failed after %s: %w", name, opts.TotalTimeout, lastErr)
		}

		wait := backoff
		if deadline, ok := deadlineCtx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("%s failed after %s: %w", name, opts.TotalTimeout, lastErr)
			}
			if wait > remaining {
				wait = remaining
			}
		}
		logger.Warn(context.Background(), "startup_dependency_unavailable",
			zap.String("dependency", name),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", wait),
			zap.Duration("attempt_timeout", opts.AttemptTimeout),
			zap.Duration("total_timeout", opts.TotalTimeout),
			zap.Error(err))
		if err := opts.Sleep(deadlineCtx, wait); err != nil {
			return fmt.Errorf("%s failed after %s: %w", name, opts.TotalTimeout, lastErr)
		}
		if backoff < opts.MaxBackoff {
			backoff *= 2
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
