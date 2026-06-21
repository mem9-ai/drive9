package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryStartupOperationSucceedsAfterFailures(t *testing.T) {
	temporaryErr := errors.New("temporary metadb failure")
	var waits []time.Duration
	attempts := 0
	err := retryStartupOperation(context.Background(), "metadb", startupRetryOptions{
		TotalTimeout:   time.Second,
		AttemptTimeout: 50 * time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Sleep: func(_ context.Context, d time.Duration) error {
			waits = append(waits, d)
			return nil
		},
	}, func(ctx context.Context) error {
		attempts++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("expected per-attempt context deadline")
		}
		if attempts < 3 {
			return temporaryErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryStartupOperation: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(waits) != 2 || waits[0] != time.Millisecond || waits[1] != 2*time.Millisecond {
		t.Fatalf("waits = %v, want [1ms 2ms]", waits)
	}
}

func TestRetryStartupOperationStopsAfterTotalTimeout(t *testing.T) {
	temporaryErr := errors.New("metadb down")
	attempts := 0
	err := retryStartupOperation(context.Background(), "metadb", startupRetryOptions{
		TotalTimeout:   10 * time.Millisecond,
		AttemptTimeout: time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	}, func(context.Context) error {
		attempts++
		return temporaryErr
	})
	if !errors.Is(err, temporaryErr) {
		t.Fatalf("err = %v, want wrapped temporary error", err)
	}
	if attempts == 0 {
		t.Fatal("expected at least one attempt")
	}
}

func TestRetryStartupOperationHonorsParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := retryStartupOperation(ctx, "metadb", startupRetryOptions{
		TotalTimeout:   time.Second,
		AttemptTimeout: time.Second,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		Sleep: func(context.Context, time.Duration) error {
			t.Fatal("sleep should not run after canceled parent context")
			return nil
		},
	}, func(ctx context.Context) error {
		attempts++
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRetryStartupOperationPassesAttemptTimeout(t *testing.T) {
	const attemptTimeout = 25 * time.Millisecond
	err := retryStartupOperation(context.Background(), "metadb", startupRetryOptions{
		TotalTimeout:   time.Second,
		AttemptTimeout: attemptTimeout,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	}, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected per-attempt deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > attemptTimeout {
			t.Fatalf("remaining attempt timeout = %s, want within (0, %s]", remaining, attemptTimeout)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryStartupOperation: %v", err)
	}
}
