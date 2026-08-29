package migration

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestAdaptiveLimitReducesOnBackpressureAndRecoversAfterHealthyWindow(t *testing.T) {
	now := time.Unix(100, 0)
	limit, err := newAdaptiveLimit(8, now)
	if err != nil {
		t.Fatal(err)
	}
	if limit.Current() != 4 {
		t.Fatalf("initial = %d", limit.Current())
	}
	if !limit.OnFailure(&client.StatusError{StatusCode: http.StatusTooManyRequests}, now) || limit.Current() != 2 {
		t.Fatalf("after 429 = %d", limit.Current())
	}
	if !limit.OnFailure(context.DeadlineExceeded, now) || limit.Current() != 1 {
		t.Fatalf("after timeout = %d", limit.Current())
	}
	limit.OnSuccess(now.Add(29 * time.Second))
	if limit.Current() != 1 {
		t.Fatalf("early recovery = %d", limit.Current())
	}
	limit.OnSuccess(now.Add(30 * time.Second))
	if limit.Current() != 2 {
		t.Fatalf("healthy recovery = %d", limit.Current())
	}
}

func TestAdaptiveLimitDoesNotTreatContractAuthOrCASAsBackpressure(t *testing.T) {
	limit, err := newAdaptiveLimit(8, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		&client.StatusError{StatusCode: http.StatusUnauthorized},
		&client.StatusError{StatusCode: http.StatusForbidden},
		&client.StatusError{StatusCode: http.StatusConflict},
		ErrApplyVerification,
	} {
		if limit.OnFailure(err, time.Unix(101, 0)) {
			t.Fatalf("error %v classified as backpressure", err)
		}
	}
	if limit.Current() != 4 {
		t.Fatalf("limit = %d", limit.Current())
	}
}

func TestWorkerRetryJitterIsInjectedAndBounded(t *testing.T) {
	worker := &Worker{retryJitter: func(delay time.Duration) time.Duration { return delay / 2 }}
	if got := worker.nextRetryDelay(3, time.Second); got != 400*time.Millisecond {
		t.Fatalf("injected delay=%s, want 400ms", got)
	}
	worker.retryJitter = func(delay time.Duration) time.Duration { return delay * 2 }
	if got := worker.nextRetryDelay(3, time.Second); got != 800*time.Millisecond {
		t.Fatalf("upper-clamped delay=%s, want 800ms", got)
	}
	worker.retryJitter = func(time.Duration) time.Duration { return 0 }
	if got := worker.nextRetryDelay(3, time.Second); got != 400*time.Millisecond {
		t.Fatalf("lower-clamped delay=%s, want 400ms", got)
	}

	worker.retryJitter = randomRetryJitter
	for attempt := range 20 {
		base := retryDelay(attempt, time.Second)
		got := worker.nextRetryDelay(attempt, time.Second)
		if got < base/2 || got > base {
			t.Fatalf("attempt=%d jittered delay=%s outside [%s,%s]", attempt, got, base/2, base)
		}
	}
}

func TestWorkerBackoffStatusUpdatesAndClears(t *testing.T) {
	worker := &Worker{}
	worker.largeProgress.Store(&GenerationStatus{Stage: "apply_files"})
	worker.setLargeProgress(*worker.largeProgress.Load())
	lastProgress := worker.largeProgress.Load().LastProgressAt
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	worker.retryWait = func(ctx context.Context, delay time.Duration) error {
		once.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	done := make(chan error, 1)
	go func() { done <- worker.waitForRetry(context.Background(), 2*time.Second) }()
	<-started
	status := worker.largeProgress.Load()
	if status == nil || status.RetryDelayMS != 2000 || status.BackoffUntil.IsZero() || status.Stage != "apply_files" {
		t.Fatalf("backoff status=%+v", status)
	}
	if !status.LastProgressAt.Equal(lastProgress) || status.LastStatusAt.Before(lastProgress) {
		t.Fatalf("backoff changed successful progress timestamp: before=%s status=%+v", lastProgress, status)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	status = worker.largeProgress.Load()
	if status.RetryDelayMS != 0 || !status.BackoffUntil.IsZero() {
		t.Fatalf("completed backoff status=%+v", status)
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker.retryWait = nil
	done = make(chan error, 1)
	go func() { done <- worker.waitForRetry(ctx, time.Second) }()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled backoff succeeded")
	}
	status = worker.largeProgress.Load()
	if status.RetryDelayMS != 0 || !status.BackoffUntil.IsZero() {
		t.Fatalf("canceled backoff status=%+v", status)
	}
}
