package fuse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleFlightBasicDedup(t *testing.T) {
	sf := NewSingleFlight()

	var calls atomic.Int32
	var wg sync.WaitGroup
	const n = 10

	results := make([][]byte, n)
	errs := make([]error, n)
	owners := make([]bool, n)

	// Use a channel to ensure all goroutines are launched before any proceeds.
	ready := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-ready
			results[idx], errs[idx], owners[idx] = sf.Do(context.Background(), "key1", func() ([]byte, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond) // simulate slow fetch
				return []byte("hello"), nil
			})
		}(i)
	}
	close(ready) // release all goroutines
	wg.Wait()

	// The function should have been called exactly once.
	if got := calls.Load(); got != 1 {
		t.Fatalf("fn called %d times, want 1", got)
	}

	// Exactly one goroutine should be the owner.
	ownerCount := 0
	for _, o := range owners {
		if o {
			ownerCount++
		}
	}
	if ownerCount != 1 {
		t.Fatalf("owner count = %d, want 1", ownerCount)
	}

	// All goroutines should get the same result.
	for i := 0; i < n; i++ {
		if string(results[i]) != "hello" {
			t.Errorf("result[%d] = %q, want %q", i, results[i], "hello")
		}
		if errs[i] != nil {
			t.Errorf("err[%d] = %v, want nil", i, errs[i])
		}
	}

	// No inflight calls remaining.
	if got := sf.Inflight(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}
}

func TestSingleFlightErrorPropagated(t *testing.T) {
	sf := NewSingleFlight()
	testErr := errors.New("fetch failed")

	var calls atomic.Int32
	var wg sync.WaitGroup
	const n = 5

	ready := make(chan struct{})
	results := make([][]byte, n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-ready
			results[idx], errs[idx], _ = sf.Do(context.Background(), "err-key", func() ([]byte, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return nil, testErr
			})
		}(i)
	}
	close(ready)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("fn called %d times, want 1", got)
	}
	for i := 0; i < n; i++ {
		if results[i] != nil {
			t.Errorf("result[%d] = %v, want nil", i, results[i])
		}
		if !errors.Is(errs[i], testErr) {
			t.Errorf("err[%d] = %v, want %v", i, errs[i], testErr)
		}
	}
}

func TestSingleFlightDifferentKeysParallel(t *testing.T) {
	sf := NewSingleFlight()
	var callsA, callsB atomic.Int32

	var wg sync.WaitGroup
	ready := make(chan struct{})

	// 5 goroutines for key A, 5 for key B.
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-ready
			_, _, _ = sf.Do(context.Background(), "A", func() ([]byte, error) {
				callsA.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("a"), nil
			})
		}()
		go func() {
			defer wg.Done()
			<-ready
			_, _, _ = sf.Do(context.Background(), "B", func() ([]byte, error) {
				callsB.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("b"), nil
			})
		}()
	}
	close(ready)
	wg.Wait()

	// Each key should have exactly one call.
	if got := callsA.Load(); got != 1 {
		t.Fatalf("key A fn called %d times, want 1", got)
	}
	if got := callsB.Load(); got != 1 {
		t.Fatalf("key B fn called %d times, want 1", got)
	}
}

func TestSingleFlightSequentialCallsSameKey(t *testing.T) {
	sf := NewSingleFlight()
	var calls atomic.Int32

	// First call.
	data1, err1, owner1 := sf.Do(context.Background(), "k", func() ([]byte, error) {
		calls.Add(1)
		return []byte("v1"), nil
	})
	if err1 != nil {
		t.Fatalf("call 1 err = %v, want nil", err1)
	}
	if string(data1) != "v1" {
		t.Fatalf("call 1 data = %q, want %q", data1, "v1")
	}
	if !owner1 {
		t.Fatalf("call 1 owner = false, want true")
	}

	// Second sequential call — should execute fn again (no dedup for non-concurrent calls).
	data2, err2, owner2 := sf.Do(context.Background(), "k", func() ([]byte, error) {
		calls.Add(1)
		return []byte("v2"), nil
	})
	if err2 != nil {
		t.Fatalf("call 2 err = %v, want nil", err2)
	}
	if string(data2) != "v2" {
		t.Fatalf("call 2 data = %q, want %q", data2, "v2")
	}
	if !owner2 {
		t.Fatalf("call 2 owner = false, want true")
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("fn called %d times, want 2 (sequential calls should both execute)", got)
	}
}

func TestSingleFlightInflight(t *testing.T) {
	sf := NewSingleFlight()
	if got := sf.Inflight(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}

	started := make(chan struct{})
	release := make(chan struct{})

	go func() {
		sf.Do(context.Background(), "inflight-key", func() ([]byte, error) {
			close(started)
			<-release
			return []byte("done"), nil
		})
	}()

	<-started
	if got := sf.Inflight(); got != 1 {
		t.Fatalf("inflight = %d, want 1", got)
	}
	close(release)

	// Wait for completion using a channel-based poll instead of time.Sleep.
	deadline := time.After(2 * time.Second)
	for {
		if sf.Inflight() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for inflight to reach 0")
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func TestSingleFlightNilData(t *testing.T) {
	sf := NewSingleFlight()

	data, err, owner := sf.Do(context.Background(), "nil-key", func() ([]byte, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if data != nil {
		t.Fatalf("data = %v, want nil", data)
	}
	if !owner {
		t.Fatalf("owner = false, want true")
	}
}

func TestSingleFlightConcurrentDifferentKeysDoNotBlock(t *testing.T) {
	sf := NewSingleFlight()

	// Key A blocks indefinitely. Key B should complete independently.
	blockA := make(chan struct{})
	startedA := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sf.Do(context.Background(), "block-A", func() ([]byte, error) {
			close(startedA)
			<-blockA
			return []byte("a"), nil
		})
	}()

	// Wait for goroutine A to actually start its fn.
	<-startedA

	// Key B should not be blocked by key A.
	done := make(chan struct{})
	go func() {
		sf.Do(context.Background(), "free-B", func() ([]byte, error) {
			return []byte("b"), nil
		})
		close(done)
	}()

	select {
	case <-done:
		// success — B completed while A is still blocked
	case <-time.After(1 * time.Second):
		t.Fatal("key B should not be blocked by key A")
	}

	close(blockA)
	wg.Wait()
}

func TestSingleFlightSamePathDifferentRevisionNotShared(t *testing.T) {
	// Regression: when a file's revision changes, concurrent reads for the
	// same path but different revisions must NOT share results. The
	// singleflight key in Dat9FS.Read includes the revision, so
	// "file.txt@1" and "file.txt@2" are independent keys.
	sf := NewSingleFlight()

	var callsR1, callsR2 atomic.Int32
	var wg sync.WaitGroup
	ready := make(chan struct{})

	// 3 goroutines for path@rev1, 3 for path@rev2.
	for i := 0; i < 3; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-ready
			_, _, _ = sf.Do(context.Background(), "file.txt@1", func() ([]byte, error) {
				callsR1.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("data-rev1"), nil
			})
		}()
		go func() {
			defer wg.Done()
			<-ready
			_, _, _ = sf.Do(context.Background(), "file.txt@2", func() ([]byte, error) {
				callsR2.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("data-rev2"), nil
			})
		}()
	}
	close(ready)
	wg.Wait()

	// Each revision should trigger exactly one fetch.
	if got := callsR1.Load(); got != 1 {
		t.Fatalf("rev1 fn called %d times, want 1", got)
	}
	if got := callsR2.Load(); got != 1 {
		t.Fatalf("rev2 fn called %d times, want 1", got)
	}
}

func TestSingleFlightContextCancelPiggybacker(t *testing.T) {
	// When a piggybacker's context is cancelled before the owner finishes,
	// the piggybacker should return ctx.Err() immediately. The owner
	// should still complete successfully.
	sf := NewSingleFlight()

	ownerStarted := make(chan struct{})
	ownerRelease := make(chan struct{})

	// Start owner goroutine with a long-running fn.
	var ownerData []byte
	var ownerErr error
	var ownerIsOwner bool
	var ownerWg sync.WaitGroup
	ownerWg.Add(1)
	go func() {
		defer ownerWg.Done()
		ownerData, ownerErr, ownerIsOwner = sf.Do(context.Background(), "cancel-key", func() ([]byte, error) {
			close(ownerStarted)
			<-ownerRelease
			return []byte("result"), nil
		})
	}()

	<-ownerStarted

	// Start piggybacker with a cancellable context.
	ctx, cancel := context.WithCancel(context.Background())
	piggyDone := make(chan struct{})
	var piggyData []byte
	var piggyErr error
	var piggyIsOwner bool
	go func() {
		piggyData, piggyErr, piggyIsOwner = sf.Do(ctx, "cancel-key", func() ([]byte, error) {
			t.Error("piggybacker fn should not be called")
			return nil, nil
		})
		close(piggyDone)
	}()

	// Give the piggybacker time to enter Do and block on the owner.
	time.Sleep(10 * time.Millisecond)

	// Cancel the piggybacker's context.
	cancel()

	select {
	case <-piggyDone:
		// Piggybacker returned.
	case <-time.After(1 * time.Second):
		t.Fatal("piggybacker did not return after context cancellation")
	}

	if piggyIsOwner {
		t.Error("piggybacker should not be owner")
	}
	if !errors.Is(piggyErr, context.Canceled) {
		t.Errorf("piggybacker err = %v, want context.Canceled", piggyErr)
	}
	if piggyData != nil {
		t.Errorf("piggybacker data = %v, want nil", piggyData)
	}

	// Let owner finish.
	close(ownerRelease)
	ownerWg.Wait()

	if !ownerIsOwner {
		t.Error("owner should be owner")
	}
	if ownerErr != nil {
		t.Errorf("owner err = %v, want nil", ownerErr)
	}
	if string(ownerData) != "result" {
		t.Errorf("owner data = %q, want %q", ownerData, "result")
	}
}

func TestSingleFlightContextCancelOwnerNotAffected(t *testing.T) {
	// Even if all piggybackers cancel, the owner call runs to completion.
	sf := NewSingleFlight()

	ownerStarted := make(chan struct{})
	ownerRelease := make(chan struct{})
	var calls atomic.Int32

	var wg sync.WaitGroup
	wg.Add(1)

	// Owner goroutine.
	go func() {
		defer wg.Done()
		sf.Do(context.Background(), "owner-key", func() ([]byte, error) {
			calls.Add(1)
			close(ownerStarted)
			<-ownerRelease
			return []byte("done"), nil
		})
	}()

	<-ownerStarted

	// Launch 3 piggybackers, all with contexts that will be cancelled.
	const nPiggy = 3
	piggyCtxs := make([]context.CancelFunc, nPiggy)
	piggyDone := make(chan struct{}, nPiggy)
	for i := 0; i < nPiggy; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		piggyCtxs[i] = cancel
		go func() {
			sf.Do(ctx, "owner-key", func() ([]byte, error) {
				t.Error("piggybacker fn should not be called")
				return nil, nil
			})
			piggyDone <- struct{}{}
		}()
	}

	// Give piggybackers time to block.
	time.Sleep(10 * time.Millisecond)

	// Cancel all piggybackers.
	for _, cancel := range piggyCtxs {
		cancel()
	}

	// Wait for all piggybackers to return.
	for i := 0; i < nPiggy; i++ {
		select {
		case <-piggyDone:
		case <-time.After(1 * time.Second):
			t.Fatal("piggybacker did not return after context cancellation")
		}
	}

	// Owner should still be inflight.
	if got := sf.Inflight(); got != 1 {
		t.Fatalf("inflight = %d, want 1 (owner should still be running)", got)
	}

	// Release owner.
	close(ownerRelease)
	wg.Wait()

	// fn called exactly once — owner ran to completion despite piggy cancels.
	if got := calls.Load(); got != 1 {
		t.Fatalf("fn called %d times, want 1", got)
	}
}
