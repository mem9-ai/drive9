package fuse

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingleFlightBasicDedup(t *testing.T) {
	sf := NewSingleFlight()

	var calls atomic.Int32
	var wg sync.WaitGroup
	const n = 10

	// All goroutines request the same key concurrently.
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
			results[idx], errs[idx], owners[idx] = sf.Do("key1", func() ([]byte, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond) // simulate slow fetch
				return []byte("hello"), nil
			})
		}(i)
	}
	close(ready) // release all goroutines
	wg.Wait()

	// The function should have been called exactly once.
	require.Equal(t, int32(1), calls.Load(), "fn should be called exactly once")

	// Exactly one goroutine should be the owner.
	ownerCount := 0
	for _, o := range owners {
		if o {
			ownerCount++
		}
	}
	require.Equal(t, 1, ownerCount, "exactly one goroutine should be the owner")

	// All goroutines should get the same result.
	for i := 0; i < n; i++ {
		require.Equal(t, []byte("hello"), results[i])
		require.NoError(t, errs[i])
	}

	// No inflight calls remaining.
	require.Equal(t, 0, sf.Inflight())
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
			results[idx], errs[idx], _ = sf.Do("err-key", func() ([]byte, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return nil, testErr
			})
		}(i)
	}
	close(ready)
	wg.Wait()

	require.Equal(t, int32(1), calls.Load())
	for i := 0; i < n; i++ {
		require.Nil(t, results[i])
		require.ErrorIs(t, errs[i], testErr)
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
			sf.Do("A", func() ([]byte, error) {
				callsA.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("a"), nil
			})
		}()
		go func() {
			defer wg.Done()
			<-ready
			sf.Do("B", func() ([]byte, error) {
				callsB.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("b"), nil
			})
		}()
	}
	close(ready)
	wg.Wait()

	// Each key should have exactly one call.
	require.Equal(t, int32(1), callsA.Load(), "key A fn called once")
	require.Equal(t, int32(1), callsB.Load(), "key B fn called once")
}

func TestSingleFlightSequentialCallsSameKey(t *testing.T) {
	sf := NewSingleFlight()
	var calls atomic.Int32

	// First call.
	data1, err1, owner1 := sf.Do("k", func() ([]byte, error) {
		calls.Add(1)
		return []byte("v1"), nil
	})
	require.NoError(t, err1)
	require.Equal(t, []byte("v1"), data1)
	require.True(t, owner1)

	// Second sequential call — should execute fn again (no dedup for non-concurrent calls).
	data2, err2, owner2 := sf.Do("k", func() ([]byte, error) {
		calls.Add(1)
		return []byte("v2"), nil
	})
	require.NoError(t, err2)
	require.Equal(t, []byte("v2"), data2)
	require.True(t, owner2)

	require.Equal(t, int32(2), calls.Load(), "sequential calls should both execute")
}

func TestSingleFlightInflight(t *testing.T) {
	sf := NewSingleFlight()
	require.Equal(t, 0, sf.Inflight())

	started := make(chan struct{})
	release := make(chan struct{})

	go func() {
		sf.Do("inflight-key", func() ([]byte, error) {
			close(started)
			<-release
			return []byte("done"), nil
		})
	}()

	<-started
	require.Equal(t, 1, sf.Inflight())
	close(release)

	// Wait for completion.
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, sf.Inflight())
}

func TestSingleFlightNilData(t *testing.T) {
	sf := NewSingleFlight()

	data, err, owner := sf.Do("nil-key", func() ([]byte, error) {
		return nil, nil
	})
	require.NoError(t, err)
	require.Nil(t, data)
	require.True(t, owner)
}

func TestSingleFlightConcurrentDifferentKeysDoNotBlock(t *testing.T) {
	sf := NewSingleFlight()

	// Key A blocks indefinitely. Key B should complete independently.
	blockA := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sf.Do("block-A", func() ([]byte, error) {
			<-blockA
			return []byte("a"), nil
		})
	}()

	// Give goroutine A time to start.
	time.Sleep(10 * time.Millisecond)

	// Key B should not be blocked by key A.
	done := make(chan struct{})
	go func() {
		sf.Do("free-B", func() ([]byte, error) {
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
			sf.Do("file.txt@1", func() ([]byte, error) {
				callsR1.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("data-rev1"), nil
			})
		}()
		go func() {
			defer wg.Done()
			<-ready
			sf.Do("file.txt@2", func() ([]byte, error) {
				callsR2.Add(1)
				time.Sleep(30 * time.Millisecond)
				return []byte("data-rev2"), nil
			})
		}()
	}
	close(ready)
	wg.Wait()

	// Each revision should trigger exactly one fetch.
	require.Equal(t, int32(1), callsR1.Load(), "rev1 fn called once")
	require.Equal(t, int32(1), callsR2.Load(), "rev2 fn called once")
}
