package fuse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type initialSyncRangeGate struct {
	offset   int64
	release  chan struct{}
	canceled chan struct{}
	once     sync.Once
	data     []byte
}

func (g *initialSyncRangeGate) respond(data string) {
	g.once.Do(func() {
		g.data = []byte(data)
		close(g.release)
	})
}

type initialSyncRangeOutcome struct {
	done chan struct{}
	data []byte
	err  error
}

type initialSyncRangeFixture struct {
	fs       *Dat9FS
	entry    *InodeEntry
	handle   *FileHandle
	handleID uint64
	started  chan *initialSyncRangeGate
	heads    atomic.Int32
	reads    atomic.Int32
	outcomes []*initialSyncRangeOutcome
}

func newInitialSyncRangeFixture(t *testing.T) *initialSyncRangeFixture {
	t.Helper()
	f := &initialSyncRangeFixture{started: make(chan *initialSyncRangeGate, 8)}
	abort, stop := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			f.heads.Add(1)
			w.Header().Set("Content-Length", "8")
			w.Header().Set("X-Dat9-Revision", "1")
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/object" {
			w.Header().Set("Location", f.fs.client.BaseURL()+"/object")
			w.WriteHeader(http.StatusFound)
			return
		}
		start, end, ok := parseTestBytesRange(r.Header.Get("Range"))
		if !ok || end-start+1 != 4 || (start != 0 && start != 4) {
			t.Errorf("unexpected range %q", r.Header.Get("Range"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.reads.Add(1)
		gate := &initialSyncRangeGate{offset: start, release: make(chan struct{}), canceled: make(chan struct{})}
		select {
		case f.started <- gate:
		case <-abort.Done():
			return
		}
		select {
		case <-gate.release:
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/8", start, end))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(gate.data)
		case <-r.Context().Done():
			close(gate.canceled)
		case <-abort.Done():
		}
	}))
	opts := &MountOptions{TrustLocalEvents: true, ReadCacheMaxFileBytes: 4, ParallelReadBlockSize: 4, ParallelReadConcurrency: 2, ReadConcurrency: 2}
	opts.setDefaults()
	f.fs = NewDat9FS(newTestClient(server.URL), opts)
	f.fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	ino := f.fs.inodes.Lookup("/file.bin", false, 8, time.Unix(1, 0))
	f.fs.inodes.UpdateRevision(ino, 1)
	f.entry = mustGetInodeEntry(t, f.fs, ino)
	f.handle = &FileHandle{Ino: ino, Path: "/file.bin", BaseRev: 1}
	f.handleID = f.fs.allocateFileHandle(f.handle)
	t.Cleanup(func() {
		stop() // Unblock every request even when a gate assertion fails.
		for _, outcome := range f.outcomes {
			select {
			case <-outcome.done:
			case <-time.After(5 * time.Second):
				t.Error("gated read did not drain during cleanup")
			}
		}
		f.fs.fileHandles.Delete(f.handleID)
		f.fs.directoryPrefetch.shutdown()
		server.Close()
	})
	return f
}

func (f *initialSyncRangeFixture) start(fn func() ([]byte, error)) *initialSyncRangeOutcome {
	outcome := &initialSyncRangeOutcome{done: make(chan struct{})}
	f.outcomes = append(f.outcomes, outcome)
	go func() {
		defer close(outcome.done)
		outcome.data, outcome.err = fn()
	}()
	return outcome
}

func awaitInitialSync(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deterministic read checkpoint")
	}
}

func (f *initialSyncRangeFixture) nextRequest(t *testing.T) *initialSyncRangeGate {
	t.Helper()
	select {
	case gate := <-f.started:
		return gate
	case <-time.After(5 * time.Second):
		t.Fatal("expected independent HTTP range did not start")
		return nil
	}
}

func (f *initialSyncRangeFixture) key(t *testing.T, offset int64) DiskReadCacheKey {
	t.Helper()
	key, ok := f.fs.diskReadCacheKey(f.handle.Path, f.entry, offset, 4)
	if !ok {
		t.Fatal("range cache key unavailable")
	}
	return key
}

// The real flight completion channel proves the callback already captured its
// immutable cause. Observing it does not add a production hook or guess timing.
func (f *initialSyncRangeFixture) flight(t *testing.T, generation uint64, offset int64) *singleflightCall {
	t.Helper()
	key := f.fs.initialSyncFlightKey(f.key(t, offset).flightKey(), generation)
	f.fs.readFlight.mu.Lock()
	call := f.fs.readFlight.calls[key]
	f.fs.readFlight.mu.Unlock()
	if call == nil {
		t.Fatalf("active flight %q unavailable at request gate", key)
	}
	return call
}

func (f *initialSyncRangeFixture) assertDrained(t *testing.T, rejected bool) {
	t.Helper()
	if f.fs.readFlight.Inflight() != 0 {
		t.Fatal("read left an active flight")
	}
	if rejected {
		for _, offset := range []int64{0, 4} {
			if data, ok := f.fs.diskReadCache.Get(f.key(t, offset)); ok {
				t.Fatalf("rejected range entered cache: offset=%d data=%q", offset, data)
			}
		}
	}
	if fh, ok := f.fs.fileHandles.Get(f.handleID); !ok || fh != f.handle || fh.BaseRev != 1 || fh.DirtySeq != 0 {
		t.Fatal("read lost or rebaselined its FD")
	}
	if entry := mustGetInodeEntry(t, f.fs, f.handle.Ino); entry.Nlookup != 1 {
		t.Fatalf("lookup references changed: %d", entry.Nlookup)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	releases := make([]func(), 0, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for range 2 {
		release, err := f.fs.acquireRemoteReadSlot(ctx)
		if err != nil {
			t.Fatalf("read slot leaked: %v", err)
		}
		releases = append(releases, release)
	}
	if !f.fs.mountViewMu.TryLock() {
		t.Fatal("rejected read retained a publication lock")
	}
	f.fs.mountViewMu.Unlock()
}

func TestInitialSyncParallelOwnerCancellationIsTerminal(t *testing.T) {
	f := newInitialSyncRangeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generation := f.fs.mountViewGeneration.Load()
	request := &initialSyncRequest{}
	var attempts atomic.Int32
	outcome := f.start(func() ([]byte, error) {
		return executeInitialSync(f.fs, ctx, request, generation, func(ctx context.Context, _ bool) ([]byte, error) {
			attempts.Add(1)
			data, _, err := f.fs.readDiskCachedBlocks(ctx, f.handle.Path, f.handle, f.entry, 0, 8, generation)
			return data, err
		})
	})
	first, second := f.nextRequest(t), f.nextRequest(t)
	if first.offset == 4 {
		first, second = second, first
	}
	if first.offset != 0 || second.offset != 4 {
		t.Fatal("parallel workers did not fetch two distinct blocks")
	}
	flight := f.flight(t, generation, 0)
	f.fs.resetMountViewWithInitialSync(true)
	first.respond("OLD!")
	awaitInitialSync(t, flight.done)
	if cause, ok := mountViewCause(flight.err); !ok || !cause.initial || cause.generation != generation {
		t.Fatalf("first rejected flight cause=%v", flight.err)
	}
	cancel() // The second actual HTTP request must observe owner cancellation.
	awaitInitialSync(t, second.canceled)
	awaitInitialSync(t, outcome.done)
	if !errors.Is(outcome.err, context.Canceled) || outcome.data != nil || request.retried || attempts.Load() != 1 {
		t.Fatalf("data=%q err=%v retried=%t attempts=%d", outcome.data, outcome.err, request.retried, attempts.Load())
	}
	if f.heads.Load() != 0 || f.reads.Load() != 2 {
		t.Fatalf("unexpected recovery HTTP: HEAD=%d ranges=%d", f.heads.Load(), f.reads.Load())
	}
	f.assertDrained(t, true)
}

func TestInitialSyncParallelRuntimeInterleavingsDoNotRecover(t *testing.T) {
	for _, order := range []string{"runtime-before-first", "runtime-after-first-rejection"} {
		t.Run(order, func(t *testing.T) {
			f := newInitialSyncRangeFixture(t)
			generation := f.fs.mountViewGeneration.Load()
			request := &initialSyncRequest{}
			var attempts atomic.Int32
			outcome := f.start(func() ([]byte, error) {
				return executeInitialSync(f.fs, context.Background(), request, generation, func(ctx context.Context, _ bool) ([]byte, error) {
					attempts.Add(1)
					data, _, err := f.fs.readDiskCachedBlocks(ctx, f.handle.Path, f.handle, f.entry, 0, 8, generation)
					if order == "runtime-after-first-rejection" {
						// All actual workers have drained and captured the first
						// cause; reset before the executor's eligibility decision.
						f.fs.resetMountView()
					}
					return data, err
				})
			})
			first, second := f.nextRequest(t), f.nextRequest(t)
			if first.offset == second.offset {
				t.Fatal("parallel workers fetched the same block")
			}
			if order == "runtime-before-first" {
				f.fs.resetMountView()
			}
			f.fs.resetMountViewWithInitialSync(true)
			first.respond("OLD!")
			second.respond("OLD!")
			awaitInitialSync(t, outcome.done)
			cause, view := mountViewCause(outcome.err)
			if !view || cause.generation != generation || cause.initial != (order == "runtime-after-first-rejection") {
				t.Fatalf("immutable cause=%v for order=%s", outcome.err, order)
			}
			if outcome.data != nil || request.retried || attempts.Load() != 1 || f.reads.Load() != 2 {
				t.Fatalf("data=%q retried=%t attempts=%d HTTP=%d", outcome.data, request.retried, attempts.Load(), f.reads.Load())
			}
			f.assertDrained(t, true)
		})
	}
}

func TestInitialSyncFlightWaiterKeepsEarlierRuntimeOwnerSource(t *testing.T) {
	f := newInitialSyncRangeFixture(t)
	generation := f.fs.mountViewGeneration.Load()
	key := f.key(t, 0)
	owner := f.start(func() ([]byte, error) {
		data, _, err := f.fs.readDiskCachedRangeForGeneration(context.Background(), f.handle.Path, f.handle, key, generation)
		return data, err
	})
	gate := f.nextRequest(t)
	flight := f.flight(t, generation, 0)
	f.fs.resetMountView()
	waiterGeneration := f.fs.mountViewGeneration.Load()
	request := &initialSyncRequest{}
	var attempts atomic.Int32
	waiter := f.start(func() ([]byte, error) {
		return executeInitialSync(f.fs, context.Background(), request, waiterGeneration, func(ctx context.Context, _ bool) ([]byte, error) {
			attempts.Add(1)
			data, _, err := f.fs.readDiskCachedRangeCancellable(ctx, f.handle.Path, f.handle, key, waiterGeneration)
			return data, err
		})
	})
	waitForWaiters(t, f.fs.readFlight, key.flightKey(), 1)
	f.fs.resetMountViewWithInitialSync(true)
	if f.fs.initialSyncResetGeneration.Load() != waiterGeneration+1 {
		t.Fatal("waiter stage did not cross exactly the first reset")
	}
	gate.respond("OLD!")
	awaitInitialSync(t, owner.done)
	awaitInitialSync(t, waiter.done)
	awaitInitialSync(t, flight.done)
	cause, view := mountViewCause(waiter.err)
	if !view || cause.generation != generation || cause.initial || waiter.err != owner.err || waiter.err != flight.err {
		t.Fatalf("waiter=%v owner=%v flight=%v; source was relabeled", waiter.err, owner.err, flight.err)
	}
	if owner.data != nil || waiter.data != nil || request.retried || attempts.Load() != 1 || f.reads.Load() != 1 {
		t.Fatalf("owner=%q waiter=%q retried=%t attempts=%d HTTP=%d", owner.data, waiter.data, request.retried, attempts.Load(), f.reads.Load())
	}
	f.assertDrained(t, true)
}

type initialSyncQueuedViewError struct {
	err    error
	queued chan struct{}
	once   sync.Once
}

func (e *initialSyncQueuedViewError) Error() string { return e.err.Error() }
func (e *initialSyncQueuedViewError) Unwrap() error {
	// The range helper returns flight errors without unwrapping. The worker
	// first classifies this cause only after sending its result to the queue.
	e.once.Do(func() { close(e.queued) })
	return e.err
}

func TestInitialSyncParallelKeepsEarlierRuntimeOwnerSource(t *testing.T) {
	f := newInitialSyncRangeFixture(t)
	oldGeneration := f.fs.mountViewGeneration.Load()
	oldKey := f.key(t, 4)
	oldOwner := f.start(func() ([]byte, error) {
		data, _, err := f.fs.readDiskCachedRangeForGeneration(context.Background(), f.handle.Path, f.handle, oldKey, oldGeneration)
		return data, err
	})
	oldGate := f.nextRequest(t)
	f.fs.resetMountView()
	generation := f.fs.mountViewGeneration.Load()
	key := f.key(t, 0)
	ready, release, queued := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	finishFirst := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(finishFirst)
	firstOwner := f.start(func() ([]byte, error) {
		data, err, _ := f.fs.readFlight.Do(context.Background(), key.flightKey(), func() ([]byte, error) {
			close(ready)
			<-release
			cause := f.fs.lockMountViewReadCause(generation)
			if cause == nil {
				f.fs.mountViewMu.RUnlock()
				return nil, errors.New("first owner was released before reset")
			}
			return nil, &initialSyncQueuedViewError{err: cause, queued: queued}
		})
		return data, err
	})
	awaitInitialSync(t, ready)
	request := &initialSyncRequest{}
	var attempts atomic.Int32
	outcome := f.start(func() ([]byte, error) {
		return executeInitialSync(f.fs, context.Background(), request, generation, func(ctx context.Context, fresh bool) ([]byte, error) {
			attempts.Add(1)
			if fresh {
				return []byte("incorrect recovery"), nil
			}
			data, _, err := f.fs.readDiskCachedBlocks(ctx, f.handle.Path, f.handle, f.entry, 0, 8, generation)
			return data, err
		})
	})
	waitForWaiters(t, f.fs.readFlight, key.flightKey(), 1)
	waitForWaiters(t, f.fs.readFlight, oldKey.flightKey(), 1)
	f.fs.resetMountViewWithInitialSync(true)
	finishFirst()
	awaitInitialSync(t, queued) // First marker is queued before the runtime cause.
	oldGate.respond("OLD!")
	awaitInitialSync(t, oldOwner.done)
	awaitInitialSync(t, firstOwner.done)
	awaitInitialSync(t, outcome.done)
	cause, view := mountViewCause(outcome.err)
	if !view || cause.initial || cause.generation != oldGeneration || outcome.err != oldOwner.err {
		t.Fatalf("parallel=%v owner=%v; earlier runtime source was lost", outcome.err, oldOwner.err)
	}
	if outcome.data != nil || request.retried || attempts.Load() != 1 || f.reads.Load() != 1 || f.heads.Load() != 0 {
		t.Fatalf("data=%q retried=%t attempts=%d ranges=%d HEAD=%d", outcome.data, request.retried, attempts.Load(), f.reads.Load(), f.heads.Load())
	}
	f.assertDrained(t, true)
}

func TestInitialSyncPostFirstFlightAndFreshFetchAreIndependent(t *testing.T) {
	f := newInitialSyncRangeFixture(t)
	generation := f.fs.mountViewGeneration.Load()
	key := f.key(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := &initialSyncRequest{}
	var attempts atomic.Int32
	recovery := f.start(func() ([]byte, error) {
		return executeInitialSync(f.fs, ctx, request, generation, func(owner context.Context, fresh bool) ([]byte, error) {
			attempts.Add(1)
			if owner != ctx {
				return nil, errors.New("first recovery changed owner context")
			}
			data, _, err := f.fs.readDiskCachedRangeForGeneration(owner, f.handle.Path, f.handle, key,
				f.fs.mountViewGeneration.Load(), readRecoveryDeadline(fresh, initialSyncDeadline(owner, fresh))...)
			return data, err
		})
	})
	oldGate := f.nextRequest(t)
	oldFlight := f.flight(t, generation, 0)
	f.fs.resetMountViewWithInitialSync(true)
	current := f.fs.mountViewGeneration.Load()
	ordinary := f.start(func() ([]byte, error) {
		data, _, err := f.fs.readDiskCachedRangeForGeneration(context.Background(), f.handle.Path, f.handle, key, current)
		return data, err
	})
	ordinaryGate := f.nextRequest(t) // Cannot join the pre-first flight.
	ordinaryFlight := f.flight(t, current, 0)
	if ordinaryFlight == oldFlight {
		t.Fatal("post-first request joined the old owner")
	}
	oldGate.respond("OLD!")
	awaitInitialSync(t, oldFlight.done)
	freshGate := f.nextRequest(t) // Cannot join/register the post-first flight.
	freshGate.respond("DATA")
	awaitInitialSync(t, recovery.done)
	postKey := f.fs.initialSyncFlightKey(key.flightKey(), current)
	if recovery.err != nil || string(recovery.data) != "DATA" || !request.retried || attempts.Load() != 2 {
		t.Fatalf("data=%q err=%v retried=%t attempts=%d", recovery.data, recovery.err, request.retried, attempts.Load())
	}
	if f.flight(t, current, 0) != ordinaryFlight || f.fs.readFlight.Waiters(postKey) != 0 || f.reads.Load() != 3 {
		t.Fatal("fresh attempt altered/joined ordinary flight ownership")
	}
	ordinaryGate.respond("DATA")
	awaitInitialSync(t, ordinary.done)
	if ordinary.err != nil || string(ordinary.data) != "DATA" {
		t.Fatalf("ordinary data=%q err=%v", ordinary.data, ordinary.err)
	}
	f.assertDrained(t, false)
}
