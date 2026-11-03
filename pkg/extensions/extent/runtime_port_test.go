package extent

// Offline rows ported from v1 pkg/fuse/extent_open_probe_test.go and
// extent_teardown_test.go (d39a6d1b3): the metadata-hold teardown gate, the
// stop/close/refuse teardown contract, the runtime-pointer race discipline,
// and the single-flight ensureRuntime waiter semantics. The v1 rows that
// drove a real CloseSession through vendored jfsmeta (teardown-op coverage
// via Transport.Enter, delete_slice admission through compactChunk) ride the
// pkg/extent transport/meta integration and are covered there; the miss-cache
// TTL row lives in extent_test.go.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extent"
)

// extentShutdownRecorder records Shutdown calls on the object store.
type extentShutdownRecorder struct {
	object.ObjectStorage
	shutdowns atomic.Int64
}

func (s *extentShutdownRecorder) Shutdown() { s.shutdowns.Add(1) }

// v1 TestExtentMetaHoldDrains: the teardown gate waits for in-flight
// metadata RPCs, refuses a late handler RPC, but admits the teardown-owned
// RPCs CloseRuntime flushes itself (session cleanup and dead-slice drain).
func TestPortExtentMetaHoldDrains(t *testing.T) {
	teardownOps := []string{jfsmeta.Drive9OpCleanStaleSession, jfsmeta.Drive9OpDeleteSlice}
	h := newExtentMetaHold()
	if !h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("first enter must be admitted")
	}
	// A zero-timeout close closes the gate synchronously but must report
	// that the in-flight RPC was not drained yet.
	if h.closeAndWait(0) {
		t.Fatal("closeAndWait(0) must report a timeout while an RPC is in flight")
	}
	if h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("a late non-cleanup RPC must be refused once the gate is closed")
	}
	for _, op := range teardownOps {
		if !h.enter(op) {
			t.Fatalf("teardown RPC %q must be admitted through the closed gate", op)
		}
		h.leave()
	}

	// The original lookup is still in flight: a real closeAndWait returns
	// true only after it finishes.
	closed := make(chan bool, 1)
	go func() { closed <- h.closeAndWait(2 * time.Second) }()
	h.leave() // the pre-teardown RPC
	select {
	case ok := <-closed:
		if !ok {
			t.Fatal("closeAndWait must report drained after the RPC finished")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closeAndWait did not return after the RPC finished")
	}
	if h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("a non-cleanup RPC must stay refused")
	}
	for _, op := range teardownOps {
		if !h.enter(op) {
			t.Fatalf("teardown RPC %q must stay admitted", op)
		}
		h.leave()
	}
}

// v1 TestExtentTeardownStopsAndClosesRuntime: teardown marks the mount torn
// down, stops the compaction loop, closes the runtime exactly once, clears
// the pointer, and refuses a later ensureRuntime.
func TestPortTeardownStopsAndClosesRuntime(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	var stopped atomic.Bool
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.rt.Store(&extentRuntime{
		rt:   &extent.Runtime{Storage: rec},
		hold: newExtentMetaHold(),
		stop: func() { stopped.Store(true) },
	})

	e.stopRuntimeLoop()
	if !stopped.Load() {
		t.Fatal("compaction loop was not stopped")
	}
	if !e.extentTornDown {
		t.Fatal("mount was not marked torn down")
	}

	e.closeRuntime()
	if e.rt.Load() != nil {
		t.Fatal("runtime pointer still set after teardown")
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("runtime shutdowns = %d, want 1", got)
	}

	if err := e.ensureRuntime(); !errors.Is(err, errExtentTornDown) {
		t.Fatalf("ensureRuntime after teardown = %v, want errExtentTornDown", err)
	}
}

// v1 TestExtentRuntimePointerRace: a hot runtime reader concurrent with the
// teardown that clears the pointer; under -race this pins the snapshot
// discipline.
func TestPortRuntimePointerRace(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.rt.Store(&extentRuntime{rt: &extent.Runtime{Storage: rec}, hold: newExtentMetaHold()})

	var stop atomic.Bool
	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started)
		for !stop.Load() {
			_ = e.vfs()
		}
	}()
	<-started
	// Teardown runs while the reader is still looping, so the read/write
	// window is actually exercised.
	e.stopRuntimeLoop()
	e.closeRuntime()
	stop.Store(true)
	wg.Wait()
	if e.rt.Load() != nil {
		t.Fatal("runtime pointer still set after teardown")
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("runtime shutdowns = %d, want 1", got)
	}
}

// credEndpoint serves POST /v1/data-credential. release gates the first
// response; every response is a 503 so the build fails deterministically.
func credEndpoint(t *testing.T, hold time.Duration) (*httptest.Server, *atomic.Int32, chan struct{}, chan struct{}) {
	t.Helper()
	var calls atomic.Int32
	firstSeen := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		once.Do(func() { close(firstSeen) })
		if hold > 0 {
			time.Sleep(hold)
		} else {
			<-release
		}
		http.Error(w, "no credential", http.StatusServiceUnavailable)
	}))
	t.Cleanup(ts.Close)
	return ts, &calls, firstSeen, release
}

// v1 TestEnsureExtentRuntimeWaiterWakesOnTeardown: a single-flight waiter
// parked behind an in-progress build must be woken (with errExtentTornDown)
// by the teardown broadcast, not linger until the build finishes.
func TestPortEnsureRuntimeWaiterWakesOnTeardown(t *testing.T) {
	ts, _, firstSeen, release := credEndpoint(t, 0)
	e, err := New(Options{Client: client.New(ts.URL, ""), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	builderDone := make(chan error, 1)
	go func() { builderDone <- e.ensureRuntime() }()
	<-firstSeen // the builder holds the single-flight slot and the mutex is free

	waiterDone := make(chan error, 1)
	go func() { waiterDone <- e.ensureRuntime() }()
	time.Sleep(50 * time.Millisecond) // let the waiter park

	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waiterDone:
		if !errors.Is(err, errExtentTornDown) {
			t.Fatalf("waiter err = %v, want errExtentTornDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by teardown")
	}
	// Release the builder: it finishes with its own build error (or the
	// torn-down install refusal) and the goroutine terminates.
	close(release)
	select {
	case <-builderDone:
	case <-time.After(3 * time.Second):
		t.Fatal("builder did not terminate after release")
	}
}

// v1 TestEnsureExtentRuntimeWaitersShareBuildError: every waiter parked on a
// failed build gets that same error, and a later caller starts a fresh build
// instead of inheriting the stale one.
func TestPortEnsureRuntimeWaitersShareBuildError(t *testing.T) {
	ts, calls, firstSeen, _ := credEndpoint(t, 400*time.Millisecond)
	e, err := New(Options{Client: client.New(ts.URL, ""), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	builderDone := make(chan error, 1)
	go func() { builderDone <- e.ensureRuntime() }()
	<-firstSeen

	waiterDone := make(chan error, 1)
	go func() { waiterDone <- e.ensureRuntime() }()
	var builderErr, waiterErr error
	select {
	case builderErr = <-builderDone:
	case <-time.After(3 * time.Second):
		t.Fatal("builder did not finish")
	}
	select {
	case waiterErr = <-waiterDone:
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not observe the failed build")
	}
	if waiterErr == nil || builderErr == nil || !errors.Is(waiterErr, builderErr) {
		t.Fatalf("waiter err = %v, builder err = %v, want the shared build error", waiterErr, builderErr)
	}
	// The waiter parked on the builder's request: one credential call for
	// both.
	if got := calls.Load(); got != 1 {
		t.Fatalf("credential calls = %d, want 1 shared build", got)
	}

	// A fresh caller (generation moved on) attempts its own build instead
	// of short-circuiting on the stale error.
	fresh := make(chan error, 1)
	go func() { fresh <- e.ensureRuntime() }()
	select {
	case err := <-fresh:
		if err == nil || errors.Is(err, builderErr) {
			t.Fatalf("fresh caller err = %v, want an unrelated fresh build error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fresh caller did not finish")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("credential calls after fresh caller = %d, want 2", got)
	}
}
