package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/extent"
)

// extentShutdownRecorder is an object store that records Shutdown calls.
type extentShutdownRecorder struct {
	object.ObjectStorage
	shutdowns atomic.Int64
}

func (s *extentShutdownRecorder) Shutdown() { s.shutdowns.Add(1) }

// TestExtentTeardownStopsAndClosesRuntime pins the teardown hook: it marks the
// fs torn down, stops the compaction loop, closes the runtime exactly once,
// clears the pointer, and refuses a later ensureExtentRuntime.
func TestExtentTeardownStopsAndClosesRuntime(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	var stopped atomic.Bool
	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{
		rt:   &extent.Runtime{Storage: rec},
		hold: newExtentMetaHold(),
		stop: func() { stopped.Store(true) },
	})

	fs.stopExtentRuntimeLoop()
	if !stopped.Load() {
		t.Fatal("compaction loop was not stopped")
	}
	if !fs.extentTornDown {
		t.Fatal("fs was not marked torn down")
	}

	fs.closeExtentRuntime()
	if fs.extentRT.Load() != nil {
		t.Fatal("extentRT still set after teardown")
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("runtime shutdowns = %d, want 1", got)
	}

	if err := fs.ensureExtentRuntime(); err == nil {
		t.Fatal("ensureExtentRuntime must refuse after teardown")
	}
}

// TestExtentMetaHoldDrains pins the teardown gate: it waits for in-flight
// metadata RPCs, refuses a late handler RPC, but admits the teardown-owned RPCs
// CloseRuntime flushes itself (session cleanup and dead-slice drain).
func TestExtentMetaHoldDrains(t *testing.T) {
	teardownOps := []string{jfsmeta.Drive9OpCleanStaleSession, jfsmeta.Drive9OpDeleteSlice}
	h := newExtentMetaHold()
	if !h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("first enter must be admitted")
	}
	// A zero-timeout close closes the gate synchronously but must report that the
	// in-flight RPC was not drained yet.
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

	// The original lookup is still in flight: a real closeAndWait returns true
	// only after it finishes.
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

// holdGatedMeta is a jfsmeta.Meta whose only live method, CloseSession, mirrors
// the transport's gate: it enters the hold before doing the cleanup and reports
// EIO when the gate refuses. Embedding the interface leaves every other method
// unimplemented, so a test that calls one panics loudly.
type holdGatedMeta struct {
	jfsmeta.Meta
	hold   *extentMetaHold
	closes atomic.Int64
}

func (m *holdGatedMeta) CloseSession() error {
	if m.hold != nil {
		// CloseSession flushes both a session cleanup and its dead-slice queue;
		// both must pass the closed gate or the held blob is never released.
		for _, op := range []string{jfsmeta.Drive9OpCleanStaleSession, jfsmeta.Drive9OpDeleteSlice} {
			if !m.hold.enter(op) {
				return syscall.EIO
			}
			m.hold.leave()
		}
	}
	m.closes.Add(1)
	return nil
}

// TestCloseExtentRuntimeAttemptsSessionCleanup drives closeExtentRuntime and
// asserts its own teardown RPCs (session cleanup and dead-slice drain) are
// attempted and admitted. With a refuse-all gate the hold was closed before
// CloseRuntime ran, so this call returned EIO and the held blob was never
// released.
func TestCloseExtentRuntimeAttemptsSessionCleanup(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	hold := newExtentMetaHold()
	meta := &holdGatedMeta{hold: hold}

	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{rt: &extent.Runtime{Meta: meta, Storage: rec}, hold: hold})
	fs.closeExtentRuntime()

	if got := meta.closes.Load(); got != 1 {
		t.Fatalf("CloseSession calls = %d, want 1 (the drain gate refused the cleanup RPC)", got)
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("storage shutdowns = %d, want 1", got)
	}
	if hold.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("a late non-cleanup RPC must be refused after closeExtentRuntime")
	}
}

// TestExtentTeardownOpsCoverRealCloseSession drives a real jfsmeta.Meta's
// CloseSession through a closed gate and records every op the gate is asked
// about, so a JuiceFS bump that adds a third teardown RPC fails here instead of
// silently leaking at unmount. The op set comes from the vendored code path,
// not from the allowlist, so this is not circular.
//
// The format is initialized and ReadOnly is left false, so FlushSession runs as
// it does in production (its drive9 flush methods are no-ops today, but a bump
// that moved one onto an RPC would be caught here).
//
// Only clean_stale_session is test-pinned. delete_slice needs a non-empty
// dead-slice queue (a full write + Compact path), so its allowlist entry rests
// on the vendored trace a dependency bump must re-check: juicefs
// pkg/meta/base.go CloseSession -> doCleanStaleSession sends
// clean_stale_session, and stopDeleteSliceTasks drains the delete-slice
// workers, whose deleteSlice_ -> doDeleteSlice sends delete_slice.
func TestExtentTeardownOpsCoverRealCloseSession(t *testing.T) {
	var mu sync.Mutex
	var ops []string
	record := func(op string) {
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
	}
	tr := extent.NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		return []byte("{}"), 0, nil
	})
	hold := newExtentMetaHold()
	tr.Enter = func(op string) bool {
		record(op)
		return hold.enter(op)
	}
	tr.Leave = hold.leave

	conf := jfsmeta.DefaultConf()
	conf.NoBGJob = true
	conf.Sid = 42
	m := jfsmeta.NewDrive9Meta(conf, tr)
	// Init sets m.fmt, so CloseSession's FlushSession leg runs (as production
	// does with ReadOnly=false) instead of dereferencing a nil format.
	if err := m.Init(&jfsmeta.Format{Name: "drive9", UUID: "drive9-extent", Storage: "file", BlockSize: 4 << 20}, false); err != nil {
		t.Fatal(err)
	}

	hold.closeAndWait(0) // closed gate: only teardown-owned ops may pass
	mu.Lock()
	ops = nil
	mu.Unlock()
	if err := m.CloseSession(); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ops) == 0 {
		t.Fatal("CloseSession issued no metadata RPC")
	}
	for _, op := range ops {
		if !extentTeardownOp(op) {
			t.Fatalf("CloseSession issued %q; the closed gate refuses it and teardown would leak", op)
		}
	}
	found := false
	for _, op := range ops {
		if op == jfsmeta.Drive9OpCleanStaleSession {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected clean_stale_session among %v", ops)
	}
}

// TestEnsureExtentRuntimeWaiterWakesOnTeardown parks a single-flight waiter
// behind an in-progress build, then tears down: the teardown must broadcast the
// build condition so the waiter does not linger forever.
func TestEnsureExtentRuntimeWaiterWakesOnTeardown(t *testing.T) {
	fs := &Dat9FS{}
	parked := make(chan struct{})
	fs.extentMu.Lock()
	fs.extentBuild.cond = sync.NewCond(&fs.extentMu)
	fs.extentBuild.building = true
	fs.extentBuild.gen = 1
	testHookExtentBuildWait = func() { close(parked) }
	t.Cleanup(func() { testHookExtentBuildWait = nil })
	fs.extentMu.Unlock()

	done := make(chan error, 1)
	go func() { done <- fs.ensureExtentRuntime() }()
	<-parked // the waiter now holds extentMu and is about to park

	// stopExtentRuntimeLoop blocks on extentMu until the waiter's Wait releases
	// it, so the teardown broadcast can never race ahead of the park.
	fs.stopExtentRuntimeLoop()
	select {
	case err := <-done:
		if !errors.Is(err, errExtentTornDown) {
			t.Fatalf("waiter err = %v, want errExtentTornDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by teardown")
	}
}

// TestEnsureExtentRuntimeWaitersShareBuildError pins the single-flight outcome:
// every waiter parked on a failed build gets that same error, and a later caller
// starts a fresh build instead of inheriting the stale one.
func TestEnsureExtentRuntimeWaitersShareBuildError(t *testing.T) {
	boom := errors.New("build boom")
	fs := &Dat9FS{}
	parked := make(chan struct{})
	fs.extentMu.Lock()
	fs.extentBuild.cond = sync.NewCond(&fs.extentMu)
	fs.extentBuild.building = true
	fs.extentBuild.gen = 1
	testHookExtentBuildWait = func() { close(parked) }
	t.Cleanup(func() { testHookExtentBuildWait = nil })
	fs.extentMu.Unlock()

	done := make(chan error, 1)
	go func() { done <- fs.ensureExtentRuntime() }()
	<-parked // the waiter now holds extentMu and is about to park

	// Simulate the in-progress build failing, as ensureExtentRuntime's deferred
	// publisher would. The waiter holds extentMu between the hook and Wait, so
	// this lock can only be acquired after the waiter has parked — no race.
	fs.extentMu.Lock()
	fs.extentBuild.building = false
	fs.extentBuild.err = boom
	fs.extentBuild.cond.Broadcast()
	fs.extentMu.Unlock()

	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("waiter err = %v, want %v", err, boom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not observe the failed build")
	}

	// A fresh caller (generation moved on) attempts its own build. With no
	// client it fails at setup, not with the stale error, proving it did not
	// short-circuit on the previous failure.
	if err := fs.ensureExtentRuntime(); err == nil || errors.Is(err, boom) {
		t.Fatalf("fresh caller err = %v, want an unrelated build error", err)
	}
}

// TestExtentRuntimePointerRace drives a hot runtime reader concurrently with
// the teardown that clears the pointer. Under -race it pins the atomic-snapshot
// discipline: a bare field read/write here is a data race.
func TestExtentRuntimePointerRace(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{rt: &extent.Runtime{Storage: rec}, hold: newExtentMetaHold()})

	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started)
		for i := 0; i < 2000; i++ {
			_ = fs.extentVFS()
		}
	}()
	<-started // overlap the reader with teardown, or the window is never hit
	fs.stopExtentRuntimeLoop()
	fs.closeExtentRuntime()
	wg.Wait()
	if fs.extentRT.Load() != nil {
		t.Fatal("extentRT still set after teardown")
	}
}
