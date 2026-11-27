package writeback

// The unconditional commit-stamp table's lifecycle (#1083): every successful
// commit stamps its pathname, so create→upload→delete churn on uniquely
// named files must not retain every name for the mount's lifetime. The table
// trims once no rename window holds the stamps; pins are per-window (an
// ended window never keeps another one's obligations), and ending the last
// window restores the bound without waiting for future commits.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestCommitSeqLifecycleBounded asserts boundedness after obligations end,
// pin survival while a window is open, idle restoration of the bound at
// window close, and progress under overlapping windows.
func TestCommitSeqLifecycleBounded(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(nil, nil, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)

	limit := cq.commitSeqLimitLocked()
	churn := func(round, n int) {
		for i := 0; i < n; i++ {
			cq.recordCommitSeq(fmt.Sprintf("/tmp/churn-%d-%d.txt", round, i))
		}
	}
	stampCount := func() int {
		cq.mu.Lock()
		defer cq.mu.Unlock()
		return len(cq.commitSeq)
	}

	// Obligation-free churn: the table stays at the bound, and the oldest
	// stamps are reclaimed first.
	churn(0, 3*limit)
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps after obligation-free churn, want <= %d", n, limit)
	}
	if got := cq.CommitSeq("/tmp/churn-0-0.txt"); got != 0 {
		t.Fatalf("the oldest stamp survived the trim: %d", got)
	}

	// An open window pins what its flow may still compare: the renamed
	// endpoints and the paths under its prefixes.
	cq.recordCommitSeq("/keep/old.txt")
	cq.recordCommitSeq("/keep/dst.txt")
	wantOld := cq.CommitSeq("/keep/old.txt")
	preLanding := cq.CommitSeq("/keep/dst.txt")
	win := cq.BeginCommitSeqWindow("/keep/old.txt", "/keep/dst.txt")
	// A landing DURING the window inside its universe — the delta case the
	// fences exist for.
	cq.recordCommitSeq("/keep/dst.txt")
	wantLanding := cq.CommitSeq("/keep/dst.txt")
	if wantLanding <= preLanding {
		t.Fatalf("the in-window landing did not advance the stamp: %d <= %d", wantLanding, preLanding)
	}
	// Churn OUTSIDE the window's universe is not this window's obligation:
	// it stays reclaimable while the window is open.
	churn(1, 3*limit)
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps while a narrow window is open, want <= %d", n, limit)
	}
	if got := cq.CommitSeq("/keep/old.txt"); got != wantOld {
		t.Fatalf("pinned stamp /keep/old.txt = %d, want %d", got, wantOld)
	}
	if got := cq.CommitSeq("/keep/dst.txt"); got != wantLanding {
		t.Fatalf("pinned in-window landing stamp = %d, want %d", got, wantLanding)
	}

	// Ending the window restores the bound IMMEDIATELY — an idle mount must
	// not retain the window's growth until some unrelated future commit.
	win.End()
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps right after the window ended, want <= %d without further churn", n, limit)
	}

	// The same restoration must hold when the OVER-limit mass is the
	// window's OWN obligation: covered paths pin past the bound while the
	// window is open, and End alone (no later commit) reclaims them.
	wide := cq.BeginCommitSeqWindow("/wide", "/wide-dst")
	for i := 0; i < limit+limit/2; i++ {
		cq.recordCommitSeq(fmt.Sprintf("/wide/child-%d.txt", i))
	}
	if n := stampCount(); n <= limit {
		t.Fatalf("precondition: the pinned covered paths grew the table to only %d, want > %d", n, limit)
	}
	wide.End()
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps right after the covered window ended, want <= %d reclaimed immediately", n, limit)
	}

	// Overlapping windows keep progress: each window's close drops exactly
	// ITS pins, so a finished rename never waits for the last sibling.
	a := cq.BeginCommitSeqWindow("/ov/a", "/ov/a-dst")
	b := cq.BeginCommitSeqWindow("/ov/b", "/ov/b-dst")
	cq.recordCommitSeq("/ov/a/x.txt")
	cq.recordCommitSeq("/ov/b/x.txt")
	wantB := cq.CommitSeq("/ov/b/x.txt")
	a.End()
	// A's obligations are gone; B's survive A's close and any churn.
	churn(2, 3*limit)
	if got := cq.CommitSeq("/ov/b/x.txt"); got != wantB {
		t.Fatalf("window B's pin did not survive window A's close: %d, want %d", got, wantB)
	}
	if got := cq.CommitSeq("/ov/a/x.txt"); got != 0 {
		t.Fatalf("window A's stamp survived its own end: %d", got)
	}
	b.End()
	churn(3, 3*limit)
	if got := cq.CommitSeq("/ov/b/x.txt"); got != 0 {
		t.Fatalf("window B's stamp survived its own end: %d", got)
	}
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps after every window ended, want <= %d", n, limit)
	}

	// Single-endpoint window (newPath == "", e.g. PrepareOpenUnlink) must NOT
	// synthesize a "/" prefix that pins unrelated mount commits past the bound.
	cq.recordCommitSeq("/unlink/target.txt")
	wantUnlink := cq.CommitSeq("/unlink/target.txt")
	single := cq.BeginCommitSeqWindow("/unlink/target.txt", "")
	churn(4, 3*limit)
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps while single-endpoint window is open, want <= %d", n, limit)
	}
	if got := cq.CommitSeq("/unlink/target.txt"); got != wantUnlink {
		t.Fatalf("single-endpoint pinned stamp = %d, want %d", got, wantUnlink)
	}
	single.End()
}

func TestHasUncanceledPathIgnoresCanceledInFlightAndQueuedEntries(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(nil, nil, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)

	const p = "/surv-dying.txt"
	dyingInFlight := &CommitEntry{Path: p, canceled: true}
	dyingQueued := &CommitEntry{Path: p, canceled: true}
	dyingImmediate := &CommitEntry{Path: p, canceled: true}

	cq.mu.Lock()
	cq.inFlight[p] = dyingInFlight
	cq.queue = append(cq.queue, dyingQueued)
	cq.addQueuedLocked(dyingQueued)
	cq.immediate[dyingImmediate] = struct{}{}
	cq.mu.Unlock()

	if !cq.HasPath(p) {
		t.Fatal("HasPath must remain true while dying canceled entries are draining")
	}
	if cq.HasUncanceledPath(p) {
		t.Fatal("HasUncanceledPath must ignore canceled entries across inFlight, immediate, and queue")
	}

	replacement := &CommitEntry{Path: p}
	cq.mu.Lock()
	cq.queue = append(cq.queue, replacement)
	cq.addQueuedLocked(replacement)
	cq.mu.Unlock()

	if !cq.HasUncanceledPath(p) {
		t.Fatal("HasUncanceledPath must report true once a non-canceled replacement entry is queued")
	}

	// Clean up synthetic entries before DrainAll.
	cq.mu.Lock()
	delete(cq.inFlight, p)
	delete(cq.immediate, dyingImmediate)
	cq.queue = nil
	cq.rebuildQueuedIndexLocked()
	cq.mu.Unlock()
}

// TestPrepareOpenUnlinkAbortReenqueuesOverDyingCanceledInFlightEntry verifies
// deterministically that PrepareOpenUnlink's rollback/abort re-enqueue path
// enqueues a replacement commit and drains staged data even when a dying
// canceled worker entry is still recorded in CommitQueue.inFlight between
// unlockPath() and endInFlight(). Reverting PrepareOpenUnlink to HasPath
// causes this test to fail deterministically with stranded pending data.
func TestPrepareOpenUnlinkAbortReenqueuesOverDyingCanceledInFlightEntry(t *testing.T) {
	const targetPath = "/dying-reenq.txt"
	const payload = "reenqueued-after-dying-worker"

	b := testbackend.NewBackend()
	var holdFirstPut atomic.Bool
	holdFirstPut.Store(true)
	firstPutStarted := make(chan struct{})
	releaseFirstPut := make(chan struct{})
	var releaseOnce sync.Once
	unblockFirstPut := func() {
		releaseOnce.Do(func() { close(releaseFirstPut) })
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) && holdFirstPut.CompareAndSwap(true, false) {
			close(firstPutStarted)
			select {
			case <-r.Context().Done():
			case <-releaseFirstPut:
			}
			http.Error(w, "canceled", http.StatusRequestTimeout)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	cl := client.New(srv.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)

	wb, err := New(Options{
		Policy:                  PolicyWriteBack,
		SyncMode:                "interactive",
		Client:                  cl,
		Staging:                 ledger,
		RemoteCommitWaitTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	// Registered after wb.Stop so LIFO teardown unblocks any held HTTP handler
	// before wb.Stop waits on DrainAll().
	t.Cleanup(unblockFirstPut)

	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	h := open(t, ch, targetPath)
	if err := h.Write(ctx, 0, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// Wait until the first CommitQueue worker is actively inside the HTTP PUT.
	select {
	case <-firstPutStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial CommitQueue PUT to start")
	}

	acquireLock, _, _, abort := wb.PrepareOpenUnlink(targetPath, "")
	if err := acquireLock(nil); err != nil {
		t.Fatalf("acquireLock: %v", err)
	}
	var abortOnce sync.Once
	callAbort := func() { abortOnce.Do(abort) }
	defer callAbort()

	// Verify any still-visible live entry was marked canceled, then drain the
	// live canceled goroutine completely (WaitPathDeadline only acquires cq.mu,
	// never pathLock) before pinning a deterministic synthetic dying entry.
	wb.queue.mu.Lock()
	liveCur := wb.queue.inFlight[targetPath]
	liveUncanceled := liveCur != nil && !liveCur.canceled
	wb.queue.mu.Unlock()
	if liveUncanceled {
		t.Fatalf("expected in-flight entry for %s to be marked canceled after acquireLock", targetPath)
	}
	unblockFirstPut()
	if !wb.queue.WaitPathDeadline(targetPath, 5*time.Second) {
		t.Fatalf("timed out waiting for live canceled worker on %s to finish endInFlight", targetPath)
	}

	// Unconditionally pin a synthetic dying canceled CommitEntry in inFlight[targetPath]
	// representing a canceled worker paused between unlockPath() and endInFlight(),
	// with zero background goroutines racing to clear it across HasPath, abort(),
	// and HasUncanceledPath assertions.
	dying := &CommitEntry{Path: targetPath, canceled: true}
	wb.queue.mu.Lock()
	wb.queue.inFlight[targetPath] = dying
	wb.queue.mu.Unlock()
	defer wb.queue.endInFlight(dying)

	if !wb.queue.HasPath(targetPath) {
		t.Fatal("precondition: HasPath must be true while dying canceled entry remains in inFlight")
	}
	if wb.queue.HasUncanceledPath(targetPath) {
		t.Fatal("precondition: HasUncanceledPath must be false before abort() re-enqueues")
	}

	// Execute PrepareOpenUnlink's real abort() -> reenqueueCanceledPathLocked(targetPath)
	// while the dying canceled entry remains in inFlight[targetPath] (which also
	// prevents the replacement worker from passing canBeginInFlightLocked yet).
	callAbort()

	if !wb.queue.HasUncanceledPath(targetPath) {
		t.Fatal("abort() failed to re-enqueue replacement entry while dying canceled entry remained in inFlight")
	}

	// Complete the dying worker's deferred endInFlight cleanup so the replacement
	// worker can begin in-flight execution and drain staging to the backend.
	wb.queue.endInFlight(dying)

	drainCtx, cancelDrain := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDrain()
	if err := wb.Drain(drainCtx); err != nil {
		t.Fatalf("wb.Drain: %v", err)
	}
	if got := ledger.Pending.Count(); got != 0 {
		t.Fatalf("Pending.Count() after Drain = %d, want 0", got)
	}
	if ledger.Shadow.Has(targetPath) {
		t.Fatalf("Shadow.Has(%q) = true after Drain, want false", targetPath)
	}
	node, ok := b.Node(targetPath)
	if !ok {
		t.Fatalf("backend missing %q after replacement commit drained", targetPath)
	}
	if string(node.Data) != payload {
		t.Fatalf("backend payload = %q, want %q", string(node.Data), payload)
	}
}
