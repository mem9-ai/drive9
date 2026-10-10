package writeback

import (
	"context"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// asyncStack builds [writeback(writeback policy + staging/queue) → core]
// over one fake backend.
func asyncStack(t *testing.T, b *testbackend.Backend) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())

	st, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Events: fs.NewEventBus(), Staging: st})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, wb
}

func asyncOpen(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	return res.Handle
}

func TestAsyncWritebackStagesThenLands(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := asyncStack(t, b)
	ctx := context.Background()

	h := asyncOpen(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("async image")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// writeback: flush returns after staging, before the remote commit
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "async image" {
			break
		}
		if time.Now().After(deadline) {
			n, _ := b.Node("/f")
			t.Fatalf("async commit never landed: %+v", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAsyncWritebackFsyncIsDurabilityBarrier(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := asyncStack(t, b)
	ctx := context.Background()

	h := asyncOpen(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("barrier")); err != nil {
		t.Fatal(err)
	}
	// Sync both stages and waits for the queue to drain: on return the
	// bytes must already be durable — no polling.
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); !ok || string(n.Data) != "barrier" {
		t.Fatalf("backend after fsync = %+v", n)
	}
	_ = wb
}

func TestQueueCASRaceRetriesThenConflicts(t *testing.T) {
	b := testbackend.NewBackend()
	// First commit: one forced conflict, then the queue's auto-resolve
	// (stat 404 → create retry) succeeds.
	b.ForceConflict("/r", 1)
	// Second commit: more conflicts than the queue's retry budget →
	// terminal failure → staging.PendingConflict preserved.
	b.Seed("/c", []byte("base"))
	ch, wb := asyncStack(t, b)
	ctx := context.Background()

	h := asyncOpen(t, ch, "/r")
	if err := h.Write(ctx, 0, []byte("winner")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/r"); string(n.Data) != "winner" {
		t.Fatalf("retry commit = %q", n.Data)
	}

	b.ForceConflict("/c", 99)
	h2 := asyncOpen(t, ch, "/c")
	if err := h2.Write(ctx, 0, []byte("loser")); err != nil {
		t.Fatal(err)
	}
	_ = h2.Flush(ctx)
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for wb.queue.Snapshot().Conflicts < 1 {

		if time.Now().After(deadline) {
			t.Fatal("terminal conflict never recorded in pending index")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The pre-existing base content must be untouched by the failed commit.
	if n, _ := b.Node("/c"); string(n.Data) != "base" {
		t.Fatalf("base corrupted by failed commit: %q", n.Data)
	}
}

func TestLandedEventCarriesRevisionAndSize(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	bus := fs.NewEventBus()
	st, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Events: bus, Staging: st})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)

	var mu sync.Mutex
	var landed []fs.Event
	bus.On(fs.EventLanded, func(e fs.Event) {
		mu.Lock()
		landed = append(landed, e)
		mu.Unlock()
	})
	var staged []fs.Event
	bus.On(fs.EventStaged, func(e fs.Event) {
		mu.Lock()
		staged = append(staged, e)
		mu.Unlock()
	})
	var invalidated []fs.Event
	bus.On(fs.EventInvalidated, func(e fs.Event) {
		mu.Lock()
		invalidated = append(invalidated, e)
		mu.Unlock()
	})

	h := asyncOpen(t, ch, "/e")
	ctx := context.Background()
	if err := h.Write(ctx, 0, []byte("events")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(staged) != 1 || staged[0].Path != "/e" || staged[0].Size != 6 {
		t.Fatalf("staged = %+v", staged)
	}
	if len(landed) != 1 || landed[0].Path != "/e" || landed[0].Rev <= 0 || landed[0].Size != 6 {
		t.Fatalf("landed = %+v", landed)
	}
	// v1 onCommitQueueSuccess: a local async commit is not an external
	// change — no Invalidated emission, so listings and the kernel view stay
	// (the Landed event settles the committed entry in place).
	if len(invalidated) != 0 {
		t.Fatalf("local commit emitted Invalidated: %+v", invalidated)
	}
}

// TestLandedLineageDiscardsExactAncestor ports the core guarantee of
// pkg/fuse's commit_queue_lineage machinery: once this process has landed
// an exact snapshot, a replayed/older entry carrying that snapshot as an
// ancestor is discarded without another upload.
func TestLandedLineageDiscardsExactAncestor(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	st, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: st})
	if err != nil {
		t.Fatal(err)
	}
	cq := wb.queue

	// Land one snapshot of /l directly through the queue.
	if err := st.Shadow.WriteFull("/l", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	shadowGen := st.Shadow.ActiveGeneration("/l")
	pendingGen, err := st.Pending.PutWithBaseRev("/l", 2, staging.PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The live commit supersedes ancestor snapshot "snap-1".
	if err := cq.Enqueue(&CommitEntry{Path: "/l", Size: 2, Kind: staging.PendingNew, ShadowGen: shadowGen, PendingIndexGen: pendingGen, SnapshotID: "snap-2", liveAncestors: []string{"snap-1"}, liveLineageProof: true}); err != nil {
		t.Fatal(err)
	}
	if err := cq.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	puts := b.Requests()["PUT /v1/fs/l"]

	// Requeue a stale duplicate of the same image: the lineage proof must
	// discard it (after verifying the remote image) without a reupload.
	if err := st.Shadow.WriteFull("/l", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	shadowGen2 := st.Shadow.ActiveGeneration("/l")
	pendingGen2, err := st.Pending.PutWithBaseRevAndModeAndLineage("/l", 2, staging.PendingNew, 0, 0, false, "snap-1", "snap-1", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{Path: "/l", Size: 2, Kind: staging.PendingNew, ShadowGen: shadowGen2, PendingIndexGen: pendingGen2, SnapshotID: "snap-1", liveLineageProof: true}); err != nil {
		t.Fatal(err)
	}
	if err := cq.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/l"]; got != puts {
		t.Fatalf("stale ancestor re-uploaded: PUTs %d -> %d", puts, got)
	}
}
