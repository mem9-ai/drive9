package writeback

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// newConflictStack builds [writeback → core] over the counting backend with
// the small-file cache track armed, returning the plane for direct
// staging/upload exercising.
func newConflictStack(t *testing.T) (*testbackend.Backend, *Writeback, *fs.Chain, *client.Client) {
	t.Helper()
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCacheDir: t.TempDir(), Events: fs.NewEventBus()})
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
	return b, wb, ch, cl
}

// openExisting opens the seeded file read-write through the chain.
func openExisting(t *testing.T, ch *fs.Chain, path string) *bufferingHandle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle = %T, want *bufferingHandle", res.Handle)
	}
	return h
}

// TestPortConflictedEntryRebasesOntoOwnLanding pins the duplicate-commit
// race repair: the kernel's asynchronous RELEASE can race a handoff flush
// of the same handle — both drive a close-sync commit, the winner lands a
// new revision, and the loser's upload conflicts on the pre-landing base.
// The loser must rebase onto the winner's revision (the landed watermark's
// same-origin proof) and land, instead of failing its flush — which
// surfaced as EIO on the open(2) that triggered the handoff — and leaving
// a pending entry that retries forever.
func TestPortConflictedEntryRebasesOntoOwnLanding(t *testing.T) {
	b, wb, ch, _ := newConflictStack(t)
	ctx := context.Background()

	// The race winner: one overwrite cycle lands revision 2 and records
	// the watermark.
	h := openExisting(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("winner!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "winner!" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("winner cycle never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r, ok := wb.landedRevision("/f"); !ok || r <= 1 {
		t.Fatalf("landed watermark = %d %v, want the winner's revision > 1", r, ok)
	}

	// The loser: an entry staged at the PRE-landing base (exactly what the
	// racing flush captured before the winner landed).
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("loser!!"), 7, staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	// Its upload conflicts on base 1, rebases onto the winner's revision,
	// and lands — the flush that routed here succeeds instead of EIO.
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err != nil {
		t.Fatalf("conflicted entry was not repaired: %v", err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "loser!!" {
		t.Fatalf("backend = %q, want the repaired loser image", n.Data)
	}
	if _, ok := wb.wbCache.GetMeta("/f"); ok {
		t.Fatal("repaired entry survived in the write cache")
	}
}

// TestPortForeignConflictStaysPending pins the repair's safety boundary: a
// conflict whose newer revision this mount did NOT record (a foreign
// write) must NOT rebase — adopting it would let the stale image overwrite
// the foreign bytes — and the entry stays pending, exactly as before.
func TestPortForeignConflictStaysPending(t *testing.T) {
	b, wb, ch, cl := newConflictStack(t)
	ctx := context.Background()

	// Land our own revision first (watermark r2), then stage a loser at
	// base 1 — but let a FOREIGN write advance the row past r2 (re-seeding
	// the node models another client's delete+recreate; the mount never
	// observes it, so its watermark stays at r2).
	h := openExisting(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("mine!!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "mine!!" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("own cycle never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Advance the row past the watermark from OUTSIDE the mount (a direct
	// client write models a foreign writer), and let the mount observe the
	// change — the invalidation drops its same-origin watermark, so the
	// conflicted entry has nothing it may rebase onto.
	if err := cl.WriteCtx(ctx, "/f", []byte("foreign-two")); err != nil {
		t.Fatal(err)
	}
	wb.opts.Events.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/f"})

	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("loser!!"), 7, staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err == nil {
		t.Fatal("a foreign-advanced conflict must not rebase onto the watermark")
	}
	if meta, ok := wb.wbCache.GetMeta("/f"); !ok || meta.BaseRev != 1 {
		t.Fatalf("pending entry = %+v ok=%t, want it retained at its original base", meta, ok)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "foreign-two" {
		t.Fatalf("backend = %q, want the foreign image untouched", n.Data)
	}
}

// TestPortConflictRepairSuspendedWhileUnverified pins the resolver's
// continuity restriction: while the event stream is unverified the landed
// table may hold revisions recorded DURING the gap (uploads keep
// succeeding and OnSuccess keeps landing), and a foreign recreation whose
// events have not arrived could sit at a coincidentally equal revision.
// A conflicted entry must NOT rebase then — it stays pending — and the
// repair resumes once the stream re-verifies.
func TestPortConflictRepairSuspendedWhileUnverified(t *testing.T) {
	b, wb, ch, _ := newConflictStack(t)
	ctx := context.Background()
	land := func(content string) {
		t.Helper()
		h := openExisting(t, ch, "/f")
		if err := h.Write(ctx, 0, []byte(content)); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			if n, ok := b.Node("/f"); ok && string(n.Data) == content {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("cycle for %q never landed", content)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	land("first!") // revision 2, watermark recorded
	wb.opts.Events.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	land("gap!!!") // revision 3; the watermark REFILLS during the gap
	if r, ok := wb.landedRevision("/f"); !ok || r <= 2 {
		t.Fatalf("watermark during gap = %d %v, want the just-landed revision", r, ok)
	}

	// A duplicate-commit loser staged at the pre-gap base: its upload
	// conflicts, and the repair must refuse the watermark while
	// continuity is unverified.
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("loser!!"), 7, staging.PendingOverwrite, 2, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err == nil {
		t.Fatal("conflict repair must stay suspended while the stream is unverified")
	}
	if meta, ok := wb.wbCache.GetMeta("/f"); !ok || meta.BaseRev != 2 {
		t.Fatalf("suspended entry = %+v ok=%t, want it retained at its original base", meta, ok)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "gap!!!" {
		t.Fatalf("backend = %q, want the gap image untouched", n.Data)
	}

	// Re-verified: the same conflict repairs onto our own landing.
	wb.opts.Events.Emit(fs.Event{Kind: fs.EventVerified, Path: "/"})
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err != nil {
		t.Fatalf("repair did not resume after EventVerified: %v", err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "loser!!" {
		t.Fatalf("backend = %q, want the repaired image", n.Data)
	}
	if _, ok := wb.wbCache.GetMeta("/f"); ok {
		t.Fatal("repaired entry survived in the write cache")
	}
}
