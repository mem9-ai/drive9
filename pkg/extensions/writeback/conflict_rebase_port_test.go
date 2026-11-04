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

	// The loser: the SAME image staged at the PRE-landing base (exactly
	// what the racing duplicate flush captured before the winner landed).
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("winner!"), 7, staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	// Its upload conflicts on base 1, proves itself the winner's duplicate
	// (size + CRC against the landed record), rebases onto the winner's
	// revision, and lands — the flush that routed here succeeds instead of
	// EIO.
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err != nil {
		t.Fatalf("conflicted duplicate was not repaired: %v", err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "winner!" {
		t.Fatalf("backend = %q, want the repaired image", n.Data)
	}
	if _, ok := wb.wbCache.GetMeta("/f"); ok {
		t.Fatal("repaired entry survived in the write cache")
	}
}

// TestPortDivergentConflictStaysPending pins the repair's payload gate:
// two handles staging DIFFERENT whole-file images from one base both
// conflict when the first lands, but the watermark only proves the mount
// wrote the winning revision — not that the loser's image is its
// duplicate. Rebase-and-overwrite would lose the winner's bytes, so the
// divergent entry stays pending like any other divergent writer.
func TestPortDivergentConflictStaysPending(t *testing.T) {
	b, wb, ch, _ := newConflictStack(t)
	ctx := context.Background()

	h := openExisting(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("first!!")); err != nil {
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
		if n, ok := b.Node("/f"); ok && string(n.Data) == "first!!" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("winner cycle never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A divergent image staged at the same pre-landing base: same size
	// class, different bytes.
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("second!"), 7, staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err == nil {
		t.Fatal("a divergent conflicted image must not rebase onto the winner")
	}
	if meta, ok := wb.wbCache.GetMeta("/f"); !ok || meta.BaseRev != 1 {
		t.Fatalf("divergent entry = %+v ok=%t, want it retained at its original base", meta, ok)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "first!!" {
		t.Fatalf("backend = %q, want the winner's image untouched", n.Data)
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

	// A duplicate-commit loser — the SAME image the gap landing wrote —
	// staged at the pre-gap base: its upload conflicts, and the repair
	// must refuse the watermark while continuity is unverified.
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("gap!!!"), 6, staging.PendingOverwrite, 2, 0o644, false); err != nil {
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

	// Re-verified: the same duplicate conflict repairs onto our own
	// landing.
	wb.opts.Events.Emit(fs.Event{Kind: fs.EventVerified, Path: "/"})
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err != nil {
		t.Fatalf("repair did not resume after EventVerified: %v", err)
	}
	if _, ok := wb.wbCache.GetMeta("/f"); ok {
		t.Fatal("repaired entry survived in the write cache")
	}
}

// TestPortRecoveryOwnershipSnapshot pins the recovery split's snapshot
// rule: a crash can leave BOTH a pending-index entry and an older .dat/
// .meta snapshot for one path (the ledger put landed but the enqueue's
// cache removal did not). The queue starts workers inside its own
// RecoverPending; if the uploader's filter consulted the LIVE pending
// index afterwards, a path the queue had already landed and removed would
// pass and get the stale snapshot submitted as a second owner — a
// permanent conflict or a same-origin rebase republishing stale bytes.
// The ownership set is captured before either track starts, so the stale
// snapshot stays put.
func TestPortRecoveryOwnershipSnapshot(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir(), Staging: ledger})
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

	// The ledger track's state: a durable shadow image plus its pending
	// entry (what a crashed enqueue leaves behind).
	if err := ledger.Shadow.WriteFull("/f", []byte("ledger-image"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRevAndMode("/f", 12, staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	// The stale .dat/.meta snapshot the flush left for the same path.
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", []byte("stale-snapshot"), 14, staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}

	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The queue recovers its own entry and lands it; the uploader must
	// never submit the stale snapshot, and the queue's success RETIRES it
	// — it must neither serve reads nor survive into a later recovery.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := wb.wbCache.GetMeta("/f"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale snapshot was not retired after the ledger landing")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "ledger-image" {
		t.Fatalf("backend = %q, want the ledger image alone", n.Data)
	}
	// Public read behavior after the queue cleanup: a read-only reopen
	// must see the recovered successor, never the retired snapshot.
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	n, err := res.Handle.Read(context.Background(), 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "ledger-image" {
		t.Fatalf("post-cleanup read = %q, want the recovered image", got)
	}
	// And a restart's recovery finds nothing left to double-own.
	if m := wb.wbCache.ListPendingMetas(); len(m) != 0 {
		t.Fatalf("pending metas after retirement = %v, want none", m)
	}
}

// TestPortUnknownIdentityLandingRefusesRebase pins the proof's known-bytes
// requirement: landings that record no payload bytes (the ledger track,
// oversized images) can prove NOTHING — not a size match, not a checksum,
// not the revision alone. A conflicted same-size entry must stay pending
// after such a landing instead of rebasing on size-only evidence.
func TestPortUnknownIdentityLandingRefusesRebase(t *testing.T) {
	b, wb, ch, _ := newConflictStack(t)
	ctx := context.Background()

	h := openExisting(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("first!!")); err != nil {
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
		if n, ok := b.Node("/f"); ok && string(n.Data) == "first!!" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("winner cycle never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Overwrite the landing record with an identity-less one (the ledger
	// track's shape: revision known, bytes not retained).
	wb.landedMu.Lock()
	if info, ok := wb.landed["/f"]; ok {
		info.image = nil
		wb.landed["/f"] = info
	}
	wb.landedMu.Unlock()

	// A same-size, same-checksum-class duplicate staged at the stale base:
	// the unknown identity must refuse the rebase.
	dup := []byte("first!!")
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", dup, int64(len(dup)), staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err == nil {
		t.Fatal("an identity-less landing must not authorize a rebase")
	}
	if meta, ok := wb.wbCache.GetMeta("/f"); !ok || meta.BaseRev != 1 {
		t.Fatalf("entry = %+v ok=%t, want it retained at its original base", meta, ok)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "first!!" {
		t.Fatalf("backend = %q, want the winner's image untouched", n.Data)
	}
}

// TestPortRecoveryIntervalReadsServeLedgerImage pins the ownership of reads
// through the whole recovery interval, including a queue that has not (yet)
// landed: a crashed enqueue can leave a ledger entry G2 plus an older
// same-length .dat/.meta pair G1; while the ledger owns the path, a
// read-only reopen must capture the LEDGER image (or nothing), never the
// superseded cache pair that merely matches its size.
func TestPortRecoveryIntervalReadsServeLedgerImage(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir(), Staging: ledger})
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

	// The crashed-enqueue shape: a newer ledger image plus an older,
	// same-length superseded cache pair (equal length is the point — the
	// old cache-first selection accepted a pair purely by length match).
	ledgerImage := []byte("ledger-image") // 12 bytes
	staleImage := []byte("stale-image!")  // 12 bytes
	if err := ledger.Shadow.WriteFull("/f", ledgerImage, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRevAndMode("/f", int64(len(ledgerImage)), staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if err := wb.wbCache.PutWithBaseRevAndMode("/f", staleImage, int64(len(staleImage)), staging.PendingOverwrite, 1, 0o644, false); err != nil {
		t.Fatal(err)
	}

	// A read-only reopen while the queue has not landed: the captured
	// image must be the ledger's, never the same-length stale pair.
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, rerr := res.Handle.Read(context.Background(), 0, buf)
	if rerr != nil && rerr.Error() != "EOF" {
		t.Fatalf("read during recovery interval: %v", rerr)
	}
	if got := string(buf[:n]); got == string(staleImage) {
		t.Fatalf("recovery-interval read served the superseded cache pair %q", got)
	} else if got != string(ledgerImage) {
		t.Fatalf("recovery-interval read = %q, want the ledger image %q", got, ledgerImage)
	}
	// The stale pair itself is untouched until the queue's success
	// retires it — only its read authority is gone.
	if data, ok := wb.wbCache.GetView("/f"); !ok || string(data) != string(staleImage) {
		t.Fatalf("stale pair = %q ok=%t, want it retained until retirement", data, ok)
	}

	// A WRITABLE reopen through the same interval initializes from the
	// ledger image too (the shadow branch), never from the cache pair:
	// read-your-writes on the new descriptor must see the ledger bytes.
	wres, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_RDWR | syscall.O_CREAT)})
	if err != nil {
		t.Fatal(err)
	}
	// The first write triggers the base initialization; reading back
	// afterwards must show the LEDGER image underneath the write (not
	// zeros, and not the stale cache pair).
	if err := wres.Handle.Write(context.Background(), 3, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	wbuf := make([]byte, len(ledgerImage))
	n, werr := wres.Handle.Read(context.Background(), 0, wbuf)
	if werr != nil && werr.Error() != "EOF" {
		t.Fatalf("writable reopen read: %v", werr)
	}
	want := string(ledgerImage)
	want = want[:3] + "Z" + want[4:]
	if got := string(wbuf[:n]); got != want {
		t.Fatalf("writable reopen read = %q, want the ledger image with the write %q", got, want)
	}
}

// TestPortSyncLandingRefusesLedgerOwnedPendingNew pins the uploader's
// synchronous boundary on the create shape: a crashed enqueue can leave a
// ledger PendingNew entry plus an older cache pair for a path with NO
// backend row. Both synchronous entrypoints must refuse the path (no
// expect-absent create publishing the stale pair, no cache consumption),
// leaving the queue's recovery as the only landing track.
func TestPortSyncLandingRefusesLedgerOwnedPendingNew(t *testing.T) {
	b := testbackend.NewBackend() // no /g row
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir(), Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.NewChain(wb, terminal); err != nil {
		t.Fatal(err)
	}

	ledgerImage := []byte("ledger-new")
	staleImage := []byte("stale-old") // same length, superseded bytes
	if err := ledger.Shadow.WriteFull("/g", ledgerImage, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRevAndMode("/g", int64(len(ledgerImage)), staging.PendingNew, 0, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if err := wb.wbCache.PutWithBaseRevAndMode("/g", staleImage, int64(len(staleImage)), staging.PendingNew, 0, 0o644, false); err != nil {
		t.Fatal(err)
	}

	// Both synchronous entrypoints refuse before any PUT or cache use.
	ctx := context.Background()
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/g"); !wb.uploader.IsLedgerOwnedError(err) {
		t.Fatalf("UploadSyncWithRevision err = %v, want the ledger-ownership refusal", err)
	}
	if _, _, err := wb.uploader.uploadSyncWithMetadata(ctx, "/g"); !wb.uploader.IsLedgerOwnedError(err) {
		t.Fatalf("uploadSyncWithMetadata err = %v, want the ledger-ownership refusal", err)
	}
	// Nothing landed, nothing consumed: no row, no requests, the cache
	// pair byte-identical for the queue's own recovery.
	if _, ok := b.Node("/g"); ok {
		t.Fatal("a row was created despite the refusal")
	}
	if puts := b.Requests()["PUT /v1/fs/g"]; puts != 0 {
		t.Fatalf("%d PUTs issued despite the refusal", puts)
	}
	if data, ok := wb.wbCache.GetView("/g"); !ok || string(data) != string(staleImage) {
		t.Fatalf("cache pair = %q ok=%t, want it untouched", data, ok)
	}
}
