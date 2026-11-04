package writeback

import (
	"bytes"
	"context"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestPortExternalChangeDropsLandedWatermark pins the incarnation rule of
// the local landed table: the watermark is a SAME-ORIGIN proof, and a
// remote-origin change to the path ends it. A foreign delete+recreate
// leaves a replacement whose revision restarts LOW while a stale watermark
// sits high — adopting it would swap a fresh handle's correct base for the
// dead incarnation's revision and the upload would conflict forever.
func TestPortExternalChangeDropsLandedWatermark(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir(), Events: bus})
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
	ctx := context.Background()

	// landAndRecord runs one overwrite cycle and waits for BOTH the
	// backend content and the uploader's OnSuccess watermark (the backend
	// can expose content before the callback records the revision).
	landAndRecord := func(content string) {
		t.Helper()
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		h, ok := res.Handle.(*bufferingHandle)
		if !ok {
			t.Fatalf("handle = %T, want *bufferingHandle", res.Handle)
		}
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
				// "Any watermark" is deliberate: matching it against the node's
				// revision would couple the wait to the client's revision
				// bookkeeping (a conditional PUT answered without a parseable
				// revision falls back to expected+1, which never equals the
				// node's). Accepting a previous cycle's watermark is impossible
				// here — every cycle that re-polls runs after a clear
				// (EventInvalidated/EventUnverified/EventCheckout).
				if _, landed := wb.landedRevision("/f"); landed {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("cycle for %q never landed", content)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	landAndRecord("first!")
	// A remote-origin modification of the path ends the proof (the row is
	// no longer provably one this mount wrote).
	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/f"})
	if _, ok := wb.landedRevision("/f"); ok {
		t.Fatal("EventInvalidated did not drop the landed watermark")
	}

	landAndRecord("second")
	bus.Emit(fs.Event{Kind: fs.EventDelete, Path: "/f"})
	if _, ok := wb.landedRevision("/f"); ok {
		t.Fatal("EventDelete did not drop the landed watermark")
	}

	landAndRecord("third!")
	// A view checkout/reset clears the whole table: no landing is provably
	// this view's anymore.
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if _, ok := wb.landedRevision("/f"); ok {
		t.Fatal("EventCheckout did not clear the landed table")
	}
}

// TestPortWriteBackFlushStagesWithoutRemoteStat pins the #1026 alignment
// row for the small-file flush: v1's flush staged from local state only
// (snapshotWriteBackLocked fed the CAS base from the local committed
// watermark); the refactor added one synchronous remote HEAD per close to
// refresh the base. The flush must contact nobody — the local landed table
// is the only adoption source, and a foreign write (never in that table)
// stays a CAS conflict.
func TestPortWriteBackFlushStagesWithoutRemoteStat(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir()})
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
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle = %T, want *bufferingHandle", res.Handle)
	}
	if err := h.Write(ctx, 0, []byte("edited")); err != nil {
		t.Fatal(err)
	}

	before := make(map[string]int)
	for k, v := range b.Requests() {
		before[k] = v
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for k, v := range b.Requests() {
		if v > before[k] {
			t.Fatalf("flush used the remote: %s %d→%d", k, before[k], v)
		}
	}
	meta, ok := wb.wbCache.GetMeta("/f")
	if !ok {
		t.Fatal("flush did not stage a write-cache snapshot")
	}
	if meta.Kind != staging.PendingOverwrite {
		t.Fatalf("staged kind = %d, want overwrite of the seeded row", meta.Kind)
	}

	// Release submits the staged snapshot exactly once (v1's contract);
	// the landing itself is the uploader's async job.
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "edited" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("staged snapshot never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPortWriteBackFlushAdoptsOwnLandedRevision: the CAS base composes with
// THIS mount's own landed watermark (v1 adoptCommittedRevisionLocked) — a
// second overwrite cycle after our own async landing must stage against the
// landed revision without any remote stat, or its upload would 409 on the
// stale base forever.
func TestPortWriteBackFlushAdoptsOwnLandedRevision(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir()})
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
	ctx := context.Background()

	openExisting := func() *bufferingHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f",
			Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		h, ok := res.Handle.(*bufferingHandle)
		if !ok {
			t.Fatalf("handle = %T, want *bufferingHandle", res.Handle)
		}
		return h
	}

	// Cycle one: stage + land "first" asynchronously, then wait for BOTH
	// the backend content and the uploader's OnSuccess watermark — the
	// backend can expose the content before the callback records the
	// landed revision, and the next cycle's assertions read the table.
	h1 := openExisting()
	if err := h1.Write(ctx, 0, []byte("first!")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "first!" {
			if _, landed := wb.landedRevision("/f"); landed {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("first cycle never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Cycle two over the JUST-landed row: the open may or may not observe
	// the new revision (backend replica timing), but the flush must stage a
	// conditional base that composes with our own landing — never a remote
	// stat to fetch it.
	h2 := openExisting()
	if err := h2.Write(ctx, 0, []byte("second")); err != nil {
		t.Fatal(err)
	}
	before := make(map[string]int)
	for k, v := range b.Requests() {
		before[k] = v
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for k, v := range b.Requests() {
		if v > before[k] {
			t.Fatalf("second flush used the remote: %s %d→%d", k, before[k], v)
		}
	}
	meta, ok := wb.wbCache.GetMeta("/f")
	if !ok {
		t.Fatal("second flush did not stage")
	}
	if meta.BaseRev <= 0 {
		t.Fatalf("staged base rev = %d, want the landed revision adopted from the local watermark", meta.BaseRev)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "second" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("second cycle never landed (base rev %d)", meta.BaseRev)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPortUnverifiedStreamSuspendsWatermarkAdoption pins the disconnect
// window: while the change-event stream has lost continuity (ordinary file
// API still working), a foreign delete+recreate can replace a path with a
// LOW revision under a stale high watermark and no invalidation is ever
// observed. The watermark must neither survive the break nor be adopted
// during it — the open-time base of the CURRENT row stays authoritative,
// or the staged snapshot persists the dead incarnation's revision and its
// upload conflicts forever.
func TestPortUnverifiedStreamSuspendsWatermarkAdoption(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir(), Events: bus})
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
	ctx := context.Background()

	// Land our own image: the watermark records revision 2.
	cycle := func(content string) {
		t.Helper()
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		h := res.Handle.(*bufferingHandle)
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
				// Also wait for the uploader's callback: the backend can
				// expose the content before OnSuccess records the landed
				// revision the next assertions read.
				if _, landed := wb.landedRevision("/f"); landed {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("cycle for %q never landed", content)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	cycle("first!")
	if r, ok := wb.landedRevision("/f"); !ok || r <= 1 {
		t.Fatalf("watermark after cycle = %d %v, want revision > 1", r, ok)
	}

	// Stream loses continuity: the watermark must be dropped and adoption
	// suspended.
	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	if _, ok := wb.landedRevision("/f"); ok {
		t.Fatal("EventUnverified did not drop the landed watermark")
	}
	if !wb.unverified.Load() {
		t.Fatal("EventUnverified did not suspend watermark adoption")
	}

	// Foreign delete+recreate while disconnected: the replacement restarts
	// at revision 1. A fresh open records base 1; the flush must stage
	// against THAT, not the dead incarnation's watermark.
	b.Seed("/f", []byte("foreign"))
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("second!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	meta, ok := wb.wbCache.GetMeta("/f")
	if !ok {
		t.Fatal("flush did not stage during the unverified window")
	}
	if want := int64(1); meta.BaseRev != want {
		t.Fatalf("staged BaseRev = %d, want the current row's revision %d (the dead watermark was adopted)", meta.BaseRev, want)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "second!" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement image never landed — its base was poisoned")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Re-verified: adoption resumes and the table refills from our own
	// landings only.
	bus.Emit(fs.Event{Kind: fs.EventVerified, Path: "/"})
	if wb.unverified.Load() {
		t.Fatal("EventVerified did not resume watermark adoption")
	}
	cycle("third!!")
	if _, ok := wb.landedRevision("/f"); !ok {
		t.Fatal("watermark not re-recorded after the stream re-verified")
	}

	// A checkout alone must NOT resume: 401 view resets (core.mapErr) and
	// layer view switches emit EventCheckout while the SSE stream may
	// still be down — resuming on it would reopen the unverified adoption
	// window. Only the SSE reset's full sequence (handleReset emits
	// Checkout AND Verified once its catch-up completes) ends the
	// suspension.
	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	if !wb.unverified.Load() {
		t.Fatal("second EventUnverified did not suspend adoption")
	}
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if !wb.unverified.Load() {
		t.Fatal("an unrelated EventCheckout resumed the suspension")
	}
	bus.Emit(fs.Event{Kind: fs.EventVerified, Path: "/"})
	if wb.unverified.Load() {
		t.Fatal("EventVerified did not resume after the reset sequence")
	}
	cycle("fourth!")
	if _, ok := wb.landedRevision("/f"); !ok {
		t.Fatal("watermark not re-recorded after the reset resume")
	}
}

// TestPortMissingLineageDiscoversBase pins the baseRev==0 fallback: a
// second writable open of a live writer's path answers Attr.Rev 0 (#984
// stat visibility), leaving the handle with no CAS lineage. v1 fetched the
// authoritative revision at exactly this seam; without the fallback the
// writeback tier strands the staged entry (an overwrite without a base is
// rejected before upload and skipped by recovery) and close-sync degrades
// to an unconditional PUT.
func TestPortMissingLineageDiscoversBase(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir()})
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
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*bufferingHandle)
	// Model the lineage-less handle (the live-writer second open shape).
	h.mu.Lock()
	h.baseRev = 0
	h.mu.Unlock()
	if err := h.Write(ctx, 0, []byte("second!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	meta, ok := wb.wbCache.GetMeta("/f")
	if !ok {
		t.Fatal("flush did not stage")
	}
	if meta.BaseRev <= 0 {
		t.Fatalf("staged BaseRev = %d, want the discovered current revision", meta.BaseRev)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "second!" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lineage-less image never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPortDivergentBufferNotAdopted pins the adoption's lineage boundary:
// a buffer whose image does not extend this mount's landed bytes keeps
// its open-time base and surfaces as a conflict — advancing it to the
// landing's revision would erase the landing's edits without any 409 for
// the CRC gate to catch.
func TestPortDivergentBufferNotAdopted(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir()})
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
	ctx := context.Background()

	// H1 lands "winner" at revision R.
	open := func() *bufferingHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle.(*bufferingHandle)
	}
	h1 := open()
	if err := h1.Write(ctx, 0, []byte("winner")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "winner" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("winner never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	wonRev := func() int64 { n, _ := b.Node("/f"); return n.Rev }()

	// H2 buffered a DIVERGENT image from the pre-landing base (its stale
	// base models the open that raced H1's landing).
	h2 := open()
	h2.mu.Lock()
	h2.baseRev = 1
	h2.mu.Unlock()
	if err := h2.Write(ctx, 0, []byte("loser!")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	meta, ok := wb.wbCache.GetMeta("/f")
	if !ok {
		t.Fatal("divergent flush did not stage")
	}
	if meta.BaseRev >= wonRev {
		t.Fatalf("divergent buffer adopted base %d (>= winner's %d) — it must keep its own base and conflict", meta.BaseRev, wonRev)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n, _ := b.Node("/f"); string(n.Data) != "winner" {
		t.Fatalf("backend = %q, want the winner's image untouched", n.Data)
	}

	// The legitimate append still lands — through its OWN base, not
	// adoption: a fresh reopen reads the landing's revision at open, and
	// the appended image commits against it without any watermark
	// advancement. (A stale-based append whose image merely PREFIX-matches
	// the landing is exactly the divergent case above: prefix bytes are
	// not lineage, so it conflicts conservatively.)
	h3 := open()
	if err := h3.Write(ctx, 0, append([]byte("winner"), []byte("+more")...)); err != nil {
		t.Fatal(err)
	}
	if err := h3.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h3.Release(ctx); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "winner+more" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("fresh-base append never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPortFailedDiscoveryDoesNotStage pins the failed-HEAD seam: a
// lineage-less handle whose discovery probe fails transiently must not
// persist an unguarded PendingOverwrite — that entry would route
// close-sync through the legacy unconditional PUT and overwrite a
// foreign row.
func TestPortFailedDiscoveryDoesNotStage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seeded"))
	var failHead atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/f") && failHead.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCacheDir: t.TempDir()})
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
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*bufferingHandle)
	h.mu.Lock()
	h.baseRev = 0
	h.mu.Unlock()
	if err := h.Write(ctx, 0, []byte("guard!")); err != nil {
		t.Fatal(err)
	}
	failHead.Store(true)
	if err := h.Flush(ctx); err == nil {
		t.Fatal("flush must fail when the discovery HEAD fails")
	}
	failHead.Store(false)
	if _, ok := wb.wbCache.GetMeta("/f"); ok {
		t.Fatal("an unguarded entry was staged despite the failed discovery")
	}
	if n, _ := b.Node("/f"); string(n.Data) != "seeded" {
		t.Fatalf("backend = %q, want no unconditional overwrite", n.Data)
	}
	// Recovery of the failure: the retry stages with a discovered base.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "guard!" {
		t.Fatalf("backend = %q, want the retried landing", n.Data)
	}
	_ = h.Release(ctx)
}

// TestPortLandedBytesAccountingOnReplacement pins the retention ledger:
// replacing the OLDEST path's landing while the store sits near its budget
// must account the replaced image exactly once. The original bug left the
// replaced image set in the map after subtracting its bytes, so the
// eviction scan subtracted them again and the accounting drifted (reporting
// a fraction of the bytes actually retained).
func TestPortLandedBytesAccountingOnReplacement(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	retained := func() int64 {
		wb.landedMu.Lock()
		defer wb.landedMu.Unlock()
		total := int64(0)
		for _, info := range wb.landed {
			total += int64(len(info.image))
		}
		return total
	}

	// A: 600 KiB (the oldest), B: 400 KiB — exactly the 1 MiB budget.
	imgA := bytes.Repeat([]byte("A"), 600<<10)
	imgB := bytes.Repeat([]byte("B"), 400<<10)
	wb.landedAt("/a", 1, int64(len(imgA)), crc32.ChecksumIEEE(imgA), imgA)
	wb.landedAt("/b", 2, int64(len(imgB)), crc32.ChecksumIEEE(imgB), imgB)
	time.Sleep(2 * time.Millisecond)
	if got := retained(); got != 1024000 {
		t.Fatalf("retained after A+B = %d, want 1024000", got)
	}

	// Replace A (the oldest) with 800 KiB: eviction must select A's OLD
	// image exactly once. With the double-subtraction bug the accounting
	// ended at 600 KiB while 1.2 MiB→(800+400)=1.2MiB was retained.
	imgA2 := bytes.Repeat([]byte("C"), 800<<10)
	wb.landedAt("/a", 3, int64(len(imgA2)), crc32.ChecksumIEEE(imgA2), imgA2)

	wb.landedMu.Lock()
	accounted := wb.landedBytes
	wb.landedMu.Unlock()
	if accounted != retained() {
		t.Fatalf("accounted = %d, actually retained = %d (ledger drift)", accounted, retained())
	}
	if accounted > landedImageBudget {
		t.Fatalf("retained %d exceeds the %d budget", accounted, landedImageBudget)
	}
	// B's image survives the eviction of A's replacement only if it fits;
	// whatever the outcome, the two numbers must agree (checked above) and
	// no image may be double-counted.
}
