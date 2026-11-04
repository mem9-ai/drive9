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

	// landAndRecord runs one overwrite cycle and waits for the uploader's
	// success to record the same-origin watermark.
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
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("cycle for %q never landed", content)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, ok := wb.landedRevision("/f"); !ok {
			t.Fatal("landed watermark not recorded after a successful cycle")
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

	// Cycle one: stage + land "first" asynchronously, then wait for the
	// uploader's success to record the landed watermark.
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
			break
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
