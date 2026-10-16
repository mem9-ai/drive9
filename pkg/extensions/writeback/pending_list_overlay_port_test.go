package writeback

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// #918's listing half (the port of v1's resolve-dirty-and-pending before
// directory conversion): a file whose newest image is staged but not landed
// must publish its pending size through OpList — the stat path already
// answers from pendingAttr, and a listing that races a cache invalidation
// would otherwise re-publish the pre-write remote image while the commit is
// still in flight.
func TestPortListingServesPendingSpillAttrs(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/sib.txt", []byte("sib"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.Close() })
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, cerr := fs.NewChain(wb, terminal)
	if cerr != nil {
		t.Fatal(cerr)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("pending-bytes")); err != nil {
		t.Fatal(err)
	}
	// Flush stages the image; the upload stays queued (no Drain yet).
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	pending := int64(len("pending-bytes"))
	for _, row := range listRoot(t, ch) {
		switch row.Name {
		case "f":
			if row.Attr.Size != pending {
				t.Fatalf("pending listing size = %d, want %d", row.Attr.Size, pending)
			}
		case "sib.txt":
			if row.Attr.Size != 3 {
				t.Fatalf("sibling listing size = %d, want 3", row.Attr.Size)
			}
		}
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); !ok || string(n.Data) != "pending-bytes" {
		t.Fatalf("remote image after drain = %q ok=%v", n.Data, ok)
	}
}

// The live-writer half: a buffered image that has not even reached staging
// yet (the handle is still open) must still publish its buffered size —
// v1 snapshotted published write-back metadata per listing.
func TestPortListingServesLiveWriterSize(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/live.bin").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("still-in-buffer")); err != nil {
		t.Fatal(err)
	}
	for _, row := range listRoot(t, ch) {
		if row.Name == "live.bin" {
			if row.Attr.Size != int64(len("still-in-buffer")) {
				t.Fatalf("live listing size = %d, want %d", row.Attr.Size, len("still-in-buffer"))
			}
			return
		}
	}
	t.Fatal("listing did not carry live.bin")
}

func listRoot(t *testing.T, ch *fs.Chain) []fs.Entry {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpList, Path: "/"})
	if err != nil {
		t.Fatal(err)
	}
	return res.Entries
}

func mustStaging(t *testing.T) *staging.Staging {
	t.Helper()
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.Close() })
	return ledger
}
