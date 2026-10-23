package appendlog

// Review round S ports: a sibling's uncommitted overwrite INSIDE the
// truncated range is not a "pure truncator" just because the length did
// not change — it survives the truncator's landing (and lands on top of
// it); and an object this plane still holds after a rename out of the
// pattern keeps reporting its pending size/mode on Lookup/GetAttr (and
// takes path-level setattr through the claimed buffer).

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// h2 truncates abcdef to 2, h1 overwrites Y at offset 1 (aY, same length
// as the truncation), h2 lands first: h1's Y must not be retired as a pure
// truncation — its fsync lands aY on top of the truncator's ab.
func TestPortOverwriteInsideTruncatedRangeSurvivesTruncatorLanding(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h2, Size: 2, HasSize: true}); err != nil {
		t.Fatalf("h2 ftruncate(2) = %v", err)
	}
	if err := h1.Write(ctx, 1, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, h1, ctx, 0, 6); got != "aY" {
		t.Fatalf("h1 read = %q, want aY", got)
	}
	// The pure truncator lands first.
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("h2 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "ab" {
		t.Fatalf("/app.log after truncator landing = %q, want ab", node.Data)
	}
	// h1 still holds its overwrite and publishes it over the landed base.
	if got := mustRead(t, h1, ctx, 0, 6); got != "aY" {
		t.Fatalf("h1 read after truncator landing = %q, want aY (its write was retired as a pure truncation)", got)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "aY" {
		t.Fatalf("/app.log = %q, want aY (the confirmed overwrite was silently dropped)", node.Data)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "aY" {
		t.Fatalf("/app.log after closes = %q, want aY", node.Data)
	}
}

// An object this plane still holds after a rename out of the pattern keeps
// its pending state of record: after ftruncate on the renamed fd, GETATTR
// and LOOKUP of the new name report the pending size (not the lower plane's
// committed 6), and a path-level truncate of the new name goes through the
// claimed buffer too.
func TestPortRenamedClaimedObjectStatsReportPendingState(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app.log", Path2: "/app.txt"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.txt", Handle: h, Size: 2, HasSize: true})
	if err != nil {
		t.Fatalf("ftruncate(2) on the renamed fd = %v", err)
	}
	if res.Attr == nil || res.Attr.Size != 2 {
		t.Fatalf("setattr reply = %+v, want size 2", res.Attr)
	}
	for _, kind := range []fs.OpKind{fs.OpGetAttr, fs.OpLookup} {
		sres, serr := ch.Dispatch(ctx, &fs.Op{Kind: kind, Path: "/app.txt"})
		if serr != nil || sres.Attr == nil || sres.Attr.Size != 2 {
			t.Fatalf("%s of the renamed name = %+v err=%v, want the pending size 2", kind, sres.Attr, serr)
		}
	}
	// A path-level truncate of the renamed name hits the claimed buffer.
	res, err = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.txt", Size: 1, HasSize: true})
	if err != nil {
		t.Fatalf("path truncate(1) of the renamed name = %v", err)
	}
	if res.Attr == nil || res.Attr.Size != 1 {
		t.Fatalf("path setattr reply = %+v, want size 1", res.Attr)
	}
	if got := mustRead(t, h, ctx, 0, 6); got != "a" {
		t.Fatalf("fd read after path truncate = %q, want a", got)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync = %v", err)
	}
	if node, ok := b.Node("/app.txt"); !ok || string(node.Data) != "a" {
		t.Fatalf("/app.txt = %q ok=%v, want a", node.Data, ok)
	}
	sres, serr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/app.txt"})
	if serr != nil || sres.Attr == nil || sres.Attr.Size != 1 {
		t.Fatalf("getattr after landing = %+v err=%v, want 1", sres.Attr, serr)
	}
}
