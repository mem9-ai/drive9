package writeback

import (
	"context"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newPatchStack builds [writeback(close-sync) → nothing] over the
// in-memory backend; commits land synchronously at the boundary.
func newPatchStack(t *testing.T, b *testbackend.Backend) (*Writeback, *client.Client, *fs.Chain) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	w, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(w, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return w, cl, ch
}

// seedLargeFile lands an n-byte file through the stack itself.
func seedLargeFile(t *testing.T, ch *fs.Chain, path string, n int64, fill byte) {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	const chunk = 256 << 10
	buf := make([]byte, chunk)
	for i := range buf {
		buf[i] = fill
	}
	var off int64
	for off < n {
		w := int64(len(buf))
		if off+w > n {
			w = n - off
		}
		if err := h.Write(context.Background(), off, buf[:w]); err != nil {
			t.Fatal(err)
		}
		off += w
	}
	if err := h.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// countPartUploads sums recorded requests whose key starts with the given
// prefix (upload ids embed a sequence, so keys differ per request).
func countPartUploads(b *testbackend.Backend, prefix string) int {
	total := 0
	for k, v := range b.Requests() {
		if strings.HasPrefix(k, prefix) {
			total += v
		}
	}
	return total
}

// TestPartialEditPatchesOnlyDirtyParts: editing a few bytes inside one
// part of a large existing file commits as a patch — one PATCH plan, one
// dirty-part upload, no full rewrite.
func TestPartialEditPatchesOnlyDirtyParts(t *testing.T) {
	b := testbackend.NewBackend()
	_, _, ch := newPatchStack(t, b)
	ctx := context.Background()
	// 9 MiB = 2 parts at the buffer's 8 MiB part size; the edit lands in
	// part 2.
	seedLargeFile(t, ch, "/big.bin", 9<<20, 'A')
	initiates := b.Requests()["POST /v2/uploads/initiate"]

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 8<<20+123, []byte("PATCHED")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if got := b.Requests()["PATCH /v1/fs/big.bin"]; got != 1 {
		t.Fatalf("patch plans = %d, want 1", got)
	}
	uploads := countPartUploads(b, "PUT /v2/uploads/patch-")
	if uploads != 1 {
		t.Fatalf("part uploads = %d, want 1 (only the dirty part)", uploads)
	}
	if got := b.Requests()["POST /v2/uploads/initiate"]; got != initiates {
		t.Fatalf("full rewrite initiated: %d -> %d", initiates, got)
	}
	n, ok := b.Node("/big.bin")
	if !ok || len(n.Data) != 9<<20 || string(n.Data[8<<20+120:8<<20+130]) != "AAAPATCHED" {
		t.Fatalf("data wrong: len=%d around=%q", len(n.Data), n.Data[8<<20+120:8<<20+130])
	}
}

// TestPatchUnsupportedFallsBackFullWrite: a server that rejects partial
// updates for the object's storage class must get the edit as a full
// write; the rejection is learned per handle, so the next edit on the
// same handle does not patch again.
func TestPatchUnsupportedFallsBackFullWrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.PatchUnsupported = true
	_, _, ch := newPatchStack(t, b)
	ctx := context.Background()
	seedLargeFile(t, ch, "/nodb9.bin", 9<<20, 'B')

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/nodb9.bin", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 5, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PATCH /v1/fs/nodb9.bin"]; got != 1 {
		t.Fatalf("patch attempts = %d, want 1", got)
	}
	if n, ok := b.Node("/nodb9.bin"); !ok || string(n.Data[5:10]) != "first" {
		t.Fatalf("full-write fallback data wrong: %q", n.Data[5:10])
	}
	// second edit on the SAME handle: the learned class suppresses the
	// patch, the edit lands as a full write again.
	if err := h.Write(ctx, 7, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PATCH /v1/fs/nodb9.bin"]; got != 1 {
		t.Fatalf("patch attempted again after rejection: %d", got)
	}
	if n, _ := b.Node("/nodb9.bin"); string(n.Data[7:13]) != "second" {
		t.Fatalf("second edit data wrong: %q", n.Data[7:13])
	}
}

// TestSmallEditStaysDirectPUT: below the inline threshold the edit lands
// as one conditional PUT; no patch is planned.
func TestSmallEditStaysDirectPUT(t *testing.T) {
	b := testbackend.NewBackend()
	_, _, ch := newPatchStack(t, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/small.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("hello patch world")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}

	res, err = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/small.txt", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 6, []byte("PATCH")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if got := b.Requests()["PATCH /v1/fs/small.txt"]; got != 0 {
		t.Fatalf("small file patched: %d", got)
	}
	if n, ok := b.Node("/small.txt"); !ok || string(n.Data) != "hello PATCH world" {
		t.Fatalf("data = %q", n.Data)
	}
}

// TestGrowthPastOriginalWritesFull: growing beyond the original size has
// no object to copy the tail from; the commit is a full write.
func TestGrowthPastOriginalWritesFull(t *testing.T) {
	b := testbackend.NewBackend()
	_, _, ch := newPatchStack(t, b)
	ctx := context.Background()
	seedLargeFile(t, ch, "/grow.bin", 9<<20, 'C')

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/grow.bin", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 9<<20, []byte("TAIL")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if got := b.Requests()["PATCH /v1/fs/grow.bin"]; got != 0 {
		t.Fatalf("growth patched: %d", got)
	}
	if n, ok := b.Node("/grow.bin"); !ok || len(n.Data) != 9<<20+4 || string(n.Data[9<<20:]) != "TAIL" {
		t.Fatalf("grown data wrong: len=%d", len(n.Data))
	}
}

// TestWriteSyncPerWriteEditsPatch: the per-write sync track (write-sync
// policy) is the worst full-rewrite offender; its partial edits must
// patch too.
func TestWriteSyncPerWriteEditsPatch(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	w, err := New(Options{Policy: PolicyWriteSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(w, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	seedLargeFile(t, ch, "/ws.bin", 9<<20, 'D')

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/ws.bin", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 8<<20+7, []byte("WS")); err != nil {
		t.Fatal(err)
	}

	if got := b.Requests()["PATCH /v1/fs/ws.bin"]; got != 1 {
		t.Fatalf("per-write sync patch plans = %d, want 1", got)
	}
	if uploads := countPartUploads(b, "PUT /v2/uploads/patch-"); uploads != 1 {
		t.Fatalf("per-write sync part uploads = %d, want 1", uploads)
	}
	if n, _ := b.Node("/ws.bin"); n.Data[8<<20+6] != 'D' || n.Data[8<<20+7] != 'W' || n.Data[8<<20+8] != 'S' || n.Data[8<<20+9] != 'D' {
		t.Fatalf("data wrong: %q", n.Data[8<<20+6:8<<20+10])
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
}
