package bulkfile

import (
	"context"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newStack builds [bulkfile → core] over the in-memory fstest backend.
func newStack(t *testing.T, b *testbackend.Backend) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	bf, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(bf, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func openForWrite(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	return res.Handle
}

func backendData(t *testing.T, b *testbackend.Backend, path string) (string, bool) {
	t.Helper()
	n, ok := b.Node(path)
	if !ok {
		return "", false
	}
	return string(n.Data), true
}

func TestSmallFileGoesOutAsOnePut(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/small.bin")
	if err := h.Write(ctx, 0, []byte("tiny")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/small.bin"); !ok || got != "tiny" {
		t.Fatalf("backend = %q %v", got, ok)
	}
	if _, hit := b.Requests()["POST /v2/uploads/initiate"]; hit {
		t.Fatal("small write started a multipart session")
	}
}

// TestFlushBeforeWriteDoesNotEBADF locks the regression behind the EC2
// fuse-smoke failure: a FLUSH arriving before the first WRITE (fork/dup
// close interleavings) must be a no-op, and the handle must keep accepting
// writes until Release commits them.
func TestFlushBeforeWriteDoesNotEBADF(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/race.txt")
	// flush-before-write: nothing dirty, no-op, no error
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("early flush: %v", err)
	}
	if err := h.Write(ctx, 0, []byte("create-abc")); err != nil {
		t.Fatalf("write after early flush: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/race.txt"); !ok || got != "create-abc" {
		t.Fatalf("backend = %q %v", got, ok)
	}
}

// TestWriteAfterFlushCommitsSecondCycle pins v1 ResetForNextWrite: the
// handle survives flush, later writes commit in a second CAS cycle, and a
// flush with no new writes is a no-op.
func TestWriteAfterFlushCommitsSecondCycle(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/cycle.txt")
	if err := h.Write(ctx, 0, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/cycle.txt"); !ok || got != "v1" {
		t.Fatalf("after first flush backend = %q %v", got, ok)
	}
	if err := h.Write(ctx, 2, []byte("-more")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := backendData(t, b, "/cycle.txt"); got != "v1-more" {
		t.Fatalf("after second flush backend = %q", got)
	}
	// double flush with no new writes: no extra PUT, no error
	puts := b.Requests()["PUT /v1/fs/cycle.txt"]
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/cycle.txt"]; got != puts {
		t.Fatalf("empty flush re-uploaded: %d → %d", puts, got)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestTruncateClaimKeepsRemoteUntilCommit pins v1 open(O_TRUNC) semantics:
// the truncation is local; the remote keeps its old content until the
// boundary commit CASes the new image onto the existing revision.
func TestTruncateClaimKeepsRemoteUntilCommit(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	seed := openForWrite(t, ch, "/over.txt")
	if err := seed.Write(ctx, 0, []byte("old-content")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}

	over := openForWrite(t, ch, "/over.txt") // O_CREAT|O_TRUNC claim on existing
	if got, _ := backendData(t, b, "/over.txt"); got != "old-content" {
		t.Fatalf("remote truncated early: %q", got)
	}
	if err := over.Write(ctx, 0, []byte("new-content")); err != nil {
		t.Fatal(err)
	}
	if err := over.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := backendData(t, b, "/over.txt"); got != "new-content" {
		t.Fatalf("after flush backend = %q", got)
	}
	_ = over.Release(ctx)
}

// TestEmptyTruncateClaimCommitsAtBoundary: opening an existing non-empty
// file with `>` and closing without writing must still truncate remotely
// (the truncation itself is a pending mutation).
func TestEmptyTruncateClaimCommitsAtBoundary(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	seed := openForWrite(t, ch, "/trunc.txt")
	if err := seed.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}

	h := openForWrite(t, ch, "/trunc.txt")
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/trunc.txt"); !ok || got != "" {
		t.Fatalf("backend after empty truncate close = %q %v", got, ok)
	}
	_ = h.Release(ctx)
}

// TestLargeImageGoesMultipart: above the negotiated inline threshold the
// commit travels the v2 multipart protocol exactly like v1's boundary
// uploads (nothing streams during Write).
func TestLargeImageGoesMultipart(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/big.bin")
	chunk := strings.Repeat("A", 64*1024)
	for i := 0; i < 24; i++ { // 1.5 MiB > 1 MiB inline threshold
		if err := h.Write(ctx, int64(i)*int64(len(chunk)), []byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if _, hit := b.Requests()["POST /v2/uploads/initiate"]; hit {
		t.Fatal("upload initiated during Write")
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/big.bin")
	if !ok || len(n.Data) != 24*64*1024 {
		for k, v := range b.Requests() {
			t.Logf("req %s = %d", k, v)
		}
		t.Fatalf("backend = %d bytes ok=%v", len(n.Data), ok)
	}
	if b.Requests()["POST /v2/uploads/initiate"] != 1 {
		t.Fatalf("initiate = %d", b.Requests()["POST /v2/uploads/initiate"])
	}
}

// TestNonSequentialWriteSplices: rewrites and holes splice into the image
// and the boundary commit lands one coherent file (v1 full-image fallback).
func TestNonSequentialWriteSplices(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/patch.bin")
	if err := h.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 128, []byte("X")); err != nil { // hole: zeros in between
		t.Fatal(err)
	}
	if err := h.Write(ctx, 1, []byte("EY")); err != nil { // rewrite mid-image
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/patch.bin")
	if len(n.Data) != 129 || string(n.Data[:5]) != "hEYlo" || n.Data[128] != 'X' {
		t.Fatalf("backend = %q", string(n.Data[:10]))
	}
}

func TestReadYourWrites(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/rw.bin")
	if err := h.Write(ctx, 0, []byte("hello stream")); err != nil {
		t.Fatal(err)
	}
	dest := make([]byte, 5)
	if n, err := h.Read(ctx, 6, dest); err != nil || string(dest[:n]) != "strea" {
		t.Fatalf("read = %d %q %v", n, dest[:n], err)
	}
	// reads keep working after a flush committed the image
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := h.Read(ctx, 0, dest); err != nil || string(dest[:n]) != "hello" {
		t.Fatalf("post-flush read = %d %q %v", n, dest[:n], err)
	}
	_ = h.Release(ctx)
}

// TestUnlinkDiscardsCommit: removing the path of a claimed open handle must
// not resurrect it at Release (v1 unlink-survivor).
func TestUnlinkDiscardsCommit(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/doomed.txt")
	if err := h.Write(ctx, 0, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/doomed.txt"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/doomed.txt"); ok {
		t.Fatalf("path resurrected: %q", string(n.Data))
	}
}

// TestUnlinkThenRecreate: a fresh writer on a re-created path must stay
// committable even though an older handle was unlinked (per-handle flag,
// v1 fh.Unlinked).
func TestUnlinkThenRecreate(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	old := openForWrite(t, ch, "/re.txt")
	if err := old.Write(ctx, 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/re.txt"}); err != nil {
		t.Fatal(err)
	}
	fresh := openForWrite(t, ch, "/re.txt")
	if err := fresh.Write(ctx, 0, []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/re.txt"); !ok || got != "fresh" {
		t.Fatalf("backend = %q %v", got, ok)
	}
	_ = old.Release(ctx)
	if got, _ := backendData(t, b, "/re.txt"); got != "fresh" {
		t.Fatalf("stale handle clobbered re-created path: %q", got)
	}
}

// TestRenameRetargetsHandle: a rename while the claimed handle is still
// open moves its eventual commit to the new path (v1 SetPath).
func TestRenameRetargetsHandle(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/from.txt")
	if err := h.Write(ctx, 0, []byte("moved")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/from.txt", Path2: "/to.txt"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/from.txt"); ok {
		t.Fatalf("old path resurrected: %q", string(n.Data))
	}
	if got, ok := backendData(t, b, "/to.txt"); !ok || got != "moved" {
		t.Fatalf("new path = %q %v", got, ok)
	}
}

// TestNoTruncFlagNotClaimed: a writable open without O_TRUNC belongs to the
// buffered write path below; bulkfile must pass it through untouched.
func TestNoTruncFlagNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	seed := openForWrite(t, ch, "/plain.txt")
	if err := seed.Write(ctx, 0, []byte("base")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/plain.txt", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	if _, isStream := res.Handle.(*streamHandle); isStream {
		t.Fatal("write without O_TRUNC must not be claimed by bulkfile")
	}
}

// TestClaimSeedsRevThroughLazyWriteback pins the production chain shape:
// writeback answers O_CREAT lazily with a synthetic revision, so bulkfile
// must stat the authoritative revision itself — an overwrite of an existing
// file commits with a real CAS base instead of failing create-only.
func TestClaimSeedsRevThroughLazyWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	bf, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(bf, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	seed := openForWrite(t, ch, "/lazy.bin")
	if err := seed.Write(ctx, 0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}

	over := openForWrite(t, ch, "/lazy.bin")
	if err := over.Write(ctx, 0, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := over.Flush(ctx); err != nil {
		t.Fatalf("overwrite commit: %v", err)
	}
	if got, _ := backendData(t, b, "/lazy.bin"); got != "second" {
		t.Fatalf("backend = %q", got)
	}
	_ = over.Release(ctx)
}

// TestSyncOpenNotClaimed: O_SYNC/O_DSYNC opens demand per-write durable
// commits; the buffered-image claim must pass them to the write path below.
func TestSyncOpenNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/sync.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC | syscall.O_SYNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, isStream := res.Handle.(*streamHandle); isStream {
		t.Fatal("O_SYNC open must not be claimed by bulkfile")
	}
}

// TestFtruncateOnHandleStaysLocal: a size change on a claimed handle edits
// the buffered image; the remote keeps the committed content until the next
// boundary lands the truncated image.
func TestFtruncateOnHandleStaysLocal(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/trunc-handle.txt")
	if err := h.Write(ctx, 0, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/trunc-handle.txt", Size: 4, Handle: h}); err != nil {
		t.Fatalf("ftruncate on handle: %v", err)
	}
	if got, _ := backendData(t, b, "/trunc-handle.txt"); got != "0123456789" {
		t.Fatalf("remote truncated early: %q", got)
	}
	dest := make([]byte, 8)
	if n, err := h.Read(ctx, 0, dest); err != nil || string(dest[:n]) != "0123" {
		t.Fatalf("image after ftruncate = %q %v", dest[:n], err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/trunc-handle.txt"); !ok || got != "0123" {
		t.Fatalf("backend after release = %q %v", got, ok)
	}
}

// TestChmodOnHandleAppliesBelow: a mode change on a claimed handle is
// forwarded to the write plane's pending handle instead of mutating the
// stale remote state through the chain.
func TestChmodOnHandleAppliesBelow(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/chmod-handle.txt")
	if err := h.Write(ctx, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/chmod-handle.txt", Mode: 0o600, Handle: h}); err != nil {
		t.Fatalf("fchmod on handle: %v", err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/chmod-handle.txt"); !ok || n.Mode&0o777 != 0o600 {
		t.Fatalf("mode after boundary commit = %v (ok=%v)", n.Mode&0o777, ok)
	}
	_ = h.Release(ctx)
}

// TestWriteSyncPolicyNotClaimed: a write-sync mount commits every write
// inline below; the buffered-image claim must stay out of the way.
func TestWriteSyncPolicyNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	bf, err := New(Options{Client: cl, Policy: "write-sync"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(bf, terminal)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/ws.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, isStream := res.Handle.(*streamHandle); isStream {
		t.Fatal("write-sync mount must not be claimed by bulkfile")
	}
}

// newWritebackStack builds the production chain shape
// [bulkfile → writeback → core] over the in-memory backend.
func newWritebackStack(t *testing.T, b *testbackend.Backend) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	bf, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(bf, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// TestFreshCreateThroughWritebackLands: the claim forwards its create to
// the write plane and the boundary push lands through its close-sync
// commit.
func TestFreshCreateThroughWritebackLands(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/wb.txt")
	if err := h.Write(ctx, 0, []byte("through")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/wb.txt"); !ok || got != "through" {
		t.Fatalf("backend = %q %v", got, ok)
	}
	_ = h.Release(ctx)
}

// TestUnlinkNeverFlushedThroughWriteback is the resurrection regression for
// delegation: a never-pushed claim is unlinked while both planes hold state
// (the buffered image here, the lazy pending create below); forwarding the
// unlink lets the plane drop its pending, and neither may publish at
// Release.
func TestUnlinkNeverFlushedThroughWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/doomed-wb.txt")
	if err := h.Write(ctx, 0, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/doomed-wb.txt"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/doomed-wb.txt"); ok {
		t.Fatalf("path resurrected: %q", string(n.Data))
	}
}

// TestEmptyTruncateThroughWriteback: `: > existing` with no writes must
// truncate through the write plane at the boundary (the shrink travels as
// a path-addressed size change, the empty image as nothing).
func TestEmptyTruncateThroughWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	seed := openForWrite(t, ch, "/wb-trunc.txt")
	if err := seed.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}

	h := openForWrite(t, ch, "/wb-trunc.txt")
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/wb-trunc.txt"); !ok || got != "" {
		t.Fatalf("backend after empty truncate close = %q %v", got, ok)
	}
	_ = h.Release(ctx)
}

// TestRenameThroughWriteback: a never-pushed claim renames through the
// write plane (its pending state re-keys) and the boundary lands at the new
// path only.
func TestRenameThroughWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/wb-from.txt")
	if err := h.Write(ctx, 0, []byte("moved")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/wb-from.txt", Path2: "/wb-to.txt"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/wb-from.txt"); ok {
		t.Fatalf("old path resurrected: %q", string(n.Data))
	}
	if got, ok := backendData(t, b, "/wb-to.txt"); !ok || got != "moved" {
		t.Fatalf("new path = %q %v", got, ok)
	}
}
