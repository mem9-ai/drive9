package extent

// The large-file takeover's staging claim: the rows below port the
// buffered-claim semantics of the bulk data plane this extension subsumed
// (whole-image buffering, boundary pushes, unlink/rename survival), then
// lock the takeover itself — the claim decision, the overlay lookups, and
// the promotion fallback against a server without the extent plane.

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

// noThreshold keeps the ported buffering rows free of promotion attempts;
// promotion behavior has its own rows below.
const noThreshold = int64(1 << 30)

// newTakeoverStack builds [extent → core] over the in-memory fstest
// backend with the large-file takeover armed at threshold.
func newTakeoverStack(t *testing.T, b *testbackend.Backend, threshold int64) (*fs.Chain, *Extent) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Client: cl, LargeFileThreshold: threshold})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, ext
}

// newWritebackStack builds the production chain shape
// [extent → writeback → core] over the in-memory backend.
func newWritebackStack(t *testing.T, b *testbackend.Backend, threshold int64) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Client: cl, LargeFileThreshold: threshold})
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
	ch, err := fs.NewChain(ext, wb, terminal)
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
	ch, _ := newTakeoverStack(t, b, noThreshold)
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
	ch, _ := newTakeoverStack(t, b, noThreshold)
	ctx := context.Background()

	h := openForWrite(t, ch, "/race.txt")
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

// TestWriteAfterFlushCommitsSecondCycle pins the flush-survival contract:
// the handle survives flush, later writes commit in a second cycle, and a
// flush with no new writes is a no-op.
func TestWriteAfterFlushCommitsSecondCycle(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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

// TestTruncateClaimKeepsRemoteUntilCommit pins open(O_TRUNC) semantics: the
// truncation is local; the remote keeps its old content until the boundary
// commit CASes the new image onto the existing revision. An existing
// classic layout never converts to the extent plane.
func TestTruncateClaimKeepsRemoteUntilCommit(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 8)
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
// file with `>` and closing without writing must still truncate remotely.
func TestEmptyTruncateClaimCommitsAtBoundary(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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

// TestLargeImageGoesMultipartWhileUnpromotable: above the inline threshold
// but with promotion unavailable (fake backend has no credential
// endpoint), the claim falls back to the classic multipart upload at the
// boundary exactly like the bulk plane it replaced.
func TestLargeImageGoesMultipartWhileUnpromotable(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 64*1024)
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
// and the boundary commit lands one coherent file.
func TestNonSequentialWriteSplices(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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
	ch, _ := newTakeoverStack(t, b, noThreshold)
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

// TestStagingOverlayLookup: a live claim answers lookups from its image —
// the buffered file is the file until it promotes or releases.
func TestStagingOverlayLookup(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
	ctx := context.Background()

	h := openForWrite(t, ch, "/overlay.txt")
	if err := h.Write(ctx, 0, []byte("growing")); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/overlay.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 7 {
		t.Fatalf("overlay attr = %+v", res.Attr)
	}
	// Path-addressed reads serve the image too.
	data, rerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/overlay.txt", Offset: 1, Limit: 3})
	if rerr != nil || string(data.Data) != "row" {
		t.Fatalf("path read = %q %v", data.Data, rerr)
	}
	_ = h.Release(ctx)
}

// TestUnlinkDiscardsCommit: removing the path of a claimed open handle must
// not resurrect it at Release.
func TestUnlinkDiscardsCommit(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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
// committable even though an older handle was unlinked.
func TestUnlinkThenRecreate(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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
// open moves its eventual commit to the new path.
func TestRenameRetargetsHandle(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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
// buffered write path below; the takeover must pass it through untouched.
func TestNoTruncFlagNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 64)
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
	if _, isStaging := res.Handle.(*stagingHandle); isStaging {
		t.Fatal("write without O_TRUNC must not be claimed")
	}
}

// TestSyncOpenNotClaimed: O_SYNC/O_DSYNC opens demand per-write durable
// commits; the buffered-image claim must pass them to the write path below.
func TestSyncOpenNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 64)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/sync.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC | syscall.O_SYNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, isStaging := res.Handle.(*stagingHandle); isStaging {
		t.Fatal("O_SYNC open must not be claimed")
	}
}

// TestAppendOpenNotClaimed: an appender's offset discipline belongs to the
// plane that owns the existing bytes.
func TestAppendOpenNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 64)

	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/app.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC | syscall.O_APPEND), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, isStaging := res.Handle.(*stagingHandle); isStaging {
		t.Fatal("O_APPEND open must not be claimed")
	}
}

// TestWriteSyncPolicyNotClaimed: a write-sync mount commits every write
// inline below; the buffered-image claim must stay out of the way.
func TestWriteSyncPolicyNotClaimed(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Client: cl, LargeFileThreshold: 64, Policy: "write-sync"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, terminal)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/ws.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, isStaging := res.Handle.(*stagingHandle); isStaging {
		t.Fatal("write-sync mount must not be claimed")
	}
}

// TestClaimsLargeFileDecision locks the takeover claim decision.
func TestClaimsLargeFileDecision(t *testing.T) {
	fresh := uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC)
	ext := &Extent{opts: Options{LargeFileThreshold: 4096}}
	if !ext.claimsLargeFile(&fs.Op{Path: "/a.bin", Flags: fresh}) {
		t.Fatal("fresh truncate-writer must be claimed")
	}
	ext = &Extent{opts: Options{LargeFileThreshold: 0}}
	if ext.claimsLargeFile(&fs.Op{Path: "/a.bin", Flags: fresh}) {
		t.Fatal("zero threshold disables the takeover")
	}
	ext = &Extent{opts: Options{LargeFileThreshold: 4096, Policy: "write-sync"}}
	if ext.claimsLargeFile(&fs.Op{Path: "/a.bin", Flags: fresh}) {
		t.Fatal("write-sync disables the takeover")
	}
	ext = &Extent{opts: Options{LargeFileThreshold: 4096, ExtentPaths: []string{"*.bin"}}}
	if ext.claimsLargeFile(&fs.Op{Path: "/a.bin", Flags: fresh}) {
		t.Fatal("a declared path takes the byte-zero extent claim, not the takeover")
	}
	ext = &Extent{opts: Options{LargeFileThreshold: 4096, LargeFileExempt: func(p string) bool { return p == "/wal.db-wal" }}}
	if ext.claimsLargeFile(&fs.Op{Path: "/wal.db-wal", Flags: fresh}) {
		t.Fatal("an exempt path belongs to the plane inside")
	}
	if !ext.claimsLargeFile(&fs.Op{Path: "/plain.txt", Flags: fresh}) {
		t.Fatal("non-exempt paths stay claimed")
	}
}

// TestPromotionFallsBackToClassic: against a server without the extent
// plane (no credential endpoint), crossing the threshold must not fail the
// write — the claim parks on the classic plane and the boundary lands
// there. The mount-level hold keeps the second claim from re-probing the
// credential endpoint within the window.
func TestPromotionFallsBackToClassic(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 16)
	ctx := context.Background()

	h := openForWrite(t, ch, "/promote.txt")
	if err := h.Write(ctx, 0, []byte("0123456789abcdefXYZ")); err != nil {
		t.Fatalf("threshold-crossing write: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/promote.txt"); !ok || got != "0123456789abcdefXYZ" {
		t.Fatalf("backend = %q %v", got, ok)
	}

	// The hold window: a second claim's promotion attempt skips the
	// credential probe entirely.
	h2 := openForWrite(t, ch, "/promote2.txt")
	if err := h2.Write(ctx, 0, []byte("0123456789abcdefXYZ")); err != nil {
		t.Fatal(err)
	}
	_ = h2.Release(ctx)
	if got := b.Requests()["POST /v1/data-credential"]; got != 1 {
		t.Fatalf("credential probes = %d, want 1 (hold window must suppress re-probes)", got)
	}
}

// TestClaimStatDiscoversExtentLayout: the takeover claim's stat carries the
// layout; an extent-layout target is remembered even though the runtime
// cannot build here (the open then falls to the classic plane).
func TestClaimStatDiscoversExtentLayout(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedExtent("/big.bin", []byte("x"), 77)
	ch, ext := newTakeoverStack(t, b, 64)

	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if ino, ok := ext.paths.cachedIno("/big.bin"); !ok || ino != 77 {
		t.Fatalf("claim stat discovered ino = %d %v, want 77", ino, ok)
	}
	if res.Handle == nil {
		t.Fatal("classic fallback must still open a handle")
	}
}

// TestFtruncateOnHandleStaysLocal: a size change on a claimed handle edits
// the buffered image; the remote keeps the committed content until the next
// boundary lands the truncated image.
func TestFtruncateOnHandleStaysLocal(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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
// forwarded to the write plane's pending state instead of mutating the
// stale remote state through the chain.
func TestChmodOnHandleAppliesBelow(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
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

// TestClaimSeedsRevThroughLazyWriteback pins the production chain shape:
// writeback answers O_CREAT lazily with a synthetic revision, so the claim
// must stat the authoritative revision itself — an overwrite of an existing
// file commits with a real CAS base instead of failing create-only.
func TestClaimSeedsRevThroughLazyWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b, noThreshold)
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

// TestFreshCreateThroughWritebackLands: the claim forwards its create to
// the write plane and the boundary push lands through its close-sync
// commit.
func TestFreshCreateThroughWritebackLands(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b, noThreshold)
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
	ch := newWritebackStack(t, b, noThreshold)
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
// truncate through the write plane at the boundary.
func TestEmptyTruncateThroughWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b, noThreshold)
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
	ch := newWritebackStack(t, b, noThreshold)
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

// TestLegacyLargeFileStaysClassicReadWrite locks the legacy contract: an
// existing classic-layout file (the presign/multipart direct-upload plane
// large files used before the takeover) keeps reading through the classic
// paths, its edits keep writing through them, and a `>` rewrite never
// converts the layout — the claim buffers the truncated image and the
// boundary commit travels the classic multipart protocol.
func TestLegacyLargeFileStaysClassicReadWrite(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, 64*1024)
	ctx := context.Background()

	// Seed a legacy classic file large enough that any rewrite crossing
	// the takeover threshold would promote a fresh path.
	seed := openForWrite(t, ch, "/legacy.bin")
	chunk := strings.Repeat("B", 64*1024)
	for i := 0; i < 8; i++ { // 512 KiB
		if err := seed.Write(ctx, int64(i)*int64(len(chunk)), []byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/legacy.bin")
	if !ok || n.ContentLayout != "" {
		t.Fatalf("legacy layout = %q ok=%v", n.ContentLayout, ok)
	}

	// Reads of the legacy file keep serving through the classic plane.
	data, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/legacy.bin", Offset: 0, Limit: 4})
	if err != nil || string(data.Data) != "BBBB" {
		t.Fatalf("legacy read = %q %v", data.Data, err)
	}

	// An edit open without O_TRUNC is not the takeover's claim at all:
	// the write plane below owns it and the appended bytes land classically.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/legacy.bin", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, isStaging := res.Handle.(*stagingHandle); isStaging {
		t.Fatal("edit open without O_TRUNC must not be claimed")
	}
	if err := res.Handle.Write(ctx, 512*1024, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := backendData(t, b, "/legacy.bin"); !strings.HasSuffix(got, "-tail") || len(got) != 512*1024+5 {
		t.Fatalf("legacy edit = %d bytes, suffix %q", len(got), got[len(got)-16:])
	}

	// `> legacy.bin` rewrites classically: the buffered image commits
	// through the write plane below as one multipart upload (above the
	// inline threshold), and the layout never converts to extent.
	over := openForWrite(t, ch, "/legacy.bin")
	rewrite := strings.Repeat("A", 24*64*1024) // 1.5 MiB
	if err := over.Write(ctx, 0, []byte(rewrite)); err != nil {
		t.Fatal(err)
	}
	if err := over.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_ = over.Release(ctx)

	if got, ok := backendData(t, b, "/legacy.bin"); !ok || got != rewrite {
		t.Fatalf("rewrite content len = %d ok=%v", len(got), ok)
	}
	if got := b.Requests()["POST /v2/uploads/initiate"]; got != 1 {
		t.Fatalf("initiate = %d, want 1 (legacy rewrite must stay multipart)", got)
	}
	if n, ok := b.Node("/legacy.bin"); !ok || n.ContentLayout != "" {
		t.Fatalf("layout after rewrite = %q ok=%v (must stay classic)", n.ContentLayout, ok)
	}
}

// newClaimHandle builds a bare staging claim over a core-only below chain:
// the forwarding paths (push + SetAttr need a real plane below) without
// any takeover arming (threshold 0, not promotable).
func newClaimHandle(t *testing.T) *stagingHandle {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	belowChain, err := fs.NewChain(terminal)
	if err != nil {
		t.Fatal(err)
	}
	h := &stagingHandle{e: ext, path: "/repo/sparse.bin", below: fs.HandlerFilesystem(belowChain.Dispatch),
		openFlags: uint32(syscall.O_CREAT | syscall.O_RDWR)}
	ext.paths.trackStaging(h)
	t.Cleanup(func() { ext.paths.untrackStaging(h) })
	return h
}

// A sparse write past the memory bound forwards to the plane below instead
// of allocating the whole logical image (one byte at 64 MiB must not
// allocate 67 MB).
func TestSparseWriteForwardsPastBound(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 64<<20, []byte("x")); err != nil {
		t.Fatalf("sparse write: %v", err)
	}
	h.mu.Lock()
	buf := len(h.image)
	pt := h.passthrough
	h.mu.Unlock()
	if !pt {
		t.Fatalf("write past the bound did not forward (passthrough=false)")
	}
	if buf != 0 {
		t.Fatalf("image retained %d bytes after forwarding", buf)
	}
}

// A large ftruncate expresses the size below instead of allocating zeros.
func TestLargeTruncateForwardsPastBound(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	size := int64(64 << 20)
	if _, err := h.applyAttr(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Size: size}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	h.mu.Lock()
	buf := len(h.image)
	h.mu.Unlock()
	if buf > 1<<20 {
		t.Fatalf("image retained %d bytes after large truncate", buf)
	}
}

// A read-only re-open of a live claim serves the claim's pending image,
// never the not-yet-pushed remote bytes (the tier-transition overwrite
// read a mixed old/new image).
func TestClaimReopenServesPendingImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/t.bin", []byte(strings.Repeat("A", 1<<20)))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Client: cl, Policy: "writeback", LargeFileThreshold: noThreshold})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(ext, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/t.bin",
		Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte(strings.Repeat("B", 1<<20))); err != nil {
		t.Fatal(err)
	}
	ro, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/t.bin", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err := ro.Handle.Read(ctx, 512<<10, got); err != nil {
		t.Fatal(err)
	}
	if string(got[:32]) != strings.Repeat("B", 32) {
		t.Fatalf("re-open over live claim = %q, want the pending B image", got[:16])
	}
}

// An existing claim reader keeps serving after the writer's stream forwards
// past the memory bound (the emptied image answered (0, nil)).
func TestClaimReaderSurvivesPassthrough(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatal(err)
	}
	r := &claimReadHandle{claim: h}
	buf := make([]byte, 4)
	if n, err := r.Read(context.Background(), 0, buf); err != nil || string(buf[:n]) != "head" {
		t.Fatalf("pre-passthrough read = %q n=%d err=%v", buf[:n], n, err)
	}
	if err := h.Write(context.Background(), stagingMaxBufferedBytes+1, []byte("x")); err != nil {
		t.Fatalf("overflow write: %v", err)
	}
	if n, err := r.Read(context.Background(), 0, buf); err != nil || string(buf[:n]) != "head" {
		t.Fatalf("post-passthrough read = %q n=%d err=%v", buf[:n], n, err)
	}
}

// A handle-based truncate after the handoff reaches the plane below instead
// of comparing against the emptied image (size-0 skipped).
func TestPassthroughSetAttrTruncatesBelow(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(context.Background(), stagingMaxBufferedBytes+1, []byte("x")); err != nil {
		t.Fatalf("overflow write: %v", err)
	}
	if _, err := h.applyAttr(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Size: 0, HasMode: false}); err != nil {
		t.Fatalf("passthrough truncate: %v", err)
	}
	h.mu.Lock()
	pt, img := h.passthrough, len(h.image)
	h.mu.Unlock()
	if !pt || img != 0 {
		t.Fatalf("handle state pt=%v image=%d", pt, img)
	}
	// The file's real size lives below: verify through the plane.
	if st, err := h.below.GetAttr(context.Background(), h.path); err != nil {
		t.Fatalf("below stat: %v", err)
	} else if st.Size != 0 {
		t.Fatalf("below size after truncate = %d, want 0", st.Size)
	}
}

// TestTakeoverMountDiscoversExtentLayoutOnFreshMount locks the remount
// path: a takeover-only mount (no declarations, no runtime yet — exactly a
// fresh mount of a volume where a previous mount promoted files) must
// still discover extent-layout stats and remember their inodes, so opens
// route to the extent plane instead of the classic read the server
// rejects with 422.
func TestTakeoverMountDiscoversExtentLayoutOnFreshMount(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedExtent("/promoted.bin", []byte("x"), 77)
	ch, ext := newTakeoverStack(t, b, 64)

	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/promoted.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 1 {
		t.Fatalf("attr = %+v", res.Attr)
	}
	if ino, ok := ext.paths.cachedIno("/promoted.bin"); !ok || ino != 77 {
		t.Fatalf("fresh-mount discovery ino = %d %v, want 77", ino, ok)
	}
}

// TestRenameOverLiveClaimAnonymizesDestination: the rename replaces the
// destination, so its open claim goes anonymous — later lookups stop
// answering from its image and its Release cannot clobber the renamed-in
// file with the stale image.
func TestRenameOverLiveClaimAnonymizesDestination(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newTakeoverStack(t, b, noThreshold)
	ctx := context.Background()

	dst := openForWrite(t, ch, "/dst.bin")
	if err := dst.Write(ctx, 0, []byte("old-dst")); err != nil {
		t.Fatal(err)
	}
	if err := dst.Flush(ctx); err != nil { // lands remotely
		t.Fatal(err)
	}
	src := openForWrite(t, ch, "/src.bin")
	if err := src.Write(ctx, 0, []byte("src-content")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/src.bin", Path2: "/dst.bin"}); err != nil {
		t.Fatal(err)
	}
	if err := dst.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := src.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := backendData(t, b, "/dst.bin"); !ok || got != "src-content" {
		t.Fatalf("destination after rename = %q %v, want src-content", got, ok)
	}
	if _, ok := b.Node("/src.bin"); ok {
		t.Fatal("source path resurrected")
	}
}
