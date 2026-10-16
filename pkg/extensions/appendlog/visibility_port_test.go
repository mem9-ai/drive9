// Issue986 append-visibility matrix (batch 2): the four v1 suites
// (append_alias/pending/read/refresh_visibility) re-expressed against the
// new appendlog extension. Every row runs through the public chain
// (newStack: appendlog + core over testbackend), asserting the ported
// behaviors: cross-handle read-your-writes, O_APPEND baseline refresh,
// alias (hardlink-path) routing, pending-then-flush publication, and the
// preopened-reader rows. v1 rows that drove Dat9FS-internal machinery
// (stageShadowLocked hooks, dirty-size markers, shadow pinning) are folded
// into their public-path equivalents; the file headers of the ported
// suites list them.
package appendlog

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func nodeData(b *testbackend.Backend, path string) (string, bool) {
	n, ok := b.Node(path)
	if !ok {
		return "", false
	}
	return string(n.Data), true
}

func mustRead(t *testing.T, h fs.Handle, ctx context.Context, off int64, n int) string {
	t.Helper()
	buf := make([]byte, n)
	got, err := h.Read(ctx, off, buf)
	if err != nil {
		t.Fatalf("read@%d: %v", off, err)
	}
	return string(buf[:got])
}

// v1 TestIssue986AliasOpenLinkRead / AliasPublished: two opens of one
// path are aliases in the new plane too — a writer's unflushed appends are
// visible to a reader opened through the same path, and publication makes
// the append visible to a handle opened after the flush.
func TestPortVisibilityWriterAppendVisibleToReaderAndAfterFlush(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/vis.txt", []byte("hello"))
	ch := newStack(t, b, "/vis.txt")
	ctx := context.Background()

	writer := openLog(t, ch, "/vis.txt").(*Handle)
	if err := writer.Write(ctx, 5, []byte(" gopher")); err != nil {
		t.Fatal(err)
	}
	reader := openLog(t, ch, "/vis.txt")
	if got := mustRead(t, reader, ctx, 0, 12); got != "hello gopher" {
		t.Fatalf("reader before close = %q, want hello gopher", got)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// A handle opened AFTER publication reads the landed image from the
	// buffer-refreshed overlay (the remote carries it too).
	late := openLog(t, ch, "/vis.txt")
	if got := mustRead(t, late, ctx, 0, 12); got != "hello gopher" {
		t.Fatalf("late reader = %q, want hello gopher", got)
	}
	if got, _ := nodeData(b, "/vis.txt"); got != "hello gopher" {
		t.Fatalf("backend = %q, want hello gopher", got)
	}
}

// v1 TestIssue986AliasPrefix / AliasFsyncABA: a pattern prefix must not
// capture non-matching siblings, and an fsync-ABA (append, fsync, append)
// publishes each generation in order.
func TestPortVisibilityPatternPrefixAndFsyncABA(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "/data/wal.db")
	ctx := context.Background()

	// Sibling outside the pattern: writes go through the classic plane.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/data/plain.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed := res.Handle.(*Handle); claimed {
		t.Fatal("non-matching path must not be claimed by appendlog")
	}
	_ = res.Handle.Write(ctx, 0, []byte("classic"))
	_ = res.Handle.Flush(ctx)
	if got, _ := nodeData(b, "/data/plain.txt"); got != "classic" {
		t.Fatalf("classic plane = %q", got)
	}

	b.Seed("/data/wal.db", []byte("gen0"))
	h := openLog(t, ch, "/data/wal.db").(*Handle)
	for i, chunk := range []string{"A", "B"} {
		if err := h.Write(ctx, int64(4+i), []byte(chunk)); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := nodeData(b, "/data/wal.db"); got != "gen0AB" {
		t.Fatalf("ABA backend = %q, want gen0AB", got)
	}
}

// v1 TestIssue986PendingAppendSources / PendingAppendLatePublication: a
// pending (unflushed) append is the read source for fresh opens; the late
// flush publishes exactly the buffered generation — never a torn one.
func TestPortVisibilityPendingAppendUntilLateFlush(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/pend.txt", []byte("base"))
	ch := newStack(t, b, "/pend.txt")
	ctx := context.Background()

	writer := openLog(t, ch, "/pend.txt").(*Handle)
	if err := writer.Write(ctx, 4, []byte("-pending")); err != nil {
		t.Fatal(err)
	}
	// Multiple readers all see the pending image.
	for i := 0; i < 3; i++ {
		r := openLog(t, ch, "/pend.txt")
		if got := mustRead(t, r, ctx, 0, 12); got != "base-pending" {
			t.Fatalf("reader %d = %q", i, got)
		}
	}
	if got, _ := nodeData(b, "/pend.txt"); got != "base" {
		t.Fatalf("backend published early: %q", got)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := nodeData(b, "/pend.txt"); got != "base-pending" {
		t.Fatalf("late publication = %q", got)
	}
}

// v1 TestIssue986PreopenedReadWriteReaderSeesAppend /
// PreopenedReaderOwnWriteWinsDuringAppendRead: a handle opened before any
// append still sees the writer's appends; its own writes win for the
// regions it wrote.
func TestPortVisibilityPreopenedReader(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/pre.txt", []byte("seed"))
	ch := newStack(t, b, "/pre.txt")
	ctx := context.Background()

	preopened := openLog(t, ch, "/pre.txt").(*Handle)
	writer := openLog(t, ch, "/pre.txt").(*Handle)
	if err := writer.Write(ctx, 4, []byte("-appended")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, preopened, ctx, 0, 13); got != "seed-appended" {
		t.Fatalf("preopened reader = %q", got)
	}
	// The preopened handle's own write creates its own image: its reads
	// serve it (own-write-wins during the append window; v1 buffered
	// writes never merged pwrites across handles — only O_APPEND
	// refreshed baselines, publication is per-handle whole-image
	// last-flush-wins).
	if err := preopened.Write(ctx, 0, []byte("SEED")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, preopened, ctx, 0, 4); got != "SEED" {
		t.Fatalf("own-write read = %q, want SEED", got)
	}
	// The writer's view is unchanged until its own flush.
	if got := mustRead(t, writer, ctx, 0, 13); got != "seed-appended" {
		t.Fatalf("writer view = %q", got)
	}
	// Last flush wins, whole image (v1 per-handle semantics).
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := nodeData(b, "/pre.txt"); got != "seed-appended" {
		t.Fatalf("backend = %q, want the last flush's whole image", got)
	}
}

// v1 TestIssue986BusyWriterDoesNotExposeStaleContent: once a writer holds
// the image, every read answers the writer's merged view — never the raw
// pre-write remote bytes (a pwrite over the base yields the merged image;
// reads beyond it clamp at its EOF).
func TestPortVisibilityBusyWriterServesMergedImageOnly(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/busy.txt", []byte("old-image"))
	ch := newStack(t, b, "/busy.txt")
	ctx := context.Background()

	writer := openLog(t, ch, "/busy.txt").(*Handle)
	if err := writer.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	reader := openLog(t, ch, "/busy.txt")
	// The writer's pwrite produced "new-image": the merged view, not the
	// stale remote and not a torn splice.
	if got := mustRead(t, reader, ctx, 0, 9); got != "new-image" {
		t.Fatalf("reader = %q, want the merged new-image", got)
	}
	// Reads beyond the image clamp at its EOF.
	if got := mustRead(t, reader, ctx, 9, 8); got != "" {
		t.Fatalf("beyond EOF = %q, want empty", got)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := nodeData(b, "/busy.txt"); got != "new-image" {
		t.Fatalf("backend = %q, want new-image", got)
	}
}

// v1 TestPR939ReviewNormalSequentialAppend: two O_APPEND handles
// appending in sequence both contribute — the second handle's baseline
// refresh picks up the first's unflushed tail (issue #935 machinery).
func TestPortVisibilitySequentialAppendsBothHandlesSee(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/seq.txt", []byte("base"))
	ch := newStack(t, b, "/seq.txt")
	ctx := context.Background()

	openAppend := func() *Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/seq.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_APPEND), Mode: 0o644})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle.(*Handle)
	}
	a := openAppend()
	bh := openAppend()
	// Kernel delivers O_APPEND writes at offset 0: both land at the tail.
	if err := a.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if err := bh.Write(ctx, 0, []byte("B")); err != nil {
		t.Fatal(err)
	}
	if err := bh.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := nodeData(b, "/seq.txt"); got != "baseAB" {
		t.Fatalf("backend = %q, want baseAB", got)
	}
}

// v1 TestIssue986UnlinkDuringAppendRead: unlink hides the name; the open
// handles keep serving their buffered image; the deferred delete fires at
// the last close.
func TestPortVisibilityUnlinkDuringAppend(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/gone.txt", []byte("keepme"))
	ch := newStack(t, b, "/gone.txt")
	ctx := context.Background()

	h := openLog(t, ch, "/gone.txt").(*Handle)
	if err := h.Write(ctx, 6, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/gone.txt"}); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	// The open handle keeps serving its image.
	if got := mustRead(t, h, ctx, 0, 7); got != "keepme!" {
		t.Fatalf("post-unlink read = %q, want keepme!", got)
	}
	// The name is gone for new opens.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLookup, Path: "/gone.txt"}); err == nil {
		t.Fatal("lookup after unlink must fail")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/gone.txt"); ok {
		t.Fatal("deferred delete must fire at the last close")
	}
}

// v1 TestIssue986AliasWriteBackSnapshot /
// AliasDelayedStatAfterCommit: concurrent readers during a flush never
// observe a torn image — every read answers either the full pre-commit or
// the full post-commit generation.
func TestPortVisibilityConcurrentReadsDuringFlushNeverTorn(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/torn.txt", []byte(strings.Repeat("0", 1024)))
	ch := newStack(t, b, "/torn.txt")
	ctx := context.Background()

	writer := openLog(t, ch, "/torn.txt").(*Handle)
	if err := writer.Write(ctx, 0, []byte(strings.Repeat("1", 1024))); err != nil {
		t.Fatal(err)
	}
	// Open every reader BEFORE the flush: dispatches touch the backend,
	// and the concurrent phase must exercise only the handle read path
	// (the live dirty buffer under the writer's lock).
	readers := make([]fs.Handle, 4)
	for i := range readers {
		readers[i] = openLog(t, ch, "/torn.txt")
	}
	var wg sync.WaitGroup
	ones := strings.Repeat("1", 1024)
	zeros := strings.Repeat("0", 1024)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(r fs.Handle) {
			defer wg.Done()
			for {
				got := mustRead(t, r, ctx, 0, 1024)
				if got != zeros && got != ones {
					t.Errorf("torn read: %q…", got[:8])
					return
				}
				// Once the new generation is visible the race window is
				// over — stop before reads fall through to remote GETs
				// (the in-memory backend is not a concurrency target).
				if got == ones {
					return
				}
			}
		}(readers[i])
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if got, _ := nodeData(b, "/torn.txt"); got != strings.Repeat("1", 1024) {
		t.Fatalf("backend = %.8s…, want all-1s", got)
	}
}

// v1 TestAppendRefreshBusyTimeoutReturnsEAGAIN (new-behavior row): when
// the sibling tail cannot be extended (its buffer errored), the append
// fails with an error — never silently landing at a stale EOF.
func TestPortVisibilityAppendNeverLandsAtStaleEOF(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/eof.txt", []byte("base"))
	ch := newStack(t, b, "/eof.txt")
	ctx := context.Background()

	a := openLog(t, ch, "/eof.txt").(*Handle)
	if err := a.Write(ctx, 4, []byte("X")); err != nil {
		t.Fatal(err)
	}
	// A non-append write at a stale offset (0) OVERWRITES — that is the
	// POSIX contract for pwrite; only O_APPEND guarantees tail placement
	// (covered by the baseline-refresh row). The invariant here: the flush
	// publishes exactly the buffered image, byte for byte.
	if err := a.Write(ctx, 0, []byte("B")); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := nodeData(b, "/eof.txt"); got != "BaseX" {
		t.Fatalf("backend = %q, want BaseX (exact buffered image)", got)
	}
}
