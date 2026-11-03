package appendlog

// Ported from v1 pkg/fuse/sqlite_truncate_test.go and
// sqlite_truncate_pending_test.go: truncate routing and timing for SQLite
// sidecars — a pattern-matched WAL truncates locally through Handle.Truncate
// (remote landing deferred to the flush), the rotation after a truncate
// never re-uploads the old WAL image, non-pattern sidecars truncate
// remotely through the terminal at once, a busy sibling's truncate followed
// by the original handle's new generation publishes only the new
// generation, and a second open continues from the first handle's landed
// baseline.

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// walFrame builds an opaque WAL frame payload (header already separate).
func walFrame(fill byte) []byte {
	frame := make([]byte, 24+4096)
	for i := 24; i < len(frame); i++ {
		frame[i] = fill
	}
	return frame
}

// v1 truncate-timing rows: ftruncate on a live append-log handle is local —
// the SetAttr returns at once and the remote object only changes at the
// next flush boundary.
func TestPortSQLiteSidecarTruncateTimingGoesThroughHandle(t *testing.T) {
	b := testbackend.NewBackend()
	image := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2), walFrame('A')...)
	b.SeedWithLayout("/workload.db-wal", image, "append_log")
	ch := newStack(t, b, "**/workload.db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/workload.db-wal").(*Handle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/workload.db-wal", Size: 0}); err != nil {
		t.Fatal(err)
	}
	// Local now: the remote image survives until the flush boundary.
	if node, _ := b.Node("/workload.db-wal"); len(node.Data) != len(image) {
		t.Fatalf("remote changed before flush: %d bytes, want %d", len(node.Data), len(image))
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/workload.db-wal"); len(node.Data) != 0 {
		t.Fatalf("remote after flush = %d bytes, want 0", len(node.Data))
	}
	// Reads through the handle observe the truncation immediately.
	buf := make([]byte, 1)
	if n, err := h.Read(ctx, 0, buf); err != nil || n != 0 {
		t.Fatalf("post-truncate read = n=%d err=%v, want EOF", n, err)
	}
}

// v1 TestSQLiteSidecarTruncateFollowsLinkedFile (append-log rows): after a
// zero truncate, writing the new generation's header and first frame lands
// through bounded requests — the old WAL image is never re-uploaded, and the
// first frame goes through the append endpoint.
func TestPortSQLiteSidecarTruncateThenRotateNeverReuploadsOldWAL(t *testing.T) {
	b := testbackend.NewBackend()
	oldImage := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2), walFrame('A')...)
	b.SeedWithLayout("/workload.db-wal", oldImage, "append_log")
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	frame := walFrame('B')
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/workload.db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/workload.db-wal").(*Handle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/workload.db-wal", Size: 0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, newHeader); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, walHeaderSize, frame); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	want := append(append([]byte(nil), newHeader...), frame...)
	if node, _ := b.Node("/workload.db-wal"); !bytes.Equal(node.Data, want) {
		t.Fatalf("backend after rotation = %d bytes, want the new generation (%d)", len(node.Data), len(want))
	}
	for _, put := range rec.snapshot() {
		if int64(len(put.body)) > walHeaderSize {
			t.Fatalf("full WAL rewrite after truncate: PUT of %d bytes", len(put.body))
		}
	}
	if got := b.Requests()["POST /v1/fs/workload.db-wal"]; got != 1 {
		t.Fatalf("first-frame appends = %d, want 1", got)
	}
}

// v1 non-pattern sidecar rows: a sidecar outside the append-log declarations
// truncates through the terminal's generic setattr — remotely and at once,
// no handle or flush needed.
func TestPortSQLiteSidecarWithoutPatternTruncatesImmediately(t *testing.T) {
	for _, suffix := range []string{"-journal", "-shm"} {
		t.Run(suffix, func(t *testing.T) {
			b := testbackend.NewBackend()
			path := "/workload.db" + suffix
			b.Seed(path, []byte("old sidecar bytes"))
			ch := newStack(t, b, "**/workload.db-wal") // the sidecar never matches

			if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: path, Size: 0}); err != nil {
				t.Fatal(err)
			}
			if node, _ := b.Node(path); len(node.Data) != 0 {
				t.Fatalf("remote after generic truncate = %q, want empty (immediate)", node.Data)
			}
		})
	}
}

// v1 TestSQLiteZeroTruncateBusySibling (same-handle rows): the truncate and
// the new generation share the original I/O handle — the old image is
// dropped entirely and a later-opened sibling reads the new generation.
func TestPortSQLiteZeroTruncateBusySibling(t *testing.T) {
	b := testbackend.NewBackend()
	oldImage := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2), walFrame('A')...)
	b.SeedWithLayout("/busy.db-wal", oldImage, "append_log")
	ch := newStack(t, b, "**/busy.db-wal")
	ctx := context.Background()

	original := openLog(t, ch, "/busy.db-wal").(*Handle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/busy.db-wal", Size: 0}); err != nil {
		t.Fatal(err)
	}
	newData := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4), []byte("new")...)
	if err := original.Write(ctx, 0, newData); err != nil {
		t.Fatal(err)
	}
	if err := original.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/busy.db-wal"); !bytes.Equal(node.Data, newData) {
		t.Fatalf("remote after truncate + new generation = %d bytes, want only the new generation (%d)", len(node.Data), len(newData))
	}
	// A sibling opened afterwards reads the new generation, not the old WAL.
	sibling := openLog(t, ch, "/busy.db-wal")
	buf := make([]byte, len(newData))
	if n, err := sibling.Read(ctx, 0, buf); err != nil || n != len(newData) || !bytes.Equal(buf, newData) {
		t.Fatalf("sibling read = n=%d err=%v, want the new generation", n, err)
	}
}

// v1 TestSQLiteZeroTruncateDelayedPendingCreate: a second open of a freshly
// created sidecar continues from the first handle's landed tail — both
// handles' commits chain onto one append log.
func TestPortSQLiteSecondOpenSharesLandedBaseline(t *testing.T) {
	b := testbackend.NewBackend()
	var mu sync.Mutex
	appends := 0
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
			mu.Lock()
			appends++
			mu.Unlock()
		}
		return false
	}), "**/pending.db-wal")
	ctx := context.Background()

	first := openLog(t, ch, "/pending.db-wal").(*Handle)
	if err := first.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := first.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// The second open adopts the landed creation...
	second := openLog(t, ch, "/pending.db-wal").(*Handle)
	if second.isNew || second.baseRev == 0 || second.origSize != 3 {
		t.Fatalf("second open = isNew=%t rev=%d size=%d, want the landed baseline", second.isNew, second.baseRev, second.origSize)
	}
	// ...and its append extends the same log.
	if err := second.Write(ctx, 3, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := second.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/pending.db-wal"); string(node.Data) != "new!" {
		t.Fatalf("reused pending handle bytes = %q, want new!", node.Data)
	}
	// One rebased create-form attempt plus the landed create, then the
	// extension append.
	mu.Lock()
	defer mu.Unlock()
	if appends != 3 {
		t.Fatalf("append calls = %d, want 3 (create attempt + rebased create + extension)", appends)
	}
}
