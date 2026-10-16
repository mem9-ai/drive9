package appendlog

// Ported from v1 pkg/fuse/append_log_generation_reset_test.go (plus the
// gvisor-file rows that survive): the WAL generation-reset transport — salt
// proof, header-coverage and fence gates, the bounded 32-byte PUT, first
// frame append, concurrent-generation races, failure rows preserving the old
// generation, header capture before an offset-zero write, and a dirty handle
// rejecting a sibling's committed baseline until its own flush reconciles.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// seedWAL seeds an append_log object: H0 (salt1/salt2) followed by tail
// bytes of old-generation frames.
func seedWAL(t *testing.T, b *testbackend.Backend, path string, salt1, salt2 uint32, tailLen int) []byte {
	t.Helper()
	seed := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, salt1, salt2), bytes.Repeat([]byte("o"), tailLen)...)
	b.SeedWithLayout(path, seed, "append_log")
	return seed
}

// rotatedHeader returns a same-page header with different salts (H1).
func rotatedHeader(t *testing.T) []byte {
	t.Helper()
	return makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
}

// v1 TestAppendLogGenerationResetPublishesHeaderThenAppendsFirstFrame: a
// salt-proved rotation publishes exactly the 32-byte header with one bounded
// conditional PUT (the old generation's frames are never uploaded), adopts
// the new committed header, and the first frame then goes through the append
// endpoint.
func TestPortAppendLogGenerationResetPublishesHeaderThenAppendsFirstFrame(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 64)
	h1 := rotatedHeader(t)
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	puts := rec.snapshot()
	if len(puts) != 1 || puts[0].rev != "1" || !bytes.Equal(puts[0].body, h1) {
		t.Fatalf("reset PUTs = %+v, want one 32-byte header against revision 1", puts)
	}
	if _, hit := b.Requests()["POST /v1/fs/wal.db"]; hit {
		t.Fatal("generation reset must not use the append endpoint")
	}
	node, _ := b.Node("/wal.db")
	if len(node.Data) != walHeaderSize || h.baseRev != node.Rev || h.origSize != walHeaderSize {
		t.Fatalf("reset landing = %d bytes rev=%d handle=%d/%d", len(node.Data), node.Rev, h.baseRev, h.origSize)
	}
	committed, ok := parseSQLiteWALHeader(h.st.sqliteWALCommittedHeader.raw[:])
	if !h.st.sqliteWALConfirmed || !ok || committed.salt1 != 3 || committed.salt2 != 4 {
		t.Fatal("reset did not adopt the new committed WAL header")
	}

	// The first frame of the new generation appends after the reset.
	if err := h.Write(ctx, walHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["POST /v1/fs/wal.db"]; got != 1 {
		t.Fatalf("post-reset appends = %d, want 1", got)
	}
	node, _ = b.Node("/wal.db")
	if len(node.Data) != walHeaderSize+len("frame") || !bytes.Equal(node.Data[:walHeaderSize], h1) {
		t.Fatalf("backend after first frame = %d bytes", len(node.Data))
	}
}

// v1 TestAppendLogGenerationResetRequiresCompleteHeader: a partial header
// rewrite (incomplete [0, 32) coverage) must not rotate the WAL — the whole
// image is rewritten instead.
func TestPortAppendLogGenerationResetRequiresCompleteHeader(t *testing.T) {
	b := testbackend.NewBackend()
	seed := seedWAL(t, b, "/wal.db", 1, 2, 32)
	h1 := rotatedHeader(t)
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, 0, h1[:16]); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	puts := rec.snapshot()
	if len(puts) != 1 {
		t.Fatalf("PUTs = %d, want the full-image rewrite only", len(puts))
	}
	node, _ := b.Node("/wal.db")
	if len(node.Data) != len(seed) {
		t.Fatalf("backend size = %d, want %d (no reset on partial coverage)", len(node.Data), len(seed))
	}
	if !bytes.Equal(node.Data[:16], h1[:16]) || !bytes.Equal(node.Data[16:walHeaderSize], seed[16:walHeaderSize]) {
		t.Fatal("full rewrite did not preserve the unwritten header half")
	}
}

// v1 truncate-fence rows: a local truncate in the dirty generation fences
// the reset — the flush rewrites the surviving image instead of publishing a
// fresh 32-byte generation over it.
func TestPortAppendLogGenerationResetTruncateFenceBlocksReset(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 32) // 64 bytes total
	h1 := rotatedHeader(t)
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/wal.db", Size: 50}); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	node, _ := b.Node("/wal.db")
	if len(node.Data) != 50 {
		t.Fatalf("backend size = %d, want 50 (a reset would have published %d bytes)", len(node.Data), walHeaderSize)
	}
	if !bytes.Equal(node.Data[:walHeaderSize], h1) {
		t.Fatal("full rewrite lost the rotated header")
	}
}

// v1 TestAppendLogGenerationResetWithFrameBeforeFsyncUsesFullRewrite: any
// frame write in the dirty generation fences the reset to a full image.
func TestPortAppendLogGenerationResetBeyondHeaderFenceBlocksReset(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 32) // 64 bytes total
	h1 := rotatedHeader(t)
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 64, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if !h.st.sqliteWALWriteBeyondHeader {
		t.Fatal("frame write must fence a header-only generation reset")
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	node, _ := b.Node("/wal.db")
	if len(node.Data) != 64+len("frame") {
		t.Fatalf("backend size = %d, want %d (full image, no reset)", len(node.Data), 64+len("frame"))
	}
	if !bytes.Equal(node.Data[:walHeaderSize], h1) {
		t.Fatal("full rewrite lost the rotated header")
	}
}

// v1 concurrency rows (retargeted to the seq-mismatch publish gate): when a
// frame write lands while the reset PUT is in flight, the reset still
// publishes exactly its bounded 32 bytes, and because the newer generation
// keeps the handle dirty, the full-rewrite transport publishes the complete
// image within the same flush.
func TestPortAppendLogGenerationResetSeqMismatchPublishesNewerGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 64) // 96 bytes total
	h1 := rotatedHeader(t)
	rec := &putRecorder{}
	started := make(chan struct{})
	rel := newReleaseOnce()
	t.Cleanup(rel.release)
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs/wal.db" ||
			r.Header.Get("X-Dat9-Expected-Revision") != "1" { // only the reset PUT
			return rec.intercept(w, r)
		}
		// Record the body, then withhold the response after the backend has
		// fully processed the reset (a landing in flight).
		_ = rec.intercept(w, r)
		saved := httptest.NewRecorder()
		b.ServeHTTP(saved, r)
		close(started)
		<-rel.ch
		for k, vs := range saved.Header() {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(saved.Code)
		_, _ = w.Write(saved.Body.Bytes())
		return true
	}), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	<-started

	// A frame of the newer generation lands while the reset is in flight.
	if err := h.Write(ctx, walHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	rel.release()
	if err := <-done; err != nil {
		t.Fatalf("seq-mismatch flush: %v", err)
	}

	// The reset stayed bounded to the 32-byte header.
	puts := rec.snapshot()
	if len(puts) < 1 || !bytes.Equal(puts[0].body, h1) {
		t.Fatalf("reset PUT = %+v, want the bounded 32-byte header first", puts)
	}
	node, _ := b.Node("/wal.db")
	if len(node.Data) != 96 || !bytes.Equal(node.Data[:walHeaderSize], h1) ||
		string(node.Data[walHeaderSize:walHeaderSize+len("frame")]) != "frame" {
		t.Fatalf("backend after flush = %d bytes, want the complete newer generation", len(node.Data))
	}
	if h.dirty {
		t.Fatal("flush must finish with a clean handle")
	}
}

// v1 TestAppendLogGenerationResetFailurePreservesOldGeneration: a failing
// reset PUT leaves the handle exactly as it was — baseline, dirty image, and
// the H0 salt proof.
func TestPortAppendLogGenerationResetFailurePreservesOldGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 64)
	h1 := rotatedHeader(t)
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && fsPath(r.URL.Path) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"transient"}`))
			return true
		}
		return false
	}), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err == nil {
		t.Fatal("failing reset PUT must fail the flush")
	}
	if h.baseRev != 1 || h.origSize != 96 || h.buf.Size() != 96 || !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatalf("failed reset mutated handle = rev=%d size=%d dirty=%t", h.baseRev, h.origSize, h.dirty)
	}
	committed, ok := parseSQLiteWALHeader(h.st.sqliteWALCommittedHeader.raw[:])
	if !ok || committed.salt1 != 1 || committed.salt2 != 2 {
		t.Fatal("failed reset must preserve H0")
	}
}

// v1 TestAppendLogGenerationResetTerminalResponsesPreserveOldGeneration:
// coded conflicts and malformed success bodies are terminal failures that
// preserve the old generation.
func TestPortAppendLogGenerationResetTerminalResponsesPreserveOldGeneration(t *testing.T) {
	tests := []struct {
		name   string
		force  int // ForceConflict budget; 0 = use the malformed body instead
		status int
		body   string
	}{
		{name: "conflict", force: 5},
		{name: "malformed success", status: http.StatusOK, body: `{"revision":0}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			seedWAL(t, b, "/wal.db", 1, 2, 64)
			if test.force > 0 {
				b.ForceConflict("/wal.db", test.force)
			}
			h1 := rotatedHeader(t)
			ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
				if test.force == 0 && r.Method == http.MethodPut && fsPath(r.URL.Path) {
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.body))
					return true
				}
				return false
			}), "**/wal.db")
			ctx := context.Background()

			h := openLog(t, ch, "/wal.db").(*Handle)
			if err := h.Write(ctx, 0, h1); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err == nil {
				t.Fatal("terminal reset response must fail the flush")
			}
			if h.baseRev != 1 || h.origSize != 96 || !h.dirty || !h.buf.HasDirtyParts() {
				t.Fatalf("terminal result mutated handle = rev=%d size=%d dirty=%t", h.baseRev, h.origSize, h.dirty)
			}
			committed, ok := parseSQLiteWALHeader(h.st.sqliteWALCommittedHeader.raw[:])
			if !ok || committed.salt1 != 1 || committed.salt2 != 2 {
				t.Fatal("terminal result must preserve H0")
			}
		})
	}
}

// v1 TestAppendLogGenerationResetCanceledRequestPreservesOldGeneration.
func TestPortAppendLogGenerationResetCanceledRequestPreservesOldGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 64)
	h1 := rotatedHeader(t)
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			t.Errorf("canceled reset reached the server: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	}), "**/wal.db")

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(context.Background(), 0, h1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Flush(ctx); err == nil {
		t.Fatal("canceled reset must fail")
	}
	if h.baseRev != 1 || h.origSize != 96 || !h.dirty {
		t.Fatalf("canceled reset mutated handle = rev=%d size=%d dirty=%t", h.baseRev, h.origSize, h.dirty)
	}
}

// v1 TestAppendLogCapturesSQLiteWALHeaderBeforeOffsetZeroWrite: the H0 proof
// is captured from the buffer BEFORE the write overwrites it.
func TestPortAppendLogCapturesSQLiteWALHeaderBeforeOffsetZeroWrite(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 32)
	h1 := rotatedHeader(t)
	ch := newStack(t, b, "**/wal.db")

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(context.Background(), 0, h1); err != nil {
		t.Fatal(err)
	}
	if !h.st.sqliteWALConfirmed {
		t.Fatal("offset-zero header write must capture the proof")
	}
	committed, ok := parseSQLiteWALHeader(h.st.sqliteWALCommittedHeader.raw[:])
	if !ok || committed.salt1 != 1 || committed.salt2 != 2 {
		t.Fatalf("captured header = %+v, want the seeded H0", committed)
	}
}

// v1 TestAppendLogH0ProbePreservesEarlierFrameFence: the H0 probe at
// offset zero must not clear the beyond-header fence an earlier frame write
// established.
func TestPortAppendLogH0ProbePreservesEarlierFrameFence(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 32)
	h1 := rotatedHeader(t)
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, walHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if !h.st.sqliteWALWriteBeyondHeader {
		t.Fatal("frame write did not establish the reset fence")
	}
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if !h.st.sqliteWALConfirmed {
		t.Fatal("header write must still capture the proof")
	}
	if !h.st.sqliteWALWriteBeyondHeader {
		t.Fatal("H0 probe cleared the earlier frame reset fence")
	}
}

// v1 TestAppendLogNewSQLiteWALCreateAdoptsCommittedHeader: the create form
// of the append endpoint proofs the first generation's header.
func TestPortAppendLogNewSQLiteWALCreateAdoptsCommittedHeader(t *testing.T) {
	b := testbackend.NewBackend()
	h1 := rotatedHeader(t)
	ch := newStack(t, b, "**/new-wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/new-wal.db").(*Handle)
	if err := h.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !h.st.sqliteWALConfirmed {
		t.Fatal("created WAL must carry a committed-header proof")
	}
	committed, ok := parseSQLiteWALHeader(h.st.sqliteWALCommittedHeader.raw[:])
	if !ok || committed.salt1 != 3 || committed.salt2 != 4 {
		t.Fatalf("committed header = %+v, want the created H1", committed)
	}
	if node, _ := b.Node("/new-wal.db"); !bytes.Equal(node.Data, h1) {
		t.Fatalf("backend = %x, want the created header", node.Data)
	}
}

// v1 TestAppendLogGenerationResetClearsReadOnlySiblingTarget: after a reset,
// a handle opened before the rotation serves the new 32-byte generation (no
// stale 96-byte view).
func TestPortAppendLogGenerationResetClearsSiblingReaderView(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 64)
	h1 := rotatedHeader(t)
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	reader := openLog(t, ch, "/wal.db")
	writer := openLog(t, ch, "/wal.db").(*Handle)
	if err := writer.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 96)
	n, err := reader.Read(ctx, 0, buf)
	if err != nil || n != walHeaderSize || !bytes.Equal(buf[:n], h1) {
		t.Fatalf("sibling read after reset = n=%d err=%v, want the fresh 32-byte generation", n, err)
	}
}

// v1 gvisor-file TestAppendLogDirtyHandleRejectsNewCommittedBaseline: a
// dirty handle does not adopt a sibling's committed revision/size — its
// baseline moves only when its own flush reconciles through the rebase
// path.
func TestPortAppendLogDirtyHandleRejectsNewCommittedBaseline(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("pre"))
	ch := newStack(t, b, "/log.txt")
	ctx := context.Background()

	h := openLog(t, ch, "/log.txt").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	sibling := openLog(t, ch, "/log.txt").(*Handle)
	if err := sibling.Write(ctx, 3, []byte("grown")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if h.baseRev != 1 || h.origSize != 3 {
		t.Fatalf("dirty handle adopted the sibling baseline = rev=%d size=%d, want 1/3", h.baseRev, h.origSize)
	}
	if !h.dirty || string(h.buf.Bytes()) != "pretail" {
		t.Fatalf("pending bytes changed: dirty=%t data=%q", h.dirty, h.buf.Bytes())
	}
	// Its own flush reconciles through the rebase validation (here: the
	// sibling's growth makes the rebase terminal).
	if err := h.Flush(ctx); err == nil {
		t.Fatal("dirty handle flush must reconcile the drifted baseline, not silently adopt it")
	}
}
