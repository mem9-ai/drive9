package appendlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

const walHeaderSize = 32

func newStack(t *testing.T, b *testbackend.Backend, patterns ...string) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background()) // arms CachedAppendLogSupported
	ext, err := New(Options{Patterns: patterns, Client: cl})
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
	return ch
}

func openLog(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.Handle.(*Handle); !ok {
		t.Fatalf("path %s not claimed by appendlog", path)
	}
	return res.Handle
}

func TestProtocolHitUsesAppendEndpoint(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/wal.db", []byte("base")) // rev 1, size 4
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db")
	// sequential append: tail-safe
	if err := h.Write(ctx, 4, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	n, _ := b.Node("/wal.db")
	if string(n.Data) != "base-tail" {
		t.Fatalf("backend = %q", n.Data)
	}
	if got := b.Requests()["POST /v1/fs/wal.db"]; got != 1 {
		t.Fatalf("append-log calls = %d, want 1", got)
	}
	if got := b.Requests()["PUT /v1/fs/wal.db"]; got != 0 {
		t.Fatalf("full-image PUTs on protocol hit = %d, want 0", got)
	}
	// a second append continues from the adopted baseline
	if err := h.Write(ctx, 9, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ = b.Node("/wal.db")
	if string(n.Data) != "base-tail!" {
		t.Fatalf("backend after second append = %q", n.Data)
	}
	if got := b.Requests()["POST /v1/fs/wal.db"]; got != 2 {
		t.Fatalf("append-log calls = %d, want 2", got)
	}
}

func TestProtocolMissFallsThrough(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	// Non-matching path: the terminal core serves it, untouched by appendlog.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/plain.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.Handle.(*Handle); ok {
		t.Fatal("non-matching path claimed")
	}
	if err := res.Handle.Write(ctx, 0, []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/plain.txt"); string(n.Data) != "plain" {
		t.Fatalf("backend = %q", n.Data)
	}
	// The terminal wrote it as one whole-image PUT; the append protocol
	// (POST) was never consulted for a non-matching path beyond create.
	if got := b.Requests()["PUT /v1/fs/plain.txt"]; got != 1 {
		t.Fatalf("terminal PUTs = %d, want 1", got)
	}
}

func TestOverwriteBreaksTailSafetyFallsBackToRewrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/wal.db", []byte("base"))
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db")
	// non-sequential write kills tail safety → full rewrite on flush
	if err := h.Write(ctx, 0, []byte("BASE")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/wal.db")
	if string(n.Data) != "BASE" {
		t.Fatalf("backend = %q", n.Data)
	}
	if _, hit := b.Requests()["POST /v1/fs/wal.db"]; hit {
		t.Fatal("tail append attempted for overwrite pattern")
	}
	if got := b.Requests()["PUT /v1/fs/wal.db"]; got != 1 {
		t.Fatalf("rewrite PUTs = %d, want 1", got)
	}
}

func TestUnsupportedCodeFallsBackToRewrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/wal.db", []byte("base"))
	b.AppendUnsupported = true
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db")
	if err := h.Write(ctx, 4, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/wal.db")
	if string(n.Data) != "base-tail" {
		t.Fatalf("backend = %q", n.Data)
	}
	if got := b.Requests()["PUT /v1/fs/wal.db"]; got != 1 {
		t.Fatalf("fallback PUTs = %d, want 1", got)
	}
}

// A valid SQLite WAL header: magic (big-endian 0x377f0682/0x377f0683),
// format 300700, page size 4096, then zeros.
func walHeader() []byte {
	h := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(h[0:], 0x377f0682)
	binary.BigEndian.PutUint32(h[4:], 300700)
	binary.BigEndian.PutUint32(h[8:], 4096)
	return h
}

func TestWALGenerationResetPublishes32ByteHeader(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/wal.db", make([]byte, 100)) // an old, longer WAL
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	// Simulate the rotation flow: truncate (fences the tail), then rewrite
	// the whole 32-byte header — exactly the WAL checkpoint pattern.
	if err := h.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, walHeader()); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	n, _ := b.Node("/wal.db")
	if int64(len(n.Data)) != walHeaderSize {
		t.Fatalf("backend size = %d, want %d (generation reset)", len(n.Data), walHeaderSize)
	}
	// The reset is one bounded conditional PUT of exactly 32 bytes.
	puts := b.Requests()["PUT /v1/fs/wal.db"]
	if puts != 1 {
		t.Fatalf("reset PUTs = %d, want 1", puts)
	}
	if _, hit := b.Requests()["POST /v1/fs/wal.db"]; hit {
		t.Fatal("tail append used for generation reset")
	}
	// After the reset the handle is tail-safe again: appends go direct.
	if err := h.Write(ctx, walHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["POST /v1/fs/wal.db"]; got != 1 {
		t.Fatalf("post-reset appends = %d, want 1", got)
	}
	n, _ = b.Node("/wal.db")
	if int64(len(n.Data)) != walHeaderSize+5 {
		t.Fatalf("backend size after post-reset append = %d", len(n.Data))
	}
}

func TestMatcherPatterns(t *testing.T) {
	m := NewAppendLogMatcher([]string{"**/wal.db", "/logs/*.log"})
	for _, hit := range []string{"/wal.db", "/a/b/wal.db", "/logs/x.log"} {
		if !m.Matches(hit) {
			t.Errorf("%q should match", hit)
		}
	}
	for _, miss := range []string{"/wal.txt", "/other/db", "/logs/x.txt"} {
		if m.Matches(miss) {
			t.Errorf("%q should not match", miss)
		}
	}
	if (&AppendLogMatcher{}).Matches("/x") {
		t.Error("empty matcher matched")
	}
}

// v1 TestAppendLogHandleTruncate: ftruncate-to-0 on an append-log handle
// publishes an empty image through the full-rewrite route.
func TestPortAppendLogHandleTruncateRewritesEmpty(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("old contents"))
	ch := newStack(t, b, "/app.log")

	h := openLog(t, ch, "/app.log")
	if err := h.(*Handle).Truncate(0); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/app.log"); len(n.Data) != 0 {
		t.Fatalf("after truncate = %d bytes, want 0", len(n.Data))
	}
}

// v1 TestAppendLogNewZeroByteCreate: an O_CREAT of a pattern path with no
// writes materializes an empty remote file at the boundary.
func TestPortAppendLogNewZeroByteCreate(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "/logs/**")
	ctx := context.Background()

	h := openLog(t, ch, "/logs/new.log")
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/logs/new.log")
	if !ok || len(n.Data) != 0 {
		t.Fatalf("zero-byte create = %q ok=%v", n.Data, ok)
	}
}

// v1 TestAppendLogTailCommit: an O_APPEND write lands after the base tail
// without disturbing earlier bytes.
func TestPortAppendLogTailCommitPreservesBase(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/tail.log", []byte("base-"))
	ch := newStack(t, b, "/tail.log")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/tail.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_APPEND), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*Handle)
	if !ok {
		t.Fatal("not claimed by appendlog")
	}
	if err := h.Write(ctx, 0, []byte("appended")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/tail.log"); string(n.Data) != "base-appended" {
		t.Fatalf("after append = %q, want base-appended", n.Data)
	}
}

// v1 commitAppendSnapshotLocked row: fsync gives a small live append the
// same causal landing as close — the tail is committed synchronously and a
// repeated fsync of the unchanged snapshot is a no-op (no duplicate tail).
func TestPortAppendLogSyncLandsTailIdempotently(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/sync.log", []byte("base-"))
	ch := newStack(t, b, "/sync.log")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/sync.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_APPEND), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*Handle)
	if !ok {
		t.Fatal("not claimed by appendlog")
	}
	if err := h.Write(ctx, 0, []byte("tail-one")); err != nil {
		t.Fatal(err)
	}
	// fsync lands the tail immediately (causal validation on fsync, v1).
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/sync.log"); string(n.Data) != "base-tail-one" {
		t.Fatalf("after fsync = %q, want base-tail-one", n.Data)
	}
	// Repeated fsync without new writes must not append the tail again.
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/sync.log"); string(n.Data) != "base-tail-one" {
		t.Fatalf("after repeat fsync = %q, want base-tail-one (dedup)", n.Data)
	}
	// Post-fsync writes still append after the landed tail.
	if err := h.Write(ctx, 0, []byte("-two")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/sync.log"); string(n.Data) != "base-tail-one-two" {
		t.Fatalf("after second append = %q, want base-tail-one-two", n.Data)
	}
}

// v1 append_log_snapshot rows: snapshots at or below the 1MiB memory limit
// stay in RAM, larger ones spill to a temp file; Open() streams either way,
// Close removes the spill file.
func TestPortAppendLogSnapshotMemoryAndSpill(t *testing.T) {
	small, err := newAppendLogSnapshotFromReader("", 4, strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	if small.tempPath != "" || len(small.memory) != 4 {
		t.Fatalf("small snapshot = %+v, want in-memory", small)
	}
	rc, err := small.Open()
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(out) != "data" || small.Close() != nil {
		t.Fatalf("small snapshot roundtrip = %q", out)
	}

	big := bytes.Repeat([]byte("x"), 2<<20)
	spilled, err := newAppendLogSnapshotFromReader("", int64(len(big)), bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = spilled.Close() }()
	if spilled.tempPath == "" {
		t.Fatal("large snapshot did not spill to a temp file")
	}
	if spilled.Size() != int64(len(big)) {
		t.Fatalf("spilled size = %d", spilled.Size())
	}
	rc, err = spilled.Open()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("spilled roundtrip err=%v len=%d", err, len(got))
	}
	path := spilled.tempPath
	if err := spilled.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("Close did not remove the spill file")
	}
}

// v1 row: a source longer than the declared size fails the snapshot
// (requireAppendLogSnapshotEOF).
func TestPortAppendLogSnapshotRejectsOverlongSource(t *testing.T) {
	if _, err := newAppendLogSnapshotFromReader("", 2, strings.NewReader("abc")); err == nil {
		t.Fatal("overlong source accepted")
	}
}

// Integration: a full rewrite larger than the memory limit commits through
// the streamed snapshot path (temp-file body) and lands byte-identical.
func TestPortAppendLogLargeFullRewriteStreamsFromSpill(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "/big-wal.log")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big-wal.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*Handle)
	if !ok {
		t.Fatal("not claimed by appendlog")
	}
	payload := bytes.Repeat([]byte("W"), 3<<20)
	if err := h.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/big-wal.log")
	if !bytes.Equal(n.Data, payload) {
		t.Fatalf("landed %d bytes, want %d (streamed rewrite)", len(n.Data), len(payload))
	}
}

// The generation reset needs a salt proof: the dirty header must differ
// from the committed header captured before the write. A rotation with
// NEW salts publishes one bounded 32-byte generation.
func TestGenerationResetFiresOnSaltChange(t *testing.T) {
	b := testbackend.NewBackend()
	seed := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2), bytes.Repeat([]byte{7}, 4096)...)
	b.SeedWithLayout("/wal.db", seed, "append_log")
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	rotated := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	if err := h.Write(ctx, 0, rotated); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/wal.db")
	if int64(len(n.Data)) != walHeaderSize {
		t.Fatalf("backend size = %d, want %d (salt-proved generation reset drops the frames)", len(n.Data), walHeaderSize)
	}
	if !bytes.Equal(n.Data, rotated) {
		t.Fatal("backend content is not the rotated header")
	}
	// After the reset the handle is tail-safe: appends go direct.
	if err := h.Write(ctx, walHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ = b.Node("/wal.db")
	if int64(len(n.Data)) != walHeaderSize+5 {
		t.Fatalf("backend size after post-reset append = %d", len(n.Data))
	}
}

// Rewriting the committed header with the SAME salts must not fire the
// generation reset — it is not a new generation. The full image is
// rewritten instead.
func TestGenerationResetBlockedOnSameSalts(t *testing.T) {
	b := testbackend.NewBackend()
	header := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	seed := append(append([]byte(nil), header...), bytes.Repeat([]byte{7}, 4096)...)
	b.SeedWithLayout("/wal.db", seed, "append_log")
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	if err := h.Write(ctx, 0, header); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/wal.db")
	if int64(len(n.Data)) != int64(len(seed)) {
		t.Fatalf("backend size = %d, want %d (same salts: full rewrite, no reset)", len(n.Data), len(seed))
	}
	if !bytes.Equal(n.Data, seed) {
		t.Fatal("same-salt rewrite changed the image")
	}
}

// POSIX unlink-while-open: the name disappears immediately, the open
// handle keeps serving the image, and the remote delete is deferred to the
// last close (no resurrect on flush).
func TestUnlinkOpenHandleDefersDelete(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("base"))
	ch := newStack(t, b, "/log.txt")
	ctx := context.Background()

	h := openLog(t, ch, "/log.txt").(*Handle)
	if err := h.Write(ctx, 4, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/log.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/log.txt"}); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("unlinked path still visible: %v", err)
	}
	if _, ok := b.Node("/log.txt"); !ok {
		t.Fatal("remote delete must be deferred to the last close")
	}
	// The open handle keeps serving the image after the unlink.
	buf := make([]byte, 9)
	n, err := h.Read(ctx, 0, buf)
	if err != nil || n != 9 || string(buf) != "base-tail" {
		t.Fatalf("post-unlink read = %q n=%d err=%v, want base-tail", buf[:n], n, err)
	}
	// Close publishes the deferred delete; the dirty tail never lands.
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/log.txt"); ok {
		t.Fatal("close must publish the deferred delete")
	}
}
