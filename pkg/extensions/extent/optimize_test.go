package extent

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestEngineNonWritebackStagesInMemoryZeroTempFiles(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{
		Client:     client.New(f.URL, ""),
		RemoteRoot: "/",
		CacheDir:   cacheDir,
		CacheSize:  64 << 20,
		Writeback:  false,
	})
	if eng.Bridge.NativeWritebackForTest() {
		t.Fatal("expected NativeWriteback=false when Writeback=false")
	}

	h := New(Options{Engine: eng, Threshold: 0})
	// Multi-block write (>4 MiB) + single-block write cleanly round-trip in memory.
	big := bytes.Repeat([]byte("M"), (4<<20)+1024)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/big.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, big); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := eh.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := f.contentOf("/d/big.bin"); len(got) != len(big) || got != string(big) {
		t.Fatalf("content mismatch: got len %d, want %d", len(got), len(big))
	}

	// Neither rawstaging nor extent-stage-* temp files should exist under cacheDir.
	var stagingFiles []string
	_ = filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && (strings.Contains(path, "rawstaging") || strings.Contains(filepath.Base(path), "extent-stage-")) {
			stagingFiles = append(stagingFiles, path)
		}
		return nil
	})
	if len(stagingFiles) > 0 {
		t.Fatalf("unexpected disk staging artifacts when Writeback=false: %v", stagingFiles)
	}
}

func TestFlushTruncateOnlyShrinkAndGrowSendNoDirtyBlocks(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	head := strings.Repeat("ABCD", 2048) // 8192 bytes, non-zero
	f.seedExtent(t, "/d/t.bin", head)

	eng := NewEngine(EngineConfig{
		Client:     client.New(f.URL, ""),
		RemoteRoot: "/",
		CacheDir:   engineCacheDir(t),
	})
	h := New(Options{Engine: eng, Threshold: 1 << 20})
	eh := claimOpen(t, h, "/d/t.bin")

	// Shrink 8192 -> 100 via truncate-only PATCH.
	if _, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/d/t.bin", HasSize: true, Size: 100, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	})); err != nil {
		t.Fatalf("shrink SetAttr: %v", err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("shrink Flush: %v", err)
	}
	if got := f.contentOf("/d/t.bin"); got != head[:100] {
		t.Fatalf("after shrink len=%d want 100 matching head", len(got))
	}
	f.mu.Lock()
	if f.lastPatch != nil {
		t.Fatalf("expected lastPatch consumed")
	}
	f.mu.Unlock()

	// Grow 100 -> 16384 via metadata-only PATCH: must not overwrite head bytes.
	if _, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/d/t.bin", HasSize: true, Size: 16384, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	})); err != nil {
		t.Fatalf("grow SetAttr: %v", err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("grow Flush: %v", err)
	}
	got := []byte(f.contentOf("/d/t.bin"))
	if len(got) != 16384 {
		t.Fatalf("after grow len=%d, want 16384", len(got))
	}
	if !bytes.Equal(got[:100], []byte(head[:100])) {
		t.Fatal("grow overwrote existing head bytes")
	}
	for i := 100; i < len(got); i++ {
		if got[i] != 0 {
			t.Fatalf("non-zero byte at %d after grow: %d", i, got[i])
		}
	}
}

func TestFlushPendingSurfacesAndClearsCommitErr(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/err.bin", "ABCD")
	b := NewBridge(Config{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	ino, errno := b.ResolveLocal(context.Background(), "/d/err.bin")
	if errno != 0 {
		t.Fatalf("ResolveLocal: %d", errno)
	}

	b.mu.Lock()
	b.commitErr[ino] = errors.New("prior write commit failure")
	b.mu.Unlock()

	if err := b.FlushFile(context.Background(), ino); err == nil {
		t.Fatal("FlushFile must surface prior commitErr even when pending is empty")
	}
	b.mu.Lock()
	delete(b.commitErr, ino)
	b.mu.Unlock()
	if err := b.FlushFile(context.Background(), ino); err != nil {
		t.Fatalf("FlushFile after clearing commitErr: %v", err)
	}
}

func TestAssemblePatchBlocksSplitsChunkSpanToBlockSize(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/split.bin", "SEED")
	b := NewBridge(Config{Client: client.New(f.URL, ""), RemoteRoot: "/"})

	// Stage one 9 MiB pending slice (> 4 MiB ExtentBlockSize) and verify
	// assemblePatchBlocks cuts it into 4 MiB, 4 MiB, and 1 MiB pieces.
	span := bytes.Repeat([]byte("S"), (9<<20)+512)
	sum := sha256Hex(span)
	b.blocks.stagePinned(sum, span, true)

	blocks, err := b.assemblePatchBlocks(context.Background(), "/d/split.bin", []pendingWrite{
		{chunk: 1, off: 0, checksum: sum, size: int64(len(span))},
	})
	if err != nil {
		t.Fatalf("assemblePatchBlocks: %v", err)
	}
	if len(blocks) != 3 {
		t.Fatalf("assembled %d blocks, want 3 (4MiB + 4MiB + 1MiB+512)", len(blocks))
	}
	wantOffs := []uint32{0, uint32(client.ExtentBlockSize), uint32(2 * client.ExtentBlockSize)}
	wantLens := []int{int(client.ExtentBlockSize), int(client.ExtentBlockSize), (1 << 20) + 512}
	for i, blk := range blocks {
		if blk.Chunk != 1 || blk.Off != wantOffs[i] || len(blk.Data) != wantLens[i] {
			t.Fatalf("block %d = (chunk=%d off=%d len=%d), want (1 %d %d)",
				i, blk.Chunk, blk.Off, len(blk.Data), wantOffs[i], wantLens[i])
		}
	}
}

func TestBridgeReadFallbackOnNotFound416AndClassic(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/fs/missing.db-shm", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"not found","code":"not_found"}`, http.StatusNotFound)
	})
	mux.HandleFunc("/v1/fs/empty.db-shm", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, `{"error":"invalid range"}`, http.StatusRequestedRangeNotSatisfiable)
	})
	mux.HandleFunc("/v1/fs/classic.txt", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("X-Dat9-Content-Layout", "single")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = w.Write([]byte("HELLO"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	b := NewBridge(Config{Client: client.New(srv.URL, ""), RemoteRoot: "/"})

	for _, name := range []string{"/missing.db-shm", "/empty.db-shm", "/classic.txt"} {
		ino := inoFor(name)
		b.remember(ino, name)
		b.mu.Lock()
		b.sizes[ino] = 5
		b.mu.Unlock()
		var slices []meta.Slice
		if errno := b.Read(meta.Background(), ino, 0, &slices); errno != 0 {
			t.Fatalf("Read(%s) errno = %v, want 0 (no SIGBUS EIO)", name, errno)
		}
		if name == "/classic.txt" && len(slices) != 1 {
			t.Fatalf("Read(/classic.txt) slices = %d, want 1", len(slices))
		}
	}
}

func TestTryClaimExistingOpenAndTruncateGetAttr(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	payload := strings.Repeat("Z", 8192)
	f.seedExtent(t, "/d/ext.bin", payload)

	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	h := New(Options{Engine: eng, Threshold: 1})

	// Non-matching classic file declines.
	f.mu.Lock()
	f.classic["/d/classic.txt"] = "hi"
	f.ids["/d/classic.txt"] = "c1"
	f.mu.Unlock()
	if _, claimed, err := h.TryClaimExistingOpen(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/d/classic.txt", Flags: syscall.O_RDWR,
	}); err != nil || claimed {
		t.Fatalf("classic file: claimed=%v err=%v", claimed, err)
	}

	// Extent file is claimed (e.g. after writeback queue finishes landing it),
	// and O_EXCL is rejected.
	if _, claimed, err := h.TryClaimExistingOpen(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/d/ext.bin", Flags: syscall.O_CREAT | syscall.O_EXCL | syscall.O_RDWR,
	}); !claimed || !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("O_EXCL on existing extent: claimed=%v err=%v", claimed, err)
	}

	res, claimed, err := h.TryClaimExistingOpen(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/d/ext.bin", Flags: syscall.O_RDWR,
	})
	if err != nil || !claimed || res.Handle == nil {
		t.Fatalf("TryClaimExistingOpen(/d/ext.bin): claimed=%v err=%v", claimed, err)
	}
	eh := res.Handle.(*engineHandle)

	// Truncating an open extent file to 0 must immediately surface Length=0
	// through Bridge.GetAttr and commit size=0 cleanly on Flush/Release.
	if _, terr, handled := h.tryTruncate(context.Background(), &fs.Op{
		Kind: fs.OpSetAttr, Path: "/d/ext.bin", HasSize: true, Size: 0,
	}, nil); !handled || terr != nil {
		t.Fatalf("tryTruncate(0): handled=%v err=%v", handled, terr)
	}
	var attr meta.Attr
	if errno := eng.Bridge.GetAttr(meta.Background(), eh.ino, &attr); errno != 0 {
		t.Fatalf("Bridge.GetAttr after truncate(0): %v", errno)
	}
	if attr.Length != 0 {
		t.Fatalf("Bridge.GetAttr length = %d after truncate(0), want 0", attr.Length)
	}
	if err := eh.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := f.contentOf("/d/ext.bin"); got != "" {
		t.Fatalf("remote content after truncate(0)+Release = %q, want empty", got)
	}
}

func TestShrinkThenGrowClearsTruncatedTailAndReadsZero(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	orig := strings.Repeat("ABCDEFGH", 1536) // 12 KiB non-zero
	f.seedExtent(t, "/d/hyb-trunc.bin", orig)

	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	h := New(Options{Engine: eng, Threshold: 1})

	res, err := h.Do(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/d/hyb-trunc.bin", Flags: syscall.O_RDWR,
	}, nil)
	if err != nil || res.Handle == nil {
		t.Fatalf("open: err=%v handle=%v", err, res.Handle)
	}
	eh := res.Handle.(*engineHandle)

	// Shrink to 5 KiB then grow to 9 KiB before flush/close: [5 KiB .. 9 KiB)
	// must become zero both locally (pre-flush) and remotely (post-close).
	for _, sz := range []int64{5 << 10, 9 << 10} {
		if _, terr, handled := h.tryTruncate(context.Background(), &fs.Op{
			Kind: fs.OpSetAttr, Path: "/d/hyb-trunc.bin", HasSize: true, Size: sz,
		}, nil); !handled || terr != nil {
			t.Fatalf("tryTruncate(%d): handled=%v err=%v", sz, handled, terr)
		}
	}

	// Pre-flush read must already see zeros past 5 KiB.
	buf := make([]byte, 9<<10)
	n, rerr := eh.Read(context.Background(), 0, buf)
	if rerr != nil || n != len(buf) {
		t.Fatalf("pre-flush Read: n=%d err=%v", n, rerr)
	}
	if string(buf[:5<<10]) != orig[:5<<10] {
		t.Fatal("pre-flush prefix [0..5KiB) corrupted")
	}
	if !bytes.Equal(buf[5<<10:], make([]byte, 4<<10)) {
		t.Fatal("pre-flush [5KiB..9KiB) retained stale tail bytes after shrink+grow")
	}

	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := eh.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	got := f.contentOf("/d/hyb-trunc.bin")
	if len(got) != 9<<10 {
		t.Fatalf("remote len = %d, want %d", len(got), 9<<10)
	}
	if got[:5<<10] != orig[:5<<10] || got[5<<10:] != string(make([]byte, 4<<10)) {
		t.Fatal("remote [5KiB..9KiB) retained stale bytes instead of zeros")
	}
}
