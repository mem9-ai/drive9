package extent

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

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
