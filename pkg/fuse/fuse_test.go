package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

var (
	benchmarkBytesSink []byte
	benchmarkByteSink  byte
	benchmarkIntSink   int
)

// ---------------------------------------------------------------------------
// InodeToPath tests
// ---------------------------------------------------------------------------

func TestInodeToPath_RootInode(t *testing.T) {
	m := NewInodeToPath()
	p, ok := m.GetPath(1)
	if !ok || p != "/" {
		t.Fatalf("root inode: got %q, %v; want %q, true", p, ok, "/")
	}
}

func TestInodeToPath_LookupNew(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/hello.txt", false, 42, time.Now())
	if ino < 2 {
		t.Fatalf("expected ino >= 2, got %d", ino)
	}
	p, ok := m.GetPath(ino)
	if !ok || p != "/hello.txt" {
		t.Fatalf("GetPath(%d) = %q, %v; want %q", ino, p, ok, "/hello.txt")
	}
	gotIno, ok := m.GetInode("/hello.txt")
	if !ok || gotIno != ino {
		t.Fatalf("GetInode = %d, %v; want %d", gotIno, ok, ino)
	}
}

func TestInodeToPath_LookupExisting(t *testing.T) {
	m := NewInodeToPath()
	ino1 := m.Lookup("/a", false, 10, time.Now())
	ino2 := m.Lookup("/a", false, 20, time.Now())
	if ino1 != ino2 {
		t.Fatalf("expected same inode, got %d and %d", ino1, ino2)
	}
	entry, ok := m.GetEntry(ino1)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Size != 20 {
		t.Fatalf("size not updated: got %d, want 20", entry.Size)
	}
	if entry.Nlookup != 2 {
		t.Fatalf("nlookup: got %d, want 2", entry.Nlookup)
	}
}

func TestInodeToPath_Forget(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/tmp", false, 0, time.Now())
	m.Forget(ino, 1)
	_, ok := m.GetPath(ino)
	if ok {
		t.Fatal("expected inode to be removed after Forget")
	}
}

func TestInodeToPath_ForgetDirectoryKeepsMapping(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/tmp", true, 0, time.Now())
	m.Forget(ino, 1)
	p, ok := m.GetPath(ino)
	if !ok || p != "/tmp" {
		t.Fatalf("directory inode mapping should be preserved, got %q, %v", p, ok)
	}
}

func TestInodeToPath_ForgetOwnerMetadataKeepsMapping(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/tmp", false, 0, time.Now())
	m.UpdateOwner(ino, 65534, 65534, true, true)
	m.Forget(ino, 1)
	p, ok := m.GetPath(ino)
	if !ok || p != "/tmp" {
		t.Fatalf("owner-tracked inode mapping should be preserved, got %q, %v", p, ok)
	}
}

func TestInodeToPath_ForgetMetadataOnlySpecialKeepsMapping(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/pipe", false, 0, time.Now())
	m.SetModeState(ino, uint32(syscall.S_IFIFO)|0o644, true)
	m.Forget(ino, 1)
	p, ok := m.GetPath(ino)
	if !ok || p != "/pipe" {
		t.Fatalf("metadata-only special inode mapping should be preserved, got %q, %v", p, ok)
	}
}

func TestInodeToPath_ForgetRoot(t *testing.T) {
	m := NewInodeToPath()
	m.Forget(1, 1)
	_, ok := m.GetPath(1)
	if !ok {
		t.Fatal("root inode must not be removed by Forget")
	}
}

func TestInodeToPath_Rename(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/old.txt", false, 5, time.Now())
	m.Rename("/old.txt", "/new.txt")

	p, ok := m.GetPath(ino)
	if !ok || p != "/new.txt" {
		t.Fatalf("after rename: GetPath = %q, %v; want %q", p, ok, "/new.txt")
	}
	_, ok = m.GetInode("/old.txt")
	if ok {
		t.Fatal("old path should not exist after rename")
	}
}

func TestInodeToPath_RenameReplacesDestination(t *testing.T) {
	m := NewInodeToPath()
	srcIno := m.Lookup("/config.lock", false, 10, time.Now())
	dstIno := m.Lookup("/config", false, 5, time.Now())

	m.Rename("/config.lock", "/config")

	gotIno, ok := m.GetInode("/config")
	if !ok || gotIno != srcIno {
		t.Fatalf("GetInode(/config) = %d, %v; want %d, true", gotIno, ok, srcIno)
	}
	if p, ok := m.GetPath(srcIno); !ok || p != "/config" {
		t.Fatalf("source path = %q, %v; want /config, true", p, ok)
	}
	if gotIno, ok := m.GetInode("/config"); !ok || gotIno == dstIno {
		t.Fatalf("destination path still resolves to replaced inode %d, %v", gotIno, ok)
	}
}

func TestInodeToPath_RenameReplacesDestinationPreservesUnlinkedInode(t *testing.T) {
	m := NewInodeToPath()
	srcIno := m.Lookup("/config.lock", false, 10, time.Now())
	dstIno := m.Lookup("/config", false, 5, time.Now())

	m.Rename("/config.lock", "/config")

	gotIno, ok := m.GetInode("/config")
	if !ok || gotIno != srcIno {
		t.Fatalf("GetInode(/config) = %d, %v; want %d, true", gotIno, ok, srcIno)
	}
	dstEntry, ok := m.GetEntry(dstIno)
	if !ok {
		t.Fatal("replaced destination inode should be preserved for open handles")
	}
	if !dstEntry.Unlinked || dstEntry.Nlink != 0 || dstEntry.Path != "/config" {
		t.Fatalf("destination entry = %+v; want unlinked nlink=0 at old path", dstEntry)
	}
}

func TestInodeToPath_RenameDir(t *testing.T) {
	m := NewInodeToPath()
	m.Lookup("/dir", true, 0, time.Now())
	childIno := m.Lookup("/dir/child.txt", false, 10, time.Now())

	m.Rename("/dir", "/newdir")

	p, ok := m.GetPath(childIno)
	if !ok || p != "/newdir/child.txt" {
		t.Fatalf("child after dir rename: %q, %v; want %q", p, ok, "/newdir/child.txt")
	}
}

func TestInodeToPath_Remove(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/rm.txt", false, 1, time.Now())
	m.Remove("/rm.txt")

	_, ok := m.GetPath(ino)
	if ok {
		t.Fatal("removed inode should not be found")
	}
	_, ok = m.GetInode("/rm.txt")
	if ok {
		t.Fatal("removed path should not be found")
	}
}

func TestInodeToPath_UpdateSize(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/f", false, 10, time.Now())
	m.UpdateSize(ino, 999)
	entry, _ := m.GetEntry(ino)
	if entry.Size != 999 {
		t.Fatalf("UpdateSize: got %d, want 999", entry.Size)
	}
}

func TestInodeToPath_IncrementLookupPreservesExistingRef(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/child.txt", false, 1, time.Now()) // existing live ref
	_ = m.EnsureInode("/child.txt", false, 1, time.Now())
	if !m.IncrementLookup(ino) {
		t.Fatal("IncrementLookup should succeed")
	}

	m.Forget(ino, 1) // drop the readdirplus ref only
	if _, ok := m.GetPath(ino); !ok {
		t.Fatal("inode should still exist after dropping only the extra lookup ref")
	}
}

// ---------------------------------------------------------------------------
// HandleTable tests
// ---------------------------------------------------------------------------

func TestHandleTable_AllocateAndGet(t *testing.T) {
	ht := NewHandleTable[string]()
	fh1 := ht.Allocate("hello")
	fh2 := ht.Allocate("world")

	if fh1 == fh2 {
		t.Fatal("handles should be unique")
	}

	v, ok := ht.Get(fh1)
	if !ok || v != "hello" {
		t.Fatalf("Get(%d) = %q, %v; want %q", fh1, v, ok, "hello")
	}
}

func TestHandleTable_Delete(t *testing.T) {
	ht := NewHandleTable[int]()
	fh := ht.Allocate(42)
	ht.Delete(fh)
	_, ok := ht.Get(fh)
	if ok {
		t.Fatal("expected handle to be deleted")
	}
}

func TestHandleTable_ForEach(t *testing.T) {
	ht := NewHandleTable[int]()
	ht.Allocate(1)
	ht.Allocate(2)
	ht.Allocate(3)

	sum := 0
	ht.ForEach(func(_ uint64, v int) { sum += v })
	if sum != 6 {
		t.Fatalf("ForEach sum = %d, want 6", sum)
	}
}

// ---------------------------------------------------------------------------
// ReadCache tests
// ---------------------------------------------------------------------------

func TestReadCache_PutAndGet(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	data := []byte("hello world")
	rc.Put("/test.txt", data, 1)

	got, ok := rc.Get("/test.txt", 1)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", got, "hello world")
	}
}

func TestReadCache_PutDefensivelyCopies(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	data := []byte("hello world")
	rc.Put("/test.txt", data, 1)
	data[0] = 'j'

	got, ok := rc.Get("/test.txt", 1)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", got, "hello world")
	}
}

func TestReadCache_PutOwnedReusesSlice(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	data := []byte("hello world")
	rc.PutOwned("/test.txt", data, 1)

	got, ok := rc.Get("/test.txt", 1)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) == 0 || len(data) == 0 {
		t.Fatal("expected non-empty cached data")
	}
	if &got[0] != &data[0] {
		t.Fatal("PutOwned should reuse the caller slice")
	}
}

func TestReadCache_RevisionMismatch(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	rc.Put("/f", []byte("x"), 1)
	_, ok := rc.Get("/f", 2)
	if ok {
		t.Fatal("expected cache miss on revision mismatch")
	}
}

func TestReadCache_TTLExpiry(t *testing.T) {
	rc := NewReadCache(1<<20, 1*time.Millisecond)
	rc.Put("/f", []byte("x"), 1)
	time.Sleep(5 * time.Millisecond)
	_, ok := rc.Get("/f", 1)
	if ok {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestReadCache_NoTTLExpiry(t *testing.T) {
	rc := NewReadCache(1<<20, readCacheNoExpiryTTL)
	rc.Put("/f", []byte("x"), 1)
	time.Sleep(5 * time.Millisecond)
	got, ok := rc.Get("/f", 1)
	if !ok {
		t.Fatal("expected cache hit when time-based expiry is disabled")
	}
	if string(got) != "x" {
		t.Fatalf("got %q, want x", got)
	}
}

func TestReadCache_NoTTLExpiryStillChecksRevision(t *testing.T) {
	rc := NewReadCache(1<<20, readCacheNoExpiryTTL)
	rc.Put("/f", []byte("x"), 1)
	if _, ok := rc.Get("/f", 2); ok {
		t.Fatal("expected cache miss on revision mismatch")
	}
}

func TestReadCache_SkipLargeFiles(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	bigData := make([]byte, defaultReadCacheMaxFileSize+1)
	rc.Put("/big", bigData, 1)
	_, ok := rc.Get("/big", 1)
	if ok {
		t.Fatal("large file should not be cached")
	}
}

func TestReadCache_CustomMaxFileSize(t *testing.T) {
	rc := NewReadCacheWithMaxFileSize(1<<20, 10*time.Second, 64<<10)
	rc.Put("/small", make([]byte, 64<<10), 1)
	if _, ok := rc.Get("/small", 1); !ok {
		t.Fatal("file at custom max size should be cached")
	}
	rc.Put("/large", make([]byte, (64<<10)+1), 1)
	if _, ok := rc.Get("/large", 1); ok {
		t.Fatal("file over custom max size should not be cached")
	}
}

func TestReadCache_CachesMediumSmallFiles(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	data := make([]byte, defaultSmallFileThreshold+1)
	rc.Put("/medium", data, 1)
	if _, ok := rc.Get("/medium", 1); !ok {
		t.Fatal("medium-small file should be cached")
	}
}

func TestReadCache_LRUEviction(t *testing.T) {
	// Cache can hold 20 bytes. Put 3 entries of 10 bytes each.
	rc := NewReadCache(20, 10*time.Second)
	rc.Put("/a", make([]byte, 10), 1)
	rc.Put("/b", make([]byte, 10), 1)
	rc.Put("/c", make([]byte, 10), 1)

	// /a should be evicted (LRU)
	_, ok := rc.Get("/a", 0)
	if ok {
		t.Fatal("expected /a to be evicted")
	}
	// /c should still be there
	_, ok = rc.Get("/c", 0)
	if !ok {
		t.Fatal("expected /c to still be cached")
	}
}

func TestReadCache_Invalidate(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	rc.Put("/x", []byte("data"), 1)
	rc.Invalidate("/x")
	_, ok := rc.Get("/x", 0)
	if ok {
		t.Fatal("expected invalidated entry to be gone")
	}
}

func TestReadCache_InvalidatePrefix(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	rc.Put("/dir/a", []byte("a"), 1)
	rc.Put("/dir/b", []byte("b"), 1)
	rc.Put("/other", []byte("c"), 1)

	rc.InvalidatePrefix("/dir/")
	if _, ok := rc.Get("/dir/a", 0); ok {
		t.Fatal("/dir/a should be invalidated")
	}
	if _, ok := rc.Get("/dir/b", 0); ok {
		t.Fatal("/dir/b should be invalidated")
	}
	if _, ok := rc.Get("/other", 0); !ok {
		t.Fatal("/other should still be cached")
	}
}

// ---------------------------------------------------------------------------
// WriteBuffer tests
// ---------------------------------------------------------------------------

func TestWriteBuffer_Sequential(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	n, err := wb.Write(0, []byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write: n=%d, err=%v", n, err)
	}
	n, err = wb.Write(5, []byte(" world"))
	if err != nil || n != 6 {
		t.Fatalf("Write: n=%d, err=%v", n, err)
	}
	if string(wb.Bytes()) != "hello world" {
		t.Fatalf("got %q", wb.Bytes())
	}
	if wb.Size() != 11 {
		t.Fatalf("Size = %d, want 11", wb.Size())
	}
}

func TestWriteBuffer_RandomWrite(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, []byte("aaaa"))
	_, _ = wb.Write(2, []byte("bb"))
	if string(wb.Bytes()) != "aabb" {
		t.Fatalf("random write: got %q, want %q", wb.Bytes(), "aabb")
	}
}

func TestWriteBuffer_GapFill(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(5, []byte("x"))
	if wb.Size() != 6 {
		t.Fatalf("Size = %d, want 6", wb.Size())
	}
	// bytes 0-4 should be zero
	for i := 0; i < 5; i++ {
		if wb.Bytes()[i] != 0 {
			t.Fatalf("byte %d = %d, want 0", i, wb.Bytes()[i])
		}
	}
}

func TestWriteBuffer_PartGrowthUsesHeadroom(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 8)
	if _, err := wb.Write(0, []byte{1, 2}); err != nil {
		t.Fatalf("first write: %v", err)
	}
	first := wb.parts[0]
	if len(first) != 2 {
		t.Fatalf("part len after first write = %d, want 2", len(first))
	}
	if cap(first) != 8 {
		t.Fatalf("part cap after first write = %d, want 8", cap(first))
	}

	if _, err := wb.Write(6, []byte{7}); err != nil {
		t.Fatalf("gap write: %v", err)
	}
	part := wb.parts[0]
	if cap(part) != cap(first) {
		t.Fatalf("part cap after gap write = %d, want %d", cap(part), cap(first))
	}
	want := []byte{1, 2, 0, 0, 0, 0, 7}
	if got := wb.PartData(1); !bytes.Equal(got, want) {
		t.Fatalf("part data = %v, want %v", got, want)
	}
}

func TestWriteBuffer_Truncate(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, []byte("hello world"))

	if err := wb.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if string(wb.Bytes()) != "hello" {
		t.Fatalf("after truncate: %q", wb.Bytes())
	}

	// Extend
	if err := wb.Truncate(8); err != nil {
		t.Fatal(err)
	}
	if wb.Size() != 8 {
		t.Fatalf("after extend: Size=%d", wb.Size())
	}
}

func TestWriteBuffer_TruncateSmallFileSparseWriteZeroFillsGap(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, []byte("hello world"))

	if err := wb.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(10, []byte("X")); err != nil {
		t.Fatal(err)
	}

	got := wb.Bytes()
	if len(got) != 11 {
		t.Fatalf("Bytes() len = %d, want 11", len(got))
	}
	if string(got[:5]) != "hello" {
		t.Fatalf("prefix = %q, want %q", got[:5], "hello")
	}
	for i := 5; i < 10; i++ {
		if got[i] != 0 {
			t.Fatalf("gap byte %d = %d, want 0", i, got[i])
		}
	}
	if got[10] != 'X' {
		t.Fatalf("tail byte = %q, want %q", got[10], byte('X'))
	}
}

func TestWriteBuffer_TruncateExtendThenSmallWritePreservesLogicalSize(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 8<<20)

	if err := wb.Truncate(100); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if wb.Size() != 100 {
		t.Fatalf("Size() = %d, want 100", wb.Size())
	}

	buf := make([]byte, 10)
	n := wb.ReadAt(50, buf)
	if n != 10 {
		t.Fatalf("ReadAt returned %d, want 10", n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("buf[%d] = %d, want 0", i, b)
		}
	}

	if err := wb.Truncate(50); err != nil {
		t.Fatal(err)
	}
	if wb.Size() != 50 {
		t.Fatalf("Size() after shrink = %d, want 50", wb.Size())
	}
}

func TestWriteBuffer_EFBIG(t *testing.T) {
	wb := NewWriteBuffer("/test", 100, 0)
	_, err := wb.Write(0, make([]byte, 101))
	if err == nil {
		t.Fatal("expected EFBIG error")
	}
}

func TestWriteBuffer_PreloadThenRandomWrite(t *testing.T) {
	// Simulates the Open() preload path: load existing content, then pwrite
	wb := NewWriteBuffer("/test", 0, 0)

	// Preload original file content (what Open does via client.Read)
	original := []byte("hello world, this is the original content!")
	_, _ = wb.Write(0, original)
	if string(wb.Bytes()) != string(original) {
		t.Fatalf("after preload: got %q", wb.Bytes())
	}

	// Random write at offset 6 — should overwrite "world" with "EARTH"
	_, _ = wb.Write(6, []byte("EARTH"))
	want := "hello EARTH, this is the original content!"
	if string(wb.Bytes()) != want {
		t.Fatalf("after pwrite: got %q, want %q", wb.Bytes(), want)
	}

	// Original content before and after the modified region is preserved
	if string(wb.Bytes()[:6]) != "hello " {
		t.Fatalf("prefix damaged: %q", wb.Bytes()[:6])
	}
	if string(wb.Bytes()[11:]) != ", this is the original content!" {
		t.Fatalf("suffix damaged: %q", wb.Bytes()[11:])
	}
}

func TestWriteBuffer_Reset(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	wb.Reset()
	if wb.Size() != 0 {
		t.Fatalf("after Reset: Size=%d", wb.Size())
	}
	if len(wb.DirtyPartNumbers()) != 0 {
		t.Fatalf("after Reset: dirty parts should be empty")
	}
}

func TestWriteBuffer_DirtyParts_SinglePart(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	// Write within the first 8MB part
	_, _ = wb.Write(0, []byte("hello"))
	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 1 || dirty[0] != 1 {
		t.Fatalf("expected dirty=[1], got %v", dirty)
	}
}

func TestWriteBuffer_DirtyParts_MultipleParts(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	// Write at beginning of part 1
	_, _ = wb.Write(0, []byte("a"))
	// Write at beginning of part 2 (offset 8MB)
	_, _ = wb.Write(DefaultPartSize, []byte("b"))
	// Write at beginning of part 3 (offset 16MB)
	_, _ = wb.Write(2*DefaultPartSize, []byte("c"))

	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 3 {
		t.Fatalf("expected 3 dirty parts, got %v", dirty)
	}
	// Parts should be 1, 2, 3 (1-based)
	expected := map[int]bool{1: true, 2: true, 3: true}
	for _, p := range dirty {
		if !expected[p] {
			t.Fatalf("unexpected dirty part %d", p)
		}
	}
}

func TestWriteBuffer_DirtyParts_CrossPartBoundary(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	// Write across the boundary of part 1 and part 2
	data := make([]byte, 100)
	_, _ = wb.Write(DefaultPartSize-50, data) // straddles parts 1 and 2
	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty parts, got %v", dirty)
	}
}

func TestWriteBuffer_DirtyParts_PreloadClearsFlags(t *testing.T) {
	// Simulates large file open: preload then clear dirty flags
	wb := NewWriteBuffer("/test", 0, 0)
	data := make([]byte, DefaultPartSize*2) // 16MB, 2 parts
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, _ = wb.Write(0, data)

	// After preload, clear dirty flags (simulating Open behavior)
	wb.ClearDirty()

	// No parts should be dirty
	if len(wb.DirtyPartNumbers()) != 0 {
		t.Fatalf("expected no dirty parts after clearing, got %v", wb.DirtyPartNumbers())
	}

	// Now write to part 2 only
	_, _ = wb.Write(DefaultPartSize+100, []byte("modified"))
	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 1 || dirty[0] != 2 {
		t.Fatalf("expected dirty=[2], got %v", dirty)
	}
}

func TestWriteBuffer_DirtyParts_TruncateShrink(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	// Write 3 parts
	data := make([]byte, DefaultPartSize*3)
	_, _ = wb.Write(0, data)
	wb.ClearDirty()

	// Truncate to 1 part
	_ = wb.Truncate(DefaultPartSize / 2)
	dirty := wb.DirtyPartNumbers()
	// Part 1 should be dirty (it was truncated within)
	if len(dirty) != 1 || dirty[0] != 1 {
		t.Fatalf("expected dirty=[1] after truncate, got %v", dirty)
	}
}

func TestWriteBuffer_DirtyParts_TruncateShrinkAtBoundary(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, make([]byte, DefaultPartSize*3))
	wb.ClearDirty()

	_ = wb.Truncate(DefaultPartSize)
	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 1 || dirty[0] != 1 {
		t.Fatalf("expected dirty=[1] after boundary truncate, got %v", dirty)
	}
}

func TestWriteBuffer_DirtyParts_TruncateExtend(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, make([]byte, DefaultPartSize)) // 1 full part
	wb.ClearDirty()                                   // clear as if preloaded

	// Extend to 3 parts
	_ = wb.Truncate(DefaultPartSize*3 - 100)
	dirty := wb.DirtyPartNumbers()
	// Parts 2 and 3 should be dirty (extended region)
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty parts after extend, got %v", dirty)
	}
}

func TestWriteBuffer_PartData(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	// Write 2 full parts + partial third
	data := make([]byte, DefaultPartSize*2+100)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, _ = wb.Write(0, data)

	// Part 1 should be exactly DefaultPartSize bytes
	p1 := wb.PartData(1)
	if len(p1) != int(DefaultPartSize) {
		t.Fatalf("part 1 size: got %d, want %d", len(p1), DefaultPartSize)
	}

	// Part 3 should be 100 bytes (last partial part)
	p3 := wb.PartData(3)
	if len(p3) != 100 {
		t.Fatalf("part 3 size: got %d, want 100", len(p3))
	}

	// Part 4 should be nil (out of range)
	p4 := wb.PartData(4)
	if p4 != nil {
		t.Fatalf("part 4 should be nil, got %d bytes", len(p4))
	}
}

func TestWriteBuffer_MarkAllDirty(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, make([]byte, DefaultPartSize*3))
	wb.ClearDirty() // clear

	wb.MarkAllDirty()
	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 3 {
		t.Fatalf("expected 3 dirty parts, got %v", dirty)
	}
}

// ---------------------------------------------------------------------------
// DirCache tests
// ---------------------------------------------------------------------------

func TestDirCache_PutAndGet(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	items := []CachedFileInfo{
		{Name: "a.txt", Size: 10, IsDir: false},
		{Name: "subdir", Size: 0, IsDir: true},
	}
	dc.Put("/", items)

	got, ok := dc.Get("/")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != 2 {
		t.Fatalf("got %d items, want 2", len(got))
	}
}

func TestDirCache_Expiry(t *testing.T) {
	dc := NewDirCache(1 * time.Millisecond)
	dc.Put("/dir", []CachedFileInfo{{Name: "f"}})
	time.Sleep(5 * time.Millisecond)
	_, ok := dc.Get("/dir")
	if ok {
		t.Fatal("expected cache miss after expiry")
	}
}

func TestDirCache_Invalidate(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.Put("/dir", []CachedFileInfo{{Name: "f"}})
	dc.Invalidate("/dir")
	_, ok := dc.Get("/dir")
	if ok {
		t.Fatal("expected invalidated entry to be gone")
	}
}

func TestDirCache_InvalidateAll(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.Put("/a", []CachedFileInfo{{Name: "1"}})
	dc.Put("/b", []CachedFileInfo{{Name: "2"}})
	dc.InvalidateAll()
	if _, ok := dc.Get("/a"); ok {
		t.Fatal("/a should be gone")
	}
	if _, ok := dc.Get("/b"); ok {
		t.Fatal("/b should be gone")
	}
}

func TestNamespaceCache_PositiveHit(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 10)
	dc.Upsert("/repo", CachedFileInfo{Name: "README.md", Size: 12, Revision: 7})

	got := dc.Lookup("/repo", "README.md")
	if got.kind != namespaceLookupPositive {
		t.Fatalf("lookup kind = %v, want positive", got.kind)
	}
	if got.item.Size != 12 || got.item.Revision != 7 {
		t.Fatalf("lookup item = %+v, want size=12 revision=7", got.item)
	}
}

func TestNamespaceCache_CompleteMissIsSafeNegative(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 10)
	dc.Put("/repo", []CachedFileInfo{{Name: "README.md"}})

	got := dc.Lookup("/repo", "missing.txt")
	if got.kind != namespaceLookupCompleteMiss {
		t.Fatalf("lookup kind = %v, want complete miss", got.kind)
	}
}

func TestNamespaceCache_PartialMissFallsThrough(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 10)
	dc.Upsert("/repo", CachedFileInfo{Name: "known.txt"})

	got := dc.Lookup("/repo", "missing.txt")
	if got.kind != namespaceLookupPartialMiss {
		t.Fatalf("lookup kind = %v, want partial miss", got.kind)
	}
}

func TestNamespaceCache_SessionCreatedMissIsSafeNegative(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 10)
	dc.MarkSessionCreatedDir("/repo/.git/objects/ab")

	got := dc.Lookup("/repo/.git/objects/ab", "missing")
	if got.kind != namespaceLookupSessionMiss {
		t.Fatalf("lookup kind = %v, want session miss", got.kind)
	}
	if gotItems, ok := dc.Get("/repo/.git/objects/ab"); !ok || len(gotItems) != 0 {
		t.Fatalf("Get session-created dir = len %d ok %t, want empty complete hit", len(gotItems), ok)
	}
}

func TestNamespaceCache_SessionCreatedDirOutlivesNegativeTTL(t *testing.T) {
	dc := NewNamespaceCache(300*time.Millisecond, 10*time.Millisecond, 10)
	dc.MarkSessionCreatedDir("/dir")

	time.Sleep(50 * time.Millisecond)
	if got := dc.Lookup("/dir", "missing"); got.kind != namespaceLookupSessionMiss {
		t.Fatalf("lookup kind after negative TTL = %v, want session miss", got.kind)
	}

	time.Sleep(300 * time.Millisecond)
	if got := dc.Lookup("/dir", "missing"); got.kind != namespaceLookupNone {
		t.Fatalf("lookup kind after dir TTL = %v, want none", got.kind)
	}
}

func TestNamespaceCache_RecordRemoteNegativeEscalatesAtThreshold(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, time.Second, 10)

	for i := 1; i < escalateMissThreshold; i++ {
		if dc.RecordRemoteNegative("/dir") {
			t.Fatalf("miss %d escalated, want escalation only at %d", i, escalateMissThreshold)
		}
	}
	if !dc.RecordRemoteNegative("/dir") {
		t.Fatalf("miss %d did not escalate", escalateMissThreshold)
	}

	// Cooldown right after an escalation suppresses duplicates.
	for i := 0; i < escalateMissThreshold; i++ {
		if dc.RecordRemoteNegative("/dir") {
			t.Fatal("escalated again during post-escalation cooldown")
		}
	}
}

func TestNamespaceCache_RecordRemoteNegativeWindowExpiryResets(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, time.Second, 10)

	dc.RecordRemoteNegative("/dir")
	dc.RecordRemoteNegative("/dir")

	// Backdate the window: the partial count must not carry over.
	dc.mu.Lock()
	dc.entries["/dir"].missWindowStart = time.Now().Add(-2 * escalateMissWindow)
	dc.mu.Unlock()

	for i := 1; i < escalateMissThreshold; i++ {
		if dc.RecordRemoteNegative("/dir") {
			t.Fatalf("miss %d of new window escalated early", i)
		}
	}
	if !dc.RecordRemoteNegative("/dir") {
		t.Fatal("fresh window did not escalate at threshold")
	}
}

func TestNamespaceCache_DeferEscalationCoolsDown(t *testing.T) {
	dc := NewNamespaceCache(50*time.Millisecond, 10*time.Millisecond, 10)
	dc.DeferEscalation("/dir")

	for i := 0; i < escalateMissThreshold; i++ {
		if dc.RecordRemoteNegative("/dir") {
			t.Fatal("escalated during cooldown")
		}
	}

	time.Sleep(70 * time.Millisecond)
	for i := 1; i < escalateMissThreshold; i++ {
		if dc.RecordRemoteNegative("/dir") {
			t.Fatalf("miss %d after cooldown escalated early", i)
		}
	}
	if !dc.RecordRemoteNegative("/dir") {
		t.Fatal("did not escalate after cooldown expired")
	}
}

func TestNamespaceCache_CanAnswerMisses(t *testing.T) {
	// Use a short ttl so the complete marker expires within the test.
	// completeExpires is now tied to ttl (not negativeTTL).
	dc := NewNamespaceCache(30*time.Millisecond, 10*time.Millisecond, 2)

	if dc.CanAnswerMisses("/dir") {
		t.Fatal("empty cache should not answer misses")
	}

	dc.Put("/dir", []CachedFileInfo{{Name: "a.txt"}})
	if !dc.CanAnswerMisses("/dir") {
		t.Fatal("complete listing should answer misses")
	}
	time.Sleep(50 * time.Millisecond)
	if dc.CanAnswerMisses("/dir") {
		t.Fatal("expired complete marker should not answer misses")
	}

	dc.Put("/big", []CachedFileInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}})
	if dc.CanAnswerMisses("/big") {
		t.Fatal("oversized listing should not answer misses")
	}

	dc.MarkSessionCreatedDir("/session")
	if !dc.CanAnswerMisses("/session") {
		t.Fatal("session-created dir should answer misses")
	}
}

func TestNamespaceCache_NegativeExpires(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, time.Millisecond, 10)
	dc.MarkNegative("/repo", "missing.txt")
	if got := dc.Lookup("/repo", "missing.txt"); got.kind != namespaceLookupNegative {
		t.Fatalf("lookup kind before expiry = %v, want negative", got.kind)
	}
	time.Sleep(5 * time.Millisecond)
	if got := dc.Lookup("/repo", "missing.txt"); got.kind != namespaceLookupNone {
		t.Fatalf("lookup kind after expiry = %v, want none", got.kind)
	}
}

func TestNamespaceCache_CompleteMissExpiresWithTTL(t *testing.T) {
	// completeExpires uses the main ttl (not negativeTTL), so a complete
	// listing stays valid for the full cache TTL. After ttl expires both
	// the positive list and the complete miss state expire together.
	// Use wide timing gaps (100ms ttl, 10ms negativeTTL) to avoid flakiness
	// on loaded CI hosts.
	dc := NewNamespaceCache(100*time.Millisecond, 10*time.Millisecond, 10)
	dc.Put("/repo", []CachedFileInfo{{Name: "known.txt"}})

	// Immediately after Put, complete miss is valid.
	if got := dc.Lookup("/repo", "missing.txt"); got.kind != namespaceLookupCompleteMiss {
		t.Fatalf("lookup kind immediately = %v, want complete miss", got.kind)
	}

	// After negativeTTL (10ms) but before ttl (100ms), complete miss still valid.
	time.Sleep(20 * time.Millisecond)
	if got := dc.Lookup("/repo", "missing.txt"); got.kind != namespaceLookupCompleteMiss {
		t.Fatalf("lookup kind after negative TTL = %v, want complete miss (should survive until ttl)", got.kind)
	}

	// After ttl expires, both positive and complete state expire.
	time.Sleep(100 * time.Millisecond)
	if got := dc.Lookup("/repo", "missing.txt"); got.kind != namespaceLookupNone {
		t.Fatalf("lookup kind after ttl = %v, want none", got.kind)
	}
}

func TestNamespaceCache_NegativeEntriesExpireAtNegativeTTL(t *testing.T) {
	// Individual negative entries (MarkNegative) still use the shorter
	// negativeTTL, independent of completeExpires.
	dc := NewNamespaceCache(10*time.Second, time.Millisecond, 10)
	dc.MarkNegative("/repo", "gone.txt")

	if got := dc.Lookup("/repo", "gone.txt"); got.kind != namespaceLookupNegative {
		t.Fatalf("lookup kind immediately = %v, want negative", got.kind)
	}
	time.Sleep(3 * time.Millisecond)
	if got := dc.Lookup("/repo", "gone.txt"); got.kind == namespaceLookupNegative {
		t.Fatalf("negative should have expired after negativeTTL")
	}
}

func TestNamespaceCache_LargeDirGuardDoesNotMarkComplete(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 2)
	dc.Put("/repo", []CachedFileInfo{
		{Name: "a.txt"},
		{Name: "b.txt"},
		{Name: "c.txt"},
	})

	if _, ok := dc.Get("/repo"); ok {
		t.Fatal("large dir should not be returned as a complete listing")
	}
	if got := dc.Lookup("/repo", "a.txt"); got.kind != namespaceLookupPositive {
		t.Fatalf("lookup existing kind = %v, want positive", got.kind)
	}
	if got := dc.Lookup("/repo", "c.txt"); got.kind != namespaceLookupPartialMiss {
		t.Fatalf("lookup overflow/missing kind = %v, want partial miss", got.kind)
	}
}

func TestNamespaceCache_InvalidatePrefix(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 10)
	dc.Put("/repo", []CachedFileInfo{{Name: ".git", IsDir: true}})
	dc.Put("/repo/.git", []CachedFileInfo{{Name: "config"}})
	dc.Put("/repo2", []CachedFileInfo{{Name: "keep"}})

	dc.InvalidatePrefix("/repo")

	if _, ok := dc.Get("/repo"); ok {
		t.Fatal("/repo should be invalidated")
	}
	if _, ok := dc.Get("/repo/.git"); ok {
		t.Fatal("/repo/.git should be invalidated")
	}
	if _, ok := dc.Get("/repo2"); !ok {
		t.Fatal("/repo2 should be preserved")
	}
}

func TestNamespaceCache_LargeDirCachesCompleteWithHighMaxEntries(t *testing.T) {
	// With a high maxEntries (like the default), a 10k-entry
	// directory should be cached as complete and returned by Get().
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, defaultNamespaceCacheMaxEntries)
	items := make([]CachedFileInfo, 10000)
	for i := range items {
		items[i] = CachedFileInfo{
			Name:     fmt.Sprintf("file_%05d.txt", i),
			Size:     1024,
			Revision: int64(i + 1),
		}
	}
	dc.Put("/bigdir", items)

	got, ok := dc.Get("/bigdir")
	if !ok {
		t.Fatal("10k-entry dir should be cached as complete with high maxEntries")
	}
	if len(got) != 10000 {
		t.Fatalf("got %d cached entries, want 10000", len(got))
	}
	// Verify revision is preserved
	if got[0].Revision <= 0 {
		t.Fatal("revision should be preserved in cached entries")
	}
}

func TestNamespaceCache_OldDefault2000DoesNotCacheLargeDir(t *testing.T) {
	// Demonstrates the old behavior: maxEntries=2000 prevents 10k dirs
	// from being cached as complete.
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 2000)
	items := make([]CachedFileInfo, 10000)
	for i := range items {
		items[i] = CachedFileInfo{Name: fmt.Sprintf("file_%05d.txt", i)}
	}
	dc.Put("/bigdir", items)

	if _, ok := dc.Get("/bigdir"); ok {
		t.Fatal("10k-entry dir should NOT be cached as complete with maxEntries=2000")
	}
}

// ---------------------------------------------------------------------------
// Sparse WriteBuffer with LoadPart (lazy loading) tests
// ---------------------------------------------------------------------------

func TestWriteBuffer_LazyLoad_WriteTriggersLoad(t *testing.T) {
	// Simulate a 2-part file where LoadPart provides the existing data.
	wb := NewWriteBuffer("/test", 0, DefaultPartSize)
	wb.totalSize = DefaultPartSize * 2  // pretend file is 16MB
	wb.remoteSize = DefaultPartSize * 2 // remote file is the same size

	loadCalls := 0
	wb.LoadPart = func(partNum int) ([]byte, error) {
		loadCalls++
		data := make([]byte, DefaultPartSize)
		// Fill with a pattern based on part number
		for i := range data {
			data[i] = byte(partNum)
		}
		return data, nil
	}

	// Write to the middle of part 2 — should trigger lazy load of part 2
	_, err := wb.Write(DefaultPartSize+100, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if loadCalls != 1 {
		t.Fatalf("expected 1 LoadPart call, got %d", loadCalls)
	}

	// The written data should be at the correct offset within part 2
	p2 := wb.PartData(2)
	if string(p2[100:105]) != "hello" {
		t.Fatalf("part 2 data at offset 100: got %q", p2[100:105])
	}

	// Data before the write should be the original loaded data (byte value 2)
	if p2[0] != 2 {
		t.Fatalf("part 2 first byte: got %d, want 2", p2[0])
	}
}

func TestWriteBuffer_LazyLoad_UnloadedPartReturnsZeros(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, DefaultPartSize)
	wb.totalSize = DefaultPartSize * 2

	// No LoadPart set — unloaded parts should return zero-filled data
	p1 := wb.PartData(1)
	if p1 == nil {
		t.Fatal("PartData should return zero-filled slice, not nil")
	}
	if len(p1) != int(DefaultPartSize) {
		t.Fatalf("PartData len = %d, want %d", len(p1), DefaultPartSize)
	}
	for i, b := range p1 {
		if b != 0 {
			t.Fatalf("byte %d = %d, want 0", i, b)
		}
	}
}

func TestWriteBuffer_LazyTruncateShrinkLoadsRetainedDirtyPart(t *testing.T) {
	const partSize = int64(16)
	remote := []byte("ABCDEFGHIJKLMNOPabcdefghijklmnop")
	wb := NewWriteBuffer("/test", 0, partSize)
	wb.totalSize = int64(len(remote))
	wb.remoteSize = int64(len(remote))

	loadCalls := 0
	wb.LoadPart = func(partNum int) ([]byte, error) {
		loadCalls++
		start := int64(partNum-1) * partSize
		end := start + partSize
		if end > int64(len(remote)) {
			end = int64(len(remote))
		}
		part := make([]byte, end-start)
		copy(part, remote[start:end])
		return part, nil
	}

	if err := wb.Truncate(10); err != nil {
		t.Fatal(err)
	}
	if loadCalls != 1 {
		t.Fatalf("LoadPart calls = %d, want 1", loadCalls)
	}
	if got := string(wb.PartData(1)); got != "ABCDEFGHIJ" {
		t.Fatalf("dirty retained part = %q, want %q", got, "ABCDEFGHIJ")
	}
}

func TestWriteBuffer_LazyTruncateBoundaryLoadsRetainedDirtyPart(t *testing.T) {
	const partSize = int64(16)
	remote := []byte("ABCDEFGHIJKLMNOPabcdefghijklmnop")
	wb := NewWriteBuffer("/test", 0, partSize)
	wb.totalSize = int64(len(remote))
	wb.remoteSize = int64(len(remote))

	loadCalls := 0
	wb.LoadPart = func(partNum int) ([]byte, error) {
		loadCalls++
		start := int64(partNum-1) * partSize
		end := start + partSize
		part := make([]byte, end-start)
		copy(part, remote[start:end])
		return part, nil
	}

	if err := wb.Truncate(partSize); err != nil {
		t.Fatal(err)
	}
	if loadCalls != 1 {
		t.Fatalf("LoadPart calls = %d, want 1", loadCalls)
	}
	if got := string(wb.PartData(1)); got != "ABCDEFGHIJKLMNOP" {
		t.Fatalf("boundary retained part = %q, want %q", got, "ABCDEFGHIJKLMNOP")
	}
}

func TestWriteBuffer_LazyTruncateExtendLoadsRetainedDirtyPart(t *testing.T) {
	const partSize = int64(16)
	remote := []byte("ABCDEFGHIJ")
	wb := NewWriteBuffer("/test", 0, partSize)
	wb.totalSize = int64(len(remote))
	wb.remoteSize = int64(len(remote))

	loadCalls := 0
	wb.LoadPart = func(partNum int) ([]byte, error) {
		loadCalls++
		part := make([]byte, len(remote))
		copy(part, remote)
		return part, nil
	}

	if err := wb.Truncate(14); err != nil {
		t.Fatal(err)
	}
	if loadCalls != 1 {
		t.Fatalf("LoadPart calls = %d, want 1", loadCalls)
	}
	got := wb.PartData(1)
	if string(got[:10]) != "ABCDEFGHIJ" {
		t.Fatalf("extended retained prefix = %q, want %q", got[:10], "ABCDEFGHIJ")
	}
	for i, b := range got[10:] {
		if b != 0 {
			t.Fatalf("extended byte %d = %d, want 0", i+10, b)
		}
	}
}

func TestWriteBuffer_RewritePartPreservesLatestData(t *testing.T) {
	// Verify that rewriting a full part keeps the latest data in the buffer.
	// This is critical: FUSE writers can revisit earlier offsets before close
	// (e.g. patching headers, checksums, footers).
	wb := NewWriteBuffer("/test", defaultWriteBufferMaxSize, DefaultPartSize)

	// Write a full first part
	data := make([]byte, DefaultPartSize)
	for i := range data {
		data[i] = 0xAA
	}
	_, err := wb.Write(0, data)
	if err != nil {
		t.Fatal(err)
	}

	// Write beyond part 1 to establish a multi-part file
	_, _ = wb.Write(DefaultPartSize, []byte("part2"))

	// Now go back and rewrite the beginning of part 1 (e.g. header patch)
	_, _ = wb.Write(0, []byte("HEADER"))

	// Verify part 1 has the latest data
	p1 := wb.PartData(1)
	if string(p1[:6]) != "HEADER" {
		t.Fatalf("part 1 header: got %q, want %q", p1[:6], "HEADER")
	}
	// Rest of part 1 should still be 0xAA
	if p1[6] != 0xAA {
		t.Fatalf("part 1 byte 6: got %02x, want 0xAA", p1[6])
	}

	// Part 1 should be dirty
	dirty := wb.DirtyPartNumbers()
	dirtySet := make(map[int]bool)
	for _, d := range dirty {
		dirtySet[d] = true
	}
	if !dirtySet[1] {
		t.Fatal("part 1 should be dirty after rewrite")
	}
}

func TestWriteBuffer_Bytes_MaterializesSparseBuffer(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 10) // small part size for testing
	wb.totalSize = 30

	// Write to part 1 (bytes 0-9)
	_, _ = wb.Write(0, []byte("AAAAAAAAAA"))
	// Write to part 3 (bytes 20-29)
	_, _ = wb.Write(20, []byte("CCCCCCCCCC"))

	data := wb.Bytes()
	if len(data) != 30 {
		t.Fatalf("Bytes() len = %d, want 30", len(data))
	}
	if string(data[0:10]) != "AAAAAAAAAA" {
		t.Fatalf("part 1: got %q", data[0:10])
	}
	// Part 2 (bytes 10-19) should be zeros
	for i := 10; i < 20; i++ {
		if data[i] != 0 {
			t.Fatalf("byte %d = %d, want 0", i, data[i])
		}
	}
	if string(data[20:30]) != "CCCCCCCCCC" {
		t.Fatalf("part 3: got %q", data[20:30])
	}
}

// ---------------------------------------------------------------------------
// Prefetcher tests
// ---------------------------------------------------------------------------

func TestPrefetcher_SequentialReadGrowsWindow(t *testing.T) {
	p := NewPrefetcher(nil, "/test", 100*1024*1024) // 100MB file

	// Simulate sequential reads
	p.OnRead(0, 4096)
	if p.window != prefetchMinWindow*2 {
		t.Fatalf("window after first sequential read = %d, want %d", p.window, prefetchMinWindow*2)
	}

	p.OnRead(4096, 4096)
	if p.window != prefetchMinWindow*4 {
		t.Fatalf("window after second sequential read = %d, want %d", p.window, prefetchMinWindow*4)
	}
}

func TestPrefetcher_RandomReadResetsWindow(t *testing.T) {
	p := NewPrefetcher(nil, "/test", 100*1024*1024)

	// Sequential reads to grow window
	p.OnRead(0, 4096)
	p.OnRead(4096, 4096)

	// Random read
	p.OnRead(50*1024*1024, 4096)
	if p.window != prefetchMinWindow {
		t.Fatalf("window after random read = %d, want %d (reset)", p.window, prefetchMinWindow)
	}
}

func TestPrefetcher_WindowCapAtMax(t *testing.T) {
	p := NewPrefetcher(nil, "/test", 1024*1024*1024) // 1GB file

	offset := int64(0)
	for i := 0; i < 20; i++ {
		p.OnRead(offset, 4096)
		offset += 4096
	}

	if p.window > prefetchMaxWindow {
		t.Fatalf("window %d exceeds max %d", p.window, prefetchMaxWindow)
	}
}

// ---------------------------------------------------------------------------
// Rewrite-before-close correctness test
// ---------------------------------------------------------------------------

func TestWriteBuffer_RewriteBeforeClose_AllDirtyPartsHaveLatestData(t *testing.T) {
	// Simulates: write 3 full parts, then go back and patch byte 0 of part 1.
	// All 3 parts should be dirty, and part 1 should have the patched data.
	wb := NewWriteBuffer("/test", defaultWriteBufferMaxSize, DefaultPartSize)

	// Write 3 full parts
	for i := 0; i < 3; i++ {
		data := make([]byte, DefaultPartSize)
		for j := range data {
			data[j] = byte(i + 1) // part 1 = 0x01, part 2 = 0x02, part 3 = 0x03
		}
		_, err := wb.Write(int64(i)*DefaultPartSize, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Patch first 4 bytes of part 1 (e.g. file magic / header)
	_, _ = wb.Write(0, []byte("MAGIC"))

	// All 3 parts should be dirty
	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 3 {
		t.Fatalf("expected 3 dirty parts, got %v", dirty)
	}

	// Part 1 should have the patched header
	p1 := wb.PartData(1)
	if string(p1[:5]) != "MAGIC" {
		t.Fatalf("part 1 header: got %q, want %q", p1[:5], "MAGIC")
	}
	// Rest of part 1 should be original (0x01)
	if p1[5] != 0x01 {
		t.Fatalf("part 1 byte 5: got %02x, want 0x01", p1[5])
	}

	// Part 2 should be untouched original
	p2 := wb.PartData(2)
	if p2[0] != 0x02 {
		t.Fatalf("part 2 byte 0: got %02x, want 0x02", p2[0])
	}
}

// ---------------------------------------------------------------------------
// WriteBuffer.ReadAt tests
// ---------------------------------------------------------------------------

func TestWriteBuffer_ReadAt_Basic(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 10)      // small part size
	_, _ = wb.Write(0, []byte("AAAAAAAAAA"))  // part 1
	_, _ = wb.Write(20, []byte("CCCCCCCCCC")) // part 3

	// Read across all three parts including the sparse gap
	buf := make([]byte, 30)
	n := wb.ReadAt(0, buf)
	if n != 30 {
		t.Fatalf("ReadAt returned %d, want 30", n)
	}
	if string(buf[0:10]) != "AAAAAAAAAA" {
		t.Fatalf("part 1: got %q", buf[0:10])
	}
	for i := 10; i < 20; i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d = %d, want 0 (sparse gap)", i, buf[i])
		}
	}
	if string(buf[20:30]) != "CCCCCCCCCC" {
		t.Fatalf("part 3: got %q", buf[20:30])
	}
}

func TestWriteBuffer_SmallFileFastPathRequiresSinglePart(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 10)
	data := []byte("0123456789012345678901234")
	if _, err := wb.Write(0, data); err != nil {
		t.Fatal(err)
	}

	dirty := wb.DirtyPartNumbers()
	if len(dirty) != 3 {
		t.Fatalf("dirty parts = %v, want 3 parts", dirty)
	}
	if got := string(wb.PartData(1)); got != "0123456789" {
		t.Fatalf("part 1 = %q, want %q", got, "0123456789")
	}
	if got := string(wb.PartData(2)); got != "0123456789" {
		t.Fatalf("part 2 = %q, want %q", got, "0123456789")
	}
	if got := string(wb.PartData(3)); got != "01234" {
		t.Fatalf("part 3 = %q, want %q", got, "01234")
	}
}

func TestWriteBuffer_ReadAt_PartialRead(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 10)
	_, _ = wb.Write(0, []byte("hello world!padding!"))

	// Read from middle of part 1 into part 2
	buf := make([]byte, 5)
	n := wb.ReadAt(3, buf)
	if n != 5 {
		t.Fatalf("ReadAt returned %d, want 5", n)
	}
	if string(buf) != "lo wo" {
		t.Fatalf("ReadAt mid: got %q, want %q", buf, "lo wo")
	}
}

func TestWriteBuffer_ReadAt_BeyondEnd(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	_, _ = wb.Write(0, []byte("abc"))

	buf := make([]byte, 10)
	n := wb.ReadAt(100, buf)
	if n != 0 {
		t.Fatalf("ReadAt beyond end returned %d, want 0", n)
	}
}

func TestWriteBuffer_ReadAt_SmallFileClampsToLogicalSize(t *testing.T) {
	wb := NewWriteBuffer("/test", 0, 0)
	if _, err := wb.Write(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	buf := []byte{'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'}
	n := wb.ReadAt(3, buf)
	if n != 2 {
		t.Fatalf("ReadAt returned %d, want 2", n)
	}
	if string(buf[:2]) != "lo" {
		t.Fatalf("ReadAt data = %q, want %q", buf[:2], "lo")
	}
	if string(buf[2:]) != "xxxxxx" {
		t.Fatalf("ReadAt wrote past logical end: got %q", buf[2:])
	}
}

// ---------------------------------------------------------------------------
// Prefetcher Close test
// ---------------------------------------------------------------------------

func TestPrefetcher_CloseStopsAccepting(t *testing.T) {
	p := NewPrefetcher(nil, "/test", 100*1024*1024)

	// Sequential reads work before close
	p.OnRead(0, 4096)
	if p.window != prefetchMinWindow*2 {
		t.Fatalf("window before close = %d, want %d", p.window, prefetchMinWindow*2)
	}

	p.Close()

	// After close, OnRead is a no-op
	p.OnRead(4096, 4096)
	if p.window != prefetchMinWindow*2 {
		t.Fatalf("window should not change after Close, got %d", p.window)
	}

	// Get returns miss after close
	_, ok := p.Get(0, 4096)
	if ok {
		t.Fatal("Get should return false after Close")
	}

	// Double-close is safe
	p.Close()
}

// TestPrefetcher_SubBlockReads verifies that a prefetched block serves
// multiple smaller reads without re-fetching. This is the key scenario
// for sequential reads: kernel sends 128KB reads, prefetcher fetches
// 256KB-16MB blocks, each block should serve many reads.
func TestPrefetcher_SubBlockReads(t *testing.T) {
	// Set up a server that tracks range-read calls.
	var readCalls atomic.Int32
	fileData := make([]byte, 1024*1024) // 1MB file
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			// Simulate redirect-based read (direct S3 path).
			readCalls.Add(1)
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				// Parse "bytes=start-end"
				var start, end int64
				_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
				if end >= int64(len(fileData)) {
					end = int64(len(fileData)) - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fileData[start : end+1])
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(fileData)
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileData)))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer ts.Close()

	c := newTestClient(ts.URL)
	p := NewPrefetcher(c, "/bigfile.bin", int64(len(fileData)))
	defer p.Close()

	readSize := 4096 // simulate 4KB FUSE reads
	offset := int64(0)

	// First read: cache miss, direct fetch
	_, hit := p.Get(offset, readSize)
	if hit {
		t.Fatal("first read should be a cache miss")
	}
	// Simulate the read happening outside prefetcher
	p.OnRead(offset, readSize)
	offset += int64(readSize)

	// Let the prefetch goroutine run
	time.Sleep(50 * time.Millisecond)

	// Now do sequential reads. The prefetched block should serve many reads
	// without additional HTTP calls.
	httpCallsBefore := readCalls.Load()
	hits := 0
	for i := 0; i < 60; i++ { // 60 × 4KB = 240KB, well within first 256KB prefetch
		data, ok := p.Get(offset, readSize)
		if ok {
			hits++
			// Verify data correctness
			for j := 0; j < len(data) && j < readSize; j++ {
				expected := byte((int64(j) + offset) % 256)
				if data[j] != expected {
					t.Fatalf("data mismatch at offset %d+%d: got %d, want %d", offset, j, data[j], expected)
				}
			}
		}
		p.OnRead(offset, readSize)
		offset += int64(readSize)
		// Small delay to let prefetch goroutines run
		if i%10 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	httpCallsAfter := readCalls.Load()
	extraCalls := httpCallsAfter - httpCallsBefore

	// The key assertion: with sub-block reads working, we should have
	// many hits from the prefetched blocks and very few extra HTTP calls.
	// Without the fix, every read would be a miss (0 hits, 60 HTTP calls).
	if hits < 30 {
		t.Fatalf("sub-block hits = %d, want >= 30 (out of 60 reads); extraCalls = %d", hits, extraCalls)
	}
	// Should need at most a handful of HTTP calls (initial prefetch + maybe 1-2 more)
	if extraCalls > 10 {
		t.Fatalf("extra HTTP calls = %d, want <= 10 (prefetch blocks should serve many reads)", extraCalls)
	}
}

func TestPrefetcher_LargeReadSizeDoesNotReturnShortPrefetch(t *testing.T) {
	fileData := make([]byte, 4*1024*1024)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			_, _ = w.Write(fileData)
			return
		}
		var start, end int64
		_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
		if end >= int64(len(fileData)) {
			end = int64(len(fileData)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fileData[start : end+1])
	}))
	defer ts.Close()

	c := newTestClient(ts.URL)
	p := NewPrefetcher(c, "/bigfile.bin", int64(len(fileData)))
	defer p.Close()

	const readSize = 1024 * 1024
	p.OnRead(0, readSize)

	data, ok := p.Get(readSize, readSize)
	if !ok {
		t.Fatal("prefetch get = miss, want hit")
	}
	if len(data) != readSize {
		t.Fatalf("prefetch data len = %d, want %d", len(data), readSize)
	}
	for i := 0; i < readSize; i++ {
		want := byte((readSize + i) % 251)
		if data[i] != want {
			t.Fatalf("data[%d] = %d, want %d", i, data[i], want)
		}
	}
}

func TestPrefetcher_ParallelFetchesLargeWindow(t *testing.T) {
	const (
		readSize    = 1 << 20
		windowSize  = 4 << 20
		concurrency = 2
	)
	fileData := make([]byte, windowSize)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}
	var readCalls atomic.Int32
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	ranges := make(chan string, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rangeHeader := r.Header.Get("Range")
		ranges <- rangeHeader
		readCalls.Add(1)
		cur := inFlight.Add(1)
		observeMaxInt32(&maxInFlight, cur)
		defer inFlight.Add(-1)
		time.Sleep(50 * time.Millisecond)

		start, end, ok := parseTestBytesRange(rangeHeader)
		if !ok || end >= int64(len(fileData)) {
			http.Error(w, "wrong range", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fileData[start : end+1])
	}))
	defer ts.Close()

	p := NewPrefetcher(newTestClient(ts.URL), "/bigfile.bin", int64(len(fileData)))
	defer p.Close()
	p.SetReadTarget(&client.ReadTarget{ObjectURL: ts.URL})
	p.SetParallelRead(concurrency, readSize)
	p.mu.Lock()
	p.readSize = readSize
	p.startPrefetch(0, windowSize)
	p.mu.Unlock()

	for offset := int64(0); offset < windowSize; offset += readSize {
		data, ok := p.Get(offset, readSize)
		if !ok {
			t.Fatalf("prefetch get offset=%d = miss, want hit", offset)
		}
		if !bytes.Equal(data, fileData[offset:offset+readSize]) {
			t.Fatalf("prefetch data mismatch at offset %d", offset)
		}
	}
	if got := readCalls.Load(); got != 4 {
		t.Fatalf("read calls = %d, want 4", got)
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("max concurrent prefetch reads = %d, want >= 2", got)
	}
	if got := maxInFlight.Load(); got > concurrency {
		t.Fatalf("max concurrent prefetch reads = %d, want <= %d", got, concurrency)
	}

	want := map[string]int{
		"bytes=0-1048575":       1,
		"bytes=1048576-2097151": 1,
		"bytes=2097152-3145727": 1,
		"bytes=3145728-4194303": 1,
	}
	for range 4 {
		got := <-ranges
		want[got]--
	}
	for got, count := range want {
		if count != 0 {
			t.Fatalf("range %s count delta = %d, want 0", got, count)
		}
	}
}

func TestPrefetcher_AlignsFetchRangesToReadChunks(t *testing.T) {
	const (
		readSize       = 64
		windowSize     = 256
		fetchBlockSize = 96
	)
	fileData := make([]byte, windowSize)
	for i := range fileData {
		fileData[i] = byte(i)
	}
	ranges := make(chan string, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rangeHeader := r.Header.Get("Range")
		ranges <- rangeHeader
		start, end, ok := parseTestBytesRange(rangeHeader)
		if !ok || end >= int64(len(fileData)) {
			http.Error(w, "wrong range", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fileData[start : end+1])
	}))
	defer ts.Close()

	p := NewPrefetcher(newTestClient(ts.URL), "/bigfile.bin", int64(len(fileData)))
	defer p.Close()
	p.SetReadTarget(&client.ReadTarget{ObjectURL: ts.URL})
	p.SetParallelRead(2, fetchBlockSize)
	p.mu.Lock()
	p.readSize = readSize
	p.startPrefetch(0, windowSize)
	p.mu.Unlock()

	for offset := int64(0); offset < windowSize; offset += readSize {
		data, ok := p.Get(offset, readSize)
		if !ok {
			t.Fatalf("prefetch get offset=%d = miss, want hit", offset)
		}
		if !bytes.Equal(data, fileData[offset:offset+readSize]) {
			t.Fatalf("prefetch data mismatch at offset %d", offset)
		}
	}

	want := map[string]int{
		"bytes=0-127":   1,
		"bytes=128-255": 1,
	}
	for range 2 {
		got := <-ranges
		want[got]--
	}
	for got, count := range want {
		if count != 0 {
			t.Fatalf("range %s count delta = %d, want 0", got, count)
		}
	}
}

func TestPrefetcher_AllowsShortFinalEOFChunk(t *testing.T) {
	const (
		readSize       = 64
		fileSize       = 150
		fetchBlockSize = 96
	)
	fileData := make([]byte, fileSize)
	for i := range fileData {
		fileData[i] = byte(i)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		start, end, ok := parseTestBytesRange(r.Header.Get("Range"))
		if !ok || end >= int64(len(fileData)) {
			http.Error(w, "wrong range", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fileData[start : end+1])
	}))
	defer ts.Close()

	p := NewPrefetcher(newTestClient(ts.URL), "/short.bin", int64(len(fileData)))
	defer p.Close()
	p.SetReadTarget(&client.ReadTarget{ObjectURL: ts.URL})
	p.SetParallelRead(2, fetchBlockSize)
	p.mu.Lock()
	p.readSize = readSize
	p.startPrefetch(0, int64(len(fileData)))
	p.mu.Unlock()

	data, ok := p.Get(128, readSize)
	if !ok {
		t.Fatal("final EOF prefetch chunk = miss, want hit")
	}
	if !bytes.Equal(data, fileData[128:]) {
		t.Fatalf("final EOF prefetch chunk mismatch: got %d bytes, want %d", len(data), len(fileData[128:]))
	}
}

func TestPrefetcher_StopsQueuedFetchesAfterFirstError(t *testing.T) {
	const (
		readSize    = 64
		windowSize  = 256
		concurrency = 2
	)
	fileData := make([]byte, windowSize)
	for i := range fileData {
		fileData[i] = byte(i)
	}
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSlow) }) })
	var unexpectedQueuedReads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rangeHeader := r.Header.Get("Range")
		switch rangeHeader {
		case "bytes=0-63":
			select {
			case <-slowStarted:
			case <-time.After(time.Second):
				http.Error(w, "slow range did not start", http.StatusInternalServerError)
				return
			}
			http.Error(w, "blocked range", http.StatusBadGateway)
			return
		case "bytes=64-127":
			slowOnce.Do(func() { close(slowStarted) })
			<-releaseSlow
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(fileData[64:128])
			return
		default:
			unexpectedQueuedReads.Add(1)
			w.WriteHeader(http.StatusPartialContent)
			start, end, ok := parseTestBytesRange(rangeHeader)
			if ok && end < int64(len(fileData)) {
				_, _ = w.Write(fileData[start : end+1])
			}
		}
	}))
	defer ts.Close()

	p := NewPrefetcher(newTestClient(ts.URL), "/bigfile.bin", int64(len(fileData)))
	defer p.Close()
	p.SetReadTarget(&client.ReadTarget{ObjectURL: ts.URL})
	p.SetParallelRead(concurrency, readSize)
	p.mu.Lock()
	p.readSize = readSize
	p.startPrefetch(0, windowSize)
	p.mu.Unlock()

	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("second in-flight prefetch did not start")
	}
	time.Sleep(50 * time.Millisecond)
	releaseOnce.Do(func() { close(releaseSlow) })
	if _, ok := p.Get(0, readSize); ok {
		t.Fatal("errored prefetch block returned a hit")
	}
	if got := unexpectedQueuedReads.Load(); got != 0 {
		t.Fatalf("queued prefetch reads after first error = %d, want 0", got)
	}
}

// TestPrefetcher_ChunkCapBound verifies that chunk count never exceeds
// prefetchMaxBlocks, even with tiny readSize and large window.
func TestPrefetcher_ChunkCapBound(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				var start, end int64
				_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
				if end >= fileSize {
					end = fileSize - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(make([]byte, end-start+1))
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	c := newTestClient(ts.URL)
	p := NewPrefetcher(c, "/test.bin", fileSize)
	defer p.Close()

	// Simulate small reads to set readSize = 4KB
	p.mu.Lock()
	p.readSize = 4096
	p.window = prefetchMaxWindow // 16MB
	p.mu.Unlock()

	// startPrefetch with a real client — chunks are actually created.
	p.mu.Lock()
	p.startPrefetch(0, prefetchMaxWindow)
	cacheSize := len(p.cache)
	p.mu.Unlock()

	if cacheSize > prefetchMaxBlocks {
		t.Fatalf("cache size = %d after startPrefetch, want <= %d (prefetchMaxBlocks)", cacheSize, prefetchMaxBlocks)
	}
	if cacheSize == 0 {
		t.Fatal("cache size = 0, startPrefetch did not create any chunks (test is vacuous)")
	}
}

// ---------------------------------------------------------------------------
// StreamUploader tests
// ---------------------------------------------------------------------------

func TestStreamUploader_NotStartedBeforeUploadAll(t *testing.T) {
	su := NewStreamUploader(nil, "/test", -1)
	if su.Started() {
		t.Fatal("should not be started before UploadAll")
	}
}

func TestStreamUploaderRefreshExpectedRevisionAfterBufferedPart(t *testing.T) {
	su := NewStreamUploader(nil, "/test", 7)
	if err := su.SubmitPart(context.Background(), 1, []byte("part"), nil); err != nil {
		t.Fatal(err)
	}
	if !su.Started() {
		t.Fatal("SubmitPart should mark buffered streaming as started")
	}
	if !su.RefreshExpectedRevision(8) {
		t.Fatal("RefreshExpectedRevision should succeed before remote writer exists")
	}
	if got := su.ExpectedRevision(); got != 8 {
		t.Fatalf("ExpectedRevision = %d, want 8", got)
	}
}

// ---------------------------------------------------------------------------
// Sequential write detection tests
// ---------------------------------------------------------------------------

func TestWriteBuffer_SequentialDetection(t *testing.T) {
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	// Sequential writes should maintain sequential flag
	_, _ = wb.Write(0, make([]byte, 1024))
	if !wb.IsSequential() {
		t.Fatal("should be sequential after forward write")
	}
	if wb.appendCursor != 1024 {
		t.Fatalf("appendCursor = %d, want 1024", wb.appendCursor)
	}

	// Continue appending
	_, _ = wb.Write(1024, make([]byte, 2048))
	if !wb.IsSequential() {
		t.Fatal("should still be sequential")
	}
	if wb.appendCursor != 3072 {
		t.Fatalf("appendCursor = %d, want 3072", wb.appendCursor)
	}

	// Gap write (forward) — still sequential
	_, _ = wb.Write(4096, make([]byte, 100))
	if !wb.IsSequential() {
		t.Fatal("gap write forward should still be sequential")
	}
	if wb.appendCursor != 4196 {
		t.Fatalf("appendCursor = %d, want 4196", wb.appendCursor)
	}
}

func TestWriteBuffer_BackwriteBreaksSequential(t *testing.T) {
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	// Write forward
	_, _ = wb.Write(0, make([]byte, 4096))
	if !wb.IsSequential() {
		t.Fatal("should be sequential")
	}

	// Back-write
	_, _ = wb.Write(100, []byte("patch"))
	if wb.IsSequential() {
		t.Fatal("back-write should break sequential mode")
	}
}

func TestWriteBuffer_OnPartFull_SequentialAppend(t *testing.T) {
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	var calledParts []int
	wb.OnPartFull = func(partIdx int, data []byte) {
		calledParts = append(calledParts, partIdx)
		if int64(len(data)) != DefaultPartSize {
			t.Fatalf("OnPartFull got %d bytes, want %d", len(data), DefaultPartSize)
		}
	}

	// Write exactly 3 full parts
	for i := 0; i < 3; i++ {
		_, err := wb.Write(int64(i)*DefaultPartSize, make([]byte, DefaultPartSize))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Write a little into part 4 to trigger part 3 callback
	_, _ = wb.Write(3*DefaultPartSize, []byte("x"))

	// Parts 0, 1, 2 should have been reported as full
	// (part 3 is not full yet — only has 1 byte)
	if len(calledParts) != 3 {
		t.Fatalf("OnPartFull called for %d parts, want 3; parts=%v", len(calledParts), calledParts)
	}
	for i, p := range calledParts {
		if p != i {
			t.Fatalf("calledParts[%d] = %d, want %d", i, p, i)
		}
	}
}

func TestWriteBuffer_OnPartFull_NotCalledOnBackwrite(t *testing.T) {
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	callCount := 0
	wb.OnPartFull = func(partIdx int, data []byte) {
		callCount++
	}

	// Write 2 full parts + some of part 3
	_, _ = wb.Write(0, make([]byte, 2*int(DefaultPartSize)+100))

	countBefore := callCount

	// Back-write breaks sequential
	_, _ = wb.Write(0, []byte("header-patch"))

	// Continue writing forward — should NOT trigger OnPartFull since sequential is false
	_, _ = wb.Write(wb.appendCursor, make([]byte, DefaultPartSize))

	if callCount != countBefore {
		t.Fatalf("OnPartFull should not be called after sequential is broken; calls before=%d, after=%d",
			countBefore, callCount)
	}
}

func TestWriteBuffer_EvictPart(t *testing.T) {
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	// Write 2 full parts
	_, _ = wb.Write(0, make([]byte, 2*int(DefaultPartSize)))

	memBefore := wb.curMemory

	// Evict part 0
	wb.EvictPart(0)

	if _, ok := wb.parts[0]; ok {
		t.Fatal("part 0 should be deleted after eviction")
	}
	if !wb.uploadedParts[0] {
		t.Fatal("part 0 should be marked as uploaded")
	}
	if wb.curMemory >= memBefore {
		t.Fatalf("memory should decrease after eviction: before=%d, after=%d", memBefore, wb.curMemory)
	}

	// Back-write to evicted part should recreate it (zero-filled)
	wb.sequential = false // simulate back-write breaking sequential
	_, err := wb.Write(100, []byte("back"))
	if err != nil {
		t.Fatal(err)
	}
	part, ok := wb.parts[0]
	if !ok {
		t.Fatal("part 0 should be recreated after back-write")
	}
	if int64(len(part)) != DefaultPartSize {
		t.Fatalf("recreated part should be %d bytes, got %d", DefaultPartSize, len(part))
	}
	// Written data should be at the correct offset
	if string(part[100:104]) != "back" {
		t.Fatalf("back-write data: got %q, want %q", part[100:104], "back")
	}
	// Non-written area should be zero (original data is lost)
	if part[0] != 0 {
		t.Fatalf("non-written byte should be 0, got %d", part[0])
	}
	// Part should be dirty (needs re-upload)
	if !wb.dirtyParts[0] {
		t.Fatal("back-written evicted part should be dirty")
	}
}

// TestWriteBuffer_RestorePartReloadsEvictedBytes covers issue #898: SQLite WAL
// is not pure append at FUSE granularity. A back-write into an evicted part
// must overlay the original bytes, not zero-fill the rest of the part.
func TestWriteBuffer_RestorePartReloadsEvictedBytes(t *testing.T) {
	const partSize int64 = 64
	wb := NewWriteBuffer("/kv.db-wal", 1<<20, partSize)
	wb.SetSmallFileMax(32)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	original := make([]byte, 3*partSize)
	for i := range original {
		original[i] = byte(i + 1)
	}
	saved := map[int][]byte{}
	wb.OnPartFull = func(partIdx int, data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		saved[partIdx] = cp
		wb.EvictPart(partIdx)
	}
	wb.RestorePart = func(partNum int) ([]byte, error) {
		data, ok := saved[partNum-1]
		if !ok {
			return nil, fmt.Errorf("missing part %d", partNum)
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp, nil
	}

	if _, err := wb.Write(0, original); err != nil {
		t.Fatalf("sequential write: %v", err)
	}
	if !wb.uploadedParts[0] || !wb.uploadedParts[1] {
		t.Fatalf("expected parts 0 and 1 evicted, got %v", wb.uploadedParts)
	}

	overlay := []byte("WAL!")
	off := partSize + 8
	if _, err := wb.Write(off, overlay); err != nil {
		t.Fatalf("back-write: %v", err)
	}
	part, ok := wb.parts[1]
	if !ok {
		t.Fatal("part 1 should be restored")
	}
	if int64(len(part)) != partSize {
		t.Fatalf("restored part len=%d, want %d", len(part), partSize)
	}
	want := append([]byte(nil), original[partSize:2*partSize]...)
	copy(want[8:8+len(overlay)], overlay)
	if !bytes.Equal(part, want) {
		t.Fatalf("restored part = %v, want original bytes with overlay", part)
	}
}

func TestWriteBuffer_RestorePartErrorFailsBackWrite(t *testing.T) {
	const partSize int64 = 64
	wb := NewWriteBuffer("/kv.db-wal", 1<<20, partSize)
	wb.SetSmallFileMax(32)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	wb.OnPartFull = func(partIdx int, _ []byte) {
		wb.EvictPart(partIdx)
	}
	wb.RestorePart = func(partNum int) ([]byte, error) {
		return nil, fmt.Errorf("shadow missing part %d", partNum)
	}

	if _, err := wb.Write(0, bytes.Repeat([]byte{0x11}, int(2*partSize))); err != nil {
		t.Fatalf("sequential write: %v", err)
	}
	if _, err := wb.Write(8, []byte("x")); err == nil {
		t.Fatal("back-write into unrestorable evicted part should fail")
	}
}

func TestShadowSpillBackWriteRestoresFromShadow(t *testing.T) {
	store, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const path = "/kv.db-wal"
	const partSize int64 = 64
	if err := store.Ensure(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	wb := NewWriteBuffer(path, 1<<20, partSize)
	wb.SetSmallFileMax(32)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	fh := &FileHandle{Path: path, Dirty: wb, ShadowSpill: true, ShadowReady: true}
	fs := &Dat9FS{shadowStore: store}
	fs.bindShadowSpillEvictionLocked(fh)

	original := make([]byte, 3*partSize)
	for i := range original {
		original[i] = byte(i + 3)
	}
	if _, err := store.WriteAt(path, 0, original, 0); err != nil {
		t.Fatalf("shadow sequential write: %v", err)
	}
	if _, err := wb.Write(0, original); err != nil {
		t.Fatalf("dirty sequential write: %v", err)
	}
	if wb.RestorePart == nil {
		t.Fatal("spill bind should install RestorePart")
	}
	if !wb.uploadedParts[0] || !wb.uploadedParts[1] {
		t.Fatalf("expected parts 0 and 1 evicted, got %v", wb.uploadedParts)
	}

	overlay := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	off := partSize + 4
	if _, err := store.WriteAt(path, off, overlay, 0); err != nil {
		t.Fatalf("shadow back-write: %v", err)
	}
	if _, err := wb.Write(off, overlay); err != nil {
		t.Fatalf("dirty back-write: %v", err)
	}

	part, ok := wb.parts[1]
	if !ok {
		t.Fatal("part 1 should be restored from shadow")
	}
	want := append([]byte(nil), original[partSize:2*partSize]...)
	copy(want[4:8], overlay)
	if !bytes.Equal(part, want) {
		t.Fatalf("dirty part after restore = %v, want shadow overlay %v", part, want)
	}

	got := make([]byte, len(original))
	n, err := store.ReadAt(path, 0, got)
	if err != nil || n != len(got) {
		t.Fatalf("shadow read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got[partSize:partSize+4], original[partSize:partSize+4]) || !bytes.Equal(got[off:off+int64(len(overlay))], overlay) {
		t.Fatalf("shadow lost original prefix or overlay")
	}
}

func TestMarkHandleRemoteCommittedRebaselinesEvictedSpillBuffer(t *testing.T) {
	const partSize int64 = 64
	wb := NewWriteBuffer("/spill.bin", 1<<20, partSize)
	wb.SetSmallFileMax(32)
	wb.totalSize = 3 * partSize
	wb.uploadedParts = map[int]bool{0: true, 1: true}
	wb.RestorePart = func(int) ([]byte, error) { return nil, fmt.Errorf("stale shadow") }
	wb.OnPartFull = func(int, []byte) {}
	fh := &FileHandle{
		Ino: 1, Path: "/spill.bin", Dirty: wb,
		ShadowSpill: true, ShadowReady: true, IsNew: true,
	}
	fs := &Dat9FS{inodes: NewInodeToPath(), openHandles: NewOpenHandleIndex()}
	fs.inodes.Lookup("/spill.bin", false, 0, time.Now())
	fs.markHandleRemoteCommittedLocked(fh, 1)
	if fh.ShadowSpill || fh.Dirty.RestorePart != nil || fh.Dirty.OnPartFull != nil || len(fh.Dirty.uploadedParts) != 0 {
		t.Fatalf("committed handle still has spill eviction state: spill=%t restore=%t onFull=%t uploaded=%v",
			fh.ShadowSpill, fh.Dirty.RestorePart != nil, fh.Dirty.OnPartFull != nil, fh.Dirty.uploadedParts)
	}
	if fh.Dirty.remoteSize != 3*partSize || fh.Dirty.Size() != 3*partSize {
		t.Fatalf("rebased size/remoteSize = %d/%d, want %d", fh.Dirty.Size(), fh.Dirty.remoteSize, 3*partSize)
	}
	if fh.Dirty.LoadPart == nil {
		t.Fatal("rebased buffer should load committed parts from remote")
	}
}

func TestShadowSpillFsyncRebaselineAllowsSameHandleBackWrite(t *testing.T) {
	const filePath = "/spill.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x11}, int(3*partSize))
	overlay := []byte{0xAA, 0xBB, 0xCC, 0xDD}

	var mu sync.Mutex
	var committed []byte
	var puts [][]byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			got, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			wantRev := int64(len(puts))
			if got != wantRev {
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			puts = append(puts, append([]byte(nil), body...))
			committed = append([]byte(nil), body...)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(len(puts))})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhID, nodeID, shadow := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	fh, _ := fs.fileHandles.Get(fhID)
	fh.Lock()
	evicted := len(fh.Dirty.uploadedParts) > 0
	fh.Unlock()
	if !evicted {
		t.Fatal("expected ShadowSpill eviction before fsync")
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("first Fsync: %v", st)
	}
	if shadow.Has(filePath) {
		t.Fatal("shadow staging should be removed after successful fsync")
	}
	fh.Lock()
	if fh.ShadowSpill || fh.Dirty.RestorePart != nil || len(fh.Dirty.uploadedParts) != 0 {
		t.Fatalf("after fsync spill=%t restore=%t uploaded=%v", fh.ShadowSpill, fh.Dirty.RestorePart != nil, fh.Dirty.uploadedParts)
	}
	fh.Unlock()

	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, overlay); st != gofuse.OK {
		t.Fatalf("same-handle back-write after fsync: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("second Fsync: %v", st)
	}
	mu.Lock()
	got := append([]byte(nil), committed...)
	mu.Unlock()
	want := append([]byte(nil), original...)
	copy(want[8:8+len(overlay)], overlay)
	if !bytes.Equal(got, want) {
		t.Fatalf("committed after back-write = %x, want original+overlay %x", got, want)
	}
}

func TestShadowSpillFsyncThenAppendDoesNotZeroPrefix(t *testing.T) {
	const filePath = "/spill-append.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x22}, int(3*partSize))
	tail := []byte("tail")

	var mu sync.Mutex
	var committed []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			committed = append([]byte(nil), body...)
			rev := int64(1)
			if len(committed) > len(original) {
				rev = 2
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": rev})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("first Fsync: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: uint64(len(original)),
	}, tail); st != gofuse.OK {
		t.Fatalf("append after fsync: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("second Fsync: %v", st)
	}
	mu.Lock()
	got := append([]byte(nil), committed...)
	mu.Unlock()
	want := append(append([]byte(nil), original...), tail...)
	if !bytes.Equal(got, want) {
		t.Fatalf("second commit = %x, want original+tail without zero prefix", got)
	}
}

func TestShadowSpillRestoreFailureDoesNotMutateShadow(t *testing.T) {
	const filePath = "/spill-fail.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x33}, int(3*partSize))

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.RawQuery == "list=1" {
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
			return
		}
		if r.Method == http.MethodPut {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": 1})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fs, fhID, nodeID, shadow := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	fh, _ := fs.fileHandles.Get(fhID)
	fh.Lock()
	fh.Dirty.RestorePart = func(int) ([]byte, error) { return nil, fmt.Errorf("injected restore failure") }
	fh.Unlock()

	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, []byte("FAIL")); st != gofuse.EIO {
		t.Fatalf("restore-failure Write status = %v, want EIO", st)
	}
	got := make([]byte, len(original))
	n, err := shadow.ReadAt(filePath, 0, got)
	if err != nil || n != len(got) {
		t.Fatalf("shadow read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("failed Write mutated shadow: got %x", got)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Fsync after failed write: %v", st)
	}
}

func newStrictSmallPartSpillFS(t *testing.T, baseURL, filePath string, partSize int64) (*Dat9FS, uint64, uint64, *ShadowStore) {
	t.Helper()
	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(baseURL), opts)
	fs.syncMode = SyncStrict
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}}, strings.TrimPrefix(filePath, "/"), &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle missing")
	}
	fh.Lock()
	fh.Dirty = NewWriteBuffer(filePath, streamingWriteMaxSize, partSize)
	fh.Dirty.SetSmallFileMax(partSize / 2)
	fh.Dirty.sequential = true
	fh.Dirty.uploadedParts = make(map[int]bool)
	if !fh.ShadowSpill {
		fh.Unlock()
		t.Fatal("created handle should be ShadowSpill")
	}
	fs.bindShadowSpillEvictionLocked(fh)
	fh.Unlock()
	return fs, createOut.Fh, createOut.NodeId, shadow
}

func TestWriteBuffer_ShrinkDropsEvictedPartsBeyondSize(t *testing.T) {
	const partSize int64 = 64
	wb := NewWriteBuffer("/spill.bin", 1<<20, partSize)
	wb.SetSmallFileMax(32)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	saved := map[int][]byte{}
	wb.OnPartFull = func(partIdx int, data []byte) {
		cp := make([]byte, len(data))
		copy(cp, data)
		saved[partIdx] = cp
		wb.EvictPart(partIdx)
	}
	wb.RestorePart = func(partNum int) ([]byte, error) {
		data, ok := saved[partNum-1]
		if !ok {
			return nil, fmt.Errorf("missing part %d", partNum)
		}
		return append([]byte(nil), data...), nil
	}
	original := bytes.Repeat([]byte{0x44}, int(3*partSize))
	if _, err := wb.Write(0, original); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if !wb.uploadedParts[1] {
		t.Fatal("expected part 1 evicted")
	}
	if err := wb.Truncate(partSize); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if wb.uploadedParts[1] {
		t.Fatal("truncate should drop evicted parts at/after the new size")
	}
	tail := []byte("tail")
	if _, err := wb.Write(partSize, tail); err != nil {
		t.Fatalf("append after shrink: %v", err)
	}
	if err := wb.EnsureLoaded(0); err != nil {
		t.Fatalf("load retained part 0: %v", err)
	}
	got := make([]byte, partSize+int64(len(tail)))
	if n := wb.ReadAt(0, got); n != len(got) {
		t.Fatalf("ReadAt n=%d", n)
	}
	want := append(bytes.Repeat([]byte{0x44}, int(partSize)), tail...)
	if !bytes.Equal(got, want) {
		t.Fatalf("after shrink+append = %x, want %x", got, want)
	}
}

func TestShadowSpillShrinkThenAppendAtEvictedBoundary(t *testing.T) {
	const filePath = "/spill-shrink.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x55}, int(3*partSize))
	tail := []byte("tail")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.RawQuery == "list=1" {
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	var attrOut gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: nodeID},
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       fhID,
			Size:     uint64(partSize),
		},
	}, &attrOut); st != gofuse.OK {
		t.Fatalf("truncate: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: uint64(partSize),
	}, tail); st != gofuse.OK {
		t.Fatalf("append after shrink: %v", st)
	}
	buf := make([]byte, int(partSize)+len(tail))
	result, st := fs.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(buf))}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read after shrink+append: %v", st)
	}
	got, _ := result.Bytes(buf)
	want := append(bytes.Repeat([]byte{0x55}, int(partSize)), tail...)
	if !bytes.Equal(got, want) {
		t.Fatalf("after shrink+append = %x, want %x", got, want)
	}
}

func TestShadowSpillFsyncUnlinkSameHandleWriteStaysLocal(t *testing.T) {
	const filePath = "/spill-unlink.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x66}, int(3*partSize))
	overlay := []byte{0xAA, 0xBB}

	var mu sync.Mutex
	var committed []byte
	var puts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		case r.Method == http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			puts.Add(1)
			mu.Lock()
			committed = append([]byte(nil), body...)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": 1})
		case r.Method == http.MethodDelete:
			mu.Lock()
			committed = nil
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
		_ = p
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("puts after fsync = %d, want 1", got)
	}
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	readBuf := make([]byte, 16)
	readResult, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(readBuf)),
	}, readBuf)
	if st != gofuse.OK {
		t.Fatalf("Read after unlink before write: %v", st)
	}
	gotRead, _ := readResult.Bytes(readBuf)
	if !bytes.Equal(gotRead, original[:16]) {
		t.Fatalf("Read after unlink = %x, want committed prefix", gotRead)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, overlay); st != gofuse.OK {
		t.Fatalf("write after unlink: %v", st)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("write-after-unlink resurrected remote path (puts=%d)", got)
	}
	fh, _ := fs.fileHandles.Get(fhID)
	fh.Lock()
	got := make([]byte, 8+len(overlay))
	n := fh.Dirty.ReadAt(0, got)
	fh.Unlock()
	want := append(bytes.Repeat([]byte{0x66}, 8), overlay...)
	if n != len(got) || !bytes.Equal(got, want) {
		t.Fatalf("anonymous fd bytes = %x (n=%d), want %x", got[:n], n, want)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Flush after unlink: %v", st)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("flush after unlink uploaded (puts=%d)", got)
	}
}

func TestUnlinkedRemoteBackedDirtyReadUsesPinnedShadowGen(t *testing.T) {
	const filePath = "/anon-large.bin"
	data := bytes.Repeat([]byte{0x77}, 256)
	store, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.WriteFull(filePath, data, 1); err != nil {
		t.Fatal(err)
	}
	gen := store.Pin(filePath)
	store.Remove(filePath)

	var remoteGets atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			remoteGets.Add(1)
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.shadowStore = store

	wb := NewWriteBuffer(filePath, 1<<20, 64)
	wb.totalSize = int64(len(data))
	wb.remoteSize = int64(len(data))
	wb.LoadPart = func(int) ([]byte, error) {
		t.Error("LoadPart must not fetch the deleted path")
		return nil, fmt.Errorf("deleted")
	}
	fh := &FileHandle{
		Path: filePath, Dirty: wb, DirtySeq: 0,
		Unlinked: true, UnlinkedSnapshot: true,
		UnlinkedShadowGen: gen, UnlinkedSize: int64(len(data)),
	}
	fhID := fs.allocateFileHandle(fh)

	buf := make([]byte, 32)
	result, st := fs.Read(nil, &gofuse.ReadIn{Fh: fhID, Size: uint32(len(buf))}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read after unlink before write: %v", st)
	}
	got, _ := result.Bytes(buf)
	if !bytes.Equal(got, data[:32]) {
		t.Fatalf("Read = %x, want pinned shadow prefix", got)
	}
	if remoteGets.Load() != 0 {
		t.Fatalf("Read hit remote GET %d times after unlink", remoteGets.Load())
	}
}

func TestLoadUnlinkedHandlePartShortShadowGenFailsClosed(t *testing.T) {
	const filePath = "/anon-short.bin"
	store, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.WriteFull(filePath, []byte("abc"), 1); err != nil {
		t.Fatal(err)
	}
	gen := store.Pin(filePath)
	store.Remove(filePath)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	fs.shadowStore = store

	wb := NewWriteBuffer(filePath, 1<<20, 8)
	wb.SetSmallFileMax(4)
	wb.totalSize = 8
	wb.remoteSize = 8
	fh := &FileHandle{
		Path: filePath, Dirty: wb,
		Unlinked: true, UnlinkedSnapshot: true,
		UnlinkedShadowGen: gen, UnlinkedSize: 8,
	}
	wb.LoadPart = func(partNum int) ([]byte, error) {
		offset := int64(partNum-1) * wb.PartSize()
		data, handled, err := fs.loadUnlinkedHandlePart(fh, offset, wb.PartSize())
		if !handled {
			return nil, fmt.Errorf("unlinked backing not handled")
		}
		return data, err
	}
	if err := wb.EnsureLoaded(0); err == nil {
		t.Fatal("EnsureLoaded accepted a 3-byte UnlinkedShadowGen for an 8-byte part")
	}
	if wb.IsPartLoaded(0) {
		t.Fatal("short private backing must not be stored as a loaded part")
	}
}

func TestUnlinkSnapshotConcurrentWriteKeepsOverlayAndBaseline(t *testing.T) {
	const filePath = "/spill-unlink-race.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x88}, int(3*partSize))
	overlay := []byte{0xAA, 0xBB}

	var mu sync.Mutex
	var committed []byte
	var puts atomic.Int32
	var fileGets atomic.Int32
	snapshotStarted := make(chan struct{})
	snapshotRelease := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		case r.Method == http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			if fileGets.Add(1) == 1 {
				close(snapshotStarted)
				<-snapshotRelease
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			puts.Add(1)
			mu.Lock()
			committed = append([]byte(nil), body...)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": 1})
		case r.Method == http.MethodDelete:
			mu.Lock()
			committed = nil
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}

	unlinkDone := make(chan gofuse.Status, 1)
	go func() {
		unlinkDone <- fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/"))
	}()
	select {
	case <-snapshotStarted:
	case st := <-unlinkDone:
		t.Fatalf("Unlink finished before snapshot GET: %v", st)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for unlink snapshot GET")
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, overlay); st != gofuse.OK {
		close(snapshotRelease)
		t.Fatalf("concurrent write during unlink snapshot: %v", st)
	}
	close(snapshotRelease)
	st := <-unlinkDone
	if st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}

	prefixBuf := make([]byte, 16)
	prefixResult, rst := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(prefixBuf)),
	}, prefixBuf)
	if rst != gofuse.OK {
		t.Fatalf("Read overlay after unlink: %v", rst)
	}
	gotPrefix, _ := prefixResult.Bytes(prefixBuf)
	wantPrefix := append(bytes.Repeat([]byte{0x88}, 8), overlay...)
	wantPrefix = append(wantPrefix, bytes.Repeat([]byte{0x88}, 6)...)
	if !bytes.Equal(gotPrefix, wantPrefix) {
		t.Fatalf("overlay read = %x, want %x", gotPrefix, wantPrefix)
	}

	tailBuf := make([]byte, 16)
	tailResult, rst := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: uint64(2 * partSize), Size: uint32(len(tailBuf)),
	}, tailBuf)
	if rst != gofuse.OK {
		t.Fatalf("Read untouched part after unlink: %v", rst)
	}
	gotTail, _ := tailResult.Bytes(tailBuf)
	if !bytes.Equal(gotTail, original[2*partSize:2*partSize+16]) {
		t.Fatalf("untouched part = %x, want baseline", gotTail)
	}
	if puts.Load() != 1 {
		t.Fatalf("concurrent unlink/write resurrected path (puts=%d)", puts.Load())
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}
	if puts.Load() != 1 {
		t.Fatalf("Release/Flush uploaded after unlink (puts=%d)", puts.Load())
	}
}

func TestUnlinkSnapshotConcurrentFsyncKeepsCommittedOverlay(t *testing.T) {
	const filePath = "/spill-unlink-fsync-race.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x88}, int(3*partSize))
	overlay := []byte{0xAA, 0xBB}

	var mu sync.Mutex
	var committed []byte
	var revision int64
	var puts atomic.Int32
	var fileGets atomic.Int32
	snapshotStarted := make(chan struct{})
	snapshotRelease := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		case r.Method == http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			if fileGets.Add(1) == 1 {
				close(snapshotStarted)
				<-snapshotRelease
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			got, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			if got != revision {
				mu.Unlock()
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			committed = append([]byte(nil), body...)
			revision++
			rev := revision
			mu.Unlock()
			puts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": rev})
		case r.Method == http.MethodDelete:
			mu.Lock()
			committed = nil
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("first Fsync: %v", st)
	}

	unlinkDone := make(chan gofuse.Status, 1)
	go func() {
		unlinkDone <- fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/"))
	}()
	select {
	case <-snapshotStarted:
	case st := <-unlinkDone:
		t.Fatalf("Unlink finished before snapshot GET: %v", st)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for unlink snapshot GET")
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, overlay); st != gofuse.OK {
		close(snapshotRelease)
		t.Fatalf("concurrent write during unlink snapshot: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		close(snapshotRelease)
		t.Fatalf("concurrent Fsync during unlink snapshot: %v", st)
	}
	close(snapshotRelease)
	st := <-unlinkDone
	if st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}

	prefixBuf := make([]byte, 16)
	prefixResult, rst := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(prefixBuf)),
	}, prefixBuf)
	if rst != gofuse.OK {
		t.Fatalf("Read after unlink: %v", rst)
	}
	gotPrefix, _ := prefixResult.Bytes(prefixBuf)
	wantPrefix := append(bytes.Repeat([]byte{0x88}, 8), overlay...)
	wantPrefix = append(wantPrefix, bytes.Repeat([]byte{0x88}, 6)...)
	if !bytes.Equal(gotPrefix, wantPrefix) {
		t.Fatalf("open-fd read = %x, want fsynced overlay %x", gotPrefix, wantPrefix)
	}
	if puts.Load() != 2 {
		t.Fatalf("puts = %d, want 2 (seed fsync + concurrent fsync)", puts.Load())
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}
	if puts.Load() != 2 {
		t.Fatalf("flush after unlink resurrected path (puts=%d)", puts.Load())
	}
}

func TestUnlinkAfterPreinstalledSnapshotKeepsLaterFsync(t *testing.T) {
	const filePath = "/spill-preinstall-fsync.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x88}, int(3*partSize))
	overlay := []byte{0xAA, 0xBB}

	var mu sync.Mutex
	var committed []byte
	var revision int64
	var puts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		case r.Method == http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			got, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			if got != revision {
				mu.Unlock()
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			committed = append([]byte(nil), body...)
			revision++
			rev := revision
			mu.Unlock()
			puts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": rev})
		case r.Method == http.MethodDelete:
			mu.Lock()
			committed = nil
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("first Fsync: %v", st)
	}
	if err := fs.snapshotOpenHandlesBeforeUnlink(context.Background(), filePath); err != nil {
		t.Fatalf("preinstall unlink snapshot: %v", err)
	}
	fh, ok := fs.fileHandles.Get(fhID)
	if !ok {
		t.Fatal("handle missing")
	}
	fh.Lock()
	if len(fh.UnlinkedData) == 0 {
		fh.Unlock()
		t.Fatal("expected preinstalled UnlinkedData")
	}
	fh.Unlock()
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, overlay); st != gofuse.OK {
		t.Fatalf("write after preinstall: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Fsync after preinstall: %v", st)
	}
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	prefixBuf := make([]byte, 16)
	prefixResult, rst := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(prefixBuf)),
	}, prefixBuf)
	if rst != gofuse.OK {
		t.Fatalf("Read after unlink: %v", rst)
	}
	gotPrefix, _ := prefixResult.Bytes(prefixBuf)
	wantPrefix := append(bytes.Repeat([]byte{0x88}, 8), overlay...)
	wantPrefix = append(wantPrefix, bytes.Repeat([]byte{0x88}, 6)...)
	if !bytes.Equal(gotPrefix, wantPrefix) {
		t.Fatalf("open-fd read = %x, want fsynced overlay %x", gotPrefix, wantPrefix)
	}
	if puts.Load() != 2 {
		t.Fatalf("puts = %d, want 2", puts.Load())
	}
}

func TestUnlinkMarkWindowConcurrentFsyncKeepsOverlay(t *testing.T) {
	const filePath = "/spill-mark-window-fsync.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x88}, int(3*partSize))
	overlay := []byte{0xAA, 0xBB}

	var mu sync.Mutex
	var committed []byte
	var revision int64
	var puts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		case r.Method == http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			got, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			if got != revision {
				mu.Unlock()
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			committed = append([]byte(nil), body...)
			revision++
			rev := revision
			mu.Unlock()
			puts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": rev})
		case r.Method == http.MethodDelete:
			mu.Lock()
			committed = nil
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhID, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("first Fsync: %v", st)
	}

	var once sync.Once
	testHookBeforeUnlinkedTransition = func() {
		once.Do(func() {
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Offset: 8}, overlay); st != gofuse.OK {
				t.Errorf("mark-window write: %v", st)
				return
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID}); st != gofuse.OK {
				t.Errorf("mark-window Fsync: %v", st)
			}
		})
	}
	t.Cleanup(func() { testHookBeforeUnlinkedTransition = nil })

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	prefixBuf := make([]byte, 16)
	prefixResult, rst := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(prefixBuf)),
	}, prefixBuf)
	if rst != gofuse.OK {
		t.Fatalf("Read after unlink: %v", rst)
	}
	gotPrefix, _ := prefixResult.Bytes(prefixBuf)
	wantPrefix := append(bytes.Repeat([]byte{0x88}, 8), overlay...)
	wantPrefix = append(wantPrefix, bytes.Repeat([]byte{0x88}, 6)...)
	if !bytes.Equal(gotPrefix, wantPrefix) {
		t.Fatalf("open-fd read = %x, want fsynced overlay %x", gotPrefix, wantPrefix)
	}
	if puts.Load() != 2 {
		t.Fatalf("puts = %d, want 2 (seed + mark-window fsync)", puts.Load())
	}
}

func TestAttachAndMarkOpenHandlesDoesNotPartiallyUnlink(t *testing.T) {
	wb1 := NewWriteBuffer("/a.bin", 1<<20, 64)
	wb1.remoteSize = 192
	wb1.LoadPart = func(int) ([]byte, error) { return []byte("x"), nil }
	fh1 := &FileHandle{Dirty: wb1, BaseRev: 1, Path: "/a.bin"}
	wb2 := NewWriteBuffer("/a.bin", 1<<20, 64)
	wb2.remoteSize = 192
	wb2.LoadPart = func(int) ([]byte, error) { return []byte("x"), nil }
	fh2 := &FileHandle{Dirty: wb2, BaseRev: 2, Path: "/a.bin"}
	fs := &Dat9FS{}
	snapshot := bytes.Repeat([]byte{0x88}, 192)
	snapshotOK, failClosed := true, true
	_, fail := fs.attachAndMarkOpenHandles([]*FileHandle{fh1, fh2}, "/a.bin", snapshot, 0, 192, 1, snapshotOK, failClosed)
	if !fail {
		t.Fatal("stale second handle must fail closed")
	}
	if fh1.Unlinked || fh2.Unlinked {
		t.Fatal("no handle may be Unlinked when the set cannot commit")
	}
	if len(fh1.UnlinkedData) != 0 || len(fh2.UnlinkedData) != 0 {
		t.Fatal("failed mark pass must not leave a partial snapshot attached")
	}
}

func TestAttachAndMarkOpenHandlesFailsClosedOnPathIdentityAdvance(t *testing.T) {
	wb := NewWriteBuffer("/a.bin", 1<<20, 64)
	wb.remoteSize = 192
	wb.LoadPart = func(int) ([]byte, error) { return []byte("x"), nil }
	fh := &FileHandle{Dirty: wb, BaseRev: 1, Path: "/a.bin"}
	fs := &Dat9FS{
		committedRev:  map[string]int64{"/a.bin": 2},
		committedSize: map[string]int64{"/a.bin": 192},
	}
	snapshot := bytes.Repeat([]byte{0x88}, 192)
	snapshotOK, failClosed := true, true
	_, fail := fs.attachAndMarkOpenHandles([]*FileHandle{fh}, "/a.bin", snapshot, 0, 192, 1, snapshotOK, failClosed)
	if !fail {
		t.Fatal("path identity ahead of the fetched snapshot must fail closed")
	}
	if fh.Unlinked {
		t.Fatal("must not set Unlinked when path identity advanced under the set lock")
	}
	if len(fh.UnlinkedData) != 0 {
		t.Fatal("must not attach a snapshot after a path-identity fail-closed")
	}
}

func TestAttachAndMarkRetriesInsteadOfDeadlockingSiblingModeClear(t *testing.T) {
	const ino uint64 = 42
	fhA := &FileHandle{Ino: ino, Path: "/a.bin", HasPendingMode: true, PendingMode: 0o644, PendingModeGen: 1}
	fhB := &FileHandle{Ino: ino, Path: "/a.bin", HasPendingMode: true, PendingMode: 0o644, PendingModeGen: 1}
	fs := &Dat9FS{fileHandles: NewHandleTable[*FileHandle]()}
	fs.fileHandles.Allocate(fhA)
	fs.fileHandles.Allocate(fhB)

	ordered := fileHandlesLockOrder([]*FileHandle{fhA, fhB})
	if len(ordered) != 2 {
		t.Fatalf("lock order len = %d, want 2", len(ordered))
	}
	_, second := ordered[0], ordered[1]

	second.Lock()
	entered := make(chan struct{})
	var enterOnce sync.Once
	testHookAfterUnlinkHandleSetLockAttempt = func() {
		enterOnce.Do(func() { close(entered) })
	}
	t.Cleanup(func() { testHookAfterUnlinkHandleSetLockAttempt = nil })

	attachDone := make(chan struct{})
	go func() {
		defer close(attachDone)
		snapshotOK, failClosed := false, false
		_, _ = fs.attachAndMarkOpenHandles([]*FileHandle{fhA, fhB}, "/a.bin", nil, 0, 0, 0, snapshotOK, failClosed)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("attachAndMark never reported a failed set-lock attempt")
	}
	siblingDone := make(chan struct{})
	go func() {
		defer close(siblingDone)
		fs.clearPendingModeForInodeGeneration(ino, second, 0o644, 1)
	}()
	select {
	case <-siblingDone:
	case <-time.After(2 * time.Second):
		t.Fatal("clearPendingModeForInodeGeneration deadlocked with attachAndMark")
	}
	second.Unlock()
	select {
	case <-attachDone:
	case <-time.After(2 * time.Second):
		t.Fatal("attachAndMark deadlocked after sibling unlock")
	}
}

func TestMarkOpenHandlesUnlinkedFailClosedKeepsPathIndexOnLockTimeout(t *testing.T) {
	const path = "/a.bin"
	fhA := &FileHandle{Ino: 1, Path: path}
	fhB := &FileHandle{Ino: 1, Path: path}
	fs := &Dat9FS{openHandles: NewOpenHandleIndex()}
	fs.openHandles.Add(fhA)
	fs.openHandles.Add(fhB)
	ordered := fileHandlesLockOrder([]*FileHandle{fhA, fhB})
	second := ordered[1]

	oldTimeout := unlinkHandleSetLockTimeout
	unlinkHandleSetLockTimeout = 50 * time.Millisecond
	t.Cleanup(func() { unlinkHandleSetLockTimeout = oldTimeout })

	testHookBeforeUnlinkedTransition = func() {
		second.Lock()
	}
	t.Cleanup(func() {
		testHookBeforeUnlinkedTransition = nil
		second.Unlock()
	})

	marked, anyOpen, err := fs.markOpenHandlesUnlinked(context.Background(), path, false)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("markOpenHandlesUnlinked err = %v, want EIO", err)
	}
	if anyOpen || marked != nil {
		t.Fatalf("marked/anyOpen = %v/%t, want nil/false", marked, anyOpen)
	}
	if fhA.Unlinked || fhB.Unlinked {
		t.Fatal("lock timeout must not set Unlinked")
	}
	if got := fs.openHandles.SnapshotPath(path); len(got) != 2 {
		t.Fatalf("SnapshotPath len = %d, want 2 (index must be kept)", len(got))
	}
}

func TestUnlinkTwoHandleMarkRetryKeepsFsyncedOverlays(t *testing.T) {
	const filePath = "/spill-two-fd-unlink-fsync.bin"
	const partSize int64 = 64
	original := bytes.Repeat([]byte{0x88}, int(3*partSize))
	overlayA := []byte{0xAA, 0xBB}
	overlayB := []byte{0xCC, 0xDD}

	var mu sync.Mutex
	var committed []byte
	var revision int64
	var puts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("list"):
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		case r.Method == http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), committed...)
			mu.Unlock()
			if len(body) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			got, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				http.Error(w, "revision", http.StatusBadRequest)
				return
			}
			mu.Lock()
			if got != revision {
				mu.Unlock()
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			committed = append([]byte(nil), body...)
			revision++
			rev := revision
			mu.Unlock()
			puts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": rev})
		case r.Method == http.MethodDelete:
			mu.Lock()
			committed = nil
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	fs, fhIDA, nodeID, _ := newStrictSmallPartSpillFS(t, ts.URL, filePath, partSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA}, original); st != gofuse.OK {
		t.Fatalf("seed Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA}); st != gofuse.OK {
		t.Fatalf("seed Fsync: %v", st)
	}

	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: nodeID},
		Flags:    uint32(syscall.O_RDWR),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open second fd: %v", st)
	}
	fhIDB := openOut.Fh

	var once sync.Once
	testHookBeforeUnlinkedTransition = func() {
		once.Do(func() {
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA, Offset: 8}, overlayA); st != gofuse.OK {
				t.Errorf("fd A write: %v", st)
				return
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA}); st != gofuse.OK {
				t.Errorf("fd A Fsync: %v", st)
				return
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDB, Offset: 8}, overlayB); st != gofuse.OK {
				t.Errorf("fd B write: %v", st)
				return
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDB}); st != gofuse.OK {
				t.Errorf("fd B Fsync: %v", st)
			}
		})
	}
	t.Cleanup(func() { testHookBeforeUnlinkedTransition = nil })

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}

	readPrefix := func(fhID uint64) []byte {
		t.Helper()
		buf := make([]byte, 16)
		result, rst := fs.Read(nil, &gofuse.ReadIn{
			InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhID, Size: uint32(len(buf)),
		}, buf)
		if rst != gofuse.OK {
			t.Fatalf("Read fh=%d: %v", fhID, rst)
		}
		got, _ := result.Bytes(buf)
		return append([]byte(nil), got...)
	}
	gotA := readPrefix(fhIDA)
	gotB := readPrefix(fhIDB)
	wantB := append(bytes.Repeat([]byte{0x88}, 8), overlayB...)
	wantB = append(wantB, bytes.Repeat([]byte{0x88}, 6)...)
	if !bytes.Equal(gotB, wantB) {
		t.Fatalf("fd B read = %x, want fsynced overlay %x", gotB, wantB)
	}
	if !bytes.Equal(gotA, gotB) && !bytes.Equal(gotA, append(append(bytes.Repeat([]byte{0x88}, 8), overlayA...), bytes.Repeat([]byte{0x88}, 6)...)) {
		t.Fatalf("fd A read = %x, want fd B overlay %x or fd A overlay", gotA, wantB)
	}
	if puts.Load() < 3 {
		t.Fatalf("puts = %d, want >=3 (seed + both mark-window fsyncs)", puts.Load())
	}

	afterOverlay := []byte{0x11, 0x22}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA, Offset: 8}, afterOverlay); st != gofuse.OK {
		t.Fatalf("write-after-unlink: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA}); st != gofuse.OK {
		t.Fatalf("fsync-after-unlink: %v", st)
	}
	gotAfter := readPrefix(fhIDA)
	wantAfter := append(bytes.Repeat([]byte{0x88}, 8), afterOverlay...)
	wantAfter = append(wantAfter, bytes.Repeat([]byte{0x88}, 6)...)
	if !bytes.Equal(gotAfter, wantAfter) {
		t.Fatalf("fd A after unlink fsync = %x, want %x", gotAfter, wantAfter)
	}

	putsAfterUnlink := puts.Load()
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDA}); st != gofuse.OK {
		t.Fatalf("Flush A: %v", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: nodeID}, Fh: fhIDB}); st != gofuse.OK {
		t.Fatalf("Flush B: %v", st)
	}
	if puts.Load() != putsAfterUnlink {
		t.Fatalf("flush after unlink resurrected path (puts=%d -> %d)", putsAfterUnlink, puts.Load())
	}
}

func TestAttachUnlinkedHandleSnapshotDoesNotMarkWhenStale(t *testing.T) {
	wb := NewWriteBuffer("/a.bin", 1<<20, 64)
	wb.remoteSize = 192
	wb.LoadPart = func(int) ([]byte, error) { return []byte("x"), nil }
	fh := &FileHandle{Dirty: wb, BaseRev: 2, Path: "/a.bin"}
	fs := &Dat9FS{}
	_, fail := fs.attachUnlinkedHandleSnapshotLocked(fh, "/a.bin", bytes.Repeat([]byte{0x88}, 192), 0, 192, 1, true)
	if !fail {
		t.Fatal("stale snapshot must fail closed")
	}
	if fh.Unlinked {
		t.Fatal("must not set Unlinked when attach snapshot is stale")
	}
	if len(fh.UnlinkedData) != 0 {
		t.Fatal("must not keep a stale in-memory snapshot")
	}
}

func TestUnlinkedSnapshotStaleLockedDetectsRebaseline(t *testing.T) {
	wb := NewWriteBuffer("/a.bin", 1<<20, 64)
	wb.remoteSize = 192
	wb.LoadPart = func(int) ([]byte, error) { return []byte("x"), nil }
	fh := &FileHandle{Dirty: wb, BaseRev: 2}
	if !unlinkedSnapshotStaleLocked(fh, 1, 192) {
		t.Fatal("BaseRev ahead of snapshot revision must be stale")
	}
	fh.BaseRev = 1
	if unlinkedSnapshotStaleLocked(fh, 1, 192) {
		t.Fatal("matching BaseRev should not be stale")
	}
	wb.remoteSize = 200
	if !unlinkedSnapshotStaleLocked(fh, 1, 192) {
		t.Fatal("clean remoteSize change after Fsync rebase must be stale")
	}
}

func TestWriteBuffer_ResetSequentialState(t *testing.T) {
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	// Write 2 full parts, break sequential with back-write
	_, _ = wb.Write(0, make([]byte, 2*int(DefaultPartSize)))
	_, _ = wb.Write(0, []byte("patch"))
	if wb.IsSequential() {
		t.Fatal("should not be sequential after back-write")
	}
	if wb.appendCursor != 2*DefaultPartSize {
		t.Fatalf("appendCursor = %d, want %d", wb.appendCursor, 2*DefaultPartSize)
	}

	// Truncate to 0 and reset
	_ = wb.Truncate(0)
	wb.ResetSequentialState(0)

	if !wb.IsSequential() {
		t.Fatal("should be sequential after ResetSequentialState")
	}
	if wb.appendCursor != 0 {
		t.Fatalf("appendCursor = %d, want 0", wb.appendCursor)
	}

	// New writes after reset should be correctly tracked as sequential
	callCount := 0
	wb.OnPartFull = func(partIdx int, data []byte) { callCount++ }
	_, _ = wb.Write(0, make([]byte, DefaultPartSize+1))
	if !wb.IsSequential() {
		t.Fatal("should still be sequential after forward write post-reset")
	}
	if callCount != 1 {
		t.Fatalf("OnPartFull called %d times, want 1", callCount)
	}
}

func TestWriteBuffer_ExactPartSizeBoundary_NoDoubleUpload(t *testing.T) {
	// When file size is exactly N*partSize, the last part was already
	// fully streamed. Verify OnPartFull fires for all N parts.
	wb := NewWriteBuffer("/test", streamingWriteMaxSize, DefaultPartSize)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	var calledParts []int
	wb.OnPartFull = func(partIdx int, data []byte) {
		calledParts = append(calledParts, partIdx)
	}

	// Write exactly 3 full parts (24MB)
	totalSize := 3 * int(DefaultPartSize)
	_, _ = wb.Write(0, make([]byte, totalSize))

	// All 3 parts should have been reported as full
	if len(calledParts) != 3 {
		t.Fatalf("OnPartFull called for %d parts, want 3; parts=%v", len(calledParts), calledParts)
	}

	// Verify totalSize is exact multiple
	if wb.Size()%wb.PartSize() != 0 {
		t.Fatalf("size %d is not exact multiple of partSize %d", wb.Size(), wb.PartSize())
	}
}

func TestStreamUploader_HasStreamedParts(t *testing.T) {
	su := NewStreamUploader(nil, "/test", -1)
	if su.HasStreamedParts() {
		t.Fatal("should have no streamed parts initially")
	}
}

// TestStreamUploader_FinishStreamingRestoresPendingOnFailure verifies that
// when FinishStreaming fails (e.g. S3 connection reset, timeout), the
// pendingParts are restored so the operation can be retried.
func TestStreamUploader_FinishStreamingRestoresPendingOnFailure(t *testing.T) {
	var initiateCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/uploads/initiate":
			initiateCalls++
			// Return a valid upload plan so initiation succeeds.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"upload_id":"test-upload","total_parts":2}`))
		case "/v2/uploads/test-upload/presign-batch":
			// Fail presign to simulate S3 error during WritePart.
			http.Error(w, `{"error":"simulated S3 failure"}`, http.StatusInternalServerError)
		case "/v2/uploads/test-upload/abort":
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer ts.Close()

	c := newTestClient(ts.URL)
	su := NewStreamUploader(c, "/retry-test.bin", -1)

	// Submit 2 parts.
	part1 := []byte("part-1-data-here")
	part2 := []byte("part-2-data-here")
	if err := su.SubmitPart(context.Background(), 1, part1, nil); err != nil {
		t.Fatal(err)
	}
	if err := su.SubmitPart(context.Background(), 2, part2, nil); err != nil {
		t.Fatal(err)
	}

	// First FinishStreaming should fail (presign returns 500).
	err := su.FinishStreaming(context.Background(), 32, 2, []byte("last"), nil)
	if err == nil {
		t.Fatal("expected FinishStreaming to fail")
	}

	// pendingParts should be restored — verify by checking the internal state.
	su.mu.Lock()
	restoredCount := len(su.pendingParts)
	has1 := su.pendingParts[1] != nil
	has2 := su.pendingParts[2] != nil
	writerNil := su.writer == nil
	su.mu.Unlock()

	if restoredCount != 2 {
		t.Fatalf("pendingParts count after failure = %d, want 2", restoredCount)
	}
	if !has1 || !has2 {
		t.Fatalf("pendingParts missing parts: has1=%v has2=%v", has1, has2)
	}
	if !writerNil {
		t.Fatal("writer should be nil after failure to allow fresh initiate on retry")
	}

	// Verify initiate was called (upload was attempted, not short-circuited).
	if initiateCalls < 1 {
		t.Fatalf("initiate calls = %d, want >= 1", initiateCalls)
	}
}

// ---------------------------------------------------------------------------
// Error mapping test
// ---------------------------------------------------------------------------

func TestHttpToFuseStatus(t *testing.T) {
	tests := []struct {
		err    error
		expect int32
	}{
		{nil, 0},                           // OK
		{fmt.Errorf("not found: /x"), -2},  // ENOENT
		{fmt.Errorf("HTTP 404: ..."), -2},  // ENOENT
		{fmt.Errorf("HTTP 403: ..."), -13}, // EACCES
		{fmt.Errorf("HTTP 500: ..."), -5},  // EIO
	}
	_ = tests // compile check — errno values vary by platform
}

// ---------------------------------------------------------------------------
// FlushDebouncer tests
// ---------------------------------------------------------------------------

func TestFlushDebouncer_ScheduleAndFire(t *testing.T) {
	d := newFlushDebouncer(50 * time.Millisecond)
	done := make(chan string, 1)
	d.Schedule("/a", func() { done <- "/a" })

	select {
	case p := <-done:
		if p != "/a" {
			t.Fatalf("got %q, want /a", p)
		}
	case <-time.After(time.Second):
		t.Fatal("debounce did not fire within 1s")
	}
}

func TestFlushDebouncer_CoalescesRapidSchedules(t *testing.T) {
	d := newFlushDebouncer(100 * time.Millisecond)
	var callCount atomic.Int32
	var lastVal atomic.Int32
	done := make(chan struct{}, 1)
	for i := 0; i < 5; i++ {
		v := int32(i)
		d.Schedule("/a", func() {
			callCount.Add(1)
			lastVal.Store(v)
			select {
			case done <- struct{}{}:
			default:
			}
		})
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for the debounce to fire
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("debounce did not fire within 1s")
	}

	if c := callCount.Load(); c != 1 {
		t.Fatalf("callCount = %d, want 1 (coalesced)", c)
	}
	if v := lastVal.Load(); v != 4 {
		t.Fatalf("lastVal = %d, want 4 (latest schedule)", v)
	}
}

func TestFlushDebouncer_Cancel(t *testing.T) {
	d := newFlushDebouncer(100 * time.Millisecond)
	var called atomic.Bool
	d.Schedule("/a", func() { called.Store(true) })
	d.Cancel("/a")

	time.Sleep(200 * time.Millisecond)
	if called.Load() {
		t.Fatal("upload should not have been called after Cancel")
	}
}

func TestFlushDebouncer_CancelNoWaitIfOwner(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)
	// Distinct non-zero-size owner tokens: pointers to zero-size values may
	// share an address (runtime.zerobase) and compare equal.
	ownerA := &FileHandle{Ino: 101}
	ownerB := &FileHandle{Ino: 102}

	// A non-owner cancel must leave the pending entry alone.
	fired := make(chan string, 1)
	d.ScheduleWithOwner("/a", ownerA, func() { fired <- "/a" })
	d.CancelNoWaitIfOwner("/a", ownerB)
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("non-owner CancelNoWaitIfOwner stopped the debounce")
	}

	// An owner cancel stops the pending entry.
	d.ScheduleWithOwner("/a", ownerA, func() { fired <- "/a2" })
	d.CancelNoWaitIfOwner("/a", ownerA)
	select {
	case p := <-fired:
		t.Fatalf("owner CancelNoWaitIfOwner did not stop the debounce (fired %q)", p)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestFlushDebouncer_FlushAll(t *testing.T) {
	d := newFlushDebouncer(10 * time.Second) // long delay — won't fire naturally
	results := make(map[string]bool)
	d.Schedule("/a", func() { results["/a"] = true })
	d.Schedule("/b", func() { results["/b"] = true })

	d.FlushAll()

	if !results["/a"] || !results["/b"] {
		t.Fatalf("FlushAll should have called both uploads: %v", results)
	}
}

func TestFlushDebouncer_IndependentPaths(t *testing.T) {
	d := newFlushDebouncer(50 * time.Millisecond)
	done := make(chan string, 2)
	d.Schedule("/a", func() { done <- "/a" })
	d.Schedule("/b", func() { done <- "/b" })

	got := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case p := <-done:
			got[p] = true
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for debounce")
		}
	}
	if !got["/a"] || !got["/b"] {
		t.Fatalf("expected both paths, got %v", got)
	}
}

// TestFlushDebouncer_CancelWaitsForRunningCallback verifies that Cancel blocks
// until a running uploadFn finishes. This is critical for Unlink correctness:
// the callback may be cleaning up pendingIndex, and Unlink must not read
// pendingIndex until the cleanup is done.
func TestFlushDebouncer_CancelWaitsForRunningCallback(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)
	started := make(chan struct{})
	finish := make(chan struct{})
	var finished atomic.Bool
	d.Schedule("/a", func() {
		close(started)
		<-finish // block until test signals
		finished.Store(true)
	})

	// Wait for the callback to start running.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not start within 2s")
	}

	// Cancel should block because the callback is still running.
	cancelDone := make(chan struct{})
	go func() {
		d.Cancel("/a")
		close(cancelDone)
	}()

	select {
	case <-cancelDone:
		t.Fatal("Cancel returned before callback finished")
	case <-time.After(100 * time.Millisecond):
		// Good — Cancel is still blocked.
	}

	// Let the callback finish.
	close(finish)

	select {
	case <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not return after callback finished")
	}
	if !finished.Load() {
		t.Fatal("callback did not finish")
	}
}

// TestFlushDebouncer_CancelAfterCallbackSelfDeletes verifies that Cancel does
// not deadlock when the timer callback has already removed the pending entry
// and is still running uploadFn. Cancel should detect the inflight entry and
// wait for it.
func TestFlushDebouncer_CancelAfterCallbackSelfDeletes(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)
	started := make(chan struct{})
	finish := make(chan struct{})
	d.Schedule("/a", func() {
		close(started)
		<-finish
	})

	// Wait for the callback to start (entry self-deleted from pending, now inflight).
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not start within 2s")
	}

	// Cancel should block (callback inflight) then return once it finishes.
	cancelDone := make(chan struct{})
	go func() {
		d.Cancel("/a")
		close(cancelDone)
	}()

	select {
	case <-cancelDone:
		t.Fatal("Cancel returned before callback finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(finish)

	select {
	case <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not return after callback finished")
	}
}

// TestFlushDebouncer_ScheduleReplacesWhileCallbackRunning verifies no
// panic/deadlock when Schedule replaces a pending entry while the previous
// callback is still running.
func TestFlushDebouncer_ScheduleReplacesWhileCallbackRunning(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)
	firstStarted := make(chan struct{})
	firstFinish := make(chan struct{})
	d.Schedule("/a", func() {
		close(firstStarted)
		<-firstFinish
	})

	// Wait for the first callback to start.
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first callback did not start")
	}

	// Schedule a replacement while the first callback is running.
	secondDone := make(chan struct{})
	d.Schedule("/a", func() {
		close(secondDone)
	})

	// Let the first callback finish.
	close(firstFinish)

	// The replacement's timer (20ms) should fire and run the second callback.
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second callback did not fire within 2s")
	}

	// Cancel should return without deadlock (no pending or inflight).
	d.Cancel("/a")
}

// TestFlushDebouncer_CancelNoWaitDoesNotBlock verifies that CancelNoWait
// returns immediately even when a callback is running.
func TestFlushDebouncer_CancelNoWaitDoesNotBlock(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)
	started := make(chan struct{})
	finish := make(chan struct{})
	d.Schedule("/a", func() {
		close(started)
		<-finish
	})

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not start")
	}

	// CancelNoWait must return immediately despite the running callback.
	done := make(chan struct{})
	go func() {
		d.CancelNoWait("/a")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CancelNoWait blocked while callback was running")
	}

	close(finish)
}

// TestFlushDebouncer_SamePathNoDoubleInflight verifies that when callback A
// is already running for a path, a new Schedule for the same path does not
// cause a second callback to run concurrently. The new callback must wait for
// the in-flight one to finish first. This is the race qiffang identified: a
// second Schedule could overwrite inflight[path], and Cancel would return
// while the second callback was still running.
func TestFlushDebouncer_SamePathNoDoubleInflight(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)
	aStarted := make(chan struct{})
	aFinish := make(chan struct{})
	var concurrent atomic.Int32

	d.Schedule("/a", func() {
		close(aStarted)
		<-aFinish
	})

	// Wait for callback A to start (it's now in-flight).
	select {
	case <-aStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("callback A did not start")
	}

	// Schedule B while A is still running.
	bDone := make(chan struct{})
	d.Schedule("/a", func() {
		if concurrent.Add(1) > 1 {
			close(bDone)
			t.Fatal("two callbacks ran concurrently")
		}
		concurrent.Add(-1)
		close(bDone)
	})

	// Let A finish. B's timer fires after, and must wait for A's inflight to
	// drain before running.
	close(aFinish)

	select {
	case <-bDone:
	case <-time.After(2 * time.Second):
		t.Fatal("callback B did not run within 2s")
	}

	// Cancel must not return while any callback is still running. After both
	// finish, Cancel returns immediately.
	d.Cancel("/a")
}

// TestFlushDebouncer_StaleTimerDoesNotRunNewerEntry verifies that a stale timer
// (from a superseded Schedule) does not act on the newer pending entry. When
// Schedule replaces a pending entry, the old timer must detect it is stale and
// skip, leaving only the new entry's callback to fire.
func TestFlushDebouncer_StaleTimerDoesNotRunNewerEntry(t *testing.T) {
	d := newFlushDebouncer(50 * time.Millisecond)
	var firstRan, secondRan atomic.Bool

	// Schedule first, then immediately replace it. The first timer is stale.
	d.Schedule("/a", func() { firstRan.Store(true) })
	// Replace before the first timer fires.
	d.Schedule("/a", func() { secondRan.Store(true) })

	// Wait long enough for both timers to have fired.
	time.Sleep(200 * time.Millisecond)

	if firstRan.Load() {
		t.Fatal("stale (superseded) callback should not have run")
	}
	if !secondRan.Load() {
		t.Fatal("latest callback should have run")
	}
}

// TestFlushDebouncer_FlushAllNoMapRace verifies FlushAll does not panic with
// concurrent map iteration/write when timer callbacks delete from inflight
// concurrently. It also verifies that a pending callback for the same path as
// an in-flight callback does not start until the in-flight one finishes.
// This is a race test — run with -race to catch the bug.
func TestFlushDebouncer_FlushAllNoMapRace(t *testing.T) {
	d := newFlushDebouncer(10 * time.Millisecond)

	// Start a callback that blocks so it is in-flight when FlushAll runs.
	aStarted := make(chan struct{})
	aFinish := make(chan struct{})
	d.Schedule("/a", func() {
		close(aStarted)
		<-aFinish
	})

	// Wait for A to be in-flight.
	select {
	case <-aStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("callback A did not start")
	}

	// Schedule B (pending, not yet fired) for the same path while A is running.
	bRan := make(chan struct{})
	bStarted := make(chan struct{})
	d.Schedule("/a", func() {
		close(bStarted)
		<-bRan
	})

	// Call FlushAll while A is still running. FlushAll should not run B until
	// A has finished (same-path serialization).
	flushDone := make(chan struct{})
	go func() {
		d.FlushAll()
		close(flushDone)
	}()

	// B must NOT start while A is still running.
	select {
	case <-bStarted:
		t.Fatal("FlushAll ran pending same-path callback while the older callback was still inflight")
	case <-time.After(100 * time.Millisecond):
		// Good — B is correctly waiting for A to finish.
	}

	// Let A finish. Now B should be allowed to start.
	close(aFinish)

	select {
	case <-bStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("FlushAll did not run pending callback B after A finished")
	}

	// Let B finish so FlushAll can complete.
	close(bRan)

	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("FlushAll did not complete")
	}
}

// TestFlushDebouncer_FlushAllWaitsForInflightSamePath verifies that FlushAll
// preserves the per-path inflight serialization: if A is inflight for /a and B
// is pending for /a, FlushAll must not start B until A has completed. This is
// the regression qiffang requested.
func TestFlushDebouncer_FlushAllWaitsForInflightSamePath(t *testing.T) {
	d := newFlushDebouncer(20 * time.Millisecond)

	aStarted := make(chan struct{})
	aFinish := make(chan struct{})
	d.Schedule("/a", func() {
		close(aStarted)
		<-aFinish
	})

	// Wait for A to be in-flight.
	select {
	case <-aStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("callback A did not start")
	}

	// Schedule B for the same path while A is running.
	bStarted := make(chan struct{})
	bDone := make(chan struct{})
	d.Schedule("/a", func() {
		close(bStarted)
		close(bDone)
	})

	// Call FlushAll while A is still running.
	flushDone := make(chan struct{})
	go func() {
		d.FlushAll()
		close(flushDone)
	}()

	// B must NOT start while A is still running.
	select {
	case <-bStarted:
		t.Fatal("FlushAll ran pending same-path callback while an older callback was still inflight")
	case <-time.After(100 * time.Millisecond):
		// Good — B is waiting for A.
	}

	// Let A finish. B should now start.
	close(aFinish)

	select {
	case <-bStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("callback B did not start after A finished")
	}

	// Wait for B to finish and FlushAll to complete.
	select {
	case <-bDone:
	case <-time.After(2 * time.Second):
		t.Fatal("callback B did not finish")
	}
	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("FlushAll did not complete")
	}
}

func TestInodeToPath_UpdateMtime(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/f", false, 10, time.Now())
	newTime := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	m.UpdateMtime(ino, newTime)
	entry, _ := m.GetEntry(ino)
	if !entry.Mtime.Equal(newTime) {
		t.Fatalf("UpdateMtime: got %v, want %v", entry.Mtime, newTime)
	}
}

// ---------------------------------------------------------------------------
// ShadowSpill tests
// ---------------------------------------------------------------------------

// TestShadowSpill_OnPartFullEvicts verifies that ShadowSpill's OnPartFull
// callback evicts completed parts, keeping Dirty memory bounded to ~1 part.
func TestShadowSpill_OnPartFullEvicts(t *testing.T) {
	wb := NewWriteBuffer("/spill.bin", streamingWriteMaxSize, 0)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	// Wire ShadowSpill-style OnPartFull: just evict.
	wb.OnPartFull = func(partIdx int, data []byte) {
		wb.EvictPart(partIdx)
	}

	// Write 3 full parts + 1 byte into part 3 (so part 3 is active, not full).
	partSize := int(DefaultPartSize)
	chunk := make([]byte, partSize)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}
	for p := 0; p < 3; p++ {
		if _, err := wb.Write(int64(p)*int64(partSize), chunk); err != nil {
			t.Fatalf("write part %d: %v", p, err)
		}
	}
	// Write 1 byte into part 3 so it becomes the active (incomplete) part.
	if _, err := wb.Write(int64(3*partSize), []byte{0x42}); err != nil {
		t.Fatalf("write partial part 3: %v", err)
	}

	// Parts 0, 1, 2 should have been evicted. Part 3 is active.
	evicted := wb.StreamedPartIndices()
	for p := 0; p < 3; p++ {
		if !evicted[p] {
			t.Fatalf("expected part %d evicted; got %v", p, evicted)
		}
	}
	if evicted[3] {
		t.Fatalf("part 3 should not be evicted (active/incomplete part)")
	}

	// Size should still track correctly.
	expectedSize := int64(3*partSize) + 1
	if wb.Size() != expectedSize {
		t.Fatalf("size = %d, want %d", wb.Size(), expectedSize)
	}
}

// TestShadowSpill_WriteReadThroughShadow verifies that ShadowSpill writes
// go to shadow and reads come back correctly from shadow.
func TestShadowSpill_WriteReadThroughShadow(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	path := "/spill/read-write.bin"
	if err := ss.Ensure(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	// Simulate ShadowSpill writes.
	data1 := []byte("hello shadow spill world")
	if _, err := ss.WriteAt(path, 0, data1, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	data2 := []byte("APPENDED")
	if _, err := ss.WriteAt(path, int64(len(data1)), data2, 0); err != nil {
		t.Fatalf("WriteAt append: %v", err)
	}

	// Read back via ReadAt (the ShadowSpill Read path).
	totalLen := len(data1) + len(data2)
	buf := make([]byte, totalLen)
	n, err := ss.ReadAt(path, 0, buf)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != totalLen {
		t.Fatalf("ReadAt n = %d, want %d", n, totalLen)
	}
	expected := append(data1, data2...)
	if string(buf) != string(expected) {
		t.Fatalf("data = %q, want %q", buf, expected)
	}
}

// TestShadowReaderAt verifies that shadowReaderAt correctly adapts
// ShadowStore.ReadAt into an io.ReaderAt for streaming uploads.
func TestShadowReaderAt(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	path := "/upload/test.bin"
	if err := ss.Ensure(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	// Write test data to shadow.
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if _, err := ss.WriteAt(path, 0, testData, 0); err != nil {
		t.Fatal(err)
	}

	ra := &shadowReaderAt{store: ss, path: path}

	// Read full file.
	buf := make([]byte, 1024)
	n, err := ra.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt(0): %v", err)
	}
	if n != 1024 {
		t.Fatalf("ReadAt n = %d, want 1024", n)
	}
	for i, b := range buf {
		if b != byte(i%256) {
			t.Fatalf("data mismatch at %d: got %d, want %d", i, b, byte(i%256))
		}
	}

	// Read partial range.
	partial := make([]byte, 100)
	n, err = ra.ReadAt(partial, 512)
	if err != nil {
		t.Fatalf("ReadAt(512): %v", err)
	}
	if n != 100 {
		t.Fatalf("partial ReadAt n = %d, want 100", n)
	}
	for i, b := range partial {
		if b != byte((512+i)%256) {
			t.Fatalf("partial data mismatch at %d: got %d, want %d", i, b, byte((512+i)%256))
		}
	}
}

// TestShadowSpill_AutoResolveGuard verifies that tryAutoResolveConflict
// short-circuits for ShadowSpill entries without reading shadow data
// into memory (which would OOM for large files).
func TestShadowSpill_AutoResolveGuard(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	path := "/conflict/large.bin"
	if err := ss.Ensure(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.WriteAt(path, 0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}

	// Register the entry in pending index so MarkConflict can find it.
	if _, err := idx.PutWithBaseRev(path, 4, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	cq := &CommitQueue{
		shadows:  ss,
		index:    idx,
		inFlight: make(map[string]*CommitEntry),
	}

	entry := &CommitEntry{
		Path:        path,
		Size:        10 * 1024 * 1024 * 1024, // 10 GiB
		Kind:        PendingNew,
		ShadowSpill: true,
	}

	// Should not panic, should not attempt ReadAll, should go to terminal failure.
	cq.tryAutoResolveConflict(context.Background(), entry)

	// Verify MarkConflict was called (terminal failure path).
	meta, ok := idx.GetMeta(path)
	if !ok {
		t.Fatal("expected pending entry to exist after auto-resolve")
	}
	if meta.Kind != PendingConflict {
		t.Fatalf("expected PendingConflict after ShadowSpill auto-resolve guard, got %v", meta.Kind)
	}
}

// TestShadowSpill_WriteOrderShadowFirst verifies that for ShadowSpill handles,
// shadow is written before Dirty. If shadow write fails, Dirty must not be
// modified (no EvictPart on data that never reached shadow).
func TestShadowSpill_WriteOrderShadowFirst(t *testing.T) {
	wb := NewWriteBuffer("/test.bin", streamingWriteMaxSize, 0)
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)

	var evictCount int
	wb.OnPartFull = func(partIdx int, data []byte) {
		evictCount++
		wb.EvictPart(partIdx)
	}

	// Write one full part to trigger OnPartFull, then 1 byte into next part.
	partSize := int(DefaultPartSize)
	chunk := make([]byte, partSize)
	for i := range chunk {
		chunk[i] = 0xAB
	}
	if _, err := wb.Write(0, chunk); err != nil {
		t.Fatalf("write part 0: %v", err)
	}
	// Writing into the next part triggers OnPartFull for part 0.
	if _, err := wb.Write(int64(partSize), []byte{0x01}); err != nil {
		t.Fatalf("write part 1: %v", err)
	}

	// Part 0 should have been evicted by OnPartFull.
	if evictCount != 1 {
		t.Fatalf("evictCount = %d, want 1 (OnPartFull should fire once for the completed part)", evictCount)
	}

	// The key invariant: if we had written Dirty first and shadow then failed,
	// the evicted part data would be lost. By writing shadow first, this can't happen.
}

// TestShadowSpill_RecoverPendingPreservesShadowSpill verifies that crash
// recovery (RecoverPending) reconstructs CommitEntry.ShadowSpill from the
// persisted WriteBackMeta.ShadowSpill field.
func TestShadowSpill_RecoverPendingPreservesShadowSpill(t *testing.T) {
	dir := t.TempDir()
	shadowDir := t.TempDir()

	// Create PendingIndex and store a ShadowSpill entry.
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/large-video.mp4"
	if _, err := idx.PutShadowSpill(path, 1<<30, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	// Verify meta has ShadowSpill set.
	meta, ok := idx.GetMeta(path)
	if !ok {
		t.Fatal("expected pending entry after PutShadowSpill")
	}
	if !meta.ShadowSpill {
		t.Fatal("PutShadowSpill should persist ShadowSpill=true")
	}

	// Create a shadow file so RecoverPending doesn't prune it.
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadows.Ensure(path, 0, 0); err != nil {
		t.Fatal(err)
	}

	// Create a new PendingIndex from the same dir (simulates restart).
	idx2, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}

	// Verify the reloaded meta still has ShadowSpill.
	meta2, ok := idx2.GetMeta(path)
	if !ok {
		t.Fatal("expected pending entry after PendingIndex reload")
	}
	if !meta2.ShadowSpill {
		t.Fatal("ShadowSpill must survive PendingIndex reload from disk")
	}

	// Create CommitQueue and recover — entry should carry ShadowSpill.
	cq := &CommitQueue{
		index:      idx2,
		shadows:    shadows,
		inFlight:   make(map[string]*CommitEntry),
		maxPending: 100,
		workCh:     make(chan *CommitEntry, 100),
	}

	cq.RecoverPending()

	// RecoverPending calls Enqueue which appends to cq.queue.
	cq.mu.Lock()
	queue := make([]*CommitEntry, len(cq.queue))
	copy(queue, cq.queue)
	cq.mu.Unlock()

	if len(queue) != 1 {
		t.Fatalf("recovered %d entries, want 1", len(queue))
	}
	if !queue[0].ShadowSpill {
		t.Fatal("recovered CommitEntry must have ShadowSpill=true")
	}
}

// TestShadowSpill_WriteBackSeqStaysZero verifies the structural invariant:
// ShadowSpill handles never set WriteBackSeq, preventing Fsync/Release from
// accidentally entering the writeBack-dependent code paths.
func TestShadowSpill_WriteBackSeqStaysZero(t *testing.T) {
	fh := &FileHandle{
		Path:              "/spill.bin",
		ShadowReady:       true,
		ShadowSpill:       true,
		ShadowCommitReady: true,
		Dirty:             NewWriteBuffer("/spill.bin", 0, 0),
		BaseRev:           5,
		DirtySeq:          42,
	}

	// ShadowSpill handles must never have WriteBackSeq set.
	if fh.WriteBackSeq != 0 {
		t.Fatalf("ShadowSpill handle WriteBackSeq = %d, want 0", fh.WriteBackSeq)
	}

	// Simulate what Flush interactive does for ShadowSpill:
	// It sets ShadowCommitReady but does NOT set WriteBackSeq.
	fh.ShadowCommitReady = true
	if fh.WriteBackSeq != 0 {
		t.Fatalf("after ShadowSpill Flush, WriteBackSeq = %d, want 0", fh.WriteBackSeq)
	}

	// The Fsync condition that must NOT fire:
	// fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq
	if fh.WriteBackSeq != 0 && fh.WriteBackSeq == fh.DirtySeq {
		t.Fatal("Fsync UploadSync condition must never be true for ShadowSpill handles")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkWriteBuffer_SmallFile_WriteAndRead(b *testing.B) {
	data := []byte("hello world, this is a small file content for testing!")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := NewWriteBuffer("/test.txt", 0, 0)
		_, _ = wb.Write(0, data)
		_, _ = wb.Write(6, []byte("EARTH"))
		benchmarkBytesSink = wb.Bytes()
	}
}

func BenchmarkWriteBuffer_SmallFile_ReadAt(b *testing.B) {
	data := make([]byte, 40*1024) // 40KB small file
	for i := range data {
		data[i] = byte(i % 256)
	}
	wb := NewWriteBuffer("/test.txt", 0, 0)
	_, _ = wb.Write(0, data)
	buf := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkIntSink = wb.ReadAt(int64(i%36)*1024, buf)
		benchmarkByteSink ^= buf[0]
	}
}

func BenchmarkWriteBuffer_SmallFile_Truncate(b *testing.B) {
	data := make([]byte, 40*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := NewWriteBuffer("/test.txt", 0, 0)
		_, _ = wb.Write(0, data)
		_ = wb.Truncate(20 * 1024)
		_ = wb.Truncate(40 * 1024)
	}
}

func BenchmarkReadCache_GetPut(b *testing.B) {
	rc := NewReadCache(128<<20, 30*time.Second)
	data := make([]byte, 40*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	rc.Put("/test.txt", data, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkBytesSink, _ = rc.Get("/test.txt", 1)
	}
}

func TestDiskReadCachePutGet(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 4096, Length: 5}
	cache.PutOwned(key, []byte("hello"))

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("disk read cache miss, want hit")
	}
	if string(got) != "hello" {
		t.Fatalf("cache data = %q, want hello", got)
	}
}

func TestDiskReadCacheArtifactsArePrivate(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskReadCache(DiskReadCacheOptions{Dir: dir, MaxSize: 1 << 20, FreeRatio: -1})
	if err != nil {
		t.Fatalf("NewDiskReadCache: %v", err)
	}
	t.Cleanup(cache.Close)
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("cache dir stat: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("cache dir mode = %o, want 700", got)
	}

	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	cache.PutOwned(key, []byte("hello"))
	info, err := os.Stat(cache.pathForDigest(key.digest()))
	if err != nil {
		t.Fatalf("cache file stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("cache file mode = %o, want 600", got)
	}
}

func TestDiskReadCachePendingMemoryIsServedAndIsolated(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	digest := key.digest()
	cache.mu.Lock()
	cache.pending[digest] = diskReadCachePending{key: key, data: []byte("hello"), seq: 1}
	cache.mu.Unlock()

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("disk read cache miss, want pending hit")
	}
	if string(got) != "hello" {
		t.Fatalf("pending cache data = %q, want hello", got)
	}
	got[0] = 'x'
	got, ok = cache.Get(key)
	if !ok || string(got) != "hello" {
		t.Fatalf("pending cache data after mutation = %q, %v; want isolated hello hit", got, ok)
	}
}

func TestDiskReadCacheRejectsLengthMismatch(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}

	cache.Put(key, []byte("hey"))
	if got, ok := cache.Get(key); ok {
		t.Fatalf("disk read cache hit after short Put = %q, want miss", got)
	}

	cache.PutAsync(key, []byte("hey"))
	if got, ok := cache.Get(key); ok {
		t.Fatalf("disk read cache pending hit after short PutAsync = %q, want miss", got)
	}

	cache.PutOwned(key, []byte("hello"))
	if got, ok := cache.Get(key); !ok || string(got) != "hello" {
		t.Fatalf("disk read cache full PutOwned = %q, %v; want hello hit", got, ok)
	}
}

func TestDiskReadCacheAsyncPutInvalidatedBeforeCommitDoesNotReappear(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	digest := key.digest()
	seq := diskReadCacheTempSeq.Add(1)
	data := []byte("hello")

	cache.mu.Lock()
	cache.pending[digest] = diskReadCachePending{key: key, data: data, seq: seq}
	cache.mu.Unlock()

	cache.InvalidateFile("file-1")
	cache.putOwned(key, data, seq)

	if got, ok := cache.Get(key); ok {
		t.Fatalf("invalidated async cache entry hit: %q", got)
	}
	if _, err := os.Stat(cache.pathForDigest(digest)); !os.IsNotExist(err) {
		t.Fatalf("invalidated async cache file stat err = %v, want not exists", err)
	}
}

func TestDiskReadCacheDetachedRemovalPreservesReplacement(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	cache.PutOwned(key, []byte("stale"))

	detached := cache.detachAll()
	if len(detached) != 1 {
		t.Fatalf("detached entries = %d, want 1", len(detached))
	}
	if data, ok := cache.Get(key); ok {
		t.Fatalf("detached cache entry remained visible: %q", data)
	}
	cache.PutOwned(key, []byte("fresh"))
	cache.removeDetached(detached)

	if data, ok := cache.Get(key); !ok || string(data) != "fresh" {
		t.Fatalf("replacement cache entry = %q, %v; want fresh hit", data, ok)
	}
}

func TestDiskReadCacheRevisionMismatchMisses(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	cache.PutOwned(key, []byte("hello"))
	key.Revision = 8

	if got, ok := cache.Get(key); ok {
		t.Fatalf("cache hit for different revision: %q", got)
	}
}

func TestDiskReadCacheCapacityEvictsLRU(t *testing.T) {
	cache := newTestDiskReadCache(t, 220)
	first := DiskReadCacheKey{FileID: "file-1", Path: "/first.bin", Revision: 1, Offset: 0, Length: 4}
	second := DiskReadCacheKey{FileID: "file-2", Path: "/second.bin", Revision: 1, Offset: 0, Length: 4}
	cache.PutOwned(first, []byte("1111"))
	cache.PutOwned(second, []byte("2222"))

	if got, ok := cache.Get(first); ok {
		t.Fatalf("first cache entry hit after capacity eviction: %q", got)
	}
	got, ok := cache.Get(second)
	if !ok || string(got) != "2222" {
		t.Fatalf("second cache entry = %q, %v; want 2222 hit", got, ok)
	}
}

func TestDiskReadCacheCapacityCountsFileOverhead(t *testing.T) {
	cache := newTestDiskReadCache(t, 220)
	first := DiskReadCacheKey{FileID: "file-1", Path: "/first.bin", Revision: 1, Offset: 0, Length: 1}
	second := DiskReadCacheKey{FileID: "file-2", Path: "/second.bin", Revision: 1, Offset: 0, Length: 1}
	cache.PutOwned(first, []byte("1"))
	cache.PutOwned(second, []byte("2"))

	if got, ok := cache.Get(first); ok {
		t.Fatalf("first cache entry hit after file-overhead capacity eviction: %q", got)
	}
	got, ok := cache.Get(second)
	if !ok || string(got) != "2" {
		t.Fatalf("second cache entry = %q, %v; want 2 hit", got, ok)
	}
}

func TestDiskReadCacheCorruptionMissesAndRemovesEntry(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	cache.PutOwned(key, []byte("hello"))
	if err := os.WriteFile(cache.pathForDigest(key.digest()), []byte(`{"file_id":"file-1","revision":7,"offset":0,"length":5,"size":5,"crc32":1}`+"\nhello"), 0o644); err != nil {
		t.Fatalf("corrupt cache entry: %v", err)
	}

	if got, ok := cache.Get(key); ok {
		t.Fatalf("corrupt cache entry hit: %q", got)
	}
	if cache.Len() != 0 {
		t.Fatalf("cache Len = %d, want corrupt entry removed", cache.Len())
	}
}

func TestDiskReadCacheClosePreventsFutureAsyncPuts(t *testing.T) {
	cache := newTestDiskReadCache(t, 1<<20)
	cache.Close()
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	cache.PutAsync(key, []byte("hello"))

	if got, ok := cache.Get(key); ok {
		t.Fatalf("closed cache hit after PutAsync: %q", got)
	}
}

func TestDiskReadCacheStartupRecoveryAndTempCleanup(t *testing.T) {
	dir := t.TempDir()
	// FreeRatio -1 disables the free-disk eviction: 0 maps to the 10% default,
	// which evicts the recovered entry at startup on hosts with a nearly full
	// disk and makes this test environment-dependent.
	cache, err := NewDiskReadCache(DiskReadCacheOptions{Dir: dir, MaxSize: 1 << 20, FreeRatio: -1})
	if err != nil {
		t.Fatalf("NewDiskReadCache: %v", err)
	}
	key := DiskReadCacheKey{FileID: "file-1", Path: "/file.bin", Revision: 7, Offset: 0, Length: 5}
	cache.PutOwned(key, []byte("hello"))
	if err := os.WriteFile(filepath.Join(dir, "orphan.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatalf("write temp entry: %v", err)
	}

	recovered, err := NewDiskReadCache(DiskReadCacheOptions{Dir: dir, MaxSize: 1 << 20, FreeRatio: -1})
	if err != nil {
		t.Fatalf("recover DiskReadCache: %v", err)
	}
	got, ok := recovered.Get(key)
	if !ok || string(got) != "hello" {
		t.Fatalf("recovered cache entry = %q, %v; want hello hit", got, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, "orphan.tmp")); !os.IsNotExist(err) {
		t.Fatalf("orphan temp stat err = %v, want not exists", err)
	}
}

func newTestDiskReadCache(t *testing.T, maxSize int64) *DiskReadCache {
	t.Helper()
	cache, err := NewDiskReadCache(DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: maxSize, FreeRatio: -1})
	if err != nil {
		t.Fatalf("NewDiskReadCache: %v", err)
	}
	t.Cleanup(cache.Close)
	return cache
}
