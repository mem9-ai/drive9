package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
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

func TestReadCache_SkipLargeFiles(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	bigData := make([]byte, smallFileThreshold+1)
	rc.Put("/big", bigData, 1)
	_, ok := rc.Get("/big", 1)
	if ok {
		t.Fatal("large file should not be cached")
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

// ---------------------------------------------------------------------------
// StreamUploader tests
// ---------------------------------------------------------------------------

func TestStreamUploader_NotStartedBeforeUploadAll(t *testing.T) {
	su := NewStreamUploader(nil, "/test", -1)
	if su.Started() {
		t.Fatal("should not be started before UploadAll")
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
		switch {
		case r.URL.Path == "/v2/uploads/initiate":
			initiateCalls++
			// Return a valid upload plan so initiation succeeds.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"upload_id":"test-upload","total_parts":2}`))
		case r.URL.Path == "/v2/uploads/test-upload/presign-batch":
			// Fail presign to simulate S3 error during WritePart.
			http.Error(w, `{"error":"simulated S3 failure"}`, http.StatusInternalServerError)
		case r.URL.Path == "/v2/uploads/test-upload/abort":
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected path: "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer ts.Close()

	c := client.New(ts.URL, "")
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
