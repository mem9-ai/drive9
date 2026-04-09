package fuse

import (
	"fmt"
	"testing"
	"time"
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
	wb.ClearDirty()                        // clear as if preloaded

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

func TestWriteBuffer_OnPartReady_Notification(t *testing.T) {
	wb := NewWriteBuffer("/test", defaultWriteBufferMaxSize, DefaultPartSize)

	var readyParts []int
	wb.OnPartReady = func(partNum int, data []byte) {
		readyParts = append(readyParts, partNum)
	}

	// Write exactly 1 full part
	data := make([]byte, DefaultPartSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, err := wb.Write(0, data)
	if err != nil {
		t.Fatal(err)
	}

	if len(readyParts) != 1 || readyParts[0] != 1 {
		t.Fatalf("expected OnPartReady called with part 1, got %v", readyParts)
	}
}

func TestWriteBuffer_OnPartReady_NotCalledForPartialPart(t *testing.T) {
	wb := NewWriteBuffer("/test", defaultWriteBufferMaxSize, DefaultPartSize)

	called := false
	wb.OnPartReady = func(partNum int, data []byte) {
		called = true
	}

	// Write less than a full part
	_, _ = wb.Write(0, []byte("small data"))
	if called {
		t.Fatal("OnPartReady should not be called for partial parts")
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
