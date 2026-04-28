package fuse

import (
	"bytes"
	"os"
	"testing"
)

func TestShadowStoreWriteExtentsReadAt(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Create a write buffer with some dirty data.
	wb := NewWriteBuffer("/test/file.txt", 0, 0)
	_, _ = wb.Write(0, []byte("hello world"))

	// Write extents to shadow.
	if err := ss.WriteExtents("/test/file.txt", wb, 42); err != nil {
		t.Fatal(err)
	}

	// Verify size.
	if sz := ss.Size("/test/file.txt"); sz != 11 {
		t.Errorf("size = %d, want 11", sz)
	}

	// Read back.
	buf := make([]byte, 11)
	n, err := ss.ReadAt("/test/file.txt", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Errorf("read n = %d, want 11", n)
	}
	if !bytes.Equal(buf, []byte("hello world")) {
		t.Errorf("data = %q, want %q", buf, "hello world")
	}

	// Verify base rev.
	if rev := ss.BaseRev("/test/file.txt"); rev != 42 {
		t.Errorf("baseRev = %d, want 42", rev)
	}
}

func TestShadowStorePartialWrite(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write buffer with data in two separate parts.
	wb := NewWriteBuffer("/partial.txt", 0, 1024)         // 1KB part size
	_, _ = wb.Write(0, bytes.Repeat([]byte("A"), 1024))   // Part 0 — full
	_, _ = wb.Write(1024, bytes.Repeat([]byte("B"), 500)) // Part 1 — partial

	if err := ss.WriteExtents("/partial.txt", wb, 1); err != nil {
		t.Fatal(err)
	}

	// Read part 0.
	buf0 := make([]byte, 1024)
	n, _ := ss.ReadAt("/partial.txt", 0, buf0)
	if n != 1024 {
		t.Errorf("read n = %d, want 1024", n)
	}
	if !bytes.Equal(buf0, bytes.Repeat([]byte("A"), 1024)) {
		t.Error("part 0 data mismatch")
	}

	// Read part 1.
	buf1 := make([]byte, 500)
	n, _ = ss.ReadAt("/partial.txt", 1024, buf1)
	if n != 500 {
		t.Errorf("read n = %d, want 500", n)
	}
	if !bytes.Equal(buf1, bytes.Repeat([]byte("B"), 500)) {
		t.Error("part 1 data mismatch")
	}
}

func TestShadowStoreRemove(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/remove.txt", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	_ = ss.WriteExtents("/remove.txt", wb, 1)

	if !ss.Has("/remove.txt") {
		t.Error("expected shadow file to exist")
	}

	ss.Remove("/remove.txt")

	if ss.Has("/remove.txt") {
		t.Error("expected shadow file to be removed")
	}
}

func TestShadowStoreRename(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/old.txt", 0, 0)
	_, _ = wb.Write(0, []byte("renamed"))
	_ = ss.WriteExtents("/old.txt", wb, 1)

	ok := ss.Rename("/old.txt", "/new.txt")
	if !ok {
		t.Fatal("expected rename to succeed")
	}

	if ss.Has("/old.txt") {
		t.Error("expected old path to be gone")
	}

	buf := make([]byte, 7)
	n, err := ss.ReadAt("/new.txt", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 || !bytes.Equal(buf, []byte("renamed")) {
		t.Errorf("read after rename = %q, want %q", buf[:n], "renamed")
	}
}

func TestShadowStoreReadAll(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/readall.txt", 0, 0)
	_, _ = wb.Write(0, []byte("full content"))
	_ = ss.WriteExtents("/readall.txt", wb, 1)

	data, err := ss.ReadAll("/readall.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("full content")) {
		t.Errorf("ReadAll = %q, want %q", data, "full content")
	}
}

func TestShadowStoreCheckDiskSpace(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Just verify the function doesn't panic.
	_ = ss.CheckDiskSpace()
}

func TestShadowStorePinUnpinRemove(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write some data.
	wb := NewWriteBuffer("/pinned.txt", 0, 0)
	_, _ = wb.Write(0, []byte("pinned data"))
	_ = ss.WriteExtents("/pinned.txt", wb, 1)

	// Pin the path — get generation token.
	gen := ss.Pin("/pinned.txt")
	if gen == 0 {
		t.Fatal("expected non-zero generation token from Pin")
	}

	// Remove while pinned — shadow is retired (removed from active files
	// but fd stays alive for the pinned reader).
	ss.Remove("/pinned.txt")

	// Active Has returns false (retired shadows are not active).
	if ss.Has("/pinned.txt") {
		t.Fatal("expected Has to return false after retire")
	}

	// ReadAtGen should work on the retired shadow.
	buf := make([]byte, 11)
	n, err := ss.ReadAtGen(gen, 0, buf)
	if err != nil {
		t.Fatalf("ReadAtGen on retired shadow: %v", err)
	}
	if n != 11 || !bytes.Equal(buf[:n], []byte("pinned data")) {
		t.Errorf("ReadAtGen data = %q, want %q", buf[:n], "pinned data")
	}

	// SizeGen should return the retired size.
	if sz := ss.SizeGen(gen); sz != 11 {
		t.Errorf("SizeGen = %d, want 11", sz)
	}

	// Unpin — should trigger retired cleanup.
	ss.Unpin(gen)

	// ReadAtGen should fail after Unpin.
	_, err = ss.ReadAtGen(gen, 0, buf)
	if err == nil {
		t.Error("expected ReadAtGen to fail after Unpin")
	}

	// SizeGen should return -1 after Unpin.
	if sz := ss.SizeGen(gen); sz != -1 {
		t.Errorf("SizeGen after Unpin = %d, want -1", sz)
	}
}

func TestShadowStorePinMultipleReaders(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/multi.txt", 0, 0)
	_, _ = wb.Write(0, []byte("multi"))
	_ = ss.WriteExtents("/multi.txt", wb, 1)

	// Two pins (same generation).
	gen1 := ss.Pin("/multi.txt")
	gen2 := ss.Pin("/multi.txt")
	if gen1 != gen2 {
		t.Fatalf("expected same generation, got %d vs %d", gen1, gen2)
	}

	ss.Remove("/multi.txt")

	// First unpin — still one ref on the retired entry.
	ss.Unpin(gen1)
	// Retired shadow exists but Has returns false (not active).
	if ss.Has("/multi.txt") {
		t.Fatal("expected Has to return false after retire")
	}

	// Second unpin — retired shadow cleaned up.
	ss.Unpin(gen2)
}

func TestShadowStoreRemoveWithoutPin(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/nopins.txt", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	_ = ss.WriteExtents("/nopins.txt", wb, 1)

	// Remove without pin — should remove immediately.
	ss.Remove("/nopins.txt")
	if ss.Has("/nopins.txt") {
		t.Error("expected immediate removal when not pinned")
	}
}

func TestShadowStoreRenamePinState(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/before.txt", 0, 0)
	_, _ = wb.Write(0, []byte("rename"))
	_ = ss.WriteExtents("/before.txt", wb, 1)

	gen := ss.Pin("/before.txt")
	ok := ss.Rename("/before.txt", "/after.txt")
	if !ok {
		t.Fatal("rename failed")
	}

	// Remove while pinned under new name — retires the shadow.
	ss.Remove("/after.txt")
	if ss.Has("/after.txt") {
		t.Fatal("expected Has to return false after retire")
	}

	// Unpin triggers retired cleanup.
	ss.Unpin(gen)
}

func TestShadowStoreRenameFailureRollbackPinState(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/rollback.txt", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	_ = ss.WriteExtents("/rollback.txt", wb, 1)

	gen := ss.Pin("/rollback.txt")

	// Delete the on-disk shadow file to force os.Rename failure.
	sp := ss.shadowPath("/rollback.txt")
	_ = os.Remove(sp)

	ok := ss.Rename("/rollback.txt", "/target.txt")
	if ok {
		t.Fatal("expected rename to fail when disk file is missing")
	}

	// Generation should still be valid on old path.
	// Remove retires, then Unpin cleans up.
	ss.Remove("/rollback.txt")
	ss.Unpin(gen)
}

func TestShadowStorePinIfExists(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// PinIfExists on a non-existent path should return false.
	if _, ok := ss.PinIfExists("/missing.txt"); ok {
		t.Fatal("expected PinIfExists to return false for missing path")
	}

	// Write some data.
	wb := NewWriteBuffer("/exists.txt", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	_ = ss.WriteExtents("/exists.txt", wb, 1)

	// PinIfExists on an existing path should return true.
	gen, ok := ss.PinIfExists("/exists.txt")
	if !ok {
		t.Fatal("expected PinIfExists to return true for existing path")
	}

	// Remove while pinned — retires shadow.
	ss.Remove("/exists.txt")
	if ss.Has("/exists.txt") {
		t.Fatal("expected Has to return false after retire")
	}

	// Unpin triggers retired cleanup.
	ss.Unpin(gen)
}

func TestShadowStoreRemovePreventsPin(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/race.txt", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	_ = ss.WriteExtents("/race.txt", wb, 1)

	// Remove with no pins — should delete immediately.
	ss.Remove("/race.txt")

	// PinIfExists after removal must return false.
	if _, ok := ss.PinIfExists("/race.txt"); ok {
		t.Fatal("expected PinIfExists to return false after Remove")
	}
}

// TestShadowStoreRetireAllowsNewWriter verifies that after Remove retires a
// pinned shadow, a new writer can create a fresh shadow at the same path
// without interfering with the retired reader, and the retired reader's Unpin
// does not affect the new shadow.
func TestShadowStoreRetireAllowsNewWriter(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Old writer creates shadow.
	wb1 := NewWriteBuffer("/file.txt", 0, 0)
	_, _ = wb1.Write(0, []byte("old data"))
	_ = ss.WriteExtents("/file.txt", wb1, 1)

	// Reader pins it.
	gen := ss.Pin("/file.txt")

	// Commit queue succeeds, retires the shadow.
	ss.Remove("/file.txt")

	// New writer creates fresh shadow at same path.
	wb2 := NewWriteBuffer("/file.txt", 0, 0)
	_, _ = wb2.Write(0, []byte("new data!!!"))
	if err := ss.WriteExtents("/file.txt", wb2, 2); err != nil {
		t.Fatal(err)
	}

	// New shadow is readable.
	buf := make([]byte, 11)
	n, err := ss.ReadAt("/file.txt", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], []byte("new data!!!")) {
		t.Errorf("new shadow data = %q, want %q", buf[:n], "new data!!!")
	}

	// Old reader unpins — should NOT delete the new shadow.
	ss.Unpin(gen)

	// New shadow must still exist.
	if !ss.Has("/file.txt") {
		t.Fatal("expected new shadow to survive old reader's Unpin")
	}
	buf2 := make([]byte, 11)
	n2, err := ss.ReadAt("/file.txt", 0, buf2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf2[:n2], []byte("new data!!!")) {
		t.Errorf("new shadow data after unpin = %q, want %q", buf2[:n2], "new data!!!")
	}
}

// TestShadowStoreReadAtGenActive verifies that ReadAtGen and SizeGen work
// for active (not yet retired) generations, covering the transition window
// where Remove hasn't been called yet.
func TestShadowStoreReadAtGenActive(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	wb := NewWriteBuffer("/active.txt", 0, 0)
	_, _ = wb.Write(0, []byte("active data"))
	_ = ss.WriteExtents("/active.txt", wb, 1)

	gen := ss.Pin("/active.txt")

	// SizeGen should work while shadow is still active (not retired).
	if sz := ss.SizeGen(gen); sz != 11 {
		t.Errorf("SizeGen on active gen = %d, want 11", sz)
	}

	// ReadAtGen should work while shadow is still active.
	buf := make([]byte, 11)
	n, err := ss.ReadAtGen(gen, 0, buf)
	if err != nil {
		t.Fatalf("ReadAtGen on active gen: %v", err)
	}
	if n != 11 || !bytes.Equal(buf, []byte("active data")) {
		t.Errorf("ReadAtGen data = %q, want %q", buf[:n], "active data")
	}

	ss.Unpin(gen)
}

func TestShadowStoreUnpinZeroNoop(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Unpin(0) should be a no-op and not panic.
	ss.Unpin(0)
}

// TestShadowStorePinIfExistsDiskOnly verifies that PinIfExists loads a shadow
// file from disk when it is not in the in-memory files map (e.g. after
// crash/restart recovery where pending shadows exist on disk only).
func TestShadowStorePinIfExistsDiskOnly(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write shadow data and then remove from in-memory map only (simulate
	// crash recovery: disk file exists, files map does not).
	wb := NewWriteBuffer("/disk-only.txt", 0, 0)
	_, _ = wb.Write(0, []byte("disk data"))
	_ = ss.WriteExtents("/disk-only.txt", wb, 1)

	// Remove from in-memory map without deleting disk file.
	ss.mu.Lock()
	sf := ss.files["/disk-only.txt"]
	_ = sf.fd.Close()
	delete(ss.files, "/disk-only.txt")
	ss.mu.Unlock()

	// PinIfExists should load from disk and succeed.
	gen, ok := ss.PinIfExists("/disk-only.txt")
	if !ok {
		t.Fatal("expected PinIfExists to load from disk and return true")
	}
	if gen == 0 {
		t.Fatal("expected non-zero generation token")
	}

	// Read via generation should work.
	buf := make([]byte, 9)
	n, err := ss.ReadAtGen(gen, 0, buf)
	if err != nil {
		t.Fatalf("ReadAtGen after disk load: %v", err)
	}
	if n != 9 || !bytes.Equal(buf[:n], []byte("disk data")) {
		t.Errorf("data = %q, want %q", buf[:n], "disk data")
	}

	ss.Unpin(gen)
}

func TestShadowStoreCheckDiskSpaceThrottled(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// First call should run the real check and cache result.
	r1 := ss.CheckDiskSpaceThrottled()

	// Second call within the interval should return cached result without syscall.
	r2 := ss.CheckDiskSpaceThrottled()
	if r1 != r2 {
		t.Fatalf("throttled results differ: %v vs %v", r1, r2)
	}

	// Verify initial diskOK state matches the real check.
	if r1 != ss.CheckDiskSpace() {
		t.Fatal("throttled result does not match real check")
	}
}
