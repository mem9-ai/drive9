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

	// Pin the path.
	ss.Pin("/pinned.txt")

	// Remove while pinned — should defer.
	ss.Remove("/pinned.txt")

	// File should still be readable.
	if !ss.Has("/pinned.txt") {
		t.Fatal("expected shadow file to still exist while pinned")
	}
	buf := make([]byte, 11)
	n, err := ss.ReadAt("/pinned.txt", 0, buf)
	if err != nil {
		t.Fatalf("read after pinned remove: %v", err)
	}
	if n != 11 || !bytes.Equal(buf[:n], []byte("pinned data")) {
		t.Errorf("data = %q, want %q", buf[:n], "pinned data")
	}

	// Unpin — should trigger deferred removal.
	ss.Unpin("/pinned.txt")

	if ss.Has("/pinned.txt") {
		t.Error("expected shadow file to be removed after unpin")
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

	// Two pins.
	ss.Pin("/multi.txt")
	ss.Pin("/multi.txt")

	ss.Remove("/multi.txt")

	// First unpin — still pinned.
	ss.Unpin("/multi.txt")
	if !ss.Has("/multi.txt") {
		t.Fatal("expected shadow file to still exist with refs=1")
	}

	// Second unpin — now removed.
	ss.Unpin("/multi.txt")
	if ss.Has("/multi.txt") {
		t.Error("expected shadow file to be removed after all unpins")
	}
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

	ss.Pin("/before.txt")
	ok := ss.Rename("/before.txt", "/after.txt")
	if !ok {
		t.Fatal("rename failed")
	}

	// Remove while pinned under new name.
	ss.Remove("/after.txt")
	if !ss.Has("/after.txt") {
		t.Fatal("expected shadow file to exist under new name while pinned")
	}

	// Unpin under new name triggers removal.
	ss.Unpin("/after.txt")
	if ss.Has("/after.txt") {
		t.Error("expected removal after unpin of renamed path")
	}
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

	ss.Pin("/rollback.txt")

	// Delete the on-disk shadow file to force os.Rename failure.
	sp := ss.shadowPath("/rollback.txt")
	_ = os.Remove(sp)

	ok := ss.Rename("/rollback.txt", "/target.txt")
	if ok {
		t.Fatal("expected rename to fail when disk file is missing")
	}

	// Pin state should be rolled back to old path.
	// Unpin on old path should work, not panic or leak.
	ss.Remove("/rollback.txt")
	if !ss.Has("/rollback.txt") {
		t.Fatal("expected shadow file to exist while pinned after rollback")
	}

	ss.Unpin("/rollback.txt")
	// After unpin + pending remove, file entry should be gone.
	if ss.Has("/rollback.txt") {
		t.Error("expected removal after unpin of rolled-back path")
	}
}

func TestShadowStorePinIfExists(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// PinIfExists on a non-existent path should return false.
	if ss.PinIfExists("/missing.txt") {
		t.Fatal("expected PinIfExists to return false for missing path")
	}

	// Write some data.
	wb := NewWriteBuffer("/exists.txt", 0, 0)
	_, _ = wb.Write(0, []byte("data"))
	_ = ss.WriteExtents("/exists.txt", wb, 1)

	// PinIfExists on an existing path should return true.
	if !ss.PinIfExists("/exists.txt") {
		t.Fatal("expected PinIfExists to return true for existing path")
	}

	// Remove while pinned — should defer.
	ss.Remove("/exists.txt")
	if !ss.Has("/exists.txt") {
		t.Fatal("expected shadow file to still exist while pinned via PinIfExists")
	}

	// Unpin triggers removal.
	ss.Unpin("/exists.txt")
	if ss.Has("/exists.txt") {
		t.Error("expected shadow file to be removed after unpin")
	}
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
	if ss.PinIfExists("/race.txt") {
		t.Fatal("expected PinIfExists to return false after Remove")
	}
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
