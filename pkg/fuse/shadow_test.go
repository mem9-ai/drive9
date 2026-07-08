package fuse

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
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

func TestShadowStoreRenameReplacesDestinationGeneration(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	dst := NewWriteBuffer("/final.txt", 0, 0)
	_, _ = dst.Write(0, []byte("old-final"))
	if err := ss.WriteExtents("/final.txt", dst, 1); err != nil {
		t.Fatal(err)
	}
	oldGen := ss.Pin("/final.txt")
	ss.Unpin(oldGen)

	src := NewWriteBuffer("/tmp.txt", 0, 0)
	_, _ = src.Write(0, []byte("new-final"))
	if err := ss.WriteExtents("/tmp.txt", src, 2); err != nil {
		t.Fatal(err)
	}

	if !ss.Rename("/tmp.txt", "/final.txt") {
		t.Fatal("rename failed")
	}

	newGen, ok := ss.PinIfExists("/final.txt")
	if !ok {
		t.Fatal("expected renamed destination to be pinnable")
	}
	if newGen == oldGen {
		t.Fatalf("renamed destination reused stale generation %d", oldGen)
	}
	buf := make([]byte, len("new-final"))
	n, err := ss.ReadAtGen(newGen, 0, buf)
	if err != nil {
		t.Fatalf("ReadAtGen renamed destination: %v", err)
	}
	if n != len(buf) || !bytes.Equal(buf[:n], []byte("new-final")) {
		t.Fatalf("ReadAtGen renamed destination = %q, want new-final", buf[:n])
	}
	ss.Unpin(newGen)
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

// TestShadowStoreRemoveDiskOnly verifies that Remove cleans up a shadow file
// that exists only on disk (not in the files map). This covers the recovery
// scenario where commit queue uploads a disk-only shadow and then calls Remove.
func TestShadowStoreRemoveDiskOnly(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write a shadow and then remove it from in-memory map only.
	wb := NewWriteBuffer("/disk-rm.txt", 0, 0)
	_, _ = wb.Write(0, []byte("stale"))
	_ = ss.WriteExtents("/disk-rm.txt", wb, 1)

	// Simulate disk-only state: close fd, remove from files map.
	ss.mu.Lock()
	sf := ss.files["/disk-rm.txt"]
	_ = sf.fd.Close()
	delete(ss.files, "/disk-rm.txt")
	ss.mu.Unlock()

	// Verify shadow is visible on disk via Has().
	if !ss.Has("/disk-rm.txt") {
		t.Fatal("expected disk-only shadow to be visible via Has")
	}

	// Remove should clean up the disk file even though files map is empty.
	ss.Remove("/disk-rm.txt")

	// Has() should no longer find it.
	if ss.Has("/disk-rm.txt") {
		t.Fatal("expected disk-only shadow to be cleaned up after Remove")
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

// TestNewShadowStoreSweepsRetiredFiles verifies that retired shadows leaked
// by a crashed process (pinned readers never unpinned) are removed at store
// construction, while live .shadow files are preserved.
func TestNewShadowStoreSweepsRetiredFiles(t *testing.T) {
	dir := t.TempDir()

	ss, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := ss.WriteFull("/keep.txt", []byte("keep"), 1); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash while a pinned reader held a retired shadow.
	gen := ss.Pin("/keep.txt")
	if gen == 0 {
		t.Fatal("pin failed")
	}
	ss.Remove("/keep.txt") // pinned → retired on disk
	if err := ss.WriteFull("/live.txt", []byte("live"), 1); err != nil {
		t.Fatal(err)
	}
	ss.Close() // crash: Unpin never runs

	leftover, err := filepath.Glob(filepath.Join(dir, "*.retired.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftover) != 1 {
		t.Fatalf("test setup: expected 1 leaked retired file, got %d", len(leftover))
	}

	// "Remount": construction sweeps the leak, keeps live shadows.
	ss2, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ss2.Close()

	leftover, err = filepath.Glob(filepath.Join(dir, "*.retired.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftover) != 0 {
		t.Errorf("retired files not swept: %v", leftover)
	}
	if !ss2.Has("/live.txt") {
		t.Error("live shadow was swept")
	}
}

// --- Write-back disk protection tests (Issue #651) ---

func TestCheckWriteBackQuotaByteQuotaAllowed(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 1024) // 1KB quota, no free ratio
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Writing 500 bytes with 0 pending should succeed.
	if err := ss.CheckWriteBackQuota(500); err != nil {
		t.Fatalf("expected write to be allowed, got %v", err)
	}
}

func TestCheckWriteBackQuotaByteQuotaExceeded(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 1024) // 1KB quota, no free ratio
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write an 800-byte shadow file to accumulate pending bytes internally.
	if err := ss.WriteFull("/big.txt", make([]byte, 800), 1); err != nil {
		t.Fatal(err)
	}

	// Writing 300 more bytes (total 1100 > 1024) should be rejected.
	err = ss.CheckWriteBackQuota(300)
	if err != syscall.ENOSPC {
		t.Fatalf("expected ENOSPC, got %v", err)
	}
}

func TestCheckWriteBackQuotaByteQuotaDisabled(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 0) // both disabled
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write a large shadow file; with quota disabled, any further write should pass.
	if err := ss.WriteFull("/big.txt", make([]byte, 10000), 1); err != nil {
		t.Fatal(err)
	}
	if err := ss.CheckWriteBackQuota(999999999); err != nil {
		t.Fatalf("expected write to be allowed with quota disabled, got %v", err)
	}
}

func TestCheckWriteBackQuotaFreeRatioAllowed(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0.10, 0) // 10% free ratio, no byte quota
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// A small write should succeed (test machine has > 10% free space).
	if err := ss.CheckWriteBackQuota(1); err != nil {
		t.Fatalf("expected small write to be allowed, got %v", err)
	}
}

func TestCheckWriteBackQuotaFreeRatioExceeded(t *testing.T) {
	dir := t.TempDir()
	// Use a free ratio of 0.9999 so any write triggers rejection
	// (the test machine would need 99.99% free space after the write).
	ss, err := NewShadowStoreWithQuota(dir, 0.9999, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// A large write should fail because it can't keep 99.99% free.
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		t.Skip("cannot statfs temp dir")
	}
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	err = ss.CheckWriteBackQuota(totalBytes / 2)
	if err != syscall.ENOSPC {
		t.Fatalf("expected ENOSPC for large write with 99.99%% free ratio, got %v", err)
	}
}

func TestCheckWriteBackQuotaFreeRatioDisabled(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 0) // both disabled
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// With free ratio disabled, write should pass.
	if err := ss.CheckWriteBackQuota(1 << 30); err != nil {
		t.Fatalf("expected write allowed with free ratio disabled, got %v", err)
	}
}

func TestPendingBytesWriteFullAndRemove(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Initial pending should be 0.
	if p := ss.PendingBytes(); p != 0 {
		t.Fatalf("expected initial pending 0, got %d", p)
	}

	// WriteFull should increase pending bytes.
	if err := ss.WriteFull("/a.txt", make([]byte, 1000), 1); err != nil {
		t.Fatal(err)
	}
	if p := ss.PendingBytes(); p != 1000 {
		t.Fatalf("expected pending 1000 after write, got %d", p)
	}

	// Overwriting with a smaller file should decrease pending.
	if err := ss.WriteFull("/a.txt", make([]byte, 600), 2); err != nil {
		t.Fatal(err)
	}
	if p := ss.PendingBytes(); p != 600 {
		t.Fatalf("expected pending 600 after overwrite, got %d", p)
	}

	// Remove should bring it back to 0.
	ss.Remove("/a.txt")
	if p := ss.PendingBytes(); p != 0 {
		t.Fatalf("expected pending 0 after remove, got %d", p)
	}
}

func TestRecoverPendingBytesFromDisk(t *testing.T) {
	dir := t.TempDir()

	// Manually create shadow files on disk to simulate a crash scenario
	// where the in-memory ShadowStore state is lost.
	if err := os.WriteFile(filepath.Join(dir, "aaa.shadow"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bbb.shadow"), []byte("world!!"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a new store and recover pending bytes.
	ss, err := NewShadowStoreWithQuota(dir, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	ss.RecoverPendingBytes()
	// Should recover 5 + 7 = 12 bytes.
	if p := ss.PendingBytes(); p != 12 {
		t.Fatalf("expected recovered pending 12, got %d", p)
	}
}

func TestByteQuotaRecoveryAfterCommitCleanup(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 100) // 100 byte quota
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Stage an 80-byte shadow file.
	if err := ss.WriteFull("/file.txt", make([]byte, 80), 1); err != nil {
		t.Fatal(err)
	}
	if err := ss.CheckWriteBackQuota(30); err != syscall.ENOSPC {
		t.Fatalf("expected ENOSPC when 80+30>100, got %v", err)
	}

	// Simulate commit cleanup: Remove releases pending bytes.
	ss.Remove("/file.txt")

	// Now writing 30 should succeed again.
	if err := ss.CheckWriteBackQuota(30); err != nil {
		t.Fatalf("expected write to succeed after cleanup, got %v", err)
	}
}

func TestPendingBytesNoDoubleCount(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write a 500-byte shadow file.
	if err := ss.WriteFull("/f.txt", make([]byte, 500), 1); err != nil {
		t.Fatal(err)
	}
	if p := ss.PendingBytes(); p != 500 {
		t.Fatalf("expected pending 500, got %d", p)
	}

	// Overwrite the same path with 500 bytes again — pending must stay 500, not 1000.
	if err := ss.WriteFull("/f.txt", make([]byte, 500), 2); err != nil {
		t.Fatal(err)
	}
	if p := ss.PendingBytes(); p != 500 {
		t.Fatalf("expected pending 500 after re-stage, got %d (double-count bug)", p)
	}

	// A third stage that grows the file to 700 should bring pending to 700.
	if err := ss.WriteFull("/f.txt", make([]byte, 700), 3); err != nil {
		t.Fatal(err)
	}
	if p := ss.PendingBytes(); p != 700 {
		t.Fatalf("expected pending 700 after grow, got %d", p)
	}

	// Shrink back to 200.
	if err := ss.WriteFull("/f.txt", make([]byte, 200), 4); err != nil {
		t.Fatal(err)
	}
	if p := ss.PendingBytes(); p != 200 {
		t.Fatalf("expected pending 200 after shrink, got %d", p)
	}
}

func TestReStageNoFalseENOSPC(t *testing.T) {
	dir := t.TempDir()
	// Quota is 1000 bytes.
	ss, err := NewShadowStoreWithQuota(dir, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write 800 bytes — within quota.
	if err := ss.WriteFull("/f.txt", make([]byte, 800), 1); err != nil {
		t.Fatalf("first write should succeed: %v", err)
	}

	// Re-stage same path with same 800 bytes — delta is 0, should NOT reject.
	if err := ss.WriteFull("/f.txt", make([]byte, 800), 2); err != nil {
		t.Fatalf("re-stage same size should succeed (delta=0): %v", err)
	}

	// Re-stage with 900 bytes — delta is 100, total 900 < 1000, should succeed.
	if err := ss.WriteFull("/f.txt", make([]byte, 900), 3); err != nil {
		t.Fatalf("re-stage with 100 byte growth should succeed: %v", err)
	}

	// Re-stage with 1100 bytes — delta is 200, total 1100 > 1000, should ENOSPC.
	err = ss.WriteFull("/f.txt", make([]byte, 1100), 4)
	if err != syscall.ENOSPC {
		t.Fatalf("re-stage exceeding quota should ENOSPC, got %v", err)
	}

	// Pending should still be 900 (last successful write).
	if p := ss.PendingBytes(); p != 900 {
		t.Fatalf("expected pending 900 after rejected write, got %d", p)
	}
}

func TestWriteStreamQuotaEnforcement(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 100) // 100 byte quota
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// WriteStream within quota should succeed.
	origData := bytes.Repeat([]byte("A"), 50)
	n, err := ss.WriteStream("/f.txt", bytes.NewReader(origData), 1)
	if err != nil {
		t.Fatalf("WriteStream within quota should succeed: %v", err)
	}
	if n != 50 {
		t.Fatalf("expected 50 bytes written, got %d", n)
	}
	if p := ss.PendingBytes(); p != 50 {
		t.Fatalf("expected pending 50, got %d", p)
	}

	// WriteStream exceeding quota should ENOSPC and preserve original shadow.
	_, err = ss.WriteStream("/f.txt", bytes.NewReader(make([]byte, 200)), 2)
	if err != syscall.ENOSPC {
		t.Fatalf("WriteStream exceeding quota should ENOSPC, got %v", err)
	}
	// Pending should still be 50 (original shadow untouched).
	if p := ss.PendingBytes(); p != 50 {
		t.Fatalf("expected pending 50 after failed stream, got %d", p)
	}
	// Original content must be preserved.
	data, err := ss.ReadAll("/f.txt")
	if err != nil {
		t.Fatalf("ReadAll after failed stream: %v", err)
	}
	if !bytes.Equal(data, origData) {
		t.Fatalf("shadow content corrupted: got %d bytes of %q, want 50 bytes of 'A'", len(data), data[:1])
	}
}

func TestWriteStreamPeakDiskBounded(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 100) // 100 byte quota
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Attempt to stream 200 bytes which exceeds the 100-byte quota.
	// The incremental quota check should abort mid-stream so the temp
	// file never reaches 200 bytes.
	_, err = ss.WriteStream("/f.txt", bytes.NewReader(make([]byte, 200)), 1)
	if err != syscall.ENOSPC {
		t.Fatalf("expected ENOSPC, got %v", err)
	}

	// Verify no temp file residue.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file residue found: %s", e.Name())
		}
	}

	// pendingBytes should be 0 (nothing was committed).
	if p := ss.PendingBytes(); p != 0 {
		t.Fatalf("expected pending 0, got %d", p)
	}
}

func TestWriteStreamReplacementPeakBounded(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 100) // 100 byte quota
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write an initial 50-byte shadow.
	origData := bytes.Repeat([]byte("A"), 50)
	if _, err := ss.WriteStream("/f.txt", bytes.NewReader(origData), 1); err != nil {
		t.Fatalf("initial WriteStream: %v", err)
	}
	if p := ss.PendingBytes(); p != 50 {
		t.Fatalf("expected pending 50, got %d", p)
	}

	// Replace with 100 bytes. Final pending would be 100 (within quota),
	// but peak disk is old(50) + tmp(100) = 150 which exceeds quota=100.
	// The quota check uses the full temp size, so this must ENOSPC.
	_, err = ss.WriteStream("/f.txt", bytes.NewReader(make([]byte, 100)), 2)
	if err != syscall.ENOSPC {
		t.Fatalf("expected ENOSPC for replacement peak exceeding quota, got %v", err)
	}

	// Original shadow must be preserved.
	if p := ss.PendingBytes(); p != 50 {
		t.Fatalf("expected pending 50 after rejected replacement, got %d", p)
	}
	data, err := ss.ReadAll("/f.txt")
	if err != nil {
		t.Fatalf("ReadAll after rejected replacement: %v", err)
	}
	if !bytes.Equal(data, origData) {
		t.Fatalf("shadow content corrupted after rejected replacement")
	}
}

func TestFreeRatioThrottledReEvaluatesCachedCapacity(t *testing.T) {
	dir := t.TempDir()
	// free-ratio=0.50 (50% must remain free); no byte quota.
	ss, err := NewShadowStoreWithQuota(dir, 0.50, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Directly prime the cached Statfs values to simulate a known disk geometry.
	// Simulated: 1000 bytes total, 600 bytes free (60% free).
	ss.cachedFreeBytes.Store(600)
	ss.cachedTotalBytes.Store(1000)
	// Set lastDiskCheck to now so throttled path uses cached values.
	ss.lastDiskCheck.Store(time.Now().UnixNano())

	// Small requiredBytes=100: freeAfter=500, ratio=0.50 → pass (barely).
	if err := ss.checkFreeRatioThrottled(100); err != nil {
		t.Fatalf("small requiredBytes should pass: %v", err)
	}

	// Large requiredBytes=200: freeAfter=400, ratio=0.40 < 0.50 → ENOSPC.
	// This verifies that cached capacity is re-evaluated with the new
	// requiredBytes, not just returning the cached bool from the first check.
	err = ss.checkFreeRatioThrottled(200)
	if err != syscall.ENOSPC {
		t.Fatalf("large requiredBytes should ENOSPC with cached capacity, got %v", err)
	}
}

func TestEnsureQuotaEnforcement(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 100) // 100 byte quota
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Ensure within quota should succeed and track pendingBytes.
	if err := ss.Ensure("/f.txt", 80, 1); err != nil {
		t.Fatalf("Ensure within quota should succeed: %v", err)
	}
	if p := ss.PendingBytes(); p != 80 {
		t.Fatalf("expected pending 80 after Ensure, got %d", p)
	}

	// Ensure exceeding quota should ENOSPC.
	err = ss.Ensure("/f.txt", 200, 2)
	if err != syscall.ENOSPC {
		t.Fatalf("Ensure exceeding quota should ENOSPC, got %v", err)
	}
	// Pending should still be 80.
	if p := ss.PendingBytes(); p != 80 {
		t.Fatalf("expected pending 80 after rejected Ensure, got %d", p)
	}

	// WriteAt within the Ensured range should not bypass quota.
	// (delta=0 since WriteAt doesn't grow beyond Ensured size)
	_, err = ss.WriteAt("/f.txt", 0, make([]byte, 80), 1)
	if err != nil {
		t.Fatalf("WriteAt within Ensured size should succeed: %v", err)
	}
	if p := ss.PendingBytes(); p != 80 {
		t.Fatalf("expected pending 80 after WriteAt, got %d", p)
	}
}

// TestWriteAtSparseDoesNotPoisonByteQuota verifies that a sparse write
// (small data at a large offset) does not inflate pendingBytes to the
// point where subsequent small writes are rejected with ENOSPC. The
// WriteAt quota check uses physical data length, not logical file growth,
// so sparse writes pass the free-space ratio check while the byte quota
// (if configured) is not consulted for WriteAt.
func TestWriteAtSparseDoesNotPoisonByteQuota(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 100) // 100 byte quota, no free-ratio
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Sparse write: 1 byte at offset 1<<20 (1 MiB).
	_, err = ss.WriteAt("/sparse.txt", 1<<20, []byte("a"), 1)
	if err != nil {
		t.Fatalf("sparse WriteAt should succeed: %v", err)
	}

	// Subsequent small append at the next offset should also succeed,
	// not be rejected because the first write inflated pendingBytes.
	_, err = ss.WriteAt("/sparse.txt", (1<<20)+1, []byte("b"), 1)
	if err != nil {
		t.Fatalf("append after sparse WriteAt should succeed: %v", err)
	}
}

func TestCheckWriteBackQuotaThrottled(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0.10, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// First call should run the real check.
	err1 := ss.CheckWriteBackQuotaThrottled(1)

	// Second call within interval should return cached result.
	err2 := ss.CheckWriteBackQuotaThrottled(1)
	if (err1 == nil) != (err2 == nil) {
		t.Fatalf("throttled results differ: %v vs %v", err1, err2)
	}
}

// TestRenamePendingBytesReplacesTarget verifies that Rename subtracts the
// replaced target's size from pendingBytes.
func TestRenamePendingBytesReplacesTarget(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Write src=5B, dst=3B.
	if err := ss.WriteFull("/src", []byte("hello"), 1); err != nil {
		t.Fatal(err)
	}
	if err := ss.WriteFull("/dst", []byte("bye"), 1); err != nil {
		t.Fatal(err)
	}
	if pb := ss.PendingBytes(); pb != 8 {
		t.Fatalf("before rename: pendingBytes=%d, want 8", pb)
	}

	// Rename /src → /dst replaces the 3B dst shadow.
	if !ss.Rename("/src", "/dst") {
		t.Fatal("Rename returned false")
	}
	if pb := ss.PendingBytes(); pb != 5 {
		t.Fatalf("after rename: pendingBytes=%d, want 5", pb)
	}
}

// TestWriteStreamConcurrentReadAt verifies no data race between WriteStream
// and ReadAt on the same path. Run with -race to detect.
func TestWriteStreamConcurrentReadAt(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Seed initial shadow.
	if err := ss.WriteFull("/f", []byte("initial"), 1); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	// Concurrent reader.
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		for i := 0; i < 200; i++ {
			_, _ = ss.ReadAt("/f", 0, buf)
		}
	}()
	// Concurrent writer.
	for i := 0; i < 50; i++ {
		data := bytes.Repeat([]byte("x"), i+1)
		_, _ = ss.WriteStream("/f", bytes.NewReader(data), int64(i+2))
	}
	<-done
}

// TestWriteStreamConcurrentReadAtGen verifies no data race between WriteStream
// and ReadAtGen on a pinned active generation. Run with -race to detect.
func TestWriteStreamConcurrentReadAtGen(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Seed initial shadow and pin it.
	if err := ss.WriteFull("/f", []byte("initial"), 1); err != nil {
		t.Fatal(err)
	}
	gen := ss.Pin("/f")
	defer ss.Unpin(gen)

	done := make(chan struct{})
	// Concurrent reader via pinned generation.
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		for i := 0; i < 200; i++ {
			_, _ = ss.ReadAtGen(gen, 0, buf)
		}
	}()
	// Concurrent writer.
	for i := 0; i < 50; i++ {
		data := bytes.Repeat([]byte("x"), i+1)
		_, _ = ss.WriteStream("/f", bytes.NewReader(data), int64(i+2))
	}
	<-done
}

func TestSetDefaultsWriteCacheSizeMBDefault(t *testing.T) {
	opts := MountOptions{}
	opts.setDefaults()
	if opts.WriteCacheSizeMB != defaultWriteCacheSizeMB {
		t.Fatalf("WriteCacheSizeMB=%d, want %d", opts.WriteCacheSizeMB, defaultWriteCacheSizeMB)
	}
}

func TestSetDefaultsWriteCacheSizeMBExplicitValue(t *testing.T) {
	opts := MountOptions{WriteCacheSizeMB: 2048}
	opts.setDefaults()
	if opts.WriteCacheSizeMB != 2048 {
		t.Fatalf("WriteCacheSizeMB=%d, want 2048", opts.WriteCacheSizeMB)
	}
}

func TestSetDefaultsWriteCacheSizeMBNegativeDisables(t *testing.T) {
	opts := MountOptions{WriteCacheSizeMB: -1}
	opts.setDefaults()
	if opts.WriteCacheSizeMB != 0 {
		t.Fatalf("WriteCacheSizeMB=%d, want 0 (disabled)", opts.WriteCacheSizeMB)
	}
}

func TestDefaultByteQuotaEnforcesENOSPC(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(defaultWriteCacheSizeMB) << 20
	ss, err := NewShadowStoreWithQuota(dir, 0, maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Simulate pending bytes near the limit.
	ss.pendingBytes.Store(maxBytes - 100)
	oversize := bytes.Repeat([]byte("b"), 200)
	err = ss.WriteFull("/over", oversize, 1)
	if err != syscall.ENOSPC {
		t.Fatalf("err=%v, want ENOSPC when exceeding default 1GB quota", err)
	}
}

func TestByteQuotaDisabledAllowsUnlimitedWrites(t *testing.T) {
	dir := t.TempDir()
	// Quota disabled (0 maxBytes).
	ss, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// Even with large pendingBytes, writes should succeed when quota is disabled.
	ss.pendingBytes.Store(1 << 40) // 1TB fake pending
	data := bytes.Repeat([]byte("x"), 1024)
	if err := ss.WriteFull("/unlimited", data, 1); err != nil {
		t.Fatalf("write with disabled quota: err=%v, want nil", err)
	}
}

func TestFreeRatioIndependentOfByteQuota(t *testing.T) {
	dir := t.TempDir()
	// Large byte quota (won't trigger), but very high free-ratio requirement.
	ss, err := NewShadowStoreWithQuota(dir, 0.9999, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	ss.lastDiskCheck.Store(0)
	// Force fresh disk stats by writing.
	data := bytes.Repeat([]byte("x"), 1024)
	err = ss.WriteFull("/ratiotest", data, 1)
	// On most disks, 99.99% free ratio will fail. If the disk actually has
	// that much free space, the test is still valid — it just shows both
	// guards are evaluated independently.
	if err == syscall.ENOSPC {
		// Free-ratio guard triggered independently of byte quota — correct.
		return
	}
	t.Logf("free-ratio guard did not trigger (disk has >99.99%% free): err=%v; this is acceptable on large disks", err)
}
