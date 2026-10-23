package writebuffer

import (
	"bytes"
	"sync/atomic"
	"syscall"
	"testing"
)

// The small-file fast path requires partSize >= smallFileMax (default
// 50_000), so these tests force it with a 64KiB part size.
func smallModeBuffer() *WriteBuffer {
	return NewWriteBuffer("/f", 1<<20, 64<<10)
}

// Part mode: a part size below the small-file threshold keeps every write
// on the split-part path.
func partModeBuffer(partSize int64) *WriteBuffer {
	return NewWriteBuffer("/f", 1<<20, partSize)
}

func TestSmallFileFastPathAndAccessors(t *testing.T) {
	wb := smallModeBuffer()
	if n, err := wb.Write(0, []byte("hello")); err != nil || n != 5 {
		t.Fatalf("first write = %d %v", n, err)
	}
	if n, err := wb.Write(5, []byte(" world")); err != nil || n != 6 {
		t.Fatalf("append write = %d %v", n, err)
	}
	if wb.Size() != 11 {
		t.Fatalf("size = %d, want 11", wb.Size())
	}
	data, small := wb.SmallFileBytes()
	if !small || string(data) != "hello world" {
		t.Fatalf("small image = %q small=%v", data, small)
	}
	if got := wb.Bytes(); string(got) != "hello world" {
		t.Fatalf("Bytes = %q", got)
	}
	if nums := wb.DirtyPartNumbers(); len(nums) != 1 || nums[0] != 1 {
		t.Fatalf("dirty parts = %v, want [1]", nums)
	}
	if !wb.HasDirtyParts() {
		t.Fatal("buffer reports clean after writes")
	}
	buf := make([]byte, 4)
	if n := wb.ReadAt(6, buf); n != 4 || string(buf) != "worl" {
		t.Fatalf("ReadAt(6) = %d %q", n, buf)
	}
	if n := wb.ReadAt(11, buf); n != 0 {
		t.Fatalf("ReadAt at EOF = %d, want 0", n)
	}
}

func TestWriteBeyondMaxReturnsEFBIG(t *testing.T) {
	wb := NewWriteBuffer("/f", 8, 64<<10)
	if _, err := wb.Write(0, make([]byte, 9)); err != syscall.EFBIG {
		t.Fatalf("oversized write err = %v, want EFBIG", err)
	}
	if err := wb.Truncate(9); err != syscall.EFBIG {
		t.Fatalf("oversized truncate err = %v, want EFBIG", err)
	}
}

func TestPartModeSplitAcrossParts(t *testing.T) {
	wb := partModeBuffer(16)
	payload := bytes.Repeat([]byte("x"), 40) // 3 parts of 16
	if _, err := wb.Write(0, payload); err != nil {
		t.Fatal(err)
	}
	if wb.Size() != 40 {
		t.Fatalf("size = %d, want 40", wb.Size())
	}
	if _, small := wb.SmallFileBytes(); small {
		t.Fatal("part-mode buffer reports small fast path")
	}
	if got := wb.Bytes(); !bytes.Equal(got, payload) {
		t.Fatalf("Bytes mismatch: %d bytes", len(got))
	}
	if nums := wb.DirtyPartNumbers(); len(nums) != 3 {
		t.Fatalf("dirty parts = %v, want 3 entries", nums)
	}
	if part := wb.PartData(2); len(part) != 16 {
		t.Fatalf("part 2 len = %d, want 16", len(part))
	}
	if wb.PartData(4) != nil {
		t.Fatal("out-of-range part returned data")
	}
	buf := make([]byte, 20)
	if n := wb.ReadAt(10, buf); n != 20 || !bytes.Equal(buf, bytes.Repeat([]byte("x"), 20)) {
		t.Fatalf("cross-part ReadAt = %d", n)
	}
}

func TestPartModeGapZeroFill(t *testing.T) {
	wb := partModeBuffer(4)
	if _, err := wb.Write(0, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(10, []byte("cd")); err != nil {
		t.Fatal(err)
	}
	want := []byte("ab\x00\x00\x00\x00\x00\x00\x00\x00cd")
	if got := wb.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("gap fill = %q, want %q", got, want)
	}
}

func TestTruncateShrinkAndGrowSmallMode(t *testing.T) {
	wb := smallModeBuffer()
	if _, err := wb.Write(0, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := wb.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if got := wb.Bytes(); string(got) != "hello" {
		t.Fatalf("after shrink = %q", got)
	}
	if err := wb.Truncate(8); err != nil {
		t.Fatal(err)
	}
	got := wb.Bytes()
	if len(got) != 8 || string(got[:5]) != "hello" || !allZero(got[5:]) {
		t.Fatalf("after grow = %q", got)
	}
	if wb.Size() != 8 {
		t.Fatalf("size after grow = %d", wb.Size())
	}
}

func TestTruncateShrinkPartMode(t *testing.T) {
	wb := partModeBuffer(4)
	if _, err := wb.Write(0, bytes.Repeat([]byte("a"), 12)); err != nil {
		t.Fatal(err)
	}
	if err := wb.Truncate(6); err != nil {
		t.Fatal(err)
	}
	if wb.Size() != 6 {
		t.Fatalf("size = %d, want 6", wb.Size())
	}
	if got := wb.Bytes(); string(got) != "aaaaaa" {
		t.Fatalf("after shrink = %q", got)
	}
	// The dropped tail part no longer reports dirty.
	for _, pn := range wb.DirtyPartNumbers() {
		if pn > 2 {
			t.Fatalf("stale dirty part %d survived the truncate", pn)
		}
	}
	// Re-growing zero-fills.
	if err := wb.Truncate(9); err != nil {
		t.Fatal(err)
	}
	if got := wb.Bytes(); string(got[:6]) != "aaaaaa" || !allZero(got[6:]) {
		t.Fatalf("after regrow = %q", got)
	}
}

func TestLazyLoadRespectsRemoteSize(t *testing.T) {
	wb := partModeBuffer(4)
	var loads atomic.Int32
	remote := map[int][]byte{1: []byte("aaaa"), 2: []byte("bbbb"), 3: []byte("cccc")}
	wb.LoadPart = func(partNum int) ([]byte, error) {
		loads.Add(1)
		return remote[partNum], nil
	}
	wb.SetRemoteSize(8) // parts 1-2 (starts 0,4) are remote-backed; part 3 (start 8) is new

	// A write entirely in the new region must not fetch anything.
	if _, err := wb.Write(8, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if got := loads.Load(); got != 0 {
		t.Fatalf("new-region write fetched %d part(s)", got)
	}
	// Touching the remote-backed region loads exactly that part.
	if _, err := wb.Write(0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("remote-region write fetched %d part(s), want 1", got)
	}
	// Full read reconstructs base + overwrite (part 2 loads on demand).
	// The lazy boundary does not change the logical size: only the write
	// extent does (0-1 and 8-10 → 10).
	if err := wb.EnsureLoaded(1); err != nil { // 0-based index of part 2
		t.Fatal(err)
	}
	if wb.Size() != 10 {
		t.Fatalf("size = %d, want 10", wb.Size())
	}
}

func TestClearDirtyAndReset(t *testing.T) {
	wb := smallModeBuffer()
	if _, err := wb.Write(0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	wb.ClearDirty()
	if nums := wb.DirtyPartNumbers(); len(nums) != 0 {
		t.Fatalf("dirty parts after ClearDirty = %v", nums)
	}
	if wb.HasDirtyParts() {
		t.Fatal("HasDirtyParts after ClearDirty")
	}
	if got := wb.Bytes(); string(got) != "data" {
		t.Fatalf("ClearDirty dropped readable data: %q", got)
	}

	wb.Reset()
	if wb.Size() != 0 || wb.Bytes() != nil {
		t.Fatalf("Reset left size=%d bytes=%v", wb.Size(), wb.Bytes())
	}
	if _, small := wb.SmallFileBytes(); small {
		t.Fatal("Reset left small-file state behind")
	}
}

func TestMarkAllDirty(t *testing.T) {
	wb := partModeBuffer(4)
	if _, err := wb.Write(0, []byte("0123456789ab")); err != nil {
		t.Fatal(err)
	}
	wb.ClearDirty()
	wb.MarkAllDirty()
	nums := wb.DirtyPartNumbers()
	if len(nums) != 3 {
		t.Fatalf("MarkAllDirty set %d parts, want 3", len(nums))
	}
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
