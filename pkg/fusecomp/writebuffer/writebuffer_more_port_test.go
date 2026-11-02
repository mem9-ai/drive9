// v1 fuse_test writebuffer rows not yet ported: random writes, gap-fill
// headroom, truncate-extend composition, and EFBIG on the small-file
// path.
package writebuffer

import (
	"bytes"
	"syscall"
	"testing"
)

func newWB(t *testing.T, path string, max, part int64) *WriteBuffer {
	t.Helper()
	return NewWriteBuffer(path, max, part)
}

// v1 TestWriteBuffer_RandomWrite: scattered writes compose into the
// right image with zero-filled gaps.
func TestPortWriteBufferRandomWrite(t *testing.T) {
	wb := newWB(t, "/r.bin", 1<<20, 128)
	if _, err := wb.Write(10, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(2, []byte("B")); err != nil {
		t.Fatal(err)
	}
	got := wb.Bytes()
	want := make([]byte, 11)
	copy(want[2:], "B")
	copy(want[10:], "A")
	if !bytes.Equal(got, want) {
		t.Fatalf("image = %q, want %q", got, want)
	}
}

// v1 TestWriteBuffer_GapFill: a write landing past EOF zero-fills the
// gap.
func TestPortWriteBufferGapFill(t *testing.T) {
	wb := newWB(t, "/g.bin", 1<<20, 128)
	if _, err := wb.Write(5, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	got := wb.Bytes()
	if len(got) != 9 || !bytes.Equal(got[:5], []byte{0, 0, 0, 0, 0}) || string(got[5:]) != "tail" {
		t.Fatalf("image = %q", got)
	}
}

// v1 TestWriteBuffer_TruncateExtendThenSmallWritePreservesLogicalSize:
// truncating larger then writing preserves the logical size.
func TestPortWriteBufferTruncateExtendThenWrite(t *testing.T) {
	wb := newWB(t, "/t.bin", 1<<20, 128)
	if _, err := wb.Write(0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	if err := wb.Truncate(100); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(90, []byte("END")); err != nil {
		t.Fatal(err)
	}
	if wb.Size() != 100 {
		t.Fatalf("size = %d, want 100", wb.Size())
	}
	got := wb.Bytes()
	if len(got) != 100 || string(got[:4]) != "seed" || string(got[90:93]) != "END" {
		// Truncate-extend zero-fills lazily; Bytes() materializes only
		// written regions. The logical size and the two written islands
		// are the contract (v1 LoadMissingParts before Bytes()).
		t.Fatalf("image corrupted: len=%d head=%q tail=%q", len(got), got[:4], got[90:])
	}
}

// v1 TestWriteBuffer_EFBIG: writes past the max budget fail with EFBIG.
func TestPortWriteBufferEFBIG(t *testing.T) {
	wb := newWB(t, "/big.bin", 64, 32)
	if _, err := wb.Write(0, make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(64, []byte("x")); !isEFBIG(err) {
		t.Fatalf("err = %v, want EFBIG", err)
	}
}

func isEFBIG(err error) bool {
	if err == nil {
		return false
	}
	return err == syscall.EFBIG || err.Error() == syscall.EFBIG.Error()
}
