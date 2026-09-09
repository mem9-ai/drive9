package datastore

import (
	"bytes"
	"testing"
)

func TestMarshalChunkSliceRoundTrip(t *testing.T) {
	ops := []SliceOp{
		{FileOff: 0, Len: 4096, BlockKey: "blocks/a", BlockOff: 0, BlockLen: 4096, ChecksumSHA256: "aa", Kind: SliceKindData},
		{FileOff: 4096, Len: 8192, Kind: SliceKindHole},
		{FileOff: 12288, Len: 4, BlockKey: "blocks/c", BlockOff: 12, BlockLen: 16, ChecksumSHA256: "cc"},
	}
	buf, err := marshalChunkSlices(ops)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := decodeChunkSliceBlob("ino", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3", len(rows))
	}
	if rows[0].BlockKey != "blocks/a" || rows[0].Seq != 1 || rows[1].Kind != SliceKindHole || rows[2].BlockOff != 12 {
		t.Fatalf("decoded=%+v", rows)
	}
	again, err := EncodeSliceRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, again) {
		t.Fatalf("marshal(decode) diverged: %d vs %d", len(buf), len(again))
	}
	n, err := countChunkSliceRecords(buf)
	if err != nil || n != 3 {
		t.Fatalf("count=%d err=%v", n, err)
	}
}

func TestShouldCompactOnWriteJuiceFSKick(t *testing.T) {
	// JuiceFS meta.Write: numSlices%100==99 || numSlices>350; sync at >=2500.
	cases := []struct {
		n    int
		kick bool
		sync bool
	}{
		{5, false, false},
		{32, false, false},
		{98, false, false},
		{99, true, false},
		{100, false, false},
		{199, true, false},
		{350, false, false},
		{351, true, false},
		{2499, true, false},
		{2500, true, true},
	}
	for _, tc := range cases {
		if got := ShouldCompactOnWrite(tc.n); got != tc.kick {
			t.Errorf("ShouldCompactOnWrite(%d)=%v want %v", tc.n, got, tc.kick)
		}
		if got := ShouldSyncCompactOnWrite(tc.n); got != tc.sync {
			t.Errorf("ShouldSyncCompactOnWrite(%d)=%v want %v", tc.n, got, tc.sync)
		}
	}
}

func TestCompactChunkBlobKeepsConcurrentSuffix(t *testing.T) {
	// JuiceFS doCompactChunk: origin prefix CAS, skipped head, compacted
	// middle, writes that arrived during copy stay after origin.
	a := SliceOp{FileOff: 0, Len: 4096, BlockKey: "a", BlockLen: 4096}
	b := SliceOp{FileOff: 0, Len: 4096, BlockKey: "b", BlockLen: 4096}
	c := SliceOp{FileOff: 4096, Len: 4096, BlockKey: "c", BlockLen: 4096}
	d := SliceOp{FileOff: 8192, Len: 4096, BlockKey: "d", BlockLen: 4096}
	origin, err := marshalChunkSlices([]SliceOp{a, b, c})
	if err != nil {
		t.Fatal(err)
	}
	extra, err := marshalChunkSlice(d)
	if err != nil {
		t.Fatal(err)
	}
	current := append(append([]byte{}, origin...), extra...)
	e := SliceOp{FileOff: 0, Len: 8192, BlockKey: "e", BlockLen: 8192}
	got, err := CompactChunkBlob(current, origin, 0, []SliceOp{e})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := decodeChunkSliceBlob("", 0, got)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].BlockKey != "e" || rows[1].BlockKey != "d" {
		t.Fatalf("compacted=%+v want e then concurrent d", rows)
	}
	skipA, err := CompactChunkBlob(current, origin, 1, []SliceOp{e})
	if err != nil {
		t.Fatal(err)
	}
	skipRows, err := decodeChunkSliceBlob("", 0, skipA)
	if err != nil {
		t.Fatal(err)
	}
	if len(skipRows) != 3 || skipRows[0].BlockKey != "a" || skipRows[1].BlockKey != "e" || skipRows[2].BlockKey != "d" {
		t.Fatalf("skipped compact=%+v want a,e,d", skipRows)
	}
}

func TestCompactChunkBlobAbortsWhenOriginMoved(t *testing.T) {
	a := SliceOp{FileOff: 0, Len: 4, BlockKey: "a", BlockLen: 4}
	b := SliceOp{FileOff: 0, Len: 4, BlockKey: "b", BlockLen: 4}
	origin, err := marshalChunkSlice(a)
	if err != nil {
		t.Fatal(err)
	}
	other, err := marshalChunkSlice(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CompactChunkBlob(other, origin, 0, []SliceOp{a}); err != ErrCompactAborted {
		t.Fatalf("err=%v want ErrCompactAborted", err)
	}
	if !bytes.Equal(origin, origin) {
		t.Fatal("sanity")
	}
}
