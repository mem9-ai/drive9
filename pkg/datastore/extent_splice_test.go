package datastore

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestSliceOpJSONRoundTrip(t *testing.T) {
	raw := []byte(`{"file_off":8,"len":13,"block_key":"blocks/x","block_off":0,"checksum_sha256":"abc"}`)
	var op SliceOp
	if err := json.Unmarshal(raw, &op); err != nil {
		t.Fatal(err)
	}
	if op.FileOff != 8 || op.Len != 13 || op.BlockKey != "blocks/x" || op.ChecksumSHA256 != "abc" {
		t.Fatalf("decoded=%+v", op)
	}
	out, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"file_off", "len", "block_key", "block_off", "checksum_sha256"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing %s in %s", key, out)
		}
	}
}

func TestSkipSomeDoesNotSkipLargeSlicePunchedByOverlays(t *testing.T) {
	// JuiceFS skipSome: overlays punch the first large slice, so it is not
	// an identity prefix. Compacting only the overlays would bake zeros
	// over page 8 (28672) still covered by the large slice.
	rows := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 2732032, BlockKey: "big", BlockOff: 0, Kind: SliceKindData},
		{Chunk: 0, Seq: 2, FileOff: 8192, Len: 8192, BlockKey: "ov1", BlockOff: 0, Kind: SliceKindData},
		{Chunk: 0, Seq: 3, FileOff: 40960, Len: 4096, BlockKey: "ov2", BlockOff: 0, Kind: SliceKindData},
	}
	if n := SkipSome(rows); n != 0 {
		t.Fatalf("SkipSome=%d want 0", n)
	}
	if CompactRangeOverlapsSkipped(rows[:1], rows[1:]) != true {
		t.Fatal("overlay compact range overlaps the large head")
	}
	flat := FlattenBySeq(rows)
	var hit SliceRow
	for _, row := range flat {
		if row.FileOff <= 28672 && row.FileOff+row.Len > 28672 {
			hit = row
			break
		}
	}
	if hit.BlockKey != "big" {
		t.Fatalf("page 8 flatten=%+v want big remnant, not a hole", hit)
	}
}

func TestPackCompactRunsBakesInteriorHoles(t *testing.T) {
	// JuiceFS compactChunk skips leading/trailing holes, then Compact()
	// writes interior holes as zeros in one object.
	rows := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 4096, BlockKey: "a", Kind: SliceKindData},
		{Chunk: 0, Seq: 2, FileOff: 4096, Len: 12288, Kind: SliceKindHole},
		{Chunk: 0, Seq: 3, FileOff: 16384, Len: 4096, BlockKey: "b", Kind: SliceKindData},
	}
	runs := PackCompactRuns(rows)
	if len(runs) != 1 {
		t.Fatalf("runs=%d want 1 (interior hole baked)", len(runs))
	}
	if len(runs[0]) != 3 {
		t.Fatalf("run=%+v want data,hole,data", runs[0])
	}
	if runs[0][1].Kind != SliceKindHole {
		t.Fatalf("interior hole missing: %+v", runs[0])
	}
	start, end := runs[0][0].FileOff, runs[0][len(runs[0])-1].FileOff+runs[0][len(runs[0])-1].Len
	if start != 0 || end != 20480 {
		t.Fatalf("baked span [%d,%d) want [0,20480)", start, end)
	}
}

func TestPackCompactRunsSkipsTrailingGrowHole(t *testing.T) {
	rows := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 4096, BlockKey: "a", Kind: SliceKindData},
		{Chunk: 0, Seq: 2, FileOff: 4096, Len: 12288, Kind: SliceKindHole},
	}
	runs := PackCompactRuns(rows)
	for _, run := range runs {
		for _, row := range run {
			if row.Kind == SliceKindHole {
				t.Fatalf("trailing grow hole must not be packed: %+v", row)
			}
		}
	}
}

func TestSpliceGrowTruncateDoesNotRequireHoleOps(t *testing.T) {
	rows, err := SpliceRows(nil, []SliceOp{{
		FileOff: 0, Len: 8, BlockKey: "a", BlockOff: 0, BlockLen: 8, ChecksumSHA256: "aa",
	}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	to := int64(64)
	grown, err := SpliceRows(rows, nil, &to, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(grown) != 1 || grown[0].Len != 8 {
		t.Fatalf("grow must keep data rows only, got %+v", grown)
	}
}

func TestPackCompactRunsSameOffsetCollapsesToLaterSeq(t *testing.T) {
	rows := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 8, BlockKey: "old"},
		{Chunk: 0, Seq: 2, FileOff: 0, Len: 8, BlockKey: "new"},
	}
	runs := PackCompactRuns(rows)
	if len(runs) != 1 || len(runs[0]) != 1 || runs[0][0].BlockKey != "new" {
		t.Fatalf("same-offset overlap must compact to later seq, runs=%+v", runs)
	}
}

func TestPackCompactRunsOverlappingLastWriteWins(t *testing.T) {
	rows := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 8, BlockKey: "old"},
		{Chunk: 0, Seq: 2, FileOff: 2, Len: 4, BlockKey: "new"},
	}
	runs := PackCompactRuns(rows)
	if len(runs) != 1 {
		t.Fatalf("flattened overlapping rows must pack, runs=%d", len(runs))
	}
	var keys []string
	var off int64
	for _, row := range runs[0] {
		if row.FileOff != off {
			t.Fatalf("run must be contiguous, off=%d row=%+v", off, row)
		}
		keys = append(keys, row.BlockKey)
		off += row.Len
	}
	if off != 8 {
		t.Fatalf("coverage end=%d want 8", off)
	}
	if len(keys) < 2 || keys[0] != "old" || keys[1] != "new" {
		t.Fatalf("visible keys=%v want old,new,...", keys)
	}
}

func TestPackCompactRunsMergesAdjacentSmallRows(t *testing.T) {
	rows := []SliceRow{
		{FileOff: 0, Len: 8, BlockKey: "a"},
		{FileOff: 8, Len: 8, BlockKey: "b"},
		{FileOff: 16, Len: ExtentMaxBlockSize, BlockKey: "big"},
		{FileOff: 16 + ExtentMaxBlockSize, Len: 4, BlockKey: "c"},
	}
	runs := PackCompactRuns(rows)
	if len(runs) != 1 {
		t.Fatalf("runs=%d want 1", len(runs))
	}
	if len(runs[0]) != 2 || runs[0][0].BlockKey != "a" || runs[0][1].BlockKey != "b" {
		t.Fatalf("run=%+v", runs[0])
	}
}

func TestTruncateHoleOpsCoversGrownAndShrunkRange(t *testing.T) {
	if ops := TruncateHoleOps(8, 8); len(ops) != 0 {
		t.Fatalf("same size: %v", ops)
	}
	grow := TruncateHoleOps(8, 24)
	if len(grow) != 1 || grow[0].FileOff != 8 || grow[0].Len != 16 || grow[0].Kind != SliceKindHole {
		t.Fatalf("grow holes=%+v", grow)
	}
	shrink := TruncateHoleOps(24, 8)
	if len(shrink) != 1 || shrink[0].FileOff != 8 || shrink[0].Len != 16 || shrink[0].Kind != SliceKindHole {
		t.Fatalf("shrink holes=%+v", shrink)
	}
}

func TestSameChunkPrefixAllowsConcurrentAppend(t *testing.T) {
	prefix := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 8, BlockKey: "a", BlockOff: 0},
		{Chunk: 0, Seq: 2, FileOff: 0, Len: 4, BlockKey: "b", BlockOff: 0},
	}
	current := append(append([]SliceRow{}, prefix...), SliceRow{
		Chunk: 0, Seq: 3, FileOff: 2, Len: 4, BlockKey: "c", BlockOff: 0,
	})
	if !sameChunkPrefix(current, prefix, 0) {
		t.Fatal("JuiceFS compact CAS must accept a longer suffix")
	}
	if sameChunkSnapshot(current, prefix, 0) {
		t.Fatal("full snapshot match must fail when a write arrived")
	}
	changed := append([]SliceRow{}, prefix...)
	changed[1].BlockKey = "other"
	if sameChunkPrefix(changed, prefix, 0) {
		t.Fatal("changed prefix must abort compact")
	}
}

func TestFlattenBySeqLaterWriteWins(t *testing.T) {
	rows := []SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 8, BlockKey: "old"},
		{Chunk: 0, Seq: 2, FileOff: 2, Len: 4, BlockKey: "new"},
	}
	flat := FlattenBySeq(rows)
	if len(flat) != 3 {
		t.Fatalf("flat=%+v want 3 rows (left, overlay, right)", flat)
	}
	if flat[0].BlockKey != "old" || flat[0].FileOff != 0 || flat[0].Len != 2 {
		t.Fatalf("left=%+v", flat[0])
	}
	if flat[1].BlockKey != "new" || flat[1].FileOff != 2 || flat[1].Len != 4 {
		t.Fatalf("mid=%+v", flat[1])
	}
	if flat[2].BlockKey != "old" || flat[2].FileOff != 6 || flat[2].Len != 2 {
		t.Fatalf("right=%+v", flat[2])
	}
}

func TestRefDeltaCountsUniqueBlockKeys(t *testing.T) {
	before := []SliceRow{{
		FileOff: 0, Len: 100, BlockKey: "a", BlockOff: 0, BlockLen: 100,
	}}
	after, err := SpliceRows(before, []SliceOp{{
		FileOff: 40, Len: 20, BlockKey: "b", BlockOff: 0, BlockLen: 20,
	}}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	delta := RefDelta(before, after)
	if delta["a"] != 0 {
		t.Fatalf("block a delta=%d want 0 (remnants still reference a)", delta["a"])
	}
	if delta["b"] != 1 {
		t.Fatalf("block b delta=%d want 1", delta["b"])
	}
	after2, err := SpliceRows(after, []SliceOp{{
		FileOff: 0, Len: 100, BlockKey: "c", BlockOff: 0, BlockLen: 100,
	}}, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	delta = RefDelta(after, after2)
	if delta["a"] != -1 {
		t.Fatalf("overwrite all remnants: a delta=%d want -1", delta["a"])
	}
	if delta["b"] != -1 {
		t.Fatalf("overwrite all remnants: b delta=%d want -1", delta["b"])
	}
	if delta["c"] != 1 {
		t.Fatalf("overwrite all remnants: c delta=%d want 1", delta["c"])
	}
}

func TestSpliceAppendAndOverwrite(t *testing.T) {
	var rows []SliceRow
	var err error
	rows, err = SpliceRows(rows, []SliceOp{{
		FileOff: 0, Len: 10, BlockKey: "a", BlockOff: 0, BlockLen: 10, ChecksumSHA256: "aa",
	}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	end, err := coverageEnd(rows)
	if err != nil || end != 10 {
		t.Fatalf("coverage=%d err=%v", end, err)
	}
	rows, err = SpliceRows(rows, []SliceOp{{
		FileOff: 10, Len: 5, BlockKey: "b", BlockOff: 0, BlockLen: 5, ChecksumSHA256: "bb",
	}}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	end, err = coverageEnd(rows)
	if err != nil || end != 15 {
		t.Fatalf("coverage=%d err=%v rows=%+v", end, err, rows)
	}
	rows, err = SpliceRows(rows, []SliceOp{{
		FileOff: 3, Len: 4, BlockKey: "c", BlockOff: 0, BlockLen: 4, ChecksumSHA256: "cc",
	}}, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	end, err = coverageEnd(rows)
	if err != nil || end != 15 {
		t.Fatalf("overwrite coverage=%d err=%v rows=%+v", end, err, rows)
	}
	if len(rows) != 4 {
		t.Fatalf("want 4 rows after middle overwrite, got %d: %+v", len(rows), rows)
	}
	if rows[0].Len != 3 || rows[0].BlockKey != "a" {
		t.Fatalf("left remnant = %+v", rows[0])
	}
	if rows[1].BlockKey != "c" || rows[1].FileOff != 3 {
		t.Fatalf("middle = %+v", rows[1])
	}
	if rows[2].FileOff != 7 || rows[2].BlockKey != "a" || rows[2].Len != 3 {
		t.Fatalf("right remnant of a = %+v", rows[2])
	}
	if rows[3].FileOff != 10 || rows[3].BlockKey != "b" {
		t.Fatalf("tail = %+v", rows[3])
	}
}

func TestSpliceTruncateGrowViaHoles(t *testing.T) {
	rows, err := SpliceRows(nil, []SliceOp{{
		FileOff: 0, Len: 8, BlockKey: "a", BlockOff: 0, BlockLen: 8,
	}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	to := int64(20)
	holes := SplitHoleRange(8, 12)
	rows, err = SpliceRows(rows, holes, &to, 2)
	if err != nil {
		t.Fatal(err)
	}
	end, err := coverageEnd(rows)
	if err != nil || end != 20 {
		t.Fatalf("coverage=%d err=%v rows=%+v", end, err, rows)
	}
	if rows[1].Kind != SliceKindHole {
		t.Fatalf("grow row = %+v want hole", rows[1])
	}
}

func TestSpliceTruncate(t *testing.T) {
	rows, err := SpliceRows(nil, []SliceOp{{
		FileOff: 0, Len: 20, BlockKey: "a", BlockOff: 0, BlockLen: 20,
	}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	to := int64(8)
	rows, err = SpliceRows(rows, nil, &to, 2)
	if err != nil {
		t.Fatal(err)
	}
	end, err := coverageEnd(rows)
	if err != nil || end != 8 {
		t.Fatalf("truncate coverage=%d err=%v", end, err)
	}
	if len(rows) != 1 || rows[0].Len != 8 || rows[0].BlockKey != "a" {
		t.Fatalf("truncated row = %+v", rows)
	}
}

func TestSpliceOracle(t *testing.T) {
	model := make([]byte, 0, 256)
	var rows []SliceRow
	apply := func(off int, data []byte) {
		end := off + len(data)
		if end > len(model) {
			n := make([]byte, end)
			copy(n, model)
			model = n
		}
		copy(model[off:], data)
		key := string(rune('a' + len(rows)%26))
		var err error
		rows, err = SpliceRows(rows, []SliceOp{{
			FileOff: int64(off), Len: int64(len(data)), BlockKey: key, BlockOff: 0, BlockLen: int64(len(data)),
		}}, nil, 1)
		if err != nil {
			t.Fatalf("splice: %v", err)
		}
		gotEnd, err := coverageEnd(rows)
		if err != nil {
			t.Fatalf("coverage: %v", err)
		}
		if gotEnd != int64(len(model)) {
			t.Fatalf("size %d vs model %d", gotEnd, len(model))
		}
	}
	apply(0, bytes.Repeat([]byte("hello"), 3))
	apply(5, []byte("XXXX"))
	apply(int(len(model)), []byte("tail"))
	to := int64(10)
	var err error
	rows, err = SpliceRows(rows, nil, &to, 2)
	if err != nil {
		t.Fatal(err)
	}
	model = model[:10]
	end, err := coverageEnd(rows)
	if err != nil || end != 10 {
		t.Fatalf("after truncate coverage=%d err=%v", end, err)
	}
}

func TestResolveContentLayout(t *testing.T) {
	layout, err := ResolveContentLayout("", "/x.db-wal")
	if err != nil || layout != ContentLayoutSingle {
		t.Fatalf("no explicit layout = %q %v, want single (profile owns defaults)", layout, err)
	}
	layout, err = ResolveContentLayout(ContentLayoutSingle, "/x.db-wal")
	if err != nil || layout != ContentLayoutSingle {
		t.Fatalf("explicit single wins = %q %v", layout, err)
	}
	layout, err = ResolveContentLayout(ContentLayoutExtent, "/x.txt")
	if err != nil || layout != ContentLayoutExtent {
		t.Fatalf("explicit extent = %q %v", layout, err)
	}
	layout, err = ResolveContentLayout(ContentLayoutAppendLog, "/x.db-wal")
	if err != nil || layout != ContentLayoutAppendLog {
		t.Fatalf("explicit append_log = %q %v", layout, err)
	}
}

func TestSplitPrepareRange(t *testing.T) {
	parts := SplitPrepareRange(ExtentChunkSize-100, 200)
	if len(parts) != 2 {
		t.Fatalf("chunk split got %d parts: %+v", len(parts), parts)
	}
	if parts[0].Len != 100 || parts[1].FileOff != ExtentChunkSize {
		t.Fatalf("parts=%+v", parts)
	}
}
