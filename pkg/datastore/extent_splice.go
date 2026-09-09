package datastore

import (
	"fmt"
	"sort"
)

// SliceRow is one non-overlapping extent covering [FileOff, FileOff+Len).
type SliceRow struct {
	InodeID        string `json:"inode_id"`
	Chunk          int64  `json:"chunk"`
	Seq            int64  `json:"seq,omitempty"`
	FileOff        int64  `json:"file_off"`
	Len            int64  `json:"len"`
	BlockKey       string `json:"block_key"`
	BlockOff       int64  `json:"block_off"`
	BlockLen       int64  `json:"block_len"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	Kind           string `json:"kind"`
	BornGen        int64  `json:"born_gen"`
}

// SliceOp is one declarative write: file interval [FileOff, FileOff+Len) is
// block_key[BlockOff, BlockOff+Len).
type SliceOp struct {
	FileOff        int64  `json:"file_off"`
	Len            int64  `json:"len"`
	BlockKey       string `json:"block_key,omitempty"`
	BlockOff       int64  `json:"block_off"`
	BlockLen       int64  `json:"block_len,omitempty"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	Kind           string `json:"kind,omitempty"`
}

const (
	SliceKindData = "data"
	SliceKindHole = "hole"
)

func (r SliceRow) end() int64 { return r.FileOff + r.Len }

func (op SliceOp) end() int64 { return op.FileOff + op.Len }

func (op SliceOp) isHole() bool {
	return op.Kind == SliceKindHole || (op.Kind == "" && op.BlockKey == "" && op.ChecksumSHA256 == "")
}

func validateOp(op SliceOp) error {
	if op.Len <= 0 {
		return fmt.Errorf("op len must be > 0")
	}
	maxLen := int64(ExtentMaxBlockSize)
	if op.isHole() {
		maxLen = ExtentChunkSize
	}
	if op.Len > maxLen {
		return fmt.Errorf("op len %d exceeds max %d", op.Len, maxLen)
	}
	if op.FileOff < 0 || op.BlockOff < 0 {
		return fmt.Errorf("negative offset")
	}
	if !op.isHole() && op.BlockKey == "" {
		return fmt.Errorf("missing block_key")
	}
	if ChunkOf(op.FileOff) != ChunkOf(op.end()-1) {
		return fmt.Errorf("op crosses chunk boundary")
	}
	if !op.isHole() && op.BlockLen > 0 && op.BlockOff+op.Len > op.BlockLen {
		return fmt.Errorf("block_off+len exceeds block_len")
	}
	return nil
}

// SpliceRows applies ops then optional truncate onto a non-overlapping row set.
func SpliceRows(existing []SliceRow, ops []SliceOp, truncateTo *int64, bornGen int64) ([]SliceRow, error) {
	return spliceRows(existing, ops, truncateTo, bornGen)
}

// RefDelta returns block_key -> refs change from before to after.
func RefDelta(before, after []SliceRow) map[string]int64 {
	return refDelta(before, after)
}

func spliceRows(existing []SliceRow, ops []SliceOp, truncateTo *int64, bornGen int64) ([]SliceRow, error) {
	rows := append([]SliceRow(nil), existing...)
	for _, op := range ops {
		if err := validateOp(op); err != nil {
			return nil, err
		}
		rows = applyOp(rows, op, bornGen)
	}
	if truncateTo != nil {
		if *truncateTo < 0 {
			return nil, fmt.Errorf("truncate_to must be >= 0")
		}
		rows = applyTruncate(rows, *truncateTo)
	}
	sortSliceRows(rows)
	if err := assertNonOverlapping(rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func applyOp(rows []SliceRow, op SliceOp, bornGen int64) []SliceRow {
	a, b := op.FileOff, op.end()
	next := make([]SliceRow, 0, len(rows)+2)
	for _, row := range rows {
		r0, r1 := row.FileOff, row.end()
		if r1 <= a || r0 >= b {
			next = append(next, row)
			continue
		}
		if r0 < a {
			left := row
			left.Len = a - r0
			next = append(next, left)
		}
		if r1 > b {
			right := row
			skip := b - r0
			right.FileOff = b
			right.BlockOff = row.BlockOff + skip
			right.Len = r1 - b
			next = append(next, right)
		}
	}
	kind := SliceKindData
	if op.isHole() {
		kind = SliceKindHole
	}
	next = append(next, SliceRow{
		Chunk:          ChunkOf(op.FileOff),
		FileOff:        op.FileOff,
		Len:            op.Len,
		BlockKey:       op.BlockKey,
		BlockOff:       op.BlockOff,
		BlockLen:       op.BlockLen,
		ChecksumSHA256: op.ChecksumSHA256,
		Kind:           kind,
		BornGen:        bornGen,
	})
	return next
}

func applyTruncate(rows []SliceRow, truncateTo int64) []SliceRow {
	if truncateTo == 0 {
		return nil
	}
	next := make([]SliceRow, 0, len(rows))
	for _, row := range rows {
		if row.FileOff >= truncateTo {
			continue
		}
		if row.end() > truncateTo {
			row.Len = truncateTo - row.FileOff
		}
		next = append(next, row)
	}
	return next
}

func sortSliceRows(rows []SliceRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Chunk != rows[j].Chunk {
			return rows[i].Chunk < rows[j].Chunk
		}
		return rows[i].FileOff < rows[j].FileOff
	})
}

func assertNonOverlapping(rows []SliceRow) error {
	for i := 1; i < len(rows); i++ {
		if rows[i].FileOff < rows[i-1].end() {
			return fmt.Errorf("overlapping slice rows at file_off %d", rows[i].FileOff)
		}
	}
	return nil
}

// FlattenBySeq is JuiceFS chunk read: later seq overlays earlier coverage.
func FlattenBySeq(rows []SliceRow) []SliceRow {
	if len(rows) == 0 {
		return nil
	}
	ordered := append([]SliceRow(nil), rows...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Chunk != ordered[j].Chunk {
			return ordered[i].Chunk < ordered[j].Chunk
		}
		if ordered[i].Seq != ordered[j].Seq {
			return ordered[i].Seq < ordered[j].Seq
		}
		return ordered[i].FileOff < ordered[j].FileOff
	})
	var flat []SliceRow
	for _, row := range ordered {
		op := SliceOp{
			FileOff: row.FileOff, Len: row.Len, BlockKey: row.BlockKey,
			BlockOff: row.BlockOff, BlockLen: row.BlockLen,
			ChecksumSHA256: row.ChecksumSHA256, Kind: row.Kind,
		}
		next, err := spliceRows(flat, []SliceOp{op}, nil, row.BornGen)
		if err != nil {
			flat = applyOp(flat, op, row.BornGen)
			sortSliceRows(flat)
			continue
		}
		flat = next
	}
	return flat
}

func coverageEnd(rows []SliceRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if rows[0].FileOff != 0 {
		return 0, ErrExtentCoverageHole
	}
	end := int64(0)
	for _, row := range rows {
		if row.Len <= 0 {
			return 0, fmt.Errorf("non-positive row len")
		}
		if row.FileOff != end {
			return 0, ErrExtentCoverageHole
		}
		if ChunkOf(row.FileOff) != row.Chunk {
			return 0, fmt.Errorf("row chunk mismatch")
		}
		if ChunkOf(row.FileOff) != ChunkOf(row.end()-1) {
			return 0, fmt.Errorf("row crosses chunk")
		}
		end = row.end()
	}
	return end, nil
}

func refDelta(before, after []SliceRow) map[string]int64 {
	delta := make(map[string]int64)
	for key := range uniqueBlockKeys(before) {
		delta[key]--
	}
	for key := range uniqueBlockKeys(after) {
		delta[key]++
	}
	for k, v := range delta {
		if v == 0 {
			delete(delta, k)
		}
	}
	return delta
}

func uniqueBlockKeys(rows []SliceRow) map[string]struct{} {
	out := make(map[string]struct{})
	for _, row := range rows {
		if row.BlockKey == "" {
			continue
		}
		out[row.BlockKey] = struct{}{}
	}
	return out
}

// SplitPrepareRange cuts a dirty range on 4MiB and 64MiB chunk boundaries.
func SplitPrepareRange(fileOff, length int64) []SliceOp {
	if length <= 0 {
		return nil
	}
	var out []SliceOp
	off := fileOff
	end := fileOff + length
	for off < end {
		chunkEnd := (off &^ (ExtentChunkSize - 1)) + ExtentChunkSize
		blockEnd := off + ExtentMaxBlockSize
		next := end
		if chunkEnd < next {
			next = chunkEnd
		}
		if blockEnd < next {
			next = blockEnd
		}
		out = append(out, SliceOp{FileOff: off, Len: next - off})
		off = next
	}
	return out
}

// SplitHoleRange cuts only on 64MiB chunk boundaries (holes are not 4MiB objects).
func SplitHoleRange(fileOff, length int64) []SliceOp {
	if length <= 0 {
		return nil
	}
	var out []SliceOp
	off := fileOff
	end := fileOff + length
	for off < end {
		chunkEnd := (off &^ (ExtentChunkSize - 1)) + ExtentChunkSize
		next := end
		if chunkEnd < next {
			next = chunkEnd
		}
		out = append(out, SliceOp{FileOff: off, Len: next - off, Kind: SliceKindHole})
		off = next
	}
	return out
}

// TruncateHoleOps is JuiceFS doTruncate: hole-cover [min(old,new), max(old,new)).
func TruncateHoleOps(oldSize, newSize int64) []SliceOp {
	if newSize < 0 || oldSize < 0 || newSize == oldSize {
		return nil
	}
	left, right := oldSize, newSize
	if left > right {
		left, right = right, left
	}
	return SplitHoleRange(left, right-left)
}
