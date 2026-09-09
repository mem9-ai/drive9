package datastore

import (
	"encoding/binary"
	"fmt"
)

// JuiceFS chunk.slices is one append-only blob per (inode, indx). Drive9
// keeps block_key strings so records are length-prefixed, but the CAS is the
// same: origin prefix of the blob, compacted records, concurrent suffix.

const (
	chunkSliceKindData byte = 0
	chunkSliceKindHole byte = 1
)

func marshalChunkSlice(op SliceOp) ([]byte, error) {
	if op.Len <= 0 {
		return nil, fmt.Errorf("chunk slice len must be > 0")
	}
	if len(op.BlockKey) > 65535 || len(op.ChecksumSHA256) > 65535 {
		return nil, fmt.Errorf("chunk slice key too long")
	}
	kind := chunkSliceKindData
	if op.isHole() {
		kind = chunkSliceKindHole
	}
	n := 1 + 8 + 8 + 8 + 8 + 2 + len(op.BlockKey) + 2 + len(op.ChecksumSHA256)
	buf := make([]byte, n)
	buf[0] = kind
	binary.BigEndian.PutUint64(buf[1:9], uint64(op.FileOff))
	binary.BigEndian.PutUint64(buf[9:17], uint64(op.Len))
	binary.BigEndian.PutUint64(buf[17:25], uint64(op.BlockOff))
	binary.BigEndian.PutUint64(buf[25:33], uint64(op.BlockLen))
	binary.BigEndian.PutUint16(buf[33:35], uint16(len(op.BlockKey)))
	copy(buf[35:], op.BlockKey)
	off := 35 + len(op.BlockKey)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(op.ChecksumSHA256)))
	copy(buf[off+2:], op.ChecksumSHA256)
	return buf, nil
}

func marshalChunkSlices(ops []SliceOp) ([]byte, error) {
	var out []byte
	for _, op := range ops {
		rec, err := marshalChunkSlice(op)
		if err != nil {
			return nil, err
		}
		out = append(out, rec...)
	}
	return out, nil
}

func decodeChunkSliceBlob(inodeID string, chunk int64, buf []byte) ([]SliceRow, error) {
	var out []SliceRow
	pos := 0
	seq := int64(1)
	for pos < len(buf) {
		row, n, err := decodeChunkSliceRecord(buf[pos:])
		if err != nil {
			return nil, err
		}
		row.InodeID = inodeID
		row.Chunk = chunk
		row.Seq = seq
		out = append(out, row)
		pos += n
		seq++
	}
	return out, nil
}

func decodeChunkSliceRecord(buf []byte) (SliceRow, int, error) {
	if len(buf) < 35 {
		return SliceRow{}, 0, fmt.Errorf("truncated chunk slice header")
	}
	kind := buf[0]
	fileOff := int64(binary.BigEndian.Uint64(buf[1:9]))
	length := int64(binary.BigEndian.Uint64(buf[9:17]))
	blockOff := int64(binary.BigEndian.Uint64(buf[17:25]))
	blockLen := int64(binary.BigEndian.Uint64(buf[25:33]))
	keyLen := int(binary.BigEndian.Uint16(buf[33:35]))
	if len(buf) < 35+keyLen+2 {
		return SliceRow{}, 0, fmt.Errorf("truncated chunk slice key")
	}
	key := string(buf[35 : 35+keyLen])
	off := 35 + keyLen
	csumLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
	if len(buf) < off+2+csumLen {
		return SliceRow{}, 0, fmt.Errorf("truncated chunk slice checksum")
	}
	csum := string(buf[off+2 : off+2+csumLen])
	rowKind := SliceKindData
	if kind == chunkSliceKindHole {
		rowKind = SliceKindHole
	}
	n := off + 2 + csumLen
	return SliceRow{
		FileOff: fileOff, Len: length, BlockKey: key, BlockOff: blockOff,
		BlockLen: blockLen, ChecksumSHA256: csum, Kind: rowKind,
	}, n, nil
}

func countChunkSliceRecords(buf []byte) (int, error) {
	n := 0
	pos := 0
	for pos < len(buf) {
		_, rec, err := decodeChunkSliceRecord(buf[pos:])
		if err != nil {
			return 0, err
		}
		pos += rec
		n++
	}
	return n, nil
}

func chunkSlicePrefixSize(buf []byte, records int) (int, error) {
	if records < 0 {
		return 0, fmt.Errorf("negative skip")
	}
	pos := 0
	for i := 0; i < records; i++ {
		if pos >= len(buf) {
			return 0, fmt.Errorf("skip past chunk blob")
		}
		_, rec, err := decodeChunkSliceRecord(buf[pos:])
		if err != nil {
			return 0, err
		}
		pos += rec
	}
	return pos, nil
}

func sliceRowToOp(row SliceRow) SliceOp {
	return SliceOp{
		FileOff: row.FileOff, Len: row.Len, BlockKey: row.BlockKey,
		BlockOff: row.BlockOff, BlockLen: row.BlockLen,
		ChecksumSHA256: row.ChecksumSHA256, Kind: row.Kind,
	}
}

// EncodeSliceRows is the JuiceFS origin blob for compact CAS.
func EncodeSliceRows(rows []SliceRow) ([]byte, error) {
	return marshalChunkSlices(sliceRowsToOps(rows))
}

func sliceRowsToOps(rows []SliceRow) []SliceOp {
	out := make([]SliceOp, 0, len(rows))
	for _, row := range rows {
		out = append(out, sliceRowToOp(row))
	}
	return out
}

// CompactChunkBlob is JuiceFS doCompactChunk: if current still starts with
// origin, keep skipped leading records, replace the origin tail with ops,
// and keep bytes appended after origin (concurrent meta.Write).
func CompactChunkBlob(current, origin []byte, skipped int, ops []SliceOp) ([]byte, error) {
	if len(origin) == 0 || len(current) < len(origin) || string(current[:len(origin)]) != string(origin) {
		return nil, ErrCompactAborted
	}
	skipBytes, err := chunkSlicePrefixSize(origin, skipped)
	if err != nil {
		return nil, err
	}
	mid, err := marshalChunkSlices(ops)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, skipBytes+len(mid)+len(current)-len(origin))
	out = append(out, current[:skipBytes]...)
	out = append(out, mid...)
	out = append(out, current[len(origin):]...)
	return out, nil
}
