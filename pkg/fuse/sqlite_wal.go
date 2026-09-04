package fuse

import "encoding/binary"

const (
	sqliteWALHeaderSize    = 32
	sqliteWALFormatVersion = 3007000
	// SQLite's magic chooses checksum input word order. Header fields and
	// checksum results themselves stay big-endian on disk.
	sqliteWALMagicBig    = 0x377f0683
	sqliteWALMagicLittle = 0x377f0682
)

// sqliteWALHeader is the validated fixed WAL header used only to prove the
// explicitly configured SQLite generation-reset optimization. It is not a WAL
// frame parser and does not determine transaction recoverability.
type sqliteWALHeader struct {
	raw      [sqliteWALHeaderSize]byte
	pageSize int64
	salt1    uint32
	salt2    uint32
}

func parseSQLiteWALHeader(data []byte) (sqliteWALHeader, bool) {
	if len(data) < sqliteWALHeaderSize {
		return sqliteWALHeader{}, false
	}

	var header sqliteWALHeader
	copy(header.raw[:], data[:sqliteWALHeaderSize])
	magic := binary.BigEndian.Uint32(header.raw[0:4])
	littleEndianChecksum := false
	switch magic {
	case sqliteWALMagicBig:
	case sqliteWALMagicLittle:
		littleEndianChecksum = true
	default:
		return sqliteWALHeader{}, false
	}
	if binary.BigEndian.Uint32(header.raw[4:8]) != sqliteWALFormatVersion {
		return sqliteWALHeader{}, false
	}
	header.pageSize = int64(binary.BigEndian.Uint32(header.raw[8:12]))
	if !sqliteWALPageSizeValid(header.pageSize) {
		return sqliteWALHeader{}, false
	}
	header.salt1 = binary.BigEndian.Uint32(header.raw[16:20])
	header.salt2 = binary.BigEndian.Uint32(header.raw[20:24])
	first, second := sqliteWALChecksum(header.raw[:24], littleEndianChecksum)
	if binary.BigEndian.Uint32(header.raw[24:28]) != first || binary.BigEndian.Uint32(header.raw[28:32]) != second {
		return sqliteWALHeader{}, false
	}
	return header, true
}

func sqliteWALPageSizeValid(size int64) bool {
	return size >= 512 && size <= 65536 && size&(size-1) == 0
}

func (header sqliteWALHeader) saltsDiffer(other sqliteWALHeader) bool {
	return header.salt1 != other.salt1 || header.salt2 != other.salt2
}

func sqliteWALChecksum(data []byte, littleEndian bool) (uint32, uint32) {
	var first, second uint32
	for offset := 0; offset+8 <= len(data); offset += 8 {
		word := binary.BigEndian.Uint32(data[offset : offset+4])
		next := binary.BigEndian.Uint32(data[offset+4 : offset+8])
		if littleEndian {
			word = binary.LittleEndian.Uint32(data[offset : offset+4])
			next = binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		}
		first += word + second
		second += next + first
	}
	return first, second
}
