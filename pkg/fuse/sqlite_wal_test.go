package fuse

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestParseSQLiteWALHeaderSQLiteFixture(t *testing.T) {
	// Captured from Python's bundled SQLite after WAL mode, synchronous=FULL,
	// CREATE TABLE, and one INSERT. This prevents the checksum test fixtures
	// below from only validating a self-consistent implementation.
	raw, err := hex.DecodeString("377f0682002de21800001000000000008c2f2fa71946e8ab7c16329fdfe6505d")
	if err != nil {
		t.Fatal(err)
	}
	header, ok := parseSQLiteWALHeader(raw)
	if !ok {
		t.Fatal("SQLite-produced WAL header did not parse")
	}
	if header.pageSize != 4096 {
		t.Fatalf("page size = %d, want 4096", header.pageSize)
	}
}

func TestParseSQLiteWALHeader(t *testing.T) {
	bigEndian := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 0x11223344, 0x55667788)
	littleEndian := makeSQLiteWALHeaderForTest(t, sqliteWALMagicLittle, 65536, 0x99aabbcc, 0xddeeff00)

	tests := []struct {
		name string
		data []byte
		ok   bool
		page int64
		s1   uint32
		s2   uint32
	}{
		{name: "big endian checksum", data: bigEndian, ok: true, page: 4096, s1: 0x11223344, s2: 0x55667788},
		{name: "little endian checksum", data: littleEndian, ok: true, page: 65536, s1: 0x99aabbcc, s2: 0xddeeff00},
		{name: "short", data: bigEndian[:31]},
		{name: "wrong magic", data: corruptSQLiteWALHeader(bigEndian, 0, 0xff)},
		{name: "wrong version", data: corruptSQLiteWALHeader(bigEndian, 7, 0xff)},
		{name: "wrong page size", data: corruptSQLiteWALHeader(bigEndian, 11, 0x01)},
		{name: "wrong checksum", data: corruptSQLiteWALHeader(bigEndian, 31, 0xff)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header, ok := parseSQLiteWALHeader(test.data)
			if ok != test.ok {
				t.Fatalf("parseSQLiteWALHeader() ok = %t, want %t", ok, test.ok)
			}
			if !test.ok {
				return
			}
			if header.pageSize != test.page || header.salt1 != test.s1 || header.salt2 != test.s2 {
				t.Fatalf("header = %+v, want page/salts %d/%08x/%08x", header, test.page, test.s1, test.s2)
			}
		})
	}
}

func TestSQLiteWALHeaderSaltChange(t *testing.T) {
	first, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, 0x377f0682, 4096, 1, 2))
	if !ok {
		t.Fatal("first header did not parse")
	}
	same, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, 0x377f0682, 4096, 1, 2))
	if !ok {
		t.Fatal("same-salt header did not parse")
	}
	changed, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, 0x377f0682, 4096, 3, 4))
	if !ok {
		t.Fatal("changed-salt header did not parse")
	}
	if first.saltsDiffer(same) {
		t.Fatal("same salts reported as changed")
	}
	if !first.saltsDiffer(changed) {
		t.Fatal("changed salts reported as unchanged")
	}
}

func makeSQLiteWALHeaderForTest(t *testing.T, magic, pageSize, salt1, salt2 uint32) []byte {
	t.Helper()
	header := make([]byte, sqliteWALHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], magic)
	binary.BigEndian.PutUint32(header[4:8], 3007000)
	binary.BigEndian.PutUint32(header[8:12], pageSize)
	binary.BigEndian.PutUint32(header[12:16], 7)
	binary.BigEndian.PutUint32(header[16:20], salt1)
	binary.BigEndian.PutUint32(header[20:24], salt2)
	first, second := sqliteWALChecksumForTest(header[:24], magic == sqliteWALMagicLittle)
	binary.BigEndian.PutUint32(header[24:28], first)
	binary.BigEndian.PutUint32(header[28:32], second)
	return header
}

func sqliteWALChecksumForTest(data []byte, littleEndian bool) (uint32, uint32) {
	var first, second uint32
	for offset := 0; offset < len(data); offset += 8 {
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

func corruptSQLiteWALHeader(header []byte, offset int, xor byte) []byte {
	copy := append([]byte(nil), header...)
	copy[offset] ^= xor
	return copy
}
