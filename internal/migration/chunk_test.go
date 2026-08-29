package migration

import (
	"bytes"
	"strings"
	"testing"
)

func TestChunkRoundTripAndDescriptorValidation(t *testing.T) {
	writer, err := newChunkWriter(recordSource)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/a", "/b"} {
		record := generationRecord{
			Key: path,
			Source: &sourceGenerationRecord{
				Path: path, Kind: EntryRegular, Size: 1, ChecksumSHA256: strings.Repeat("a", 64),
			},
		}
		if err := writer.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	body, descriptor, err := writer.Close("source-000001")
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.RecordCount != 2 || descriptor.FirstKey != "/a" || descriptor.LastKey != "/b" ||
		descriptor.PayloadBytes != int64(len(body)) || len(descriptor.ChecksumSHA256) != 64 {
		t.Fatalf("descriptor = %+v body=%d", descriptor, len(body))
	}

	reader, err := newChunkReader(body, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"/a", "/b"} {
		record, ok, err := reader.Next()
		if err != nil || !ok || record.Key != want || record.Source == nil || record.Source.Path != want {
			t.Fatalf("record=%+v ok=%t err=%v want=%q", record, ok, err, want)
		}
	}
	if record, ok, err := reader.Next(); err != nil || ok || record.Key != "" {
		t.Fatalf("terminal record=%+v ok=%t err=%v", record, ok, err)
	}
}

func TestChunkRejectsInvalidRecordsAndPayloads(t *testing.T) {
	t.Run("out of order", func(t *testing.T) {
		writer, err := newChunkWriter(recordSource)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/b", "/a"} {
			err = writer.Write(generationRecord{Key: path, Source: &sourceGenerationRecord{Path: path, Kind: EntryDirectory}})
			if path == "/b" && err != nil {
				t.Fatal(err)
			}
		}
		if err == nil {
			t.Fatal("out-of-order record accepted")
		}
	})

	t.Run("wrong payload kind", func(t *testing.T) {
		writer, err := newChunkWriter(recordSource)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Write(generationRecord{Key: "/a", Target: &targetGenerationRecord{Path: "/a"}}); err == nil {
			t.Fatal("wrong record kind accepted")
		}
	})

	body, descriptor := testChunk(t)
	for _, tc := range []struct {
		name   string
		mutate func([]byte, *chunkDescriptor) []byte
	}{
		{name: "checksum", mutate: func(body []byte, descriptor *chunkDescriptor) []byte {
			descriptor.ChecksumSHA256 = strings.Repeat("f", 64)
			return body
		}},
		{name: "size", mutate: func(body []byte, descriptor *chunkDescriptor) []byte {
			descriptor.PayloadBytes++
			return body
		}},
		{name: "truncated", mutate: func(body []byte, _ *chunkDescriptor) []byte {
			return body[:len(body)-1]
		}},
		{name: "trailing", mutate: func(body []byte, descriptor *chunkDescriptor) []byte {
			body = append(append([]byte(nil), body...), []byte("trailing")...)
			descriptor.PayloadBytes = int64(len(body))
			descriptor.ChecksumSHA256 = checksumHex(body)
			return body
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copyDescriptor := descriptor
			copyBody := tc.mutate(append([]byte(nil), body...), &copyDescriptor)
			reader, err := newChunkReader(copyBody, copyDescriptor)
			if err == nil {
				_, _, err = reader.Next()
				if err == nil {
					_, _, err = reader.Next()
				}
			}
			if err == nil {
				t.Fatal("invalid chunk accepted")
			}
		})
	}
}

func TestChunkRejectsOversizedRecord(t *testing.T) {
	writer, err := newChunkWriter(recordSource)
	if err != nil {
		t.Fatal(err)
	}
	record := generationRecord{
		Key: "/large",
		Source: &sourceGenerationRecord{
			Path: "/large", Kind: EntrySymlink, LinkTarget: string(bytes.Repeat([]byte{'x'}, maxGenerationRecordBytes+1)),
		},
	}
	if err := writer.Write(record); err == nil {
		t.Fatal("oversized record accepted")
	}
}

func testChunk(t *testing.T) ([]byte, chunkDescriptor) {
	t.Helper()
	writer, err := newChunkWriter(recordSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(generationRecord{Key: "/a", Source: &sourceGenerationRecord{Path: "/a", Kind: EntryDirectory}}); err != nil {
		t.Fatal(err)
	}
	body, descriptor, err := writer.Close("source-000001")
	if err != nil {
		t.Fatal(err)
	}
	return body, descriptor
}
