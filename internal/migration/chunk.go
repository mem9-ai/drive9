package migration

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

const (
	generationFormatVersion  = "v1"
	maxGenerationRecordBytes = 4 << 20
	maxChunkPayloadBytes     = 64 << 20
)

type generationRecordKind string

const (
	recordSource            generationRecordKind = "source"
	recordTarget            generationRecordKind = "target"
	recordDiff              generationRecordKind = "diff"
	recordDirectoryIdentity generationRecordKind = "directory_identity"
)

type sourceGenerationRecord struct {
	Path            string    `json:"path"`
	LocalPath       string    `json:"local_path,omitempty"`
	Kind            EntryKind `json:"kind"`
	Device          uint64    `json:"device"`
	Inode           uint64    `json:"inode"`
	Size            int64     `json:"size"`
	MtimeNS         int64     `json:"mtime_ns"`
	CtimeNS         int64     `json:"ctime_ns"`
	VersionMode     uint32    `json:"version_mode"`
	Mode            uint32    `json:"mode"`
	ChecksumSHA256  string    `json:"checksum_sha256,omitempty"`
	LinkTarget      string    `json:"link_target,omitempty"`
	HardlinkKey     string    `json:"hardlink_key,omitempty"`
	HardlinkPrimary bool      `json:"hardlink_primary,omitempty"`
}

type targetGenerationRecord struct {
	Path             string    `json:"path"`
	Kind             EntryKind `json:"kind"`
	Size             int64     `json:"size"`
	Mode             *uint32   `json:"mode"`
	MetadataComplete bool      `json:"metadata_complete"`
	IdentityKind     string    `json:"identity_kind"`
	Revision         *int64    `json:"revision"`
	ResourceID       string    `json:"resource_id"`
	Nlink            uint32    `json:"nlink"`
	ChecksumSHA256   *string   `json:"checksum_sha256"`
}

type diffGenerationRecord struct {
	Path          string                  `json:"path"`
	PrimaryPath   string                  `json:"primary_path,omitempty"`
	PrimarySource *sourceGenerationRecord `json:"primary_source,omitempty"`
	PrimaryTarget *targetGenerationRecord `json:"primary_target,omitempty"`
	Operation     string                  `json:"operation"`
	DependencyKey string                  `json:"dependency_key"`
	Finding       FindingKind             `json:"finding"`
	Severity      Severity                `json:"severity"`
	Source        *sourceGenerationRecord `json:"source,omitempty"`
	Target        *targetGenerationRecord `json:"target,omitempty"`
}

type directoryIdentityRecord struct {
	Path        string `json:"path"`
	LocalPath   string `json:"local_path"`
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	Size        int64  `json:"size"`
	MtimeNS     int64  `json:"mtime_ns"`
	CtimeNS     int64  `json:"ctime_ns"`
	VersionMode uint32 `json:"version_mode"`
}

type generationRecord struct {
	Key               string                   `json:"key"`
	Source            *sourceGenerationRecord  `json:"source,omitempty"`
	Target            *targetGenerationRecord  `json:"target,omitempty"`
	Diff              *diffGenerationRecord    `json:"diff,omitempty"`
	DirectoryIdentity *directoryIdentityRecord `json:"directory_identity,omitempty"`
}

type chunkHeader struct {
	FormatVersion string               `json:"format_version"`
	RecordKind    generationRecordKind `json:"record_kind"`
}

type chunkDescriptor struct {
	FormatVersion  string               `json:"format_version"`
	ID             string               `json:"id"`
	Stage          generationStage      `json:"stage"`
	Kind           generationRecordKind `json:"kind"`
	RecordCount    int64                `json:"record_count"`
	FirstKey       string               `json:"first_key"`
	LastKey        string               `json:"last_key"`
	PayloadBytes   int64                `json:"payload_bytes"`
	ChecksumSHA256 string               `json:"checksum_sha256"`
}

type chunkWriter struct {
	kind     generationRecordKind
	buffer   bytes.Buffer
	gzip     *gzip.Writer
	count    int64
	firstKey string
	lastKey  string
	closed   bool
}

func newChunkWriter(kind generationRecordKind) (*chunkWriter, error) {
	if !validGenerationRecordKind(kind) {
		return nil, fmt.Errorf("invalid generation record kind %q", kind)
	}
	w := &chunkWriter{kind: kind}
	w.gzip = gzip.NewWriter(&w.buffer)
	if err := writeChunkJSON(w.gzip, chunkHeader{FormatVersion: generationFormatVersion, RecordKind: kind}); err != nil {
		_ = w.gzip.Close()
		return nil, fmt.Errorf("write chunk header: %w", err)
	}
	return w, nil
}

func (w *chunkWriter) Write(record generationRecord) error {
	if w == nil || w.closed {
		return fmt.Errorf("chunk writer is closed")
	}
	if err := validateGenerationRecord(w.kind, record); err != nil {
		return err
	}
	if w.count > 0 && record.Key <= w.lastKey {
		return fmt.Errorf("chunk record key %q is not greater than %q", record.Key, w.lastKey)
	}
	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode chunk record: %w", err)
	}
	if len(body) > maxGenerationRecordBytes {
		return fmt.Errorf("chunk record exceeds %d bytes", maxGenerationRecordBytes)
	}
	if _, err := w.gzip.Write(append(body, '\n')); err != nil {
		return fmt.Errorf("write chunk record: %w", err)
	}
	if w.count == 0 {
		w.firstKey = record.Key
	}
	w.lastKey = record.Key
	w.count++
	return nil
}

func (w *chunkWriter) Close(id string) ([]byte, chunkDescriptor, error) {
	if w == nil || w.closed {
		return nil, chunkDescriptor{}, fmt.Errorf("chunk writer is closed")
	}
	w.closed = true
	if err := validateGenerationIdentifier(id); err != nil {
		return nil, chunkDescriptor{}, fmt.Errorf("invalid chunk ID: %w", err)
	}
	if err := w.gzip.Close(); err != nil {
		return nil, chunkDescriptor{}, fmt.Errorf("close chunk: %w", err)
	}
	if w.buffer.Len() > maxChunkPayloadBytes {
		return nil, chunkDescriptor{}, fmt.Errorf("chunk payload exceeds %d bytes", maxChunkPayloadBytes)
	}
	body := append([]byte(nil), w.buffer.Bytes()...)
	return body, chunkDescriptor{
		FormatVersion:  generationFormatVersion,
		ID:             id,
		Kind:           w.kind,
		RecordCount:    w.count,
		FirstKey:       w.firstKey,
		LastKey:        w.lastKey,
		PayloadBytes:   int64(len(body)),
		ChecksumSHA256: checksumHex(body),
	}, nil
}

type chunkReader struct {
	descriptor chunkDescriptor
	gzip       *gzip.Reader
	decoder    *json.Decoder
	count      int64
	firstKey   string
	lastKey    string
	finished   bool
}

func newChunkReader(body []byte, descriptor chunkDescriptor) (*chunkReader, error) {
	if descriptor.FormatVersion != generationFormatVersion || !validGenerationRecordKind(descriptor.Kind) {
		return nil, fmt.Errorf("chunk descriptor format or kind mismatch")
	}
	if descriptor.RecordCount < 0 || descriptor.PayloadBytes != int64(len(body)) || descriptor.PayloadBytes > maxChunkPayloadBytes {
		return nil, fmt.Errorf("chunk descriptor count or size mismatch")
	}
	if descriptor.ChecksumSHA256 != checksumHex(body) {
		return nil, fmt.Errorf("chunk payload checksum mismatch")
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("open chunk gzip: %w", err)
	}
	decoder := json.NewDecoder(gzipReader)
	var header chunkHeader
	if err := decoder.Decode(&header); err != nil {
		_ = gzipReader.Close()
		return nil, fmt.Errorf("decode chunk header: %w", err)
	}
	if header.FormatVersion != generationFormatVersion || header.RecordKind != descriptor.Kind {
		_ = gzipReader.Close()
		return nil, fmt.Errorf("chunk header mismatch")
	}
	return &chunkReader{descriptor: descriptor, gzip: gzipReader, decoder: decoder}, nil
}

func (r *chunkReader) Next() (generationRecord, bool, error) {
	if r == nil || r.finished {
		return generationRecord{}, false, nil
	}
	var record generationRecord
	err := r.decoder.Decode(&record)
	if err == io.EOF {
		r.finished = true
		closeErr := r.gzip.Close()
		if closeErr != nil {
			return generationRecord{}, false, fmt.Errorf("close chunk reader: %w", closeErr)
		}
		if r.count != r.descriptor.RecordCount || r.firstKey != r.descriptor.FirstKey || r.lastKey != r.descriptor.LastKey {
			return generationRecord{}, false, fmt.Errorf("chunk descriptor record summary mismatch")
		}
		return generationRecord{}, false, nil
	}
	if err != nil {
		return generationRecord{}, false, fmt.Errorf("decode chunk record: %w", err)
	}
	if err := validateGenerationRecord(r.descriptor.Kind, record); err != nil {
		return generationRecord{}, false, err
	}
	if r.count > 0 && record.Key <= r.lastKey {
		return generationRecord{}, false, fmt.Errorf("chunk record key %q is not greater than %q", record.Key, r.lastKey)
	}
	if r.count == 0 {
		r.firstKey = record.Key
	}
	r.lastKey = record.Key
	r.count++
	if r.count > r.descriptor.RecordCount {
		return generationRecord{}, false, fmt.Errorf("chunk contains more records than declared")
	}
	return record, true, nil
}

func validateGenerationRecord(kind generationRecordKind, record generationRecord) error {
	if record.Key == "" {
		return fmt.Errorf("generation record key is empty")
	}
	payloads := 0
	if record.Source != nil {
		payloads++
	}
	if record.Target != nil {
		payloads++
	}
	if record.Diff != nil {
		payloads++
	}
	if record.DirectoryIdentity != nil {
		payloads++
	}
	if payloads != 1 {
		return fmt.Errorf("generation record requires exactly one payload")
	}
	switch kind {
	case recordSource:
		if record.Source == nil || record.Source.Path != record.Key || !validEntryKind(record.Source.Kind) {
			return fmt.Errorf("invalid Source generation record")
		}
	case recordTarget:
		if record.Target == nil || record.Target.Path != record.Key || !validEntryKind(record.Target.Kind) {
			return fmt.Errorf("invalid Target generation record")
		}
	case recordDiff:
		if record.Diff == nil || record.Diff.Operation == "" || record.Diff.Path == "" && record.Diff.Finding == "" {
			return fmt.Errorf("invalid Diff generation record")
		}
	case recordDirectoryIdentity:
		if record.DirectoryIdentity == nil || record.DirectoryIdentity.Path != record.Key || record.DirectoryIdentity.LocalPath == "" {
			return fmt.Errorf("invalid directory identity record")
		}
	default:
		return fmt.Errorf("invalid generation record kind %q", kind)
	}
	return nil
}

func validGenerationRecordKind(kind generationRecordKind) bool {
	return kind == recordSource || kind == recordTarget || kind == recordDiff || kind == recordDirectoryIdentity
}

func validEntryKind(kind EntryKind) bool {
	return kind == EntryRegular || kind == EntryDirectory || kind == EntrySymlink || kind == EntrySpecial
}

func writeChunkJSON(writer io.Writer, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = writer.Write(append(body, '\n'))
	return err
}

func checksumHex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
