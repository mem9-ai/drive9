package fuse

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
)

// JournalOp identifies the type of operation recorded in a journal entry.
type JournalOp int

const (
	JournalWrite    JournalOp = iota // Write data to a file
	JournalTruncate                  // Truncate a file
	JournalRename                    // Rename a file
	JournalUnlink                    // Delete a file
	JournalMkdir                     // Create a directory
	JournalRmdir                     // Remove a directory
	JournalFsync                     // Fsync a file (local durability marker)
	JournalCommit                    // Remote commit completed (can be compacted)
)

// JournalEntry represents a single operation in the WAL.
type JournalEntry struct {
	Seq       uint64    `json:"seq"`
	Op        JournalOp `json:"op"`
	Inode     uint64    `json:"inode,omitempty"`
	Path      string    `json:"path"`
	NewPath   string    `json:"new_path,omitempty"` // for Rename
	Offset    int64     `json:"offset,omitempty"`   // for Write
	Length    int64     `json:"length,omitempty"`    // for Write
	BaseRev   int64     `json:"base_rev,omitempty"`
	Timestamp int64     `json:"timestamp,omitempty"`
}

// Journal is an append-only WAL for crash recovery. Each entry is
// length-prefixed + CRC32 for integrity verification.
//
// Wire format per entry:
//
//	[4 bytes: payload length (little-endian uint32)]
//	[N bytes: JSON payload]
//	[4 bytes: CRC32 of payload (little-endian uint32)]
type Journal struct {
	mu     sync.Mutex
	fd     *os.File
	seq    atomic.Uint64
	path   string
}

// NewJournal opens or creates a journal WAL file at the given path.
func NewJournal(path string) (*Journal, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal open: %w", err)
	}
	j := &Journal{
		fd:   fd,
		path: path,
	}
	return j, nil
}

// Append writes a journal entry to the WAL. The entry is length-prefixed
// and CRC32-checksummed for integrity.
func (j *Journal) Append(entry JournalEntry) error {
	entry.Seq = j.seq.Add(1)

	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("journal marshal: %w", err)
	}

	// Build wire frame: [len:4][payload:N][crc:4]
	frame := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	copy(frame[4:4+len(payload)], payload)
	checksum := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(frame[4+len(payload):], checksum)

	j.mu.Lock()
	defer j.mu.Unlock()

	_, err = j.fd.Write(frame)
	if err != nil {
		return fmt.Errorf("journal write: %w", err)
	}
	return nil
}

// Fsync ensures all journal entries are durable on disk.
func (j *Journal) Fsync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fd.Sync()
}

// Replay reads all entries from the journal file and calls fn for each
// valid entry. Corrupt entries (bad CRC or truncated) are skipped, and
// replay continues from the next valid entry boundary.
func (j *Journal) Replay(fn func(JournalEntry)) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, err := j.fd.Seek(0, 0); err != nil {
		return err
	}

	data, err := os.ReadFile(j.path)
	if err != nil {
		return err
	}

	var maxSeq uint64
	pos := 0
	for pos+8 <= len(data) { // need at least 4 (len) + 4 (crc)
		payloadLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		frameEnd := pos + 4 + int(payloadLen) + 4
		if frameEnd > len(data) {
			break // truncated entry
		}

		payload := data[pos+4 : pos+4+int(payloadLen)]
		storedCRC := binary.LittleEndian.Uint32(data[pos+4+int(payloadLen):])
		computedCRC := crc32.ChecksumIEEE(payload)

		if storedCRC != computedCRC {
			// Corrupt entry — skip to next possible entry.
			pos++
			continue
		}

		var entry JournalEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			pos = frameEnd
			continue
		}

		fn(entry)
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
		pos = frameEnd
	}

	j.seq.Store(maxSeq)
	return nil
}

// Compact removes all committed entries from the journal by rewriting
// the file with only uncommitted entries.
func (j *Journal) Compact() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, err := j.fd.Seek(0, 0); err != nil {
		return err
	}

	data, err := os.ReadFile(j.path)
	if err != nil {
		return err
	}

	// First pass: find committed sequences.
	committed := make(map[string]bool) // path → committed
	pos := 0
	for pos+8 <= len(data) {
		payloadLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		frameEnd := pos + 4 + int(payloadLen) + 4
		if frameEnd > len(data) {
			break
		}
		payload := data[pos+4 : pos+4+int(payloadLen)]
		storedCRC := binary.LittleEndian.Uint32(data[pos+4+int(payloadLen):])
		if storedCRC != crc32.ChecksumIEEE(payload) {
			pos++
			continue
		}
		var entry JournalEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			pos = frameEnd
			continue
		}
		if entry.Op == JournalCommit {
			committed[entry.Path] = true
		}
		pos = frameEnd
	}

	if len(committed) == 0 {
		return nil // nothing to compact
	}

	// Second pass: collect non-committed frames.
	var kept []byte
	pos = 0
	for pos+8 <= len(data) {
		payloadLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		frameEnd := pos + 4 + int(payloadLen) + 4
		if frameEnd > len(data) {
			break
		}
		payload := data[pos+4 : pos+4+int(payloadLen)]
		storedCRC := binary.LittleEndian.Uint32(data[pos+4+int(payloadLen):])
		if storedCRC != crc32.ChecksumIEEE(payload) {
			pos++
			continue
		}
		var entry JournalEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			pos = frameEnd
			continue
		}
		if !committed[entry.Path] {
			kept = append(kept, data[pos:frameEnd]...)
		}
		pos = frameEnd
	}

	// Rewrite journal file.
	tmpPath := j.path + ".compact"
	if err := atomicWrite(tmpPath, kept); err != nil {
		return fmt.Errorf("journal compact write: %w", err)
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		return fmt.Errorf("journal compact rename: %w", err)
	}

	// Reopen.
	_ = j.fd.Close()
	fd, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("journal reopen: %w", err)
	}
	j.fd = fd
	return nil
}

// Close closes the journal file.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fd.Close()
}
