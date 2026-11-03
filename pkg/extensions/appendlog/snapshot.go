// appendLogSnapshot owns immutable request bytes for the append-log commit
// paths. Snapshots at or below the 1MiB memory limit stay in RAM; larger
// images spill to a temp file so the rebase retry reopens a bounded reader
// without holding the whole image (or a live WriteBuffer) in memory, and
// the commit streams from disk.
package appendlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

const appendLogSnapshotMemoryLimit = 1 << 20

type appendLogSnapshot struct {
	size      int64
	memory    []byte
	tempPath  string
	closeOnce sync.Once
	closeErr  error
}

func newAppendLogSnapshotFromReader(tempDir string, size int64, source io.Reader) (*appendLogSnapshot, error) {
	if size < 0 {
		return nil, fmt.Errorf("append-log snapshot size must be non-negative: %d", size)
	}
	if source == nil {
		return nil, fmt.Errorf("append-log snapshot source is required")
	}
	if size <= appendLogSnapshotMemoryLimit {
		data := make([]byte, size)
		if _, err := io.ReadFull(source, data); err != nil {
			return nil, fmt.Errorf("read append-log snapshot: %w", err)
		}
		if err := requireAppendLogSnapshotEOF(source); err != nil {
			return nil, err
		}
		return &appendLogSnapshot{size: size, memory: data}, nil
	}

	file, err := os.CreateTemp(tempDir, ".drive9-append-log-")
	if err != nil {
		return nil, fmt.Errorf("create append-log snapshot: %w", err)
	}
	path := file.Name()
	cleanup := func(cause error) (*appendLogSnapshot, error) {
		closeErr := file.Close()
		removeErr := os.Remove(path)
		if closeErr != nil {
			cause = fmt.Errorf("%w; close append-log snapshot: %v", cause, closeErr)
		}
		if removeErr != nil && !os.IsNotExist(removeErr) {
			cause = fmt.Errorf("%w; remove append-log snapshot: %v", cause, removeErr)
		}
		return nil, cause
	}
	if _, err := io.CopyN(file, source, size); err != nil {
		return cleanup(fmt.Errorf("copy append-log snapshot: %w", err))
	}
	if err := requireAppendLogSnapshotEOF(source); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		return cleanup(fmt.Errorf("close append-log snapshot: %w", err))
	}
	return &appendLogSnapshot{size: size, tempPath: path}, nil
}

func newAppendLogSnapshotFromReaderAt(tempDir string, source io.ReaderAt, offset, size int64) (*appendLogSnapshot, error) {
	if offset < 0 || size < 0 || offset > int64(^uint64(0)>>1)-size {
		return nil, fmt.Errorf("invalid append-log snapshot range offset=%d size=%d", offset, size)
	}
	if source == nil {
		return nil, fmt.Errorf("append-log snapshot source is required")
	}
	return newAppendLogSnapshotFromReader(tempDir, size, io.NewSectionReader(source, offset, size))
}

func (snapshot *appendLogSnapshot) Size() int64 {
	if snapshot == nil {
		return 0
	}
	return snapshot.size
}

func (snapshot *appendLogSnapshot) Open() (io.ReadCloser, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("nil append-log snapshot")
	}
	if snapshot.tempPath == "" {
		return io.NopCloser(bytes.NewReader(snapshot.memory)), nil
	}
	file, err := os.Open(snapshot.tempPath)
	if err != nil {
		return nil, fmt.Errorf("open append-log snapshot: %w", err)
	}
	return file, nil
}

func (snapshot *appendLogSnapshot) Close() error {
	if snapshot == nil {
		return nil
	}
	snapshot.closeOnce.Do(func() {
		if snapshot.tempPath == "" {
			return
		}
		snapshot.closeErr = os.Remove(snapshot.tempPath)
		if os.IsNotExist(snapshot.closeErr) {
			snapshot.closeErr = nil
		}
	})
	return snapshot.closeErr
}

// appendLogSnapshotSQLiteWALHeader parses the snapshot's first 32 bytes as
// a WAL header, proving a committed generation after a tail append or full
// rewrite.
func appendLogSnapshotSQLiteWALHeader(snapshot *appendLogSnapshot) (sqliteWALHeader, bool) {
	if snapshot == nil || snapshot.Size() < sqliteWALHeaderSize {
		return sqliteWALHeader{}, false
	}
	reader, err := snapshot.Open()
	if err != nil {
		return sqliteWALHeader{}, false
	}
	defer func() { _ = reader.Close() }()
	var raw [sqliteWALHeaderSize]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return sqliteWALHeader{}, false
	}
	return parseSQLiteWALHeader(raw[:])
}

func requireAppendLogSnapshotEOF(source io.Reader) error {
	var extra [1]byte
	n, err := source.Read(extra[:])
	if n > 0 {
		return fmt.Errorf("append-log snapshot source exceeds declared size")
	}
	if err == nil {
		return fmt.Errorf("append-log snapshot source made no progress after declared size")
	}
	if err == io.EOF {
		return nil
	}
	return fmt.Errorf("read append-log snapshot tail: %w", err)
}
