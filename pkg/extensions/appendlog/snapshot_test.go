package appendlog

// Ported from v1 pkg/fuse/append_log_snapshot_test.go: the four rows not
// already covered by appendlog_test.go — frozen semantics of memory and
// ReaderAt snapshots, strict EOF/size validation, and temp-file placement
// with idempotent Close.

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v1 TestAppendLogSnapshotMemoryReadersAreFrozen: a memory snapshot is an
// immutable copy; mutating the source after capture and re-opening the
// snapshot repeatedly must keep serving the original bytes.
func TestPortAppendLogSnapshotMemoryReadersAreFrozen(t *testing.T) {
	source := []byte("immutable tail")
	snapshot, err := newAppendLogSnapshotFromReader("", int64(len(source)), bytes.NewReader(source))
	if err != nil {
		t.Fatalf("newAppendLogSnapshotFromReader: %v", err)
	}
	defer func() { _ = snapshot.Close() }()

	source[0] = 'X'
	for attempt := 0; attempt < 2; attempt++ {
		reader, err := snapshot.Open()
		if err != nil {
			t.Fatalf("Open attempt %d: %v", attempt, err)
		}
		got, err := io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("read attempt %d: read=%v close=%v", attempt, err, closeErr)
		}
		if string(got) != "immutable tail" {
			t.Fatalf("attempt %d bytes = %q, want immutable tail", attempt, got)
		}
	}
}

// v1 TestAppendLogSnapshotLargeBodyUsesRemovableTempFile (the rows not yet
// covered): the spill file lives in the configured directory and Close is
// idempotent.
func TestPortAppendLogSnapshotSpillUsesConfiguredDirAndIdempotentClose(t *testing.T) {
	dir := t.TempDir()
	source := bytes.Repeat([]byte("x"), appendLogSnapshotMemoryLimit+1)
	snapshot, err := newAppendLogSnapshotFromReader(dir, int64(len(source)), bytes.NewReader(source))
	if err != nil {
		t.Fatalf("newAppendLogSnapshotFromReader: %v", err)
	}
	if snapshot.tempPath == "" {
		t.Fatal("large snapshot must use a temp file")
	}
	if filepath.Dir(snapshot.tempPath) != dir {
		t.Fatalf("temp path = %q, want directory %q", snapshot.tempPath, dir)
	}

	reader, err := snapshot.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("snapshot bytes match = %t, read error = %v", bytes.Equal(got, source), err)
	}
	path := snapshot.tempPath
	if err := snapshot.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temp path after Close: %v, want not exist", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// v1 TestAppendLogSnapshotRejectsIncompleteAndInvalidInput: negative sizes,
// short bodies, overlong bodies, and unusable temp directories all fail the
// snapshot.
func TestPortAppendLogSnapshotRejectsIncompleteAndInvalidInput(t *testing.T) {
	big := string(bytes.Repeat([]byte("x"), appendLogSnapshotMemoryLimit+1))
	tests := []struct {
		name string
		dir  string
		size int64
		body string
	}{
		{name: "negative size", dir: t.TempDir(), size: -1},
		{name: "short input", dir: t.TempDir(), size: 4, body: "abc"},
		{name: "extra input", dir: t.TempDir(), size: 3, body: "abcd"},
		{name: "missing temp directory", dir: filepath.Join(t.TempDir(), "missing"), size: appendLogSnapshotMemoryLimit + 1, body: big},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newAppendLogSnapshotFromReader(test.dir, test.size, strings.NewReader(test.body)); err == nil {
				t.Fatal("newAppendLogSnapshotFromReader error = nil, want failure")
			}
		})
	}
}

// v1 TestAppendLogSnapshotReaderAtRangeIsFrozen: a ReaderAt capture freezes
// its range against later source mutation, and invalid ranges are rejected.
func TestPortAppendLogSnapshotReaderAtRangeIsFrozen(t *testing.T) {
	source := []byte("prefix-tail-suffix")
	snapshot, err := newAppendLogSnapshotFromReaderAt(t.TempDir(), bytes.NewReader(source), 7, 4)
	if err != nil {
		t.Fatalf("newAppendLogSnapshotFromReaderAt: %v", err)
	}
	defer func() { _ = snapshot.Close() }()

	source[7] = 'X'
	reader, err := snapshot.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(got) != "tail" {
		t.Fatalf("range bytes = %q, err = %v, want tail", got, err)
	}

	if _, err := newAppendLogSnapshotFromReaderAt(t.TempDir(), bytes.NewReader(source), -1, 1); err == nil {
		t.Fatal("negative offset must fail")
	}
	if _, err := newAppendLogSnapshotFromReaderAt(t.TempDir(), bytes.NewReader(source), 100, 1); err == nil {
		t.Fatal("out-of-bounds range must fail")
	}
}
