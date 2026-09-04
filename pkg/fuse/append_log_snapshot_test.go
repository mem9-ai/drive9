package fuse

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendLogSnapshotMemoryReadersAreFrozen(t *testing.T) {
	source := []byte("immutable tail")
	snapshot, err := newAppendLogSnapshotFromReader(t.TempDir(), int64(len(source)), bytes.NewReader(source))
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

func TestAppendLogSnapshotLargeBodyUsesRemovableTempFile(t *testing.T) {
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
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp path after Close: %v, want not exist", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestAppendLogSnapshotRejectsIncompleteAndInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		size int64
		body string
	}{
		{name: "negative size", dir: t.TempDir(), size: -1},
		{name: "short input", dir: t.TempDir(), size: 4, body: "abc"},
		{name: "extra input", dir: t.TempDir(), size: 3, body: "abcd"},
		{name: "missing temp directory", dir: filepath.Join(t.TempDir(), "missing"), size: appendLogSnapshotMemoryLimit + 1, body: string(bytes.Repeat([]byte("x"), appendLogSnapshotMemoryLimit+1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newAppendLogSnapshotFromReader(test.dir, test.size, bytes.NewBufferString(test.body))
			if err == nil {
				t.Fatal("newAppendLogSnapshotFromReader error = nil, want failure")
			}
		})
	}
}

func TestAppendLogSnapshotReaderAtRangeIsFrozen(t *testing.T) {
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

func TestAppendLogSnapshotFromActiveShadowIsFrozen(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(_ http.ResponseWriter, _ *http.Request) {})
	defer closeServer()
	original := bytes.Repeat([]byte("a"), appendLogSnapshotMemoryLimit+1)
	replacement := bytes.Repeat([]byte("b"), len(original))
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(fh.Path, original, fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fh.ShadowSpill = true
	fh.ShadowReady = true

	snapshot, err := fs.newAppendLogSnapshotLocked(fh, 0, int64(len(original)))
	if err != nil {
		t.Fatalf("newAppendLogSnapshotLocked: %v", err)
	}
	defer func() { _ = snapshot.Close() }()
	if snapshot.tempPath == "" {
		t.Fatal("large active-shadow snapshot must use a temp file")
	}
	if err := shadow.WriteFull(fh.Path, replacement, fh.BaseRev); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		reader, err := snapshot.Open()
		if err != nil {
			t.Fatalf("Open attempt %d: %v", attempt, err)
		}
		got, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read attempt %d: read=%v close=%v", attempt, readErr, closeErr)
		}
		if !bytes.Equal(got, original) {
			t.Fatalf("attempt %d did not preserve the frozen active-shadow bytes", attempt)
		}
	}
}
