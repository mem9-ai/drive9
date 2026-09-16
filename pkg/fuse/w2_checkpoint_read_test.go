package fuse

import (
	"bytes"
	"net/http"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// TestW2CleanSiblingAtLatestRevisionServesSidecarRead covers the revision-gated
// clean-sibling reuse: a checkpoint fd reads committed WAL bytes from the
// writer sibling's clean buffer when the writer is at the latest committed
// revision.
func TestW2CleanSiblingAtLatestRevisionServesSidecarRead(t *testing.T) {
	fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()

	content := []byte("committed-wal-frames")
	writer := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  6,
		OrigSize: int64(len(content)),
	}
	if _, err := writer.Dirty.Write(0, content); err != nil {
		t.Fatal(err)
	}
	writer.Dirty.ClearDirty()
	fs.openHandles.Add(writer)
	defer fs.openHandles.Remove(writer)
	fs.recordCommittedRevision("/db-wal", 6)

	reader := &FileHandle{Ino: 2, Path: "/db-wal", Dirty: NewWriteBuffer("/db-wal", 1024, 0), BaseRev: 5}
	data, n, ok, st, src := fs.readSQLitePersistentJournalVisibleRange("/db-wal", reader, 5, 0, uint32(len(content)))
	if st != gofuse.OK || !ok {
		t.Fatalf("visible range = ok=%t st=%d, want ok/OK", ok, st)
	}
	if src != "sqlite-sidecar-clean-sibling" {
		t.Fatalf("source = %q, want sqlite-sidecar-clean-sibling", src)
	}
	if n != len(content) || !bytes.Equal(data, content) {
		t.Fatalf("data = %q n=%d, want %q/%d", data, n, content, len(content))
	}
}

// TestW2BehindRevisionCleanSiblingIsNotServed pins the -shm short-read guard:
// a clean sibling whose revision is behind the latest committed revision must
// never be served.
func TestW2BehindRevisionCleanSiblingIsNotServed(t *testing.T) {
	fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()

	content := []byte("stale-wal-bytes")
	stale := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  5,
		OrigSize: int64(len(content)),
	}
	if _, err := stale.Dirty.Write(0, content); err != nil {
		t.Fatal(err)
	}
	stale.Dirty.ClearDirty()
	fs.openHandles.Add(stale)
	defer fs.openHandles.Remove(stale)
	fs.recordCommittedRevision("/db-wal", 6)

	reader := &FileHandle{Ino: 2, Path: "/db-wal", Dirty: NewWriteBuffer("/db-wal", 1024, 0), BaseRev: 6}
	data, _, ok, st, src := fs.readSQLitePersistentJournalVisibleRange("/db-wal", reader, 6, 0, uint32(len(content)))
	if st != gofuse.OK {
		t.Fatalf("status = %d, want OK", st)
	}
	if ok || data != nil || src != "" {
		t.Fatalf("behind-revision sibling served: ok=%t src=%q", ok, src)
	}
}

// TestW2CleanSiblingRevisionAdvanceDiscardsCandidate pins the post-copy
// revalidation: if a same-path commit advances the latest committed revision
// after the sibling bytes are copied, the stale candidate must be discarded
// and the read falls back instead of returning behind-revision bytes.
func TestW2CleanSiblingRevisionAdvanceDiscardsCandidate(t *testing.T) {
	fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()

	content := []byte("committed-wal-frames")
	writer := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  6,
		OrigSize: int64(len(content)),
	}
	if _, err := writer.Dirty.Write(0, content); err != nil {
		t.Fatal(err)
	}
	writer.Dirty.ClearDirty()
	fs.openHandles.Add(writer)
	defer fs.openHandles.Remove(writer)
	fs.recordCommittedRevision("/db-wal", 6)

	// Advance the committed revision between the copy and the revalidation.
	testHookAfterCleanSiblingCopy = func(path string) {
		fs.recordCommittedRevision(path, 7)
	}
	t.Cleanup(func() { testHookAfterCleanSiblingCopy = nil })

	reader := &FileHandle{Ino: 2, Path: "/db-wal", Dirty: NewWriteBuffer("/db-wal", 1024, 0), BaseRev: 6}
	data, _, ok, st, src := fs.readSQLitePersistentJournalVisibleRange("/db-wal", reader, 6, 0, uint32(len(content)))
	if st != gofuse.OK {
		t.Fatalf("status = %d, want OK", st)
	}
	if ok || data != nil || src != "" {
		t.Fatalf("stale candidate served after revision advance: ok=%t src=%q", ok, src)
	}
}
