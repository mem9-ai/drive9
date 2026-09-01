package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func sqliteCheckpointImages(t *testing.T) ([]byte, []byte) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skipf("python3 not available for sqlite checkpoint fixture: %v", err)
	}
	dir := t.TempDir()
	db := filepath.Join(dir, "app.db")
	checkpointA := filepath.Join(dir, "checkpoint-a.db")
	checkpointB := filepath.Join(dir, "checkpoint-b.db")
	script := `
import os, shutil, sqlite3, sys
db, checkpoint_a, checkpoint_b = sys.argv[1:]
for p in [db, db + "-wal", db + "-shm", db + "-journal", checkpoint_a, checkpoint_b]:
    try:
        os.remove(p)
    except FileNotFoundError:
        pass
conn = sqlite3.connect(db)
mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower()
if mode != "wal":
    raise RuntimeError(f"journal_mode={mode}")
conn.execute("PRAGMA synchronous=FULL")
conn.execute("PRAGMA wal_autocheckpoint=0")
conn.execute("PRAGMA mmap_size=0")
conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
for i in range(1, 6):
    conn.execute("INSERT INTO t(v) VALUES (?)", (f"v{i}",))
    conn.commit()
busy, log, checkpointed = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
if busy != 0:
    raise RuntimeError(f"checkpoint A busy: {(busy, log, checkpointed)}")
if conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] != 5:
    raise RuntimeError("checkpoint A row count mismatch")
shutil.copyfile(db, checkpoint_a)
for i in range(6, 13):
    conn.execute("INSERT INTO t(v) VALUES (?)", (f"v{i}",))
    conn.commit()
busy, log, checkpointed = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
if busy != 0:
    raise RuntimeError(f"checkpoint B busy: {(busy, log, checkpointed)}")
if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
    raise RuntimeError("checkpoint B integrity_check failed")
if conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] != 12:
    raise RuntimeError("checkpoint B row count mismatch")
conn.close()
shutil.copyfile(db, checkpoint_b)
`
	cmd := exec.Command(python, "-c", script, db, checkpointA, checkpointB)
	cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite checkpoint fixture failed: %v\n%s", err, out)
	}
	a, err := os.ReadFile(checkpointA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(checkpointB)
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func sqliteRowCountFromImage(t *testing.T, image []byte) int {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skipf("python3 not available for sqlite image validation: %v", err)
	}
	db := filepath.Join(t.TempDir(), "fresh.db")
	if err := os.WriteFile(db, image, 0o644); err != nil {
		t.Fatal(err)
	}
	script := `
import sqlite3, sys
conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
conn.close()
print(integrity)
print(count)
`
	out, err := exec.Command(python, "-c", script, db).CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite image validation failed: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 || lines[0] != "ok" {
		t.Fatalf("sqlite image validation output = %q, want integrity ok + row count", out)
	}
	var rows int
	if _, err := fmt.Sscanf(lines[1], "%d", &rows); err != nil {
		t.Fatalf("parse row count %q: %v", lines[1], err)
	}
	return rows
}

type casFileServer struct {
	t    *testing.T
	path string

	mu       sync.Mutex
	revision int64
	body     []byte
	puts     []casFilePut
}

type casFilePut struct {
	expected string
	revision int64
	body     []byte
}

func newCASFileServer(t *testing.T, path string, revision int64, body []byte) (*casFileServer, *httptest.Server) {
	t.Helper()
	s := &casFileServer{
		t:        t,
		path:     path,
		revision: revision,
		body:     append([]byte(nil), body...),
	}
	ts := httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s, ts
}

func (s *casFileServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/fs"+s.path {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		expected := r.Header.Get("X-Dat9-Expected-Revision")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		wantExpected := fmt.Sprintf("%d", s.revision)
		if expected != wantExpected {
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
			return
		}
		s.revision++
		s.body = append([]byte(nil), body...)
		s.puts = append(s.puts, casFilePut{
			expected: expected,
			revision: s.revision,
			body:     append([]byte(nil), body...),
		})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": s.revision})
	case http.MethodHead:
		s.mu.Lock()
		revision := s.revision
		size := len(s.body)
		s.mu.Unlock()
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", fmt.Sprintf("%d", revision))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		s.mu.Lock()
		body := append([]byte(nil), s.body...)
		s.mu.Unlock()
		_, _ = w.Write(body)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *casFileServer) snapshot() (int64, []byte, []casFilePut) {
	s.mu.Lock()
	defer s.mu.Unlock()
	puts := make([]casFilePut, len(s.puts))
	copy(puts, s.puts)
	return s.revision, append([]byte(nil), s.body...), puts
}

func TestDirtySiblingKeepsOriginalBaseRevAfterCommittedRevisionAdvances(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*FileHandle)
	}{
		{
			name: "dirty-buffer",
		},
		{
			name: "writeback-snapshot",
			configure: func(fh *FileHandle) {
				fh.WriteBackSeq = fh.DirtySeq
			},
		},
		{
			name: "shadow-commit-ready",
			configure: func(fh *FileHandle) {
				fh.ShadowCommitReady = true
				fh.ShadowCommitSeq = fh.DirtySeq
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)

			const filePath = "/workload.db"
			ino := fs.inodes.Lookup(filePath, false, 8192, time.Now())
			fs.inodes.UpdateRevision(ino, 2)

			stale := &FileHandle{
				Ino:      ino,
				Path:     filePath,
				Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
				OrigSize: 8192,
				BaseRev:  2,
			}
			if _, err := stale.Dirty.Write(0, []byte("checkpoint-A")); err != nil {
				t.Fatal(err)
			}
			stale.DirtySeq = fs.markDirtySize(ino, stale.Dirty.Size())
			if tc.configure != nil {
				tc.configure(stale)
			}
			fs.openHandles.Add(stale)

			fs.refreshCommittedRevisionForOpenHandles(filePath, 3, nil)

			if stale.BaseRev != 2 {
				t.Fatalf("dirty sibling BaseRev advanced to %d; want original 2 so stale content cannot be paired with rev3", stale.BaseRev)
			}

			stale.Lock()
			got := fs.expectedRevisionForHandleLocked(stale)
			stale.Unlock()
			if got != 2 {
				t.Fatalf("expected revision for dirty sibling = %d, want 2", got)
			}
		})
	}
}

func TestCommitQueueRejectsPayloadOlderThanDurableWatermark(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCalls.Add(1)
		http.Error(w, "stale payload must not reach server", http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/repro.db"
	oldMain := []byte("sqlite-main: checkpoint A has 5 rows")
	if err := shadow.WriteFull(path, oldMain, 2); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(oldMain)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	entry := &CommitEntry{
		Path:                  path,
		BaseRev:               3,
		PayloadBaseRev:        2,
		PayloadBaseRevSet:     true,
		Size:                  int64(len(oldMain)),
		Kind:                  PendingOverwrite,
		ShadowGen:             shadowGen,
		PendingIndexGen:       pendingGen,
		DurableWatermarkRev:   3,
		DisableAutoResolveLWW: true,
	}

	err = cq.CommitNow(context.Background(), entry)
	if !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("CommitNow err = %v, want errCommitPayloadStale", err)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d uploads; stale checkpoint-A payload must not be PUT after checkpoint-B watermark", got)
	}
	if data, err := shadow.ReadAll(path); err != nil || string(data) != string(oldMain) {
		t.Fatalf("shadow changed after rejected stale payload: data=%q err=%v", data, err)
	}
}

func TestSQLiteCheckpointABStaleHandleCannotOverwriteDurableMainDB(t *testing.T) {
	var putCalls atomic.Int32
	var handlerMu sync.Mutex
	var handlerErr error
	recordHandlerErr := func(err error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		recordHandlerErr(fmt.Errorf("stale checkpoint-A payload reached server: expected_rev=%q body=%q", r.Header.Get("X-Dat9-Expected-Revision"), body))
		http.Error(w, "stale payload reached server", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.client.SetSmallFileThresholdForTests(50000)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = NewCommitQueue(fs.client, shadow, pending, nil, 1, 8)

	const path = "/app-v2-dev.db"
	checkpointA := []byte("main.db checkpoint A: 5 rows")
	ino := fs.inodes.Lookup(path, false, int64(len(checkpointA)), time.Now())
	fs.inodes.UpdateRevision(ino, 2)
	fs.recordCommittedRevision(path, 2)

	stale := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize: int64(len(checkpointA)),
		BaseRev:  2,
	}
	if _, err := stale.Dirty.Write(0, checkpointA); err != nil {
		t.Fatal(err)
	}
	stale.DirtySeq = fs.markDirtySize(ino, stale.Dirty.Size())
	if err := fs.shadowStore.WriteFull(path, checkpointA, stale.BaseRev); err != nil {
		t.Fatal(err)
	}
	stale.ShadowStageGen = fs.shadowStore.ActiveGeneration(path)
	stale.PendingIndexGen, err = fs.pendingIndex.PutWithBaseRev(path, int64(len(checkpointA)), PendingOverwrite, stale.BaseRev)
	if err != nil {
		t.Fatal(err)
	}
	fs.openHandles.Add(stale)

	// Checkpoint B has already reached the authoritative server as rev3.
	// The stale handle must keep its rev2 payload base and be fenced before a
	// close/release path can upload checkpoint A as a new rev4.
	fs.recordCommittedRevision(path, 3)
	fs.inodes.UpdateRevision(ino, 3)
	fs.refreshCommittedRevisionForOpenHandles(path, 3, nil)
	if stale.BaseRev != 2 {
		t.Fatalf("stale main.db handle BaseRev = %d, want 2", stale.BaseRev)
	}

	stale.Lock()
	expectedRev := fs.expectedRevisionForHandleLocked(stale)
	entry := &CommitEntry{
		Path:    path,
		Inode:   ino,
		BaseRev: expectedRev,
		Size:    stale.Dirty.Size(),
		Kind:    PendingOverwrite,
	}
	fs.bindCommitEntryToHandleLocked(entry, stale, stale.BaseRev)
	stale.Unlock()

	if err := fs.commitQueue.CommitNow(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("CommitNow err = %v, want errCommitPayloadStale", err)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d uploads; stale checkpoint-A must not become a newer revision after checkpoint-B", got)
	}
	handlerMu.Lock()
	err = handlerErr
	handlerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteCheckpointABStaleHandleReleaseFreshRemoteKeepsCheckpointB(t *testing.T) {
	checkpointA, checkpointB := sqliteCheckpointImages(t)
	if rows := sqliteRowCountFromImage(t, checkpointA); rows != 5 {
		t.Fatalf("checkpoint A rows = %d, want 5", rows)
	}
	if rows := sqliteRowCountFromImage(t, checkpointB); rows != 12 {
		t.Fatalf("checkpoint B rows = %d, want 12", rows)
	}

	const path = "/app-v2-dev.db"
	server, ts := newCASFileServer(t, path, 2, checkpointA)
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, writeBack, 1)
	defer uploader.DrainAll()
	fs.SetWriteBack(writeBack, uploader)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(fs.client, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	fs.commitQueue = cq

	ino := fs.inodes.Lookup(path, false, int64(len(checkpointA)), time.Now())
	fs.inodes.UpdateRevision(ino, 2)
	fs.recordCommittedRevision(path, 2)

	// H-A: a stale main.db handle has checkpoint-A bytes staged in the
	// writeback/shadow stores. This is the Release/CommitQueue side of the
	// production incident, but the bytes are real sqlite3 checkpoint output.
	stale := &FileHandle{
		Ino:         ino,
		Path:        path,
		Dirty:       fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize:    int64(len(checkpointA)),
		BaseRev:     2,
		WritePolicy: WritePolicyWriteBack,
	}
	if _, err := stale.Dirty.Write(0, checkpointA); err != nil {
		t.Fatal(err)
	}
	stale.DirtySeq = fs.markDirtySize(ino, stale.Dirty.Size())
	stale.Lock()
	if err := fs.stageShadowLocked(stale, true); err != nil {
		stale.Unlock()
		t.Fatal(err)
	}
	if err := fs.snapshotWriteBackLocked(stale); err != nil {
		stale.Unlock()
		t.Fatal(err)
	}
	stale.WriteBackSeq = stale.DirtySeq
	stale.Unlock()
	staleID := fs.allocateFileHandle(stale)

	// H-B: checkpoint B writes the real 12-row sqlite image and strict fsyncs
	// it to the server. This must become the durable watermark for /app.db.
	newer := &FileHandle{
		Ino:         ino,
		Path:        path,
		Dirty:       fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize:    int64(len(checkpointA)),
		BaseRev:     2,
		WritePolicy: WritePolicyWriteBack,
	}
	if _, err := newer.Dirty.Write(0, checkpointB); err != nil {
		t.Fatal(err)
	}
	newer.DirtySeq = fs.markDirtySize(ino, newer.Dirty.Size())
	newerID := fs.allocateFileHandle(newer)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: newerID}); st != gofuse.OK {
		t.Fatalf("checkpoint B Fsync status = %v, want OK", st)
	}
	rev, body, puts := server.snapshot()
	if rev != 3 {
		t.Fatalf("server revision after checkpoint B = %d, want 3", rev)
	}
	if rows := sqliteRowCountFromImage(t, body); rows != 12 {
		t.Fatalf("server rows after checkpoint B fsync = %d, want 12", rows)
	}
	if len(puts) != 1 || puts[0].expected != "2" {
		t.Fatalf("checkpoint B PUT history = %+v, want one PUT expected rev2", puts)
	}
	if stale.BaseRev != 2 {
		t.Fatalf("stale main.db BaseRev = %d, want 2 after checkpoint B watermark", stale.BaseRev)
	}

	// Closing the stale H-A handle exercises the real FUSE Release
	// writeback-cache path. The queued entry is based on rev2 payload under a
	// rev3 durable watermark, so it must be fenced before it can upload A as
	// rev4. A fresh remote snapshot must still be the 12-row checkpoint B.
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: staleID})
	cq.DrainAll()
	rev, body, puts = server.snapshot()
	if rev != 3 {
		t.Fatalf("server revision after stale Release = %d, want 3 (no old checkpoint-A rev4)", rev)
	}
	if len(puts) != 1 {
		t.Fatalf("server PUTs after stale Release = %d, want only checkpoint B PUT", len(puts))
	}
	if rows := sqliteRowCountFromImage(t, body); rows != 12 {
		t.Fatalf("fresh remote sqlite rows after stale Release = %d, want 12", rows)
	}
	if meta, ok := pending.GetMeta(path); !ok || meta.Kind != PendingConflict {
		t.Fatalf("stale checkpoint-A pending meta = %+v ok=%t, want preserved conflict", meta, ok)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: newerID})
}

func TestSQLitePersistentJournalStagedDirtyHandleCannotAdoptCommittedRevision(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	const path = "/app.db-wal"
	ino := fs.inodes.Lookup(path, false, 32, time.Now())
	fs.inodes.UpdateRevision(ino, 2)
	fs.recordCommittedRevision(path, 2)
	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize: 32,
		BaseRev:  2,
	}
	if _, err := fh.Dirty.Write(0, []byte("non-empty wal frame payload")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fh.Lock()
	if err := fs.stageShadowLocked(fh, true); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	fh.Unlock()
	fs.openHandles.Add(fh)

	fs.recordCommittedRevision(path, 3)
	fs.refreshCommittedRevisionForOpenHandles(path, 3, nil)

	if fh.BaseRev != 2 {
		t.Fatalf("staged dirty journal BaseRev = %d, want 2; path-keyed sidecar staging must not adopt a newer CAS base", fh.BaseRev)
	}
	fh.Lock()
	entry := &CommitEntry{Path: path, Inode: ino, BaseRev: fs.expectedRevisionForHandleLocked(fh), Size: fh.Dirty.Size(), Kind: PendingOverwrite}
	fs.bindCommitEntryToHandleLocked(entry, fh, fh.BaseRev)
	fh.Unlock()
	if entry.PayloadBaseRev != 2 || !entry.PayloadBaseRevSet {
		t.Fatalf("journal entry payload base = %d set=%t, want rev2 binding", entry.PayloadBaseRev, entry.PayloadBaseRevSet)
	}
	if entry.DurableWatermarkRev != 3 || !entry.DisableAutoResolveLWW {
		t.Fatalf("journal entry watermark = %d disable_lww=%t, want rev3 fence", entry.DurableWatermarkRev, entry.DisableAutoResolveLWW)
	}
}

func TestSQLitePersistentJournalUnstagedDirtyHandleCannotAdoptDuringStaging(t *testing.T) {
	for _, path := range []string{"/app.db-wal", "/app.db-journal"} {
		t.Run(path, func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			shadow, err := NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			pending, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.shadowStore = shadow
			fs.pendingIndex = pending

			ino := fs.inodes.Lookup(path, false, 32, time.Now())
			fs.inodes.UpdateRevision(ino, 2)
			fs.recordCommittedRevision(path, 2)
			fh := &FileHandle{
				Ino:      ino,
				Path:     path,
				Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
				OrigSize: 32,
				BaseRev:  2,
			}
			oldPayload := []byte("old sqlite sidecar payload")
			if _, err := fh.Dirty.Write(0, oldPayload); err != nil {
				t.Fatal(err)
			}
			fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
			fs.openHandles.Add(fh)

			// This is the pre-staging race from the production RCA: a sibling
			// has already made rev3 durable, but this sidecar handle still
			// carries rev2-era dirty bytes and has not recorded ShadowStageGen or
			// PendingIndexGen yet. Neither the sibling-revision refresh nor
			// stageShadowLocked may adopt rev3 before staging those bytes.
			fs.recordCommittedRevision(path, 3)
			fs.refreshCommittedRevisionForOpenHandles(path, 3, nil)
			if fh.BaseRev != 2 {
				t.Fatalf("unstaged dirty journal BaseRev = %d after sibling refresh, want 2", fh.BaseRev)
			}
			fh.Lock()
			if err := fs.stageShadowLocked(fh, true); err != nil {
				fh.Unlock()
				t.Fatal(err)
			}
			fh.Unlock()

			if fh.BaseRev != 2 {
				t.Fatalf("unstaged dirty journal BaseRev = %d, want 2 after stageShadowLocked", fh.BaseRev)
			}
			if got := shadow.BaseRev(path); got != 2 {
				t.Fatalf("shadow base rev = %d, want 2", got)
			}
			meta, ok := pending.GetMeta(path)
			if !ok {
				t.Fatal("pending meta missing after staging")
			}
			if meta.BaseRev != 2 {
				t.Fatalf("pending base rev = %d, want 2", meta.BaseRev)
			}
			fh.Lock()
			entry := &CommitEntry{Path: path, Inode: ino, BaseRev: fs.expectedRevisionForHandleLocked(fh), Size: fh.Dirty.Size(), Kind: PendingOverwrite}
			fs.bindCommitEntryToHandleLocked(entry, fh, fh.BaseRev)
			fh.Unlock()
			if entry.PayloadBaseRev != 2 || !entry.PayloadBaseRevSet {
				t.Fatalf("entry payload base = %d set=%t, want rev2", entry.PayloadBaseRev, entry.PayloadBaseRevSet)
			}
			if entry.DurableWatermarkRev != 3 || !entry.DisableAutoResolveLWW {
				t.Fatalf("entry watermark = %d disable_lww=%t, want rev3 fence", entry.DurableWatermarkRev, entry.DisableAutoResolveLWW)
			}
		})
	}
}

func TestSQLitePersistentJournalUnstagedShadowSpillCannotAdoptDuringStaging(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	const path = "/app.db-wal"
	ino := fs.inodes.Lookup(path, false, 0, time.Now())
	fs.inodes.UpdateRevision(ino, 2)
	fs.recordCommittedRevision(path, 2)
	oldPayload := []byte("old sqlite wal shadowspill payload")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:         ino,
		Path:        path,
		Dirty:       fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize:    int64(len(oldPayload)),
		BaseRev:     2,
		ShadowReady: true,
		ShadowSpill: true,
	}
	if _, err := fh.Dirty.Write(0, oldPayload); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fs.openHandles.Add(fh)

	fs.recordCommittedRevision(path, 3)
	fs.refreshCommittedRevisionForOpenHandles(path, 3, nil)
	if fh.BaseRev != 2 {
		t.Fatalf("unstaged ShadowSpill journal BaseRev = %d after sibling refresh, want 2", fh.BaseRev)
	}
	fh.Lock()
	if err := fs.stageShadowLocked(fh, true); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	fh.Unlock()

	if fh.BaseRev != 2 {
		t.Fatalf("unstaged ShadowSpill journal BaseRev = %d, want 2 after stageShadowLocked", fh.BaseRev)
	}
	if got := shadow.BaseRev(path); got != 2 {
		t.Fatalf("shadow base rev = %d, want 2", got)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending meta missing after ShadowSpill staging")
	}
	if meta.BaseRev != 2 || !meta.ShadowSpill {
		t.Fatalf("pending meta base/shadowspill = %d/%t, want 2/true", meta.BaseRev, meta.ShadowSpill)
	}
	fh.Lock()
	entry := &CommitEntry{Path: path, Inode: ino, BaseRev: fs.expectedRevisionForHandleLocked(fh), Size: fh.Dirty.Size(), Kind: PendingOverwrite, ShadowSpill: true}
	fs.bindCommitEntryToHandleLocked(entry, fh, fh.BaseRev)
	fh.Unlock()
	if entry.PayloadBaseRev != 2 || entry.DurableWatermarkRev != 3 || !entry.DisableAutoResolveLWW {
		t.Fatalf("entry payload/watermark/lww = %d/%d/%t, want 2/3/true", entry.PayloadBaseRev, entry.DurableWatermarkRev, entry.DisableAutoResolveLWW)
	}
}

func TestSQLitePersistentJournalImmediateRemoteSyncCannotAdoptStalePayload(t *testing.T) {
	for _, path := range []string{"/app.db-wal", "/app.db-journal"} {
		t.Run(path, func(t *testing.T) {
			var (
				mu       sync.Mutex
				revision int64 = 3
				body           = []byte("rev3 sqlite sidecar bytes")
				attempts []struct {
					expected string
					body     []byte
				}
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.URL.Path != "/v1/fs"+path {
					http.NotFound(w, r)
					return
				}
				data, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				expected := r.Header.Get("X-Dat9-Expected-Revision")
				attempts = append(attempts, struct {
					expected string
					body     []byte
				}{expected: expected, body: append([]byte(nil), data...)})
				if expected != fmt.Sprintf("%d", revision) {
					http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
					return
				}
				revision++
				body = append([]byte(nil), data...)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": revision})
			}))
			defer ts.Close()

			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)

			ino := fs.inodes.Lookup(path, false, int64(len(body)), time.Now())
			fs.inodes.UpdateRevision(ino, 2)
			// A newer same-path sidecar revision has already become durable
			// in this mount, but this handle still owns rev2-era dirty bytes.
			// Immediate remote-sync must let the server CAS reject that stale
			// payload instead of swapping in the newer BaseRev and accepting it
			// as rev4.
			fs.recordCommittedRevision(path, 3)

			oldPayload := []byte("rev2 stale sqlite sidecar bytes")
			fh := &FileHandle{
				Ino:      ino,
				Path:     path,
				Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
				OrigSize: int64(len(oldPayload)),
				BaseRev:  2,
			}
			if _, err := fh.Dirty.Write(0, oldPayload); err != nil {
				t.Fatal(err)
			}
			fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())

			fh.Lock()
			st := fs.flushHandle(context.Background(), fh)
			fh.Unlock()
			if st == gofuse.OK {
				t.Fatal("stale SQLite sidecar remote-sync unexpectedly succeeded")
			}
			if fh.BaseRev != 2 {
				t.Fatalf("stale SQLite sidecar BaseRev = %d, want 2", fh.BaseRev)
			}

			mu.Lock()
			finalRev := revision
			finalBody := append([]byte(nil), body...)
			gotAttempts := append([]struct {
				expected string
				body     []byte
			}(nil), attempts...)
			mu.Unlock()

			if finalRev != 3 {
				t.Fatalf("server revision = %d, want 3; stale payload must not be accepted as rev4", finalRev)
			}
			if string(finalBody) != "rev3 sqlite sidecar bytes" {
				t.Fatalf("server body = %q, want rev3 bytes", finalBody)
			}
			if len(gotAttempts) != 1 {
				t.Fatalf("PUT attempts = %d, want 1", len(gotAttempts))
			}
			if gotAttempts[0].expected != "2" {
				t.Fatalf("PUT expected revision = %q, want stale base rev 2", gotAttempts[0].expected)
			}
			if string(gotAttempts[0].body) != string(oldPayload) {
				t.Fatalf("PUT body = %q, want stale payload attempt for CAS rejection", gotAttempts[0].body)
			}
		})
	}
}

func TestReleaseWriteBackCleanupKeepsNewerGeneration(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls.Add(1)
			http.Error(w, "stale release entry must be rejected before upload", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, writeBack, 0)
	fs.SetWriteBack(writeBack, uploader)
	defer uploader.DrainAll()
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(fs.client, shadow, pending, nil, 1, 8, fs.remoteRoot())
	cq.PathLock = fs.lockRemoteCommitPath
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = cq
	defer cq.DrainAll()

	const path = "/same-path.db"
	oldPayload := []byte("old")
	newPayload := []byte("new")

	ino := fs.inodes.Lookup(path, false, int64(len(oldPayload)), time.Now())
	fs.inodes.UpdateRevision(ino, 2)
	fs.recordCommittedRevision(path, 2)
	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
		BaseRev:  2,
		OrigSize: int64(len(oldPayload)),
	}
	if _, err := fh.Dirty.Write(0, oldPayload); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fh.Lock()
	if err := fs.stageShadowLocked(fh, true); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	fh.Unlock()

	oldGen, _, err := writeBack.PutWithBaseRevAndModeTimings(path, oldPayload, int64(len(oldPayload)), PendingOverwrite, 2, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	fh.WriteBackGen = oldGen
	fh.WriteBackSeq = fh.DirtySeq

	fs.recordCommittedRevision(path, 3)
	fhID := fs.allocateFileHandle(fh)

	newGen, _, err := writeBack.PutWithBaseRevAndModeTimings(path, newPayload, int64(len(newPayload)), PendingOverwrite, 3, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if newGen == oldGen {
		t.Fatal("writeBack generation did not advance")
	}

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	})
	cq.DrainAll()

	meta, ok := writeBack.GetMeta(path)
	if !ok {
		t.Fatal("newer writeBack generation was removed by stale cleanup")
	}
	if meta.Generation != newGen {
		t.Fatalf("writeBack generation = %d, want newer generation %d", meta.Generation, newGen)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d uploads; stale release entry must be rejected before upload", got)
	}
}

func TestCommitQueueStaleGenerationCannotPoisonNewerPendingEntry(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCalls.Add(1)
		http.Error(w, "stale generation must not upload", http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/same-path.db"
	oldPayload := []byte("old handle payload")
	freshPayload := []byte("fresh handle payload")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	oldShadowGen := shadow.ActiveGeneration(path)
	oldPendingGen, err := pending.PutWithBaseRev(path, int64(len(oldPayload)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull(path, freshPayload, 3); err != nil {
		t.Fatal(err)
	}
	freshPendingGen, err := pending.PutWithBaseRev(path, int64(len(freshPayload)), PendingOverwrite, 3)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           2,
		PayloadBaseRev:    2,
		PayloadBaseRevSet: true,
		Size:              int64(len(oldPayload)),
		Kind:              PendingOverwrite,
		ShadowGen:         oldShadowGen,
		PendingIndexGen:   oldPendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d uploads; stale generation must be rejected locally", got)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("newer pending entry was removed by stale commit failure")
	}
	if meta.Generation != freshPendingGen {
		t.Fatalf("pending generation = %d, want newer generation %d", meta.Generation, freshPendingGen)
	}
	if meta.Kind == PendingConflict {
		t.Fatal("newer pending entry was incorrectly marked conflicted by stale commit failure")
	}
	data, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(freshPayload) {
		t.Fatalf("shadow data = %q, want fresh payload", data)
	}
}

func TestCommitQueueFencedConflictDoesNotLWWRebaseOldPayload(t *testing.T) {
	var putCalls atomic.Int32
	var headCalls atomic.Int32
	var getCalls atomic.Int32
	var handlerMu sync.Mutex
	var handlerErr error
	recordHandlerErr := func(err error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			http.Error(w, "fenced conflict must not auto-resolve", http.StatusInternalServerError)
		case http.MethodGet:
			getCalls.Add(1)
			http.Error(w, "fenced conflict must not read for LWW", http.StatusInternalServerError)
		default:
			putCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			if r.Header.Get("X-Dat9-Expected-Revision") != "2" {
				recordHandlerErr(fmt.Errorf("unexpected expected revision %q body=%q", r.Header.Get("X-Dat9-Expected-Revision"), body))
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/checkpoint.db"
	oldPayload := []byte("checkpoint A: 5 rows")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(oldPayload)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:                  path,
		BaseRev:               2,
		PayloadBaseRev:        2,
		PayloadBaseRevSet:     true,
		Size:                  int64(len(oldPayload)),
		Kind:                  PendingOverwrite,
		ShadowGen:             shadowGen,
		PendingIndexGen:       pendingGen,
		DisableAutoResolveLWW: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want exactly initial conflicting upload", got)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0; fenced entry must not enter auto-resolve", got)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0; fenced entry must not read for LWW", got)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending entry should remain for manual recovery after fenced conflict")
	}
	if meta.Kind != PendingConflict {
		t.Fatalf("pending kind = %v, want PendingConflict", meta.Kind)
	}
	handlerMu.Lock()
	err = handlerErr
	handlerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCommitQueueLiveDurableWatermarkRejectsEntryThatBecameStaleAfterEnqueue(t *testing.T) {
	var requestCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCalls.Add(1)
		http.Error(w, "stale payload must be rejected before remote I/O", http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/checkpoint.db"
	oldPayload := []byte("checkpoint A")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(oldPayload)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	var liveWatermark atomic.Int64
	lockEntered := make(chan struct{})
	releaseLock := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseLock)
		})
	}
	defer release()
	cq.DurableWatermark = func(string) int64 {
		return liveWatermark.Load()
	}
	cq.PathLock = func(string) func() {
		enterOnce.Do(func() {
			close(lockEntered)
			<-releaseLock
		})
		return func() {}
	}

	entry := &CommitEntry{
		Path:              path,
		BaseRev:           2,
		PayloadBaseRev:    2,
		PayloadBaseRevSet: true,
		Size:              int64(len(oldPayload)),
		Kind:              PendingOverwrite,
		ShadowGen:         shadowGen,
		PendingIndexGen:   pendingGen,
	}
	if err := cq.Enqueue(entry); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lockEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("commit worker did not enter path lock")
	}

	// Simulate a sibling strict fsync making checkpoint B durable after this
	// entry was already enqueued but before it dispatches its payload.
	liveWatermark.Store(3)
	release()
	cq.DrainAll()

	if got := requestCalls.Load(); got != 0 {
		t.Fatalf("remote requests = %d, want 0; stale payload must be fenced at dispatch", got)
	}
	if entry.DurableWatermarkRev != 3 || !entry.DisableAutoResolveLWW {
		t.Fatalf("entry watermark/lww = %d/%t, want 3/true", entry.DurableWatermarkRev, entry.DisableAutoResolveLWW)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending stale entry should remain for manual recovery")
	}
	if meta.Kind != PendingConflict {
		t.Fatalf("pending kind = %v, want PendingConflict", meta.Kind)
	}
}

func TestCommitQueueRefusesLWWWhenPayloadBaseOlderThanServerRevision(t *testing.T) {
	const path = "/checkpoint.db"
	oldPayload := []byte("checkpoint A: 5 rows")
	freshPayload := []byte("checkpoint B: 12 rows")
	server, ts := newCASFileServer(t, path, 3, freshPayload)
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(oldPayload)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           2,
		PayloadBaseRev:    2,
		PayloadBaseRevSet: true,
		Size:              int64(len(oldPayload)),
		Kind:              PendingOverwrite,
		ShadowGen:         shadowGen,
		PendingIndexGen:   pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	rev, body, puts := server.snapshot()
	if rev != 3 {
		t.Fatalf("server revision = %d, want 3; stale payload must not LWW into rev4", rev)
	}
	if string(body) != string(freshPayload) {
		t.Fatalf("server body = %q, want checkpoint B payload", body)
	}
	if len(puts) != 0 {
		t.Fatalf("successful PUT history = %+v, want no stale LWW write", puts)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending entry should remain for manual recovery")
	}
	if meta.Kind != PendingConflict {
		t.Fatalf("pending kind = %v, want PendingConflict", meta.Kind)
	}
}

func TestShadowSpillSyncCleanupKeepsNewerGeneration(t *testing.T) {
	const path = "/large.db-wal"
	freshPayload := []byte("checkpoint B fresh shadow generation")
	var putCalls atomic.Int32
	var putMu sync.Mutex
	var gotBody []byte
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs"+path {
			http.NotFound(w, r)
			return
		}
		putCalls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErrors.Recordf("read PUT body: %v", err)
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		if r.Header.Get("X-Dat9-Expected-Revision") != "3" {
			handlerErrors.Recordf("expected revision = %q, want 3", r.Header.Get("X-Dat9-Expected-Revision"))
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
			return
		}
		if string(body) != string(freshPayload) {
			handlerErrors.Recordf("PUT body = %q, want fresh payload", body)
			http.Error(w, "stale shadow payload", http.StatusInternalServerError)
			return
		}
		putMu.Lock()
		gotBody = append([]byte(nil), body...)
		putMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":4}`))
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	oldPayload := []byte("checkpoint A old shadow generation")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	oldShadowGen := shadow.ActiveGeneration(path)
	oldPendingGen, err := pending.PutShadowSpillWithMode(path, int64(len(oldPayload)), PendingOverwrite, 2, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Path:              path,
		ShadowReady:       true,
		ShadowSpill:       true,
		ShadowCommitReady: true,
		ShadowStageGen:    oldShadowGen,
		PendingIndexGen:   oldPendingGen,
	}

	// H2 stages a newer generation after H1's upload has returned but before
	// H1 runs cleanup. H1 must not path-wide delete H2's only staged copy.
	if err := shadow.WriteFull(path, freshPayload, 3); err != nil {
		t.Fatal(err)
	}
	freshShadowGen := shadow.ActiveGeneration(path)
	freshPendingGen, err := pending.PutShadowSpillWithMode(path, int64(len(freshPayload)), PendingOverwrite, 3, 0, false)
	if err != nil {
		t.Fatal(err)
	}

	fs.removeShadowPendingStagingGenerationLocked(fh, path, oldShadowGen, oldPendingGen)

	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("newer pending generation was removed by stale cleanup")
	}
	if meta.Generation != freshPendingGen {
		t.Fatalf("pending generation = %d, want fresh generation %d", meta.Generation, freshPendingGen)
	}
	data, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(freshPayload) {
		t.Fatalf("shadow data after cleanup = %q, want fresh payload", data)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           3,
		PayloadBaseRev:    3,
		PayloadBaseRevSet: true,
		Size:              int64(len(freshPayload)),
		Kind:              PendingOverwrite,
		ShadowSpill:       true,
		ShadowGen:         freshShadowGen,
		PendingIndexGen:   freshPendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want fresh generation commit", got)
	}
	putMu.Lock()
	committedBody := append([]byte(nil), gotBody...)
	putMu.Unlock()
	if string(committedBody) != string(freshPayload) {
		t.Fatalf("committed body = %q, want fresh payload", committedBody)
	}
	if pending.HasPending(path) {
		t.Fatal("pending entry should be removed after fresh generation commit")
	}
	if shadow.Has(path) {
		t.Fatal("shadow entry should be removed after fresh generation commit")
	}
	handlerErrors.Check(t)
}

func TestRemoveHandleStagingWithoutGenerationPreservesQueueOwnedStaging(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = writeBack
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	const path = "/queued.db"
	payload := []byte("queue-owned generation")
	writeBackGen, _, err := writeBack.PutWithBaseRevAndModeTimings(path, payload, int64(len(payload)), PendingOverwrite, 3, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, payload, 3); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingOverwrite, 3)
	if err != nil {
		t.Fatal(err)
	}

	// This mirrors the queue-owned state after a CommitEntry has taken over and
	// the producing handle's generation fields have been cleared. A later gen-0
	// handle cleanup must not path-wide delete that queued payload.
	fs.removeHandleStagingLocked(&FileHandle{Path: path})

	wbMeta, ok := writeBack.GetMeta(path)
	if !ok {
		t.Fatal("queue-owned writeback entry was removed by gen-0 handle cleanup")
	}
	if wbMeta.Generation != writeBackGen {
		t.Fatalf("writeback generation = %d, want %d", wbMeta.Generation, writeBackGen)
	}
	pendingMeta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("queue-owned pending entry was removed by gen-0 handle cleanup")
	}
	if pendingMeta.Generation != pendingGen {
		t.Fatalf("pending generation = %d, want %d", pendingMeta.Generation, pendingGen)
	}
	data, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(payload) {
		t.Fatalf("shadow data = %q, want queue-owned payload", data)
	}
	if got := shadow.ActiveGeneration(path); got != shadowGen {
		t.Fatalf("shadow generation = %d, want %d", got, shadowGen)
	}
}

func TestShadowSpillDirectPUTCleanupKeepsNewerGeneration(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	const path = "/small.db-wal"
	oldPayload := []byte("old checkpoint A")
	freshPayload := []byte("fresh checkpoint B")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	oldShadowGen := shadow.ActiveGeneration(path)
	oldPendingGen, err := pending.PutShadowSpill(path, int64(len(oldPayload)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Path:              path,
		ShadowReady:       true,
		ShadowSpill:       true,
		ShadowCommitReady: true,
		ShadowStageGen:    oldShadowGen,
		PendingIndexGen:   oldPendingGen,
	}

	if err := shadow.WriteFull(path, freshPayload, 3); err != nil {
		t.Fatal(err)
	}
	freshShadowGen := shadow.ActiveGeneration(path)
	freshPendingGen, err := pending.PutShadowSpill(path, int64(len(freshPayload)), PendingOverwrite, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the direct-PUT ShadowSpill success tail after H1 uploaded gen 1
	// but H2 staged gen 2 before H1's cleanup. The cache seed must not live-read
	// gen 2 as if it were H1's uploaded payload, and cleanup must only target
	// H1's captured generations.
	if fs.seedReadCacheFromShadowGenerationLocked(path, int64(len(oldPayload)), 4, oldShadowGen) {
		t.Fatal("stale direct-PUT success tail seeded read cache from newer shadow generation")
	}
	fs.markHandleRemoteCommittedLocked(fh, 4)
	fs.removeShadowPendingStagingGenerationLocked(fh, path, oldShadowGen, oldPendingGen)

	if cached, ok := fs.readCache.Get(path, 4); ok {
		t.Fatalf("read cache contains %q for rev4; stale success tail must not cache a live newer generation", cached)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("newer pending generation was removed by stale direct-PUT cleanup")
	}
	if meta.Generation != freshPendingGen {
		t.Fatalf("pending generation = %d, want fresh generation %d", meta.Generation, freshPendingGen)
	}
	data, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(freshPayload) {
		t.Fatalf("shadow data = %q, want fresh payload", data)
	}
	if got := shadow.ActiveGeneration(path); got != freshShadowGen {
		t.Fatalf("shadow generation = %d, want fresh generation %d", got, freshShadowGen)
	}
}
