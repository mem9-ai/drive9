package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestCreateOldInodeCleanupPreservesNewMapping(t *testing.T) {
	for _, oldOpen := range []bool{false, true} {
		t.Run(fmt.Sprint(oldOpen), func(t *testing.T) {
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{"revision":1}`) })
			const path = "/recreated"
			old := fs.inodes.Lookup(path, false, 3, time.Now())
			var oldHandle gofuse.OpenOut
			if oldOpen {
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: old}}, &oldHandle); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			writer, flush := createCloseSyncShadowTestFile(t, fs, "recreated", 0o644)
			_, kept := fs.inodes.GetEntry(old)
			if kept != oldOpen {
				t.Errorf("old inode retained=%t, open=%t", kept, oldOpen)
			}
			if st := fs.Flush(nil, flush); st != gofuse.OK {
				t.Fatal(st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{Fh: flush.Fh})
			fs.Forget(old, 1)
			if oldOpen {
				fs.Release(nil, &gofuse.ReleaseIn{Fh: oldHandle.Fh})
			}
			if got, ok := fs.inodes.GetInode(path); !ok || got != writer.Ino {
				t.Fatalf("old cleanup removed live mapping: %d/%t, want %d", got, ok, writer.Ino)
			}
			if got := fs.inodes.Lookup(path, false, 18, time.Now()); got != writer.Ino {
				t.Fatalf("lookup allocated duplicate inode %d, want %d", got, writer.Ino)
			}
		})
	}
}

func TestInodeRemovalCannotDeleteAnotherIncarnation(t *testing.T) {
	for _, cleanup := range []string{"forget", "unreferenced"} {
		t.Run(cleanup, func(t *testing.T) {
			m := NewInodeToPath()
			old := m.Lookup("/file", false, 0, time.Now())
			m.RemoveLinkPreserve("/file")
			live := m.Lookup("/file", false, 0, time.Now())
			if cleanup == "forget" {
				m.Forget(old, 1)
			} else {
				m.ForgetKeepMapping(old, 1)
				m.RemoveFileIfUnreferenced(old)
			}
			if got, ok := m.GetInode("/file"); !ok || got != live {
				t.Fatalf("mapping=%d/%t, want %d", got, ok, live)
			}
		})
	}
}

func TestCreateClearsWatermarkWithoutNotFound(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) })
	const path = "/watermark-wal"
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
	// No inode and no ENOENT observation: only Create can clear this watermark.
	fs.recordCommittedRevisionWithSize(path, 7, 99)
	writer, _ := createCloseSyncShadowTestFile(t, fs, "watermark-wal", 0o644)
	fs.openHandles.Remove(writer)
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: writer.Ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	got, st, err := readDat9FSTestRange(fs, writer.Ino, out.Fh, 0, 30)
	if err != nil || st != gofuse.OK || string(got) != "close-sync content" {
		t.Fatalf("created read=%q/%v/%v", got, st, err)
	}
}

func TestRecoveryRejectsShortShadowWithoutJournal(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		t.Run(fmt.Sprint(synchronous), func(t *testing.T) {
			var uploads atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					uploads.Add(1)
					_, _ = io.WriteString(w, `{"revision":8}`)
					return
				}
				_, _ = io.WriteString(w, "REMOTE")
			})
			const path = "/torn"
			if _, err := fs.pendingIndex.PutShadowSpill(path, 6, PendingOverwrite, 7); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("TO"), 0o600); err != nil {
				t.Fatal(err)
			}
			idx, err := NewPendingIndex(fs.pendingIndex.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := idx.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			fs.pendingIndex = idx
			cq := NewCommitQueue(fs.client, fs.shadowStore, idx, nil, 1, 8)
			if synchronous {
				cq.DrainAll()
				cq.RecoverPendingSync(context.Background())
			} else {
				cq.RecoverPending()
				cq.DrainAll()
			}
			if got := uploads.Load(); got != 0 {
				t.Fatalf("uploaded truncated recovery %d times", got)
			}
			if idx.shadowReadGeneration(path, fs.shadowStore) != 0 {
				t.Fatal("short shadow became readable")
			}
			ino := fs.inodes.Lookup(path, false, 6, time.Now())
			fs.inodes.UpdateRevision(ino, 7)
			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
			got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 6)
			if err != nil || st != gofuse.OK || string(got) != "REMOTE" {
				t.Fatalf("torn recovery read=%q/%v/%v", got, st, err)
			}
		})
	}
}

func TestRecoveryPreservesSecondFlushAfterSnapshotFailure(t *testing.T) {
	for _, second := range []string{"B", "BBBBBBBB", "BBBBBBBBBBBBBBBB"} {
		for _, spill := range []bool{false, true} {
			t.Run(fmt.Sprintf("size=%d/spill=%t", len(second), spill), func(t *testing.T) {
				t.Parallel()
				var mu sync.Mutex
				var uploaded string
				fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPut {
						t.Errorf("unexpected request %s", r.Method)
						w.WriteHeader(http.StatusNotFound)
						return
					}
					data, _ := io.ReadAll(r.Body)
					mu.Lock()
					uploaded = string(data)
					mu.Unlock()
					_, _ = io.WriteString(w, `{"revision":1}`)
				})
				fs.opts.WritePolicy = WritePolicyWriteBack
				cache, err := NewWriteBackCache(fs.pendingIndex.dir)
				if err != nil {
					t.Fatal(err)
				}
				fs.writeBack = cache
				journalPath := filepath.Join(t.TempDir(), "journal.wal")
				journal, err := NewJournal(journalPath)
				if err != nil {
					t.Fatal(err)
				}
				fs.journal = journal
				fs.pendingIndex.SetJournal(journal)
				shadow := fs.shadowStore
				if !spill {
					fs.shadowStore = nil
				}
				var created gofuse.CreateOut
				if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o644}, "snapshot", &created); st != gofuse.OK {
					t.Fatal(st)
				}
				fs.shadowStore = shadow
				header := gofuse.InHeader{NodeId: created.NodeId}
				write := func(data string) {
					t.Helper()
					if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: created.Fh}, []byte(data)); st != gofuse.OK || int(n) != len(data) {
						t.Fatalf("write=%d/%v", n, st)
					}
				}
				flush := func() {
					t.Helper()
					if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: header, Fh: created.Fh}); st != gofuse.OK {
						t.Fatalf("flush=%v", st)
					}
				}
				write("AAAAAAAA")
				flush()
				if data, ok := cache.Get("/snapshot"); !ok || string(data) != "AAAAAAAA" {
					t.Fatalf("first snapshot=%q/%t", data, ok)
				}
				var attr gofuse.AttrOut
				if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: header, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Fh: created.Fh, Size: 0}}, &attr); st != gofuse.OK {
					t.Fatal(st)
				}
				write(second)
				// Force the second best-effort snapshot to fail before replacing .dat.
				// Shadow staging and WAL publication use the real unmodified directory.
				originalDir := cache.dir
				badDir := filepath.Join(t.TempDir(), "not-a-directory")
				if err := os.WriteFile(badDir, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
				cache.dir = badDir
				flush()
				cache.dir = originalDir
				if data, ok := cache.Get("/snapshot"); !ok || string(data) != "AAAAAAAA" {
					t.Fatalf("failed snapshot replaced A: %q/%t", data, ok)
				}
				shadowDir := shadow.dir
				shadow.Close()
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
				// Restart through the same index -> journal -> migration -> queue order
				// as Mount, without Release repairing the deliberately failed snapshot.
				idx, err := NewPendingIndex(originalDir)
				if err != nil {
					t.Fatal(err)
				}
				if err := idx.RecoverFromDisk(); err != nil {
					t.Fatal(err)
				}
				recovered, err := NewShadowStoreWithQuota(shadowDir, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				replay, err := NewJournal(journalPath)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = replay.Close() }()
				idx.setShadowStore(recovered)
				if _, err := replayJournalIntoPending(replay, idx, recovered); err != nil {
					t.Fatal(err)
				}
				oldCache, err := NewWriteBackCache(originalDir)
				if err != nil {
					t.Fatal(err)
				}
				if err := migrateLegacyWriteBack(recovered, oldCache, idx); err != nil {
					t.Fatal(err)
				}
				data, err := recovered.ReadAll("/snapshot")
				if err != nil || string(data) != second {
					t.Fatalf("acknowledged B replaced during recovery: %q/%v, want %q", data, err, second)
				}
				if meta, ok := idx.GetMeta("/snapshot"); !ok || meta.Size != int64(len(second)) {
					t.Fatalf("recovered metadata=%+v, want B size %d", meta, len(second))
				}
				cq := NewCommitQueue(fs.client, recovered, idx, replay, 1, 8)
				cq.DrainAll()
				if n := cq.RecoverPendingSync(context.Background()); n != 1 {
					t.Fatalf("recovered commits=%d", n)
				}
				mu.Lock()
				defer mu.Unlock()
				if uploaded != second {
					t.Fatalf("uploaded=%q, want acknowledged %q", uploaded, second)
				}
			})
		}
	}
}

func TestRecoveredWALMetadataAdvancesPublicationGeneration(t *testing.T) {
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := WriteBackMeta{Path: "/wal", Size: 3, Generation: 41, Kind: PendingNew, Mtime: time.Now()}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.publishRecoveredMeta(raw); err != nil {
		t.Fatal(err)
	}
	next, err := idx.Put("/wal", 4, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if next <= 41 {
		t.Fatalf("reused restored generation: %d", next)
	}
	if idx.RemoveIfGeneration("/wal", 41) {
		t.Fatal("stale recovery removed successor")
	}
}

func TestShadowProvenanceTransitions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		old       int64
		local     bool
		next      int64
		whole     bool
		want      int64
		wantLocal bool
	}{
		{"recovered partial", 0, false, 7, false, 0, false},
		{"recovered full", 0, false, 7, true, 7, false},
		{"new full", 7, false, 0, true, 0, true},
		{"known partial without base", 7, false, 0, false, 7, false},
		{"known partial refresh", 7, false, 8, false, 8, false},
		{"local partial without base", 0, true, 0, false, 0, true},
		{"local partial adopted base", 0, true, 8, false, 8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sf := &ShadowFile{baseRev: tc.old, localNew: tc.local}
			sf.establishProvenance(tc.next, tc.whole)
			if sf.baseRev != tc.want || sf.localNew != tc.wantLocal {
				t.Fatalf("provenance=%d/%t, want %d/%t", sf.baseRev, sf.localNew, tc.want, tc.wantLocal)
			}
		})
	}
}

func TestFailedMountStagingClosesResources(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(http.ResponseWriter, *http.Request) {})
	j, err := NewJournal(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	fs.journal = j
	fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, j, 1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	fs.journalSyncerCancel = cancel
	if err := fs.shadowStore.WriteFull("/file", []byte("data"), 1); err != nil {
		t.Fatal(err)
	}
	fd := fs.shadowStore.files["/file"].fd
	closeFailedMountStaging(fs)
	if !fs.commitQueue.stopped {
		t.Fatal("commit workers not stopped before staging was closed")
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("syncer not canceled")
	}
	if _, err := fd.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("shadow fd not closed: %v", err)
	}
	if _, err := j.fd.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("journal fd not closed: %v", err)
	}
}

func TestLegacyMigrationFillsOnlyMissingShadow(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewWriteBackCache(idx.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.PutWithBaseRev("/legacy", []byte("data"), 4, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	if err := idx.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyWriteBack(s, cache, idx); err != nil {
		t.Fatal(err)
	}
	data, err := s.ReadAll("/legacy")
	if err != nil || string(data) != "data" {
		t.Fatalf("migration=%q/%v", data, err)
	}
	if gen := idx.recoverShadowSource("/legacy", idx.Generation("/legacy"), s); gen == 0 {
		t.Fatal("valid legacy recovery not bound")
	}
}

func TestReplayFullMetadataKeepsNewerDiskPublication(t *testing.T) {
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutWithBaseRev("/file", 3, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	old, _ := idx.GetMeta("/file")
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := idx.PutWithBaseRev("/file", 4, PendingOverwrite, 8)
	if err != nil {
		t.Fatal(err)
	}
	j, err := NewJournal(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: "/file", Meta: raw})
	mustFsync(t, j)
	if _, err := replayJournalIntoPending(j, idx, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := idx.GetMeta("/file")
	if got.Generation != latest || got.Size != 4 || got.BaseRev != 8 {
		t.Fatalf("newer disk metadata replaced: %+v", got)
	}
}

func TestRecoveryKeepsValidConflictReadable(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("conflict read reached remote: %s", r.Method)
		w.WriteHeader(http.StatusNotFound)
	})
	const path = "/conflict"
	if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("staged"), 0o600); err != nil {
		t.Fatal(err)
	}
	gen, err := fs.pendingIndex.PutShadowSpill(path, 6, PendingOverwrite, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.pendingIndex.MarkConflictIfGeneration(path, gen); err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()
	ino := fs.inodes.Lookup(path, false, 6, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 6)
	if err != nil || st != gofuse.OK || string(got) != "staged" {
		t.Fatalf("conflicted recovery read=%q/%v/%v", got, st, err)
	}
}
