package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newCloseSyncShadowTestFS(t *testing.T, handler http.HandlerFunc) *Dat9FS {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	var err error
	// Durability tests do not depend on the host disk free-space ratio.
	fs.shadowStore, err = NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func createCloseSyncShadowTestFile(t *testing.T, fs *Dat9FS, name string, mode uint32) (*FileHandle, *gofuse.FlushIn) {
	t.Helper()
	var out gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: mode}, name, &out); st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	data := []byte("close-sync content")
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}, data); st != gofuse.OK || int(n) != len(data) {
		t.Fatalf("Write = %d, %v", n, st)
	}
	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok || !fh.ShadowSpill || fh.ShadowStageGen == 0 {
		t.Fatal("expected a generation-bound shadow handle")
	}
	return fh, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}
}

func TestCloseSyncShadowUploadSkipsLocalSyncOnlyForCloseSync(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     WritePolicy
		generation bool
		syncMode   SyncMode
		pending    bool
		wantRemote bool
	}{
		{"close-sync", WritePolicyCloseSync, true, SyncStrict, false, true},
		{"writeback", WritePolicyWriteBack, true, SyncStrict, false, false},
		{"legacy-close-sync", WritePolicyCloseSync, false, SyncStrict, false, false},
		{"interactive-close-sync", WritePolicyCloseSync, true, SyncInteractive, false, false},
		{"auto-close-sync", WritePolicyCloseSync, true, SyncAuto, false, false},
		{"staged-close-sync", WritePolicyCloseSync, true, SyncStrict, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				puts.Add(1)
				body, _ := io.ReadAll(r.Body)
				if string(body) != "close-sync content" || r.Header.Get("X-Dat9-Expected-Revision") != "0" {
					t.Errorf("upload = %q, revision = %q", body, r.Header.Get("X-Dat9-Expected-Revision"))
				}
				_, _ = io.WriteString(w, `{"revision":1}`)
			})
			fh, _ := createCloseSyncShadowTestFile(t, fs, "sync.txt", 0o644)
			fs.syncMode = tc.syncMode
			if tc.pending {
				if _, err := fs.pendingIndex.PutShadowSpill(fh.Path, fh.Dirty.Size(), PendingNew, 0); err != nil {
					t.Fatal(err)
				}
			}
			makeCloseSyncShadowUnsyncable(t, fs.shadowStore, fh.Path)
			fh.Lock()
			fh.WritePolicy = tc.policy
			if !tc.generation {
				fh.ShadowStageGen = 0
			}
			st := fs.syncHandleToRemoteWithoutAppendLogLocked(context.Background(), fh, shadowUploadRemoteDurable)
			dirty := fh.Dirty.HasDirtyParts()
			fh.Unlock()
			if tc.wantRemote {
				if st != gofuse.OK || puts.Load() != 1 || dirty {
					t.Fatalf("close-sync: status=%v puts=%d dirty=%t", st, puts.Load(), dirty)
				}
			} else if st == gofuse.OK || puts.Load() != 0 || !dirty {
				t.Fatalf("local sync failure: status=%v puts=%d dirty=%t", st, puts.Load(), dirty)
			}
		})
	}
}

func TestCloseSyncShadowUploadWaitsForRemoteAck(t *testing.T) {
	started, allow := make(chan struct{}), make(chan struct{})
	var release, startedOnce sync.Once
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPut || string(body) != "close-sync content" {
			t.Errorf("unexpected upload: %s %q", r.Method, body)
		}
		startedOnce.Do(func() { close(started) })
		<-allow
		_, _ = io.WriteString(w, `{"revision":1}`)
	})
	t.Cleanup(func() { release.Do(func() { close(allow) }) })
	fh, input := createCloseSyncShadowTestFile(t, fs, "pinned.txt", 0o644)
	done := make(chan gofuse.Status, 1)
	go func() { done <- fs.Flush(nil, input) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	select {
	case st := <-done:
		t.Fatalf("Flush returned before remote acknowledgement: %v", st)
	default:
	}
	release.Do(func() { close(allow) })
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Flush: %v", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not finish")
	}
	if fh.Dirty.HasDirtyParts() || fh.BaseRev != 1 || fs.shadowStore.Has(fh.Path) {
		t.Fatal("successful upload did not finalize the committed generation")
	}
}

func TestCloseSyncShadowUploadFailureRetainsInProcessRetryableData(t *testing.T) {
	var puts atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		if puts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"revision":1}`)
	})
	fh, input := createCloseSyncShadowTestFile(t, fs, "retry.txt", 0o644)
	gen := fs.shadowStore.ActiveGeneration(fh.Path)
	if st := fs.Flush(nil, input); st == gofuse.OK {
		t.Fatal("failed upload returned success")
	}
	if !fh.Dirty.HasDirtyParts() || fs.shadowStore.ActiveGeneration(fh.Path) != gen || !fh.IsNew {
		t.Fatal("failed upload changed dirty state or shadow ownership")
	}
	data, err := fs.shadowStore.ReadAll(fh.Path)
	if err != nil || string(data) != "close-sync content" {
		t.Fatalf("retry data = %q, %v", data, err)
	}
	if st := fs.Flush(nil, input); st != gofuse.OK || puts.Load() != 2 {
		t.Fatalf("retry: status=%v puts=%d", st, puts.Load())
	}
}

func TestShadowRemoteDurabilityRejectsStaleGeneration(t *testing.T) {
	var calls atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	fh, _ := createCloseSyncShadowTestFile(t, fs, "stale.txt", 0o644)
	gen := fs.shadowStore.ActiveGeneration(fh.Path)
	if _, err := fs.shadowStore.WriteAt(fh.Path, 0, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	_, err := uploadFromShadowRemote(context.Background(), fs.client, fs.shadowStore, fh.Path, fh.Path, 0, gen, shadowUploadRemoteDurable)
	if !errors.Is(err, errCommitPayloadStale) || calls.Load() != 0 {
		t.Fatalf("stale upload: err=%v remote calls=%d", err, calls.Load())
	}
}

// A live pipe fd cannot fsync, but can be closed exactly once by the store.
// Uploads open their own fd on the unchanged shadow path. This injects an I/O
// error without leaving a closed descriptor in the store's ownership map.
func makeCloseSyncShadowUnsyncable(t *testing.T, shadows *ShadowStore, path string) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	shadows.mu.Lock()
	defer shadows.mu.Unlock()
	original := shadows.files[path].fd
	shadows.files[path].fd = reader
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShadowRemoteDurabilityRejectsMissingGeneration(t *testing.T) {
	var calls atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	fh, _ := createCloseSyncShadowTestFile(t, fs, "missing-gen.txt", 0o644)
	_, err := uploadFromShadowRemote(context.Background(), fs.client, fs.shadowStore, fh.Path, fh.Path, 0, 0, shadowUploadRemoteDurable)
	if !errors.Is(err, errInvalidShadowUpload) || calls.Load() != 0 {
		t.Fatalf("unfenced upload: err=%v calls=%d", err, calls.Load())
	}
}

func TestCloseSyncInteractiveFsyncKeepsLocalBarrier(t *testing.T) {
	var calls atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	fs.syncMode = SyncInteractive
	journal, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	fs.journal = journal
	fs.pendingIndex.SetJournal(journal)
	fh, input := createCloseSyncShadowTestFile(t, fs, "staged.txt", 0o644)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: input.InHeader, Fh: input.Fh}); st != gofuse.OK {
		t.Fatalf("interactive Fsync: %v", st)
	}
	if fh.PendingIndexGen == 0 {
		t.Fatal("interactive fsync did not stage recovery metadata")
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: input.InHeader, Fh: input.Fh}, []byte("newer")); st != gofuse.OK {
		t.Fatal(st)
	}
	makeCloseSyncShadowUnsyncable(t, fs.shadowStore, fh.Path)
	if st := fs.Flush(nil, input); st == gofuse.OK || calls.Load() != 0 {
		t.Fatalf("staged close skipped local barrier: status=%v calls=%d", st, calls.Load())
	}
}

func TestCloseSyncStrictFsyncKeepsLocalBarrier(t *testing.T) {
	var calls atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	fh, input := createCloseSyncShadowTestFile(t, fs, "fsync.txt", 0o644)
	makeCloseSyncShadowUnsyncable(t, fs.shadowStore, fh.Path)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: input.InHeader, Fh: input.Fh}); st == gofuse.OK || calls.Load() != 0 {
		t.Fatalf("explicit Fsync skipped local barrier: status=%v calls=%d", st, calls.Load())
	}
}

func TestCloseSyncShadowCommitHasNoRecoveryUpload(t *testing.T) {
	for _, truncateShadow := range []bool{false, true} {
		name := "surviving-shadow"
		if truncateShadow {
			name = "torn-shadow"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPut {
					t.Errorf("unexpected recovery request: %s", r.Method)
				}
				_, _ = io.WriteString(w, `{"revision":1}`)
			})
			journalPath := filepath.Join(t.TempDir(), "journal.wal")
			journal, err := NewJournal(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = journal.Close() })
			fs.journal = journal
			fs.pendingIndex.SetJournal(journal)
			fh, input := createCloseSyncShadowTestFile(t, fs, "recover.txt", 0o644)
			if st := fs.Flush(nil, input); st != gofuse.OK {
				t.Fatal(st)
			}
			if err := journal.FsyncShared(); err != nil {
				t.Fatal(err)
			}
			fs.shadowStore.Close()
			// Model a lost unlink and, optionally, lost shadow page-cache bytes.
			// Reopening the stores must not turn either orphan into an upload.
			data := []byte("close-sync content")
			if truncateShadow {
				data = data[:3]
			}
			if err := os.WriteFile(fs.shadowStore.shadowPath(fh.Path), data, 0o600); err != nil {
				t.Fatal(err)
			}
			recoveredShadow, err := NewShadowStore(fs.shadowStore.dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(recoveredShadow.Close)
			if err := recoveredShadow.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			recoveredPending, err := NewPendingIndex(fs.pendingIndex.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := recoveredPending.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			replay, err := NewJournal(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = replay.Close() })
			if err := replayJournalIntoPending(replay, recoveredPending, recoveredShadow); err != nil {
				t.Fatal(err)
			}
			cq := NewCommitQueue(fs.client, recoveredShadow, recoveredPending, replay, 1, 8)
			cq.RecoverPending()
			cq.DrainAll()
			if recoveredPending.HasPending(fh.Path) || calls.Load() != 1 {
				t.Fatalf("acknowledged path replayed: pending=%t requests=%d", recoveredPending.HasPending(fh.Path), calls.Load())
			}
		})
	}
}

func TestCloseSyncShadowUploadEmptyAndMultipart(t *testing.T) {
	for _, size := range []int64{0, 256 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			expected := int64(0)
			recorder := newMultipartUploadRecorder(t, "/payload.bin", size, &expected)
			fs := newCloseSyncShadowTestFS(t, recorder.server.Config.Handler.ServeHTTP)
			fs.client.SetSmallFileThresholdForTests(128 * 1024)
			fh, input := createCloseSyncShadowTestFile(t, fs, "payload.bin", 0o644)
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: input.InHeader, Fh: input.Fh, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Size: uint64(size),
			}}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatal(st)
			}
			makeCloseSyncShadowUnsyncable(t, fs.shadowStore, fh.Path)
			if st := fs.Flush(nil, input); st != gofuse.OK {
				t.Fatal(st)
			}
			if size == 0 && recorder.directFilePuts.Load() != 1 || size > 0 && recorder.completeCalls.Load() != 1 {
				t.Fatalf("upload route: direct=%d multipart=%d", recorder.directFilePuts.Load(), recorder.completeCalls.Load())
			}
		})
	}
}

func TestCloseSyncAppendLogFsyncFallbackKeepsLocalBarrier(t *testing.T) {
	var calls atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"revision":1}`)
	})
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/fsync.txt"})
	fh, input := createCloseSyncShadowTestFile(t, fs, "fsync.txt", 0o644)
	makeCloseSyncShadowUnsyncable(t, fs.shadowStore, fh.Path)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: input.InHeader, Fh: input.Fh}); st == gofuse.OK || calls.Load() != 0 {
		t.Fatalf("append-log Fsync fallback skipped local barrier: status=%v calls=%d", st, calls.Load())
	}
}

func TestCloseSyncShadowDurabilityGuard(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *Dat9FS, *FileHandle, *StagingGens)
		want   shadowUploadDurability
	}{
		{"eligible", func(*testing.T, *Dat9FS, *FileHandle, *StagingGens) {}, shadowUploadRemoteDurable},
		{"interactive", func(_ *testing.T, fs *Dat9FS, _ *FileHandle, _ *StagingGens) { fs.syncMode = SyncInteractive }, shadowUploadLocalDurable},
		{"auto", func(_ *testing.T, fs *Dat9FS, _ *FileHandle, _ *StagingGens) { fs.syncMode = SyncAuto }, shadowUploadLocalDurable},
		{"writeback", func(_ *testing.T, _ *Dat9FS, fh *FileHandle, _ *StagingGens) { fh.WritePolicy = WritePolicyWriteBack }, shadowUploadLocalDurable},
		{"o-sync", func(_ *testing.T, fs *Dat9FS, fh *FileHandle, _ *StagingGens) {
			fh.WritePolicy = fs.writePolicyForOpen(syscall.O_SYNC)
		}, shadowUploadLocalDurable},
		{"o-dsync", func(_ *testing.T, fs *Dat9FS, fh *FileHandle, _ *StagingGens) {
			fh.WritePolicy = fs.writePolicyForOpen(syscall.O_DSYNC)
		}, shadowUploadLocalDurable},
		{"missing-generation", func(_ *testing.T, _ *Dat9FS, _ *FileHandle, gens *StagingGens) { gens.ShadowGen = 0 }, shadowUploadLocalDurable},
		{"owned-pending", func(_ *testing.T, _ *Dat9FS, _ *FileHandle, gens *StagingGens) { gens.PendingIndexGen = 1 }, shadowUploadLocalDurable},
		{"owned-writeback", func(_ *testing.T, _ *Dat9FS, _ *FileHandle, gens *StagingGens) { gens.WriteBackGen = 1 }, shadowUploadLocalDurable},
		{"queued-shadow", func(_ *testing.T, _ *Dat9FS, fh *FileHandle, _ *StagingGens) { fh.ShadowCommitReady = true }, shadowUploadLocalDurable},
		{"queued-writeback", func(_ *testing.T, _ *Dat9FS, fh *FileHandle, _ *StagingGens) { fh.WriteBackSeq = 1 }, shadowUploadLocalDurable},
		{"path-pending", func(t *testing.T, fs *Dat9FS, fh *FileHandle, _ *StagingGens) {
			if _, err := fs.pendingIndex.PutShadowSpill(fh.Path, fh.Dirty.Size(), PendingNew, 0); err != nil {
				t.Fatal(err)
			}
		}, shadowUploadLocalDurable},
		{"path-writeback", func(t *testing.T, fs *Dat9FS, fh *FileHandle, _ *StagingGens) {
			var err error
			fs.writeBack, err = NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = fs.writeBack.Put(fh.Path, []byte("staged"), 6, PendingNew); err != nil {
				t.Fatal(err)
			}
		}, shadowUploadLocalDurable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newCloseSyncShadowTestFS(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected remote request") })
			fh, _ := createCloseSyncShadowTestFile(t, fs, "guard.txt", 0o644)
			fh.Lock()
			defer fh.Unlock()
			gens := fs.captureHandleStagingGensLocked(fh)
			tc.change(t, fs, fh, &gens)
			if got := fs.foregroundShadowUploadDurabilityLocked(fh, gens); got != tc.want {
				t.Fatalf("durability=%v, want %v", got, tc.want)
			}
		})
	}
}
