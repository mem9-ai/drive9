package fuse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestRecoveredCommitWithoutGenerationPreservesStaging(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		name := "async"
		if synchronous {
			name = "sync"
		}
		t.Run(name, func(t *testing.T) {
			var requests, successes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "unowned recovery must not upload", http.StatusInternalServerError)
			}))
			defer server.Close()
			shadow, err := NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			pending, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			const path = "/retained.txt"
			if err := shadow.WriteFull(path, []byte("newer"), 0); err != nil {
				t.Fatal(err)
			}
			generation, err := pending.PutWithBaseRev(path, 5, PendingNew, 0)
			if err != nil {
				t.Fatal(err)
			}
			journal, err := NewJournal(filepath.Join(t.TempDir(), "wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.Close() }()
			cq := NewCommitQueue(newTestClient(server.URL), shadow, pending, journal, 1, 8)
			defer cq.DrainAll()
			cq.OnSuccess = func(*CommitEntry, int64) { successes.Add(1) }
			entry := &CommitEntry{Path: path, Size: 5, Kind: PendingNew, recovered: true, ShadowGen: shadow.ActiveGeneration(path)}
			if synchronous {
				cq.DrainAll()
				if err := cq.CommitNow(context.Background(), entry); !errors.Is(err, syscall.ESTALE) {
					t.Fatalf("unowned sync recovery = %v", err)
				}
			} else {
				if err := cq.Enqueue(entry); err != nil {
					t.Fatal(err)
				}
				cq.DrainAll()
			}
			if requests.Load() != 0 || successes.Load() != 0 {
				t.Fatalf("unowned recovery requests=%d successes=%d", requests.Load(), successes.Load())
			}
			if meta, ok := pending.GetMeta(path); !ok || meta.Generation != generation || meta.Kind != PendingNew {
				t.Fatalf("unowned rejection modified current metadata: %+v/%t", meta, ok)
			}
			if data, err := shadow.ReadAll(path); err != nil || string(data) != "newer" {
				t.Fatalf("retained bytes=%q/%v", data, err)
			}
			if err := journal.Replay(func(entry JournalEntry) {
				if entry.Op == JournalCommit {
					t.Error("unowned rejection wrote a path-only completion")
				}
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPendingRenameConflictRequiresExactGeneration(t *testing.T) {
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/file"
	old, err := idx.PutWithBaseRev(path, 3, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	current, err := idx.PutWithBaseRev(path, 5, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, generation := range []uint64{0, old} {
		lock := idx.acquirePathLock(path)
		err := idx.markRenameConflictLocked(path, generation)
		idx.releasePathLock(path, lock)
		if err != nil {
			t.Fatal(err)
		}
		if meta, ok := idx.GetMeta(path); !ok || meta.Generation != current || meta.Kind != PendingNew || meta.Size != 5 {
			t.Fatalf("stale generation %d poisoned successor: %+v/%t", generation, meta, ok)
		}
	}
	idx.dir = filepath.Join(t.TempDir(), "missing")
	lock := idx.acquirePathLock(path)
	err = idx.markRenameConflictLocked(path, current)
	idx.releasePathLock(path, lock)
	if err == nil {
		t.Fatal("expected conflict persistence failure")
	}
	if meta, ok := idx.GetMeta(path); !ok || meta.Generation != current || meta.Kind != PendingConflict {
		t.Fatalf("persistence failure did not fence memory: %+v/%t", meta, ok)
	}
}
