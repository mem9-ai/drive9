package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise recovery through the real queue, including its conflict resolver:
// an intact old snapshot is useful only if it stays readable and its upload
// remains fenced by the revision recorded with that snapshot.
func TestRecoveryCompleteSnapshotFallback(t *testing.T) {
	for _, state := range []string{"short-shadow", "torn-newer-wal", "missing-newer-wal"} {
		for _, synchronous := range []bool{false, true} {
			for _, remoteAdvanced := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/sync=%t/remote-advanced=%t", state, synchronous, remoteAdvanced), func(t *testing.T) {
					const path = "/snapshot"
					const acknowledged = "ACKNOWLEDGED"
					var puts, successes atomic.Int32
					fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
						switch r.Method {
						case http.MethodPut:
							puts.Add(1)
							data, err := io.ReadAll(r.Body)
							if err != nil || string(data) != acknowledged || r.Header.Get("X-Dat9-Expected-Revision") != "7" {
								t.Errorf("upload data=%q err=%v revision=%q", data, err, r.Header.Get("X-Dat9-Expected-Revision"))
							}
							if remoteAdvanced {
								http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
								return
							}
							successes.Add(1)
							_, _ = io.WriteString(w, `{"revision":8}`)
						case http.MethodHead:
							w.Header().Set("X-Dat9-Revision", "8")
							w.Header().Set("Content-Length", "12")
						case http.MethodGet:
							_, _ = io.WriteString(w, "REMOTE-NEWER")
						default:
							t.Errorf("unexpected method %s", r.Method)
							w.WriteHeader(http.StatusBadRequest)
						}
					})
					cache, err := NewWriteBackCache(fs.pendingIndex.dir)
					if err != nil {
						t.Fatal(err)
					}
					if err := cache.PutWithBaseRev(path, []byte(acknowledged), int64(len(acknowledged)), PendingOverwrite, 7); err != nil {
						t.Fatal(err)
					}
					if err := fs.pendingIndex.RecoverFromDisk(); err != nil {
						t.Fatal(err)
					}
					old, _ := fs.pendingIndex.GetMeta(path)
					if state != "missing-newer-wal" {
						if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("AC"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if state != "short-shadow" {
						j, err := NewJournal(filepath.Join(t.TempDir(), "wal"))
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = j.Close() })
						newer := *old
						newer.Mtime = old.Mtime.Add(time.Second)
						newer.Generation++
						newer.Size = 20
						raw, err := json.Marshal(newer)
						if err != nil {
							t.Fatal(err)
						}
						mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: path, Meta: raw})
						mustFsync(t, j)
						if err := replayJournalIntoPending(j, fs.pendingIndex, fs.shadowStore); err != nil {
							t.Fatal(err)
						}
						if meta, ok := fs.pendingIndex.GetMeta(path); !ok || meta.Generation != old.Generation || meta.Size != old.Size {
							t.Fatalf("rejected newer frame discarded complete publication: %+v/%t", meta, ok)
						}
					}
					// Reload the cache as Mount does, so deleting .meta cannot be
					// hidden by this fixture's old in-memory cache entry.
					reloaded, err := NewWriteBackCache(fs.pendingIndex.dir)
					if err != nil {
						t.Fatal(err)
					}
					if err := migrateLegacyWriteBack(fs.shadowStore, reloaded, fs.pendingIndex); err != nil {
						t.Fatal(err)
					}
					if data, err := fs.shadowStore.ReadAll(path); err != nil || string(data) != acknowledged {
						t.Fatalf("recovered data=%q/%v, want intact snapshot", data, err)
					}
					cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
					if synchronous {
						cq.DrainAll()
						cq.RecoverPendingSync(context.Background())
					} else {
						cq.RecoverPending()
						cq.DrainAll()
					}
					if puts.Load() != 1 {
						t.Fatalf("PUTs=%d, want one CAS attempt", puts.Load())
					}
					if remoteAdvanced {
						if successes.Load() != 0 || !fs.pendingIndex.HasPending(path) || fs.pendingIndex.shadowReadGeneration(path, fs.shadowStore) == 0 {
							t.Fatal("conflict lost the locally recoverable snapshot or changed the remote")
						}
						if data, err := fs.shadowStore.ReadAll(path); err != nil || string(data) != acknowledged {
							t.Fatalf("conflict recovery data=%q/%v", data, err)
						}
					} else if successes.Load() != 1 || fs.pendingIndex.HasPending(path) {
						t.Fatal("complete snapshot was not committed and cleaned up")
					}
				})
			}
		}
	}
}

func TestShadowExtentsEstablishProvenanceOnlyAfterFullCoverage(t *testing.T) {
	for _, rev := range []int64{0, 7} {
		t.Run(fmt.Sprint(rev), func(t *testing.T) {
			s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Close)
			const path = "/extents"
			if err := os.WriteFile(s.shadowPath(path), []byte("OLD1OLD2"), 0o600); err != nil {
				t.Fatal(err)
			}
			for part := 0; part < 2; part++ {
				wb := NewWriteBuffer(path, 8, 4)
				wb.totalSize = 8
				if _, err := wb.Write(int64(part*4), []byte("NEWW")); err != nil {
					t.Fatal(err)
				}
				if err := s.WriteExtents(path, wb, rev); err != nil {
					t.Fatal(err)
				}
				pin, ok := s.PinResident(path, rev)
				if ok {
					s.Unpin(pin)
				}
				if want := part == 1 && rev != 0; ok != want {
					t.Fatalf("part=%d revision=%d readable=%t, want %t", part, rev, ok, want)
				}
			}
		})
	}
}

func TestRecoveryFallbackSurvivesSecondRestart(t *testing.T) {
	for _, acknowledged := range []string{"OLD", "OLD!", "OLDER-DATA"} {
		t.Run(fmt.Sprint(len(acknowledged)), func(t *testing.T) {
			cache, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			const path = "/twice"
			if err := cache.PutWithBaseRev(path, []byte(acknowledged), int64(len(acknowledged)), PendingOverwrite, 7); err != nil {
				t.Fatal(err)
			}
			old, _ := cache.GetMeta(path)
			newer := *old
			newer.Size, newer.BaseRev = 4, 8
			newer.Generation++
			newer.Mtime = old.Mtime.Add(time.Second)
			raw, err := json.Marshal(newer)
			if err != nil {
				t.Fatal(err)
			}
			journalPath := filepath.Join(t.TempDir(), "wal")
			j, err := NewJournal(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: path, Meta: raw})
			mustFsync(t, j)
			_ = j.Close()
			shadowDir := t.TempDir()
			for restart := 0; restart < 2; restart++ {
				idx, err := NewPendingIndex(cache.dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := idx.RecoverFromDisk(); err != nil {
					t.Fatal(err)
				}
				s, err := NewShadowStoreWithQuota(shadowDir, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(s.Close)
				if restart == 0 {
					if err := os.WriteFile(s.shadowPath(path), []byte("X"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				j, err := NewJournal(journalPath)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = j.Close() })
				if err := replayJournalIntoPending(j, idx, s); err != nil {
					t.Fatal(err)
				}
				if restart == 0 && (j.seq.Load() < 2 || j.DurableSeq() != j.seq.Load()) {
					t.Fatal("fallback must supersede the rejected frame on stable storage before migration")
				}
				if err := migrateLegacyWriteBack(s, cache, idx); err != nil {
					t.Fatal(err)
				}
				meta, ok := idx.GetMeta(path)
				if !ok || meta.BaseRev != old.BaseRev || meta.Size != old.Size || meta.Generation != old.Generation {
					t.Fatalf("restart %d: old bytes adopted rejected WAL metadata: %+v", restart, meta)
				}
				if data, err := s.ReadAll(path); err != nil || string(data) != acknowledged {
					t.Fatalf("restart %d: payload=%q/%v", restart, data, err)
				}
				// No upload/commit marker before the second crash.
				s.Close()
				_ = j.Close()
			}
		})
	}
}

func TestRecoveryReconcilesBytesBeforeEnqueue(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/one", "/two"} {
		if err := os.WriteFile(s.shadowPath(path), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := idx.PutWithBaseRev(path, 4, PendingOverwrite, 7); err != nil {
			t.Fatal(err)
		}
	}
	// An unbuffered dispatch channel holds recovery at its first enqueue.
	// The second enqueue prevents a misplaced trailing rescan from racing
	// the assertion; no sleeps or scheduler assumptions are required.
	cq := &CommitQueue{index: idx, shadows: s, maxPending: 2, workCh: make(chan *CommitEntry)}
	done := make(chan struct{})
	go func() {
		cq.RecoverPending()
		close(done)
	}()
	<-cq.workCh
	if got := s.PendingBytes(); got != 8 {
		t.Errorf("before first worker receives data: pending bytes=%d, want 8", got)
	}
	<-cq.workCh
	<-done
}

func TestRecoveryFallbackPublicationFailure(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/file"
	if err := cache.PutWithBaseRev(path, []byte("OLD"), 3, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	idx, err := NewPendingIndex(cache.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	old, _ := idx.GetMeta(path)
	newer := *old
	newer.Size, newer.BaseRev = 4, 8
	newer.Mtime = old.Mtime.Add(time.Second)
	raw, err := json.Marshal(newer)
	if err != nil {
		t.Fatal(err)
	}
	j, err := NewJournal(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: path, Meta: raw})
	mustFsync(t, j)
	// Replay can still read the WAL, but publishing its fallback must fail.
	_ = j.Close()
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := replayJournalIntoPending(j, idx, s); err == nil {
		t.Fatal("failed fallback publication must stop recovery before migration")
	}
	if meta, ok := idx.GetMeta(path); !ok || meta.BaseRev != 7 || meta.Size != 3 {
		t.Fatal("failed recovery erased the durable snapshot metadata")
	}
}

func TestLegacyMigrationRejectsIncompleteSnapshot(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/file"
	if err := cache.PutWithBaseRev(path, []byte("complete"), 8, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cache.datFile(path), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.shadowPath(path), []byte("AC"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyWriteBack(s, cache, nil); err != nil {
		t.Fatal(err)
	}
	if data, err := s.ReadAll(path); err != nil || string(data) != "AC" {
		t.Fatalf("incomplete backup replaced shadow: %q/%v", data, err)
	}
	if pin, ok := s.PinResident(path, 7); ok {
		s.Unpin(pin)
		t.Fatal("incomplete backup established revision provenance")
	}
}
