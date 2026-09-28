package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLayerJournalCompactsSupersededPublications(t *testing.T) {
	idx, shadows := newLayerShutdownState(t)
	j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	idx.SetJournal(j)
	if err := shadows.WriteFull("/a", []byte("latest"), 0); err != nil {
		t.Fatal(err)
	}
	for range 50 {
		gen, err := idx.Put("/a", 6, PendingOverwrite)
		if err != nil {
			t.Fatal(err)
		}
		if err := idx.MarkLayerCommittedIfGeneration("/a", gen, 0, 0600, true, LayerCacheIdentity{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	var entries []JournalEntry
	if err := j.Replay(func(e JournalEntry) { entries = append(entries, e) }); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Op != JournalPendingMeta {
		t.Fatalf("compaction kept %d frames, want the last complete publication", len(entries))
	}
	recovered, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, recovered, shadows); err != nil {
		t.Fatal(err)
	}
	meta, ok := recovered.GetMeta("/a")
	if !ok || !meta.LayerClean || meta.Mode != 0600 || meta.Size != 6 {
		t.Fatalf("latest clean publication lost: %+v", meta)
	}
}

func TestLayerJournalCompactsWhileMountedAndOnShutdown(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "while_mounted"}[automatic], func(t *testing.T) {
			idx, shadows := newLayerShutdownState(t)
			j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = j.Close() })
			if automatic {
				j.compactAfterBytes = 1024
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { j.SyncLoop(ctx, time.Millisecond); close(done) }()
				t.Cleanup(func() { cancel(); <-done })
			}
			idx.SetJournal(j)
			if err := shadows.WriteFull("/a", []byte("data"), 0); err != nil {
				t.Fatal(err)
			}
			for i := range 100 {
				if _, err := idx.PutLayerCache("/a", 4, 0, 0600, true, LayerCacheIdentity{LayerID: "layer-1", EntrySeq: int64(i + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			if automatic {
				deadline := time.Now().Add(time.Second)
				for {
					info, err := os.Stat(j.path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Size() <= 2*j.compactAfterBytes {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("background WAL compaction did not run: %d bytes", info.Size())
					}
					time.Sleep(time.Millisecond)
				}
			} else {
				fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
				fs.journal, fs.pendingIndex, fs.shadowStore = j, idx, shadows
				if err := fs.flushAll(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(j.path)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				scanJournalFrames(data, func(JournalEntry, []byte) { count++ })
				if count != 1 {
					t.Fatalf("clean shutdown retained %d frames", count)
				}
			}
			restarted, err := NewJournal(j.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = restarted.Close() }()
			recovered, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := replayJournalIntoPending(restarted, recovered, nil); err != nil {
				t.Fatal(err)
			}
			meta, ok := recovered.GetMeta("/a")
			if !ok || !meta.LayerClean || meta.LayerID != "layer-1" || meta.LayerEntrySeq != 100 {
				t.Fatalf("latest publication identity lost: %+v", meta)
			}
		})
	}
}

func TestJournalCompactionPreservesLaterRecoveryFrames(t *testing.T) {
	for _, done := range []JournalOp{JournalCommit, JournalUnlink} {
		t.Run(map[JournalOp]string{JournalCommit: "commit", JournalUnlink: "unlink"}[done], func(t *testing.T) {
			j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = j.Close() })
			meta, err := json.Marshal(WriteBackMeta{Path: "/a", Size: 4, Kind: PendingOverwrite})
			if err != nil {
				t.Fatal(err)
			}
			mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: "/a", Meta: meta})
			mustAppend(t, j, JournalEntry{Op: done, Path: "/a"})
			mustAppend(t, j, JournalEntry{Op: JournalPendingMeta, Path: "/a", Meta: meta})
			mustAppend(t, j, JournalEntry{Op: JournalFsync, Path: "/a", Length: 7, BaseRev: 3})
			mustAppend(t, j, JournalEntry{Op: JournalWrite, Path: "/other", Length: 2})
			if err := j.Compact(); err != nil {
				t.Fatal(err)
			}
			var got []JournalEntry
			if err := j.Replay(func(e JournalEntry) { got = append(got, e) }); err != nil {
				t.Fatal(err)
			}
			if len(got) != 3 || got[0].Op != JournalPendingMeta || got[1].Op != JournalFsync || got[2].Path != "/other" {
				t.Fatalf("newer recovery frames changed: %+v", got)
			}
		})
	}
}

func TestJournalConcurrentCompactionKeepsLatestPublications(t *testing.T) {
	j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	j.compactAfterBytes = 1024
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx.SetJournal(j)
	const writers, publications = 4, 20
	done := make(chan error, writers)
	for writer := range writers {
		go func() {
			path := fmt.Sprintf("/file-%d", writer)
			for seq := range publications {
				if _, err := idx.PutLayerCache(path, int64(seq), 0, 0600, true, LayerCacheIdentity{LayerID: "layer-1", EntrySeq: int64(seq + 1)}); err != nil {
					done <- err
					return
				}
				if err := j.Compact(); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for range writers {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	recovered, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, recovered, nil); err != nil {
		t.Fatal(err)
	}
	for writer := range writers {
		meta, ok := recovered.GetMeta(fmt.Sprintf("/file-%d", writer))
		if !ok || !meta.LayerClean || meta.LayerEntrySeq != publications || meta.Size != publications-1 {
			t.Errorf("concurrent compaction lost latest publication: %+v", meta)
		}
	}
}
