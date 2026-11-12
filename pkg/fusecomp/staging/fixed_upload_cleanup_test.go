package staging

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFixedUploadPendingCleanupPreservesNewerGeneration(t *testing.T) {
	for _, newer := range []bool{false, true} {
		dir := t.TempDir()
		j, err := NewJournal(filepath.Join(dir, "journal.wal"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = j.Close() }()
		idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
		if err != nil {
			t.Fatal(err)
		}
		idx.SetJournal(j)
		old, err := idx.Put("/file", 6, PendingNew)
		if err != nil {
			t.Fatal(err)
		}
		if err := j.PrepareFixedUpload("original", "/file", []byte(`{"generation":1}`)); err != nil {
			t.Fatal(err)
		}
		var next uint64
		if newer {
			next, err = idx.Put("/file", 9, PendingNew)
			if err != nil {
				t.Fatal(err)
			}
		}
		matched, err := idx.RetireFixedUploadGeneration("/file", old)
		if err != nil || matched == newer {
			t.Fatal(matched, err)
		}
		// A completion is still recoverable until its owner explicitly settles.
		if pending, err := j.PendingFixedUploads(); err != nil || len(pending) != 1 {
			t.Fatal(pending, err)
		}
		fresh, err := NewPendingIndex(idx.Dir())
		if err != nil {
			t.Fatal(err)
		}
		if err := fresh.RecoverFromDisk(); err != nil {
			t.Fatal(err)
		}
		if err := ReplayJournalIntoPending(j, fresh, nil); err != nil {
			t.Fatal(err)
		}
		meta, exists := fresh.GetMeta("/file")
		if exists != newer || exists && (meta.Generation != next || meta.Size != 9) {
			t.Fatal(meta, exists)
		}
		if _, err := idx.RetireFixedUploadGeneration("/file", old); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFixedUploadPendingCleanupRetriesDiskFailureAndAbsentIndex(t *testing.T) {
	for _, absent := range []bool{false, true} {
		dir := t.TempDir()
		j, err := NewJournal(filepath.Join(dir, "journal.wal"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = j.Close() }()
		idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
		if err != nil {
			t.Fatal(err)
		}
		idx.SetJournal(j)
		gen, err := idx.Put("/file", 6, PendingNew)
		if err != nil {
			t.Fatal(err)
		}
		if absent {
			idx.Remove("/file") // old WAL frame remains; absence must still fence it.
		} else {
			metaPath := filepath.Join(idx.Dir(), HashPath("/file")+".meta")
			if err := os.Mkdir(metaPath, 0700); err != nil {
				t.Fatal(err)
			}
			blocker := filepath.Join(metaPath, "block")
			if err := os.WriteFile(blocker, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := idx.RetireFixedUploadGeneration("/file", gen); err == nil {
				t.Fatal("cleanup failure returned success")
			}
			if meta, ok := idx.GetMeta("/file"); !ok || meta.Generation != gen {
				t.Fatal("failed cleanup forgot its generation", meta)
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(metaPath); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := idx.RetireFixedUploadGeneration("/file", gen); err != nil {
			t.Fatal(err)
		}
		fresh, err := NewPendingIndex(idx.Dir())
		if err != nil {
			t.Fatal(err)
		}
		if err := fresh.RecoverFromDisk(); err != nil {
			t.Fatal(err)
		}
		if err := ReplayJournalIntoPending(j, fresh, nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := fresh.GetMeta("/file"); ok {
			t.Fatal("retired generation resurrected")
		}
	}
}

func TestFixedUploadCacheCleanupPreservesNewerAndRetriesFailure(t *testing.T) {
	for _, mode := range []string{"same", "newer", "failed"} {
		t.Run(mode, func(t *testing.T) {
			c, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put("/file", []byte("old"), 3, PendingNew); err != nil {
				t.Fatal(err)
			}
			old, _ := c.GetMeta("/file")
			if mode == "newer" {
				if err := c.Put("/file", []byte("newer"), 5, PendingNew); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "failed" {
				if err := os.Remove(c.DatFile("/file")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(c.DatFile("/file"), 0700); err != nil {
					t.Fatal(err)
				}
				blocker := filepath.Join(c.DatFile("/file"), "block")
				if err := os.WriteFile(blocker, nil, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := c.RetireFixedUploadGeneration("/file", old.Generation); err == nil {
					t.Fatal("failed deletion reported success")
				}
				if meta, ok := c.GetMeta("/file"); !ok || meta.Generation != old.Generation {
					t.Fatal("failed cleanup forgot generation", meta)
				}
				if err := os.Remove(blocker); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(c.DatFile("/file")); err != nil {
					t.Fatal(err)
				}
			}
			matched, err := c.RetireFixedUploadGeneration("/file", old.Generation)
			if err != nil || matched == (mode == "newer") {
				t.Fatal(matched, err)
			}
			fresh, err := NewWriteBackCache(c.dir)
			if err != nil {
				t.Fatal(err)
			}
			data, exists := fresh.Get("/file")
			if exists != (mode == "newer") || exists && string(data) != "newer" {
				t.Fatal(string(data), exists)
			}
		})
	}
}

func TestFixedUploadCleanupRejectsUnknownGenerationAndJournalFailure(t *testing.T) {
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put("/file", 3, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.RetireFixedUploadGeneration("/file", 0); err == nil {
		t.Fatal("zero generation removed metadata")
	}
	journal, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	idx.SetJournal(journal)
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.RetireFixedUploadGeneration("/file", gen); err == nil {
		t.Fatal("failed marker was treated as durable")
	}
	if meta, ok := idx.GetMeta("/file"); !ok || meta.Generation != gen {
		t.Fatal(meta)
	}
	fresh, err := NewPendingIndex(idx.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if meta, ok := fresh.GetMeta("/file"); !ok || meta.Generation != gen {
		t.Fatal("failed marker removed disk metadata", meta)
	}
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put("/file", []byte("old"), 3, PendingNew); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.RetireFixedUploadGeneration("/file", 0); err == nil {
		t.Fatal("zero generation removed cache")
	}
	if data, ok := cache.Get("/file"); !ok || string(data) != "old" {
		t.Fatal(string(data), ok)
	}
}
