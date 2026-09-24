package fuse

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadOnlyShadowWriteBackOwnsOnlyDat(t *testing.T) {
	for _, name := range []string{"plain.bin", "app.db-wal"} {
		for _, missingIndex := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/missingIndex=%t", name, missingIndex), func(t *testing.T) {
				fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "REMOTE") })
				fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
				if missingIndex {
					fs.pendingIndex = nil
				}
				path := "/" + name
				cache, err := NewWriteBackCache(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if err := cache.PutWithBaseRev(path, []byte("GOOD"), 4, PendingOverwrite, 1); err != nil {
					t.Fatal(err)
				}
				fs.writeBack, err = NewWriteBackCache(cache.dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("TORN"), 0o600); err != nil {
					t.Fatal(err)
				}
				ino := fs.inodes.Lookup(path, false, 4, time.Now())
				fs.inodes.UpdateRevision(ino, 2)
				var out gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
				if reader, _ := fs.fileHandles.Get(out.Fh); reader.ShadowPinned {
					t.Fatal("Open authorized a shadow using only write-back metadata")
				}
				got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 30)
				if err != nil || st != gofuse.OK || string(got) != "GOOD" {
					t.Fatalf("read=%q/%v/%v", got, st, err)
				}
			})
		}
	}
}

func TestReadOnlyShadowRecreatedInode(t *testing.T) {
	for _, name := range []string{"plain.bin", "app.db-wal"} {
		t.Run(name, func(t *testing.T) {
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) })
			fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
			path := "/" + name
			old := fs.inodes.Lookup(path, false, 4, time.Now())
			fs.inodes.UpdateRevision(old, 7)
			fs.recordCommittedRevisionWithSize(path, 7, 4)
			if !fs.inodes.AddAlias(old, "/surviving-alias", "old-resource", 2, false, 4, time.Now()) {
				t.Fatal("add alias")
			}
			fs.cacheRemoteNotFoundPath(path)
			writer, _ := createCloseSyncShadowTestFile(t, fs, name, 0o644)
			fs.openHandles.Remove(writer)
			if writer.Ino == old || fs.inodes.GetRevision(old) != 7 {
				t.Fatal("Create reused or reset surviving inode")
			}
			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: writer.Ino}}, &out); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
			got, st, err := readDat9FSTestRange(fs, writer.Ino, out.Fh, 0, 30)
			if err != nil || st != gofuse.OK || string(got) != "close-sync content" {
				t.Fatalf("read recreated=%q/%v/%v", got, st, err)
			}
		})
	}
}

func TestReadOnlyShadowPendingContentGeneration(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "remote") })
	const path = "/generation.bin"
	ino := fs.inodes.Lookup(path, false, 6, time.Now())
	fs.inodes.UpdateRevision(ino, 9)
	if err := fs.shadowStore.WriteFull(path, []byte("staged"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.pendingIndex.PutShadowSpill(path, 6, PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	check := func(want string) {
		t.Helper()
		got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 6)
		if err != nil || st != gofuse.OK || string(got) != want {
			t.Fatalf("read=%q/%v/%v, want %q", got, st, err, want)
		}
	}
	check("staged")
	// Same fd/pin, different contents. The old metadata cannot validate it.
	if err := fs.shadowStore.WriteFull(path, []byte("OTHER!"), 1); err != nil {
		t.Fatal(err)
	}
	check("remote")
	if _, err := fs.pendingIndex.PutShadowSpill(path, 6, PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	check("OTHER!")
}

func TestShadowRecoveredPartialMutationKeepsUnknownRevision(t *testing.T) {
	mutations := map[string]func(*ShadowStore, string) error{
		"ensure-same":   func(s *ShadowStore, p string) error { return s.Ensure(p, 8, 7) },
		"ensure-shrink": func(s *ShadowStore, p string) error { return s.Ensure(p, 4, 7) },
		"ensure-grow":   func(s *ShadowStore, p string) error { return s.Ensure(p, 16, 7) },
		"truncate":      func(s *ShadowStore, p string) error { return s.Truncate(p, 4, 7) },
		"partial-write": func(s *ShadowStore, p string) error { _, err := s.WriteAt(p, 0, []byte("new"), 7); return err },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			const path = "/recovered"
			if err := os.WriteFile(s.shadowPath(path), []byte("oldbytes"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := mutate(s, path); err != nil {
				t.Fatal(err)
			}
			if pin, ok := s.PinResident(path, 7); ok {
				s.Unpin(pin)
				t.Fatal("partial mutation blessed recovered bytes")
			}
			if got := s.BaseRev(path); got != 0 {
				t.Fatalf("base revision=%d, want unknown", got)
			}
			if err := s.WriteFull(path, []byte("replacement"), 7); err != nil {
				t.Fatal(err)
			}
			if pin, ok := s.PinResident(path, 7); !ok {
				t.Fatal("full replacement not readable")
			} else {
				s.Unpin(pin)
			}
		})
	}
}

func TestCloseSyncTruncateIgnoresPassiveForeignPID(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	const pid = 2345
	active := test.open(t, pid)
	test.header.Pid = pid
	test.write(t, active, "pending")
	if st := test.truncate(0); st != gofuse.OK {
		t.Fatal(st)
	}
	test.assertRemote(t, "seed", 1)
	test.write(t, active, "rewrite")
	test.release(test.old)
	test.assertRemote(t, "seed", 1)
	test.flush(t, active)
	test.assertRemote(t, "rewrite", 2)
	test.release(active)
}

func TestReadOnlyShadowUnboundPendingDoesNotAuthorizeOrDelete(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "remote") })
	const path = "/unbound"
	// Publish before a shadow exists: no source can be bound to this metadata.
	if _, err := fs.pendingIndex.PutShadowSpill(path, 6, PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	ino := fs.inodes.Lookup(path, false, 6, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 6)
	if err != nil || st != gofuse.OK || string(got) != "remote" {
		t.Fatalf("unbound read=%q/%v/%v", got, st, err)
	}
	if !fs.shadowStore.Has(path) {
		t.Fatal("reader deleted pending recovery payload")
	}
	// Becoming resident must not lazily bind the unrelated metadata either.
	fs.shadowStore.EnsureActiveGeneration(path)
	got, st, err = readDat9FSTestRange(fs, ino, out.Fh, 0, 6)
	if err != nil || st != gofuse.OK || string(got) != "remote" {
		t.Fatalf("resident unbound read=%q/%v/%v", got, st, err)
	}
}

func TestShadowDiscardOrphanAccounting(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := os.WriteFile(s.shadowPath("/accounted"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.RecoverPendingBytes()
	if err := s.WriteFull("/active", []byte("active"), 1); err != nil {
		t.Fatal(err)
	}
	// This untracked orphan must not be subtracted from active accounting, and
	// discarding another orphan must not discover it via a directory rescan.
	if err := os.WriteFile(s.shadowPath("/untracked"), []byte("untracked"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.discardDiskOnly("/accounted")
	if got := s.PendingBytes(); got != 6 {
		t.Fatalf("pending=%d, want 6", got)
	}
	s.discardDiskOnly("/untracked")
	s.discardDiskOnly("/accounted")
	if got := s.PendingBytes(); got != 6 {
		t.Fatalf("untracked/repeated discard pending=%d", got)
	}
	s.Remove("/active")
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("after active removal=%d", got)
	}
}

func TestLegacyWriteBackMigrationPreservesExistingShadow(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/legacy"
	if err := cache.PutWithBaseRev(path, []byte("GOOD"), 4, PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.shadowPath(path), []byte("NEWER"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyWriteBack(s, cache, nil); err != nil {
		t.Fatal(err)
	}
	data, err := s.ReadAll(path)
	if err != nil || string(data) != "NEWER" {
		t.Fatalf("migrated=%q/%v", data, err)
	}
}

func TestShadowRecoveredPartialExtentsKeepUnknownRevision(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const path = "/extents"
	const size = int64(128 << 20)
	f, err := os.Create(s.shadowPath(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	wb := NewWriteBuffer(path, streamingWriteMaxSize, DefaultPartSize)
	wb.totalSize = size // existing logical file; only the touched part is resident
	if _, err := wb.Write(0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtents(path, wb, 7); err != nil {
		t.Fatal(err)
	}
	if pin, ok := s.PinResident(path, 7); ok {
		s.Unpin(pin)
		t.Fatal("partial extents blessed recovered remainder")
	}
	if got := s.BaseRev(path); got != 0 {
		t.Fatalf("revision=%d", got)
	}
}

func TestLegacyWriteBackMigrationPreservesNewerPending(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/newer"
	if err := cache.PutWithBaseRev(path, []byte("older"), 5, PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteFull(path, []byte("newer"), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutShadowSpill(path, 5, PendingOverwrite, 2); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacyWriteBack(s, cache, idx); err != nil {
		t.Fatal(err)
	}
	data, err := s.ReadAll(path)
	if err != nil || string(data) != "newer" {
		t.Fatalf("newer pending=%q/%v", data, err)
	}
}

func TestShadowPartialMutationWithoutRevisionKeepsKnownBase(t *testing.T) {
	for _, extents := range []bool{false, true} {
		t.Run(fmt.Sprint(extents), func(t *testing.T) {
			s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			const path = "/known"
			if err := s.WriteFull(path, []byte("contents"), 7); err != nil {
				t.Fatal(err)
			}
			if extents {
				wb := NewWriteBuffer(path, 0, 4)
				wb.totalSize = 8
				if _, err := wb.Write(0, []byte("xy")); err != nil {
					t.Fatal(err)
				}
				if err := s.WriteExtents(path, wb, 0); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := s.WriteAt(path, 0, []byte("xy"), 0); err != nil {
					t.Fatal(err)
				}
			}
			if got := s.BaseRev(path); got != 7 {
				t.Fatalf("known base=%d, want 7", got)
			}
		})
	}
}

func TestShadowRecoveredAccountingSurvivesRename(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for path, data := range map[string]string{"/source": "src", "/dest": "dest"} {
		if err := os.WriteFile(s.shadowPath(path), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.RecoverPendingBytes()
	pin, ok := s.PinIfExists("/source")
	if !ok {
		t.Fatal("pin recovered source")
	}
	defer s.Unpin(pin)
	if !s.Rename("/source", "/dest") {
		t.Fatal("rename")
	}
	if got := s.PendingBytes(); got != 3 {
		t.Fatalf("after replacing disk-only destination=%d", got)
	}
	s.discardDiskOnly("/source")
	if got := s.PendingBytes(); got != 3 {
		t.Fatalf("old path discard subtracted renamed contents: %d", got)
	}
	s.Remove("/dest")
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("after removal=%d", got)
	}
}
