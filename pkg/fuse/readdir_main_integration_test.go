package fuse

import (
	"fmt"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestCachedLookupDoesNotFenceNewerListing(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	ino := fs.inodes.LookupWithIdentity("/file", "resource", 1, false, 4096, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	live, _ := fs.inodes.GetEntry(ino)
	fs.dirCache.Put("/", []CachedFileInfo{cachedInfoFromEntry("file", live)})
	observed := fs.inodes.AttrVersion() // A remote LIST starts here.
	var out gofuse.EntryOut
	if hit, st := fs.lookupFromDirCache("/", "/file", "file", &out); !hit || st != gofuse.OK {
		t.Fatalf("cached lookup = %v/%v", hit, st)
	}
	if got := fs.inodes.AttrVersion(); got != observed {
		t.Errorf("cached lookup advanced mutation version: %d -> %d", observed, got)
	}
	listed := fs.cachedToDirEntries("/", []CachedFileInfo{{
		Name: "file", ResourceID: "resource", Revision: 2, Size: 128, observedVersion: observed,
	}})
	if len(listed) != 1 || listed[0].Size != 128 {
		t.Fatalf("newer remote LIST rejected after cached lookup: %+v", listed)
	}
	entry, _ := fs.inodes.GetEntry(ino)
	if entry.Nlookup != live.Nlookup+1 {
		t.Fatalf("cached lookup reference = %d, want %d", entry.Nlookup, live.Nlookup+1)
	}
}

func TestDirectoryReplacementReclaimsUnreferencedInodes(t *testing.T) {
	m := NewInodeToPath()
	for i := range 100 {
		m.EnsureDirEntry("/file", CachedFileInfo{
			ResourceID: fmt.Sprintf("resource-%d", i), Size: 128, observedVersion: m.AttrVersion(),
		}, false)
	}
	if len(m.byInode) != 2 || len(m.byID) != 1 {
		t.Fatalf("replacement leaked inodes: byInode=%d byID=%d", len(m.byInode), len(m.byID))
	}
}

func TestDirectoryReplacementPreservesReferences(t *testing.T) {
	for _, ref := range []string{"lookup", "open"} {
		t.Run(ref, func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			old := fs.inodes.EnsureDirEntry("/file", CachedFileInfo{ResourceID: "old", Size: 4096}, false)
			if ref == "lookup" {
				fs.inodes.IncrementLookup(old.Ino)
			} else {
				fs.openHandles.Add(&FileHandle{Ino: old.Ino, Path: "/file"})
			}
			entries := fs.cachedToDirEntries("/", []CachedFileInfo{{
				Name: "file", ResourceID: "new", Size: 128, observedVersion: fs.inodes.AttrVersion(),
			}})
			if len(entries) != 1 || entries[0].Ino == old.Ino || entries[0].Size != 128 {
				t.Fatalf("replacement = %+v", entries)
			}
			retained, ok := fs.inodes.GetEntry(old.Ino)
			if !ok || !retained.Unlinked || retained.Size != 4096 {
				t.Fatalf("lost referenced predecessor: %+v", retained)
			}
			if ref == "lookup" {
				fs.Forget(old.Ino, 1)
				if _, ok := fs.inodes.GetEntry(old.Ino); ok {
					t.Fatal("final FORGET retained unlinked predecessor")
				}
				if got, ok := fs.inodes.GetInode("/file"); !ok || got != entries[0].Ino {
					t.Fatal("reclaiming predecessor removed the replacement mapping")
				}
			} else {
				fs.Forget(old.Ino, 0)
				if _, ok := fs.inodes.GetEntry(old.Ino); !ok {
					t.Fatal("FORGET removed predecessor with an open handle")
				}
			}
		})
	}
}

func TestDirectoryObservationPreservesExplicitTime(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	ino := fs.inodes.LookupWithIdentity("/file", "resource", 1, false, 4096, time.Now())
	explicit := time.Unix(123, 0)
	fs.inodes.SetLocalMtime(ino, explicit)
	entries := fs.cachedToDirEntries("/", []CachedFileInfo{{
		Name: "file", ResourceID: "resource", Size: 4096, Mtime: time.Now(), observedVersion: fs.inodes.AttrVersion(),
	}})
	if len(entries) != 1 || !entries[0].Mtime.Equal(explicit) {
		t.Fatalf("listing lost explicit mtime: %+v", entries)
	}
}

func TestReadDirPlusPendingPreservesExplicitTime(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	ino := fs.inodes.Lookup("/file", false, 4096, time.Now())
	var err error
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.pendingIndex.PutWithBaseRev("/file", 4096, PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	explicit := time.Unix(123, 0)
	fs.inodes.SetLocalMtime(ino, explicit)
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "file", Size: 0}})
	attr := readDirPlusAttrs(t, fs, 1, "/")["file"]
	if attr.Size != 4096 || attr.Mtime != uint64(explicit.Unix()) {
		t.Fatalf("pending reply lost size/time: %+v", attr)
	}
}
