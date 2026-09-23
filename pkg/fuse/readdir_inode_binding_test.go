package fuse

import (
	"fmt"
	"testing"
	"time"
)

func TestReadDirPlusLoadedHandleKeepsReplacedInodeAttributes(t *testing.T) {
	for _, reference := range []string{"alias", "open", "dirty-alias"} {
		for _, sizes := range [][2]int64{{4096, 8192}, {8192, 512}, {4096, 0}} {
			t.Run(fmt.Sprintf("%s/%d-to-%d", reference, sizes[0], sizes[1]), func(t *testing.T) {
				opts := &MountOptions{}
				opts.setDefaults()
				fs := NewDat9FS(newTestClient("http://localhost"), opts)
				dir := fs.inodes.Lookup("/dir", true, 0, time.Now())
				old := fs.inodes.LookupWithIdentity("/dir/file", "old", 1, false, sizes[0], time.Now())
				fs.inodes.UpdateMode(old, 0o600)
				if reference != "open" {
					fs.inodes.LookupWithIdentity("/alias", "old", 2, false, sizes[0], time.Now())
				} else {
					fs.openHandles.Add(&FileHandle{Ino: old, Path: "/dir/file"})
				}
				live, _ := fs.inodes.GetEntry(old)
				fs.dirCache.Put("/dir", []CachedFileInfo{cachedInfoFromEntry("file", live)})
				dh := &DirHandle{Ino: dir, Path: "/dir"}
				first := readDirPlusHandleAttrs(t, fs, dh)["file"]
				if first.NodeId != old || first.Size != uint64(sizes[0]) {
					t.Fatalf("initial snapshot = %+v", first)
				}
				if reference == "open" {
					// Match Unlink's last-link/open-handle lifetime path.
					fs.inodes.RemoveLinkPreserve("/dir/file")
				} else {
					fs.inodes.RemoveLink("/dir/file")
				}
				if _, ok := fs.inodes.GetEntry(old); !ok {
					t.Fatal("fixture did not retain old inode")
				}
				replacement := fs.inodes.LookupWithIdentity("/dir/file", "new", 1, false, sizes[1], time.Now())
				if replacement == old {
					t.Fatal("replacement reused old inode")
				}
				var err error
				fs.pendingIndex, err = NewPendingIndex(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				gen, err := fs.pendingIndex.PutWithBaseRevAndMode("/dir/file", sizes[1], PendingNew, 0, 0o644, true)
				if err != nil {
					t.Fatal(err)
				}
				wantSize := sizes[0]
				if reference == "dirty-alias" {
					wantSize += 17
					fs.markDirtySize(old, wantSize)
				}
				assertOld := func(phase string) {
					t.Helper()
					attr := readDirPlusHandleAttrs(t, fs, dh)["file"]
					if attr.NodeId != old || attr.Size != uint64(wantSize) || attr.Mode&0o777 != 0o600 {
						t.Errorf("%s mixed inode identity and attributes: %+v; want inode=%d size=%d mode=0600", phase, attr, old, wantSize)
					}
				}
				assertOld("pending replacement")
				if fs.pendingIndex.Generation("/dir/file") != gen {
					t.Fatal("enumeration consumed replacement generation")
				}
				fs.onCommitQueueSuccess(&CommitEntry{Path: "/dir/file", Inode: replacement, Size: sizes[1], Kind: PendingNew, HasMode: true, Mode: 0o644}, 2)
				fs.pendingIndex.RemoveIfGeneration("/dir/file", gen)
				assertOld("replacement committed")
				current, _ := fs.inodes.GetEntry(replacement)
				if current.Size != sizes[1] || current.ResourceID != "new" {
					t.Fatalf("replacement changed: %+v", current)
				}
			})
		}
	}
}

// The pending snapshot must precede the atomic binding/copy observation. Both
// replacement timings preserve A, without blocking legitimate writes to A.
func TestReadDirPlusBindingSnapshotReplacementOrder(t *testing.T) {
	for _, timing := range []string{"before-binding", "after-binding"} {
		t.Run(timing, func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			old := fs.inodes.LookupWithIdentity("/file", "old", 2, false, 8192, time.Now())
			fs.inodes.LookupWithIdentity("/alias", "old", 2, false, 8192, time.Now())
			var err error
			fs.pendingIndex, err = NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fs.pendingIndex.PutWithBaseRev("/file", 8192, PendingOverwrite, 1); err != nil {
				t.Fatal(err)
			}
			local := fs.snapshotDirLocal("/")
			replace := func() {
				fs.inodes.RemoveLink("/file")
				fs.inodes.LookupWithIdentity("/file", "new", 1, false, 512, time.Now())
				if _, err := fs.pendingIndex.PutWithBaseRev("/file", 512, PendingNew, 0); err != nil {
					t.Fatal(err)
				}
			}
			if timing == "before-binding" {
				replace()
			}
			entry, bound := fs.inodes.dirEntryWithBinding(old, "/file")
			if entry == nil || bound != (timing == "after-binding") {
				t.Fatalf("binding = %v entry=%+v", bound, entry)
			}
			if timing == "after-binding" {
				replace()
			}
			if bound {
				item, _ := fs.localDirEntryInfo("/file", cachedInfoFromEntry("file", entry), entry, local)
				entry.Size = item.Size
			}
			if entry.Ino != old || entry.Size != 8192 {
				t.Fatalf("replacement contaminated snapshot: %+v", entry)
			}
			current, _ := fs.inodes.GetInode("/file")
			if current == old {
				t.Fatal("replacement did not change binding")
			}
		})
	}
}
