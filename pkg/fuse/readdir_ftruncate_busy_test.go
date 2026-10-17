package fuse

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func readBusyDirPlusPage(t *testing.T, fs *Dat9FS, handle, offset uint64) (map[string]gofuse.EntryOut, uint64, gofuse.Status) {
	t.Helper()
	out := gofuse.NewDirEntryList(make([]byte, 4096), offset)
	status := fs.ReadDirPlus(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}, Fh: handle, Offset: offset, Size: 4096}, out)
	buf := reflect.ValueOf(out).Elem().FieldByName("buf").Bytes()
	if status != gofuse.OK && len(buf) != 0 {
		t.Fatalf("failed page contains %d published bytes", len(buf))
	}
	attrs := make(map[string]gofuse.EntryOut)
	for len(buf) != 0 {
		var attr gofuse.EntryOut
		size := binary.Size(attr)
		if len(buf) < size+24 {
			t.Fatal("short directory record")
		}
		if err := binary.Read(bytes.NewReader(buf[:size]), nativeEndian, &attr); err != nil {
			t.Fatal(err)
		}
		buf = buf[size:]
		nameLen := int(nativeEndian.Uint32(buf[16:20]))
		recordSize := (24 + nameLen + 7) &^ 7
		if len(buf) < recordSize {
			t.Fatal("short directory name")
		}
		attrs[string(buf[24:24+nameLen])] = attr
		buf = buf[recordSize:]
	}
	return attrs, out.Offset, status
}

func TestReadDirPlusBusyTruncatePagination(t *testing.T) {
	for _, unbound := range []bool{false, true} {
		for _, prefix := range []bool{false, true} {
			t.Run(fmt.Sprintf("unbound=%t/prefix=%t", unbound, prefix), func(t *testing.T) {
				fs, ino, _ := newFtruncateVisibilityFS(t, []byte("hello world"))
				ownerPath := "/file.bin"
				if unbound {
					if !fs.inodes.AddAlias(ino, "/survivor", "old-resource", 2, false, 11, time.Now()) {
						t.Fatal("alias")
					}
					ownerPath = "/survivor"
				}
				writerID := addFtruncateTestHandle(t, fs, ino, ownerPath, []byte("hello world"))
				reviewFtruncate(t, fs, ino, writerID, 5)
				var replacement uint64
				var generation uint64
				if unbound {
					fs.inodes.RemoveLink("/file.bin")
					replacement = fs.inodes.LookupWithIdentity("/file.bin", "replacement", 1, false, 99, time.Now())
					var err error
					fs.pendingIndex, err = NewPendingIndex(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					generation, err = fs.pendingIndex.PutWithBaseRev("/file.bin", 99, PendingNew, 0)
					if err != nil {
						t.Fatal(err)
					}
				}
				// Model a newer rolled-back marker while the accepted older truncate
				// still resides in its live owner. Its size must be read from that fd.
				later := fs.markDirtySize(ino, 77)
				fs.clearDirtySize(ino, later)
				entries := []DirEntry{}
				var ready uint64
				if prefix {
					ready = fs.inodes.Lookup("/a-ready", false, 7, time.Now())
					entries = append(entries, DirEntry{Name: "a-ready", Ino: ready, Mode: uint32(syscall.S_IFREG) | 0o644})
				}
				entries = append(entries, DirEntry{Name: "file.bin", Ino: ino, Mode: uint32(syscall.S_IFREG) | 0o600})
				dh := &DirHandle{Ino: 1, Path: "/", Entries: entries, entriesGeneration: fs.mountViewGeneration.Load()}
				handle := fs.dirHandles.Allocate(dh)
				t.Cleanup(func() { fs.dirHandles.Delete(handle) })
				before, _ := fs.inodes.GetEntry(ino)
				rootBefore, _ := fs.inodes.GetEntry(1)
				writer, _ := fs.fileHandles.Get(writerID)
				writer.Lock()
				locked := true
				t.Cleanup(func() {
					if locked {
						writer.Unlock()
					}
				})
				if _, _, busy := fs.dirtyHandleSize(ino); !busy {
					t.Fatal("fixture did not reach busy size selection")
				}
				attrs, offset, status := readBusyDirPlusPage(t, fs, handle, 0)
				wantOffset := uint64(3)
				wantCount := 3
				if prefix {
					wantOffset, wantCount = 4, 4
					if attrs["a-ready"].NodeId != ready || attrs["a-ready"].Size != 7 {
						t.Fatal("valid prefix changed")
					}
				}
				if status != gofuse.OK || offset != wantOffset || len(attrs) != wantCount {
					t.Fatalf("partial page status=%v offset=%d entries=%v", status, offset, attrs)
				}
				if attr, emitted := attrs["file.bin"]; !emitted || attr != (gofuse.EntryOut{}) {
					t.Fatal("busy child must publish only its name, without attributes or identity")
				}
				if attrs["."].NodeId != gofuse.FUSE_UNKNOWN_INO || attrs[".."].NodeId != gofuse.FUSE_UNKNOWN_INO {
					t.Fatal("dots acquired inode identity")
				}
				attrs, next, status := readBusyDirPlusPage(t, fs, handle, offset)
				if status != gofuse.OK || next != offset || len(attrs) != 0 {
					t.Fatalf("empty busy page=%v offset=%d entries=%v", status, next, attrs)
				}
				after, _ := fs.inodes.GetEntry(ino)
				rootAfter, _ := fs.inodes.GetEntry(1)
				if after.Nlookup != before.Nlookup || rootAfter.Nlookup != rootBefore.Nlookup {
					t.Fatal("busy entry or dots leaked a lookup reference")
				}
				writer.Unlock()
				locked = false
				attrs, next, status = readBusyDirPlusPage(t, fs, handle, offset-1)
				if status != gofuse.OK || next != offset || len(attrs) != 1 || attrs["file.bin"].NodeId != ino || attrs["file.bin"].Size != 5 {
					t.Fatalf("retry=%v offset=%d attributes=%v", status, next, attrs)
				}
				after, _ = fs.inodes.GetEntry(ino)
				if after.Nlookup != before.Nlookup+1 {
					t.Fatal("successful retry must acquire exactly one lookup reference")
				}
				if unbound {
					current, _ := fs.inodes.GetEntry(replacement)
					if current.Size != 99 || fs.pendingIndex.Generation("/file.bin") != generation {
						t.Fatal("old directory page changed replacement state")
					}
				}
			})
		}
	}
}

func TestDirectoryBusyTruncatePreservesLiveMetadata(t *testing.T) {
	fs, ino, _ := newFtruncateVisibilityFS(t, []byte("hello world"))
	writerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", []byte("hello world"))
	reviewFtruncate(t, fs, ino, writerID, 5)
	later := fs.markDirtySize(ino, 77)
	fs.clearDirtySize(ino, later)
	before, _ := fs.inodes.GetEntry(ino)
	writer, _ := fs.fileHandles.Get(writerID)
	writer.Lock()
	defer writer.Unlock()
	item, preserve, busy := fs.localDirEntryInfo("/file.bin", CachedFileInfo{Name: "file.bin", Size: 999}, before, fs.snapshotDirLocal("/"))
	if !preserve || !busy || item.Size != before.Size {
		t.Fatalf("busy projection=%+v preserve=%t busy=%t", item, preserve, busy)
	}
	entries := fs.cachedToDirEntries("/", []CachedFileInfo{{Name: "file.bin", Size: 999}})
	after, _ := fs.inodes.GetEntry(ino)
	if len(entries) != 1 || entries[0].Ino != ino || after.Size != before.Size || after.attrVersion != before.attrVersion {
		t.Fatal("busy listing conversion replaced live inode metadata")
	}
	dh := &DirHandle{Ino: 1, Path: "/", Entries: entries, entriesGeneration: fs.mountViewGeneration.Load()}
	handle := fs.dirHandles.Allocate(dh)
	defer fs.dirHandles.Delete(handle)
	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := fs.ReadDir(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}, Fh: handle, Size: 4096}, out); status != gofuse.OK {
		t.Fatal(status)
	}
	if !bytes.Contains(reflect.ValueOf(out).Elem().FieldByName("buf").Bytes(), []byte("file.bin")) {
		t.Fatal("busy size hid the ordinary READDIR name")
	}
}
