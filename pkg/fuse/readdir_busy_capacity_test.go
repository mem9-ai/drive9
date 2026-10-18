package fuse

import (
	"encoding/binary"
	"reflect"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadDirPlusBusyRecordCapacity(t *testing.T) {
	fs, ino, _ := newFtruncateVisibilityFS(t, []byte("hello world"))
	id := addFtruncateTestHandle(t, fs, ino, "/file.bin", []byte("hello world"))
	reviewFtruncate(t, fs, ino, id, 5)
	later := fs.markDirtySize(ino, 77)
	fs.clearDirtySize(ino, later)
	dh := &DirHandle{Ino: 1, Path: "/", entriesGeneration: fs.mountViewGeneration.Load(), Entries: []DirEntry{
		{Name: "file.bin", Ino: ino, Mode: uint32(syscall.S_IFREG)},
		{Name: "next.bin", Ino: ino, Mode: uint32(syscall.S_IFREG)},
	}}
	handle := fs.dirHandles.Allocate(dh)
	defer fs.dirHandles.Delete(handle)
	owner, _ := fs.fileHandles.Get(id)
	owner.Lock()
	defer owner.Unlock()
	before, _ := fs.inodes.GetEntry(ino)
	entrySize := binary.Size(gofuse.EntryOut{}) + 24 + 8
	for _, capacity := range []int{entrySize - 1, entrySize, entrySize*2 - 1, entrySize * 2} {
		out := gofuse.NewDirEntryList(make([]byte, capacity), 2)
		st := fs.ReadDirPlus(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}, Fh: handle, Offset: 2, Size: uint32(capacity)}, out)
		count := capacity / entrySize
		want := gofuse.OK
		if count == 0 {
			want = gofuse.EINVAL
		}
		buf := reflect.ValueOf(out).Elem().FieldByName("buf").Bytes()
		if st != want || out.Offset != uint64(2+count) || len(buf) != count*entrySize {
			t.Fatalf("capacity=%d status=%v offset=%d bytes=%d", capacity, st, out.Offset, len(buf))
		}
		for i := 0; i < count; i++ {
			record := buf[i*entrySize : (i+1)*entrySize]
			for _, value := range record[:binary.Size(gofuse.EntryOut{})] {
				if value != 0 {
					t.Fatal("name-only record published attributes")
				}
			}
			dirent := record[binary.Size(gofuse.EntryOut{}):]
			if nativeEndian.Uint64(dirent[:8]) != gofuse.FUSE_UNKNOWN_INO || nativeEndian.Uint64(dirent[8:16]) != uint64(3+i) || nativeEndian.Uint32(dirent[20:24]) != 0 {
				t.Fatal("invalid unknown inode/type or cookie")
			}
		}
	}
	after, _ := fs.inodes.GetEntry(ino)
	if after.Nlookup != before.Nlookup {
		t.Fatal("busy pages acquired lookup references")
	}
}
