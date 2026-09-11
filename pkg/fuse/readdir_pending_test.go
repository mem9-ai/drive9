package fuse

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func readDirPlusAttrs(t *testing.T, fs *Dat9FS, dirIno uint64, dirPath string) map[string]gofuse.EntryOut {
	t.Helper()
	dh := &DirHandle{Ino: dirIno, Path: dirPath}
	fh := fs.dirHandles.Allocate(dh)
	defer fs.dirHandles.Delete(fh)
	out := gofuse.NewDirEntryList(make([]byte, 8192), 0)
	if st := fs.ReadDirPlus(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno}, Fh: fh, Size: 8192,
	}, out); st != gofuse.OK {
		t.Fatalf("ReadDirPlus: %v", st)
	}
	attrs := make(map[string]gofuse.EntryOut)
	buf := dirEntryListBuf(t, out)
	for len(buf) > 0 {
		var attr gofuse.EntryOut
		attrSize := binary.Size(attr)
		if len(buf) < attrSize+24 {
			t.Fatalf("short READDIRPLUS record: %d bytes", len(buf))
		}
		if err := binary.Read(bytes.NewReader(buf[:attrSize]), nativeEndian, &attr); err != nil {
			t.Fatal(err)
		}
		buf = buf[attrSize:]
		nameLen := int(nativeEndian.Uint32(buf[16:20]))
		recordSize := (24 + nameLen + 7) &^ 7
		if recordSize > len(buf) {
			t.Fatalf("short dirent: %d bytes, want %d", len(buf), recordSize)
		}
		name := string(buf[24 : 24+nameLen])
		if _, exists := attrs[name]; exists {
			t.Fatalf("duplicate directory entry %q", name)
		}
		attrs[name] = attr
		buf = buf[recordSize:]
	}
	return attrs
}

func TestReadDirPlusPendingSameNameAttributes(t *testing.T) {
	for _, source := range []string{"pending", "writeback", "pending-over-writeback", "dirty-over-pending", "truncate"} {
		for _, cacheHit := range []bool{true, false} {
			name := source + "/remote-list"
			if cacheHit {
				name = source + "/dir-cache"
			}
			t.Run(name, func(t *testing.T) {
				const filePath = "/dir/file.dat"
				wantSize, staleSize := int64(4096), int64(0)
				if source == "truncate" {
					wantSize, staleSize = 0, 4096
				}
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if cacheHit || r.Method != http.MethodGet || r.URL.Query().Get("list") != "1" {
						t.Errorf("unexpected request: %s %s", r.Method, r.URL)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{
						map[string]any{"name": "file.dat", "size": staleSize, "revision": 1},
					}})
				}))
				defer ts.Close()
				opts := &MountOptions{SyncMode: SyncInteractive, AttrTTL: 30 * time.Second}
				opts.setDefaults()
				fs := NewDat9FS(newTestClient(ts.URL), opts)
				dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
				ino := fs.inodes.Lookup(filePath, false, wantSize, time.Now())
				fs.inodes.UpdateMode(ino, 0o600)
				if cacheHit {
					fs.dirCache.Put("/dir", []CachedFileInfo{{Name: "file.dat", Size: staleSize, Revision: 1, HasMode: true, Mode: 0o644}})
				}
				pending, err := NewPendingIndex(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				fs.pendingIndex = pending
				var generation uint64
				if source != "writeback" {
					size := wantSize
					if source == "dirty-over-pending" {
						size = 128
					}
					generation, err = pending.PutWithBaseRevAndMode(filePath, size, PendingNew, 0, 0o600, true)
					if err != nil {
						t.Fatal(err)
					}
				}
				if source == "writeback" || source == "pending-over-writeback" {
					fs.writeBack, err = NewWriteBackCache(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					size := wantSize
					if source == "pending-over-writeback" {
						size = 128
					}
					if err := fs.writeBack.PutWithBaseRevAndMode(filePath, make([]byte, size), size, PendingNew, 0, 0o600, true); err != nil {
						t.Fatal(err)
					}
				}
				if source == "dirty-over-pending" {
					fs.markDirtySize(ino, wantSize)
				}
				attrs := readDirPlusAttrs(t, fs, dirIno, "/dir")
				got, ok := attrs["file.dat"]
				if !ok || len(attrs) != 3 || got.Size != uint64(wantSize) || got.Mode&0o777 != 0o600 {
					t.Fatalf("READDIRPLUS attributes = %+v, names=%d; want size=%d mode=0600", got, len(attrs), wantSize)
				}
				entry, _ := fs.inodes.GetEntry(ino)
				if entry.Size != wantSize {
					t.Fatalf("live inode size = %d, want %d", entry.Size, wantSize)
				}
				if pending.Generation(filePath) != generation {
					t.Fatal("enumeration changed pending generation ownership")
				}
			})
		}
	}
}
