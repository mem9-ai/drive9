package fuse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

func newTestDat9FS(t *testing.T, size int64, get func(http.ResponseWriter, *http.Request)) (*Dat9FS, uint64, func()) {
	t.Helper()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			get(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, size, time.Now())
	return fs, ino, ts.Close
}

func TestOpenWritableFailsWhenSmallFilePreloadFails(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 16, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	defer cleanup()

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.EIO {
		t.Fatalf("Open status = %v, want EIO", st)
	}
}

func TestOpenWritableFailsWhenLargeFilePreloadFails(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, smallFileThreshold+1, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	defer cleanup()

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.EIO {
		t.Fatalf("Open status = %v, want EIO", st)
	}
}

func TestGetAttrPrefersDirtyHandleSize(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 4, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "data")
	})
	defer cleanup()

	fh := &FileHandle{
		Ino:   ino,
		Path:  "/file.bin",
		Dirty: NewWriteBuffer("/file.bin", 0),
	}
	if _, err := fh.Dirty.Write(0, []byte("dirty-size")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fs.fileHandles.Allocate(fh)

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: ino},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got, want := out.Size, uint64(len("dirty-size")); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
}

func TestGetAttrUsesLatestDirtyHandleSize(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 4, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "data")
	})
	defer cleanup()

	fh1 := &FileHandle{
		Ino:   ino,
		Path:  "/file.bin",
		Dirty: NewWriteBuffer("/file.bin", 0),
	}
	fh2 := &FileHandle{
		Ino:   ino,
		Path:  "/file.bin",
		Dirty: NewWriteBuffer("/file.bin", 0),
	}
	fhID1 := fs.fileHandles.Allocate(fh1)
	fhID2 := fs.fileHandles.Allocate(fh2)

	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID1}, []byte("abc")); st != gofuse.OK {
		t.Fatalf("first write status = %v, want OK", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID2}, []byte("abcdefghi")); st != gofuse.OK {
		t.Fatalf("second write status = %v, want OK", st)
	}

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: ino},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got, want := out.Size, uint64(len("abcdefghi")); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
}

func TestGetAttrDirectoryDoesNotRequireRemoteStat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: dirIno}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("GetAttr mode = %o, want directory mode", out.Mode)
	}
}

func TestLookupFallsBackToParentListWhenDirStatUnsupported(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.Path == "/v1/fs/" && r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries": []map[string]any{{
						"name":  "dir",
						"isDir": true,
						"size":  0,
					}},
				})
				return
			}
			http.NotFound(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "dir", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Lookup mode = %o, want directory mode", out.Mode)
	}
}
