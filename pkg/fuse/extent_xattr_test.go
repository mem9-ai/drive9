package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

type extentXattrTestServer struct {
	mu       sync.Mutex
	attrs    map[string][]byte
	requests []string
	flags    []uint32
}

func (s *extentXattrTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/extent/meta" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	op := r.Header.Get("X-Drive9-Extent-Op")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, op)
	w.Header().Set("Content-Type", "application/json")
	switch op {
	case "set_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
			Name  string `json:"name"`
			Value []byte `json:"value"`
			Flags uint32 `json:"flags"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode != 42 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.flags = append(s.flags, request.Flags)
		s.attrs[request.Name] = append([]byte(nil), request.Value...)
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0})
	case "get_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
			Name  string `json:"name"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode != 42 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		value, ok := s.attrs[request.Name]
		if !ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"errno": int(syscall.ENODATA)})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0, "value": value})
	case "list_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode != 42 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		names := make([]string, 0, len(s.attrs))
		for name := range s.attrs {
			names = append(names, name)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(names)))
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0, "names": names})
	case "remove_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
			Name  string `json:"name"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode != 42 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		delete(s.attrs, request.Name)
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0})
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func newExtentXattrTestFS(t *testing.T, handler http.Handler) (*Dat9FS, uint64) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	fs := &Dat9FS{
		client: client.New(server.URL, "owner"),
		inodes: NewInodeToPath(),
		xattrs: NewXAttrStore(),
	}
	ino := fs.inodes.Lookup("/extent", false, 0, time.Now())
	fs.inodes.SetExtentIno(ino, 42)
	return fs, ino
}

func TestExtentXattrHandlersUseDurableInodeRPC(t *testing.T) {
	backend := &extentXattrTestServer{attrs: make(map[string][]byte)}
	fs, ino := newExtentXattrTestFS(t, http.HandlerFunc(backend.serveHTTP))
	header := gofuse.InHeader{NodeId: ino}

	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{
		InHeader: header,
		Flags:    xattrCreateFlag,
	}, "user.zeta", []byte("durable")); status != gofuse.OK {
		t.Fatalf("SetXAttr status=%v", status)
	}
	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "user.alpha", []byte("a")); status != gofuse.OK {
		t.Fatalf("SetXAttr second status=%v", status)
	}
	if size, status := fs.GetXAttr(nil, &header, "user.zeta", nil); status != gofuse.OK || size != 7 {
		t.Fatalf("GetXAttr size/status=%d/%v, want 7/OK", size, status)
	}
	dest := make([]byte, 7)
	if size, status := fs.GetXAttr(nil, &header, "user.zeta", dest); status != gofuse.OK || size != 7 || string(dest) != "durable" {
		t.Fatalf("GetXAttr=%d/%v/%q", size, status, dest)
	}
	list := make([]byte, len("user.alpha")+1+len("user.zeta")+1)
	if size, status := fs.ListXAttr(nil, &header, list); status != gofuse.OK || int(size) != len(list) {
		t.Fatalf("ListXAttr size/status=%d/%v", size, status)
	}
	if got := string(list); got != "user.alpha\x00user.zeta\x00" {
		t.Fatalf("ListXAttr=%q, want sorted names", got)
	}
	if status := fs.RemoveXAttr(nil, &header, "user.zeta"); status != gofuse.OK {
		t.Fatalf("RemoveXAttr status=%v", status)
	}
	if _, status := fs.GetXAttr(nil, &header, "user.zeta", nil); status != gofuse.ENOATTR {
		t.Fatalf("GetXAttr removed status=%v, want ENOATTR", status)
	}

	backend.mu.Lock()
	requests := append([]string(nil), backend.requests...)
	flags := append([]uint32(nil), backend.flags...)
	backend.mu.Unlock()
	if len(flags) != 2 || flags[0] != xattrCreateFlag || flags[1] != 0 {
		t.Fatalf("set flags=%v, want [%d 0]", flags, xattrCreateFlag)
	}
	want := []string{"set_xattr", "set_xattr", "get_xattr", "get_xattr", "list_xattr", "remove_xattr", "get_xattr"}
	if len(requests) != len(want) {
		t.Fatalf("requests=%v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("requests=%v, want %v", requests, want)
		}
	}
}

func TestExtentXattrFailureDoesNotFallBackToSessionStore(t *testing.T) {
	fs, ino := newExtentXattrTestFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": int(syscall.EIO)})
	}))
	header := gofuse.InHeader{NodeId: ino}
	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "user.test", []byte("value")); status != gofuse.EIO {
		t.Fatalf("SetXAttr status=%v, want EIO", status)
	}
	if _, ok := fs.xattrs.Get("/extent", "user.test"); ok {
		t.Fatal("failed extent SetXAttr polluted the session xattr store")
	}
}

func TestClassicXattrStillUsesSessionStore(t *testing.T) {
	var calls int
	fs, _ := newExtentXattrTestFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	classic := fs.inodes.Lookup("/classic", false, 0, time.Now())
	header := gofuse.InHeader{NodeId: classic}
	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "user.test", []byte("local")); status != gofuse.OK {
		t.Fatalf("classic SetXAttr status=%v", status)
	}
	dest := make([]byte, 5)
	if size, status := fs.GetXAttr(nil, &header, "user.test", dest); status != gofuse.OK || size != 5 || string(dest) != "local" {
		t.Fatalf("classic GetXAttr=%d/%v/%q", size, status, dest)
	}
	if calls != 0 {
		t.Fatalf("classic xattr made %d extent RPCs", calls)
	}
}
