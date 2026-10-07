package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
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
	attrs    map[uint64]map[string][]byte
	requests []string
	flags    []uint32
}

func (s *extentXattrTestServer) inodeAttrs(inode uint64) map[string][]byte {
	attrs := s.attrs[inode]
	if attrs == nil {
		attrs = make(map[string][]byte)
		s.attrs[inode] = attrs
	}
	return attrs
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
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.flags = append(s.flags, request.Flags)
		s.inodeAttrs(request.Inode)[request.Name] = append([]byte(nil), request.Value...)
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0})
	case "get_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
			Name  string `json:"name"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		value, ok := s.attrs[request.Inode][request.Name]
		if !ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"errno": int(syscall.ENODATA)})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0, "value": value})
	case "list_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		attrs := s.attrs[request.Inode]
		names := make([]string, 0, len(attrs))
		for name := range attrs {
			names = append(names, name)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(names)))
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0, "names": names})
	case "remove_xattr":
		var request struct {
			Inode uint64 `json:"inode"`
			Name  string `json:"name"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Inode == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		delete(s.attrs[request.Inode], request.Name)
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": 0})
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func newExtentXattrTestFS(t *testing.T, handler http.Handler) (*Dat9FS, uint64) {
	return newExtentXattrTestFSWithKind(t, handler, false)
}

func newExtentXattrTestFSWithKind(t *testing.T, handler http.Handler, isDir bool) (*Dat9FS, uint64) {
	return newExtentXattrTestFSWithIdentity(t, handler, "/extent", isDir, 42)
}

func newExtentXattrTestFSWithIdentity(t *testing.T, handler http.Handler, path string, isDir bool, extentIno uint64) (*Dat9FS, uint64) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	fs := &Dat9FS{
		client: client.New(server.URL, "owner"),
		inodes: NewInodeToPath(),
		xattrs: NewXAttrStore(),
	}
	ino := fs.inodes.Lookup(path, isDir, 0, time.Now())
	fs.inodes.SetExtentIno(ino, extentIno)
	return fs, ino
}

func TestExtentDirectoryXattrHandlersUseDurableInodeRPC(t *testing.T) {
	backend := &extentXattrTestServer{attrs: make(map[uint64]map[string][]byte)}
	fs, ino := newExtentXattrTestFSWithKind(t, http.HandlerFunc(backend.serveHTTP), true)
	header := gofuse.InHeader{NodeId: ino}

	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "user.directory", []byte("durable")); status != gofuse.OK {
		t.Fatalf("SetXAttr directory status=%v", status)
	}
	dest := make([]byte, len("durable"))
	if size, status := fs.GetXAttr(nil, &header, "user.directory", dest); status != gofuse.OK || size != uint32(len(dest)) || string(dest) != "durable" {
		t.Fatalf("GetXAttr directory=%d/%v/%q", size, status, dest)
	}
	if _, ok := fs.xattrs.Get("/extent", "user.directory"); ok {
		t.Fatal("directory extent xattr fell back to the session store")
	}

	backend.mu.Lock()
	requests := append([]string(nil), backend.requests...)
	backend.mu.Unlock()
	want := []string{"set_xattr", "get_xattr"}
	if len(requests) != len(want) {
		t.Fatalf("requests=%v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("requests=%v, want %v", requests, want)
		}
	}
}

func TestExtentXattrHandlersUseDurableInodeRPC(t *testing.T) {
	backend := &extentXattrTestServer{attrs: make(map[uint64]map[string][]byte)}
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
	if size, status := fs.ListXAttr(nil, &header, make([]byte, len(list)-1)); status != gofuse.Status(syscall.ERANGE) || int(size) != len(list) {
		t.Fatalf("ListXAttr small buffer size/status=%d/%v, want %d/ERANGE", size, status, len(list))
	}
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
	want := []string{"set_xattr", "set_xattr", "get_xattr", "get_xattr", "list_xattr", "list_xattr", "remove_xattr", "get_xattr"}
	if len(requests) != len(want) {
		t.Fatalf("requests=%v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("requests=%v, want %v", requests, want)
		}
	}
}

func TestExtentFuseOverlayStatPersistsByInodeAcrossFreshInstances(t *testing.T) {
	backend := &extentXattrTestServer{attrs: make(map[uint64]map[string][]byte)}
	handler := http.HandlerFunc(backend.serveHTTP)
	const (
		originalPath = "/upper/etc/drive9.conf"
		renamedPath  = "/upper/etc/drive9-renamed.conf"
		value        = "65532:65532:100640"
	)

	first, firstIno := newExtentXattrTestFSWithIdentity(t, handler, originalPath, false, 42)
	firstHeader := gofuse.InHeader{NodeId: firstIno}
	if status := first.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: firstHeader}, fuseOverlayOverrideStatXattr, []byte(value)); status != gofuse.OK {
		t.Fatalf("SetXAttr status=%v, want OK", status)
	}
	if _, stored := first.xattrs.Get(originalPath, fuseOverlayOverrideStatXattr); stored {
		t.Fatal("extent override_stat fell back to the first mount's session store")
	}

	second, secondIno := newExtentXattrTestFSWithIdentity(t, handler, originalPath, false, 42)
	secondHeader := gofuse.InHeader{NodeId: secondIno}
	dest := make([]byte, len(value))
	if size, status := second.GetXAttr(nil, &secondHeader, fuseOverlayOverrideStatXattr, dest); status != gofuse.OK || size != uint32(len(value)) || string(dest) != value {
		t.Fatalf("fresh GetXAttr=%d/%v/%q, want %d/OK/%q", size, status, dest, len(value), value)
	}
	if _, stored := second.xattrs.Get(originalPath, fuseOverlayOverrideStatXattr); stored {
		t.Fatal("fresh extent override_stat was copied into the session store")
	}

	second.inodes.Rename(originalPath, renamedPath)
	if size, status := second.GetXAttr(nil, &secondHeader, fuseOverlayOverrideStatXattr, dest); status != gofuse.OK || size != uint32(len(value)) || string(dest) != value {
		t.Fatalf("renamed GetXAttr=%d/%v/%q, want %d/OK/%q", size, status, dest, len(value), value)
	}

	recreated, recreatedIno := newExtentXattrTestFSWithIdentity(t, handler, originalPath, false, 43)
	recreatedHeader := gofuse.InHeader{NodeId: recreatedIno}
	if _, status := recreated.GetXAttr(nil, &recreatedHeader, fuseOverlayOverrideStatXattr, nil); status != gofuse.ENOATTR {
		t.Fatalf("recreated GetXAttr status=%v, want ENOATTR", status)
	}
}

func TestExtentXattrFailureDoesNotFallBackToSessionStore(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EIO, syscall.ENOSYS, syscall.ENOTSUP} {
		t.Run(errno.Error(), func(t *testing.T) {
			fs, ino := newExtentXattrTestFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"errno": int(errno)})
			}))
			header := gofuse.InHeader{NodeId: ino}
			if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "user.test", []byte("value")); status != gofuse.Status(errno) {
				t.Fatalf("SetXAttr status=%v, want %v", status, errno)
			}
			if _, ok := fs.xattrs.Get("/extent", "user.test"); ok {
				t.Fatal("failed extent SetXAttr polluted the session xattr store")
			}
		})
	}
}

func TestExtentXattrRejectsInvalidNamespaceWithoutRPCOrFallback(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux xattr namespace contract")
	}
	var calls int
	fs, ino := newExtentXattrTestFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	header := gofuse.InHeader{NodeId: ino}
	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "invalid.namespace", []byte("value")); status != gofuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("SetXAttr invalid namespace status=%v, want EOPNOTSUPP", status)
	}
	if _, status := fs.GetXAttr(nil, &header, "invalid.namespace", nil); status != gofuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("GetXAttr invalid namespace status=%v, want EOPNOTSUPP", status)
	}
	if status := fs.RemoveXAttr(nil, &header, "invalid.namespace"); status != gofuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("RemoveXAttr invalid namespace status=%v, want EOPNOTSUPP", status)
	}
	if calls != 0 {
		t.Fatalf("invalid namespace made %d extent RPCs", calls)
	}
	if _, ok := fs.xattrs.Get("/extent", "invalid.namespace"); ok {
		t.Fatal("invalid namespace polluted the session xattr store")
	}
}

func TestExtentDirectoryXattrFailureDoesNotFallBackToSessionStore(t *testing.T) {
	fs, ino := newExtentXattrTestFSWithKind(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": int(syscall.EIO)})
	}), true)
	header := gofuse.InHeader{NodeId: ino}
	if status := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: header}, "user.test", []byte("value")); status != gofuse.EIO {
		t.Fatalf("SetXAttr directory status=%v, want EIO", status)
	}
	if _, ok := fs.xattrs.Get("/extent", "user.test"); ok {
		t.Fatal("failed directory extent SetXAttr polluted the session xattr store")
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

func TestRequiredExtentXattrMountSynthesizesClassicFuseOverlayStat(t *testing.T) {
	newFS := func() (*Dat9FS, uint64) {
		fs := &Dat9FS{
			inodes: NewInodeToPath(),
			xattrs: NewXAttrStore(),
			opts:   &MountOptions{RequireExtentXattrV1: true},
			uid:    1001,
			gid:    1002,
		}
		ino := fs.inodes.Lookup("/api-inbound.txt", false, 17, time.Unix(10, 0))
		fs.inodes.UpdateMode(ino, 0o640)
		return fs, ino
	}

	want := "1001:1002:100640"
	for attempt := 0; attempt < 2; attempt++ {
		fs, ino := newFS()
		header := gofuse.InHeader{NodeId: ino}
		if size, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, nil); status != gofuse.OK || size != uint32(len(want)) {
			t.Fatalf("attempt %d size/status=%d/%v, want %d/OK", attempt, size, status, len(want))
		}
		if size, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, make([]byte, len(want)-1)); status != gofuse.Status(syscall.ERANGE) || size != uint32(len(want)) {
			t.Fatalf("attempt %d short buffer=%d/%v, want %d/ERANGE", attempt, size, status, len(want))
		}
		dest := make([]byte, len(want))
		if size, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, dest); status != gofuse.OK || size != uint32(len(want)) || string(dest) != want {
			t.Fatalf("attempt %d value=%d/%v/%q, want %d/OK/%q", attempt, size, status, dest, len(want), want)
		}
		list := make([]byte, len(fuseOverlayOverrideStatXattr)+1)
		if size, status := fs.ListXAttr(nil, &header, list); status != gofuse.OK || size != uint32(len(list)) || string(list) != fuseOverlayOverrideStatXattr+"\x00" {
			t.Fatalf("attempt %d list=%d/%v/%q", attempt, size, status, list)
		}
		if _, stored := fs.xattrs.Get("/api-inbound.txt", fuseOverlayOverrideStatXattr); stored {
			t.Fatalf("attempt %d synthesized stat xattr polluted the session store", attempt)
		}
	}
}

func TestRequiredExtentXattrMountReadsClassicFileAcrossFreshInstances(t *testing.T) {
	const content = "durable-api-bytes"
	backend, server := newCASFileServer(t, "/api-inbound.txt", 1, []byte(content))
	defer server.Close()

	for attempt := 0; attempt < 2; attempt++ {
		fs, ino := pr939HandleFS(t, "/api-inbound.txt", content)
		fs.client = newTestClient(server.URL)
		fs.opts.RequireExtentXattrV1 = true
		header := gofuse.InHeader{NodeId: ino}

		if _, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, nil); status != gofuse.OK {
			t.Fatalf("attempt %d synthesized xattr status=%v, want OK", attempt, status)
		}
		var opened gofuse.OpenOut
		if status := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: syscall.O_RDONLY}, &opened); status != gofuse.OK {
			t.Fatalf("attempt %d Open status=%v, want OK", attempt, status)
		}
		result, status := fs.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: opened.Fh, Size: uint32(len(content) + 1)}, nil)
		if status != gofuse.OK {
			t.Fatalf("attempt %d Read status=%v, want OK", attempt, status)
		}
		data, status := result.Bytes(make([]byte, len(content)+1))
		result.Done()
		if status != gofuse.OK || string(data) != content {
			t.Fatalf("attempt %d Read=%q/%v, want %q/OK", attempt, data, status, content)
		}
		if handle, ok := fs.fileHandles.Get(opened.Fh); ok {
			fs.deleteFileHandle(opened.Fh, handle)
		}
	}

	if _, gets := backend.proofRequestCounts(); gets != 2 {
		t.Fatalf("classic data GETs=%d, want 2", gets)
	}
}

func TestRequiredExtentXattrMountDoesNotMaskClassicDataFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs/missing-data" {
			http.Error(w, "missing data", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	fs, ino := pr939HandleFS(t, "/missing-data", "expected")
	fs.client = newTestClient(server.URL)
	fs.opts.RequireExtentXattrV1 = true
	header := gofuse.InHeader{NodeId: ino}
	if _, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, nil); status != gofuse.OK {
		t.Fatalf("synthesized xattr status=%v, want OK", status)
	}
	var opened gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: syscall.O_RDONLY}, &opened); status != gofuse.OK {
		t.Fatalf("Open status=%v, want OK", status)
	}
	if result, status := fs.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: opened.Fh, Size: 8}, nil); status == gofuse.OK || result != nil {
		t.Fatalf("Read result/status=%v/%v, want nil/failure", result, status)
	}
}

func TestClassicFuseOverlayStatSynthesisIsRootfsOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *MountOptions
	}{
		{name: "ordinary mount", opts: &MountOptions{}},
		{name: "missing options", opts: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &Dat9FS{inodes: NewInodeToPath(), xattrs: NewXAttrStore(), opts: tc.opts, uid: 1001, gid: 1002}
			ino := fs.inodes.Lookup("/classic", false, 4, time.Now())
			header := gofuse.InHeader{NodeId: ino}
			if _, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, nil); status != gofuse.ENOATTR {
				t.Fatalf("GetXAttr status=%v, want ENOATTR", status)
			}
			if size, status := fs.ListXAttr(nil, &header, nil); status != gofuse.OK || size != 0 {
				t.Fatalf("ListXAttr=%d/%v, want 0/OK", size, status)
			}
		})
	}
}

func TestClassicFuseOverlayStatDoesNotMaskExtentFailure(t *testing.T) {
	fs, ino := newExtentXattrTestFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"errno": int(syscall.EIO)})
	}))
	fs.opts = &MountOptions{RequireExtentXattrV1: true}
	header := gofuse.InHeader{NodeId: ino}
	if _, status := fs.GetXAttr(nil, &header, fuseOverlayOverrideStatXattr, nil); status != gofuse.EIO {
		t.Fatalf("GetXAttr extent failure status=%v, want EIO", status)
	}
}
