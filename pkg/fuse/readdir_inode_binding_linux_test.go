package fuse

// #918's hardlink-identity regression, ported to the extension-stack
// facade: a name replaced under a live enumeration keeps the snapshot's
// inode for the resumed pages, the old fd and its alias retain the old
// identity and payload, and the replacement lands as a new object.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"unsafe"

	"github.com/mem9-ai/drive9/pkg/fs"
)

var nativeEndian = func() binary.ByteOrder {
	var x uint16 = 1
	if *(*byte)(unsafe.Pointer(&x)) == 1 {
		return binary.LittleEndian
	}
	return binary.BigEndian
}()

func TestReadDirPlusReplacedInodeKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("Linux FUSE unavailable: %v", err)
	}
	for _, mode := range []string{"interactive", "strict"} {
		for _, sizes := range [][2]int{{4096, 8192}, {8192, 512}} {
			t.Run(fmt.Sprintf("%s/%d-to-%d", mode, sizes[0], sizes[1]), func(t *testing.T) {
				testReadDirPlusReplacedInodeKernel(t, mode, sizes[0], sizes[1])
			})
		}
	}
}

func testReadDirPlusReplacedInodeKernel(t *testing.T, mode string, oldSize, newSize int) {
	t.Helper()
	oldData, newData := bytes.Repeat([]byte("A"), oldSize), bytes.Repeat([]byte("B"), newSize)
	var mu sync.Mutex
	remote := make(map[string][]byte)
	resourceIDs := make(map[string]string)
	nextResourceID := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p := strings.TrimPrefix(req.URL.Path, "/v1/fs")
		switch {
		case req.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":1048576}`))
		case req.Method == http.MethodGet && req.URL.Query().Has("list"):
			mu.Lock()
			names := make([]string, 0, len(remote))
			for name := range remote {
				names = append(names, name)
			}
			mu.Unlock()
			var b strings.Builder
			b.WriteString(`{"entries":[`)
			for i, name := range names {
				if i > 0 {
					b.WriteByte(',')
				}
				mu.Lock()
				id := resourceIDs[name]
				nlink := 0
				for _, other := range resourceIDs {
					if other == id {
						nlink++
					}
				}
				size := len(remote[name])
				mu.Unlock()
				fmt.Fprintf(&b, `{"name":%q,"size":%d,"revision":1,"resource_id":%q,"nlink":%d}`, strings.TrimPrefix(name, "/"), size, id, nlink)
			}
			b.WriteString(`]}`)
			_, _ = w.Write([]byte(b.String()))
		case req.Method == http.MethodPut:
			data, err := io.ReadAll(req.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			remote[p] = data
			if resourceIDs[p] == "" {
				nextResourceID++
				resourceIDs[p] = fmt.Sprintf("resource-%d", nextResourceID)
			}
			mu.Unlock()
			_, _ = w.Write([]byte(`{"revision":1}`))
		case req.Method == http.MethodHead || req.Method == http.MethodGet:
			mu.Lock()
			data, found := remote[p]
			id := resourceIDs[p]
			nlink := 0
			for _, other := range resourceIDs {
				if other == id {
					nlink++
				}
			}
			mu.Unlock()
			if p == "/" {
				w.Header().Set("X-Dat9-IsDir", "true")
			} else if !found {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-Revision", "1")
			if id != "" {
				w.Header().Set("X-Dat9-Resource-ID", id)
				w.Header().Set("X-Dat9-Nlink", strconv.Itoa(nlink))
			}
			if req.Method == http.MethodGet {
				_, _ = w.Write(data)
			}
		case req.Method == http.MethodPost && req.URL.Query().Has("chmod"):
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodDelete:
			mu.Lock()
			delete(remote, p)
			delete(resourceIDs, p)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPost && req.URL.Query().Has("hardlink"):
			mu.Lock()
			source := req.Header.Get("X-Dat9-Hardlink-Source")
			data, ok := remote[source]
			if ok {
				remote[p] = data
				resourceIDs[p] = resourceIDs[source]
			}
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	st := mountPendingKernelStack(t, ts.URL, mode)

	path := filepath.Join(st.mp, "0-alias.dat")
	write := func(data []byte, mode os.FileMode) {
		t.Helper()
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.Write(data)
		syncErr, closeErr := f.Sync(), f.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			t.Fatalf("write/sync/close: %v/%v/%v", writeErr, syncErr, closeErr)
		}
	}
	write(oldData, 0o600)
	st.drain(t)
	// Create the retained alias first, then the replaceable directory name.
	// The mount preserves insertion order for this cached listing.
	alias := path
	// v1 seeded the cached listing before the enumeration so it serves
	// insertion order (the chain list would order by the remote's map).
	st.f.Ctx.DirCache.Put("/", []fs.DirEntryInfo{{Name: "0-alias.dat", Size: int64(oldSize)}})
	path = filepath.Join(st.mp, "file.dat")
	if err := os.Link(alias, path); err != nil {
		t.Fatal(err)
	}
	oldFD, err := os.Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = oldFD.Close() }()
	oldInfo, err := oldFD.Stat()
	if err != nil {
		t.Fatal(err)
	}
	oldIno := oldInfo.Sys().(*syscall.Stat_t).Ino
	dir, err := os.Open(st.mp)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	var afterAliasCookie int64
	readNames := func(afterAlias bool) {
		t.Helper()
		buf := make([]byte, 8192)
		var names []string
		for {
			n, err := syscall.ReadDirent(int(dir.Fd()), buf)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
			// Linux getdents64: inode, offset, record length, type, name.
			for offset := 0; offset < n; {
				if n-offset < 19 {
					t.Fatal("short dirent header")
				}
				ino := nativeEndian.Uint64(buf[offset : offset+8])
				cookie := int64(nativeEndian.Uint64(buf[offset+8 : offset+16]))
				length := int(nativeEndian.Uint16(buf[offset+16 : offset+18]))
				if length < 20 || length > n-offset {
					t.Fatal("invalid dirent length")
				}
				name := string(bytes.SplitN(buf[offset+19:offset+length], []byte{0}, 2)[0])
				if name == "0-alias.dat" {
					afterAliasCookie = cookie
				}
				if name != "." && name != ".." {
					names = append(names, name)
					if ino != oldIno {
						t.Fatalf("snapshot %s inode=%d, want old inode=%d", name, ino, oldIno)
					}
				}
				offset += length
			}
		}
		if afterAlias {
			if len(names) != 1 || names[0] != "file.dat" {
				t.Fatalf("resumed directory names: %v", names)
			}
		} else if len(names) != 2 || names[0] != "0-alias.dat" || names[1] != "file.dat" || afterAliasCookie <= 0 {
			t.Fatalf("initial directory names/cookie: %v / %d", names, afterAliasCookie)
		}
	}
	readNames(false)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	write(newData, 0o644)
	replacement, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Sys().(*syscall.Stat_t).Ino == oldIno {
		t.Fatal("replacement reused old inode")
	}
	calls := st.f.plusCalls.Load()
	// Restore a cookie returned by the first enumeration. Return only the
	// replaced name: if its alias is in the same READDIRPLUS reply, Linux
	// can accept the alias attributes first and mask the bad second reply.
	if _, err := syscall.Seek(int(dir.Fd()), afterAliasCookie, 0); err != nil {
		t.Fatal(err)
	}
	readNames(true)
	if st.f.plusCalls.Load() <= calls {
		t.Fatal("rewind did not issue READDIRPLUS")
	}
	assertOld := func(phase string) {
		t.Helper()
		info, err := oldFD.Stat()
		if err != nil {
			t.Fatal(err)
		}
		aliasInfo, err := os.Stat(alias)
		if err != nil {
			t.Fatal(err)
		}
		if aliasInfo.Size() != int64(oldSize) || aliasInfo.Sys().(*syscall.Stat_t).Ino != oldIno {
			t.Errorf("%s: old alias attributes changed: %+v", phase, aliasInfo)
		}
		if info.Size() != int64(oldSize) || info.Sys().(*syscall.Stat_t).Ino != oldIno {
			t.Errorf("%s: old inode=%d size=%d, want inode=%d size=%d", phase, info.Sys().(*syscall.Stat_t).Ino, info.Size(), oldIno, oldSize)
		}
	}
	assertOld("after rewind")
	st.drain(t)
	assertOld("after drain")
	mu.Lock()
	equal := bytes.Equal(remote["/file.dat"], newData)
	aliasEqual := bytes.Equal(remote["/0-alias.dat"], oldData)
	mu.Unlock()
	if !equal {
		t.Fatal("replacement remote payload changed")
	}
	if !aliasEqual {
		t.Fatalf("alias remote payload replaced: head=%q", string(remote["/0-alias.dat"][:min(len(remote["/0-alias.dat"]), 8)]))
	}
	got := make([]byte, oldSize+1)
	n, err := oldFD.ReadAt(got, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != oldSize || !bytes.Equal(got[:n], oldData) {
		t.Errorf("old FD read %d bytes, want intact %d-byte payload", n, oldSize)
	}
}
