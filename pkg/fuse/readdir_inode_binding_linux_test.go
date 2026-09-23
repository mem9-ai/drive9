package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadDirPlusReplacedInodeKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("Linux FUSE unavailable: %v", err)
	}
	for _, profile := range []string{MountProfileNone, MountProfileCodingAgent} {
		for _, mode := range []SyncMode{SyncInteractive, SyncStrict} {
			for _, sizes := range [][2]int{{4096, 8192}, {8192, 512}} {
				t.Run(fmt.Sprintf("%s/%s/%d-to-%d", profile, mode, sizes[0], sizes[1]), func(t *testing.T) {
					testReadDirPlusReplacedInodeKernel(t, profile, mode, sizes[0], sizes[1])
				})
			}
		}
	}
}

func testReadDirPlusReplacedInodeKernel(t *testing.T, profile string, mode SyncMode, oldSize, newSize int) {
	t.Helper()
	oldData, newData := bytes.Repeat([]byte("A"), oldSize), bytes.Repeat([]byte("B"), newSize)
	var armed atomic.Bool
	entered, allow := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(allow) }) }
	if mode == SyncStrict {
		release()
	}
	var mu sync.Mutex
	remote := make(map[string][]byte)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p := strings.TrimPrefix(req.URL.Path, "/v1/fs")
		switch {
		case req.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 1 << 20})
		case req.Method == http.MethodGet && req.URL.Query().Has("list"):
			mu.Lock()
			entries := make([]map[string]any, 0, len(remote))
			for name, data := range remote {
				entries = append(entries, map[string]any{"name": strings.TrimPrefix(name, "/"), "size": len(data), "revision": 1})
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
		case req.Method == http.MethodPut:
			data, err := io.ReadAll(req.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if armed.Load() {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-allow:
				case <-req.Context().Done():
					return
				}
			}
			mu.Lock()
			remote[p] = data
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case req.Method == http.MethodHead || req.Method == http.MethodGet:
			mu.Lock()
			data, found := remote[p]
			mu.Unlock()
			if p == "/" {
				w.Header().Set("X-Dat9-IsDir", "true")
			} else if !found {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-Revision", "1")
			if req.Method == http.MethodGet {
				_, _ = w.Write(data)
			}
		case req.Method == http.MethodDelete:
			mu.Lock()
			delete(remote, p)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPost && req.URL.Query().Has("hardlink"):
			mu.Lock()
			data, ok := remote[req.Header.Get("X-Dat9-Hardlink-Source")]
			if ok {
				remote[p] = data
			}
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPost && req.URL.Query().Has("chmod"):
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(release)
	opts := &MountOptions{Profile: profile, LocalRoot: t.TempDir(), MountPoint: t.TempDir(),
		SyncMode: mode, WritePolicy: WritePolicyWriteBack, DirTTL: 30 * time.Second,
		AttrTTL: 30 * time.Second, EntryTTL: 30 * time.Second, DirectMountStrict: os.Geteuid() == 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	var err error
	fs.shadowStore, err = NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack, err = NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	queue := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 16)
	queue.PathLock = fs.lockRemoteCommitPath
	queue.DurableWatermark = fs.latestCommittedRevision
	queue.OnSuccess = fs.onCommitQueueSuccess
	queue.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = queue
	t.Cleanup(func() { release(); queue.DrainAll() })
	fs.dirCache.Put("/", []CachedFileInfo{})
	kernelFS := &readdirPendingKernelFS{Dat9FS: fs}
	server, err := gofuse.NewServer(kernelFS, opts.MountPoint, newGoFuseMountOptions(opts))
	if err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("cannot mount Linux FUSE: %v", err)
	}
	go server.Serve()
	t.Cleanup(func() {
		release()
		queue.DrainAll()
		fs.notifyWg.Wait()
		if err := server.Unmount(); err != nil {
			t.Error(err)
		}
	})
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(opts.MountPoint, "0-alias.dat")
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
	drain := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if got := fs.Drain(ctx); !got.OK {
			t.Fatalf("drain: %+v", got)
		}
	}
	write(oldData, 0o600)
	drain()
	// Create the retained alias first, then the replaceable directory name.
	// The mount preserves insertion order for this cached listing.
	alias := path
	path = filepath.Join(opts.MountPoint, "file.dat")
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
	dir, err := os.Open(opts.MountPoint)
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
	armed.Store(true)
	write(newData, 0o644)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("replacement PUT did not reach gate")
	}
	if mode == SyncInteractive {
		meta, ok := fs.pendingIndex.GetMeta("/file.dat")
		if !ok || meta.Size != int64(newSize) {
			t.Fatalf("replacement pending: %+v", meta)
		}
		mu.Lock()
		_, committed := remote["/file.dat"]
		mu.Unlock()
		if committed {
			t.Fatal("replacement escaped commit gate")
		}
	}
	replacement, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Sys().(*syscall.Stat_t).Ino == oldIno {
		t.Fatal("replacement reused old inode")
	}
	calls := kernelFS.plusCalls.Load()
	// Restore a cookie returned by the first enumeration. Return only the
	// replaced name: if its alias is in the same READDIRPLUS reply, Linux
	// can accept the alias attributes first and mask the bad second reply.
	if _, err := syscall.Seek(int(dir.Fd()), afterAliasCookie, 0); err != nil {
		t.Fatal(err)
	}
	readNames(true)
	if kernelFS.plusCalls.Load() <= calls {
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
	release()
	drain()
	assertOld("after drain")
	got := make([]byte, oldSize+1)
	n, err := oldFD.ReadAt(got, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != oldSize || !bytes.Equal(got[:n], oldData) {
		t.Errorf("old FD read %d bytes, want intact %d-byte payload", n, oldSize)
	}
	mu.Lock()
	equal := bytes.Equal(remote["/file.dat"], newData)
	mu.Unlock()
	if !equal {
		t.Fatal("replacement remote payload changed")
	}
}
