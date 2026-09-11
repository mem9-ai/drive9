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
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type readdirPendingKernelFS struct {
	*Dat9FS
	plusCalls atomic.Int32
}

func (fs *readdirPendingKernelFS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	fs.plusCalls.Add(1)
	return fs.Dat9FS.ReadDirPlus(cancel, input, out)
}

func TestReadDirPlusPendingKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("Linux FUSE unavailable: %v", err)
	}
	for _, profile := range []string{MountProfileNone, MountProfileCodingAgent} {
		for _, mode := range []SyncMode{SyncInteractive, SyncStrict} {
			t.Run(profile+"/"+mode.String(), func(t *testing.T) {
				testReadDirPlusPendingKernel(t, profile, mode)
			})
		}
	}
}

func testReadDirPlusPendingKernel(t *testing.T, profile string, mode SyncMode) {
	t.Helper()
	const fileCount = 4
	payload := bytes.Repeat([]byte("917!"), 1024)
	allowCommit := make(chan struct{})
	started := make(chan string, fileCount)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowCommit) }) }
	if mode == SyncStrict {
		release()
	}
	var mu sync.Mutex
	remote := make(map[string][]byte)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch {
		case r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 1 << 20})
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			mu.Lock()
			entries := make([]map[string]any, 0, len(remote))
			for name, data := range remote {
				entries = append(entries, map[string]any{"name": strings.TrimPrefix(name, "/"), "size": len(data), "revision": 1})
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			select {
			case started <- p:
			default:
				t.Errorf("unexpected extra upload for %s", p)
			}
			select {
			case <-allowCommit:
			case <-r.Context().Done():
				return
			}
			mu.Lock()
			remote[p] = data
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
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
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(release)
	opts := &MountOptions{
		Profile: profile, LocalRoot: t.TempDir(), MountPoint: t.TempDir(),
		SyncMode: mode, WritePolicy: WritePolicyWriteBack,
		DirTTL: 30 * time.Second, AttrTTL: 30 * time.Second, EntryTTL: 30 * time.Second,
		FlushDebounce:     0,
		DirectMountStrict: os.Geteuid() == 0,
	}
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
	queue := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, fileCount, 16)
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
	for i := range fileCount {
		name := fmt.Sprintf("file-%d.dat", i)
		f, err := os.Create(filepath.Join(opts.MountPoint, name))
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.Write(payload)
		syncErr := f.Sync()
		closeErr := f.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			t.Fatalf("write/sync/close: %v / %v / %v", writeErr, syncErr, closeErr)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range fileCount {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("uploads did not reach the controlled commit boundary")
		}
	}
	if mode == SyncInteractive {
		for i := range fileCount {
			meta, ok := fs.pendingIndex.GetMeta(fmt.Sprintf("/file-%d.dat", i))
			if !ok || meta.Size != int64(len(payload)) {
				t.Fatalf("pending metadata: %+v, found=%v", meta, ok)
			}
		}
		mu.Lock()
		committed := len(remote)
		mu.Unlock()
		if committed != 0 {
			t.Fatal("remote commit escaped the test gate")
		}
	}
	assertSizes := func(phase string) {
		t.Helper()
		for i := range fileCount {
			info, err := os.Stat(filepath.Join(opts.MountPoint, fmt.Sprintf("file-%d.dat", i)))
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != int64(len(payload)) {
				t.Errorf("%s: file %d size=%d, want %d", phase, i, info.Size(), len(payload))
			}
		}
	}
	assertSizes("before listdir")
	entries, err := os.ReadDir(opts.MountPoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != fileCount {
		t.Fatalf("listdir returned %d entries, want %d", len(entries), fileCount)
	}
	for i, entry := range entries {
		if entry.Name() != fmt.Sprintf("file-%d.dat", i) {
			t.Errorf("unexpected name %q", entry.Name())
		}
	}
	if kernelFS.plusCalls.Load() == 0 {
		t.Fatal("kernel did not exercise READDIRPLUS")
	}
	assertSizes("after listdir, before commit")
	release()
	if drained := fs.Drain(ctx); !drained.OK {
		t.Fatalf("drain failed: %+v", drained)
	}
	assertSizes("immediately after drain")
	mu.Lock()
	defer mu.Unlock()
	for i := range fileCount {
		if !bytes.Equal(remote[fmt.Sprintf("/file-%d.dat", i)], payload) {
			t.Errorf("remote file %d payload mismatch", i)
		}
	}
}
