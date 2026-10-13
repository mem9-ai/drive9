package fuse

// #918's kernel-level regression, ported to the extension-stack facade: a
// READDIRPLUS that races un-landed writes must publish the pending sizes
// (and not the remote's stale rows), and draining must land every payload.

import (
	"bytes"
	"context"
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

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// countingFS counts READDIRPLUS dispatches so the test can prove the kernel
// actually exercised the PLUS path.
type countingFS struct {
	*FS
	plusCalls atomic.Int32
}

func (c *countingFS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	c.plusCalls.Add(1)
	return c.FS.ReadDirPlus(cancel, input, out)
}

// pendingKernelStack is the writeback+core facade mounted on a real Linux
// FUSE mount — the minimal stack the #918 readdir rows need.
type pendingKernelStack struct {
	f      *countingFS
	server *gofuse.Server
	wb     *writeback.Writeback
	mp     string
}

// mountPendingKernelStack mounts the facade (writeback over core) on a
// kernel mountpoint. It skips when Linux FUSE is unavailable unless
// DRIVE9_REQUIRE_KERNEL_FUSE pins the run.
func mountPendingKernelStack(t *testing.T, tsURL, syncMode string) *pendingKernelStack {
	t.Helper()
	cl := client.New(tsURL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	bus := fs.NewEventBus()
	ledger, lerr := staging.NewStaging(t.TempDir())
	if lerr != nil {
		t.Fatal(lerr)
	}
	t.Cleanup(func() { ledger.Close() })
	wbcache, werr := staging.NewWriteBackCache(t.TempDir())
	if werr != nil {
		t.Fatal(werr)
	}
	wb, err := writeback.New(writeback.Options{
		Policy:     writeback.PolicyWriteBack,
		SyncMode:   syncMode,
		Client:     cl,
		Staging:    ledger,
		WriteCache: wbcache,
		Events:     bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{
		Extensions:  []fs.Handler{wb, terminal},
		Events:      bus,
		AttrTTL:     30 * time.Second,
		EntryTTL:    30 * time.Second,
		NegEntryTTL: time.Second,
		DirTTL:      30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cf := &countingFS{FS: f}
	mp := t.TempDir()
	server, err := gofuse.NewServer(cf, mp, &gofuse.MountOptions{
		FsName:             "drive9",
		Name:               "drive9",
		MaxReadAhead:       8 << 20,
		MaxWrite:           1024 * 1024,
		MaxBackground:      32,
		EnableLocks:        true,
		EnableDirectIoMmap: true,
		DirectMountStrict:  os.Geteuid() == 0,
	})
	if err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("cannot mount Linux FUSE: %v", err)
	}
	f.Init2(context.Background())
	go server.Serve()
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Error(err)
		}
	})
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	return &pendingKernelStack{f: cf, server: server, wb: wb, mp: mp}
}

func (s *pendingKernelStack) drain(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.wb.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
}

func TestReadDirPlusPendingKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("Linux FUSE unavailable: %v", err)
	}
	for _, mode := range []string{"interactive", "strict"} {
		t.Run(mode, func(t *testing.T) {
			testReadDirPlusPendingKernel(t, mode)
		})
	}
}

func testReadDirPlusPendingKernel(t *testing.T, mode string) {
	t.Helper()
	const fileCount = 4
	payload := bytes.Repeat([]byte("917!"), 1024)
	allowCommit := make(chan struct{})
	started := make(chan string, fileCount)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowCommit) }) }
	if mode == "strict" {
		release()
	}
	var mu sync.Mutex
	remote := make(map[string][]byte)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":1048576}`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
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
				fmt.Fprintf(&b, `{"name":%q,"size":%d,"revision":1}`, strings.TrimPrefix(name, "/"), len(payload))
			}
			b.WriteString(`]}`)
			_, _ = w.Write([]byte(b.String()))
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
			_, _ = w.Write([]byte(`{"revision":1}`))
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
		case r.Method == http.MethodDelete:
			mu.Lock()
			delete(remote, p)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(release)
	st := mountPendingKernelStack(t, ts.URL, mode)

	for i := range fileCount {
		name := fmt.Sprintf("file-%d.dat", i)
		f, err := os.Create(filepath.Join(st.mp, name))
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
	if mode == "interactive" {
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
			info, err := os.Stat(filepath.Join(st.mp, fmt.Sprintf("file-%d.dat", i)))
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != int64(len(payload)) {
				t.Errorf("%s: file %d size=%d, want %d", phase, i, info.Size(), len(payload))
			}
		}
	}
	assertSizes("before listdir")
	// Force the chain path: drop the facade's namespace cache so the
	// kernel's next READDIRPLUS answers from a fresh OpList — the listing
	// must resolve pending state instead of publishing remote rows the
	// gated commits have not produced yet.
	st.f.Ctx.DirCache.InvalidateAll()
	entries, err := os.ReadDir(st.mp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != fileCount {
		t.Fatalf("listdir returned %d entries, want %d", len(entries), fileCount)
	}
	got := make(map[string]bool, len(entries))
	for _, entry := range entries {
		got[entry.Name()] = true
	}
	for i := range fileCount {
		if !got[fmt.Sprintf("file-%d.dat", i)] {
			t.Errorf("listdir missing file-%d.dat (got %v)", i, got)
		}
	}
	if st.f.plusCalls.Load() == 0 {
		t.Fatal("kernel did not exercise READDIRPLUS")
	}
	assertSizes("after listdir, before commit")
	release()
	st.drain(t)
	assertSizes("immediately after drain")
	mu.Lock()
	defer mu.Unlock()
	for i := range fileCount {
		if !bytes.Equal(remote[fmt.Sprintf("/file-%d.dat", i)], payload) {
			t.Errorf("remote file %d payload mismatch", i)
		}
	}
}
