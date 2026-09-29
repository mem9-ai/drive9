package fuse

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newPendingAppendTestFS(t *testing.T, flags uint32) (*Dat9FS, uint64, *FileHandle, uint64, *FileHandle, uint64, *casFileServer, func(), func()) {
	t.Helper()
	const p = "/pending-probe"
	server, ts := newCASFileServer(t, p, 1, []byte("hello"))
	t.Cleanup(ts.Close)
	fs, ino := pr939HandleFS(t, p, "hello")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.opts.WritePolicy = WritePolicyWriteBack
	fs.remoteReadTimeout = 100 * time.Millisecond
	r, rid := pr939Handle(t, fs, ino, p, "hello", false)
	r.Flags = flags
	r.WritePolicy = WritePolicyWriteBack
	if flags == uint32(syscall.O_RDONLY) {
		r.Dirty = nil
	} else {
		if st := fs.preloadWritableHandle(context.Background(), r); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	t.Cleanup(func() { fs.releaseHandleShadowPin(r); fs.deleteFileHandle(rid, r) })
	w, wid := pr939Handle(t, fs, ino, p, "hello", false)
	t.Cleanup(func() { fs.deleteFileHandle(wid, w) })
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue = cq
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	cq.PathLock = func(string) func() { <-release; return func() {} }
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	t.Cleanup(func() { cq.CancelPath(p); unblock(); cq.DrainAll() })
	stage := func() {
		t.Helper()
		w.Lock()
		err := fs.stageShadowLocked(w, false)
		if err == nil {
			err = fs.enqueueStagedShadowCommitLocked(w)
		}
		w.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	finish := func() { unblock(); cq.DrainAll() }
	fs.readCache.Put(p, []byte("hello"), 1)
	return fs, ino, r, rid, w, wid, server, stage, finish
}

func TestIssue986PendingAppendSources(t *testing.T) {
	for _, flags := range []uint32{0, uint32(syscall.O_RDWR)} {
		for _, mode := range []string{"healthy", "missing", "unverified", "writeback-only", "metadata-only", "committed"} {
			t.Run(fmt.Sprintf("flags=%d/%s", flags, mode), func(t *testing.T) {
				fs, ino, r, rid, w, wid, _, stage, finish := newPendingAppendTestFS(t, flags)
				if mode == "metadata-only" {
					if _, err := fs.pendingIndex.PutWithBaseRev(w.Path, 5, PendingChmod, 1); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
						t.Fatal(st)
					}
					stage()
				}
				switch mode {
				case "missing":
					fs.shadowStore.Remove(w.Path)
				case "unverified":
					if err := fs.shadowStore.WriteFull(w.Path, []byte("hello gopher"), 0); err != nil {
						t.Fatal(err)
					}
				case "writeback-only":
					fs.commitQueue.CancelPath(w.Path)
					cache, err := NewWriteBackCache(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					fs.writeBack = cache
					if err := cache.PutWithBaseRev(w.Path, []byte("hello gopher"), 12, PendingOverwrite, 1); err != nil {
						t.Fatal(err)
					}
				case "committed":
					finish()
				}
				data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
				if mode == "missing" || mode == "unverified" {
					if st != gofuse.EIO {
						t.Fatalf("must not return stale %q/%v/%v", data, st, err)
					}
				} else {
					want := "hello gopher"
					if mode == "metadata-only" {
						want = "hello"
					}
					if err != nil || st != gofuse.OK || string(data) != want {
						t.Fatalf("read=%q/%v/%v want=%q path=%s", data, st, err, want, r.Path)
					}
				}
			})
		}
	}
}

func TestIssue986PendingAppendRecovery(t *testing.T) {
	for _, mode := range []string{"missing-then-restaged", "changed-during-pin"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, r, rid, w, wid, _, stage, _ := newPendingAppendTestFS(t, uint32(syscall.O_RDWR))
			fs.remoteReadTimeout = 200 * time.Millisecond
			if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
				t.Fatal(st)
			}
			stage()
			calls := 0
			if mode == "missing-then-restaged" {
				fs.shadowStore.Remove(w.Path)
				testHookAfterPendingAppendRead = func(p string, pending bool) {
					if p != r.Path || !pending || calls > 0 {
						return
					}
					calls++
					if err := fs.shadowStore.WriteFull(p, []byte("hello gopher"), 1); err != nil {
						t.Fatal(err)
					}
					if _, err := fs.pendingIndex.PutWithBaseRev(p, 12, PendingOverwrite, 1); err != nil {
						t.Fatal(err)
					}
				}
				defer func() { testHookAfterPendingAppendRead = nil }()
			} else {
				testHookAfterAliasPublishedPin = func(p string) {
					if p != r.Path {
						return
					}
					calls++
					if calls == 1 {
						if err := fs.shadowStore.WriteFull(p, []byte("hello gopher"), 1); err != nil {
							t.Fatal(err)
						}
						if _, err := fs.pendingIndex.PutWithBaseRev(p, 12, PendingOverwrite, 1); err != nil {
							t.Fatal(err)
						}
					}
				}
				defer func() { testHookAfterAliasPublishedPin = nil }()
			}
			data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
			wantCalls := 1
			if mode == "changed-during-pin" {
				wantCalls = 2
			}
			if err != nil || st != gofuse.OK || string(data) != "hello gopher" || calls != wantCalls {
				t.Fatalf("recovery %q/%v/%v calls=%d", data, st, err, calls)
			}
		})
	}
}

func TestIssue986PendingAppendLatePublication(t *testing.T) {
	for _, flags := range []uint32{0, uint32(syscall.O_RDWR)} {
		for _, point := range []string{"before-shadow", "after-shadow"} {
			t.Run(fmt.Sprintf("flags=%d/%s", flags, point), func(t *testing.T) {
				fs, ino, r, rid, _, wid, _, stage, _ := newPendingAppendTestFS(t, flags)
				entered := false
				publish := func(p string) {
					if p != r.Path || entered {
						return
					}
					entered = true
					if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
						t.Fatal(st)
					}
					stage()
					fs.shadowStore.Remove(p)
				}
				testHookBeforePublishedFallback = func(p string, afterShadow bool) {
					if afterShadow == (point == "after-shadow") {
						publish(p)
					}
				}
				defer func() { testHookBeforePublishedFallback = nil }()
				data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
				if !entered || st != gofuse.EIO {
					t.Fatalf("late publication entered=%v read=%q/%v/%v", entered, data, st, err)
				}
			})
		}
	}
}

func TestIssue986AppendReadContention(t *testing.T) {
	for _, mode := range []string{"actual-clean-read", "held-clean-lock", "held-dirty-lock"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once, gate sync.Once
			var armed atomic.Bool
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			fs, ino, busy, bid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false, func(req *http.Request) {
				if armed.Load() && req.Method == http.MethodGet && req.URL.Path == "/v1/fs/a" {
					gate.Do(func() { close(entered); <-release })
				}
			})
			fs.remoteReadTimeout = 100 * time.Millisecond
			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &out); st != gofuse.OK {
				t.Fatal(st)
			}
			observer, _ := fs.fileHandles.Get(out.Fh)
			defer fs.deleteFileHandle(out.Fh, observer)
			done := make(chan gofuse.Status, 1)
			if mode == "actual-clean-read" {
				busy.Lock()
				busy.Dirty = fs.newWriteBuffer(busy.Path, maxPreloadSize, 0)
				busy.Dirty.totalSize = 5
				busy.Dirty.remoteSize = 5
				busy.Dirty.LoadPart = func(int) ([]byte, error) {
					t.Error("clean Read unexpectedly calls LoadPart")
					return []byte("hello"), nil
				}
				busy.Unlock()
				armed.Store(true)
				go func() { _, st, _ := readDat9FSTestRange(fs, ino, bid, 0, 5); done <- st }()
				defer func() {
					unblock()
					select {
					case result := <-done:
						if result != gofuse.OK {
							t.Errorf("clean read: %v", result)
						}
					case <-time.After(3 * time.Second):
						t.Error("clean read did not finish after releasing GET")
					}
				}()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("GET did not reach barrier")
				}
				if !busy.TryLock() {
					t.Fatal("clean remote read holds fh.mu")
				}
				busy.Unlock()
			}
			if n, st := pr939Append(fs, ino, wid, " gopher"); n != 7 || st != gofuse.OK {
				t.Fatalf("append=%d/%v", n, st)
			}
			if mode == "held-clean-lock" {
				busy.Lock()
				defer busy.Unlock()
			}
			if mode == "held-dirty-lock" {
				w.Lock()
				defer w.Unlock()
			}
			data, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 64)
			if mode == "actual-clean-read" {
				if err != nil || st != gofuse.OK || string(data) != "hello gopher" {
					t.Fatalf("actual lazy read blocked append: %q/%v/%v", data, st, err)
				}
				t.Log("actual clean O_RDWR remote GET paused: mutex available; append reader returns hello gopher before GET resumes")
			} else {
				if st != gofuse.EIO {
					t.Fatalf("busy source bypass: %q/%v/%v", data, st, err)
				}
				t.Logf("%s causes bounded EIO; no stale success", mode)
			}
		})
	}
}

// Cancellation after selecting an unavailable pending source uses the same
// append context; it must not wait out another budget or return cached bytes.
func TestIssue986PendingAppendCancellation(t *testing.T) {
	fs, ino, r, rid, w, wid, _, stage, _ := newPendingAppendTestFS(t, uint32(syscall.O_RDWR))
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	stage()
	fs.shadowStore.Remove(w.Path)
	canceled := make(chan struct{})
	var once sync.Once
	cancel := func() { once.Do(func() { close(canceled) }) }
	defer cancel()
	testHookAfterPendingAppendRead = func(path string, pending bool) {
		if path == r.Path && pending {
			cancel()
		}
	}
	defer func() { testHookAfterPendingAppendRead = nil }()
	_, st := fs.Read(canceled, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: rid, Size: 64}, make([]byte, 64))
	if st != gofuse.EINTR {
		t.Fatalf("pending read cancellation: %v, want EINTR", st)
	}
}
