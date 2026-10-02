//go:build linux

package fuse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

type initialSyncDirectoryFixture struct {
	t          *testing.T
	fs         *Dat9FS
	watcher    *SSEWatcher
	dh         *DirHandle
	fh         uint64
	listCalls  atomic.Int32
	beforeList func(int32, http.ResponseWriter, *http.Request) bool
	releases   []func()
	reads      []<-chan struct{}
}

func newInitialSyncDirectoryFixture(t *testing.T) *initialSyncDirectoryFixture {
	t.Helper()
	f := &initialSyncDirectoryFixture{t: t}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/" || r.URL.Query().Get("list") != "1" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		call := f.listCalls.Add(1)
		if f.beforeList != nil && f.beforeList(call, w, r) {
			return
		}
		name := "new-only.txt"
		if call == 1 {
			name = "old-only.txt"
		}
		writeDirectoryListing(t, w, []client.FileInfo{{Name: name, Revision: 1, Mtime: 1, Size: 7}})
	}))
	opts := &MountOptions{DirTTL: time.Minute}
	opts.setDefaults()
	f.fs = NewDat9FS(newTestClient(server.URL), opts)
	f.watcher = &SSEWatcher{fs: f.fs}
	f.dh = &DirHandle{Ino: 1, Path: "/"}
	f.fh = f.fs.dirHandles.Allocate(f.dh)
	previousListHook := testHookAfterDirectoryList
	previousOutputHook := testHookBeforeDirectoryOutputCheck
	t.Cleanup(func() {
		for _, release := range f.releases {
			release()
		}
		for _, done := range f.reads {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("directory read did not finish during cleanup")
			}
		}
		testHookAfterDirectoryList = previousListHook
		testHookBeforeDirectoryOutputCheck = previousOutputHook
		f.fs.directoryPrefetch.shutdown()
		server.Close()
	})
	return f
}

func (f *initialSyncDirectoryFixture) gate() (<-chan struct{}, func()) {
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	f.releases = append(f.releases, open)
	return release, open
}

func (f *initialSyncDirectoryFixture) initialReset() {
	f.watcher.handleEvent(nil, &client.ResetEvent{Reason: "initial_sync"})
}

func (f *initialSyncDirectoryFixture) wait(done <-chan struct{}) {
	f.t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		f.t.Fatal("timed out waiting for directory race handshake")
	}
}

type initialSyncDirectoryRead struct {
	out    *gofuse.DirEntryList
	status gofuse.Status
	done   chan struct{}
}

func (f *initialSyncDirectoryFixture) startRead(plus bool, offset uint64, size int) *initialSyncDirectoryRead {
	r := &initialSyncDirectoryRead{out: gofuse.NewDirEntryList(make([]byte, size), offset), done: make(chan struct{})}
	f.reads = append(f.reads, r.done)
	go func() {
		defer close(r.done)
		input := &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}, Fh: f.fh, Offset: offset, Size: uint32(size)}
		if plus {
			r.status = f.fs.ReadDirPlus(nil, input, r.out)
		} else {
			r.status = f.fs.ReadDir(nil, input, r.out)
		}
	}()
	return r
}

func (f *initialSyncDirectoryFixture) assertNoOutput(r *initialSyncDirectoryRead) {
	f.t.Helper()
	if len(dirEntryListBuf(f.t, r.out)) != 0 {
		f.t.Fatal("directory entries emitted before final validation")
	}
	for _, entry := range f.fs.inodes.Snapshot() {
		want := int64(0)
		if entry.Ino == 1 {
			want = 1
		}
		if entry.Nlookup != want {
			f.t.Fatalf("%s lookup references = %d, want %d", entry.Path, entry.Nlookup, want)
		}
	}
}

func (f *initialSyncDirectoryFixture) assertUnlocked() {
	f.t.Helper()
	if !f.fs.mountViewMu.TryLock() {
		f.t.Fatal("mount-view lock retained after directory read")
	}
	f.fs.mountViewMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !f.dh.lockRead(ctx) {
		f.t.Fatal("directory read token retained after directory read")
	}
	f.dh.unlockRead()
}

// READDIRPLUS prefixes each dirent with EntryOut; the existing READDIR parser
// intentionally handles only plain dirents.
func initialSyncDirectoryNames(t *testing.T, out *gofuse.DirEntryList, plus bool) []string {
	t.Helper()
	if !plus {
		return parseDirEntryNames(t, out)
	}
	buf := dirEntryListBuf(t, out)
	var names []string
	for len(buf) > 0 {
		prefix := int(unsafe.Sizeof(gofuse.EntryOut{}))
		if len(buf) < prefix+24 {
			t.Fatal("short READDIRPLUS entry")
		}
		nameLen := int(nativeEndian.Uint32(buf[prefix+16 : prefix+20]))
		length := prefix + (24+nameLen+7)&^7
		if length > len(buf) {
			t.Fatal("READDIRPLUS entry exceeds buffer")
		}
		names = append(names, string(buf[prefix+24:prefix+24+nameLen]))
		buf = buf[length:]
	}
	return names
}

func TestInitialSyncDirectoryReadThreeFences(t *testing.T) {
	for _, plus := range []bool{false, true} {
		operation := "ReadDir"
		if plus {
			operation = "ReadDirPlus"
		}
		for _, window := range []string{"remote response", "handle installation", "final output"} {
			t.Run(operation+"/"+window, func(t *testing.T) {
				f := newInitialSyncDirectoryFixture(t)
				originalGeneration := f.fs.mountViewGeneration.Load()
				reached := make(chan struct{})
				release, open := f.gate()
				hook := func(fs *Dat9FS, path string, generation uint64) {
					if fs == f.fs && path == "/" && generation == originalGeneration {
						close(reached)
						<-release
					}
				}
				switch window {
				case "remote response":
					f.beforeList = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
						if call == 1 {
							close(reached)
							<-release
						}
						return false
					}
				case "handle installation":
					testHookAfterDirectoryList = hook
				case "final output":
					testHookBeforeDirectoryOutputCheck = hook
				}
				r := f.startRead(plus, 0, 4096)
				f.wait(reached)
				f.assertNoOutput(r)
				cached, installed := f.fs.dirCache.Get("/")
				if window == "remote response" {
					if installed {
						t.Fatal("remote response fence was reached after cache installation")
					}
				} else if !installed || len(cached) != 1 || cached[0].Name != "old-only.txt" {
					t.Fatal("earlier remote-response fence did not pass")
				}
				f.dh.mu.Lock()
				handleInstalled := f.dh.Entries != nil
				f.dh.mu.Unlock()
				if handleInstalled != (window == "final output") {
					t.Fatalf("handle installed = %t at %s", handleInstalled, window)
				}
				f.initialReset()
				if got := f.fs.initialSyncResetGeneration.Load(); got != originalGeneration+1 || got != f.fs.mountViewGeneration.Load() {
					t.Fatalf("tagged generation = %d, original = %d", got, originalGeneration)
				}
				f.assertNoOutput(r)
				open()
				f.wait(r.done)
				if r.status != gofuse.OK {
					t.Fatalf("status = %v, want OK", r.status)
				}
				if got := initialSyncDirectoryNames(t, r.out, plus); !slices.Equal(got, []string{".", "..", "new-only.txt"}) {
					t.Fatalf("output = %v, want only current entries", got)
				}
				if got := f.listCalls.Load(); got != 2 {
					t.Fatalf("root LIST requests = %d, want exactly two", got)
				}
				f.dh.mu.Lock()
				entries := cloneLoadedDirEntries(f.dh.Entries)
				generation := f.dh.entriesGeneration
				f.dh.mu.Unlock()
				if generation != originalGeneration+1 || len(entries) != 1 || entries[0].Name != "new-only.txt" {
					t.Fatalf("handle snapshot = %v at generation %d", entries, generation)
				}
				cached, installed = f.fs.dirCache.Get("/")
				if !installed || len(cached) != 1 || cached[0].Name != "new-only.txt" {
					t.Fatalf("cache snapshot = %v, installed = %t", cached, installed)
				}
				for _, entry := range f.fs.inodes.Snapshot() {
					want := int64(0)
					if entry.Ino == 1 || plus && entry.Path == "/new-only.txt" {
						want = 1
					}
					if entry.Nlookup != want {
						t.Fatalf("%s lookup references = %d, want %d", entry.Path, entry.Nlookup, want)
					}
				}
				f.assertUnlocked()
				// A following read and reset must both make progress.
				next := f.startRead(plus, 3, 4096)
				f.wait(next.done)
				if next.status != gofuse.OK || len(dirEntryListBuf(t, next.out)) != 0 {
					t.Fatal("following EOF read did not finish correctly")
				}
				f.fs.resetMountView()
			})
		}
	}
}

func TestInitialSyncDirectoryReadExcludedResets(t *testing.T) {
	for _, plus := range []bool{false, true} {
		for _, kind := range []string{"authorization", "rollback", "structural", "after current", "later initial", "mixed before", "mixed after", "multiple initial"} {
			t.Run(kind+"/plus="+map[bool]string{false: "false", true: "true"}[plus], func(t *testing.T) {
				f := newInitialSyncDirectoryFixture(t)
				if kind == "after current" {
					f.watcher.handleStreamCurrent(1)
					f.fs.markStatCacheUnverified() // Disconnect must not re-arm the tag.
				}
				if kind == "later initial" {
					f.initialReset()
				}
				f.beforeList = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
					if call != 1 {
						return false
					}
					switch kind {
					case "authorization":
						_ = f.fs.resetMountViewOnAuthorizationError(&client.StatusError{StatusCode: http.StatusUnauthorized})
					case "rollback":
						f.fs.resetMountView() // Same generic reset entrypoint as layer rollback.
					case "structural":
						f.watcher.handleEvent(nil, &client.ResetEvent{Reason: "structural_change"})
					case "mixed before":
						f.fs.resetMountView()
						f.initialReset()
					case "mixed after":
						f.initialReset()
						f.fs.resetMountView()
					case "multiple initial":
						f.initialReset()
						f.initialReset()
					default:
						f.initialReset()
					}
					return false
				}
				r := f.startRead(plus, 0, 4096)
				f.wait(r.done)
				if r.status != gofuse.Status(syscall.EAGAIN) || f.listCalls.Load() != 1 {
					t.Fatalf("status = %v, LIST requests = %d; want EAGAIN without reload", r.status, f.listCalls.Load())
				}
				f.assertNoOutput(r)
				f.assertUnlocked()
			})
		}
	}
}

func TestInitialSyncDirectoryReadReloadFailures(t *testing.T) {
	for _, plus := range []bool{false, true} {
		for _, failure := range []string{"second reset", "401", "403", "500"} {
			t.Run(failure+"/plus="+map[bool]string{false: "false", true: "true"}[plus], func(t *testing.T) {
				f := newInitialSyncDirectoryFixture(t)
				want := gofuse.Status(syscall.EAGAIN)
				f.beforeList = func(call int32, w http.ResponseWriter, _ *http.Request) bool {
					if call == 1 {
						f.initialReset()
						return false
					}
					if failure == "second reset" {
						f.initialReset()
						return false
					}
					status := http.StatusInternalServerError
					switch failure {
					case "401":
						status = http.StatusUnauthorized
					case "403":
						status = http.StatusForbidden
					}
					http.Error(w, http.StatusText(status), status)
					return true
				}
				if failure == "401" || failure == "403" {
					want = gofuse.EACCES
				}
				r := f.startRead(plus, 0, 4096)
				f.wait(r.done)
				if r.status != want || f.listCalls.Load() != 2 {
					t.Fatalf("status = %v, LIST requests = %d; want %v and exactly two", r.status, f.listCalls.Load(), want)
				}
				f.assertNoOutput(r)
				f.assertUnlocked()
			})
		}
	}
}

func TestInitialSyncDirectoryReadHTTPErrorsDoNotReload(t *testing.T) {
	for _, plus := range []bool{false, true} {
		for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusGatewayTimeout} {
			t.Run(http.StatusText(status)+"/plus="+map[bool]string{false: "false", true: "true"}[plus], func(t *testing.T) {
				f := newInitialSyncDirectoryFixture(t)
				f.beforeList = func(_ int32, w http.ResponseWriter, _ *http.Request) bool {
					f.initialReset()
					http.Error(w, http.StatusText(status), status)
					return true
				}
				r := f.startRead(plus, 0, 4096)
				f.wait(r.done)
				want := gofuse.EACCES
				if status == http.StatusGatewayTimeout {
					want = gofuse.Status(syscall.EAGAIN)
				}
				if r.status != want || f.listCalls.Load() != 1 {
					t.Fatalf("status = %v, requests = %d, want %v without reload", r.status, f.listCalls.Load(), want)
				}
				f.assertNoOutput(r)
			})
		}
	}
}

func TestInitialSyncDirectoryReadCancellation(t *testing.T) {
	for _, duringReload := range []bool{false, true} {
		t.Run(map[bool]string{false: "before reload", true: "during reload"}[duringReload], func(t *testing.T) {
			f := newInitialSyncDirectoryFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			f.releases = append(f.releases, cancel)
			reloadStarted := make(chan struct{})
			if !duringReload {
				testHookAfterDirectoryList = func(fs *Dat9FS, path string, _ uint64) {
					if fs == f.fs && path == "/" {
						f.initialReset()
						cancel()
					}
				}
			}
			f.beforeList = func(call int32, _ http.ResponseWriter, r *http.Request) bool {
				if call == 1 {
					if duringReload {
						f.initialReset()
					}
					return false
				}
				close(reloadStarted)
				<-r.Context().Done()
				return true
			}
			done := make(chan struct{})
			f.reads = append(f.reads, done)
			var readErr error
			go func() {
				defer close(done)
				_, _, readErr = f.fs.loadDirHandleEntriesForRead(ctx, f.dh, false)
				if readErr == nil {
					f.fs.mountViewMu.RUnlock()
				}
			}()
			if duringReload {
				f.wait(reloadStarted)
				cancel()
			}
			f.wait(done)
			if !errors.Is(readErr, context.Canceled) {
				t.Fatalf("read error = %v, want context.Canceled", readErr)
			}
			want := int32(1)
			if duringReload {
				want = 2
			}
			if got := f.listCalls.Load(); got != want {
				t.Fatalf("LIST requests = %d, want %d", got, want)
			}
			if !f.fs.mountViewMu.TryLock() {
				t.Fatal("canceled read retained mount-view lock")
			}
			f.fs.mountViewMu.Unlock()
		})
	}
}

func TestInitialSyncDirectoryReadPreservesOffsetAndRefresh(t *testing.T) {
	for _, plus := range []bool{false, true} {
		for _, gvisor := range []bool{false, true} {
			for _, offset := range []uint64{0, 2} {
				t.Run(map[bool]string{false: "ReadDir", true: "ReadDirPlus"}[plus]+"/gvisor="+map[bool]string{false: "false", true: "true"}[gvisor]+"/offset="+map[uint64]string{0: "0", 2: "2"}[offset], func(t *testing.T) {
					f := newInitialSyncDirectoryFixture(t)
					f.fs.opts.GVisorCompat = gvisor
					f.beforeList = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
						if call == 1 {
							f.initialReset()
						}
						return false
					}
					// One entry per page, including READDIRPLUS's EntryOut prefix.
					size := 40
					if plus {
						size += int(unsafe.Sizeof(gofuse.EntryOut{}))
					}
					var names []string
					for {
						r := f.startRead(plus, offset, size)
						f.wait(r.done)
						if r.status != gofuse.OK {
							t.Fatalf("page status = %v", r.status)
						}
						page := initialSyncDirectoryNames(t, r.out, plus)
						if len(page) == 0 {
							break
						}
						names = append(names, page...)
						offset = r.out.Offset
						if len(names) > 3 {
							t.Fatal("pagination duplicated entries")
						}
					}
					want := []string{"new-only.txt"}
					if len(names) > 1 {
						want = []string{".", "..", "new-only.txt"}
					}
					if !slices.Equal(names, want) || f.listCalls.Load() != 2 {
						t.Fatalf("pages = %v, LIST requests = %d", names, f.listCalls.Load())
					}
					f.fs.dirCache.Upsert("/", CachedFileInfo{Name: "later.txt", Revision: 1})
					rewind := f.startRead(plus, 0, 4096)
					f.wait(rewind.done)
					got := initialSyncDirectoryNames(t, rewind.out, plus)
					if rewind.status != gofuse.OK || slices.Contains(got, "later.txt") != gvisor {
						t.Fatalf("rewind status = %v, entries = %v, gvisor = %t", rewind.status, got, gvisor)
					}
					f.assertUnlocked()
				})
			}
		}
	}
}
