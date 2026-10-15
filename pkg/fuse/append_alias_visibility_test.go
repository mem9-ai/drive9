package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newAliasAppendTestFS(t *testing.T, flags uint32, writerFirst bool, requestHooks ...func(*http.Request)) (*Dat9FS, uint64, *FileHandle, uint64, *FileHandle, uint64, *casFileServer) {
	t.Helper()
	server := &casFileServer{t: t, path: "/a", revision: 1, body: []byte("hello")}
	var aliasMu sync.Mutex
	aliases := map[string]bool{"/a": true}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, hook := range requestHooks {
			hook(r)
		}
		name := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		if r.Method == http.MethodDelete && testSurvivorDeleteStatus != nil {
			if status := testSurvivorDeleteStatus(name); status != 0 {
				http.Error(w, "injected delete failure", status)
				return
			}
		}
		aliasMu.Lock()
		if r.Method == http.MethodPost && r.URL.Query().Get("hardlink") == "1" {
			source := r.Header.Get("X-Dat9-Hardlink-Source")
			if !aliases[source] || aliases[name] {
				aliasMu.Unlock()
				http.Error(w, "invalid link", http.StatusConflict)
				return
			}
			aliases[name] = true
			aliasMu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		if !aliases[name] {
			aliasMu.Unlock()
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodDelete {
			delete(aliases, name)
			aliasMu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		nlink := len(aliases)
		aliasMu.Unlock()

		resourceID := "alias-append-file"
		if testSurvivorResourceID != nil {
			resourceID = testSurvivorResourceID(name)
		}
		w.Header().Set("X-Dat9-Resource-ID", resourceID)
		w.Header().Set("X-Dat9-Nlink", strconv.Itoa(nlink))
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" {
			_, body, _ := server.snapshot()
			var first, last int
			if n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &first, &last); err != nil || n != 2 || first < 0 || last < first {
				t.Errorf("invalid range %q", r.Header.Get("Range"))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if first >= len(body) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			last = min(last, len(body)-1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(body)))
			w.Header().Set("Content-Length", strconv.Itoa(last-first+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[first : last+1])
			return
		}
		copyRequest := r.Clone(r.Context())
		copyURL := *r.URL
		copyURL.Path = "/v1/fs/a"
		copyRequest.URL = &copyURL
		server.serveHTTP(w, copyRequest)
	}))
	t.Cleanup(ts.Close)
	fs, ino := pr939HandleFS(t, "/a", "hello")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.opts.GVisorCompat = false
	fs.opts.WritePolicy = WritePolicyWriteBack
	fs.syncMode = SyncStrict
	fs.pendingIndex.setShadowStore(fs.shadowStore)
	fs.inodes.SetIdentity(ino, "alias-append-file", 1)
	open := func(flags uint32) (*FileHandle, uint64) {
		var out gofuse.OpenOut
		if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: flags}, &out); st != gofuse.OK {
			t.Fatal(st)
		}
		h, ok := fs.fileHandles.Get(out.Fh)
		if !ok {
			t.Fatal("opened handle missing")
		}
		t.Cleanup(func() {
			fs.releaseHandleShadowPin(h)
			fs.deleteFileHandle(out.Fh, h)
		})
		return h, out.Fh
	}
	var r, w *FileHandle
	var rid, wid uint64
	if writerFirst {
		w, wid = open(uint32(syscall.O_WRONLY | syscall.O_APPEND))
	} else {
		r, rid = open(flags)
	}
	var linked gofuse.EntryOut
	if st := fs.Link(nil, &gofuse.LinkIn{InHeader: gofuse.InHeader{NodeId: 1}, Oldnodeid: ino}, "b", &linked); st != gofuse.OK {
		t.Fatal(st)
	}
	if writerFirst {
		r, rid = open(flags)
	} else {
		w, wid = open(uint32(syscall.O_WRONLY | syscall.O_APPEND))
	}
	if r.Ino != w.Ino || r.Path == w.Path || linked.NodeId != ino {
		t.Fatalf("bad production alias fixture: reader=%s writer=%s", r.Path, w.Path)
	}
	return fs, ino, r, rid, w, wid, server
}

func assertAliasAppendRead(t *testing.T, fs *Dat9FS, ino, rid uint64, want string) {
	t.Helper()
	for _, off := range []int64{0, 5} {
		data, st, err := readDat9FSTestRange(fs, ino, rid, off, 64)
		if err != nil || st != gofuse.OK || string(data) != want[off:] {
			t.Fatalf("offset=%d got=%q status=%v err=%v want=%q", off, data, st, err, want[off:])
		}
	}
}

func TestIssue986AliasOpenLinkRead(t *testing.T) {
	for _, first := range []bool{false, true} {
		for _, flags := range []uint32{uint32(syscall.O_RDONLY), uint32(syscall.O_RDWR)} {
			t.Run(fmt.Sprintf("writerFirst=%v/flags=%d", first, flags), func(t *testing.T) {
				fs, ino, _, rid, _, wid, _ := newAliasAppendTestFS(t, flags, first)
				if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
					t.Fatal(st)
				}
				assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
			})
		}
	}
}

func TestIssue986AliasPublished(t *testing.T) {
	for _, during := range []bool{false, true} {
		for _, flags := range []uint32{uint32(syscall.O_RDONLY), uint32(syscall.O_RDWR)} {
			t.Run(fmt.Sprintf("during=%v/flags=%d", during, flags), func(t *testing.T) {
				fs, ino, r, rid, w, wid, _ := newAliasAppendTestFS(t, flags, false)
				// Warm old reader state, including a prefetch image that must not win after commit.
				got, st, err := readDat9FSTestRange(fs, ino, rid, 0, 5)
				if err != nil || st != gofuse.OK || string(got) != "hello" {
					t.Fatal("warm", string(got), st, err)
				}
				r.Prefetch = NewPrefetcher(fs.client, r.Path, 5)
				t.Cleanup(r.Prefetch.Close)
				ready := make(chan struct{})
				close(ready)
				r.Prefetch.cache[0] = &prefetchBlock{offset: 0, data: []byte("hello"), ready: ready}
				if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
					t.Fatal(st)
				}
				cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
				fs.commitQueue = cq
				release := make(chan struct{})
				cq.PathLock = func(string) func() { <-release; return func() {} }
				cq.OnSuccess = fs.onCommitQueueSuccess
				cq.OnCleanup = fs.onCommitQueueCleanup
				defer cq.DrainAll()
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				stage := func() {
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
				if during {
					testHookAfterSamePathDirtyHandleScan = func(string) { testHookAfterSamePathDirtyHandleScan = nil; stage() }
					defer func() { testHookAfterSamePathDirtyHandleScan = nil }()
				} else {
					stage()
					fs.deleteFileHandle(wid, w)
				}
				assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
				unblock()
				cq.DrainAll()
				assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
			})
		}
	}
}

func TestIssue986AliasPrefix(t *testing.T) {
	fs, ino, r, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDONLY), false)
	w.Lock()
	w.Dirty = fs.newWriteBuffer(w.Path, maxPreloadSize, 5)
	w.Dirty.totalSize = 5
	w.Dirty.remoteSize = 5
	w.Dirty.markCommittedPrefix(1, 5)
	loads := 0
	w.Dirty.LoadPart = func(int) ([]byte, error) { loads++; return nil, syscall.EIO }
	w.Unlock()
	if _, st := pr939Append(fs, ino, wid, "!"); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.readCache.Put(r.Path, []byte("hello"), 1)
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 5)
	if err != nil || st != gofuse.OK || string(data) != "hello" || loads != 0 {
		t.Fatalf("prefix=%q/%v/%v loads=%d", data, st, err, loads)
	}
	data, st, err = readDat9FSTestRange(fs, ino, rid, 5, 1)
	if err != nil || st != gofuse.OK || string(data) != "!" {
		t.Fatal("tail", string(data), st, err)
	}
	fs.remoteReadTimeout = 5 * time.Millisecond
	_, st, _ = readDat9FSTestRange(fs, ino, rid, 0, 6)
	if st != gofuse.EIO {
		t.Fatal("mixed", st)
	}
}

func TestIssue986AliasFsyncABA(t *testing.T) {
	fs, ino, r, rid, _, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	testHookAfterAppendRead = func(string) {
		testHookAfterAppendRead = nil
		if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: rid}, []byte("HELLO")); st != gofuse.OK {
			t.Fatal(st)
		}
		if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: rid}); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	defer func() { testHookAfterAppendRead = nil }()
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 5)
	_, body, _ := server.snapshot()
	if err != nil || st != gofuse.OK || string(data) != "HELLO" || string(body) != "HELLO" || r.DirtySeq != 0 {
		t.Fatalf("ABA=%q/%v/%v server=%q", data, st, err, body)
	}
}

func TestIssue986AliasActiveShadow(t *testing.T) {
	fs, ino, _, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	w.Lock()
	err := fs.stageShadowLocked(w, false)
	w.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
}

func TestIssue986AliasSourceRetarget(t *testing.T) {
	fs, ino, _, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	testHookAfterSamePathDirtyHandleScan = func(string) {
		testHookAfterSamePathDirtyHandleScan = nil
		fs.finishLocalRename(&gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, w.Path, "/moved")
	}
	defer func() { testHookAfterSamePathDirtyHandleScan = nil }()
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
}

func TestIssue986AliasPublishedSafety(t *testing.T) {
	for _, mode := range []string{"generation", "same-revision", "postpin-generation", "replacement", "ambiguous", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, r, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
				t.Fatal(st)
			}
			w.Lock()
			err := fs.stageShadowLocked(w, false)
			w.Dirty.ClearDirty()
			w.DirtySeq = 0
			w.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			fs.deleteFileHandle(wid, w)
			fs.remoteReadTimeout = 5 * time.Millisecond
			switch mode {
			case "generation":
				if err := fs.shadowStore.WriteFull(w.Path, []byte("WRONG"), 0); err != nil {
					t.Fatal(err)
				}
			case "same-revision":
				if err := fs.shadowStore.WriteFull(w.Path, []byte("WRONG"), 1); err != nil {
					t.Fatal(err)
				}
			case "postpin-generation":
				testHookAfterAliasPublishedPin = func(p string) {
					testHookAfterAliasPublishedPin = nil
					if err := fs.shadowStore.WriteFull(p, []byte("WRONG"), 1); err != nil {
						t.Fatal(err)
					}
				}
				defer func() { testHookAfterAliasPublishedPin = nil }()
			case "replacement":
				testHookAfterAliasPublishedPin = func(p string) {
					testHookAfterAliasPublishedPin = nil
					fs.inodes.RemoveLink(p)
					fs.inodes.LookupWithIdentity(p, "replacement", 1, false, 5, time.Now())
				}
				defer func() { testHookAfterAliasPublishedPin = nil }()
			case "ambiguous":
				if err := fs.shadowStore.WriteFull(r.Path, []byte("WRONG"), 1); err != nil {
					t.Fatal(err)
				}
				if _, err := fs.pendingIndex.PutWithBaseRev(r.Path, 5, PendingOverwrite, 1); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				if err := fs.shadowStore.WriteFull(w.Path, []byte("WRONG"), 0); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "canceled" {
				cancel := make(chan struct{})
				close(cancel)
				_, st := fs.Read(cancel, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: rid, Size: 64}, make([]byte, 64))
				if st != gofuse.EINTR {
					t.Fatalf("cancel=%v", st)
				}
				return
			}
			data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
			if err != nil || st != gofuse.EIO {
				t.Fatalf("unsafe fallback=%q status=%v err=%v", data, st, err)
			}
		})
	}
}

func TestIssue986AliasWriteBackSnapshot(t *testing.T) {
	fs, ino, _, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = cache
	if err := cache.PutWithBaseRev(w.Path, []byte("hello gopher"), 12, PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	w.Lock()
	w.Dirty.ClearDirty()
	w.DirtySeq = 0
	w.Unlock()
	fs.deleteFileHandle(wid, w)
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
}

func TestIssue986AliasMetadataOnlyDoesNotWait(t *testing.T) {
	fs, ino, _, rid, w, _, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	if _, err := fs.pendingIndex.PutWithBaseRev(w.Path, 5, PendingChmod, 1); err != nil {
		t.Fatal(err)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello")
}

func TestIssue986AliasCancelDuringPublishedRead(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			fs, ino, r, _, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
				t.Fatal(st)
			}
			w.Lock()
			err := fs.stageShadowLocked(w, false)
			w.Dirty.ClearDirty()
			w.DirtySeq = 0
			w.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			fs.deleteFileHandle(wid, w)
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			entered := false
			testHookAfterAliasPublishedPin = func(string) {
				entered = true
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
			}
			defer func() { testHookAfterAliasPublishedPin = nil }()
			_, _, _, st, _, _ := fs.readActiveAppendVisibleRange(ctx, r.Path, ino, r, 0, 64)
			want := gofuse.EINTR
			if deadline {
				want = gofuse.EIO
			}
			if !entered || st != want {
				t.Fatalf("during-read entered=%v status=%v want=%v", entered, st, want)
			}
		})
	}
}

func TestIssue986AliasDelayedStatAfterCommit(t *testing.T) {
	fs, ino, r, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	r.Lock()
	err := r.Dirty.EnsureLoaded(0)
	r.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	stale, err := fs.client.StatCtx(context.Background(), r.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	// Keep this reader busy across commit publication and the delayed Stat.
	// Commit-side TryLock refresh legitimately skips it; the next Read must
	// recover using the published fence even after inode metadata regresses.
	r.Lock()
	unlockReader := sync.OnceFunc(r.Unlock)
	t.Cleanup(unlockReader)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
		t.Fatal(st)
	}
	if fs.latestCommittedRevision(w.Path) != 2 {
		t.Fatal("missing alias commit publication")
	}
	entry, _ := fs.inodes.GetEntry(ino)
	fs.updateEntryFromStat(entry, stale)
	if fs.inodes.GetRevision(ino) != 1 || r.BaseRev != 1 {
		t.Fatal("late stat precondition")
	}
	unlockReader()
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
}
