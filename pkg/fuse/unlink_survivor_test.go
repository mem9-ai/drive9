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

var testSurvivorDeleteStatus func(string) int
var testSurvivorResourceID func(string) string

func TestIssue986SurvivorBasic(t *testing.T) {
	for _, direction := range []string{"reader", "writer"} {
		for _, dirty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/dirty=%v", direction, dirty), func(t *testing.T) {
				fs, ino, r, rid, w, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
				want := "hello gopher"
				if dirty {
					if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
						t.Fatal(st)
					}
					w.Lock()
					err := fs.stageShadowLocked(w, false)
					w.Unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
				name := "a"
				if direction == "writer" {
					name = "b"
				}
				if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, name); st != gofuse.OK {
					t.Fatal("unlink", st)
				}
				if r.Unlinked || w.Unlinked || r.Path != w.Path {
					t.Fatalf("binding r=%s/%v w=%s/%v", r.Path, r.Unlinked, w.Path, w.Unlinked)
				}
				tail := " gopher"
				if dirty {
					tail = "!"
					want += "!"
				}
				if _, st := pr939Append(fs, ino, wid, tail); st != gofuse.OK {
					t.Fatal(st)
				}
				assertAliasAppendRead(t, fs, ino, rid, want)
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
					t.Fatal(st)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid})
				assertAliasAppendRead(t, fs, ino, rid, want)
				_, body, _ := server.snapshot()
				if string(body) != want {
					t.Fatal("remote", string(body))
				}
				if _, err := fs.client.StatCtx(context.Background(), "/"+name); err == nil {
					t.Fatal("deleted path resurrected")
				}
			})
		}
	}
}
func TestIssue986SurvivorDeleteFailure(t *testing.T) {
	fs, ino, r, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	testSurvivorDeleteStatus = func(string) int { return http.StatusForbidden }
	defer func() { testSurvivorDeleteStatus = nil }()
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st == gofuse.OK {
		t.Fatal("expected delete failure")
	}
	if r.Unlinked || w.Unlinked {
		t.Fatal("failed delete privatized fd")
	}
	for _, p := range []string{"/a", "/b"} {
		if _, err := fs.client.StatCtx(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
		t.Fatal(st)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
}
func TestIssue986SurvivorLateOpen(t *testing.T) {
	for _, flags := range []uint32{uint32(syscall.O_RDONLY), uint32(syscall.O_RDWR)} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			fs, ino, _, _, _, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			defer unblock()
			testHookBeforeSurvivorOpenRegister = func(fh *FileHandle) {
				if fh.Path == "/b" {
					close(entered)
					<-resume
				}
			}
			defer func() { testHookBeforeSurvivorOpenRegister = nil }()
			var opened gofuse.OpenOut
			openedDone := make(chan struct{})
			var openStatus gofuse.Status
			go func() {
				defer close(openedDone)
				openStatus = fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: flags}, &opened)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Open did not reach registration gate")
			}
			// Writable Open holds the old-path fence: allow it to finish after pass1,
			// before DELETE, rather than constructing a test-only deadlock.
			testHookAfterUnlinkFirstMark = func(p string) {
				if p != "/b" {
					return
				}
				unblock()
				<-openedDone
			}
			defer func() { testHookAfterUnlinkFirstMark = nil }()
			if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
				if flags != uint32(syscall.O_RDWR) {
					t.Fatal(st)
				}
				if _, err := fs.client.StatCtx(context.Background(), "/b"); err != nil {
					t.Fatal("failed handoff deleted source", err)
				}
				unblock()
				awaitSurvivorTestGate(t, openedDone, "writable Open completes before retry")
				if retry := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); retry != gofuse.OK {
					t.Fatal("retry after writable Open", retry)
				}
			}
			if openStatus != gofuse.OK {
				t.Fatal(openStatus)
			}
			late, _ := fs.fileHandles.Get(opened.Fh)
			defer fs.deleteFileHandle(opened.Fh, late)
			if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
				t.Fatal(st)
			}
			data, st, err := readDat9FSTestRange(fs, ino, opened.Fh, 5, 7)
			if late.Path != "/a" || late.Unlinked || err != nil || st != gofuse.OK || string(data) != " gopher" {
				t.Fatalf("late Open binding path=%s unlinked=%v read=%q/%v/%v", late.Path, late.Unlinked, data, st, err)
			}
			t.Logf("late Open bound to survivor: flags=%d Path=%s Unlinked=%v tail=%q", flags, late.Path, late.Unlinked, data)
		})
	}
}

func TestIssue986SurvivorCapturedMark(t *testing.T) {
	fs, ino, _, _, _, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	captured, allowRegister := make(chan struct{}), make(chan struct{})
	registered, allowCheck := make(chan struct{}), make(chan struct{})
	var regOnce, checkOnce sync.Once
	releaseReg := func() { regOnce.Do(func() { close(allowRegister) }) }
	releaseCheck := func() { checkOnce.Do(func() { close(allowCheck) }) }
	defer releaseReg()
	defer releaseCheck()
	testHookBeforeSurvivorOpenRegister = func(h *FileHandle) {
		if h.Path == "/b" {
			close(captured)
			<-allowRegister
		}
	}
	defer func() { testHookBeforeSurvivorOpenRegister = nil }()
	testHookAfterSurvivorOpenRegister = func(h *FileHandle) {
		if h.Path == "/b" {
			close(registered)
			<-allowCheck
		}
	}
	defer func() { testHookAfterSurvivorOpenRegister = nil }()
	var out gofuse.OpenOut
	done := make(chan struct{})
	var st gofuse.Status
	go func() {
		defer close(done)
		st = fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	}()
	<-captured
	testHookAfterUnlinkSurvivorPrepare = func(p, s string) {
		if p == "/b" {
			releaseReg()
			<-registered
		}
	}
	defer func() { testHookAfterUnlinkSurvivorPrepare = nil }()
	testHookBeforeUnlinkedTransition = func() { testHookBeforeUnlinkedTransition = nil; releaseCheck(); <-done }
	defer func() { testHookBeforeUnlinkedTransition = nil }()
	if status := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); status != gofuse.OK {
		t.Fatal(status)
	}
	if st != gofuse.OK {
		t.Fatal(st)
	}
	h, _ := fs.fileHandles.Get(out.Fh)
	defer fs.deleteFileHandle(out.Fh, h)
	if h.Unlinked || h.Path != "/a" {
		t.Fatalf("captured mark privatized rebound fd: %+v", h)
	}
	if _, status := pr939Append(fs, ino, wid, " gopher"); status != gofuse.OK {
		t.Fatal(status)
	}
	assertAliasAppendRead(t, fs, ino, out.Fh, "hello gopher")
}
func TestIssue986SurvivorWriteDuringSettle(t *testing.T) {
	fs, ino, _, rid, w, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	testHookAfterUnlinkSurvivorSettle = func(p string) {
		if p != "/b" {
			return
		}
		testHookAfterUnlinkSurvivorSettle = nil
		if _, st := pr939Append(fs, ino, wid, "!"); st != gofuse.OK {
			t.Fatal(st)
		}
		w.Lock()
		err := fs.stageShadowLocked(w, false)
		w.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() { testHookAfterUnlinkSurvivorSettle = nil }()
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.EIO {
		t.Fatalf("unsafe handoff was not rejected: %v", st)
	}
	if _, err := fs.client.StatCtx(context.Background(), "/b"); err != nil {
		t.Fatal("failed preparation deleted source")
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher!")
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
		t.Fatal("safe retry", st)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher!")
}
func TestIssue986SurvivorLastLink(t *testing.T) {
	fs, ino, _, _, w, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	for _, name := range []string{"b", "a"} {
		if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, name); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	if !w.Unlinked {
		t.Fatal("last link must stay private")
	}
	_, _, before := server.snapshot()
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
		t.Fatal(st)
	}
	data, st, err := readDat9FSTestRange(fs, ino, wid, 0, 64)
	if err != nil || st != gofuse.OK || string(data) != "hello gopher" {
		t.Fatalf("private fd=%q/%v/%v", data, st, err)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid})
	_, _, after := server.snapshot()
	if len(after) != len(before) {
		t.Fatal("last-link private write published")
	}
	for _, p := range []string{"/a", "/b"} {
		if _, err := fs.client.StatCtx(context.Background(), p); err == nil {
			t.Fatal("resurrected", p)
		}
	}
}
func TestIssue986SurvivorWarmReadOnly(t *testing.T) {
	fs, ino, r, rid, _, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDONLY), false)
	r.Prefetch = NewPrefetcher(fs.client, r.Path, 5)
	defer r.Prefetch.Close()
	ready := make(chan struct{})
	close(ready)
	r.Prefetch.cache[0] = &prefetchBlock{offset: 0, data: []byte("hello"), ready: ready}
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
		t.Fatal(st)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
}

func awaitSurvivorTestGate(t *testing.T, gate <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-gate:
	case <-time.After(3 * time.Second):
		t.Fatalf("gate timeout: %s", label)
	}
}
func TestIssue986SurvivorPausedProducer(t *testing.T) {
	for _, kind := range []string{"queue", "fsync"} {
		t.Run(kind, func(t *testing.T) {
			fs, ino, _, rid, w, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			server.firstPutStarted = started
			server.releaseFirstPut = release
			if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
				t.Fatal(st)
			}
			synced := make(chan gofuse.Status, 1)
			fsyncJoined := false
			if kind == "queue" {
				cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
				fs.commitQueue = cq
				cq.PathLock = fs.lockRemoteCommitPath
				cq.OnSuccess = fs.onCommitQueueSuccess
				cq.OnCleanup = fs.onCommitQueueCleanup
				defer func() { unblock(); cq.DrainAll() }()
				w.Lock()
				err := fs.stageShadowLocked(w, false)
				if err == nil {
					err = fs.enqueueStagedShadowCommitLocked(w)
				}
				w.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			} else {
				go func() { synced <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}) }()
				defer func() {
					unblock()
					if fsyncJoined {
						return
					}
					select {
					case st := <-synced:
						if st != gofuse.OK {
							t.Errorf("fsync=%v", st)
						}
					case <-time.After(5 * time.Second):
						t.Error("fsync stuck")
					}
				}()
			}
			awaitSurvivorTestGate(t, started, "old-path PUT")
			prepare := make(chan struct{})
			var once sync.Once
			reached := func(p string) {
				if p == "/b" {
					once.Do(func() { close(prepare) })
				}
			}
			if kind == "queue" {
				testHookBeforeUnlinkSurvivorWait = reached
				defer func() { testHookBeforeUnlinkSurvivorWait = nil }()
			} else {
				testHookBeforeUnlinkSurvivorSync = reached
				defer func() { testHookBeforeUnlinkSurvivorSync = nil }()
			}
			testSurvivorDeleteStatus = func(p string) int {
				if p == "/b" {
					_, body, _ := server.snapshot()
					if string(body) != "hello gopher" {
						t.Error("DELETE before old upload committed")
					}
				}
				return 0
			}
			defer func() { testSurvivorDeleteStatus = nil }()
			done := make(chan gofuse.Status, 1)
			go func() { done <- fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b") }()
			awaitSurvivorTestGate(t, prepare, "unlink drain/sync")
			unblock()
			select {
			case st := <-done:
				if st != gofuse.OK {
					if kind != "fsync" {
						t.Fatal("unlink", st)
					}
					select {
					case syncStatus := <-synced:
						fsyncJoined = true
						if syncStatus != gofuse.OK {
							t.Fatal("original fsync", syncStatus)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("original fsync stuck")
					}
					for _, name := range []string{"/a", "/b"} {
						if _, err := fs.client.StatCtx(context.Background(), name); err != nil {
							t.Fatal("failed preparation deleted", name, err)
						}
					}
					if w.Unlinked {
						t.Fatal("failed preparation privatized writer")
					}
					assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
					t.Logf("concurrent fsync rejection preserved names/data: %v; retrying", st)
					if retry := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); retry != gofuse.OK {
						t.Fatal("unlink retry", retry)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("unlink deadlocked")
			}
			assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
			if _, err := fs.client.StatCtx(context.Background(), "/b"); err == nil {
				t.Fatal("old path resurrected")
			}
			t.Logf("%s old-path PUT completed before DELETE; survivor bytes preserved", kind)
		})
	}
}
func TestIssue986SurvivorLateQueuedPublication(t *testing.T) { checkSurvivorLatePublication(t, false) }
func TestIssue986SurvivorCommitAfterStat(t *testing.T)       { checkSurvivorLatePublication(t, true) }

func checkSurvivorLatePublication(t *testing.T, commitBeforeBind bool) {
	t.Helper()
	fs, ino, _, rid, w, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue = cq
	cq.PathLock = func(p string) func() { <-release; return fs.lockRemoteCommitPath(p) }
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	defer func() { unblock(); cq.DrainAll() }()
	var injected bool
	testHookBeforeUnlinkSurvivorBind = func(p string) {
		if p != "/b" || injected {
			return
		}
		injected = true
		if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
			t.Fatal(st)
		}
		w.Lock()
		err := fs.stageShadowLocked(w, false)
		if err == nil {
			err = fs.enqueueStagedShadowCommitLocked(w)
		}
		w.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if commitBeforeBind {
			unblock()
			cq.DrainAll()
			if fs.hasPendingMetadataState(p) || fs.hasQueuedCommit(p) {
				t.Fatal("commit cleanup incomplete")
			}
		}
	}
	defer func() { testHookBeforeUnlinkSurvivorBind = nil }()
	testHookAfterUnlinkFirstMark = func(p string) {
		if p == "/b" {
			unblock()
		}
	}
	defer func() { testHookAfterUnlinkFirstMark = nil }()
	st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b")
	unblock()
	cq.DrainAll()
	if st != gofuse.OK {
		if _, err := fs.client.StatCtx(context.Background(), "/b"); err != nil {
			t.Fatal("failed preparation deleted old name")
		}
		t.Logf("safe preparation rejection: %v", st)
		if retry := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); retry != gofuse.OK {
			t.Fatal("fresh-baseline retry", retry)
		}
	}
	if _, err := fs.client.StatCtx(context.Background(), "/b"); err == nil {
		t.Fatal("retry did not delete source name")
	}
	if !injected {
		t.Fatal("publication window not exercised")
	}
	data, status, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
	_, body, _ := server.snapshot()
	if status != gofuse.OK || err != nil || string(data) != "hello gopher" || string(body) != "hello gopher" {
		t.Fatalf("late queue lost visibility: unlink=%v reader=%q/%v/%v remote=%q writerPath=%s base=%d", st, data, status, err, body, w.Path, w.BaseRev)
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
	assertAliasAppendRead(t, fs, ino, wid, "hello gopher")
	if _, status := pr939Append(fs, ino, wid, "!"); status != gofuse.OK {
		t.Fatal("post-handoff append", status)
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); status != gofuse.OK {
		t.Fatal("post-handoff fsync", status)
	}
	_, finalBody, _ := server.snapshot()
	if string(finalBody) != "hello gopher!" {
		t.Fatalf("post-handoff persisted=%q", finalBody)
	}
	t.Log("late queued generation preserves full read, tail, subsequent append and fsync")
}
func TestIssue986SurvivorIdentityBeforeBind(t *testing.T) {
	for _, mode := range []string{"missing", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			fs, _, r, _, w, _, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			var replaced atomic.Bool
			testSurvivorResourceID = func(p string) string {
				if p == "/a" && replaced.Load() {
					return "different-resource"
				}
				return "alias-append-file"
			}
			defer func() { testSurvivorResourceID = nil }()
			testHookAfterUnlinkSurvivorSettle = func(p string) {
				if p != "/b" {
					return
				}
				if mode == "missing" {
					if err := fs.client.DeleteFileCtx(context.Background(), "/a"); err != nil {
						t.Fatal(err)
					}
				} else {
					replaced.Store(true)
				}
			}
			defer func() { testHookAfterUnlinkSurvivorSettle = nil }()
			st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b")
			if st == gofuse.OK {
				t.Fatal("invalid survivor accepted")
			}
			if _, err := fs.client.StatCtx(context.Background(), "/b"); err != nil {
				t.Fatal("old path deleted on failed survivor check")
			}
			if r.Unlinked || w.Unlinked || w.Path != "/b" {
				t.Fatal("failed validation changed writer binding")
			}
			t.Logf("%s survivor rejected before binding/DELETE: %v", mode, st)
		})
	}
}
func TestIssue986SurvivorLateOpenAfterDelete(t *testing.T) {
	fs, ino, _, _, _, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	testHookBeforeSurvivorOpenRegister = func(h *FileHandle) {
		if h.Path == "/b" {
			close(entered)
			<-resume
		}
	}
	defer func() { testHookBeforeSurvivorOpenRegister = nil }()
	var out gofuse.OpenOut
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &out)
	}()
	awaitSurvivorTestGate(t, entered, "readonly Open register")
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
		t.Fatal(st)
	}
	unblock()
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatal(st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Open stuck")
	}
	h, _ := fs.fileHandles.Get(out.Fh)
	defer fs.deleteFileHandle(out.Fh, h)
	if h.Path != "/a" || h.Unlinked || h.UnlinkedData != nil {
		t.Fatal("late Open escaped with deleted binding/snapshot")
	}
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	assertAliasAppendRead(t, fs, ino, out.Fh, "hello gopher")
}

func TestIssue986SurvivorWriteAfterHandoff(t *testing.T) {
	fs, ino, _, rid, _, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	wrote := false
	testHookAfterUnlinkSurvivorPrepare = func(old, survivor string) {
		if old != "/b" || wrote {
			return
		}
		wrote = true
		if n, st := pr939Append(fs, ino, wid, " gopher"); n != 7 || st != gofuse.OK {
			t.Fatalf("append after bind: %d/%v", n, st)
		}
	}
	defer func() { testHookAfterUnlinkSurvivorPrepare = nil }()
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
		t.Fatal(st)
	}
	if !wrote {
		t.Fatal("write window not hit")
	}
	assertAliasAppendRead(t, fs, ino, rid, "hello gopher")
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
		t.Fatal(st)
	}
	_, body, _ := server.snapshot()
	if string(body) != "hello gopher" {
		t.Fatalf("dirty successor overwritten: %q", body)
	}
	if _, err := fs.client.StatCtx(context.Background(), "/b"); err == nil {
		t.Fatal("old name resurrected")
	}
}

func TestIssue986SurvivorPreflightNoMutation(t *testing.T) {
	for _, mode := range []string{"new", "zero-base", "private", "index-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, r, _, w, _, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			before, _ := fs.inodes.GetEntry(ino)
			rBuffer, wBuffer := r.Dirty, w.Dirty
			if err := fs.shadowStore.WriteFull("/b", []byte("hello"), 1); err != nil {
				t.Fatal(err)
			}
			gen := fs.shadowStore.Pin("/b")
			w.UnlinkedShadowGen = gen
			w.UnlinkedSnapshot = true
			w.UnlinkedSize = 5
			w.UnlinkedData = []byte("hello")
			defer func() { w.Unlinked = false; fs.rollbackUnlinkedSnapshotAttachLocked(w, 0) }()
			testHookBeforeUnlinkSurvivorBind = func(string) {
				switch mode {
				case "new":
					w.IsNew = true
				case "zero-base":
					w.ZeroBase = true
				case "private":
					w.Unlinked = true
				case "index-mismatch":
					fs.openHandles.mu.Lock()
					fs.openHandles.pathByHandle[w] = "/wrong"
					fs.openHandles.mu.Unlock()
				}
			}
			defer func() { testHookBeforeUnlinkSurvivorBind = nil }()
			st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b")
			if st == gofuse.OK {
				t.Fatal("ineligible binding accepted")
			}
			after, _ := fs.inodes.GetEntry(ino)
			if after.Path != before.Path || r.Path != "/a" || w.Path != "/b" || r.Dirty != rBuffer || w.Dirty != wBuffer || w.UnlinkedShadowGen != gen || string(w.UnlinkedData) != "hello" {
				t.Fatal("failed preflight partially mutated bindings/snapshot")
			}
			fs.shadowStore.mu.RLock()
			refs := fs.shadowStore.refs[gen]
			fs.shadowStore.mu.RUnlock()
			if refs != 1 {
				t.Fatalf("failed preflight changed pin refs=%d", refs)
			}
			if mode == "index-mismatch" {
				fs.openHandles.mu.Lock()
				if fs.openHandles.pathByHandle[w] != "/wrong" {
					t.Error("failed check changed index")
				}
				fs.openHandles.pathByHandle[w] = "/b"
				fs.openHandles.mu.Unlock()
			}
			for _, name := range []string{"/a", "/b"} {
				if _, err := fs.client.StatCtx(context.Background(), name); err != nil {
					t.Fatal("failed preflight deleted", name)
				}
			}
			t.Logf("%s rejected with preferred path, all handles, buffers and pin ownership unchanged", mode)
		})
	}
}
func TestIssue986SurvivorRegistrationAtPreflight(t *testing.T) {
	fs, ino, _, _, _, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	var out gofuse.OpenOut
	registered := false
	testHookBeforeUnlinkSurvivorIndexCheck = func() {
		if registered {
			return
		}
		registered = true
		if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	defer func() { testHookBeforeUnlinkSurvivorIndexCheck = nil }()
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st == gofuse.OK {
		t.Fatal("missed registration did not invalidate preflight")
	}
	entry, _ := fs.inodes.GetEntry(ino)
	if entry.Path != "/b" {
		t.Fatal("failed preflight published preferred path")
	}
	h, _ := fs.fileHandles.Get(out.Fh)
	defer fs.deleteFileHandle(out.Fh, h)
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
		t.Fatal("fresh snapshot retry", st)
	}
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	assertAliasAppendRead(t, fs, ino, out.Fh, "hello gopher")
}
func TestIssue986SurvivorMarkedOpen(t *testing.T) {
	for _, mode := range []string{"rebind", "identity-error", "delete-failure"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, _, _, _, wid, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			if mode == "delete-failure" {
				testSurvivorDeleteStatus = func(p string) int {
					if p == "/b" {
						return http.StatusForbidden
					}
					return 0
				}
				defer func() { testSurvivorDeleteStatus = nil }()
			}
			captured, allowReg, registered, allowCheck := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var regOnce, checkOnce sync.Once
			releaseReg := func() { regOnce.Do(func() { close(allowReg) }) }
			releaseCheck := func() { checkOnce.Do(func() { close(allowCheck) }) }
			defer releaseReg()
			defer releaseCheck()
			var h *FileHandle
			testHookBeforeSurvivorOpenRegister = func(fh *FileHandle) {
				if fh.Path == "/b" {
					h = fh
					close(captured)
					<-allowReg
				}
			}
			defer func() { testHookBeforeSurvivorOpenRegister = nil }()
			testHookAfterSurvivorOpenRegister = func(fh *FileHandle) {
				if fh == h {
					close(registered)
					<-allowCheck
				}
			}
			defer func() { testHookAfterSurvivorOpenRegister = nil }()
			var out gofuse.OpenOut
			done := make(chan gofuse.Status, 1)
			go func() { done <- fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out) }()
			awaitSurvivorTestGate(t, captured, "Open captures old path")
			testHookAfterUnlinkSurvivorPrepare = func(old, target string) {
				if old == "/b" {
					releaseReg()
					awaitSurvivorTestGate(t, registered, "late registration")
				}
			}
			defer func() { testHookAfterUnlinkSurvivorPrepare = nil }()
			var openStatus gofuse.Status
			testHookAfterUnlinkFirstMark = func(p string) {
				if p != "/b" {
					return
				}
				h.Lock()
				marked := h.Unlinked
				h.Unlock()
				if !marked {
					t.Fatal("mark window was not reached")
				}
				if mode == "identity-error" {
					testSurvivorResourceID = func(p string) string {
						if p == "/a" {
							return "replacement"
						}
						return "alias-append-file"
					}
				}
				releaseCheck()
				select {
				case openStatus = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("Open postcheck stuck")
				}
			}
			defer func() { testHookAfterUnlinkFirstMark = nil; testSurvivorResourceID = nil }()
			st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b")
			if mode == "delete-failure" {
				if st == gofuse.OK {
					t.Fatal("DELETE unexpectedly succeeded")
				}
				for _, name := range []string{"/a", "/b"} {
					if _, err := fs.client.StatCtx(context.Background(), name); err != nil {
						t.Fatal("failed DELETE lost", name)
					}
				}
			} else if st != gofuse.OK {
				t.Fatal(st)
			}
			if mode == "identity-error" {
				if openStatus == gofuse.OK {
					t.Fatal("identity mismatch exposed fd")
				}
				if _, ok := fs.fileHandles.Get(out.Fh); ok {
					t.Fatal("failed Open leaked handle table entry")
				}
				for _, indexed := range fs.openHandles.SnapshotInode(ino) {
					if indexed == h {
						t.Fatal("failed Open leaked inode entry")
					}
				}
				if h.UnlinkedData != nil || h.UnlinkedShadowGen != 0 || h.ShadowPinned {
					t.Fatal("failed Open leaked snapshot/pin")
				}
			} else {
				if openStatus != gofuse.OK {
					t.Fatal(openStatus)
				}
				defer fs.deleteFileHandle(out.Fh, h)
				if h.Path != "/a" || h.Unlinked || h.UnlinkedData != nil {
					t.Fatal("marked Open did not rebind")
				}
				if p, ok := fs.openHandles.Path(h); !ok || p != "/a" {
					t.Fatal("marked Open missing survivor index")
				}
				if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
					t.Fatal(st)
				}
				assertAliasAppendRead(t, fs, ino, out.Fh, "hello gopher")
			}
		})
	}
}
func TestIssue986SurvivorStaleOpenStat(t *testing.T) {
	fs, ino, _, _, _, wid, server := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
	captured, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	testHookBeforeSurvivorOpenRegister = func(h *FileHandle) {
		if h.Path == "/b" {
			close(captured)
			<-resume
		}
	}
	defer func() { testHookBeforeSurvivorOpenRegister = nil }()
	var out gofuse.OpenOut
	done := make(chan gofuse.Status, 1)
	go func() { done <- fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out) }()
	awaitSurvivorTestGate(t, captured, "Open captured old name")
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "b"); st != gofuse.OK {
		t.Fatal(st)
	}
	var advanceOnce sync.Once
	testHookAfterOpenSurvivorStat = func(h *FileHandle) {
		advanceOnce.Do(func() {
			if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
				t.Error(st)
				return
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
				t.Error(st)
			}
		})
	}
	defer func() { testHookAfterOpenSurvivorStat = nil }()
	unblock()
	select {
	case st := <-done:
		if st != gofuse.EIO {
			t.Fatalf("stale binding accepted: %v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Open stuck")
	}
	if _, ok := fs.fileHandles.Get(out.Fh); ok {
		t.Fatal("failed stale Open leaked fd")
	}
	_, body, _ := server.snapshot()
	if string(body) != "hello gopher" {
		t.Fatal("committed data changed")
	}
	var fresh gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &fresh); st != gofuse.OK {
		t.Fatal("fresh Open", st)
	}
	h, _ := fs.fileHandles.Get(fresh.Fh)
	defer fs.deleteFileHandle(fresh.Fh, h)
	assertAliasAppendRead(t, fs, ino, fresh.Fh, "hello gopher")
}
