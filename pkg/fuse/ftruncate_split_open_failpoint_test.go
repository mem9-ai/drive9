//go:build failpoint

package fuse

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateSplitOpenLifecycle(t *testing.T) {
	for _, caller := range []struct {
		name          string
		parent, child uint32
	}{
		{"zero", 0, 0},
		{"same", 123, 123},
		{"foreign", 123, 456},
	} {
		for _, order := range []string{"child_fsync_parent_release", "parent_release_child_fsync", "child_fsync_parent_fsync_release"} {
			t.Run(fmt.Sprintf("%s/%s", caller.name, order), func(t *testing.T) {
				fs, ino, parentID, remote, path, _ := newRenameFtruncateAdmissionFS(t, false, caller.parent)
				header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: caller.child}}
				var child gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: uint32(syscall.O_RDWR)}, &child); st != gofuse.OK {
					t.Fatal(st)
				}
				if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.OK {
					t.Fatal(st)
				}
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: child.Fh}, []byte("X")); st != gofuse.OK {
					t.Fatal(st)
				}
				if order == "parent_release_child_fsync" {
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: parentID})
					fs.commitQueue.WaitPath(path)
				}
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: child.Fh}); st != gofuse.OK {
					t.Fatal(st)
				}
				if order == "child_fsync_parent_fsync_release" {
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: parentID}); st != gofuse.OK {
						t.Fatal(st)
					}
				}
				if order != "parent_release_child_fsync" {
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: parentID})
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: child.Fh})
				fs.commitQueue.WaitPath(path)
				remote.mu.Lock()
				defer remote.mu.Unlock()
				if remote.data != "X" || remote.conflicts != 0 || remote.renames != 0 {
					t.Errorf("no-rename RESULT data=%q conflicts=%d renames=%d", remote.data, remote.conflicts, remote.renames)
				}
				if meta, ok := fs.pendingIndex.GetMeta(path); ok && meta.Kind == PendingConflict {
					t.Errorf("no-rename retained conflict: %+v", meta)
				}
				t.Logf("no-rename RESULT data=%q old_fd_progress=true", remote.data)
			})
		}
	}
}

func TestFtruncateSplitOpenCommittedZeroParentFlush(t *testing.T) {
	for _, caller := range []struct {
		name          string
		parent, child uint32
	}{{"zero", 0, 0}, {"foreign", 123, 456}} {
		t.Run(caller.name, func(t *testing.T) {
			fs, ino, parentID, remote, path, _ := newRenameFtruncateAdmissionFS(t, false, caller.parent)
			header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: caller.child}}
			var child gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: uint32(syscall.O_RDWR)}, &child); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatal(st)
			}
			for _, id := range []uint64{parentID, child.Fh} {
				fh, _ := fs.fileHandles.Get(id)
				if fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() || fh.BaseRev != 2 || fh.pendingFtruncate.Load() == nil {
					t.Fatalf("committed zero still pending: fh=%d seq=%d dirty=%t base=%d", id, fh.DirtySeq, fh.Dirty.HasDirtyParts(), fh.BaseRev)
				}
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: child.Fh}, []byte("X")); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: header, Fh: parentID}); st != gofuse.OK {
				t.Fatalf("parent flush=%v", st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: parentID})
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: child.Fh}); st != gofuse.OK {
				t.Fatalf("child fsync after parent close=%v", st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: child.Fh})
			fs.commitQueue.WaitPath(path)
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if remote.data != "X" || remote.conflicts != 0 {
				t.Fatalf("accepted child lost: data=%q conflicts=%d", remote.data, remote.conflicts)
			}
		})
	}
}

func TestFtruncateSplitOpenZeroCommit(t *testing.T) {
	for _, mode := range []string{"put_rejected", "head_unavailable", "later_remote_revision", "missing_revision", "missing_revision_later_remote", "invalid_revision", "wait_for_ack"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, parentID, remote, path, _ := newRenameFtruncateAdmissionFS(t, false, 123)
			header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 456}}
			var child gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: uint32(syscall.O_RDWR)}, &child); st != gofuse.OK {
				t.Fatal(st)
			}
			type savedHandle struct {
				data                                     string
				seq, version, shadow, pending, writeback uint64
				base                                     int64
				id                                       string
				event                                    *ftruncateInheritance
				buffer                                   *WriteBuffer
			}
			snapshot := func(id uint64) savedHandle {
				fh, _ := fs.fileHandles.Get(id)
				fh.Lock()
				defer fh.Unlock()
				return savedHandle{string(fh.Dirty.Bytes()), fh.DirtySeq, uint64(fh.Dirty.contentVersion), fh.ShadowStageGen, fh.PendingIndexGen, fh.WriteBackGen, fh.BaseRev, ftruncateSnapshotID(fh), fh.pendingFtruncate.Load(), fh.Dirty}
			}
			beforeParent, beforeChild := snapshot(parentID), snapshot(child.Fh)
			target, err := url.Parse(fs.client.BaseURL())
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(target)
			var putSuccess, rejected, forbiddenHeads atomic.Int32
			var afterPut, active atomic.Bool
			active.Store(true)
			entered, resume := make(chan struct{}), make(chan struct{})
			var enteredOnce, resumeOnce sync.Once
			unblock := func() { resumeOnce.Do(func() { close(resume) }) }
			t.Cleanup(unblock)
			proxy.ModifyResponse = func(response *http.Response) error {
				if response.Request.Method == http.MethodPut && response.Request.ContentLength == 0 && response.StatusCode < 300 {
					putSuccess.Add(1)
					afterPut.Store(true)
					if mode == "missing_revision" || mode == "missing_revision_later_remote" || mode == "invalid_revision" {
						_ = response.Body.Close()
						body := "{}"
						if mode == "invalid_revision" {
							body = `{"revision":1}`
						}
						response.Body = io.NopCloser(strings.NewReader(body))
						response.ContentLength = int64(len(body))
						response.Header.Set("Content-Length", fmt.Sprint(len(body)))
						response.Header.Del("X-Dat9-Revision")
					}
					if mode == "later_remote_revision" || mode == "missing_revision_later_remote" {
						remote.mu.Lock()
						remote.data, remote.revision = "external", 3
						remote.mu.Unlock()
					}
				}
				return nil
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if active.Load() && r.Method == http.MethodPut && r.ContentLength == 0 {
					if mode == "put_rejected" {
						rejected.Add(1)
						http.Error(w, "revision conflict", http.StatusConflict)
						return
					}
					if mode == "wait_for_ack" {
						enteredOnce.Do(func() { close(entered) })
						<-resume
					}
				}
				if active.Load() && afterPut.Load() && r.Method == http.MethodHead {
					forbiddenHeads.Add(1)
					if mode == "head_unavailable" {
						http.Error(w, "head unavailable", http.StatusServiceUnavailable)
						return
					}
				}
				proxy.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			t.Cleanup(unblock)
			fs.client = newTestClient(server.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			fs.commitQueue.client = fs.client
			fs.uploader.client = fs.client
			truncate := func() gofuse.Status {
				return fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{})
			}
			var status gofuse.Status
			if mode == "wait_for_ack" {
				done := make(chan gofuse.Status, 1)
				go func() { done <- truncate() }()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("empty PUT did not start")
				}
				early := false
				select {
				case status = <-done:
					t.Error("SetAttr acknowledged before empty PUT response")
					early = true
				default:
				}
				if snapshot(parentID) != beforeParent || snapshot(child.Fh) != beforeChild {
					t.Error("local image changed before empty PUT was acknowledged")
				}
				unblock()
				if !early {
					select {
					case status = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("SetAttr did not finish after PUT acknowledgement")
					}
				}
				if t.Failed() {
					return
				}
			} else {
				status = truncate()
			}
			active.Store(false)
			if mode == "put_rejected" || mode == "invalid_revision" {
				if status == gofuse.OK {
					t.Fatalf("unconfirmed truncate returned success: mode=%s", mode)
				}
				if snapshot(parentID) != beforeParent || snapshot(child.Fh) != beforeChild {
					t.Fatal("failed/unconfirmed truncate changed accepted buffers or ownership")
				}
				entry, _ := fs.inodes.GetEntry(ino)
				if entry.Size != 5 || entry.Revision != 1 || fs.pendingIndex.HasPending(path) || fs.commitQueue.HasPath(path) {
					t.Fatalf("failed truncate changed inode or published staging: %+v", entry)
				}
				if mode == "put_rejected" {
					if rejected.Load() == 0 {
						t.Fatal("PUT rejection was not exercised")
					}
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: parentID}); st != gofuse.OK {
						t.Fatalf("earlier accepted truncate cannot commit: %v", st)
					}
					remote.mu.Lock()
					defer remote.mu.Unlock()
					if remote.data != "hello" {
						t.Fatalf("accepted prefix lost: %q", remote.data)
					}
				} else if putSuccess.Load() != 1 {
					t.Fatalf("invalid-revision response not exercised: puts=%d", putSuccess.Load())
				}
				t.Logf("FAULT mode=%s status=%v accepted_buffers_preserved=true", mode, status)
				return
			}
			if status != gofuse.OK || putSuccess.Load() != 1 {
				t.Fatalf("synchronous zero result=%v puts=%d", status, putSuccess.Load())
			}
			entry, _ := fs.inodes.GetEntry(ino)
			if entry.Revision != 2 || entry.Size != 0 || snapshot(parentID).base != 2 || snapshot(child.Fh).base != 2 {
				t.Fatalf("zero image did not retain actual PUT revision2: inode=%+v parent=%+v child=%+v", entry, snapshot(parentID), snapshot(child.Fh))
			}
			if forbiddenHeads.Load() != 0 {
				t.Fatalf("zero commit depended on %d later HEAD requests", forbiddenHeads.Load())
			}
			if fs.pendingIndex.HasPending(path) || fs.commitQueue.HasPath(path) {
				t.Fatal("zero left independent staging/queue work")
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: child.Fh}, []byte("X")); st != gofuse.OK {
				t.Fatalf("write after zero=%v", st)
			}
			fsync := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: child.Fh})
			if mode == "later_remote_revision" || mode == "missing_revision_later_remote" {
				if fsync == gofuse.OK {
					t.Fatal("write adopted a revision belonging to another remote image")
				}
				remote.mu.Lock()
				defer remote.mu.Unlock()
				if remote.data != "external" || remote.revision != 3 {
					t.Fatalf("later external bytes overwritten: %q rev%d", remote.data, remote.revision)
				}
				if snapshot(child.Fh).data != "X" {
					t.Fatal("failed commit discarded accepted child bytes")
				}
			} else {
				if fsync != gofuse.OK {
					t.Fatalf("child fsync=%v", fsync)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: parentID})
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: child.Fh})
				fs.commitQueue.WaitPath(path)
				remote.mu.Lock()
				defer remote.mu.Unlock()
				if remote.data != "X" || remote.conflicts != 0 {
					t.Fatalf("content=%q conflicts=%d", remote.data, remote.conflicts)
				}
				if meta, ok := fs.pendingIndex.GetMeta(path); ok && meta.Kind == PendingConflict {
					t.Fatalf("retained conflict: %+v", meta)
				}
			}
			t.Logf("FAULT mode=%s actual_revision=2 post_put_heads=%d", mode, forbiddenHeads.Load())
		})
	}
}

func TestFtruncateSplitOpenHardlink(t *testing.T) {
	for _, mode := range []string{"child_fsync_parent_release", "parent_release_child_fsync", "dirty_successor_refusal"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, oldA, oldB, remote := newHardlinkFS(t)
			for _, id := range []uint64{oldA, oldB} {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			t.Cleanup(func() {
				for _, fh := range fs.openHandles.SnapshotInode(ino) {
					fh.Lock()
					fs.releaseHandleRemoteCommitPathLocked(fh)
					fh.Unlock()
				}
			})
			parentHeader := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 123}}
			childHeader := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 456}}
			var parent, child gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: parentHeader, Flags: uint32(syscall.O_RDWR)}, &parent); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: childHeader, Flags: uint32(syscall.O_RDWR)}, &child); st != gofuse.OK {
				t.Fatal(st)
			}
			reviewFtruncate(t, fs, ino, parent.Fh, 5)
			entry, _ := fs.inodes.GetEntry(ino)
			if len(entry.Paths) != 2 || entry.Nlink != 2 {
				t.Fatalf("not a shared inode: %+v", entry)
			}
			if mode == "dirty_successor_refusal" {
				hardlinkWrite(t, fs, ino, child.Fh, "H")
				before, _ := fs.fileHandles.Get(child.Fh)
				seq := before.DirtySeq
				otherHeader := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 789}}
				var other gofuse.OpenOut
				st := fs.Open(nil, &gofuse.OpenIn{InHeader: otherHeader, Flags: uint32(syscall.O_RDWR)}, &other)
				if st == gofuse.OK {
					st = fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: otherHeader, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{})
				}
				if st != gofuse.EAGAIN || before.DirtySeq != seq || string(before.Dirty.Bytes()) != "Hello" {
					t.Fatalf("alias dirty successor erased or admitted: status=%v data=%q seq=%d", st, before.Dirty.Bytes(), before.DirtySeq)
				}
				hardlinkSync(t, fs, ino, child.Fh)
				if remote() != "Hello" {
					t.Fatalf("accepted alias bytes lost: %q", remote())
				}
				return
			}
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: childHeader, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatal(st)
			}
			hardlinkWrite(t, fs, ino, child.Fh, "X")
			if mode == "parent_release_child_fsync" {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: parentHeader, Fh: parent.Fh})
				for path := range entry.Paths {
					fs.commitQueue.WaitPath(path)
				}
			}
			hardlinkSync(t, fs, ino, child.Fh)
			if mode != "parent_release_child_fsync" {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: parentHeader, Fh: parent.Fh})
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: childHeader, Fh: child.Fh})
			for path := range entry.Paths {
				fs.commitQueue.WaitPath(path)
				if meta, ok := fs.pendingIndex.GetMeta(path); ok && meta.Kind == PendingConflict {
					t.Fatalf("alias conflict %s: %+v", path, meta)
				}
			}
			if remote() != "X" {
				t.Fatalf("alias truncate/writes diverged: %q", remote())
			}
			t.Logf("HARDLINK mode=%s final=%q", mode, remote())
		})
	}
}
