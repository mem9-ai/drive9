//go:build failpoint

package fuse

import (
	"context"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncateFsyncWaitsForClosingParent(t *testing.T) {
	fs, ino, parentID, remote, path, _ := newRenameFtruncateAdmissionFS(t, false, 123)
	header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 456}}
	var child gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: uint32(syscall.O_RDWR)}, &child); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatal(st)
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: child.Fh}, []byte("X")); n != 1 || st != gofuse.OK {
		t.Fatalf("write=%d/%v", n, st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: header, Fh: parentID}); st != gofuse.OK {
		t.Fatalf("parent flush=%v", st)
	}
	parent, _ := fs.fileHandles.Get(parentID)
	entered, resume, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finished := make(chan gofuse.Status, 1)
	unblock := sync.OnceFunc(func() { close(resume) })
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReleaseBudget"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle, _ context.Context) {
		if observed == fs && fh == parent {
			close(entered)
			<-resume
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		select {
		case <-released:
		case <-time.After(3 * time.Second):
			t.Error("parent Release stuck")
		}
		_ = failpoint.Disable(point)
	})
	go func() {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: parentID})
		close(released)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("parent Release missed held-lock boundary")
	}
	go func() { finished <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: child.Fh}) }()
	select {
	case st := <-finished:
		t.Fatalf("child fsync returned during transient parent close: %v", st)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	select {
	case st := <-finished:
		if st != gofuse.OK {
			t.Fatalf("child fsync after parent release=%v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("child fsync did not resume")
	}
	fs.commitQueue.WaitPath(path)
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if remote.data != "X" || remote.conflicts != 0 {
		t.Fatalf("accepted child lost: data=%q conflicts=%d", remote.data, remote.conflicts)
	}
}

func TestFtruncateFsyncWaitRevalidation(t *testing.T) {
	for _, change := range []string{"cancel_relock", "unlink", "view"} {
		t.Run(change, func(t *testing.T) {
			fs, ino, parentID, remote, _, _ := newRenameFtruncateAdmissionFS(t, false, 123)
			fs.opts.LegacyInterruptibleMutations = true
			if change == "unlink" {
				interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == http.MethodDelete {
						remote.mu.Lock()
						remote.path = ""
						remote.mu.Unlock()
						w.WriteHeader(http.StatusNoContent)
						return true
					}
					return false
				})
			}
			header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 456}}
			var child gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: uint32(syscall.O_RDWR)}, &child); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0,
			}}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatal(st)
			}
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: child.Fh}, []byte("X")); n != 1 || st != gofuse.OK {
				t.Fatalf("write=%d/%v", n, st)
			}
			parent, _ := fs.fileHandles.Get(parentID)
			writer, _ := fs.fileHandles.Get(child.Fh)
			entered, resume, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
			beforeRelock, resumeRelock, synced := make(chan struct{}), make(chan struct{}), make(chan struct{})
			cancel := make(chan struct{})
			unblock, relock := sync.OnceFunc(func() { close(resume) }), sync.OnceFunc(func() { close(resumeRelock) })
			var once sync.Once
			releasePoint := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReleaseBudget"
			relockPoint := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateFsyncBeforeRelock"
			if err := failpoint.EnableCall(releasePoint, func(observed *Dat9FS, h *FileHandle, _ context.Context) {
				if observed == fs && h == parent {
					close(entered)
					<-resume
				}
			}); err != nil {
				t.Fatal(err)
			}
			if err := failpoint.EnableCall(relockPoint, func(observed *Dat9FS, h *FileHandle) {
				if observed == fs && h == writer {
					once.Do(func() { close(beforeRelock); <-resumeRelock })
				}
			}); err != nil {
				t.Fatal(err)
			}
			var unlock func()
			t.Cleanup(func() {
				if unlock != nil {
					unlock()
				}
				unblock()
				relock()
				select {
				case <-released:
				case <-time.After(3 * time.Second):
					t.Error("Release stuck")
				}
				select {
				case <-synced:
				case <-time.After(3 * time.Second):
					t.Error("Fsync stuck")
				}
				_ = failpoint.Disable(releasePoint)
				_ = failpoint.Disable(relockPoint)
			})
			go func() { fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: parentID}); close(released) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("Release missed boundary")
			}
			var status gofuse.Status
			go func() { status = fs.Fsync(cancel, &gofuse.FsyncIn{InHeader: header, Fh: child.Fh}); close(synced) }()
			select {
			case <-beforeRelock:
			case <-time.After(3 * time.Second):
				t.Fatal("Fsync missed relock boundary")
			}
			if change == "cancel_relock" {
				writer.Lock()
				unlock = sync.OnceFunc(writer.Unlock)
				buffer, seq, version := writer.Dirty, writer.DirtySeq, writer.Dirty.contentVersion
				gens := fs.captureHandleStagingGensLocked(writer)
				close(cancel)
				relock()
				select {
				case <-synced:
				case <-time.After(time.Second):
					t.Fatal("canceled Fsync waited for mu")
				}
				if status != gofuse.EAGAIN || writer.Dirty != buffer || writer.DirtySeq != seq || writer.Dirty.contentVersion != version || fs.captureHandleStagingGensLocked(writer) != gens || string(writer.Dirty.Bytes()) != "X" {
					t.Fatalf("canceled Fsync changed accepted state: status=%v", status)
				}
				unlock()
				unblock()
				<-released
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: child.Fh}); st != gofuse.OK {
					t.Fatal(st)
				}
			} else {
				unblock()
				<-released
				if change == "unlink" {
					if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "old"); st != gofuse.OK {
						t.Fatal(st)
					}
				} else {
					fs.resetMountView()
				}
				relock()
				select {
				case <-synced:
				case <-time.After(time.Second):
					t.Fatal("Fsync did not revalidate")
				}
				want := gofuse.EAGAIN
				if change == "unlink" {
					want = gofuse.OK
				}
				if status != want {
					t.Fatalf("Fsync after %s=%v want=%v", change, status, want)
				}
			}
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if change == "cancel_relock" && remote.data != "X" {
				t.Fatal("retry did not commit accepted bytes")
			}
			if change != "cancel_relock" && (remote.data != "" || remote.revision != 2 || remote.conflicts != 0) {
				t.Fatalf("refused/anonymous sync published data: %+v", remote)
			}
		})
	}
}
