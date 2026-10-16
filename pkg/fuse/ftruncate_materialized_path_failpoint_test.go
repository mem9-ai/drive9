//go:build failpoint

package fuse

import (
	"bytes"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncatePathCommitRetirementRevalidates(t *testing.T) {
	for _, mode := range []string{"event", "view", "commit"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, _ := truncateHandoffFS(t, []byte("hello world"))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
			truncateHandoffTruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			reader, _ := fs.fileHandles.Get(ro)
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/pathTruncateObservationBeforeRetire"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
				if observed != fs || fh != reader {
					return
				}
				reached = true
				switch mode {
				case "event":
					event := *reader.pendingFtruncate.Load()
					event.seq++
					reader.pendingFtruncate.Store(&event)
				case "view":
					fs.mountViewGeneration.Add(1)
				case "commit":
					state := stateOf(fs, ino)
					fs.recordCommittedMutation(ino, state.committedSeq+1, state.committedRevision+1, 3)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 3,
			}}, &gofuse.AttrOut{})
			_ = failpoint.Disable(point)
			if st != gofuse.OK || !reached || reader.pendingFtruncate.Load() == nil {
				t.Fatalf("status=%v reached=%t pending=%v", st, reached, reader.pendingFtruncate.Load())
			}
		})
	}
}

func TestFtruncateLargeMutationRetirementRevalidates(t *testing.T) {
	for _, mode := range []string{"event", "view", "commit", "identity", "pending"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("P"), (2<<20)+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 2<<20)
			hardlinkSync(t, fs, ino, a)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			reader, _ := fs.fileHandles.Get(b)
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateResetVerified"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
				if observed != fs || fh != reader {
					return
				}
				reached = true
				switch mode {
				case "event":
					event := *reader.pendingFtruncate.Load()
					event.seq++
					reader.pendingFtruncate.Store(&event)
				case "view":
					fs.mountViewGeneration.Add(1)
				case "commit":
					state := stateOf(fs, ino)
					fs.recordCommittedMutation(ino, state.committedSeq+1, 3, 2<<20)
				case "identity":
					fs.inodes.SetIdentity(ino, "replacement", 2)
				case "pending":
					if _, err := fs.pendingIndex.PutWithBaseRev(reader.Path, 2<<20, PendingOverwrite, 2); err != nil {
						t.Error(err)
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H"))
			_ = failpoint.Disable(point)
			if !reached || st == gofuse.OK || n != 0 || reader.DirtySeq != 0 || reader.Dirty.HasDirtyParts() || reader.pendingFtruncate.Load() == nil {
				t.Fatalf("unverified %s mutation: reached=%t Write=%d/%v seq=%d", mode, reached, n, st, reader.DirtySeq)
			}
			for _, path := range fs.ftruncateAliasPaths(ino) {
				unlock, ok := fs.tryLockRemoteCommitPath(path)
				if !ok {
					t.Fatal("refusal leaked an alias fence")
				}
				unlock()
			}
		})
	}
}

func TestFtruncateLargeAppendRetirementWaits(t *testing.T) {
	for _, mode := range []string{"positive", "unlimited", "deadline", "cancel", "new-staging"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("P"), (2<<20)+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR|syscall.O_APPEND)
			ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
			truncateHandoffTruncate(t, fs, ino, a, 2<<20)
			hardlinkSync(t, fs, ino, a)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			reader, _ := fs.fileHandles.Get(b)
			observer, _ := fs.fileHandles.Get(ro)
			fs.opts.RemoteCommitWaitTimeout = 5 * time.Second
			switch mode {
			case "unlimited":
				fs.opts.RemoteCommitWaitTimeout = 0
			case "deadline":
				fs.opts.RemoteCommitWaitTimeout = 30 * time.Millisecond
			}
			var lockOnce sync.Once
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateResetVerified"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
				if observed == fs && fh == reader {
					lockOnce.Do(observer.Lock)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			waiting := make(chan struct{}, 1)
			previous := testHookBeforeAppendSourceWait
			testHookBeforeAppendSourceWait = func(source *FileHandle) {
				if source == observer {
					waiting <- struct{}{}
				}
			}
			t.Cleanup(func() { testHookBeforeAppendSourceWait = previous })
			cancel := make(chan struct{})
			type result struct {
				n      uint32
				status gofuse.Status
			}
			finished := make(chan result, 1)
			go func() {
				n, status := fs.Write(cancel, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H"))
				finished <- result{n, status}
			}()
			select {
			case <-waiting:
			case got := <-finished:
				t.Fatalf("write returned before the busy sibling wait: %+v", got)
			case <-time.After(5 * time.Second):
				t.Fatal("write did not reach the existing append wait")
			}
			unlockObserver := sync.OnceFunc(observer.Unlock)
			t.Cleanup(unlockObserver)
			if !reader.TryLock() {
				t.Fatal("append wait retained its target mutex")
			}
			reader.Unlock()
			for _, path := range fs.ftruncateAliasPaths(ino) {
				unlock, ok := fs.tryLockRemoteCommitPath(path)
				if !ok {
					t.Fatal("append wait retained an alias fence")
				}
				unlock()
			}
			switch mode {
			case "cancel":
				close(cancel)
			case "new-staging":
				if _, err := fs.pendingIndex.PutWithBaseRev(reader.Path, 2<<20, PendingOverwrite, 2); err != nil {
					t.Fatal(err)
				}
				unlockObserver()
			case "positive", "unlimited":
				unlockObserver()
			}
			select {
			case got := <-finished:
				unlockObserver()
				_ = failpoint.Disable(point)
				if mode == "positive" || mode == "unlimited" {
					if got.n != 1 || got.status != gofuse.OK {
						t.Fatalf("append retry failed: %+v", got)
					}
					hardlinkSync(t, fs, ino, b)
					if data, rev, _ := remote.snapshot(); rev != 3 || len(data) != (2<<20)+1 || data[len(data)-1] != 'H' {
						t.Fatal("append retry lost the accepted record")
					}
				} else if got.n != 0 || got.status == gofuse.OK || reader.DirtySeq != 0 || reader.pendingFtruncate.Load() == nil {
					t.Fatalf("rejected retry changed accepted state: %+v seq=%d", got, reader.DirtySeq)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("append wait did not respect wakeup/refusal/deadline/cancellation")
			}
		})
	}
}
