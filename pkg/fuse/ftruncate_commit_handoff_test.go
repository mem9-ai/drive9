package fuse

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateAppendWaitsForBusyWritableSibling(t *testing.T) {
	for _, budget := range []time.Duration{time.Second, 0, -time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			fs, ino, a, unused, _ := newFtruncateCommitFS(t)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: unused})
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			reviewFtruncate(t, fs, ino, a, 5)
			fs.opts.RemoteCommitWaitTimeout = budget
			parent, _ := fs.fileHandles.Get(a)
			target, _ := fs.fileHandles.Get(opened.Fh)
			before := target.Dirty
			parent.Lock()
			unlock := sync.OnceFunc(parent.Unlock)
			t.Cleanup(unlock)
			waiting := make(chan struct{}, 1)
			testHookBeforeAppendSourceWait = func(source *FileHandle) {
				if source == parent {
					if !target.TryLock() {
						t.Error("preflight wait retained the target handle lock")
					} else {
						if target.RemoteCommitUnlock != nil || target.Dirty != before || target.DirtySeq != 0 || target.ftruncateInherited != "" {
							t.Error("preflight wait retained a fence or changed the target image")
						}
						if release, ok := fs.tryLockFtruncateAliases(target); ok {
							release()
						} else {
							t.Error("preflight wait retained an alias path fence")
						}
						target.Unlock()
					}
					select {
					case waiting <- struct{}{}:
					default:
					}
				}
			}
			t.Cleanup(func() { testHookBeforeAppendSourceWait = nil })
			type result struct {
				n  uint32
				st gofuse.Status
			}
			done := make(chan result, 1)
			started := time.Now()
			go func() {
				n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}, []byte("!"))
				done <- result{n, st}
			}()
			var got result
			select {
			case got = <-done:
				t.Logf("returned before source-wait hook: n=%d status=%v elapsed=%v", got.n, got.st, time.Since(started))
				unlock()
			case <-waiting:
				unlock()
				select {
				case got = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("append did not complete after source release")
				}
			case <-time.After(3 * time.Second):
				unlock()
				t.Fatal("no source wait or result")
			}
			if got.n != 1 || got.st != gofuse.OK {
				t.Fatalf("append with source released before deadline=%d/%v, want 1/OK", got.n, got.st)
			}
		})
	}
}

func TestFtruncateLargeCommittedWritableAliasRead(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	fs.inodes.SetIdentity(ino, "review-inode", 2) // Match interceptFtruncateHTTP below.
	fs.client.SetSmallFileThresholdForTests(8 << 20)
	var observer gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &observer); st != gofuse.OK {
		t.Fatal(st)
	}
	ro := openFtruncateObserver(t, fs, ino)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	data := strings.Repeat("x", maxLandedPayloadBytes+7)
	hardlinkWrite(t, fs, ino, b, data)
	hardlinkSync(t, fs, ino, b)
	for _, id := range []uint64{a, b} {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	}
	for _, path := range []string{"/file.bin", "/alias.bin"} {
		fs.commitQueue.WaitPath(path)
		if _, pending := fs.pendingIndex.GetMeta(path); pending {
			t.Fatalf("source did not retire staging for %s", path)
		}
	}
	if got := remote(); got != data {
		t.Fatal("remote does not contain exact committed large descendant")
	}
	for _, tc := range []struct {
		name string
		fh   uint64
	}{{"readonly", ro}, {"clean_writable", observer.Fh}} {
		for _, read := range []struct {
			offset int64
			size   int
			want   string
		}{
			{0, 16, data[:16]},
			{int64(len(data) - 4), 16, data[len(data)-4:]},
			{int64(len(data) + 1), 16, ""},
		} {
			got, status, err := readDat9FSTestRange(fs, ino, tc.fh, read.offset, read.size)
			if status != gofuse.OK || err != nil || string(got) != read.want {
				t.Errorf("%s committed range at %d=%q/%v/%v, want %q", tc.name, read.offset, got, status, err, read.want)
			}
		}
	}
	fs.readCache = nil
	var changed atomic.Bool
	interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet {
			changed.Store(true)
		}
		if r.Method == http.MethodHead && changed.Load() {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.Header().Set("X-Dat9-Revision", "4")
			w.Header().Set("X-Dat9-IsDir", "false")
			return true
		}
		return false
	})
	got, status, err := readDat9FSTestRange(fs, ino, observer.Fh, 0, 16)
	if !changed.Load() || err != nil || status != gofuse.EAGAIN || len(got) != 0 {
		t.Fatalf("changed committed revision returned %q/%v/%v, range requested=%t", got, status, err, changed.Load())
	}

}

func TestWriteBackDataCommitPublishesLineageBeforeChmod(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	var observer gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &observer); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	child, _ := fs.fileHandles.Get(b)
	child.Lock()
	fs.setPendingModeLocked(child, 0644, fs.nextPendingModeGen())
	err := fs.snapshotWriteBackLocked(child, true)
	child.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	uploader := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", OnDataCommitted: fs.onWriteBackDataCommitted, OnSuccess: fs.onWriteBackUploadSuccess, SnapshotStagingGens: fs.snapshotStagingGens}
	uploader.uploadOne(child.Path)
	meta, ok := fs.writeBack.GetMeta(child.Path)
	if !ok || meta.Kind != PendingChmod || remote() != "Hello" {
		t.Fatal("missing premise: exact descendant data durable with chmod remaining")
	}
	proof := fs.commitQueue.landedCommit(child.Path)
	rev := fs.latestCommittedRevision(child.Path)
	t.Logf("data rev=%d pending_kind=%v landed_rev=%d landed_sid=%q wanted_sid=%q checksum=%q", rev, meta.Kind, proof.rev, proof.snapshotID, meta.SnapshotID, proof.checksum)
	if proof.rev != rev || proof.snapshotID != meta.SnapshotID || proof.checksum == "" {
		t.Error("successful data commit lacks its exact landed proof while chmod is pending")
	}
	got, status, readErr := readDat9FSTestRange(fs, ino, observer.Fh, 0, 11)
	t.Logf("clean alias after data commit: bytes=%q status=%v error=%v", got, status, readErr)
	if readErr != nil || status != gofuse.OK || string(got) != "Hello" {
		t.Error("clean alias cannot read already committed bytes while chmod is pending")
	}
}

func TestFtruncateAppendBusySourceAbort(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, unused, _ := newFtruncateCommitFS(t)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: unused})
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			reviewFtruncate(t, fs, ino, a, 5)
			fs.opts.RemoteCommitWaitTimeout = time.Second
			if mode == "timeout" {
				fs.opts.RemoteCommitWaitTimeout = 20 * time.Millisecond
			}
			parent, _ := fs.fileHandles.Get(a)
			target, _ := fs.fileHandles.Get(opened.Fh)
			before := target.Dirty
			parent.Lock()
			unlock := sync.OnceFunc(parent.Unlock)
			t.Cleanup(unlock)
			waiting := make(chan struct{}, 1)
			testHookBeforeAppendSourceWait = func(source *FileHandle) {
				if source == parent {
					waiting <- struct{}{}
				}
			}
			t.Cleanup(func() { testHookBeforeAppendSourceWait = nil })
			cancel := make(chan struct{})
			closeCancel := sync.OnceFunc(func() { close(cancel) })
			t.Cleanup(closeCancel)
			type result struct {
				n  uint32
				st gofuse.Status
			}
			done := make(chan result, 1)
			go func() {
				n, st := fs.Write(cancel, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}, []byte("!"))
				done <- result{n, st}
			}()
			select {
			case <-waiting:
			case got := <-done:
				t.Fatalf("did not wait for busy source: %+v", got)
			case <-time.After(3 * time.Second):
				t.Fatal("did not reach source wait")
			}
			if mode == "cancel" {
				closeCancel()
			}
			select {
			case got := <-done:
				if got.n != 0 || got.st == gofuse.OK || (mode == "timeout" && got.st != gofuse.EAGAIN) {
					t.Fatalf("busy %s acknowledged bytes or ignored abort: %+v", mode, got)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("busy append ignored cancellation/deadline")
			}
			target.Lock()
			defer target.Unlock()
			if target.Dirty != before || target.DirtySeq != 0 || target.ftruncateInherited != "" || target.RemoteCommitUnlock != nil {
				t.Fatal("aborted preflight changed the target image or retained its fence")
			}
		})
	}
}

func TestWriteBackDataCommitProofBeforeModeFailure(t *testing.T) {
	for _, queued := range []bool{true, false} {
		for _, synchronous := range []bool{false, true} {
			t.Run(fmt.Sprintf("queue=%t/sync=%t", queued, synchronous), func(t *testing.T) {
				fs, ino, a, b, remote := newHardlinkFS(t)
				reviewFtruncate(t, fs, ino, a, 5)
				hardlinkSync(t, fs, ino, a)
				hardlinkWrite(t, fs, ino, b, "H")
				child, _ := fs.fileHandles.Get(b)
				child.Lock()
				rootID := child.pendingFtruncate.Load().id
				fs.setPendingModeLocked(child, 0644, fs.nextPendingModeGen())
				err := fs.snapshotWriteBackLocked(child, true)
				child.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				meta, ok := fs.writeBack.GetMeta(child.Path)
				if !ok || !meta.lineageTrusted || meta.SnapshotID == "" {
					t.Fatal("missing actual trusted writeback snapshot")
				}
				originalQueue := fs.commitQueue
				var modes atomic.Int32
				interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method != http.MethodPost || !r.URL.Query().Has("chmod") {
						return false
					}
					modes.Add(1)
					if remote() != "Hello" {
						t.Error("chmod attempted before exact data commit")
					}
					for _, path := range []string{"/file.bin", "/alias.bin"} {
						proof := fs.landedAppendCommit(path)
						if proof.rev != 3 || proof.size != 5 || proof.snapshotID != meta.SnapshotID || proof.checksum != payloadChecksum([]byte("Hello")) || !slices.Contains(proof.ancestors, rootID) {
							t.Errorf("chmod preceded exact alias proof for %s: %+v", path, proof)
						}
					}
					w.WriteHeader(http.StatusMethodNotAllowed)
					return true
				})
				if !queued {
					fs.commitQueue = nil
					t.Cleanup(func() { fs.commitQueue = originalQueue })
				}
				uploader := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", OnDataCommitted: fs.onWriteBackDataCommitted, OnSuccess: fs.onWriteBackUploadSuccess, SnapshotStagingGens: fs.snapshotStagingGens}
				if synchronous {
					_, proof, err := uploader.uploadSyncWithAppendSnapshot(t.Context(), child.Path)
					if err == nil || proof.rev != 3 || proof.snapshotID != meta.SnapshotID || proof.checksum != payloadChecksum([]byte("Hello")) {
						t.Fatalf("sync content proof with later mode error: %+v, %v", proof, err)
					}
				} else {
					uploader.uploadOne(child.Path)
				}
				pending, ok := fs.writeBack.GetMeta(child.Path)
				if modes.Load() == 0 || !ok || pending.Kind != PendingChmod || pending.Generation <= meta.Generation || !pending.HasMode || pending.Mode != meta.Mode || pending.SnapshotID != meta.SnapshotID {
					t.Fatal("mode failure did not retain its permission obligation under a fresh generation")
				}
			})
		}
	}
}

func TestWriteBackDataCommitProofAuthority(t *testing.T) {
	for _, queued := range []bool{true, false} {
		for _, mode := range []string{"older", "replaced"} {
			t.Run(fmt.Sprintf("queue=%t/%s", queued, mode), func(t *testing.T) {
				fs, ino, a, b, _ := newHardlinkFS(t)
				if !queued {
					originalQueue := fs.commitQueue
					fs.commitQueue = nil
					t.Cleanup(func() { fs.commitQueue = originalQueue })
				}
				reviewFtruncate(t, fs, ino, a, 5)
				hardlinkSync(t, fs, ino, a)
				hardlinkWrite(t, fs, ino, b, "H")
				child, _ := fs.fileHandles.Get(b)
				child.Lock()
				if err := fs.snapshotWriteBackLocked(child, true); err != nil {
					child.Unlock()
					t.Fatal(err)
				}
				child.Unlock()
				meta, _ := fs.writeBack.GetMeta(child.Path)
				meta.liveChecksum = payloadChecksum([]byte("Hello"))
				if mode == "older" {
					hardlinkSync(t, fs, ino, b)
					hardlinkWrite(t, fs, ino, a, "J")
					hardlinkSync(t, fs, ino, a)
				} else {
					fs.inodes.RemoveLinkPreserve(child.Path)
					if replacement := fs.inodes.Lookup(child.Path, false, 5, time.Now()); replacement == ino {
						t.Fatal("replacement retained the old inode identity")
					}
				}
				before := fs.landedAppendCommit(child.Path)
				fs.onWriteBackDataCommitted(*meta, 3, StagingGens{})
				after := fs.landedAppendCommit(child.Path)
				if after.rev != before.rev || after.snapshotID != before.snapshotID || after.checksum != before.checksum {
					t.Fatalf("%s ACK replaced proof authority: before=%+v after=%+v", mode, before, after)
				}
				if mode == "replaced" && fs.ftruncateCommittedSeq(ino) < meta.MutationSeq {
					t.Fatal("rejected path proof dropped the captured inode ACK")
				}
			})
		}
	}
}

func TestWriteBackDataCommitNoQueueAliasHandoff(t *testing.T) {
	for _, mode := range []string{"staged", "legacy", "revision", "size", "checksum", "ancestry", "replaced", "unlinked", "view"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, b, remote := newHardlinkFS(t)
			originalQueue := fs.commitQueue
			fs.commitQueue = nil
			t.Cleanup(func() { fs.commitQueue = originalQueue })
			if mode == "legacy" {
				fs.shadowStore, fs.pendingIndex = nil, nil
			}
			uploader := NewWriteBackUploader(fs.client, fs.writeBack, 1, "/")
			uploader.OnDataCommitted, uploader.OnSuccess = fs.onWriteBackDataCommitted, fs.onWriteBackUploadSuccess
			uploader.SnapshotStagingGens = fs.snapshotStagingGens
			fs.SetWriteBack(fs.writeBack, uploader)
			t.Cleanup(uploader.DrainAll)
			var writer, reader gofuse.OpenOut
			for _, out := range []*gofuse.OpenOut{&writer, &reader} {
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, out); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			hardlinkWrite(t, fs, ino, b, "H")
			child, _ := fs.fileHandles.Get(b)
			child.Lock()
			fs.setPendingModeLocked(child, 0644, fs.nextPendingModeGen())
			err := fs.snapshotWriteBackLocked(child, true)
			child.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			uploader.uploadOne(child.Path)
			meta, ok := fs.writeBack.GetMeta(child.Path)
			if !ok || meta.Kind != PendingChmod || remote() != "Hello" {
				t.Fatal("missing successful data ACK with a separate failed permission update")
			}
			proof := fs.landedAppendCommit(child.Path)
			if proof.rev != 3 || proof.snapshotID == "" || proof.checksum != payloadChecksum([]byte("Hello")) {
				t.Fatalf("missing actual No-CQ landed proof: %+v", proof)
			}

			if mode != "staged" && mode != "legacy" {
				switch mode {
				case "replaced", "unlinked":
					target, _ := fs.fileHandles.Get(writer.Fh)
					fs.inodes.RemoveLinkPreserve(target.Path)
					if mode == "replaced" {
						fs.inodes.Lookup(target.Path, false, 5, time.Now())
					}
				case "view":
					fs.mountViewGeneration.Add(1)
				default:
					fs.committedMu.Lock()
					for path, bad := range fs.committedAppend {
						switch mode {
						case "revision":
							bad.rev--
						case "size":
							bad.size--
						case "checksum":
							bad.checksum = payloadChecksum([]byte("other"))
						case "ancestry":
							bad.snapshotID, bad.ancestors = "unrelated", nil
						}
						fs.committedAppend[path] = bad
					}
					fs.committedMu.Unlock()
				}
				got, st, err := readDat9FSTestRange(fs, ino, reader.Fh, 0, 11)
				if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
					t.Fatalf("%s proof leaked read: %q/%v/%v", mode, got, st, err)
				}
				target, _ := fs.fileHandles.Get(writer.Fh)
				before, seq := target.Dirty, target.DirtySeq
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer.Fh}, []byte("J")); n != 0 || st != gofuse.EAGAIN {
					t.Fatalf("%s proof accepted write: %d/%v", mode, n, st)
				}
				if target.Dirty != before || target.DirtySeq != seq || remote() != "Hello" {
					t.Fatal("refused proof changed the accepted image")
				}
				return
			}
			got, status, readErr := readDat9FSTestRange(fs, ino, reader.Fh, 0, 11)
			if readErr != nil || status != gofuse.OK || string(got) != "Hello" {
				t.Errorf("No-CQ clean alias did not consume the durable proof: %q/%v/%v", got, status, readErr)
			}
			for _, id := range []uint64{a, b} {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			if remote() != "Hello" {
				t.Fatal("retiring the old producers overwrote committed bytes")
			}
			if pending, ok := fs.writeBack.GetMeta(child.Path); !ok || pending.Kind != PendingChmod || !pending.HasMode || pending.Mode != 0644 {
				t.Fatal("producer retirement lost independent chmod retry")
			}
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer.Fh}, []byte("J")); n != 1 || st != gofuse.OK {
				t.Fatalf("No-CQ untouched alias did not adopt the committed baseline: %d/%v", n, st)
			}
			got, status, readErr = readDat9FSTestRange(fs, ino, reader.Fh, 0, 11)
			if readErr != nil || status != gofuse.OK || string(got) != "Jello" {
				t.Fatalf("accepted No-CQ alias write is not visible: %q/%v/%v", got, status, readErr)
			}
		})
	}
}

func TestFtruncateAppendWaitsForLateBusySource(t *testing.T) {
	fs, ino, a, unused, _ := newFtruncateCommitFS(t)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: unused})
	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); st != gofuse.OK {
		t.Fatal(st)
	}
	parent, _ := fs.fileHandles.Get(a)
	target, _ := fs.fileHandles.Get(opened.Fh)
	var armed, locked atomic.Bool
	held := make(chan struct{})
	interceptFtruncateHTTP(t, fs, func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodHead && armed.Load() && locked.CompareAndSwap(false, true) {
			parent.Lock()
			close(held)
		}
		return false
	})
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	target.Lock()
	t.Logf("before append BaseRev=%d SID=%q inherited=%q eventRev=%d", target.BaseRev, target.ContentSnapshotID, target.ftruncateInherited, target.pendingFtruncate.Load().revision)
	target.Unlock()
	fs.opts.RemoteCommitWaitTimeout = time.Second
	var once sync.Once
	release := func() {
		select {
		case <-held:
			once.Do(parent.Unlock)
		default:
		}
	}
	waiting := make(chan struct{}, 1)
	testHookBeforeAppendSourceWait = func(source *FileHandle) {
		if source == parent {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	}
	t.Cleanup(func() { testHookBeforeAppendSourceWait = nil })
	type result struct {
		n  uint32
		st gofuse.Status
	}
	done := make(chan result, 1)
	stopped := make(chan struct{})
	armed.Store(true)
	go func() {
		defer close(stopped)
		n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}, []byte("!"))
		done <- result{n, st}
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("writer did not exit")
		}
	})
	var got result
	select {
	case <-waiting:
		release()
		select {
		case got = <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("writer did not finish after late source unlock")
		}
	case got = <-done:
		release()
		t.Log("returned without waiting for late source")
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("no result or late-source wait")
	}
	if !locked.Load() {
		t.Fatal("actual post-preflight remote observation was not reached")
	}
	if got.n != 1 || got.st != gofuse.OK {
		t.Fatalf("ordinary source contention after bounds returned %d/%v, want1/OK", got.n, got.st)
	}
}
