package fuse

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// This boundary regression supplements the unchanged four-writer/48-record
// regression and mounted acceptance matrix. It does not replace either one.
func TestIssue1023HardlinkBusySourceFenceHandoff(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, interactive)
			fs.opts.RemoteCommitWaitTimeout = time.Second
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			source, _ := fs.fileHandles.Get(ids[1])
			target, _ := fs.fileHandles.Get(opened.Fh)
			if source.Path != target.Path {
				t.Fatal("source and target must contend on the same alias fence")
			}
			issue1023ShadowAppend(t, fs, ino, ids[1], "A")

			sourceHeld := make(chan struct{})
			sourceOwnsFence := make(chan struct{})
			finishSource := make(chan struct{})
			releaseSource := sync.OnceFunc(func() { close(finishSource) })
			sourceStopped := make(chan struct{})
			writerStopped := make(chan struct{})
			waitObserved := make(chan struct{}, 1)
			prematureRefresh := make(chan struct{}, 1)
			sourceResult := make(chan gofuse.Status, 1)
			type result struct {
				n  uint32
				st gofuse.Status
			}
			writeResult := make(chan result, 1)
			var started atomic.Bool
			var sourceReleased atomic.Bool

			testHookBeforeAppendSourceRefresh = func(handle *FileHandle) {
				if handle != target {
					return
				}
				if started.CompareAndSwap(false, true) {
					// Model the exact fsync entrance: source holds its handle and
					// waits for the path reservation currently owned by target.
					go func() {
						defer close(sourceStopped)
						source.Lock()
						close(sourceHeld)
						fs.lockHandleRemoteCommitPathLocked(source)
						close(sourceOwnsFence)
						<-finishSource
						sourceReleased.Store(true)
						source.Unlock()
						// The actual Fsync borrows the reserved path fence. Its normal
						// strict/interactive tail must commit and release that fence.
						sourceResult <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
					}()
					<-sourceHeld
					return
				}
				if !sourceReleased.Load() {
					select {
					case prematureRefresh <- struct{}{}:
					default:
					}
				}
			}
			testHookBeforeAppendSourceWait = func(handle *FileHandle) {
				if handle != source {
					t.Error("append waited on a different source")
					return
				}
				if !target.TryLock() {
					t.Error("append retained target handle while waiting for source")
				} else {
					if target.RemoteCommitUnlock != nil {
						t.Error("append retained target path reservation while waiting for source")
					}
					target.Unlock()
				}
				select {
				case waitObserved <- struct{}{}:
				default:
				}
			}
			t.Cleanup(func() {
				releaseSource()
				for _, stopped := range []chan struct{}{writerStopped, sourceStopped} {
					select {
					case <-stopped:
					case <-time.After(3 * time.Second):
						t.Error("handoff goroutine did not exit before cleanup")
					}
				}
				testHookBeforeAppendSourceWait = nil
				testHookBeforeAppendSourceRefresh = nil
			})
			go func() {
				defer close(writerStopped)
				n, st := pr939Append(fs, ino, opened.Fh, "B")
				writeResult <- result{n, st}
			}()

			select {
			case <-waitObserved:
			case <-prematureRefresh:
				t.Fatal("append reacquired the fence before the contended source handoff")
			case <-writerStopped:
				t.Fatal("append exited without releasing and waiting for its busy source")
			case <-time.After(2 * time.Second):
				t.Fatal("busy source handoff was not observed")
			}
			select {
			case <-sourceOwnsFence:
			case <-time.After(2 * time.Second):
				t.Fatal("source could not acquire the yielded path fence")
			}
			releaseSource()
			select {
			case st := <-sourceResult:
				if st != gofuse.OK {
					t.Errorf("source fsync=%v", st)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("source fsync did not finish")
			}
			select {
			case got := <-writeResult:
				if got.n != 1 || got.st != gofuse.OK {
					t.Errorf("append=%d/%v, want 1/OK", got.n, got.st)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("append did not finish after source fsync")
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}); st != gofuse.OK {
				t.Errorf("target fsync=%v", st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, body, _ := server.snapshot(); string(body) != "baseAB" {
				t.Errorf("successful append records=%q, want baseAB", body)
			}
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Errorf("drain conflicts=%d", conflicts)
			}
		})
	}
}

func TestIssue1023BusySourceWaitDeadlineAndContext(t *testing.T) {
	t.Run("positive_deadline", func(t *testing.T) {
		source := &FileHandle{}
		source.Lock()
		defer source.Unlock()
		if err := waitAppendRefreshSource(t.Context(), &FileHandle{}, source, time.Now().Add(-time.Second)); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("expired positive deadline=%v, want EAGAIN", err)
		}
	})
	for _, timeout := range []time.Duration{0, -1} {
		t.Run(fmt.Sprintf("nonpositive=%s/cancel", timeout), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := &FileHandle{}
			source.Lock()
			defer source.Unlock()
			observed := make(chan struct{})
			testHookBeforeAppendSourceWait = func(handle *FileHandle) {
				if handle == source {
					close(observed)
				}
			}
			done := make(chan error, 1)
			stopped := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case <-stopped:
				case <-time.After(time.Second):
					t.Error("source wait did not exit before cleanup")
				}
				testHookBeforeAppendSourceWait = nil
			})
			go func() {
				defer close(stopped)
				done <- waitAppendRefreshSource(ctx, &FileHandle{}, source, time.Time{})
			}()
			<-observed
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("unbounded wait cancellation=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("nonpositive timeout wait ignored cancellation")
			}
		})
	}
}

func issue1023WaitForActualFsyncFence(t *testing.T, fs *Dat9FS) {
	t.Helper()
	stack := make([]byte, 1<<20)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, fmt.Sprintf("(*Dat9FS).Fsync(%p", fs)) &&
				(strings.Contains(goroutine, "(*Dat9FS).lockRemoteCommitPath(") ||
					strings.Contains(goroutine, "(*Dat9FS).lockRemoteCommitPathTimeout(")) {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("source Fsync did not hold its handle while waiting for the actual Flush reservation")
}

// The source is already contended when a concurrent same-FH Flush obtains a
// new reservation. The waiting append must resume admission and cooperate with
// that reservation, rather than wait only for a source mutex it now blocks.
func TestIssue1023BusySourceWaitObservesConcurrentFlushReservation(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, timeout := range []time.Duration{time.Second, 0, -1} {
			t.Run(fmt.Sprintf("interactive=%t/timeout=%s", interactive, timeout), func(t *testing.T) {
				const path = "/review-new-reservation.txt"
				fs, ino, server, _ := reviewR2FS(t, path, interactive)
				fs.opts.RemoteCommitWaitTimeout = timeout
				cache, err := NewWriteBackCache(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				fs.writeBack = cache
				target, targetID := pr939Handle(t, fs, ino, path, "base", false)
				source, sourceID := pr939Handle(t, fs, ino, path, "base", false)
				issue1023ShadowAppend(t, fs, ino, targetID, "A")
				issue1023ShadowAppend(t, fs, ino, sourceID, "B")

				source.Lock()
				unlockSource := sync.OnceFunc(source.Unlock)
				waitEntered := make(chan struct{})
				resumeWait := make(chan struct{})
				resume := sync.OnceFunc(func() { close(resumeWait) })
				var firstWait sync.Once
				testHookBeforeAppendSourceWait = func(handle *FileHandle) {
					if handle == source {
						firstWait.Do(func() {
							close(waitEntered)
							<-resumeWait
						})
					}
				}
				cancel := make(chan struct{})
				cancelWrite := sync.OnceFunc(func() { close(cancel) })
				type result struct {
					n  uint32
					st gofuse.Status
				}
				writeResult := make(chan result, 1)
				writerStopped := make(chan struct{})
				sourceResult := make(chan gofuse.Status, 1)
				sourceStopped := make(chan struct{})
				sourceStarted := false
				t.Cleanup(func() {
					cancelWrite()
					resume()
					unlockSource()
					// Explicit test cleanup releases the real Flush reservation if
					// the pre-fix wait cannot observe it. The production wait must
					// make progress before this cleanup is needed.
					target.Lock()
					fs.releaseHandleRemoteCommitPathLocked(target)
					target.Unlock()
					stopped := []chan struct{}{writerStopped}
					if sourceStarted {
						stopped = append(stopped, sourceStopped)
					}
					for _, done := range stopped {
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Error("reservation regression goroutine did not exit")
						}
					}
					testHookBeforeAppendSourceWait = nil
				})
				go func() {
					defer close(writerStopped)
					n, st := fs.Write(cancel, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID}, []byte("C"))
					writeResult <- result{n, st}
				}()
				select {
				case <-waitEntered:
				case <-time.After(2 * time.Second):
					t.Fatal("append did not reach the typed busy-source wait")
				}

				// This reservation is created AFTER the append released its
				// original fence and target mutex to wait for the locked source.
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID}); st != gofuse.OK {
					t.Fatalf("concurrent same-FH Flush=%v", st)
				}
				target.Lock()
				reserved := target.RemoteCommitUnlock != nil
				target.Unlock()
				if !reserved {
					t.Fatal("actual Flush did not retain a new reservation")
				}
				unlockSource()
				sourceStarted = true
				go func() {
					defer close(sourceStopped)
					sourceResult <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID})
				}()
				issue1023WaitForActualFsyncFence(t, fs)
				resume()

				select {
				case got := <-writeResult:
					if got.n != 1 || got.st != gofuse.OK {
						t.Errorf("append=%d/%v, want 1/OK", got.n, got.st)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("append stayed on source mutex despite target's new Flush reservation")
				}
				select {
				case st := <-sourceResult:
					if st != gofuse.OK {
						t.Errorf("source Fsync=%v", st)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("source Fsync remained behind target's new Flush reservation")
				}
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID}); st != gofuse.OK {
					t.Errorf("target Fsync=%v", st)
				}
				waitCommitQueueIdle(t, fs.commitQueue)
				if _, body, _ := server.snapshot(); string(body) != "baseABC" {
					t.Errorf("acknowledged records=%q, want baseABC", body)
				}
				if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
					t.Errorf("drain conflicts=%d", conflicts)
				}
			})
		}
	}
}

// Cancellation is a failure exit, so it must not clear a reservation which a
// concurrent Flush created after the writer yielded its original ownership.
func TestIssue1023BusySourceWaitCancelKeepsConcurrentFlushReservation(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			const path = "/review-new-reservation-cancel.txt"
			fs, ino, _, _ := reviewR2FS(t, path, interactive)
			fs.opts.RemoteCommitWaitTimeout = -1
			cache, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.writeBack = cache
			target, targetID := pr939Handle(t, fs, ino, path, "base", false)
			source, sourceID := pr939Handle(t, fs, ino, path, "base", false)
			issue1023ShadowAppend(t, fs, ino, targetID, "A")
			issue1023ShadowAppend(t, fs, ino, sourceID, "B")
			source.Lock()
			unlockSource := sync.OnceFunc(source.Unlock)
			waitEntered := make(chan struct{})
			resumeWait := make(chan struct{})
			resume := sync.OnceFunc(func() { close(resumeWait) })
			var firstWait sync.Once
			testHookBeforeAppendSourceWait = func(handle *FileHandle) {
				if handle == source {
					firstWait.Do(func() {
						close(waitEntered)
						<-resumeWait
					})
				}
			}
			cancel := make(chan struct{})
			cancelWrite := sync.OnceFunc(func() { close(cancel) })
			type result struct {
				n  uint32
				st gofuse.Status
			}
			writeResult := make(chan result, 1)
			writerStopped := make(chan struct{})
			t.Cleanup(func() {
				cancelWrite()
				resume()
				unlockSource()
				select {
				case <-writerStopped:
				case <-time.After(3 * time.Second):
					t.Error("cancelled writer did not exit")
				}
				target.Lock()
				fs.releaseHandleRemoteCommitPathLocked(target)
				target.Unlock()
				testHookBeforeAppendSourceWait = nil
			})
			go func() {
				defer close(writerStopped)
				n, st := fs.Write(cancel, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID}, []byte("C"))
				writeResult <- result{n, st}
			}()
			select {
			case <-waitEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("append did not enter source wait")
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID}); st != gofuse.OK {
				t.Fatal(st)
			}
			target.Lock()
			reserved := target.RemoteCommitUnlock != nil
			buffer, seq, version := target.Dirty, target.DirtySeq, target.Dirty.contentVersion
			writeBackSeq, shadowGen, pendingGen := target.WriteBackSeq, target.ShadowStageGen, target.PendingIndexGen
			before := string(target.Dirty.Bytes())
			target.Unlock()
			if !reserved {
				t.Fatal("actual Flush reservation missing")
			}

			// Wait for the existing FUSE cancellation bridge to run cf(), rather
			// than assuming that closing its channel synchronously cancels ctx.
			cancelKey := (<-chan struct{})(cancel)
			if _, ok := fuseInterruptFlags.Load(cancelKey); !ok {
				t.Fatal("writer cancellation observer was not registered")
			}
			cancelWrite()
			deadline := time.Now().Add(time.Second)
			for {
				if _, ok := fuseInterruptFlags.Load(cancelKey); !ok {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("FUSE cancellation bridge did not cancel the waiting context")
				}
				runtime.Gosched()
			}
			resume()
			select {
			case got := <-writeResult:
				if got.n != 0 || got.st != gofuse.Status(syscall.EAGAIN) {
					t.Errorf("cancelled append=%d/%v, want zero/EAGAIN", got.n, got.st)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled source wait did not return")
			}
			target.Lock()
			preserved := target.RemoteCommitUnlock != nil && target.Dirty == buffer && target.DirtySeq == seq &&
				target.Dirty.contentVersion == version && string(target.Dirty.Bytes()) == before &&
				target.WriteBackSeq == writeBackSeq && target.ShadowStageGen == shadowGen && target.PendingIndexGen == pendingGen
			target.Unlock()
			if !preserved {
				t.Error("cancellation cleared concurrent Flush ownership or changed its staged data")
			}
			if unlock, acquired := fs.tryLockRemoteCommitPath(path); acquired {
				unlock()
				t.Error("concurrent Flush path reservation was released on writer cancellation")
			}
		})
	}
}

// A completed interactive commit can miss both handle-refresh callbacks while
// the source is locked. Its clean retired shadow claim must not block a peer's
// next acknowledged append. This supplements the original mounted matrix.
func TestIssue1023InteractiveRetiredSourceAfterCommitCallbacksMiss(t *testing.T) {
	const path = "/review-retired-source-after-commit.txt"
	fs, ino, server, _ := reviewR2FS(t, path, true)
	source, sourceID := pr939Handle(t, fs, ino, path, "base", false)
	peer, peerID := pr939Handle(t, fs, ino, path, "base", false)

	server.firstPutStarted = make(chan struct{})
	server.releaseFirstPut = make(chan struct{})
	releasePut := sync.OnceFunc(func() { close(server.releaseFirstPut) })
	sourceLocked := false
	fsyncStopped := make(chan struct{})
	fsyncResult := make(chan gofuse.Status, 1)
	t.Cleanup(func() {
		releasePut()
		if sourceLocked {
			source.Unlock()
			sourceLocked = false
		}
		select {
		case <-fsyncStopped:
		case <-time.After(3 * time.Second):
			t.Error("source Fsync did not exit before cleanup")
		}
	})

	successDone := make(chan struct{})
	cleanupDone := make(chan struct{})
	var successOnce, cleanupOnce, successSignal, cleanupSignal sync.Once
	onSuccess := fs.commitQueue.OnSuccess
	onCleanup := fs.commitQueue.OnCleanup
	fs.commitQueue.OnSuccess = func(entry *CommitEntry, revision int64) {
		successOnce.Do(func() {
			if source.TryLock() {
				source.Unlock()
				t.Error("source was not locked across the success refresh callback")
			}
		})
		onSuccess(entry, revision)
		successSignal.Do(func() { close(successDone) })
	}
	fs.commitQueue.OnCleanup = func(entry *CommitEntry) {
		cleanupOnce.Do(func() {
			if source.TryLock() {
				source.Unlock()
				t.Error("source was not locked across the cleanup refresh callback")
			}
		})
		onCleanup(entry)
		cleanupSignal.Do(func() { close(cleanupDone) })
	}

	if n, status := pr939Append(fs, ino, sourceID, "A"); n != 1 || status != gofuse.OK {
		t.Fatalf("source append=%d/%v, want 1/OK", n, status)
	}
	go func() {
		defer close(fsyncStopped)
		fsyncResult <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID})
	}()
	select {
	case status := <-fsyncResult:
		if status != gofuse.OK {
			t.Fatalf("interactive source Fsync=%v", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interactive Fsync waited for the gated remote PUT")
	}
	select {
	case <-server.firstPutStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("queued source PUT did not reach the gate")
	}

	// The PUT cannot finish before this lock is held. Both actual callbacks
	// therefore miss this source through their production TryLock checks.
	source.Lock()
	sourceLocked = true
	if source.DirtySeq != 0 || source.Dirty.HasDirtyParts() || !source.ShadowReady || source.ShadowStageGen != 0 {
		t.Fatal("interactive enqueue did not transfer the dirty shadow generation")
	}
	releasePut()
	for _, done := range []chan struct{}{successDone, cleanupDone} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("commit callbacks did not complete while the source stayed locked")
		}
	}
	// HasPath covers post-cleanup in-flight state as well as queued entries.
	deadline := time.Now().Add(3 * time.Second)
	for fs.commitQueue.HasPath(path) {
		if time.Now().After(deadline) {
			t.Fatal("source commit did not drain after its callbacks")
		}
		runtime.Gosched()
	}
	if source.DirtySeq != 0 || source.Dirty.HasDirtyParts() || !source.ShadowReady || source.ShadowStageGen != 0 {
		t.Fatal("callback-miss clean retired source premise was not preserved")
	}
	if fs.shadowStore.Has(path) || fs.hasPendingLocalState(path) {
		t.Fatal("source shadow or pending staging survived successful queue cleanup")
	}
	if _, present := fs.pendingIndex.GetMeta(path); present {
		t.Fatal("source pending metadata survived successful queue cleanup")
	}
	revision, body, _ := server.snapshot()
	proof := fs.commitQueue.landedCommit(path)
	if string(body) != "baseA" || proof.rev != revision || proof.snapshotID == "" || proof.snapshotID != source.ContentSnapshotID {
		t.Fatalf("source landed body/proof=%q/%+v, revision=%d, source ID=%q", body, proof, revision, source.ContentSnapshotID)
	}
	source.Unlock()
	sourceLocked = false

	if n, status := pr939Append(fs, ino, peerID, "B"); n != 1 || status != gofuse.OK {
		t.Fatalf("peer append after retired source=%d/%v, want 1/OK", n, status)
	}
	if got := pr939HandleBytes(peer); got != "baseAB" {
		t.Fatalf("acknowledged peer image=%q, want baseAB", got)
	}
	source.Lock()
	retired := !source.ShadowReady && source.ShadowStageGen == 0 && source.DirtySeq == 0 && source.BaseRev == revision
	source.Unlock()
	if !retired {
		t.Error("peer refresh did not lazily retire the clean source shadow claim")
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: peerID}); status != gofuse.OK {
		t.Fatalf("peer Fsync=%v", status)
	}
	waitCommitQueueIdle(t, fs.commitQueue)
	for _, id := range []uint64{peerID, sourceID} {
		if status := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); status != gofuse.OK {
			t.Errorf("close Flush fh=%d status=%v", id, status)
		}
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	}
	fs.commitQueue.DrainAll()
	if _, body, _ := server.snapshot(); string(body) != "baseAB" {
		t.Errorf("remote records after drain=%q, want baseAB", body)
	}
	if fs.commitQueue.HasPath(path) {
		t.Error("source path remains busy after final drain")
	}
	if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
		t.Errorf("drain conflicts=%d", conflicts)
	}
}
