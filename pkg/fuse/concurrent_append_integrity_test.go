package fuse

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Separate handles remain open across another appender's fsync. Serializing
// these handler calls makes this particular legal interleaving deterministic;
// the mounted four-process workload remains a separate acceptance test.
func TestIssue1023AppendAfterSiblingFsync(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, nextWriter := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("interactive=%t/next=%d", interactive, nextWriter), func(t *testing.T) {
				const path = "/append-integrity.txt"
				server, ts := newCASFileServer(t, path, 1, nil)
				defer ts.Close()
				fs, ino := pr939HandleFS(t, path, "")
				if interactive {
					fs.syncMode = SyncInteractive
				}
				fs.client = newTestClient(ts.URL)
				fs.client.SetSmallFileThresholdForTests(1 << 20)
				cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
				defer cq.DrainAll()
				fs.commitQueue = cq
				cq.PathLock = fs.lockRemoteCommitPath
				cq.DurableWatermark = fs.latestCommittedRevision
				cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
				cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
				cq.IsSuperseded = fs.commitEntrySuperseded
				handles := make([]*FileHandle, 2)
				ids := make([]uint64, 2)
				for i := range ids {
					handles[i], ids[i] = pr939Handle(t, fs, ino, path, "", false)
				}
				write := func(i int, record string) {
					t.Helper()
					if n, st := pr939Append(fs, ino, ids[i], record); st != gofuse.OK || int(n) != len(record) {
						t.Fatalf("write %d: n=%d status=%v", i, n, st)
					}
				}
				syncHandle := func(i int) {
					t.Helper()
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[i]}); st != gofuse.OK {
						t.Fatalf("fsync %d: %v", i, st)
					}
					waitCommitQueueIdle(t, cq)
				}
				write(0, "A\n")
				write(1, "B\n")
				syncHandle(1)
				if _, body, _ := server.snapshot(); string(body) != "A\nB\n" {
					t.Fatalf("before third write: %q", body)
				}
				siblingClosed := nextWriter == 2
				if siblingClosed {
					// The committing sibling has closed: the old writer must
					// still recover the complete committed image.
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
					nextWriter = 0
					if _, ok := fs.fileHandles.Get(ids[1]); ok {
						t.Fatal("committing sibling remained open")
					}
				}
				write(nextWriter, "C\n")
				if got := pr939HandleBytes(handles[nextWriter]); got != "A\nB\nC\n" {
					t.Fatalf("acknowledged append image=%q, want all three records", got)
				}
				syncHandle(nextWriter)
				if !siblingClosed {
					syncHandle(1 - nextWriter)
				}
				if _, body, _ := server.snapshot(); string(body) != "A\nB\nC\n" {
					t.Fatalf("remote=%q, want all three records", body)
				}
				if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
					t.Fatalf("conflicts=%d", conflicts)
				}
			})
		}
	}
}

func TestIssue1023ConcurrentAppendRecords(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			const path = "/concurrent-append.txt"
			server, ts := newCASFileServer(t, path, 1, nil)
			defer ts.Close()
			fs, ino := pr939HandleFS(t, path, "")
			if interactive {
				fs.syncMode = SyncInteractive
			}
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 4, 32)
			defer cq.DrainAll()
			fs.commitQueue = cq
			cq.PathLock = fs.lockRemoteCommitPath
			cq.DurableWatermark = fs.latestCommittedRevision
			cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
			cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
			cq.IsSuperseded = fs.commitEntrySuperseded
			var wg sync.WaitGroup
			start := make(chan struct{})
			outcomes := make(chan error, 4)
			for writer := 0; writer < 4; writer++ {
				_, id := pr939Handle(t, fs, ino, path, "", false)
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					var firstErr error
					for record := 0; record < 12; record++ {
						line := fmt.Sprintf("w%02d-%03d\n", writer, record)
						if n, st := pr939Append(fs, ino, id, line); st != gofuse.OK || int(n) != len(line) {
							if firstErr == nil {
								firstErr = fmt.Errorf("writer %d record %d: n=%d status=%v", writer, record, n, st)
							}
						}
						runtime.Gosched()
					}
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK && firstErr == nil {
						firstErr = fmt.Errorf("writer %d fsync: %v", writer, st)
					}
					if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK && firstErr == nil {
						firstErr = fmt.Errorf("writer %d flush: %v", writer, st)
					}
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
					outcomes <- firstErr
				}()
			}
			close(start)
			wg.Wait()
			close(outcomes)
			for err := range outcomes {
				if err != nil {
					t.Error(err)
				}
			}
			waitCommitQueueIdle(t, cq)
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Errorf("conflicts=%d", conflicts)
			}
			_, body, _ := server.snapshot()
			counts := make(map[string]int)
			for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
				counts[line]++
			}
			if len(body) != 384 {
				t.Errorf("remote size=%d want=384", len(body))
			}
			for writer := 0; writer < 4; writer++ {
				for record := 0; record < 12; record++ {
					line := fmt.Sprintf("w%02d-%03d", writer, record)
					if counts[line] != 1 {
						t.Errorf("record %s count=%d want=1", line, counts[line])
					}
				}
			}
		})
	}
}

func TestIssue1023AppendRefreshPreservesStateOnRemoteChange(t *testing.T) {
	for _, scenario := range []struct{ clean, unavailable bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("clean=%t/unavailable=%t", scenario.clean, scenario.unavailable), func(t *testing.T) {
			const path = "/append-external-change.txt"
			server, ts := newCASFileServer(t, path, 1, nil)
			defer ts.Close()
			fs, ino := pr939HandleFS(t, path, "")
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
			defer cq.DrainAll()
			fs.commitQueue = cq
			cq.PathLock = fs.lockRemoteCommitPath
			cq.DurableWatermark = fs.latestCommittedRevision
			cq.OnSuccess = fs.onCommitQueueSuccess
			old, a := pr939Handle(t, fs, ino, path, "", false)
			_, b := pr939Handle(t, fs, ino, path, "", false)
			clean, cleanID := pr939Handle(t, fs, ino, path, "", false)
			for i, id := range []uint64{a, b} {
				if n, st := pr939Append(fs, ino, id, string(rune('A'+i))); st != gofuse.OK || n != 1 {
					t.Fatalf("write %d: %d %v", i, n, st)
				}
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
				t.Fatal(st)
			}
			if scenario.clean {
				old, a = clean, cleanID
				old.Lock()
				materialized := fs.materializeFullForUploadLocked(old)
				old.Unlock()
				if !materialized {
					t.Fatal("materialize clean baseline")
				}
			}
			originalData := pr939HandleBytes(old)
			originalSeq := old.DirtySeq
			originalDirty := old.Dirty.HasDirtyParts()
			if scenario.unavailable {
				ts.Close()
			} else {
				server.mu.Lock()
				server.revision++
				server.body = []byte("external")
				server.mu.Unlock()
			}
			revision, body, puts := server.snapshot()
			if n, st := pr939Append(fs, ino, a, "C"); st == gofuse.OK || n != 0 {
				t.Fatalf("write on changed/unavailable remote: n=%d status=%v", n, st)
			}
			if got := pr939HandleBytes(old); got != originalData || old.DirtySeq != originalSeq || old.Dirty.HasDirtyParts() != originalDirty {
				t.Fatalf("dirty state changed: data=%q seq=%d want=%d", got, old.DirtySeq, originalSeq)
			}
			if rev, got, after := server.snapshot(); rev != revision || string(got) != string(body) || len(after) != len(puts) {
				t.Fatalf("remote mutated: rev=%d body=%q puts=%d", rev, got, len(after))
			}
		})
	}
}

// A third appender opened before the sibling commit has no prior dirty snapshot.
func TestIssue1023CleanAppenderAfterSiblingFsync(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			const path = "/append-clean-handoff.txt"
			server, ts := newCASFileServer(t, path, 1, nil)
			defer ts.Close()
			fs, ino := pr939HandleFS(t, path, "")
			if interactive {
				fs.syncMode = SyncInteractive
			}
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
			defer cq.DrainAll()
			fs.commitQueue = cq
			cq.PathLock = fs.lockRemoteCommitPath
			cq.DurableWatermark = fs.latestCommittedRevision
			cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
			cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
			cq.IsSuperseded = fs.commitEntrySuperseded
			handles := make([]*FileHandle, 3)
			ids := make([]uint64, 3)
			for i := range ids {
				handles[i], ids[i] = pr939Handle(t, fs, ino, path, "", false)
			}
			for i, record := range []string{"A\n", "B\n"} {
				if n, st := pr939Append(fs, ino, ids[i], record); st != gofuse.OK || int(n) != len(record) {
					t.Fatalf("write %d: n=%d status=%v", i, n, st)
				}
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, cq)
			if _, body, _ := server.snapshot(); string(body) != "A\nB\n" {
				t.Fatalf("before clean append: %q", body)
			}
			t.Logf("clean appender: rev=%d content=%q seq=%d", handles[2].BaseRev, handles[2].ContentSnapshotID, handles[2].DirtySeq)
			if n, st := pr939Append(fs, ino, ids[2], "C\n"); st != gofuse.OK || n != 2 {
				t.Fatalf("third append: n=%d status=%v", n, st)
			}
			if got := pr939HandleBytes(handles[2]); got != "A\nB\nC\n" {
				t.Fatalf("acknowledged append image=%q, want all three records", got)
			}
			for _, id := range []uint64{ids[2], ids[0]} {
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
					t.Fatal(st)
				}
				waitCommitQueueIdle(t, cq)
			}
			if _, body, _ := server.snapshot(); string(body) != "A\nB\nC\n" {
				t.Fatalf("remote=%q", body)
			}
		})
	}
}

// Observe the blocked Write rather than guessing when it released its handle.
// The path fence is held by the test throughout this observation.
func issue1023LockWaitingAppender(t *testing.T, fh *FileHandle) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	stack := make([]byte, 1<<20)
	for {
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			waiting := strings.Contains(goroutine, "(*Dat9FS).Write(") &&
				strings.Contains(goroutine, "[select]")
			if waiting && fh.TryLock() {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("append did not yield its handle while waiting for the path fence")
		}
		runtime.Gosched()
	}
}

func TestIssue1023AppendRechecksLayerStateAfterFenceWait(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflict=%t", conflict), func(t *testing.T) {
			const path = "/append-layer-fence.txt"
			fs, ino := pr939HandleFS(t, path, "A")
			fs.opts.LayerRef = "layer-1"
			fh, id := pr939Handle(t, fs, ino, path, "A", conflict)
			seq := fh.DirtySeq
			unlockPath := sync.OnceFunc(fs.lockRemoteCommitPath(path))
			t.Cleanup(unlockPath)
			type outcome struct {
				n  uint32
				st gofuse.Status
			}
			done := make(chan outcome, 1)
			go func() { n, st := pr939Append(fs, ino, id, "B"); done <- outcome{n, st} }()
			issue1023LockWaitingAppender(t, fh)
			want := gofuse.ENOENT
			if conflict {
				tip := client.FSLayerEntry{LayerID: "layer-1", Path: path, Op: "upsert", Kind: "file", EntrySeq: 1}
				preserved, err := fs.preserveLayerReplayConflict(path, tip, []*FileHandle{fh}, fs.pendingIndex)
				if !preserved || err != nil {
					fh.Unlock()
					t.Fatalf("publish conflict: preserved=%t err=%v", preserved, err)
				}
				want = gofuse.Status(syscall.EAGAIN)
			} else {
				fs.markLayerWhiteout(path)
			}
			fh.Unlock()
			unlockPath()
			select {
			case got := <-done:
				if got.n != 0 || got.st != want {
					t.Errorf("append after replay: n=%d status=%v want=%v", got.n, got.st, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("append did not finish")
			}
			if got := pr939HandleBytes(fh); got != "A" || fh.DirtySeq != seq {
				t.Errorf("rejected append mutated data=%q seq=%d want=%d", got, fh.DirtySeq, seq)
			}
			if conflict {
				meta, _ := fs.pendingIndex.GetMeta(path)
				data, err := fs.shadowStore.ReadAll(path)
				if meta == nil || meta.Kind != PendingConflict || err != nil || string(data) != "A" {
					t.Errorf("conflict evidence changed: meta=%+v data=%q err=%v", meta, data, err)
				}
			}
		})
	}
}

func TestIssue1023AppendAdoptsConcurrentFlushReservation(t *testing.T) {
	const path = "/append-reservation.txt"
	fs, ino := pr939HandleFS(t, path, "A")
	fs.opts.RemoteCommitWaitTimeout = -1
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = cache
	fh, id := pr939Handle(t, fs, ino, path, "A", true)
	unlockPath := sync.OnceFunc(fs.lockRemoteCommitPath(path))
	t.Cleanup(unlockPath)
	type outcome struct {
		n  uint32
		st gofuse.Status
	}
	done := make(chan outcome, 1)
	go func() { n, st := pr939Append(fs, ino, id, "B"); done <- outcome{n, st} }()
	issue1023LockWaitingAppender(t, fh)
	// Transfer the fence to this handle, as a dup-FD Flush that wins the
	// unlocked window does. Run the real Flush tail that retains it.
	fh.RemoteCommitUnlock = unlockPath
	fh.Unlock()
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
		t.Fatal(st)
	}
	select {
	case got := <-done:
		if got.n != 1 || got.st != gofuse.OK {
			t.Fatalf("append: n=%d status=%v", got.n, got.st)
		}
	case <-time.After(3 * time.Second):
		unlockPath()
		<-done
		t.Fatal("append waited for its own concurrent Flush reservation")
	}
	if got := pr939HandleBytes(fh); got != "AB" {
		t.Fatalf("append bytes=%q", got)
	}
	fh.Lock()
	fs.releaseHandleRemoteCommitPathLocked(fh)
	fh.Unlock()
}

func TestIssue1023AppendFenceTimeoutPreservesPending(t *testing.T) {
	const path = "/append-fence-timeout.txt"
	fs, ino := pr939HandleFS(t, path, "A")
	fs.opts.RemoteCommitWaitTimeout = 10 * time.Millisecond
	fh, id := pr939Handle(t, fs, ino, path, "A", true)
	unlock := fs.lockRemoteCommitPath(path)
	defer unlock()
	fh.Lock()
	err := fs.stageShadowLocked(fh, true)
	seq, generation := fh.DirtySeq, fh.PendingIndexGen
	fh.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if n, st := pr939Append(fs, ino, id, "B"); n != 0 || st != gofuse.EAGAIN {
		t.Fatalf("blocked append: n=%d status=%v", n, st)
	}
	meta, _ := fs.pendingIndex.GetMeta(path)
	data, err := fs.shadowStore.ReadAll(path)
	if meta == nil || meta.Generation != generation || err != nil || string(data) != "A" || pr939HandleBytes(fh) != "A" || fh.DirtySeq != seq {
		t.Fatalf("timeout changed pending write: meta=%+v data=%q seq=%d err=%v", meta, data, fh.DirtySeq, err)
	}
}

func TestIssue1023AppendCoalescesDelayedPathTruncate(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			const path = "/append-truncate-handoff.txt"
			server, ts := newCASFileServer(t, path, 1, []byte("old"))
			defer ts.Close()
			fs, ino := pr939HandleFS(t, path, "old")
			if interactive {
				fs.syncMode = SyncInteractive
			}
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
			defer cq.DrainAll()
			// Keep the real SetAttr entry in its cancellable window. The
			// regression concerns undispatched truncates, not a timing guess.
			cq.zeroTruncateDelay = time.Hour
			fs.commitQueue = cq
			cq.PathLock, cq.DurableWatermark = fs.lockRemoteCommitPath, fs.latestCommittedRevision
			cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
			cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
			cq.IsSuperseded = fs.commitEntrySuperseded
			fh, id := pr939Handle(t, fs, ino, path, "old", false)
			fh.OpenPID = 101
			var attr gofuse.AttrOut
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 202}},
				Valid:    gofuse.FATTR_SIZE, Size: 0,
			}}, &attr); st != gofuse.OK {
				t.Fatal(st)
			}
			if !cq.HasPath(path) || !fh.ZeroBase {
				t.Fatal("truncate was not staged on the existing handle")
			}
			if n, st := pr939Append(fs, ino, id, "new"); n != 3 || st != gofuse.OK {
				t.Fatalf("append after staged truncate: n=%d status=%v", n, st)
			}
			if cq.HasPath(path) {
				t.Fatal("undispatched zero truncate was not coalesced")
			}
			if _, body, puts := server.snapshot(); string(body) != "old" || len(puts) != 0 {
				t.Fatalf("truncate published separately: body=%q puts=%d", body, len(puts))
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
				t.Fatalf("fsync appended content: %v", st)
			}
			waitCommitQueueIdle(t, cq)
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
				t.Fatal(st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			waitCommitQueueIdle(t, cq)
			if _, body, puts := server.snapshot(); string(body) != "new" || len(puts) != 1 {
				t.Fatalf("remote appended content=%q puts=%d", body, len(puts))
			}
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Fatalf("conflicts=%d", conflicts)
			}
		})
	}
}
