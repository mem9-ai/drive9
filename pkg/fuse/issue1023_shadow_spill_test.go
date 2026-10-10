package fuse

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Exercise Create and Open themselves: a manually allocated append handle does
// not carry Create's write-through shadow and eviction bookkeeping.
func issue1023CreateShadowAppenders(t *testing.T, interactive bool) (*Dat9FS, *casFileServer, func(), uint64, []uint64, *FileHandle) {
	t.Helper()
	const path = "/append-created-shadow.txt"
	server, ts := newCASFileServer(t, path, 0, nil)
	t.Cleanup(ts.Close)
	fs, _ := pr939HandleFS(t, path, "")
	if interactive {
		fs.syncMode = SyncInteractive
	}
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue = cq
	cq.PathLock = fs.lockRemoteCommitPath
	cq.DurableWatermark = fs.latestCommittedRevision
	cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
	cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
	cq.IsSuperseded = fs.commitEntrySuperseded
	t.Cleanup(cq.DrainAll)
	var created gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1, Caller: gofuse.Caller{Pid: 101}},
		Flags:    uint32(syscall.O_RDWR | syscall.O_APPEND),
		Mode:     defaultRegularFileMode,
	}, strings.TrimPrefix(path, "/"), &created); st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	creator, ok := fs.fileHandles.Get(created.Fh)
	if !ok || !creator.ShadowSpill || !creator.ShadowReady || creator.ShadowStageGen == 0 {
		t.Fatal("Create did not produce the expected shadow-spill handle")
	}
	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId, Caller: gofuse.Caller{Pid: 202}},
		Flags:    uint32(syscall.O_RDWR | syscall.O_APPEND),
	}, &opened); st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}
	return fs, server, ts.Close, created.NodeId, []uint64{created.Fh, opened.Fh}, creator
}

func issue1023ShadowHandlerRead(t *testing.T, fs *Dat9FS, ino, id uint64) string {
	t.Helper()
	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Size: uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}
	defer result.Done()
	data, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("ReadResult: %v", st)
	}
	return string(data)
}

func issue1023ShadowAppend(t *testing.T, fs *Dat9FS, ino, id uint64, record string) {
	t.Helper()
	if n, st := pr939Append(fs, ino, id, record); st != gofuse.OK || int(n) != len(record) {
		t.Fatalf("append %q: n=%d status=%v", record, n, st)
	}
}

func TestIssue1023CreatedAppenderAfterSiblingFsync(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			fs, server, _, ino, ids, creator := issue1023CreateShadowAppenders(t, interactive)
			issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
			issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
				t.Fatalf("sibling fsync: %v", st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, body, _ := server.snapshot(); string(body) != "A\nB\n" {
				t.Fatalf("before creator resumes: remote=%q", body)
			}
			if !creator.ShadowSpill {
				t.Fatal("creator lost the shadow-spill premise before its next write")
			}
			t.Logf("creator before resume: spill=%t size=%d base=%d seq=%d snapshot=%q shadow_generation=%d", creator.ShadowSpill, creator.Dirty.Size(), creator.BaseRev, creator.DirtySeq, creator.ContentSnapshotID, creator.ShadowStageGen)
			issue1023ShadowAppend(t, fs, ino, ids[0], "C\n")
			if got := issue1023ShadowHandlerRead(t, fs, ino, ids[0]); got != "A\nB\nC\n" {
				t.Errorf("creator read after acknowledged append=%q, want all three records", got)
			}
			for _, id := range ids {
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
					t.Errorf("fsync %d: %v", id, st)
				}
				waitCommitQueueIdle(t, fs.commitQueue)
			}
			if _, body, _ := server.snapshot(); string(body) != "A\nB\nC\n" {
				t.Errorf("remote after fsyncs=%q, want all three records", body)
			}
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Errorf("drain left conflicts=%d", conflicts)
			}
		})
	}
}

func TestIssue1023CreatedAppenderBeforeSiblingFsync(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			fs, server, _, ino, ids, _ := issue1023CreateShadowAppenders(t, interactive)
			issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
			issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
			if _, body, puts := server.snapshot(); len(body) != 0 || len(puts) != 0 {
				t.Fatal("writes unexpectedly committed before the pre-landed handoff")
			}
			t.Logf("sibling pending read=%q", issue1023ShadowHandlerRead(t, fs, ino, ids[1]))
			issue1023ShadowAppend(t, fs, ino, ids[0], "C\n")
			if got := issue1023ShadowHandlerRead(t, fs, ino, ids[0]); got != "A\nB\nC\n" {
				t.Errorf("creator read before any fsync=%q, want all three records", got)
			}
			for _, id := range ids {
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
					t.Errorf("fsync %d: %v", id, st)
				}
				waitCommitQueueIdle(t, fs.commitQueue)
			}
			if _, body, _ := server.snapshot(); string(body) != "A\nB\nC\n" {
				t.Errorf("remote after fsyncs=%q, want all three records", body)
			}
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Errorf("drain left conflicts=%d", conflicts)
			}
		})
	}
}

func TestIssue1023CreatedAppenderRefreshFailurePreservesState(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, unavailable := range []bool{false, true} {
			t.Run(fmt.Sprintf("interactive=%t/unavailable=%t", interactive, unavailable), func(t *testing.T) {
				fs, server, closeServer, ino, ids, creator := issue1023CreateShadowAppenders(t, interactive)
				issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
				issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
					t.Fatalf("sibling fsync: %v", st)
				}
				waitCommitQueueIdle(t, fs.commitQueue)
				if _, body, _ := server.snapshot(); string(body) != "A\nB\n" {
					t.Fatalf("before failed refresh: remote=%q", body)
				}
				// Buffer identity/version detects replacement and mutation without
				// materializing an evicted shadow buffer as zero-filled bytes.
				type state struct {
					buffer                      *WriteBuffer
					version, seq, writeBackSeq  uint64
					base, size                  int64
					dirty, shadowReady, spill   bool
					isNew, zeroBase             bool
					gens                        StagingGens
					snapshot, staged            string
					activeShadow, activePending uint64
				}
				capture := func() state {
					creator.Lock()
					defer creator.Unlock()
					return state{
						buffer: creator.Dirty, version: creator.Dirty.contentVersion,
						seq: creator.DirtySeq, writeBackSeq: creator.WriteBackSeq,
						base: creator.BaseRev, size: creator.Dirty.Size(), dirty: creator.Dirty.HasDirtyParts(),
						shadowReady: creator.ShadowReady, spill: creator.ShadowSpill,
						isNew: creator.IsNew, zeroBase: creator.ZeroBase,
						gens:     fs.captureHandleStagingGensLocked(creator),
						snapshot: creator.ContentSnapshotID, staged: creator.StagedSnapshotID,
						activeShadow:  fs.shadowStore.ActiveGeneration(creator.Path),
						activePending: fs.pendingIndex.Generation(creator.Path),
					}
				}
				before := capture()
				if !before.spill {
					t.Fatal("failed-refresh premise requires the original spill handle")
				}
				if unavailable {
					closeServer()
				} else {
					server.mu.Lock()
					server.revision++
					server.body = []byte("external")
					server.mu.Unlock()
				}
				revision, body, puts := server.snapshot()
				if n, st := pr939Append(fs, ino, ids[0], "C\n"); n != 0 || st == gofuse.OK {
					t.Errorf("append without valid remote proof: n=%d status=%v", n, st)
				}
				if after := capture(); after != before {
					t.Errorf("failed refresh changed handle or staging: before=%+v after=%+v", before, after)
				}
				if afterRev, afterBody, afterPuts := server.snapshot(); afterRev != revision || string(afterBody) != string(body) || len(afterPuts) != len(puts) {
					t.Errorf("failed refresh changed remote: revision=%d body=%q puts=%d", afterRev, afterBody, len(afterPuts))
				}
			})
		}
	}
}

func TestIssue1023CreatedEvictedAppenderHandoff(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, landed := range []bool{false, true} {
			t.Run(fmt.Sprintf("interactive=%t/landed=%t", interactive, landed), func(t *testing.T) {
				fs, server, _, ino, ids, creator := issue1023CreateShadowAppenders(t, interactive)
				// Keep the actual Create-installed shadow callbacks, but force a
				// completed part at one record to exercise eviction with tiny data.
				creator.Lock()
				creator.Dirty.partSize = 2
				creator.Dirty.SetSmallFileMax(1)
				creator.Unlock()
				issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
				creator.Lock()
				if len(creator.Dirty.uploadedParts) == 0 || creator.Dirty.IsPartLoaded(0) {
					creator.Unlock()
					t.Fatal("creator's first part was not evicted")
				}
				creator.Unlock()
				if got := issue1023ShadowHandlerRead(t, fs, ino, ids[0]); got != "A\n" {
					t.Fatalf("evicted creator read=%q", got)
				}
				issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
				if got := issue1023ShadowHandlerRead(t, fs, ino, ids[1]); got != "A\nB\n" {
					t.Errorf("sibling read from evicted source=%q", got)
				}
				if landed {
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
						t.Fatalf("sibling fsync: %v", st)
					}
					waitCommitQueueIdle(t, fs.commitQueue)
					if _, body, _ := server.snapshot(); string(body) != "A\nB\n" {
						t.Errorf("landed image from evicted source=%q", body)
					}
				}
				issue1023ShadowAppend(t, fs, ino, ids[0], "C\n")
				if got := issue1023ShadowHandlerRead(t, fs, ino, ids[0]); got != "A\nB\nC\n" {
					t.Errorf("creator read after evicted handoff=%q", got)
				}
				for _, id := range ids {
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
						t.Errorf("fsync %d: %v", id, st)
					}
					waitCommitQueueIdle(t, fs.commitQueue)
				}
				if _, body, _ := server.snapshot(); string(body) != "A\nB\nC\n" {
					t.Errorf("remote after evicted handoff=%q", body)
				}
			})
		}
	}
}

// Invalid or unsupported shadow evidence must not turn into acknowledged
// zero-filled or stale-prefix append data on the separately opened handle.
func TestIssue1023CreatedAppenderRejectsInvalidShadowSource(t *testing.T) {
	for _, scenario := range []string{"no-generation", "stale-generation", "wrong-length", "too-large"} {
		t.Run(scenario, func(t *testing.T) {
			fs, server, _, ino, ids, creator := issue1023CreateShadowAppenders(t, false)
			issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
			creator.Lock()
			switch scenario {
			case "no-generation":
				creator.ShadowStageGen = 0
			case "stale-generation":
				creator.ShadowStageGen++
			case "wrong-length":
				if err := fs.shadowStore.WriteFull(creator.Path, []byte("A"), 0); err != nil {
					creator.Unlock()
					t.Fatal(err)
				}
				creator.ShadowStageGen = fs.shadowStore.ActiveGeneration(creator.Path)
			case "too-large":
				// Admission rejects a live oversized child before ACK. Inject
				// unsupported source evidence, like the other faults here;
				// real oversized admission/retirement/retry has separate coverage.
				if err := creator.Dirty.Truncate(maxLandedPayloadBytes + 1); err != nil {
					creator.Unlock()
					t.Fatal(err)
				}
				if err := fs.shadowStore.Truncate(creator.Path, creator.Dirty.Size(), creator.BaseRev); err != nil {
					creator.Unlock()
					t.Fatal(err)
				}
				creator.ShadowStageGen = fs.shadowStore.ActiveGeneration(creator.Path)
			}
			creator.Unlock()
			target, _ := fs.fileHandles.Get(ids[1])
			buffer, version, seq := target.Dirty, target.Dirty.contentVersion, target.DirtySeq
			generation := fs.shadowStore.ActiveGeneration(creator.Path)
			shadow, err := fs.shadowStore.ReadAll(creator.Path)
			if err != nil {
				t.Fatal(err)
			}
			if n, st := pr939Append(fs, ino, ids[1], "B\n"); n != 0 || st != gofuse.EAGAIN {
				t.Errorf("append from invalid shadow: n=%d status=%v", n, st)
			}
			if target.Dirty != buffer || buffer.contentVersion != version || target.DirtySeq != seq || buffer.Size() != 0 {
				t.Error("rejected append changed target buffer")
			}
			if got, err := fs.shadowStore.ReadAll(creator.Path); err != nil || string(got) != string(shadow) || fs.shadowStore.ActiveGeneration(creator.Path) != generation {
				t.Error("rejected append changed shadow evidence")
			}
			if _, body, puts := server.snapshot(); len(body) != 0 || len(puts) != 0 {
				t.Error("rejected append mutated remote content")
			}
		})
	}
}

// An older fsync must not republish its ancestor after a complete descendant
// landed. Test the staging boundary before enqueue/cleanup can hide the view.
func TestIssue1023OlderFsyncDoesNotRepublishAppendAncestor(t *testing.T) {
	for _, created := range []bool{false, true} {
		t.Run(fmt.Sprintf("created=%t", created), func(t *testing.T) {
			var fs *Dat9FS
			var server *casFileServer
			var ino uint64
			var ids []uint64
			if created {
				fs, server, _, ino, ids, _ = issue1023CreateShadowAppenders(t, true)
			} else {
				const path = "/append-memory-staging.txt"
				var closeServer func()
				memoryServer, ts := newCASFileServer(t, path, 1, nil)
				server = memoryServer
				closeServer = ts.Close
				t.Cleanup(closeServer)
				fs, ino = pr939HandleFS(t, path, "")
				fs.syncMode = SyncInteractive
				fs.client = newTestClient(ts.URL)
				fs.client.SetSmallFileThresholdForTests(1 << 20)
				cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
				fs.commitQueue = cq
				cq.PathLock, cq.DurableWatermark = fs.lockRemoteCommitPath, fs.latestCommittedRevision
				cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
				cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
				cq.IsSuperseded = fs.commitEntrySuperseded
				t.Cleanup(cq.DrainAll)
				_, a := pr939Handle(t, fs, ino, path, "", false)
				_, b := pr939Handle(t, fs, ino, path, "", false)
				ids = []uint64{a, b}
			}
			issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
			issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
			issue1023ShadowAppend(t, fs, ino, ids[0], "C\n")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, body, _ := server.snapshot(); string(body) != "A\nB\nC\n" {
				t.Fatalf("descendant not landed: %q", body)
			}
			older, _ := fs.fileHandles.Get(ids[1])
			older.Lock()
			if !older.Dirty.HasDirtyParts() || older.Dirty.Size() != 4 {
				older.Unlock()
				t.Fatal("old dirty ancestor premise lost")
			}
			err := fs.stageShadowForQueuedCommitLocked(context.Background(), older, true)
			fs.releaseHandleRemoteCommitPathLocked(older)
			older.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if fs.shadowStore.Has(older.Path) {
				if data, err := fs.shadowStore.ReadAll(older.Path); err != nil || string(data) != "A\nB\nC\n" {
					t.Errorf("staging republished old ancestor: data=%q err=%v", data, err)
				}
			}
			var reader gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &reader); st != gofuse.OK {
				t.Fatal(st)
			}
			if got := issue1023ShadowHandlerRead(t, fs, ino, reader.Fh); got != "A\nB\nC\n" {
				t.Errorf("published read before queue cleanup=%q", got)
			}
		})
	}
}

func TestIssue1023AncestorStagingFailureDoesNotPublishFallback(t *testing.T) {
	for _, operation := range []string{"fsync", "flush"} {
		for _, unavailable := range []bool{false, true} {
			t.Run(fmt.Sprintf("operation=%s/unavailable=%t", operation, unavailable), func(t *testing.T) {
				fs, server, closeServer, ino, ids, _ := issue1023CreateShadowAppenders(t, true)
				cache, err := NewWriteBackCache(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				fs.writeBack = cache
				issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
				issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
				issue1023ShadowAppend(t, fs, ino, ids[0], "C\n")
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
					t.Fatal(st)
				}
				waitCommitQueueIdle(t, fs.commitQueue)
				older, _ := fs.fileHandles.Get(ids[1])
				buffer, version, seq := older.Dirty, older.Dirty.contentVersion, older.DirtySeq
				snapshot, generation := older.StagedSnapshotID, fs.pendingIndex.Generation(older.Path)
				if _, ok := cache.GetMeta(older.Path); ok {
					t.Fatal("writeback fallback premise requires a retired descendant cache")
				}
				wantStatus := gofuse.EAGAIN
				if unavailable {
					closeServer()
					// Establish the transport mapping independently of staging.
					// A closed listener is an I/O failure, not a lineage rejection.
					_, readErr := fs.client.StatCtx(context.Background(), fs.remotePath(older.Path))
					if readErr == nil || httpToFuseStatus(readErr) != gofuse.EIO {
						t.Fatalf("closed server error=%v, want original EIO mapping", readErr)
					}
					wantStatus = gofuse.EIO
				} else {
					server.mu.Lock()
					server.revision++
					server.body = []byte("external")
					server.mu.Unlock()
				}
				var st gofuse.Status
				if operation == "fsync" {
					st = fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
				} else {
					st = fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
				}
				if st != wantStatus {
					t.Errorf("failed ancestor proof: status=%v, want %v", st, wantStatus)
				}
				if older.Dirty != buffer || buffer.contentVersion != version || older.DirtySeq != seq || !buffer.HasDirtyParts() || older.StagedSnapshotID != snapshot {
					t.Error("failed ancestor proof changed dirty snapshot")
				}
				if fs.shadowStore.Has(older.Path) || fs.pendingIndex.Generation(older.Path) != generation {
					t.Error("failed ancestor proof republished shadow/pending data")
				}
				if _, ok := cache.GetMeta(older.Path); ok {
					t.Error("failed ancestor proof published writeback fallback")
				}
			})
		}
	}
}
