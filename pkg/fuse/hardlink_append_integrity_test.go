package fuse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Use Link and Open handlers to obtain two still-linked paths for one inode.
// Hand-assigning FileHandle.Path misses the retained-FD/Link identity handoff.
func issue1023HardlinkAppenders(t *testing.T, interactive bool) (*Dat9FS, uint64, *casFileServer, []uint64, *atomic.Int32) {
	t.Helper()
	return issue1023HardlinkAppendersWithBody(t, interactive, "base")
}

func issue1023HardlinkAppendersWithBody(t *testing.T, interactive bool, body string) (*Dat9FS, uint64, *casFileServer, []uint64, *atomic.Int32) {
	t.Helper()
	const original = "/hardlink-original"
	const alias = "/hardlink-alias"
	fs, ino, server, closeOld := reviewR2FS(t, original, interactive)
	cleanupKernelCacheBypassAfterTest(t, fs)
	closeOld()
	server.mu.Lock()
	server.body = []byte(body)
	server.mu.Unlock()
	fs.inodes.UpdateSize(ino, int64(len(body)))
	var linked atomic.Bool
	var putAttempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Get("hardlink") == "1" {
			if r.URL.Path != "/v1/fs"+alias || r.Header.Get("X-Dat9-Hardlink-Source") != original {
				t.Error("unexpected hardlink request")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			linked.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		if r.URL.Path != "/v1/fs"+original && (r.URL.Path != "/v1/fs"+alias || !linked.Load()) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Dat9-Resource-ID", "shared-hardlink-resource")
		w.Header().Set("X-Dat9-Mode", "420")
		if linked.Load() {
			w.Header().Set("X-Dat9-Nlink", "2")
		} else {
			w.Header().Set("X-Dat9-Nlink", "1")
		}
		if r.Method == http.MethodPut {
			putAttempts.Add(1)
		}
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/v1/fs" + original
		clone.URL = &urlCopy
		server.serveHTTP(w, clone)
	}))
	t.Cleanup(ts.Close)
	fs.client = newTestClient(ts.URL)
	// Keep mock uploads on direct PUT while exercising the FUSE proof cap.
	fs.client.SetSmallFileThresholdForTests(max(int64(1<<20), int64(len(body))+1024))
	fs.commitQueue.client = fs.client
	fs.inodes.SetIdentity(ino, "shared-hardlink-resource", 1)
	fs.inodes.UpdateMode(ino, 0o644)
	open := func(pid uint32) uint64 {
		var out gofuse.OpenOut
		if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: pid}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &out); st != gofuse.OK {
			t.Fatalf("Open: %v", st)
		}
		return out.Fh
	}
	a := open(101)
	var out gofuse.EntryOut
	if st := fs.Link(nil, &gofuse.LinkIn{InHeader: gofuse.InHeader{NodeId: 1}, Oldnodeid: ino}, "hardlink-alias", &out); st != gofuse.OK {
		t.Fatalf("Link: %v", st)
	}
	if out.NodeId != ino || out.Nlink != 2 {
		t.Fatalf("Link identity=%d/%d", out.NodeId, out.Nlink)
	}
	b := open(202)
	ids := []uint64{a, b}
	first, _ := fs.fileHandles.Get(a)
	second, _ := fs.fileHandles.Get(b)
	if first.Path != original || second.Path != alias || first.Ino != second.Ino {
		t.Fatalf("retained/alias premise=%q/%q ino=%d/%d", first.Path, second.Path, first.Ino, second.Ino)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			if h, ok := fs.fileHandles.Get(id); ok {
				h.Lock()
				fs.releaseHandleRemoteCommitPathLocked(h)
				h.Unlock()
			}
		}
	})
	return fs, ino, server, ids, &putAttempts
}

func TestIssue1023HardlinkAppendComposition(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, commitSibling := range []bool{false, true} {
			t.Run(fmt.Sprintf("interactive=%t/sibling_fsync=%t", interactive, commitSibling), func(t *testing.T) {
				fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, interactive)
				issue1023ShadowAppend(t, fs, ino, ids[0], "A")
				issue1023ShadowAppend(t, fs, ino, ids[1], "B")
				b, _ := fs.fileHandles.Get(ids[1])
				if got := pr939HandleBytes(b); got != "baseAB" {
					t.Errorf("acknowledged alias image=%q, want baseAB", got)
				}
				if commitSibling {
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
						t.Errorf("sibling fsync=%v", st)
					}
					waitCommitQueueIdle(t, fs.commitQueue)
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
					waitCommitQueueIdle(t, fs.commitQueue)
				}
				issue1023ShadowAppend(t, fs, ino, ids[0], "C")
				for _, id := range ids {
					if _, ok := fs.fileHandles.Get(id); !ok {
						continue
					}
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
						t.Errorf("fsync=%v", st)
					}
					waitCommitQueueIdle(t, fs.commitQueue)
				}
				if _, data, _ := server.snapshot(); string(data) != "baseABC" {
					t.Errorf("server=%q, want baseABC", data)
				}
				if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
					t.Errorf("drain conflicts=%d", conflicts)
				}
			})
		}
	}
}

func TestIssue1023HardlinkReplacedSourceRejectsAppend(t *testing.T) {
	fs, ino, _, ids, _ := issue1023HardlinkAppenders(t, false)
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	before := pr939HandleBytes(b)
	buffer, seq, version := b.Dirty, b.DirtySeq, b.Dirty.contentVersion
	// Exercise the identity-publication window before the old handle is
	// retargeted/marked: its old pathname now belongs to another file.
	fs.inodes.RemoveLinkPreserve(a.Path)
	other := fs.inodes.LookupWithIdentity(a.Path, "replacement-resource", 1, false, 4, time.Now())
	if other == ino {
		t.Fatal("replacement retained the old file identity")
	}
	n, st := pr939Append(fs, ino, ids[1], "B")
	if n != 0 || st != gofuse.EAGAIN {
		t.Errorf("unproved alias handoff accepted %d/%v", n, st)
	}
	if b.Dirty != buffer || b.DirtySeq != seq || buffer.contentVersion != version || pr939HandleBytes(b) != before {
		t.Error("rejected append changed target state")
	}
}

func TestIssue1023HardlinkRemoteIdentityMismatchPreservesState(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, interactive)
			issue1023ShadowAppend(t, fs, ino, ids[0], "A")
			issue1023ShadowAppend(t, fs, ino, ids[1], "B")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
			waitCommitQueueIdle(t, fs.commitQueue)
			a, _ := fs.fileHandles.Get(ids[0])
			before := pr939HandleBytes(a)
			buffer, seq, version := a.Dirty, a.DirtySeq, a.Dirty.contentVersion
			// Equal revision/size/bytes on another resource cannot certify
			// that the original alias still names this linked file.
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Dat9-Resource-ID", "replacement-resource")
				server.serveHTTP(w, r)
			}))
			t.Cleanup(ts.Close)
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			fs.commitQueue.client = fs.client
			n, st := pr939Append(fs, ino, ids[0], "C")
			if n != 0 || st != gofuse.EAGAIN {
				t.Errorf("foreign resource append=%d/%v", n, st)
			}
			if a.Dirty != buffer || a.DirtySeq != seq || buffer.contentVersion != version || pr939HandleBytes(a) != before {
				t.Error("failed identity proof mutated local image")
			}
			if _, data, _ := server.snapshot(); string(data) != "baseAB" {
				t.Errorf("server changed=%q", data)
			}
		})
	}
}

func TestIssue1023HardlinkConcurrentFsyncOrdersPublication(t *testing.T) {
	fs, ino, server, ids, puts := issue1023HardlinkAppenders(t, false)
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	issue1023ShadowAppend(t, fs, ino, ids[1], "B")
	server.firstPutStarted = make(chan struct{})
	server.releaseFirstPut = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(server.releaseFirstPut) }) }
	t.Cleanup(release)
	done := make(chan gofuse.Status, 2)
	go func() { done <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}) }()
	select {
	case <-server.firstPutStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first fsync did not start upload")
	}
	go func() { done <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}) }()
	stack := make([]byte, 1<<20)
	deadline := time.Now().Add(3 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		if puts.Load() != 1 {
			t.Error("second alias published while predecessor had not acknowledged its commit")
			break
		}
		n := runtime.Stack(stack, true)
		for _, frame := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(frame, "(*Dat9FS).Fsync(") && strings.Contains(frame, "beginImmediateMutationCommit") {
				waiting = true
			}
		}
		if waiting {
			break
		}
		runtime.Gosched()
	}
	if !waiting {
		t.Error("alias did not wait at the shared file publication boundary")
	}
	release()
	for range 2 {
		select {
		case st := <-done:
			if st != gofuse.OK {
				t.Errorf("fsync=%v", st)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("fsync failed to make progress")
		}
	}
	if _, data, _ := server.snapshot(); string(data) != "baseAB" {
		t.Errorf("server=%q", data)
	}
}

func TestIssue1023HardlinkShadowSourceKeepsPathOwnership(t *testing.T) {
	fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, true)
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	a.Lock()
	a.Dirty = fs.newWriteBuffer(a.Path, maxPreloadSize, 2)
	if _, err := a.Dirty.Write(0, []byte("base")); err != nil {
		t.Fatal(err)
	}
	a.Dirty.ClearDirty()
	if err := fs.shadowStore.WriteFull(a.Path, []byte("base"), 1); err != nil {
		t.Fatal(err)
	}
	a.ShadowReady, a.ShadowSpill = true, true
	a.ShadowStageGen = fs.shadowStore.ActiveGeneration(a.Path)
	fs.bindShadowSpillEvictionLocked(a)
	a.Unlock()
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	a.Lock()
	a.Dirty.EvictPart(1)
	sourceGeneration := a.ShadowStageGen
	a.Unlock()
	if err := fs.shadowStore.WriteFull(b.Path, []byte("wrong"), 1); err != nil {
		t.Fatal(err)
	}
	issue1023ShadowAppend(t, fs, ino, ids[1], "B")
	if got := pr939HandleBytes(b); got != "baseAB" {
		t.Errorf("alias consumed wrong source path=%q", got)
	}
	if b.ShadowStageGen != 0 || b.PendingIndexGen != 0 || b.WriteBackGen != 0 {
		t.Error("target borrowed source path cleanup generations")
	}
	if fs.shadowStore.ActiveGeneration(a.Path) != sourceGeneration {
		t.Error("alias refresh removed source staging")
	}
	for _, id := range []uint64{ids[1], ids[0]} {
		if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
			t.Errorf("fsync=%v", st)
		}
		waitCommitQueueIdle(t, fs.commitQueue)
	}
	if _, data, _ := server.snapshot(); string(data) != "baseAB" {
		t.Errorf("server=%q", data)
	}
}

func TestIssue1023HardlinkIndependentWriters(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			for trial := range 3 {
				fs, ino, server, initial, _ := issue1023HardlinkAppenders(t, interactive)
				ids := append([]uint64(nil), initial...)
				for i := 2; i < 4; i++ {
					var out gofuse.OpenOut
					if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: uint32(300 + i)}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &out); st != gofuse.OK {
						t.Fatal(st)
					}
					ids = append(ids, out.Fh)
				}
				start := make(chan struct{})
				done := make(chan string, 4)
				for writer, id := range ids {
					go func() {
						<-start
						for n := range 12 {
							record := fmt.Sprintf("w%02d-%03d\n", writer, n)
							if size, st := pr939Append(fs, ino, id, record); st != gofuse.OK || int(size) != len(record) {
								done <- fmt.Sprintf("writer=%d write=%d/%v", writer, size, st)
								return
							}
							runtime.Gosched()
						}
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
							done <- fmt.Sprintf("writer=%d fsync=%v", writer, st)
							return
						}
						if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
							done <- fmt.Sprintf("writer=%d flush=%v", writer, st)
							return
						}
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
						done <- ""
					}()
				}
				close(start)
				for range ids {
					select {
					case err := <-done:
						if err != "" {
							t.Errorf("trial=%d %s", trial, err)
						}
					case <-time.After(15 * time.Second):
						t.Fatal("writers stalled")
					}
				}
				waitCommitQueueIdle(t, fs.commitQueue)
				_, body, _ := server.snapshot()
				if !strings.HasPrefix(string(body), "base") {
					t.Fatalf("baseline lost=%q", body)
				}
				counts := make(map[string]int)
				for _, record := range strings.Split(strings.TrimSuffix(string(body[4:]), "\n"), "\n") {
					counts[record]++
				}
				if len(counts) != 48 {
					t.Errorf("trial=%d record count=%d", trial, len(counts))
				}
				for writer := range 4 {
					for n := range 12 {
						record := fmt.Sprintf("w%02d-%03d", writer, n)
						if counts[record] != 1 {
							t.Errorf("trial=%d record=%s count=%d", trial, record, counts[record])
						}
					}
				}
				if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
					t.Errorf("trial=%d conflicts=%d", trial, conflicts)
				}
			}
		})
	}
}

func TestIssue1023HardlinkCommittedAliasReadBoundary(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			fs, ino, _, ids, _ := issue1023HardlinkAppenders(t, interactive)
			cleanupKernelCacheBypassAfterTest(t, fs)
			issue1023ShadowAppend(t, fs, ino, ids[0], "A")
			issue1023ShadowAppend(t, fs, ino, ids[1], "B")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			issue1023ShadowAppend(t, fs, ino, ids[0], "C")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			for _, name := range []string{"hardlink-original", "hardlink-alias"} {
				cached := fs.dirCache.Lookup("/", name)
				if cached.kind != namespaceLookupPositive || cached.item.Size != 7 {
					t.Errorf("%s cached committed size=%d", name, cached.item.Size)
				}
				var entry gofuse.EntryOut
				if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, name, &entry); st != gofuse.OK {
					t.Fatal(st)
				}
				if entry.NodeId != ino || entry.Size != 7 {
					t.Errorf("%s Lookup identity/size=%d/%d", name, entry.NodeId, entry.Size)
				}
				var opened gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &opened); st != gofuse.OK {
					t.Fatal(st)
				}
				if opened.OpenFlags&gofuse.FOPEN_DIRECT_IO == 0 {
					t.Errorf("%s read may use stale kernel size/pages before notify completes", name)
				}
				if got := issue1023ShadowHandlerRead(t, fs, ino, opened.Fh); got != "baseABC" {
					t.Errorf("%s read=%q", name, got)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh})
			}
		})
	}
}

func TestIssue1023HardlinkOversizedSyncPublication(t *testing.T) {
	for _, variant := range []struct{ lockedPeer, spill bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("locked_peer=%t/spill=%t", variant.lockedPeer, variant.spill), func(t *testing.T) {
			base := strings.Repeat("x", maxLandedPayloadBytes+64)
			fs, ino, server, ids, _ := issue1023HardlinkAppendersWithBody(t, false, base)
			a, _ := fs.fileHandles.Get(ids[0])
			b, _ := fs.fileHandles.Get(ids[1])
			if a.Dirty.Size() != int64(len(base)) || b.Dirty.Size() != int64(len(base)) {
				t.Fatalf("large Open buffers=%d/%d", a.Dirty.Size(), b.Dirty.Size())
			}
			issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
			if variant.spill {
				a.Lock()
				err := fs.stageShadowForQueuedCommitLocked(t.Context(), a, true)
				if err == nil {
					a.ShadowSpill = true
				}
				a.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			if variant.lockedPeer {
				b.Lock()
			}
			status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
			if variant.lockedPeer {
				b.Unlock()
			}
			if status != gofuse.OK {
				t.Fatalf("first large fsync=%v", status)
			}
			revision, data, _ := server.snapshot()
			if string(data) != base+"A\n" {
				t.Fatalf("first remote size=%d, want=%d", len(data), len(base)+2)
			}
			issue1023ShadowAppend(t, fs, ino, ids[1], "B\n")
			if b.BaseRev != revision || b.Dirty.Size() != int64(len(base)+4) {
				t.Errorf("acknowledged alias append revision/size=%d/%d, want=%d/%d", b.BaseRev, b.Dirty.Size(), revision, len(base)+4)
			}
			if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); status != gofuse.OK {
				t.Errorf("alias fsync=%v", status)
			}
			_, data, _ = server.snapshot()
			if string(data) != base+"A\nB\n" {
				t.Errorf("final remote size=%d, want=%d", len(data), len(base)+4)
			}
		})
	}
}

func TestIssue1023HardlinkOversizedDirtyAliasRejectsStaleAppend(t *testing.T) {
	base := strings.Repeat("x", maxLandedPayloadBytes+64)
	fs, ino, server, ids, _ := issue1023HardlinkAppendersWithBody(t, false, base)
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	issue1023ShadowAppend(t, fs, ino, ids[0], "A\n")
	// Model an old dirty/recovered image which cannot adopt only a CAS token.
	// This is a handler failure-boundary regression, not daemon recovery E2E.
	b.Lock()
	if _, err := b.Dirty.Write(int64(len(base)), []byte("local\n")); err != nil {
		b.Unlock()
		t.Fatal(err)
	}
	b.DirtySeq = fs.markDirtySize(ino, b.Dirty.Size())
	b.LineageTrusted = false
	b.Unlock()
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); status != gofuse.OK {
		t.Fatal(status)
	}
	revision, remoteBefore, _ := server.snapshot()
	buffer, seq, contentVersion := b.Dirty, b.DirtySeq, b.Dirty.contentVersion
	before := append([]byte(nil), b.Dirty.Bytes()...)
	if n, status := pr939Append(fs, ino, ids[1], "B\n"); n != 0 || status != gofuse.EAGAIN {
		t.Errorf("unproven oversized append acknowledged n=%d/status=%v", n, status)
	}
	if b.Dirty != buffer || b.DirtySeq != seq || b.Dirty.contentVersion != contentVersion || string(b.Dirty.Bytes()) != string(before) || b.BaseRev != 1 {
		t.Error("failed oversized refresh changed retained dirty state")
	}
	if got := fs.latestCommittedRevision(b.Path); got != revision || fs.latestCommittedRevision(a.Path) != revision {
		t.Errorf("aliases did not publish committed revision: alias=%d, remote=%d", got, revision)
	}
	if _, remoteAfter, _ := server.snapshot(); string(remoteBefore) != string(remoteAfter) {
		t.Error("rejected append changed remote content")
	}
}

func TestIssue1023HardlinkOversizedSnapshotSizePublication(t *testing.T) {
	base := strings.Repeat("x", maxLandedPayloadBytes+64)
	fs, ino, server, ids, _ := issue1023HardlinkAppendersWithBody(t, false, base)
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	// Model the generic upload's redirty completion boundary: the landed
	// snapshot ends at A, while this FD must retain its newer local C.
	a.Lock()
	if _, err := a.Dirty.Write(int64(len(base)), []byte("A\nC\n")); err != nil {
		a.Unlock()
		t.Fatal(err)
	}
	a.DirtySeq = fs.markDirtySize(ino, a.Dirty.Size())
	server.mu.Lock()
	server.body, server.revision = []byte(base+"A\n"), 2
	server.mu.Unlock()
	fs.markHandleRevisionOnlyLocked(a, 2, int64(len(base)+2))
	if a.Dirty.Size() != int64(len(base)+4) || a.DirtySeq == 0 || !a.Dirty.HasDirtyParts() {
		t.Error("snapshot publication retired redirtied content")
	}
	a.Unlock()
	if b.BaseRev != 2 || b.Dirty.Size() != int64(len(base)+2) {
		t.Errorf("alias adopted live size instead of snapshot: rev/size=%d/%d", b.BaseRev, b.Dirty.Size())
	}
	if revision, size, ok := fs.latestCommittedRevisionWithSize(b.Path); !ok || revision != 2 || size != int64(len(base)+2) {
		t.Errorf("alias watermark=%d/%d/%t", revision, size, ok)
	}
}

func TestIssue1023HardlinkOrdinaryShadowBindsSnapshotIdentity(t *testing.T) {
	fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, true)
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	a.Lock()
	// The staged shadow is authoritative while the ordinary writable
	// buffer has an unloaded prefix. Its prepared identity belongs to
	// these bytes even before a sibling publishes the handle image.
	if err := fs.shadowStore.WriteFull(a.Path, []byte("baseA"), a.BaseRev); err != nil {
		a.Unlock()
		t.Fatal(err)
	}
	a.ShadowReady, a.ShadowSpill = true, false
	a.ShadowStageGen = fs.shadowStore.ActiveGeneration(a.Path)
	a.ShadowStageSeq = a.DirtySeq
	a.Dirty.EvictPart(0)
	if a.Dirty.CanMaterializeFull() || !a.Dirty.HasDirtyParts() {
		a.Unlock()
		t.Fatal("missing ordinary authoritative-shadow premise")
	}
	ensureStagedSnapshotLineageLocked(a)
	entry := &CommitEntry{Path: a.Path, Inode: ino, MutationSeq: a.DirtySeq, BaseRev: a.BaseRev, Size: 5, Kind: PendingOverwrite}
	fs.bindCommitEntryToHandleLocked(entry, a, a.BaseRev)
	entry.bindPayload([]byte("baseA"))
	entry.ShadowGen, entry.PendingIndexGen, entry.WriteBackGen = 0, 0, 0
	fs.releaseHandleRemoteCommitPathLocked(a)
	a.Unlock()
	issue1023ShadowAppend(t, fs, ino, ids[1], "B")
	if b.ContentSnapshotID != entry.SnapshotID {
		t.Errorf("copied shadow bytes without prepared identity: got=%s, want=%s", b.ContentSnapshotID, entry.SnapshotID)
	}
	if err := fs.commitQueue.CommitNow(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	// B's successful write must survive A's commit and B's next append.
	issue1023ShadowAppend(t, fs, ino, ids[1], "C")
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); status != gofuse.OK {
		t.Fatal(status)
	}
	waitCommitQueueIdle(t, fs.commitQueue)
	if _, data, _ := server.snapshot(); string(data) != "baseABC" {
		t.Errorf("acknowledged records=%q, want baseABC", data)
	}
}

func TestIssue1023HardlinkOpenPreloadBindsPendingIdentity(t *testing.T) {
	fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, true)
	a, _ := fs.fileHandles.Get(ids[0])
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	a.Lock()
	if !fs.materializeFullForUploadLocked(a) {
		a.Unlock()
		t.Fatal("source image unavailable")
	}
	ensureStagedSnapshotLineageLocked(a)
	entry := &CommitEntry{Path: a.Path, Inode: ino, MutationSeq: a.DirtySeq, BaseRev: a.BaseRev, Size: 5, Kind: PendingOverwrite}
	fs.bindCommitEntryToHandleLocked(entry, a, a.BaseRev)
	entry.bindPayload([]byte("baseA"))
	entry.ShadowGen, entry.PendingIndexGen, entry.WriteBackGen = 0, 0, 0
	fs.releaseHandleRemoteCommitPathLocked(a)
	a.Unlock()
	var opened gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 707}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); status != gofuse.OK {
		t.Fatal(status)
	}
	c, _ := fs.fileHandles.Get(opened.Fh)
	if c.ContentSnapshotID != entry.SnapshotID {
		t.Errorf("Open copied pending bytes without identity: got=%s, want=%s", c.ContentSnapshotID, entry.SnapshotID)
	}
	if got := pr939HandleBytes(c); got != "baseA" {
		t.Errorf("Open pending content=%q", got)
	}
	if err := fs.commitQueue.CommitNow(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	issue1023ShadowAppend(t, fs, ino, opened.Fh, "B")
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}); status != gofuse.OK {
		t.Fatal(status)
	}
	waitCommitQueueIdle(t, fs.commitQueue)
	if _, data, _ := server.snapshot(); string(data) != "baseAB" {
		t.Errorf("acknowledged records=%q", data)
	}
}

func TestIssue1023HardlinkAppendRechecksLateCommitBeforeAck(t *testing.T) {
	fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, false)
	var opened gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 808}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); status != gofuse.OK {
		t.Fatal(status)
	}
	c, _ := fs.fileHandles.Get(opened.Fh)
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	issue1023ShadowAppend(t, fs, ino, ids[1], "B")
	issue1023ShadowAppend(t, fs, ino, ids[0], "C")
	var committed atomic.Bool
	testHookBeforeAppendSourceRefresh = func(handle *FileHandle) {
		if handle != c || !committed.CompareAndSwap(false, true) {
			return
		}
		// The writer passed its first pending check, but the newest
		// source commits and leaves the registry before the source scan.
		if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); status != gofuse.OK {
			t.Errorf("late source fsync=%v", status)
		}
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
	}
	t.Cleanup(func() { testHookBeforeAppendSourceRefresh = nil })
	issue1023ShadowAppend(t, fs, ino, opened.Fh, "D")
	if !committed.Load() || !c.Dirty.CanMaterializeFull() {
		t.Fatal("late-commit window or complete-buffer premise not exercised")
	}
	if got := pr939HandleBytes(c); got != "baseABCD" {
		t.Errorf("acknowledged append image=%q, want baseABCD", got)
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}); status != gofuse.OK {
		t.Errorf("new writer fsync=%v", status)
	}
	if _, data, _ := server.snapshot(); string(data) != "baseABCD" {
		t.Errorf("server=%q, want baseABCD", data)
	}
}

func TestIssue1023HardlinkRetriesLocallyAdvancedSnapshotRead(t *testing.T) {
	fs, ino, server, ids, _ := issue1023HardlinkAppenders(t, false)
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	issue1023ShadowAppend(t, fs, ino, ids[0], "A")
	issue1023ShadowAppend(t, fs, ino, ids[1], "B")
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); status != gofuse.OK {
		t.Fatal(status)
	}
	parent := fs.commitQueue.landedCommit(b.Path)
	var advanced atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+a.Path && advanced.CompareAndSwap(false, true) {
			// HEAD observed rev 2; this mount lands its verified successor
			// before the following GET can return a consistent image.
			entry := &CommitEntry{Path: b.Path, Inode: ino, MutationSeq: fs.markDirtySize(ino, 7), BaseRev: parent.rev, PayloadBaseRev: parent.rev, PayloadBaseRevSet: true, Size: 7, Kind: PendingOverwrite, SnapshotID: generateMountID(), ParentSnapshotID: parent.snapshotID, liveLineageProof: true, liveAncestors: snapshotAncestors(parent.snapshotID, parent.ancestors)}
			entry.bindPayload([]byte("baseABD"))
			if err := fs.commitQueue.CommitNow(t.Context(), entry); err != nil {
				t.Error(err)
			}
		}
		w.Header().Set("X-Dat9-Resource-ID", "shared-hardlink-resource")
		w.Header().Set("X-Dat9-Nlink", "2")
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/v1/fs" + a.Path
		clone.URL = &urlCopy
		server.serveHTTP(w, clone)
	}))
	t.Cleanup(ts.Close)
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.commitQueue.client = fs.client
	issue1023ShadowAppend(t, fs, ino, ids[0], "C")
	if !advanced.Load() {
		t.Fatal("split snapshot read window not exercised")
	}
	if got := pr939HandleBytes(a); got != "baseABDC" {
		t.Errorf("append image=%q", got)
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); status != gofuse.OK {
		t.Fatal(status)
	}
	if _, data, _ := server.snapshot(); string(data) != "baseABDC" {
		t.Errorf("server=%q", data)
	}
}
