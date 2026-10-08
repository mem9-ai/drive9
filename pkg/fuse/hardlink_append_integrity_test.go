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
	const original = "/hardlink-original"
	const alias = "/hardlink-alias"
	fs, ino, server, closeOld := reviewR2FS(t, original, interactive)
	cleanupKernelCacheBypassAfterTest(t, fs)
	closeOld()
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
	fs.client.SetSmallFileThresholdForTests(1 << 20)
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
