package fuse

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func interceptRetiredReadHTTP(t *testing.T, fs *Dat9FS, intercept func(http.ResponseWriter, *http.Request) bool) {
	t.Helper()
	target, err := url.Parse(fs.client.BaseURL())
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	threshold := fs.client.CachedSmallFileThreshold()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !intercept(w, r) {
			proxy.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	fs.client = newTestClient(server.URL)
	fs.client.SetSmallFileThresholdForTests(threshold)
	if fs.commitQueue != nil {
		fs.commitQueue.client = fs.client
	}
	fs.uploader.client = fs.client
}

func retiredReadWant(t *testing.T, fs *Dat9FS, ino, handle uint64, want string) {
	t.Helper()
	got, status := truncateHandoffRead(fs, ino, handle, 0, len(want))
	if status != gofuse.OK || string(got) != want {
		t.Fatalf("self-read=%q/%v want=%q/OK", got, status, want)
	}
}

func TestRetiredFtruncateSelfRead(t *testing.T) {
	for _, linked := range []bool{false, true} {
		for _, queued := range []bool{false, true} {
			for _, mode := range []string{"direct", "cache", "interactive"} {
				for _, closeSource := range []bool{false, true} {
					t.Run(fmt.Sprintf("hardlink=%t/queue=%t/%s/close=%t", linked, queued, mode, closeSource), func(t *testing.T) {
						initial := bytes.Repeat([]byte("p"), (2<<20)+4096)
						fs, ino, remote := truncateHandoffFS(t, initial)
						queue := fs.commitQueue
						if !queued {
							fs.commitQueue = nil
							t.Cleanup(func() { fs.commitQueue = queue })
						}
						a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
						if linked {
							addTruncateHandoffAlias(t, fs, ino, remote)
						}
						b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
						t.Cleanup(func() {
							if a != 0 {
								fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
							}
							fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
						})
						truncateHandoffTruncate(t, fs, ino, a, 2<<20)
						hardlinkSync(t, fs, ino, a)
						hardlinkWrite(t, fs, ino, b, "H")
						retiredReadWant(t, fs, ino, b, "Hpppp")
						writer, _ := fs.fileHandles.Get(b)
						if mode == "cache" {
							if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
								t.Fatalf("nonfinal Flush=%v", st)
							}
							if writer.WriteBackSeq == 0 {
								t.Fatal("cache Fsync prerequisite: no owner snapshot")
							}
							retiredReadWant(t, fs, ino, b, "Hpppp")
						}
						if mode == "interactive" {
							gate := make(chan struct{})
							unblock := sync.OnceFunc(func() { close(gate) })
							t.Cleanup(unblock)
							if queued {
								queue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
							}
							fs.syncMode = SyncInteractive
							hardlinkSync(t, fs, ino, b)
							if queued && (writer.DirtySeq != 0 || !queue.HasPath(writer.Path)) {
								t.Fatal("interactive prerequisite: expected clean live owner and queued publication")
							}
							retiredReadWant(t, fs, ino, b, "Hpppp")
							if closeSource {
								fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
								a = 0
							}
							unblock()
							if queued {
								queue.WaitPath(writer.Path)
							}
							fs.syncMode = SyncStrict
						} else if closeSource {
							fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
							a = 0
						}
						hardlinkSync(t, fs, ino, b)
						retiredReadWant(t, fs, ino, b, "Hpppp")
						if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
							t.Fatalf("second write=%d/%v", n, st)
						}
						retiredReadWant(t, fs, ino, b, "HJppp")
						hardlinkSync(t, fs, ino, b)
						want := bytes.Clone(initial[:2<<20])
						copy(want, "HJ")
						if got, _, _ := remote.snapshot(); !bytes.Equal(got, want) {
							t.Fatalf("committed image differs, size=%d", len(got))
						}
					})
				}
			}
		}
	}
}

func TestRetiredFtruncateReceiptSurvivesLaterAliasCommit(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	addTruncateHandoffAlias(t, fs, ino, remote)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	c := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	writer, _ := fs.fileHandles.Get(b)
	ownSeq := writer.DirtySeq
	gate := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(unblock)
	fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
	fs.syncMode = SyncInteractive
	hardlinkSync(t, fs, ino, b)
	writer.Lock() // Force the successful-upload callback to miss the live owner.
	unblock()
	fs.commitQueue.WaitPath(writer.Path)
	writer.Unlock()
	fs.syncMode = SyncStrict
	hardlinkWrite(t, fs, ino, c, "C")
	writer.Lock() // Do not let C's clean-sibling refresh hide a lost B receipt.
	status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c})
	writer.Unlock()
	if status != gofuse.OK {
		t.Fatalf("later alias Fsync=%v", status)
	}
	fs.dirtyMu.Lock()
	newSeq := fs.mutationInodes[ino].committedSeq
	fs.dirtyMu.Unlock()
	if newSeq <= ownSeq {
		t.Fatal("prerequisite: later alias did not replace the latest inode ledger")
	}
	retiredReadWant(t, fs, ino, b, "Cpppp")
	for _, id := range []uint64{a, b, c} {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	}
}

func TestRetiredFtruncateActualRemoteBaseline(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	addTruncateHandoffAlias(t, fs, ino, remote)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	external := bytes.Repeat([]byte("q"), (2<<20)+8192)
	if _, err := fs.client.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", external, 2); err != nil {
		t.Fatal(err)
	}
	hardlinkWrite(t, fs, ino, b, "H")
	retiredReadWant(t, fs, ino, b, "Hqqqq")
	hardlinkSync(t, fs, ino, b)
	external[0] = 'H'
	if got, rev, _ := remote.snapshot(); rev != 4 || !bytes.Equal(got, external) {
		t.Fatalf("external baseline commit: size=%d revision=%d", len(got), rev)
	}
}

func TestRetiredFtruncateLoadedAndLazyReads(t *testing.T) {
	for _, target := range []int{2 << 20, 9 << 20} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			fs, ino, _ := truncateHandoffFS(t, bytes.Repeat([]byte("p"), target+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, target)
			hardlinkSync(t, fs, ino, a)
			hardlinkWrite(t, fs, ino, b, "H")
			var blocked atomic.Bool
			var requests atomic.Int32
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodGet || r.Method == http.MethodHead {
					requests.Add(1)
					if blocked.Load() {
						w.WriteHeader(http.StatusServiceUnavailable)
						return true
					}
				}
				return false
			})
			blocked.Store(true)
			retiredReadWant(t, fs, ino, b, "Hpppp")
			if requests.Load() != 0 {
				t.Fatal("loaded self-read attempted remote IO")
			}
			blocked.Store(false)
			if target > int(DefaultPartSize) {
				h, _ := fs.fileHandles.Get(b)
				if h.Dirty.PartSize() != DefaultPartSize || h.Dirty.IsPartLoaded(1) {
					t.Fatal("prerequisite: naturally unloaded second part required")
				}
				got, status := truncateHandoffRead(fs, ino, b, DefaultPartSize-2, 6)
				if status != gofuse.OK || string(got) != "pppppp" || requests.Load() == 0 {
					t.Fatalf("cross-part lazy read=%q/%v requests=%d", got, status, requests.Load())
				}
			}
			hardlinkSync(t, fs, ino, b)
			requests.Store(0)
			blocked.Store(true)
			retiredReadWant(t, fs, ino, b, "Hpppp")
			if requests.Load() != 0 {
				t.Fatal("own ACK discarded loaded bytes")
			}
			blocked.Store(false)
		})
	}
}

func TestRetiredFtruncateLazyRejectsChangedRemote(t *testing.T) {
	for _, identity := range []bool{false, true} {
		t.Run(fmt.Sprint(identity), func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (9<<20)+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 9<<20)
			hardlinkSync(t, fs, ino, a)
			hardlinkWrite(t, fs, ino, b, "H")
			h, _ := fs.fileHandles.Get(b)
			remote.mu.Lock()
			if identity {
				remote.resource = "replacement"
			} else {
				remote.revision++
			}
			remote.mu.Unlock()
			got, status := truncateHandoffRead(fs, ino, b, DefaultPartSize, 5)
			if status != gofuse.EAGAIN || len(got) != 0 || h.Dirty.IsPartLoaded(1) {
				t.Fatalf("unproven lazy read=%q/%v loaded=%t", got, status, h.Dirty.IsPartLoaded(1))
			}
			var own [1]byte
			if h.Dirty.ReadAt(0, own[:]) != 1 || own[0] != 'H' {
				t.Fatal("invalid lazy read discarded accepted own byte")
			}
		})
	}
}

func TestRetiredFtruncateCacheAckBeforeChmodError(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	var failMode atomic.Bool
	failMode.Store(true)
	interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
			if failMode.Load() {
				w.WriteHeader(http.StatusForbidden)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return true
		}
		return false
	})
	h, _ := fs.fileHandles.Get(b)
	h.Lock()
	fs.setPendingModeLocked(h, 0644, fs.nextPendingModeGen())
	h.Unlock()
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st == gofuse.OK {
		t.Fatal("metadata failure unexpectedly succeeded")
	}
	if got, rev, _ := remote.snapshot(); rev != 3 || string(got[:5]) != "Hpppp" {
		t.Fatal("prerequisite: data ACK did not precede metadata error")
	}
	retiredReadWant(t, fs, ino, b, "Hpppp")
	failMode.Store(false)
	hardlinkSync(t, fs, ino, b)
	if _, rev, _ := remote.snapshot(); rev != 3 {
		t.Fatal("metadata retry uploaded the data again")
	}
	hardlinkWrite(t, fs, ino, b, "J")
	retiredReadWant(t, fs, ino, b, "Jpppp")
	hardlinkSync(t, fs, ino, b)
}

func TestRetiredFtruncateMultipartReceipt(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	remote.mu.Lock()
	remote.inlineThreshold = 50_000
	remote.mu.Unlock()
	fs.client.SetSmallFileThresholdForTests(50_000)
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	var blocked atomic.Bool
	var requests atomic.Int32
	interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if blocked.Load() && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			requests.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	gate := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(unblock)
	fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
	fs.syncMode = SyncInteractive
	hardlinkSync(t, fs, ino, b)
	h, _ := fs.fileHandles.Get(b)
	h.Lock()
	p := h.Dirty.retiredRead.publications[len(h.Dirty.retiredRead.publications)-1]
	unblock()
	fs.commitQueue.WaitPath(h.Path)
	h.Unlock()
	fs.dirtyMu.Lock()
	ack := p.ack
	fs.dirtyMu.Unlock()
	if ack == nil || ack.actual || ack.revision != 3 {
		t.Fatalf("multipart prerequisite: predicted revision receipt=%+v", ack)
	}
	blocked.Store(true)
	retiredReadWant(t, fs, ino, b, "Hpppp")
	if requests.Load() != 0 {
		t.Fatal("completion of an own multipart image added HTTP to loaded self-read")
	}
	blocked.Store(false)
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
		t.Fatalf("write after verified multipart baseline=%d/%v", n, st)
	}
	retiredReadWant(t, fs, ino, b, "HJppp")
	fs.syncMode = SyncStrict
	hardlinkSync(t, fs, ino, b)
	if data, rev, _ := remote.snapshot(); rev != 4 || string(data[:5]) != "HJppp" {
		t.Fatalf("multipart successor: revision=%d prefix=%q", rev, data[:min(5, len(data))])
	}
}

func TestRetiredFtruncateMultiplePublications(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	// Hold the first job before it can send data. The existing bounded write
	// admission cancels it; the second full publication must cover both writes.
	fs.opts.RemoteCommitWaitTimeout = 20 * time.Millisecond
	firstJob, allowFirst := make(chan struct{}), make(chan struct{})
	secondJob, allowSecond := make(chan struct{}), make(chan struct{})
	unblockFirst := sync.OnceFunc(func() { close(allowFirst) })
	unblockSecond := sync.OnceFunc(func() { close(allowSecond) })
	var jobs atomic.Int32
	fs.commitQueue.PathLock = func(path string) func() {
		switch jobs.Add(1) {
		case 1:
			close(firstJob)
			<-allowFirst
		case 2:
			close(secondJob)
			<-allowSecond
		}
		return fs.lockRemoteCommitPath(path)
	}
	h, _ := fs.fileHandles.Get(b)
	t.Cleanup(func() {
		unblockFirst()
		unblockSecond()
		fs.commitQueue.WaitPath(h.Path)
	})
	fs.syncMode = SyncInteractive
	hardlinkSync(t, fs, ino, b)
	select {
	case <-firstJob:
	case <-time.After(10 * time.Second):
		t.Fatal("first queued payload did not reach its barrier")
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
		t.Fatalf("write while own publication uploads=%d/%v", n, st)
	}
	hardlinkSync(t, fs, ino, b)
	if got, st := truncateHandoffRead(fs, ino, b, 0, 5); st != gofuse.OK || string(got) != "HJppp" {
		t.Errorf("two pending own publications read=%q/%v", got, st)
	}
	unblockFirst()
	select {
	case <-secondJob:
	case <-time.After(10 * time.Second):
		t.Fatal("second publication did not reach its queue barrier")
	}
	if data, rev, _ := remote.snapshot(); rev != 2 || string(data[:5]) != "ppppp" {
		t.Fatal("prerequisite: canceled first publication must not have committed")
	}
	if got, st := truncateHandoffRead(fs, ino, b, 0, 5); st != gofuse.OK || string(got) != "HJppp" {
		t.Errorf("parent cancellation invalidated the later publication: %q/%v", got, st)
	}
	unblockSecond()
	fs.commitQueue.WaitPath(h.Path)
	if data, rev, _ := remote.snapshot(); rev != 3 || string(data[:5]) != "HJppp" {
		t.Fatalf("second full publication must cover both writes: revision=%d prefix=%q", rev, data[:min(5, len(data))])
	}
	// Its own receipt completes the current image despite the canceled ancestor.
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 2}, []byte("K")); n != 1 || st != gofuse.OK {
		t.Fatalf("later own write=%d/%v", n, st)
	}
	if got, st := truncateHandoffRead(fs, ino, b, 0, 5); st != gofuse.OK || string(got) != "HJKpp" {
		t.Errorf("later own image lost an accepted byte: %q/%v", got, st)
	}
	hardlinkSync(t, fs, ino, b)
	fs.commitQueue.WaitPath(h.Path)
	want := bytes.Repeat([]byte("p"), 2<<20)
	copy(want, "HJK")
	if data, _, _ := remote.snapshot(); !bytes.Equal(data, want) {
		t.Fatalf("final committed image lost accepted writes: prefix=%q", data[:min(5, len(data))])
	}
}

func TestRetiredFtruncateAckDoesNotDiscardLaterPublication(t *testing.T) {
	fs, ino, _ := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	h, _ := fs.fileHandles.Get(b)
	// Exercise the publication/ACK boundary directly: ordinary queue admission
	// may cancel P1 when a second write arrives. These immutable sources instead
	// model both successful uploads, with P2 captured before P1's callback.
	capture := func() (*CommitEntry, []byte) {
		t.Helper()
		h.Lock()
		defer h.Unlock()
		entry := &CommitEntry{Path: h.Path, Inode: ino, MutationSeq: h.DirtySeq, Size: h.Dirty.Size(), BaseRev: h.BaseRev}
		fs.bindCommitEntryToHandleLocked(entry, h, h.BaseRev)
		return entry, bytes.Clone(h.Dirty.bytesView())
	}
	p1, data1 := capture()
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
		t.Fatalf("second write=%d/%v", n, st)
	}
	p2, data2 := capture()
	h.Lock()
	h.Dirty.ClearDirty()
	fs.clearDirtySize(ino, h.DirtySeq)
	h.DirtySeq = 0 // The established queued-image handoff.
	h.Unlock()
	ack := func(entry *CommitEntry, data []byte, base int64) {
		t.Helper()
		rev, err := fs.client.WriteCtxConditionalWithRevision(context.Background(), fs.remotePath(entry.Path), data, base)
		if err != nil {
			t.Fatal(err)
		}
		entry.readActualRev = rev
		fs.onCommitQueueUploaded(entry, rev)
	}
	ack(p1, data1, 2)
	retiredReadWant(t, fs, ino, b, "HJppp")
	h.Lock()
	kept := ownsRetiredReadPublication(h, p2.retiredRead) && fs.retiredReadPending(h)
	h.Unlock()
	if !kept {
		t.Fatal("P1 ACK discarded P2 or incorrectly completed its obligation")
	}
	ack(p2, data2, 3)
	retiredReadWant(t, fs, ino, b, "HJppp")
	h.Lock()
	pending := fs.retiredReadPending(h)
	base := h.BaseRev
	h.Unlock()
	if pending || base != 4 {
		t.Fatalf("P2 ACK did not complete the current image: pending=%t base=%d", pending, base)
	}
}

func TestRetiredFtruncateCleanRebindRequiresHeadBeforeWrite(t *testing.T) {
	fs, ino, _ := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	h, _ := fs.fileHandles.Get(b)
	h.Lock()
	fs.rebindCleanWriteBufferToRemoteLocked(h, 9<<20) // A caller's enlarged local size is not a remote proof.
	before := h.Dirty
	h.Unlock()
	var unavailable atomic.Bool
	unavailable.Store(true)
	interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if unavailable.Load() && r.Method == http.MethodHead {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	write := func() (uint32, gofuse.Status) {
		return fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J"))
	}
	if n, st := write(); n != 0 || st == gofuse.OK {
		t.Fatalf("unverified rebind accepted bytes: %d/%v", n, st)
	}
	if h.Dirty != before || h.Dirty.HasDirtyParts() || h.DirtySeq != 0 {
		t.Fatal("failed verification mutated the buffer")
	}
	unavailable.Store(false)
	if n, st := write(); n != 1 || st != gofuse.OK {
		t.Fatalf("verified retry=%d/%v", n, st)
	}
	retiredReadWant(t, fs, ino, b, "HJppp")
	if got, st := truncateHandoffRead(fs, ino, b, 2<<20, 1); st != gofuse.OK || len(got) != 0 {
		t.Fatalf("rebind trusted a local enlarged EOF: %q/%v", got, st)
	}
}

func TestRetiredFtruncateSubsequentTruncates(t *testing.T) {
	for _, pathname := range []bool{false, true} {
		t.Run(fmt.Sprint(pathname), func(t *testing.T) {
			fs, ino, _ := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 2<<20)
			hardlinkSync(t, fs, ino, a)
			hardlinkWrite(t, fs, ino, b, "H")
			hardlinkSync(t, fs, ino, b)
			truncate := func(size uint64) {
				t.Helper()
				valid := uint32(gofuse.FATTR_SIZE)
				if !pathname {
					valid |= gofuse.FATTR_FH
				}
				input := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					InHeader: gofuse.InHeader{NodeId: ino}, Valid: valid, Fh: b, Size: size,
				}}
				if status := fs.SetAttr(nil, input, &gofuse.AttrOut{}); status != gofuse.OK {
					t.Fatal(status)
				}
			}
			truncate(5)
			retiredReadWant(t, fs, ino, b, "Hpppp")
			truncate(9)
			retiredReadWant(t, fs, ino, b, "Hpppp\x00\x00\x00\x00")
			hardlinkWrite(t, fs, ino, b, "J")
			retiredReadWant(t, fs, ino, b, "Jpppp\x00\x00\x00\x00")
			hardlinkSync(t, fs, ino, b)
		})
	}
}

func TestRetiredFtruncateUnlinkedOwner(t *testing.T) {
	for _, tc := range []struct {
		size       int
		writeFirst bool
	}{{2 << 20, false}, {9 << 20, false}, {9 << 20, true}} {
		t.Run(fmt.Sprintf("size=%d/writeFirst=%t", tc.size, tc.writeFirst), func(t *testing.T) {
			target := tc.size
			fs, ino, _ := truncateHandoffFS(t, bytes.Repeat([]byte("p"), target+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, target)
			hardlinkSync(t, fs, ino, a)
			hardlinkWrite(t, fs, ino, b, "H")
			h, _ := fs.fileHandles.Get(b)
			if target > int(DefaultPartSize) && h.Dirty.IsPartLoaded(1) {
				t.Fatal("prerequisite: retained part must be naturally unloaded")
			}
			var deleted atomic.Bool
			var afterDelete atomic.Int32
			replacement := bytes.Repeat([]byte("x"), target)
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if deleted.Load() {
					afterDelete.Add(1)
					// The server-side name is reused by a different resource;
					// this anonymous fd must never consult it.
					w.Header().Set("X-Dat9-Resource-ID", "replacement-resource")
					w.Header().Set("X-Dat9-Revision", "1")
					http.ServeContent(w, r, "replacement.bin", time.Unix(1000, 0), bytes.NewReader(replacement))
					return true
				}
				if r.Method == http.MethodDelete {
					deleted.Store(true)
					w.WriteHeader(http.StatusNoContent)
					return true
				}
				return false
			})
			if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file.bin"); st != gofuse.OK {
				t.Fatal(st)
			}
			if !h.Unlinked || !deleted.Load() {
				t.Fatal("prerequisite: last path must be removed with fd still open")
			}
			if target > int(DefaultPartSize) {
				if h.Dirty.IsPartLoaded(1) {
					t.Fatal("prerequisite: unlink must preserve a naturally unloaded part")
				}
				if !tc.writeFirst {
					if got, st := truncateHandoffRead(fs, ino, b, DefaultPartSize-2, 5); st != gofuse.OK || string(got) != "ppppp" {
						t.Errorf("unlinked lazy retained read=%q/%v", got, st)
					}
				}
			}
			for _, offset := range []uint64{1, uint64(target - 1)} {
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: offset}, []byte("J")); n != 1 || st != gofuse.OK {
					t.Errorf("unlinked write offset=%d: %d/%v", offset, n, st)
				}
			}
			if got, st := truncateHandoffRead(fs, ino, b, 0, 5); st != gofuse.OK || string(got) != "HJppp" {
				t.Errorf("unlinked own prefix=%q/%v", got, st)
			}
			if got, st := truncateHandoffRead(fs, ino, b, int64(target)-3, 8); st != gofuse.OK || string(got) != "ppJ" {
				t.Errorf("unlinked own tail/EOF=%q/%v", got, st)
			}
			hardlinkSync(t, fs, ino, b)
			if afterDelete.Load() != 0 {
				t.Fatal("anonymous fd attempted remote path I/O after DELETE")
			}
		})
	}
}
