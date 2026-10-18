//go:build failpoint

package fuse

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestRetiredFtruncateReadRevalidates(t *testing.T) {
	for _, mode := range []string{"view", "event", "identity", "path", "buffer", "version", "commit", "shadow", "pending", "sse"} {
		t.Run(mode, func(t *testing.T) {
			target := 9 << 20
			if mode == "shadow" || mode == "pending" {
				target = 2 << 20
			}
			fs, ino, _ := truncateHandoffFS(t, bytes.Repeat([]byte("p"), target+4096))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, target)
			hardlinkSync(t, fs, ino, a)
			hardlinkWrite(t, fs, ino, b, "H")
			h, _ := fs.fileHandles.Get(b)
			wb := h.Dirty
			offset := int64(DefaultPartSize) - 2
			if target == 2<<20 {
				offset = 0
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
					t.Fatal(st)
				}
			} else if wb.IsPartLoaded(1) {
				t.Fatal("prerequisite: second part is already loaded")
			}
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/retiredFtruncateReadBeforeValidate"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, reader *FileHandle) {
				if observed != fs || reader != h {
					return
				}
				reached = true
				// Synthetic invalidations exercise the actual return boundary;
				// ordinary same-handle changes are serialized by fh.mu.
				switch mode {
				case "sse":
					(&SSEWatcher{fs: fs, actor: "local"}).handleEvent(&client.ChangeEvent{Path: h.Path, Op: "write", Actor: "external"}, nil)
				case "view":
					fs.mountViewGeneration.Add(1)
				case "event":
					h.pendingFtruncate.Store(&ftruncateInheritance{ino: ino, path: h.Path, seq: h.DirtySeq + 1})
				case "identity":
					fs.inodes.SetIdentity(ino, "replacement", 1)
				case "path":
					fs.inodes.RemoveLinkPreserve(h.Path)
				case "buffer":
					h.Dirty = fs.newWriteBuffer(h.Path, maxPreloadSize, 0)
				case "version":
					wb.contentVersion++
				case "commit":
					fs.recordCommittedMutationQualified(ino, h.DirtySeq+1, 3, int64(target), true)
				case "shadow":
					if err := fs.shadowStore.WriteFull(h.Path, []byte("OTHER"), 1); err != nil {
						t.Error(err)
					}
				case "pending":
					if err := fs.writeBack.PutWithBaseRev(h.Path, []byte("OTHER"), 5, PendingOverwrite, 2); err != nil {
						t.Error(err)
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, status := truncateHandoffRead(fs, ino, b, offset, 6)
			_ = failpoint.Disable(point)
			if !reached || status != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("reached=%t read=%q/%v, want no bytes/EAGAIN", reached, got, status)
			}
			if target > int(DefaultPartSize) && wb.IsPartLoaded(1) {
				t.Fatal("invalidated read retained a provisional part")
			}
			var own [1]byte
			if wb.ReadAt(0, own[:]) != 1 || own[0] != 'H' {
				t.Fatal("read cleanup discarded the accepted own byte")
			}
		})
	}
}

func TestRetiredFtruncateAckPreservesSuccessorStaging(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 2<<20)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	h, _ := fs.fileHandles.Get(b)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	var once sync.Once
	point := "github.com/mem9-ai/drive9/pkg/fuse/retiredFtruncateUploadBeforeRelock"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, owner *FileHandle) {
		if observed == fs && owner == h {
			once.Do(func() { close(entered); <-resume })
		}
	}); err != nil {
		t.Fatal(err)
	}
	result := make(chan gofuse.Status, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	}()
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("paused upload did not finish")
		}
		_ = failpoint.Disable(point)
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("upload did not reach the post-ACK boundary")
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
		t.Fatalf("successor Write=%d/%v", n, st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	h.Lock()
	gens, successor := fs.captureHandleStagingGensLocked(h), h.DirtySeq
	h.Unlock()
	if gens.ShadowGen == 0 || gens.PendingIndexGen == 0 || gens.WriteBackGen == 0 {
		t.Fatal("successor staging prerequisite was not established")
	}
	unblock()
	if st := <-result; st != gofuse.OK {
		t.Fatal(st)
	}
	h.Lock()
	kept := h.DirtySeq == successor && fs.captureHandleStagingGensLocked(h) == gens
	h.Unlock()
	cached, exists := fs.writeBack.GetMeta(h.Path)
	if !kept || !exists || cached.Generation != gens.WriteBackGen || fs.shadowStore.ActiveGeneration(h.Path) != gens.ShadowGen || fs.pendingIndex.Generation(h.Path) != gens.PendingIndexGen {
		t.Fatal("older ACK removed successor staging or its claim")
	}
	retiredReadWant(t, fs, ino, b, "HJppp")
	hardlinkSync(t, fs, ino, b)
	if data, _, _ := remote.snapshot(); string(data[:5]) != "HJppp" {
		t.Fatal("successor content did not commit")
	}
}

func TestRetiredFtruncateWriteSyncRollbackProof(t *testing.T) {
	for _, compat := range []bool{false, true} {
		t.Run(fmt.Sprint(compat), func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
			fs.opts.GVisorCompat = compat
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR|syscall.O_SYNC)
			truncateHandoffTruncate(t, fs, ino, a, 2<<20)
			hardlinkSync(t, fs, ino, a)
			var modeFails atomic.Bool
			modeFails.Store(true)
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
					if modeFails.Load() {
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
			var uploaded *retiredReadPublication
			point := "github.com/mem9-ai/drive9/pkg/fuse/retiredFtruncatePublicationCaptured"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, owner *FileHandle, p *retiredReadPublication) {
				if observed == fs && owner == h {
					uploaded = p
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H"))
			_ = failpoint.Disable(point)
			if n != 0 || st == gofuse.OK || uploaded == nil {
				t.Fatalf("write-sync failure=%d/%v, captured=%t", n, st, uploaded != nil)
			}
			if data, revision, _ := remote.snapshot(); revision != 3 || string(data[:5]) != "Hpppp" {
				t.Fatal("actual data ACK prerequisite missing")
			}
			h.Lock()
			borrowed := ownsRetiredReadPublication(h, uploaded)
			h.Unlock()
			if borrowed {
				t.Fatal("rollback/committed reset retained the failed write's epoch authority")
			}
			// Duplicate late completion cannot attach itself to restored bytes.
			fs.recordRetiredReadAck(uploaded, 99, true)
			retiredReadWant(t, fs, ino, b, "Hpppp")
			modeFails.Store(false)
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
				t.Fatalf("recovery Write=%d/%v", n, st)
			}
			retiredReadWant(t, fs, ino, b, "HJppp")
		})
	}
}
