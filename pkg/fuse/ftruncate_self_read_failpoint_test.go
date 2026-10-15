//go:build failpoint

package fuse

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncateInheritedSelfReadDiscardsLazyPartsAfterInvalidation(t *testing.T) {
	for _, mode := range []string{"view", "commit"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkWrite(t, fs, ino, b, "H")
			hardlinkSync(t, fs, ino, b)
			var reads atomic.Int32
			interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodGet {
					return false
				}
				reads.Add(1)
				http.ServeContent(w, r, "file.bin", time.Time{}, strings.NewReader(remote()))
				return true
			})
			h, _ := fs.fileHandles.Get(b)
			h.Lock()
			h.Dirty.partSize = 2
			fs.rebindCleanWriteBufferToRemoteLocked(h, 5)
			h.Unlock()
			hardlinkWrite(t, fs, ino, b, "X")
			if h.Dirty.IsPartLoaded(1) || h.Dirty.IsPartLoaded(2) {
				t.Fatal("fixture did not leave inherited parts lazy")
			}
			before := reads.Load()
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateSelfReadBeforeValidate"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, reader *FileHandle) {
				if observed != fs || reader != h {
					return
				}
				reached = true
				if !h.Dirty.IsPartLoaded(1) || !h.Dirty.IsPartLoaded(2) {
					t.Error("return boundary reached without loading both inherited parts")
				}
				if mode == "view" {
					fs.mountViewGeneration.Add(1)
				} else {
					fs.recordCommittedMutation(ino, h.DirtySeq+1, 3, 5)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, b, 0, 5)
			_ = failpoint.Disable(point)
			if !reached || err != nil || st != gofuse.EAGAIN || len(got) != 0 || reads.Load()-before != 2 {
				t.Fatalf("reached=%t read=%q/%v/%v lazy GETs=%d, want EAGAIN/no bytes/2 GETs", reached, got, st, err, reads.Load()-before)
			}
			if h.Dirty.IsPartLoaded(1) || h.Dirty.IsPartLoaded(2) {
				t.Fatal("invalidated read retained provisional inherited parts")
			}
			var own [1]byte
			if n := h.Dirty.ReadAt(0, own[:]); n != 1 || own[0] != 'X' {
				t.Fatalf("accepted own byte after cleanup = %q/%d, want X/1", own, n)
			}
		})
	}
}

func TestFtruncateInheritedSelfReadRevalidates(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "pending"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			for _, mode := range []string{"view", "event", "commit", "identity", "path", "buffer", "version", "shadow"} {
				t.Run(mode, func(t *testing.T) {
					fs, ino, a, b, _ := newFtruncateCommitFS(t)
					reviewFtruncate(t, fs, ino, a, 5)
					if committed {
						hardlinkSync(t, fs, ino, a)
					}
					hardlinkWrite(t, fs, ino, b, "H")
					h, _ := fs.fileHandles.Get(b)
					if mode == "shadow" {
						h.Lock()
						err := fs.stageShadowLocked(h, false)
						h.Unlock()
						if err != nil {
							t.Fatal(err)
						}
					}
					reached := false
					point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateSelfReadBeforeValidate"
					if err := failpoint.EnableCall(point, func(observed *Dat9FS, reader *FileHandle) {
						if observed != fs || reader != h {
							return
						}
						reached = true
						// These controlled invalidations exercise the actual return boundary.
						// Buffer changes are fault injections; ordinary same-fd mutations are
						// serialized by fh.mu and cannot run concurrently here.
						switch mode {
						case "view":
							fs.mountViewGeneration.Add(1)
						case "event":
							e := *h.pendingFtruncate.Load()
							h.pendingFtruncate.Store(&e)
						case "commit":
							fs.recordCommittedMutation(ino, h.DirtySeq+1, 3, 5)
						case "identity":
							fs.inodes.SetIdentity(ino, "replacement", 1)
						case "path":
							fs.inodes.RemoveLinkPreserve(h.Path)
						case "buffer":
							h.Dirty = fs.newWriteBuffer(h.Path, maxPreloadSize, 0)
						case "version":
							h.Dirty.contentVersion++
						case "shadow":
							if err := fs.shadowStore.WriteFull(h.Path, []byte("OTHER"), 1); err != nil {
								t.Error(err)
							}
						}
					}); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = failpoint.Disable(point) })
					got, st, err := readDat9FSTestRange(fs, ino, b, 0, 5)
					_ = failpoint.Disable(point)
					if !reached || err != nil || st != gofuse.EAGAIN || len(got) != 0 {
						t.Fatalf("reached=%t read=%q/%v/%v", reached, got, st, err)
					}
					if !h.TryLock() {
						t.Fatal("read leaked handle mutex")
					}
					h.Unlock()
					// A stale return must not strand the shadow path lock or transfer staging
					// cleanup rights. A subsequent generation replacement must be possible.
					if err := fs.shadowStore.WriteFull("/file.bin", []byte("retry"), 1); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
