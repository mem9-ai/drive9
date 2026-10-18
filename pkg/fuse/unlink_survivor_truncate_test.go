package fuse

import (
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestUnlinkSurvivorInheritanceIdentity(t *testing.T) {
	for _, registered := range []bool{false, true} {
		for _, mismatch := range []string{"none", "inode", "path"} {
			t.Run(fmt.Sprintf("registered=%t/%s", registered, mismatch), func(t *testing.T) {
				fs := &Dat9FS{openHandles: NewOpenHandleIndex()}
				fh := &FileHandle{Ino: 2, Path: "/old"}
				event := &ftruncateInheritance{ino: 2, path: "/old", view: 7, seq: 9, revision: 4,
					id: "snapshot", data: []byte("hello"), complete: true, ancestors: []string{"parent"}}
				switch mismatch {
				case "inode":
					event.ino++
				case "path":
					event.path = "/other"
				}
				fh.pendingFtruncate.Store(event)
				if registered {
					fs.openHandles.Add(fh)
				}
				before := *event
				fh.Lock()
				fs.openHandles.mu.Lock()
				fs.moveSurvivorBindingIndexLocked(fh, "/survivor")
				fs.openHandles.mu.Unlock()
				fh.Unlock()
				got := fh.pendingFtruncate.Load()
				if mismatch == "none" {
					want := before
					want.path = "/survivor"
					if got == event || !reflect.DeepEqual(*got, want) {
						t.Fatal("handoff did not preserve the immutable event's identity and generation")
					}
				} else if got != event {
					t.Fatal("handoff reauthorized a mismatched truncate event")
				}
				if !reflect.DeepEqual(*event, before) {
					t.Fatal("handoff mutated a previously published event")
				}
			})
		}
	}
}

func TestFtruncateUnlinkSurvivorMutation(t *testing.T) {
	for _, mode := range []string{"no-truncate", "truncate-original-fd", "truncate-survivor-fd"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			var deleted atomic.Bool
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/v1/fs/file.bin" {
					return false
				}
				if r.Method == http.MethodDelete {
					deleted.Store(true)
					remote.mu.Lock()
					remote.nlink = 1
					remote.mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
					return true
				}
				if deleted.Load() {
					http.NotFound(w, r)
					return true
				}
				return false
			})
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			original, _ := fs.fileHandles.Get(a)
			survivor, _ := fs.fileHandles.Get(b)
			t.Cleanup(func() {
				for id, h := range map[uint64]*FileHandle{a: original, b: survivor} {
					h.Lock()
					fs.releaseHandleRemoteCommitPathLocked(h)
					h.Unlock()
					fs.releaseHandleShadowPin(h)
					fs.deleteFileHandle(id, h)
				}
			})
			if original.Path != "/file.bin" || survivor.Path != "/alias.bin" || original.Ino != survivor.Ino {
				t.Fatal("two pre-existing hardlink bindings prerequisite failed")
			}
			want := "hello world"
			if mode != "no-truncate" {
				truncateHandoffTruncate(t, fs, ino, a, 5)
				want = "hello"
			}
			hardlinkSync(t, fs, ino, a)
			before, revision, _ := remote.snapshot()
			if string(before) != want || (mode != "no-truncate" && revision != 2) {
				t.Fatal("committed truncate prerequisite failed")
			}
			if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file.bin"); st != gofuse.OK {
				t.Fatal("production Unlink failed", st)
			}
			if !deleted.Load() || original.Path != "/alias.bin" || original.Unlinked || survivor.Unlinked {
				t.Fatal("verified survivor handoff prerequisite failed")
			}
			if _, ok := fs.inodes.GetInode("/file.bin"); ok {
				t.Fatal("removed alias remains indexed")
			}
			for _, h := range []*FileHandle{original, survivor} {
				h.Lock()
				eventPath := "<nil>"
				if event := h.pendingFtruncate.Load(); event != nil {
					eventPath = event.path
				}
				t.Logf("after-unlink handle=%s event=%s ino=%d base=%d dirty=%d", h.Path, eventPath, h.Ino, h.BaseRev, h.DirtySeq)
				h.Unlock()
			}
			target := a
			if mode == "truncate-survivor-fd" {
				target = b
			}
			for attempt := 1; attempt <= 3; attempt++ {
				n, status := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: target, Offset: 0}, []byte("H"))
				t.Logf("write attempt=%d n=%d status=%v", attempt, n, status)
				if n != 1 || status != gofuse.OK {
					t.Errorf("valid surviving fd must remain writable: attempt=%d n=%d status=%v", attempt, n, status)
					data, rev, _ := remote.snapshot()
					if string(data) != want || rev != revision {
						t.Fatal("rejected write changed remote content")
					}
					continue
				}
				hardlinkSync(t, fs, ino, target)
				data, _, _ := remote.snapshot()
				if string(data) != "H"+want[1:] {
					t.Fatalf("surviving write lost truncate image: %q", data)
				}
			}
			if _, err := fs.client.StatCtx(t.Context(), "/file.bin"); err == nil {
				t.Fatal("removed path was resurrected")
			}
		})
	}
}
