package fuse

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func settledInvalidationReader(t *testing.T) (*Dat9FS, uint64, uint64, *truncateHandoffRemote) {
	t.Helper()
	fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	_, revision, _ := remote.snapshot()
	if _, err := fs.client.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("world"), revision); err != nil {
		t.Fatal(err)
	}
	(&SSEWatcher{fs: fs, actor: "local"}).handleEvent(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"}, nil)
	retiredReadWant(t, fs, ino, b, "world")
	if truncateHandoffEvent(fs, b) != nil {
		t.Fatal("reader did not retire")
	}
	return fs, ino, b, remote
}

func TestRetiredReadInvalidationAdmission(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		t.Run(fmt.Sprint(dirty), func(t *testing.T) {
			fs, ino, b, remote := settledInvalidationReader(t)
			if dirty {
				hardlinkWrite(t, fs, ino, b, "H")
			}
			_, revision, _ := remote.snapshot()
			if _, err := fs.client.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("earth"), revision); err != nil {
				t.Fatal(err)
			}
			h, _ := fs.fileHandles.Get(b)
			h.Lock() // Notification must be delivered even when TryLock fails.
			(&SSEWatcher{fs: fs, actor: "local"}).handleEvent(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"}, nil)
			h.Unlock()
			var requests atomic.Int32
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				requests.Add(1)
				return false
			})
			if dirty {
				retiredReadWant(t, fs, ino, b, "Horld")
				if requests.Load() != 0 {
					t.Fatal("invalidation made loaded own bytes depend on HTTP")
				}
			} else {
				hardlinkWrite(t, fs, ino, b, "H")
				retiredReadWant(t, fs, ino, b, "Harth")
				hardlinkSync(t, fs, ino, b)
				if data, _, _ := remote.snapshot(); string(data) != "Harth" {
					t.Fatalf("write used stale remote image: %q", data)
				}
			}
		})
	}
}

func TestRetiredReadRepeatedExternalSSE(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		truncated, alias, refresh bool
	}{
		{"no-truncate-same", false, false, false},
		{"truncate-same", true, false, false},
		{"truncate-hardlink", true, true, false},
		{"getattr-same-control", true, false, true},
		{"getattr-hardlink-control", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			if tc.alias {
				addTruncateHandoffAlias(t, fs, ino, remote)
			}
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			if tc.truncated {
				truncateHandoffTruncate(t, fs, ino, a, 5)
				hardlinkSync(t, fs, ino, a)
			}
			external := newTestClient(fs.client.BaseURL())
			external.SetSmallFileThresholdForTests(32 << 20)
			watcher := &SSEWatcher{fs: fs, actor: "local-mount"}
			for cycle, want := range []string{"world", "earth"} {
				_, rev, _ := remote.snapshot()
				actual, err := external.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte(want), rev)
				if err != nil {
					t.Fatal(err)
				}
				data, committed, _ := remote.snapshot()
				if actual != rev+1 || committed != actual || string(data) != want {
					t.Fatal("external PUT prerequisite failed")
				}
				watcher.handleChange(&client.ChangeEvent{Seq: uint64(9 + cycle), Path: "/file.bin", Op: "write", Actor: "other-client"})
				if tc.refresh {
					var out gofuse.AttrOut
					if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
						t.Fatal(st)
					}
				}
				for n := 0; n < 3; n++ {
					got, st := truncateHandoffRead(fs, ino, b, 0, 64)
					if cycle == 0 && (st != gofuse.OK || string(got) != want) {
						t.Fatalf("first-cycle prerequisite %q/%v, want %q", got, st, want)
					}
					if st != gofuse.OK || string(got) != want {
						t.Errorf("cycle=%d attempt=%d got=%q/%v want=%q", cycle+1, n+1, got, st, want)
					}
				}
				if tc.truncated && truncateHandoffEvent(fs, b) != nil {
					t.Fatal("prerequisite: B observation did not retire")
				}
				h, _ := fs.fileHandles.Get(b)
				h.Lock()
				entry, _ := fs.inodes.GetEntry(ino)
				t.Logf("cycle=%d remoteRev=%d inodeRev=%d ownerBase=%d dirtySeq=%d dirty=%t", cycle+1, committed, entry.Revision, h.BaseRev, h.DirtySeq, h.Dirty.HasDirtyParts())
				h.Unlock()
			}
		})
	}
}
