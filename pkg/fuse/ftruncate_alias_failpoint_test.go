//go:build failpoint

package fuse

import (
	"context"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestHardlinkAliasRevalidation(t *testing.T) {
	for _, mode := range []string{"shadow-generation", "metadata-generation", "alias-replaced", "committed-state"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, _, b, c, _, _ := newHardlinkReadFS(t)
			hardlinkWrite(t, fs, ino, b, "H")
			fs.commitQueue.DrainAll()
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasReadSelected"
			if e := failpoint.EnableCall(point, func(observed *Dat9FS, _ *FileHandle, source string) {
				if observed != fs {
					return
				}
				if source != "staged" {
					t.Errorf("source=%s want staged", source)
				}
				switch mode {
				case "shadow-generation":
					if err := fs.shadowStore.WriteFull("/alias.bin", []byte("other"), 2); err != nil {
						t.Error(err)
					}
				case "metadata-generation":
					if _, err := fs.pendingIndex.PutWithBaseRev("/alias.bin", 5, PendingOverwrite, 2); err != nil {
						t.Error(err)
					}
				case "alias-replaced":
					fs.inodes.RemoveLinkPreserve("/file.bin")
					fs.inodes.Lookup("/file.bin", false, 3, time.Now())
				case "committed-state":
					state := stateOf(fs, ino)
					fs.recordCommittedMutation(ino, state.committedSeq+1, state.committedRevision+1, 5)
				}
			}); e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, c, 0, 11)
			_ = failpoint.Disable(point)
			if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("invalidated %s leaked %q/%v/%v", mode, got, st, err)
			}
			t.Logf("REVALIDATION %s changed after selection: no stale bytes, EAGAIN", mode)
		})
	}
}
func TestHardlinkAliasRemoteRevisionMismatch(t *testing.T) {
	fs, ino, _, b, c, _, _ := newHardlinkReadFS(t)
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasBeforeRemoteRead"
	if e := failpoint.EnableCall(point, func(observed *Dat9FS, _ *FileHandle) {
		if observed != fs {
			return
		}
		state := stateOf(fs, ino)
		if _, err := fs.client.WriteCtxConditionalWithRevision(context.Background(), "/alias.bin", []byte("newer"), state.committedRevision); err != nil {
			t.Error(err)
		}
	}); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	got, st, err := readDat9FSTestRange(fs, ino, c, 0, 11)
	_ = failpoint.Disable(point)
	if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
		t.Fatalf("remote mismatch leaked %q/%v/%v", got, st, err)
	}
	t.Log("REMOTE_REVISION changed before fetch: old inode revision not attached to new bytes")
}

func TestHardlinkAliasFenceRechecksMembership(t *testing.T) {
	fs, ino, a, b, _ := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasFenceAcquired"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
		if observed != fs || fh.Ino != ino {
			return
		}
		fs.inodes.RemoveLinkPreserve("/alias.bin")
		fs.inodes.Lookup("/alias.bin", false, 3, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.EAGAIN {
		t.Fatal(st)
	}
	for _, p := range []string{"/file.bin", "/alias.bin"} {
		unlock, ok := fs.tryLockRemoteCommitPath(p)
		if !ok {
			t.Fatalf("failed fence leaked lock %s", p)
		}
		unlock()
	}
}

func TestHardlinkAliasFlushWaitsForChild(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	child, _ := fs.fileHandles.Get(b)
	child.Lock()
	locked := true
	defer func() {
		if locked {
			child.Unlock()
		}
	}()
	waiting := make(chan struct{}, 1)
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasFlushWait"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, _ *FileHandle) {
		if observed == fs {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	done := make(chan gofuse.Status, 1)
	go func() { done <- fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}) }()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("did not yield parent flush")
	}
	child.Unlock()
	locked = false
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatal(st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("flush deadlocked")
	}
	fs.commitQueue.WaitPath("/alias.bin")
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}
