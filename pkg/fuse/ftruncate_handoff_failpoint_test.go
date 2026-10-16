//go:build failpoint

package fuse

import (
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestFtruncateRetirementRevalidates(t *testing.T) {
	for _, mode := range []string{"event", "view", "path", "identity", "pending", "commit"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			writer := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			readerID := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
			truncateHandoffTruncate(t, fs, ino, writer, 5)
			hardlinkSync(t, fs, ino, writer)
			_, rev, _ := remote.snapshot()
			if _, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("world"), rev); err != nil {
				t.Fatal(err)
			}
			watcher := &SSEWatcher{fs: fs, actor: "local"}
			watcher.handleChange(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"})
			reader, _ := fs.fileHandles.Get(readerID)
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateResetVerified"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
				if observed != fs || fh != reader {
					return
				}
				reached = true
				switch mode {
				case "event":
					event := *reader.pendingFtruncate.Load()
					event.seq++
					reader.pendingFtruncate.Store(&event)
				case "view":
					fs.mountViewGeneration.Add(1)
				case "path":
					fs.inodes.RemoveLinkPreserve("/file.bin")
					fs.inodes.Lookup("/file.bin", false, 11, time.Now())
				case "identity":
					fs.inodes.SetIdentity(ino, "replacement", 1)
				case "pending":
					if _, err := fs.pendingIndex.PutWithBaseRev("/file.bin", 11, PendingOverwrite, 3); err != nil {
						t.Error(err)
					}
				case "commit":
					state := stateOf(fs, ino)
					fs.recordCommittedMutation(ino, state.committedSeq+1, 3, 5)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st := truncateHandoffRead(fs, ino, readerID, 0, 64)
			_ = failpoint.Disable(point)
			if !reached || st != gofuse.EAGAIN || len(got) != 0 || reader.pendingFtruncate.Load() == nil {
				t.Fatalf("reached=%t read=%q/%v observation=%v", reached, got, st, reader.pendingFtruncate.Load())
			}
			unlock, ok := fs.tryLockRemoteCommitPath("/file.bin")
			if !ok {
				t.Fatal("retirement failure leaked alias fence")
			}
			unlock()
		})
	}
}

func TestFtruncateIncompleteRootSelectionRevalidates(t *testing.T) {
	for _, mode := range []string{"event", "view", "path", "shadow", "metadata", "commit"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, readerID := newIncompleteStagedFtruncateFS(t)
			reader, _ := fs.fileHandles.Get(readerID)
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasReadSelected"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle, origin string) {
				if observed != fs || fh != reader {
					return
				}
				reached = true
				if origin != "staged" {
					t.Errorf("selected %s, want staged", origin)
				}
				switch mode {
				case "event":
					event := *reader.pendingFtruncate.Load()
					event.seq++
					reader.pendingFtruncate.Store(&event)
				case "view":
					fs.mountViewGeneration.Add(1)
				case "path":
					fs.inodes.RemoveLinkPreserve("/file.bin")
					fs.inodes.Lookup("/file.bin", false, 11, time.Now())
				case "shadow":
					if err := fs.shadowStore.WriteFull("/file.bin", []byte("replacement"), 1); err != nil {
						t.Error(err)
					}
				case "metadata":
					if _, err := fs.pendingIndex.PutWithBaseRev("/file.bin", 2<<20, PendingOverwrite, 1); err != nil {
						t.Error(err)
					}
				case "commit":
					fs.recordCommittedMutation(ino, 1000, 2, 2<<20)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st := truncateHandoffRead(fs, ino, readerID, (2<<20)-17, 64)
			_ = failpoint.Disable(point)
			if !reached || st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("reached=%t read=%d/%v, want no bytes/EAGAIN", reached, len(got), st)
			}
			unlock, ok := fs.tryLockRemoteCommitPath("/file.bin")
			if !ok {
				t.Fatal("selection failure leaked alias fence")
			}
			unlock()
		})
	}
}
