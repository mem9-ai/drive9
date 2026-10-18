package fuse

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestUnlinkSurvivorLockWaitStopsWithoutMutation(t *testing.T) {
	for _, stop := range []string{"deadline", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			fs, ino, reader, _, writer, _, _ := newAliasAppendTestFS(t, uint32(syscall.O_RDWR), false)
			before, _ := fs.inodes.GetEntry(ino)
			buffer, seq, revision := writer.Dirty, writer.DirtySeq, writer.BaseRev
			oldTimeout := unlinkHandleSetLockTimeout
			unlinkHandleSetLockTimeout = 30 * time.Millisecond
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			var unlock func()
			attempts := 0
			testHookBeforeUnlinkSurvivorBind = func(string) {
				writer.Lock()
				unlock = sync.OnceFunc(writer.Unlock)
			}
			testHookAfterUnlinkHandleSetLockAttempt = func() {
				attempts++
				if stop == "cancel" {
					cancel()
				}
			}
			t.Cleanup(func() {
				if unlock != nil {
					unlock()
				}
				testHookBeforeUnlinkSurvivorBind = nil
				testHookAfterUnlinkHandleSetLockAttempt = nil
				unlinkHandleSetLockTimeout = oldTimeout
			})
			survivor, status := fs.prepareUnlinkSurvivor(ctx, "/b")
			if survivor != "" || status != gofuse.EIO || attempts == 0 {
				t.Fatalf("unconfirmed handoff=%q/%v attempts=%d", survivor, status, attempts)
			}
			if writer.Dirty != buffer || writer.DirtySeq != seq || writer.BaseRev != revision || writer.Path != "/b" || writer.Unlinked {
				t.Fatal("failed lock acquisition changed the writer")
			}
			if !reader.TryLock() {
				t.Fatal("partial handle set remained locked")
			}
			reader.Unlock()
			after, _ := fs.inodes.GetEntry(ino)
			if after.Path != before.Path || after.ResourceID != before.ResourceID {
				t.Fatal("failed handoff published identity")
			}
			for _, path := range []string{"/a", "/b"} {
				if _, err := fs.client.StatCtx(t.Context(), path); err != nil {
					t.Fatalf("failed handoff deleted %s: %v", path, err)
				}
			}
		})
	}
}
