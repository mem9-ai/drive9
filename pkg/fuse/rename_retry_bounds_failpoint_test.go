//go:build failpoint

package fuse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func assertRetryExclusionsReleased(t *testing.T, fs *Dat9FS, oldP, newP, except string) {
	t.Helper()
	for _, p := range []string{oldP, newP} {
		if except == "remote:"+p {
			continue
		}
		fs.remoteCommitMu.Lock()
		lock := fs.remoteCommitLocks[p]
		fs.remoteCommitMu.Unlock()
		if lock != nil {
			if !lock.TryLock() {
				t.Errorf("retry retained remote lock %s", p)
			} else {
				lock.Unlock()
			}
		}
	}
	if fs.uploader != nil {
		fs.uploader.inflightMu.Lock()
		defer fs.uploader.inflightMu.Unlock()
		for _, p := range []string{oldP, newP} {
			if except != "uploader:"+p && fs.uploader.inflight[p] != nil {
				t.Errorf("retry retained uploader exclusion %s", p)
			}
		}
	}
}

func TestOwnedRenameRetryBounds(t *testing.T) {
	for _, mode := range []string{"remote_busy_recovers", "uploader_busy_recovers", "remote_deadline", "uploader_deadline", "capture_change_once", "capture_churn_deadline"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, _, _ := newFtruncateCommitFS(t)
			const oldP, newP = "/old/file.bin", "/new/file.bin"
			fs.finishLocalRename(&gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "/file.bin", oldP)
			reviewFtruncate(t, fs, ino, a, 5)
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
				t.Fatal(st)
			}
			// Isolate acquisition bounds from Flush's intentional handoff fence.
			// The integration cases separately exercise the actual Flush/Release pair.
			fh, _ := fs.fileHandles.Get(a)
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			fh.Unlock()
			fs.uploader = &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/"}
			budget := 300 * time.Millisecond
			if strings.HasSuffix(mode, "deadline") {
				budget = 80 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			deadline, _ := ctx.Deadline()
			captures, retries := 0, 0
			var releaseExternal func()
			except := ""
			enableQueueRenameHook(t, fs, func(observed *Dat9FS, phase string) {
				if observed != fs {
					return
				}
				switch phase {
				case "after_capture":
					captures++
					if got, _ := ctx.Deadline(); got != deadline {
						t.Error("retry reset deadline")
					}
					if captures == 1 {
						if strings.HasPrefix(mode, "remote_") {
							releaseExternal = fs.lockRemoteCommitPath(oldP)
							except = "remote:" + oldP
						}
						if strings.HasPrefix(mode, "uploader_") {
							releaseExternal = fs.uploader.acquirePath(oldP)
							except = "uploader:" + oldP
						}
					}
					if mode == "capture_churn_deadline" || mode == "capture_change_once" && captures == 1 {
						if _, _, err := fs.writeBack.putHandleSnapshot(oldP, []byte("hello"), 5, PendingOverwrite, 1, 0, false, "retry-image", "", true, true, nil, ino, 1, fs.snapshotStagingGens(oldP), nil); err != nil {
							t.Error(err)
						}
					}
				case "retry_released":
					retries++
					assertRetryExclusionsReleased(t, fs, oldP, newP, except)
					if strings.HasSuffix(mode, "recovers") && releaseExternal != nil {
						releaseExternal()
						releaseExternal = nil
						except = ""
					}
				}
			})
			defer func() {
				if releaseExternal != nil {
					releaseExternal()
				}
			}()
			started := time.Now()
			_, unlock, err := fs.lockOwnedRenamePaths(ctx, "/old", "/new")
			elapsed := time.Since(started)
			unlock()
			if strings.HasSuffix(mode, "deadline") {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("err=%v want deadline", err)
				}
				if elapsed > time.Second {
					t.Errorf("retry ignored bounded context: %v", elapsed)
				}
			} else if err != nil {
				t.Errorf("transient contention escaped: %v", err)
			}
			if captures < 2 || retries < 1 {
				t.Errorf("missing recapture captures=%d retries=%d", captures, retries)
			}
			assertRetryExclusionsReleased(t, fs, oldP, newP, except)
			if meta, ok := fs.pendingIndex.GetMeta(oldP); !ok || meta.Kind == PendingConflict {
				t.Errorf("retry changed pending ownership: %+v/%t", meta, ok)
			}
			if data, err := fs.shadowStore.ReadAll(oldP); err != nil || string(data) != "hello" {
				t.Errorf("retry lost accepted image %q/%v", data, err)
			}
			t.Logf("BOUNDS mode=%s captures=%d retries=%d elapsed=%v err=%v", mode, captures, retries, elapsed, err)
		})
	}
}
