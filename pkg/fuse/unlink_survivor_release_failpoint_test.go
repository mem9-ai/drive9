//go:build linux && failpoint

package fuse

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
)

func TestUnlinkSurvivorClosingHandleKernel(t *testing.T) {
	for _, window := range []string{"held_at_capture", "closed_after_capture"} {
		t.Run(window, func(t *testing.T) {
			testReadDirPlusReplacedInodeKernel(t, MountProfileCodingAgent, SyncStrict, 4096, 8192,
				func(fs *Dat9FS, raw *readdirPendingKernelFS) func() {
					queued, held, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
					allowRelease, allowFinish := make(chan struct{}), make(chan struct{})
					start, finish := sync.OnceFunc(func() { close(allowRelease) }), sync.OnceFunc(func() { close(allowFinish) })
					var firstID atomic.Uint64
					var first atomic.Pointer[FileHandle]
					raw.beforeRelease = func(id uint64) {
						if firstID.CompareAndSwap(0, id) {
							h, _ := fs.fileHandles.Get(id)
							first.Store(h)
							close(queued)
							<-allowRelease
						}
					}
					raw.afterRelease = func(id uint64) {
						if id == firstID.Load() {
							close(done)
						}
					}
					await := func(ch <-chan struct{}, name string) {
						select {
						case <-ch:
						case <-time.After(3 * time.Second):
							t.Errorf("missing %s boundary", name)
							start()
							finish()
						}
					}
					releasePoint := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReleaseBudget"
					capturePoint := "github.com/mem9-ai/drive9/pkg/fuse/unlinkSurvivorHandlesSnapshot"
					if err := failpoint.EnableCall(releasePoint, func(observed *Dat9FS, h *FileHandle, _ context.Context) {
						if observed == fs && h == first.Load() {
							close(held)
							<-allowFinish
						}
					}); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = failpoint.Disable(releasePoint) })
					var once sync.Once
					if err := failpoint.EnableCall(capturePoint, func(observed *Dat9FS) {
						if observed == fs && window == "closed_after_capture" {
							once.Do(func() {
								await(queued, "queued release")
								start()
								await(held, "held release")
								finish()
								await(done, "completed release")
							})
						}
					}); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = failpoint.Disable(capturePoint) })
					testHookBeforeUnlinkSurvivorBind = func(path string) {
						if path == "/file.dat" && window == "held_at_capture" {
							await(queued, "queued release")
							start()
							await(held, "held release")
						}
					}
					testHookAfterUnlinkHandleSetLockAttempt = finish
					return func() {
						start()
						finish()
						if firstID.Load() != 0 {
							await(done, "release cleanup")
						}
						testHookBeforeUnlinkSurvivorBind = nil
						testHookAfterUnlinkHandleSetLockAttempt = nil
						_ = failpoint.Disable(releasePoint)
						_ = failpoint.Disable(capturePoint)
					}
				})
		})
	}
}
