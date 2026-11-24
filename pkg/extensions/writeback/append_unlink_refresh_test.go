package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/stretchr/testify/require"
)

func TestUnlinkWhileAppendRefreshReadsRemote(t *testing.T) {
	for _, track := range []string{"inline", "queue", "write-cache", "close-sync-pending", "close-sync-bypass", "close-sync-large", "close-sync-wait"} {
		pendingReplacement := strings.HasPrefix(track, "close-sync-")
		lateUnlink := pendingReplacement && track != "close-sync-pending"
		names := []string{"absent", "recreated"}
		if pendingReplacement {
			names = []string{"recreated"}
		}
		for _, reuse := range names {
			t.Run(track+"/"+reuse, func(t *testing.T) {
				const path = "/append"
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				backend := testbackend.NewBackend()
				backend.Seed(path, []byte("base"))
				var pauseRead, pauseUpload atomic.Bool
				uploading := make(chan struct{})
				resumeUpload := make(chan struct{})
				reading := make(chan struct{})
				resume := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+path && pauseUpload.CompareAndSwap(true, false) {
						close(uploading)
						select {
						case <-resumeUpload:
						case <-ctx.Done():
							http.Error(w, "upload canceled", http.StatusServiceUnavailable)
							return
						}
					}
					if r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+path && pauseRead.CompareAndSwap(true, false) {
						// Capture the old object's reply before allowing unlink to reuse the name.
						reply := httptest.NewRecorder()
						backend.ServeHTTP(reply, r)
						close(reading)
						select {
						case <-resume:
						case <-ctx.Done():
							return
						}
						for key, values := range reply.Header() {
							w.Header()[key] = values
						}
						w.WriteHeader(reply.Code)
						_, _ = w.Write(reply.Body.Bytes())
						return
					}
					backend.ServeHTTP(w, r)
				}))
				t.Cleanup(srv.Close)
				cl := client.New(srv.URL, "")
				opts := Options{Client: cl, Policy: PolicyCloseSync}
				if track == "queue" || pendingReplacement {
					st, err := staging.NewStagingWithQuota(t.TempDir(), -1, 0)
					require.NoError(t, err)
					t.Cleanup(st.Close)
					opts.Staging = st
				}
				if track == "write-cache" || pendingReplacement {
					cache, err := staging.NewWriteBackCache(t.TempDir())
					require.NoError(t, err)
					opts.WriteCache = cache
				}
				if track == "queue" || track == "write-cache" {
					opts.Policy, opts.SyncMode = PolicyWriteBack, "interactive"
				}
				wb, err := New(opts)
				require.NoError(t, err)
				stopSSETestWriteback(t, wb)
				terminal, err := core.New(cl, core.Options{})
				require.NoError(t, err)
				chain, err := fs.NewChain(wb, terminal)
				require.NoError(t, err)
				res, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: path, Flags: syscall.O_RDWR | syscall.O_APPEND})
				require.NoError(t, err)
				h := res.Handle
				require.NotNil(t, h)
				require.NoError(t, h.Write(ctx, 4, []byte("-old")))
				if lateUnlink {
					// A small cache sends a small file down the bypass. A sparse
					// file at the streaming threshold takes the direct large branch.
					wb.wbCacheMaxBytes = 1
					gate, occurrence := "stage:pathLock", int32(2)
					switch track {
					case "close-sync-large":
						require.NoError(t, h.Write(ctx, commitInlineStreamThreshold, []byte("z")))
						occurrence = 1
					case "close-sync-wait":
						gate, occurrence = "flush:queueOutcome", 1
					}
					var seen atomic.Int32
					installTestGateHook(t, wb, func(name string) {
						if name == gate && seen.Add(1) == occurrence {
							close(reading)
							select {
							case <-resume:
							case <-ctx.Done():
							}
						}
					})
				} else {
					require.NoError(t, cl.WriteCtx(ctx, path, []byte("base-old-foreign")))
					pauseRead.Store(true)
				}
				flush := h.Sync
				if opts.WriteCache != nil {
					flush = h.Flush
				}
				done := make(chan error, 1)
				go func() { done <- flush(ctx) }()
				select {
				case <-reading:
				case <-ctx.Done():
					t.Fatal("flush did not reach the requested unlink point")
				}
				_, err = chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: path})
				require.NoError(t, err)
				var replacement fs.Handle
				if pendingReplacement {
					replacement = open(t, chain, path)
					require.NoError(t, replacement.Write(ctx, 0, []byte("replacement")))
					pauseUpload.Store(true)
					require.NoError(t, writebackHandleOf(replacement).stageAndEnqueue(ctx))
					select {
					case <-uploading:
					case <-ctx.Done():
						t.Fatal("replacement upload did not start")
					}
					require.True(t, opts.Staging.Pending.HasPending(path))
				} else if reuse == "recreated" {
					require.NoError(t, cl.WriteCtx(ctx, path, []byte("replacement")))
				}
				close(resume)
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("append refresh did not finish")
				}

				buf := make([]byte, 32)
				if track == "close-sync-large" {
					buf = buf[:len("base-old")]
				}
				n, err := h.Read(ctx, 0, buf)
				require.NoError(t, err)
				require.Equal(t, "base-old", string(buf[:n]))
				if replacement != nil {
					// The old Flush must have returned while the replacement was still uploading.
					close(resumeUpload)
					require.True(t, wb.queue.WaitPathDeadline(path, 5*time.Second))
					require.NoError(t, replacement.Release(ctx))
				}
				require.NoError(t, h.Release(ctx))
				require.NoError(t, wb.Drain(ctx))
				node, exists := backend.Node(path)
				if reuse == "recreated" {
					require.True(t, exists)
					require.Equal(t, "replacement", string(node.Data))
				} else {
					require.False(t, exists, "the anonymous writer resurrected the unlinked name")
				}
			})
		}
	}
}

// An upload holds its pathname lock until its remote PUT finishes. A stage
// already waiting on that lock must wake when its handle loses the name.
func TestStageLockWakesOnNamespaceChange(t *testing.T) {
	for _, change := range []string{"unlink", "rename"} {
		t.Run(change, func(t *testing.T) {
			const oldPath = "/old"
			chain, wb, _ := newSparseGrowthSuccessStack(t, nil)
			fd := open(t, chain, oldPath)
			require.NoError(t, fd.Write(t.Context(), 0, []byte("old")))
			h := writebackHandleOf(fd)
			unlockOld := wb.pathLocks.lock(oldPath)
			defer unlockOld()
			got := make(chan string, 1)
			go func() {
				path, unlock := h.lockLinkedStagePath()
				if unlock != nil {
					unlock()
				}
				got <- path
			}()
			require.Eventually(t, func() bool {
				h.mu.Lock()
				defer h.mu.Unlock()
				return h.stageNameChanged != nil
			}, 3*time.Second, time.Millisecond)

			want := ""
			if change == "rename" {
				want = "/new"
				wb.retarget(oldPath, want)
			} else {
				wb.markUnlinked(oldPath, false)
			}
			select {
			case path := <-got:
				require.Equal(t, want, path)
			case <-time.After(3 * time.Second):
				t.Fatal("stage still waiting for the old pathname lock")
			}
		})
	}
}

func TestQueuedFailureBelongsToCurrentName(t *testing.T) {
	const path = "/error"
	chain, wb, _ := newSparseGrowthSuccessStack(t, nil)
	old := open(t, chain, path)
	require.NoError(t, old.Write(t.Context(), 0, []byte("old")))

	checking, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var first atomic.Bool
	installTestGateHook(t, wb, func(gate string) {
		if gate == "flush:queueOutcome" && first.CompareAndSwap(false, true) {
			close(checking)
			<-resume
		}
	})
	done := make(chan error, 1)
	go func() { done <- writebackHandleOf(old).waitQueuedCommit() }()
	select {
	case <-checking:
	case <-time.After(3 * time.Second):
		t.Fatal("old handle never reached the queue check")
	}
	wb.markUnlinked(path, false)
	replacement := open(t, chain, path)
	wb.queue.RecordTerminalFailureForPath(path, syscall.EIO)
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("old handle did not finish its queue check")
	}
	require.EqualValues(t, syscall.EIO, fs.StatusOf(writebackHandleOf(replacement).waitQueuedCommit()))
}
