package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	for _, track := range []string{"inline", "queue", "write-cache", "close-sync-pending"} {
		names := []string{"absent", "recreated"}
		if track == "close-sync-pending" {
			names = []string{"recreated"}
		}
		for _, reuse := range names {
			t.Run(track+"/"+reuse, func(t *testing.T) {
				const path = "/append"
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
				if track == "queue" || track == "close-sync-pending" {
					st, err := staging.NewStagingWithQuota(t.TempDir(), -1, 0)
					require.NoError(t, err)
					t.Cleanup(st.Close)
					opts.Staging = st
				}
				if track == "write-cache" || track == "close-sync-pending" {
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
				require.NoError(t, cl.WriteCtx(ctx, path, []byte("base-old-foreign")))

				pauseRead.Store(true)
				flush := h.Sync
				if opts.WriteCache != nil {
					flush = h.Flush
				}
				done := make(chan error, 1)
				go func() { done <- flush(ctx) }()
				select {
				case <-reading:
				case <-ctx.Done():
					t.Fatal("append refresh did not read the newer remote revision")
				}
				_, err = chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: path})
				require.NoError(t, err)
				var replacement fs.Handle
				if track == "close-sync-pending" {
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
