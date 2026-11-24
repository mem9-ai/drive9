package writeback

import (
	"bytes"
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

// TestReadOnlyOpenFlushRacingCreateReleaseDoesNotStrandWriteCache reproduces
// the nodefs unmount failure where an O_RDONLY open arrives right as the
// creating writer closes:
//
//  1. O_RDONLY Open sees no staged pending state yet, resolves the live writer
//     via w.handleFor(path), and calls h.Flush(ctx) (Writeback.Do).
//  2. Both O_RDONLY's Flush and the writer's own close Flush observe
//     h.dirty == true before entering stageToWriteCache's path lock.
//  3. In "large_queue_create", the writer's Flush bypasses wbCache into
//     stageAndEnqueue (setting h.dirty=false, h.landed=true,
//     supersededPendingSource=pendingGenQueue while h.isNew remains true until
//     queue upload ack), FUSE_RELEASE runs immediately (setting
//     h.releasing=true, untracking h so queue ack skips it, and discarding
//     h.buf=nil), and the CommitQueue finishes uploading so
//     Pending.HasPending(path) becomes false. When O_RDONLY's parked
//     stageToWriteCache then acquires pathLocks, it must NOT mistake
//     h.buf==nil && h.isNew for an unwritten empty create and write a 0-byte
//     PendingNew entry into wbCache with no Release left to submit it.
//  4. In "append_refresh_after_release", O_RDONLY's fenceRefreshAppendBase
//     observes the newly landed remote revision after Release already discarded
//     h.buf=nil; stageToWriteCache must not dereference a nil h.buf or stage
//     onto wbCache.
//  5. In "small_writeback_create", the writer's Release completes while
//     O_RDONLY's Flush is still entering stageToWriteCache(stageV1Ladder),
//     which must not leave an unsubmitted snapshot behind in wbCache.
func TestReadOnlyOpenFlushRacingCreateReleaseDoesNotStrandWriteCache(t *testing.T) {
	t.Run("non_append_stage_after_queue_release", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		const targetPath = "/large-stream.bin"
		const reqPath = "/v1/fs" + targetPath

		backend := testbackend.NewBackend()
		var gateFirstPut atomic.Bool
		putEntered := make(chan struct{})
		allowPut := make(chan struct{})
		testDone := make(chan struct{})

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == reqPath && gateFirstPut.CompareAndSwap(true, false) {
				close(putEntered)
				select {
				case <-allowPut:
				case <-testDone:
					http.Error(w, "test abort", http.StatusServiceUnavailable)
					return
				}
			}
			backend.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(testDone) })

		st, err := staging.NewStagingWithQuota(t.TempDir(), -1, 0)
		require.NoError(t, err)
		defer st.Close()

		wbcache, err := staging.NewWriteBackCache(t.TempDir())
		require.NoError(t, err)

		cl := client.New(srv.URL, "")
		wb, err := New(Options{
			Policy:         PolicyWriteBack,
			Client:         cl,
			Staging:        st,
			WriteCache:     wbcache,
			ExtentTakeover: true,
			ExtentMinSize:  64 << 10,
		})
		require.NoError(t, err)
		stopSSETestWriteback(t, wb)
		wb.queue.SetExtentTakeover(false)

		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		chain, err := fs.NewChain(wb, terminal)
		require.NoError(t, err)

		// Exact flags of Node's fs.createWriteStream(streamPath).
		createRes, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  targetPath,
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644,
		})
		require.NoError(t, err)
		writer := createRes.Handle
		bh := writebackHandleOf(writer)
		require.NotNil(t, bh)

		payload := bytes.Repeat([]byte("z"), 128<<10)
		require.NoError(t, writer.Write(ctx, 0, payload))

		gateFirstPut.Store(true)
		require.NoError(t, writer.Flush(ctx))
		// Release runs before CommitQueue's upload ack, untracking bh and
		// discarding bh.buf to nil while bh.isNew stays true.
		require.NoError(t, writer.Release(ctx))

		select {
		case <-putEntered:
		case <-ctx.Done():
			t.Fatal("timed out waiting for CommitQueue PUT")
		}
		close(allowPut)
		require.True(t, wb.queue.WaitPathDeadline(targetPath, 5*time.Second))
		require.False(t, st.Pending.HasPending(targetPath))

		// Simulate O_RDONLY's Flush (which already passed Flush's unlocked
		// dirty check before writer.Flush retired dirty) now entering
		// stageToWriteCache(stageV1Ladder).
		snap, serr := bh.stageToWriteCache(ctx, stageV1Ladder)
		require.NoError(t, serr)
		require.Nil(t, snap)

		require.NoError(t, wb.Drain(ctx))
		meta, hasMeta := wbcache.GetMeta(targetPath)
		require.Falsef(t, hasMeta, "stranded wbCache meta after Release+Drain: %+v", meta)
		require.Zero(t, wb.uploader.Snapshot().Cached)

		node, ok := backend.Node(targetPath)
		require.True(t, ok)
		require.Equal(t, payload, node.Data)
	})

	for _, tc := range []struct {
		name                 string
		payloadSize          int
		extentRoute          bool
		headSeesPrePutAbsent bool
	}{
		{
			name:                 "large_queue_create",
			payloadSize:          128 << 10,
			extentRoute:          true,
			headSeesPrePutAbsent: true,
		},
		{
			name:                 "append_refresh_after_release",
			payloadSize:          128 << 10,
			extentRoute:          true,
			headSeesPrePutAbsent: false,
		},
		{
			name:                 "small_writeback_create",
			payloadSize:          4 << 10,
			extentRoute:          false,
			headSeesPrePutAbsent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			const targetPath = "/large-stream.bin"
			const reqPath = "/v1/fs" + targetPath

			backend := testbackend.NewBackend()

			var gateFirstHead atomic.Bool
			headEntered := make(chan struct{})
			allowHead := make(chan struct{})

			var gateFirstPut atomic.Bool
			putEntered := make(chan struct{})
			allowPut := make(chan struct{})
			testDone := make(chan struct{})

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && r.URL.Path == reqPath && gateFirstHead.CompareAndSwap(true, false) {
					close(headEntered)
					select {
					case <-allowHead:
					case <-testDone:
						http.Error(w, "test abort", http.StatusServiceUnavailable)
						return
					}
					if tc.headSeesPrePutAbsent {
						http.NotFound(w, r)
						return
					}
				}
				if r.Method == http.MethodPut && r.URL.Path == reqPath && gateFirstPut.CompareAndSwap(true, false) {
					close(putEntered)
					select {
					case <-allowPut:
					case <-testDone:
						http.Error(w, "test abort", http.StatusServiceUnavailable)
						return
					}
				}
				backend.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			t.Cleanup(func() { close(testDone) })

			st, err := staging.NewStagingWithQuota(t.TempDir(), -1, 0)
			require.NoError(t, err)
			defer st.Close()

			wbcache, err := staging.NewWriteBackCache(t.TempDir())
			require.NoError(t, err)

			cl := client.New(srv.URL, "")
			opts := Options{
				Policy:     PolicyWriteBack,
				Client:     cl,
				Staging:    st,
				WriteCache: wbcache,
			}
			if tc.extentRoute {
				opts.ExtentTakeover = true
				opts.ExtentMinSize = 64 << 10
			}
			wb, err := New(opts)
			require.NoError(t, err)
			stopSSETestWriteback(t, wb)
			if tc.extentRoute && wb.queue != nil {
				wb.queue.SetExtentTakeover(false)
			}

			terminal, err := core.New(cl, core.Options{})
			require.NoError(t, err)
			chain, err := fs.NewChain(wb, terminal)
			require.NoError(t, err)

			createRes, err := chain.Dispatch(ctx, &fs.Op{
				Kind:  fs.OpCreate,
				Path:  targetPath,
				Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_APPEND),
				Mode:  0o644,
			})
			require.NoError(t, err)
			require.NotNil(t, createRes.Handle)
			writer := createRes.Handle

			payload := bytes.Repeat([]byte("z"), tc.payloadSize)
			require.NoError(t, writer.Write(ctx, 0, payload))

			gateFirstHead.Store(true)
			gateFirstPut.Store(true)

			type roResult struct {
				res fs.Result
				err error
			}
			roDone := make(chan roResult, 1)
			go func() {
				res, derr := chain.Dispatch(ctx, &fs.Op{
					Kind:  fs.OpCreate,
					Path:  targetPath,
					Flags: uint32(syscall.O_RDONLY),
				})
				roDone <- roResult{res: res, err: derr}
			}()

			select {
			case <-headEntered:
			case <-ctx.Done():
				t.Fatal("timed out waiting for O_RDONLY Flush to enter stageToWriteCache")
			}

			// Writer closes (FUSE_FLUSH followed by FUSE_RELEASE) while upload
			// ack is held until Release untracks the handle and discards h.buf.
			require.NoError(t, writer.Flush(ctx))
			require.NoError(t, writer.Release(ctx))

			// Release the in-flight upload and wait for remote landing so
			// Pending.HasPending(targetPath) is false before O_RDONLY acquires
			// pathLocks inside stageToWriteCache.
			select {
			case <-putEntered:
			case <-ctx.Done():
				t.Fatal("timed out waiting for writer upload PUT")
			}
			close(allowPut)
			if tc.extentRoute {
				require.True(t, wb.queue.WaitPathDeadline(targetPath, 5*time.Second))
				require.False(t, st.Pending.HasPending(targetPath))
			} else {
				require.Eventually(t, func() bool {
					s := wb.uploader.Snapshot()
					return s.Queued == 0 && s.InFlight == 0 && s.Cached == 0
				}, 5*time.Second, 5*time.Millisecond)
			}

			// Let O_RDONLY's parked stageToWriteCache proceed into pathLocks.
			close(allowHead)
			var ro roResult
			select {
			case ro = <-roDone:
			case <-ctx.Done():
				t.Fatal("timed out waiting for O_RDONLY open to complete")
			}
			require.NoError(t, ro.err)
			require.NotNil(t, ro.res.Handle)

			readBuf := make([]byte, len(payload)+64)
			n, rerr := ro.res.Handle.Read(ctx, 0, readBuf)
			require.NoError(t, rerr)
			require.Equal(t, payload, readBuf[:n])
			require.NoError(t, ro.res.Handle.Release(ctx))

			// Unmount drain must succeed with zero stranded wbCache entries and
			// the intact remote payload.
			require.NoError(t, wb.Drain(ctx))
			meta, hasMeta := wbcache.GetMeta(targetPath)
			require.Falsef(t, hasMeta, "stranded wbCache meta after Release+Drain: %+v", meta)
			upSnap := wb.uploader.Snapshot()
			require.Zero(t, upSnap.Cached, "uploader cached entries must drain to 0")
			require.Zero(t, upSnap.Queued, "uploader queued entries must drain to 0")
			require.Zero(t, upSnap.InFlight, "uploader in-flight entries must drain to 0")

			node, ok := backend.Node(targetPath)
			require.True(t, ok, "remote file must exist")
			require.Equal(t, payload, node.Data)
		})
	}
}
