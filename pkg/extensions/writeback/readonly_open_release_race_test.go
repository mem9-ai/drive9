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

// A read-only open borrows the live writer to flush its image. Exercise a
// reader queued behind close and a close queued behind the reader's flush,
// then verify the final bytes and that neither upload track is stranded.
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
				}
			}
			backend.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		defer close(testDone)

		st, err := staging.NewStagingWithQuota(t.TempDir(), -1, 0)
		require.NoError(t, err)
		t.Cleanup(st.Close)

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
		name             string
		payloadSize      int
		extentRoute      bool
		readerFlushFirst bool
	}{
		{
			name:             "large_queue_create",
			payloadSize:      128 << 10,
			extentRoute:      true,
			readerFlushFirst: true,
		},
		{
			name:             "append_refresh_after_release",
			payloadSize:      128 << 10,
			extentRoute:      true,
			readerFlushFirst: false,
		},
		{
			name:             "small_writeback_create",
			payloadSize:      4 << 10,
			extentRoute:      false,
			readerFlushFirst: true,
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
						http.NotFound(w, r)
						return
					}
					http.NotFound(w, r)
					return
				}
				if r.Method == http.MethodPut && r.URL.Path == reqPath && gateFirstPut.CompareAndSwap(true, false) {
					close(putEntered)
					select {
					case <-allowPut:
					case <-testDone:
					}
				}
				backend.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			defer close(testDone)

			st, err := staging.NewStagingWithQuota(t.TempDir(), -1, 0)
			require.NoError(t, err)
			t.Cleanup(st.Close)

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

			gateFirstHead.Store(tc.readerFlushFirst)
			gateFirstPut.Store(true)
			readerWaiting := make(chan struct{})
			resumeReader := make(chan struct{})
			if !tc.readerFlushFirst {
				var first atomic.Bool
				installTestGateHook(t, wb, func(gate string) {
					if gate == "flush:flushMu" && first.CompareAndSwap(false, true) {
						close(readerWaiting)
						select {
						case <-resumeReader:
						case <-testDone:
						}
					}
				})
			}

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

			writerWaiting := make(chan struct{}, 1)
			if tc.readerFlushFirst {
				select {
				case <-headEntered:
				case <-ctx.Done():
					t.Fatal("reader did not enter the append refresh")
				}
				installTestGateHook(t, wb, func(gate string) {
					if gate == "flush:flushMu" {
						select {
						case writerWaiting <- struct{}{}:
						default:
						}
					}
				})
			} else {
				select {
				case <-readerWaiting:
				case <-ctx.Done():
					t.Fatal("reader did not reach the flush lock")
				}
			}
			writerDone := make(chan error, 1)
			go func() {
				err := writer.Flush(ctx)
				if err == nil {
					err = writer.Release(ctx)
				}
				writerDone <- err
			}()
			if tc.readerFlushFirst {
				select {
				case <-writerWaiting:
				case <-ctx.Done():
					t.Fatal("writer did not reach the flush lock")
				}
				close(allowHead)
			}
			select {
			case <-putEntered:
			case <-ctx.Done():
				t.Fatal("timed out waiting for upload PUT")
			}
			close(allowPut)
			select {
			case err := <-writerDone:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("writer close did not complete")
			}
			if tc.extentRoute {
				require.True(t, wb.queue.WaitPathDeadline(targetPath, 5*time.Second))
				require.False(t, st.Pending.HasPending(targetPath))
			} else {
				require.Eventually(t, func() bool {
					s := wb.uploader.Snapshot()
					return s.Queued == 0 && s.InFlight == 0 && s.Cached == 0
				}, 5*time.Second, 5*time.Millisecond)
			}

			if !tc.readerFlushFirst {
				close(resumeReader)
			}
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
