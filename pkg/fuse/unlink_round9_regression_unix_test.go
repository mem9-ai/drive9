//go:build !windows

package fuse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/stretchr/testify/require"
)

type round9UnlinkHook struct {
	*writeback.Writeback
	beforeAcquire func()
	afterAcquire  func()
	beforeCommit  func()
}

func (h *round9UnlinkHook) PrepareOpenUnlink(path, survivor string) (func(func() bool) error, func(func() bool, func() bool) error, func(), func()) {
	acquire, commit, finalize, abort := h.Writeback.PrepareOpenUnlink(path, survivor)
	wrapAcquire := func(aborted func() bool) error {
		if h.beforeAcquire != nil {
			h.beforeAcquire()
		}
		err := acquire(aborted)
		if err == nil && h.afterAcquire != nil {
			h.afterAcquire()
		}
		return err
	}
	wrapCommit := func(aborted func() bool, linearizeNoWAL func() bool) error {
		if h.beforeCommit != nil {
			h.beforeCommit()
		}
		return commit(aborted, linearizeNoWAL)
	}
	return wrapAcquire, wrapCommit, finalize, abort
}

func assertSwitchGatesClean(t *testing.T, f *FS) {
	t.Helper()
	res := f.viewSwitchPreflight()
	for _, r := range res.Refusals {
		if r.Gate == "inode_nlookup" {
			continue
		}
		t.Fatalf("unexpected view-switch refusal: %+v (all=%+v)", r, res.Refusals)
	}
}

// TestDeferredUnlinksBlockPreflightAndDrainWithContext covers:
//  1. ReexecPreflight refuses while deferredUnlinks exist;
//  2. FS.Drain(ctx) propagates caller deadline/cancellation into deferred unlink
//     dispatch instead of blocking 30s per entry;
//  3. FS.Drain fails when remote DELETE fails (keeping preflight refused) and
//     succeeds once remote DELETE recovers.
func TestDeferredUnlinksBlockPreflightAndDrainWithContext(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/del", []byte("bye"))

	var (
		blockDelete atomic.Bool
		failDelete  atomic.Bool
		releaseDel  = make(chan struct{})
	)
	blockDelete.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/del") {
			if blockDelete.Load() {
				select {
				case <-r.Context().Done():
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-releaseDel:
				}
			}
			if failDelete.Load() {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		blockDelete.Store(false)
		select {
		case <-releaseDel:
		default:
			close(releaseDel)
		}
	})

	cl := client.New(srv.URL, "")
	terminal, err := core.New(cl, core.Options{})
	require.NoError(t, err)
	f, err := New(Options{Extensions: []fs.Handler{terminal}, DefaultPermissions: true})
	require.NoError(t, err)

	ino := lookupFacade(t, f, "del")
	hdr := gofuse.InHeader{NodeId: ino}
	var openOut gofuse.OpenOut
	require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDONLY}, &openOut))
	require.Equal(t, gofuse.OK, f.Unlink(nil, rootHeader(), "del"))

	// Close the handle while remote DELETE fails so deferredUnlinks["/del"] stays queued.
	blockDelete.Store(false)
	failDelete.Store(true)
	f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: openOut.Fh})

	pre := f.viewSwitchPreflight()
	require.False(t, pre.OK)
	require.Contains(t, pre.Refusals, ReexecRefusal{Gate: "deferred_unlinks", Reason: "pending deferred unlinks exist", Count: 1})

	// Drain must fail while remote DELETE returns 500.
	dresp := f.Drain(context.Background())
	require.False(t, dresp.OK)
	require.Contains(t, f.viewSwitchPreflight().Refusals, ReexecRefusal{Gate: "deferred_unlinks", Reason: "pending deferred unlinks exist", Count: 1})

	// Drain with short deadline must respect caller ctx instead of waiting 30s.
	failDelete.Store(false)
	blockDelete.Store(true)
	shortCtx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	drespShort := f.Drain(shortCtx)
	require.False(t, drespShort.OK)
	require.Less(t, time.Since(start), 3*time.Second)

	// Unblock DELETE and verify Drain clears deferredUnlinks and preflight passes.
	blockDelete.Store(false)
	close(releaseDel)
	drespOK := f.Drain(context.Background())
	require.True(t, drespOK.OK, drespOK.Error)
	assertSwitchGatesClean(t, f)
}

// TestOpenUnlinkStagedRemoteFileDropsPendingAllowsSwitchAndSameNameRecreate
// verifies that open-file unlink (and closed-file unlink) of an Fsync-staged
// existing remote file removes PendingIndex[p], keeps anonymous reads/writes
// backed by unlinkedShadowPin without adopting a same-name replacement's
// Pending entry, and leaves Pending.Count() == 0 & ReexecPreflight().OK == true
// once closed.
func TestOpenUnlinkStagedRemoteFileDropsPendingAllowsSwitchAndSameNameRecreate(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/held", []byte("remote-0"))
	b.Seed("/closed_staged", []byte("remote-closed-0"))

	var holdHeldPut atomic.Bool
	holdHeldPut.Store(true)
	releasePut := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) &&
			(strings.HasSuffix(r.URL.Path, "/held") || strings.HasSuffix(r.URL.Path, "/closed_staged")) &&
			holdHeldPut.Load() {
			select {
			case <-r.Context().Done():
				http.Error(w, "canceled", http.StatusRequestTimeout)
				return
			case <-releasePut:
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	cl := client.New(srv.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	ledger, err := staging.NewStaging(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(ledger.Close)
	wb, err := writeback.New(writeback.Options{
		Policy:                  writeback.PolicyWriteBack,
		SyncMode:                "interactive",
		Client:                  cl,
		Staging:                 ledger,
		RemoteCommitWaitTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	t.Cleanup(func() {
		holdHeldPut.Store(false)
		select {
		case <-releasePut:
		default:
			close(releasePut)
		}
	})
	terminal, err := core.New(cl, core.Options{})
	require.NoError(t, err)
	f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, DefaultPermissions: true})
	require.NoError(t, err)

	// 1. Closed-file unlink of an Fsync-staged remote file must drop PendingIndex.
	closedIno := lookupFacade(t, f, "closed_staged")
	closedHdr := gofuse.InHeader{NodeId: closedIno}
	var closedOpen gofuse.OpenOut
	require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: closedHdr, Flags: syscall.O_RDWR}, &closedOpen))
	_, st := f.Write(nil, &gofuse.WriteIn{InHeader: closedHdr, Fh: closedOpen.Fh}, []byte("closed-v1"))
	require.Equal(t, gofuse.OK, st)
	require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: closedHdr, Fh: closedOpen.Fh}))
	f.Release(nil, &gofuse.ReleaseIn{InHeader: closedHdr, Fh: closedOpen.Fh})
	require.True(t, ledger.Pending.HasPending("/closed_staged"))
	require.Equal(t, gofuse.OK, f.Unlink(nil, rootHeader(), "closed_staged"))
	require.False(t, ledger.Pending.HasPending("/closed_staged"))

	// 2. Open-file unlink of an Fsync-staged remote file.
	heldIno := lookupFacade(t, f, "held")
	heldHdr := gofuse.InHeader{NodeId: heldIno}
	var heldOpen gofuse.OpenOut
	require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: heldHdr, Flags: syscall.O_RDWR}, &heldOpen))
	v1 := []byte("staged-v1")
	_, st = f.Write(nil, &gofuse.WriteIn{InHeader: heldHdr, Fh: heldOpen.Fh}, v1)
	require.Equal(t, gofuse.OK, st)
	require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: heldHdr, Fh: heldOpen.Fh}))
	require.True(t, ledger.Pending.HasPending("/held"))

	require.Equal(t, gofuse.OK, f.Unlink(nil, rootHeader(), "held"))
	require.False(t, ledger.Pending.HasPending("/held"), "PendingIndex[/held] must be removed immediately on open unlink")
	require.Equal(t, 0, ledger.Pending.Count())

	// Recreate /held while old anonymous handle is still open.
	holdHeldPut.Store(false)
	close(releasePut)
	var recOut gofuse.CreateOut
	ci := &gofuse.CreateIn{InHeader: *rootHeader(), Flags: syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC, Mode: 0o644}
	require.Equal(t, gofuse.OK, f.Create(nil, ci, "held", &recOut))
	recHdr := gofuse.InHeader{NodeId: recOut.NodeId}
	repl := []byte("brand-new-replacement")
	_, st = f.Write(nil, &gofuse.WriteIn{InHeader: recHdr, Fh: recOut.Fh}, repl)
	require.Equal(t, gofuse.OK, st)
	require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: recHdr, Fh: recOut.Fh}))

	// Old anonymous handle must still read staged-v1 (and compose local writes on it),
	// never adopting the replacement's Pending/Shadow image.
	buf := make([]byte, len(v1))
	res, st := f.Read(nil, &gofuse.ReadIn{InHeader: heldHdr, Fh: heldOpen.Fh, Size: uint32(len(v1))}, buf)
	require.Equal(t, gofuse.OK, st)
	got, _ := res.Bytes(buf)
	require.Equal(t, string(v1), string(got))

	// Partial write on old anonymous handle preloads from unlinkedShadowPin via ensureBufferLocked.
	_, st = f.Write(nil, &gofuse.WriteIn{InHeader: heldHdr, Fh: heldOpen.Fh, Offset: 7}, []byte("v2"))
	require.Equal(t, gofuse.OK, st)
	buf2 := make([]byte, 9)
	res2, st := f.Read(nil, &gofuse.ReadIn{InHeader: heldHdr, Fh: heldOpen.Fh, Size: 9}, buf2)
	require.Equal(t, gofuse.OK, st)
	got2, _ := res2.Bytes(buf2)
	require.Equal(t, "staged-v2", string(got2))

	f.Release(nil, &gofuse.ReleaseIn{InHeader: heldHdr, Fh: heldOpen.Fh})
	f.Release(nil, &gofuse.ReleaseIn{InHeader: recHdr, Fh: recOut.Fh})
	dresp := f.Drain(context.Background())
	require.True(t, dresp.OK, dresp.Error)
	require.Equal(t, 0, ledger.Pending.Count())
	require.Zero(t, ledger.Shadow.PendingBytes())
	assertSwitchGatesClean(t, f)

	node, ok := b.Node("/held")
	require.True(t, ok)
	require.Equal(t, string(repl), string(node.Data))
}

// TestOpenUnlinkWALFallbackPreservesAnonymousCleanReaderImage covers Codex P1 #1:
// when a sibling writer stages an image and closes, and WAL JournalUnlink fails
// during FS.Unlink so FinishCancelPathLocked falls back to durable shadow
// retirement, a pre-existing clean reader retains readable backing via prePin.
func TestOpenUnlinkWALFallbackPreservesAnonymousCleanReaderImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/held", []byte("old"))

	var holdPut atomic.Bool
	holdPut.Store(true)
	releasePut := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) && strings.HasSuffix(r.URL.Path, "/held") && holdPut.Load() {
			select {
			case <-r.Context().Done():
				http.Error(w, "canceled", http.StatusRequestTimeout)
				return
			case <-releasePut:
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	cl := client.New(srv.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	ledger, err := staging.NewStaging(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(ledger.Close)
	wb, err := writeback.New(writeback.Options{
		Policy:                  writeback.PolicyWriteBack,
		SyncMode:                "interactive",
		Client:                  cl,
		Staging:                 ledger,
		RemoteCommitWaitTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	t.Cleanup(func() {
		holdPut.Store(false)
		select {
		case <-releasePut:
		default:
			close(releasePut)
		}
	})
	terminal, err := core.New(cl, core.Options{})
	require.NoError(t, err)
	f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, DefaultPermissions: true})
	require.NoError(t, err)

	ino := lookupFacade(t, f, "held")
	hdr := gofuse.InHeader{NodeId: ino}
	var readerOut, writerOut gofuse.OpenOut
	require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDONLY}, &readerOut))
	require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &writerOut))

	want := []byte("wal-fallback-preserved-bytes")
	_, st := f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: writerOut.Fh}, want)
	require.Equal(t, gofuse.OK, st)
	require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: hdr, Fh: writerOut.Fh}))
	f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: writerOut.Fh})

	// Close WAL journal while shadowDir remains writable -> triggers RemoveIfGenerationDurable fallback.
	require.NoError(t, ledger.Journal.Close())
	require.Equal(t, gofuse.OK, f.Unlink(nil, rootHeader(), "held"))
	require.False(t, ledger.Shadow.Has("/held"))

	buf := make([]byte, len(want))
	res, st := f.Read(nil, &gofuse.ReadIn{InHeader: hdr, Fh: readerOut.Fh, Size: uint32(len(want))}, buf)
	require.Equal(t, gofuse.OK, st)
	got, _ := res.Bytes(buf)
	require.Equal(t, string(want), string(got))

	f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: readerOut.Fh})
	require.Zero(t, ledger.Shadow.PendingBytes())
	require.Equal(t, 0, ledger.Pending.Count())
	assertSwitchGatesClean(t, f)
}

// TestCleanReaderUnlinkSnapshotFreshnessAndPartialShadowWriteFailure covers:
//  1. Sibling Flush to wbCache after initial snapshot prep refreshes clean reader;
//  2. Shadow.WriteFull failure leaves zero orphaned .shadow bytes and serves via pendingImage;
//  3. Concurrent sibling Fsync after clean-reader snapshot prep is never clobbered
//     by a failing WriteFull/Remove (@adversary-2 HOLD / Codex r4704635633).
func TestCleanReaderUnlinkSnapshotFreshnessAndPartialShadowWriteFailure(t *testing.T) {
	t.Run("sibling_wbcache_flush_freshness", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/held", []byte("stale"))

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Method == http.MethodPut || r.Method == http.MethodPost) && strings.HasSuffix(r.URL.Path, "/held") {
				// Return 409 Conflict so wb.uploader finishes immediately without retry
				// backoff while preserving wbCache on disk and leaving remote /held="stale".
				http.Error(w, `{"code":"CONFLICT","message":"conflict"}`, http.StatusConflict)
				return
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		wb, err := writeback.New(writeback.Options{
			Policy:        writeback.PolicyWriteBack,
			Client:        cl,
			WriteCacheDir: filepath.Join(t.TempDir(), "wbcache"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })

		entered := make(chan struct{})
		proceed := make(chan struct{})
		var once sync.Once
		hook := &round9UnlinkHook{
			Writeback: wb,
			beforeAcquire: func() {
				once.Do(func() {
					close(entered)
					<-proceed
				})
			},
		}
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{hook, terminal}, DefaultPermissions: true})
		require.NoError(t, err)

		ino := lookupFacade(t, f, "held")
		hdr := gofuse.InHeader{NodeId: ino}
		var readerOut, writerOut gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDONLY}, &readerOut))
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &writerOut))

		unlinkDone := make(chan gofuse.Status, 1)
		go func() { unlinkDone <- f.Unlink(nil, rootHeader(), "held") }()
		<-entered

		// Sibling writes and Flushes into wbCache (and closes) AFTER initial prepareUnlinkSnapshots!
		fresh := []byte("fresh-wbcache-payload")
		_, st := f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: writerOut.Fh}, fresh)
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Flush(nil, &gofuse.FlushIn{InHeader: hdr, Fh: writerOut.Fh}))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: writerOut.Fh})

		close(proceed)
		require.Equal(t, gofuse.OK, <-unlinkDone)

		buf := make([]byte, len(fresh))
		res, st := f.Read(nil, &gofuse.ReadIn{InHeader: hdr, Fh: readerOut.Fh, Size: uint32(len(fresh))}, buf)
		require.Equal(t, gofuse.OK, st)
		got, _ := res.Bytes(buf)
		require.Equal(t, string(fresh), string(got))

		f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: readerOut.Fh})
	})

	t.Run("writefull_quota_failure_and_concurrent_fsync_preservation", func(t *testing.T) {
		b := testbackend.NewBackend()
		initial := []byte("initial-32-byte-remote-content!!")
		b.Seed("/clean_fail", initial)
		b.Seed("/race_fsync", initial)

		var holdPut atomic.Bool
		holdPut.Store(true)
		releasePut := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Method == http.MethodPut || r.Method == http.MethodPost) && holdPut.Load() {
				select {
				case <-r.Context().Done():
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-releasePut:
				}
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		dir := t.TempDir()
		ledger, err := staging.NewStaging(dir)
		require.NoError(t, err)
		t.Cleanup(ledger.Close)
		// Replace Shadow with a 20-byte quota store: 32-byte WriteFull fails ENOSPC,
		// while a 16-byte sibling Fsync succeeds.
		quotaShadow, err := staging.NewShadowStoreWithQuota(filepath.Join(dir, "qshadow"), 0, 20)
		require.NoError(t, err)
		t.Cleanup(quotaShadow.Close)
		ledger.Shadow = quotaShadow

		wb, err := writeback.New(writeback.Options{
			Policy:                  writeback.PolicyWriteBack,
			SyncMode:                "interactive",
			Client:                  cl,
			Staging:                 ledger,
			RemoteCommitWaitTimeout: 200 * time.Millisecond,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		t.Cleanup(func() {
			holdPut.Store(false)
			select {
			case <-releasePut:
			default:
				close(releasePut)
			}
		})

		var pauseRace atomic.Bool
		entered := make(chan struct{})
		proceed := make(chan struct{})
		hook := &round9UnlinkHook{
			Writeback: wb,
			beforeAcquire: func() {
				if pauseRace.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			},
		}
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{hook, terminal}, DefaultPermissions: true})
		require.NoError(t, err)

		// Part 1: Clean-reader WriteFull fails ENOSPC -> no orphaned shadow, pendingImage serves.
		cIno := lookupFacade(t, f, "clean_fail")
		cHdr := gofuse.InHeader{NodeId: cIno}
		var cOpen gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: cHdr, Flags: syscall.O_RDONLY}, &cOpen))
		require.Equal(t, gofuse.OK, f.Unlink(nil, rootHeader(), "clean_fail"))
		require.False(t, quotaShadow.Has("/clean_fail"))
		require.Zero(t, quotaShadow.PendingBytes())
		cbuf := make([]byte, len(initial))
		cres, st := f.Read(nil, &gofuse.ReadIn{InHeader: cHdr, Fh: cOpen.Fh, Size: uint32(len(initial))}, cbuf)
		require.Equal(t, gofuse.OK, st)
		cgot, _ := cres.Bytes(cbuf)
		require.Equal(t, string(initial), string(cgot))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: cHdr, Fh: cOpen.Fh})
		assertSwitchGatesClean(t, f)

		// Part 2 (@adversary-2 HOLD window): clean reader prepares 32B snapshot, then sibling
		// Fsyncs 16B before acquireLock; commit must NOT run failing 32B WriteFull/Remove over it.
		rIno := lookupFacade(t, f, "race_fsync")
		rHdr := gofuse.InHeader{NodeId: rIno}
		var rRead, rWrite gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: rHdr, Flags: syscall.O_RDONLY}, &rRead))
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: rHdr, Flags: syscall.O_RDWR}, &rWrite))

		pauseRace.Store(true)
		unlinkDone := make(chan gofuse.Status, 1)
		go func() { unlinkDone <- f.Unlink(nil, rootHeader(), "race_fsync") }()
		<-entered

		fsynced16 := []byte("newer-fsync-16b!")
		var truncOut gofuse.AttrOut
		require.Equal(t, gofuse.OK, f.SetAttr(nil, &gofuse.SetAttrIn{
			SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: rHdr, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Size: 0, Fh: rWrite.Fh},
		}, &truncOut))
		_, st = f.Write(nil, &gofuse.WriteIn{InHeader: rHdr, Fh: rWrite.Fh}, fsynced16)
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: rHdr, Fh: rWrite.Fh}))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: rHdr, Fh: rWrite.Fh})

		close(proceed)
		require.Equal(t, gofuse.OK, <-unlinkDone)

		rbuf := make([]byte, len(fsynced16))
		rres, st := f.Read(nil, &gofuse.ReadIn{InHeader: rHdr, Fh: rRead.Fh, Size: uint32(len(fsynced16))}, rbuf)
		require.Equal(t, gofuse.OK, st)
		rgot, _ := rres.Bytes(rbuf)
		require.Equal(t, string(fsynced16), string(rgot))

		f.Release(nil, &gofuse.ReleaseIn{InHeader: rHdr, Fh: rRead.Fh})
		require.Zero(t, quotaShadow.PendingBytes())
		require.Equal(t, 0, ledger.Pending.Count())
		assertSwitchGatesClean(t, f)
	})
}

// TestHardlinkSurvivorReversibleTransferAndConcurrentUnlink covers Codex P1 #2,
// hardlink-survivor migration + switch preflight, and concurrent hardlink
// Unlink/Rename under -race.
func TestHardlinkSurvivorReversibleTransferAndConcurrentUnlink(t *testing.T) {
	t.Run("commit_reversible_meta_failure_restores_source_shadow_and_kind", func(t *testing.T) {
		dir := t.TempDir()
		ledger, err := staging.NewStaging(dir)
		require.NoError(t, err)
		t.Cleanup(ledger.Close)

		payload := []byte("hardlink-reversible-payload")
		require.NoError(t, ledger.Shadow.WriteFull("/held", payload, 1))
		gen, perr := ledger.Pending.Put("/held", int64(len(payload)), staging.PendingOverwrite)
		require.NoError(t, perr)
		require.NotZero(t, gen)

		tx, ok, err := ledger.Pending.BeginConflictTransfer("/held", "/survivor", gen, 0, ledger.Shadow)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, tx.MovePayload())
		require.False(t, ledger.Shadow.Has("/held"))
		require.True(t, ledger.Shadow.Has("/survivor"))

		// Replace destination .meta file with a non-empty directory so CommitReversible's
		// atomicWrite fails on rename(EISDIR), exercising pre-publish rollback.
		dstMetaPath := filepath.Join(dir, "pending", staging.HashPath("/survivor")+".meta")
		require.NoError(t, os.Remove(dstMetaPath))
		require.NoError(t, os.MkdirAll(filepath.Join(dstMetaPath, "blocker"), 0o755))

		committed, cerr := tx.CommitReversible()
		require.Error(t, cerr)
		require.False(t, committed)

		// Source shadow + live PendingOverwrite pairing must be completely restored!
		require.True(t, ledger.Shadow.Has("/held"))
		require.False(t, ledger.Shadow.Has("/survivor"))
		gotBytes, rerr := ledger.Shadow.ReadAll("/held")
		require.NoError(t, rerr)
		require.Equal(t, string(payload), string(gotBytes))
		meta, mok := ledger.Pending.GetMeta("/held")
		require.True(t, mok)
		require.Equal(t, staging.PendingOverwrite, meta.Kind)
		require.False(t, ledger.Pending.HasPending("/survivor"))
	})

	t.Run("open_hardlink_survivor_migrates_and_allows_switch", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/held", []byte("v0"))
		b.Seed("/survivor", []byte("v0"))
		srv := httptest.NewServer(b)
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		ledger, err := staging.NewStaging(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(ledger.Close)
		wb, err := writeback.New(writeback.Options{
			Policy:   writeback.PolicyWriteBack,
			SyncMode: "interactive",
			Client:   cl,
			Staging:  ledger,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, DefaultPermissions: true})
		require.NoError(t, err)

		ino := lookupFacade(t, f, "held")
		hdr := gofuse.InHeader{NodeId: ino}
		var openOut gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &openOut))
		require.True(t, f.inodes.AddAlias(ino, "/survivor", "", 2, false, 2, time.Now()))

		want := []byte("migrated-to-survivor")
		_, st := f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: openOut.Fh}, want)
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: hdr, Fh: openOut.Fh}))
		require.Equal(t, gofuse.OK, f.Unlink(nil, rootHeader(), "held"))

		f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: openOut.Fh})
		dresp := f.Drain(context.Background())
		require.True(t, dresp.OK, dresp.Error)
		require.Equal(t, 0, ledger.Pending.Count())
		require.Zero(t, ledger.Shadow.PendingBytes())
		assertSwitchGatesClean(t, f)

		survNode, sok := b.Node("/survivor")
		require.True(t, sok)
		require.Equal(t, string(want), string(survNode.Data))
	})

	t.Run("concurrent_dual_hardlink_unlink_and_rename_race", func(t *testing.T) {
		for iter := 0; iter < 15; iter++ {
			b := testbackend.NewBackend()
			b.Seed("/a", []byte("seed"))
			b.Seed("/b", []byte("seed"))
			srv := httptest.NewServer(b)

			cl := client.New(srv.URL, "")
			cl.SetSmallFileThresholdForTests(1 << 20)
			ledger, err := staging.NewStaging(t.TempDir())
			require.NoError(t, err)
			wb, err := writeback.New(writeback.Options{
				Policy:   writeback.PolicyWriteBack,
				SyncMode: "interactive",
				Client:   cl,
				Staging:  ledger,
			})
			require.NoError(t, err)
			terminal, err := core.New(cl, core.Options{})
			require.NoError(t, err)
			f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, DefaultPermissions: true})
			require.NoError(t, err)

			ino := lookupFacade(t, f, "a")
			require.True(t, f.inodes.AddAlias(ino, "/b", "", 2, false, 4, time.Now()))
			hdr := gofuse.InHeader{NodeId: ino}
			var oa, ob gofuse.OpenOut
			require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &oa))
			require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &ob))
			_, _ = f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: oa.Fh}, []byte("edit-from-a"))
			_ = f.Fsync(nil, &gofuse.FsyncIn{InHeader: hdr, Fh: oa.Fh})

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				<-start
				_ = f.Unlink(nil, rootHeader(), "a")
			}()
			go func() {
				defer wg.Done()
				<-start
				if iter%2 == 0 {
					_ = f.Unlink(nil, rootHeader(), "b")
				} else {
					ri := &gofuse.RenameIn{InHeader: *rootHeader(), Newdir: 1}
					_ = f.Rename(nil, ri, "b", "c")
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				_ = f.flushOpenHandles(context.Background())
				buf := make([]byte, 16)
				_, _ = f.Read(nil, &gofuse.ReadIn{InHeader: hdr, Fh: ob.Fh, Size: 16}, buf)
			}()
			close(start)
			wg.Wait()

			f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: oa.Fh})
			f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: ob.Fh})
			dresp := f.Drain(context.Background())
			require.True(t, dresp.OK, dresp.Error)

			if iter%2 == 0 {
				// Both /a and /b were unlinked -> neither may resurrect on backend!
				_, hasA := b.Node("/a")
				_, hasB := b.Node("/b")
				require.False(t, hasA, "iter %d: /a resurrected after dual hardlink unlink", iter)
				require.False(t, hasB, "iter %d: /b resurrected after dual hardlink unlink", iter)
			} else {
				_, hasA := b.Node("/a")
				require.False(t, hasA, "iter %d: /a resurrected after unlink+rename", iter)
				nodeC, hasC := b.Node("/c")
				require.True(t, hasC, "iter %d: /c missing after unlink(/a)+rename(/b->/c)", iter)
				require.Equal(t, "edit-from-a", string(nodeC.Data))
			}

			_ = wb.Stop(context.Background())
			ledger.Close()
			srv.Close()
		}
	})
}

type failingStopHandler struct {
	fs.HandlerFunc
	stopErr error
}

func (h *failingStopHandler) Start(context.Context) error { return nil }
func (h *failingStopHandler) Drain(context.Context) error { return nil }
func (h *failingStopHandler) Stop(context.Context) error  { return h.stopErr }

// TestSelfFenceStep2ToStep3AbortAndPendingTargetStopError covers:
//  1. wbCache-only mount self-fenced between Step 2 and Step 3 preserves wbCache;
//  2. Step 2 abort() when binding.fenced flips BEFORE AbandonLayerForRollback()
//     marks stripped entry as PendingConflict instead of re-enqueuing;
//  3. Hardlink survivor self-fenced before commit preserves source as PendingConflict;
//  4. Closed locally-staged OpUnlink fenced during StatCtx preserves local recovery data;
//  5. Mount switch pendingTarget.stop failure transitions to CodeSwitchRecovering.
func TestSelfFenceStep2ToStep3AbortAndPendingTargetStopError(t *testing.T) {
	t.Run("wbcache_only_fenced_between_step2_and_step3", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/held", []byte("v0"))
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut || r.Method == http.MethodPost {
				http.Error(w, `{"code":"CONFLICT","message":"conflict"}`, http.StatusConflict)
				return
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		wbCache, err := staging.NewWriteBackCache(filepath.Join(t.TempDir(), "wbc"))
		require.NoError(t, err)
		wb, err := writeback.New(writeback.Options{
			Policy:     writeback.PolicyWriteBack,
			Client:     cl,
			WriteCache: wbCache,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })

		binding := &layerMountBinding{}
		hook := &round9UnlinkHook{
			Writeback: wb,
			beforeCommit: func() {
				// Simulate self-fence setting binding.fenced right after Step 2
				// released fenceMu, BEFORE AbandonLayerForRollback runs!
				binding.fenceMu.Lock()
				binding.fenced.Store(true)
				binding.fenceMu.Unlock()
			},
		}
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{hook, terminal}, DefaultPermissions: true})
		require.NoError(t, err)
		f.layerFence = binding

		ino := lookupFacade(t, f, "held")
		hdr := gofuse.InHeader{NodeId: ino}
		var openOut gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &openOut))
		_, st := f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: openOut.Fh}, []byte("cached-bytes"))
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Flush(nil, &gofuse.FlushIn{InHeader: hdr, Fh: openOut.Fh}))
		require.True(t, wbCache.HasMeta("/held"))

		require.Equal(t, gofuse.EIO, f.Unlink(nil, rootHeader(), "held"))
		require.True(t, wbCache.HasMeta("/held"), "wbCache recovery data must survive fenced unlink")
		require.True(t, f.openIdx.HasOpenPath("/held"))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: openOut.Fh})
	})

	t.Run("step2_abort_before_abandon_layer_marks_pending_conflict", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/held", []byte("v0"))
		var holdPut atomic.Bool
		holdPut.Store(true)
		releasePut := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Method == http.MethodPut || r.Method == http.MethodPost) && holdPut.Load() {
				select {
				case <-r.Context().Done():
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-releasePut:
				}
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		ledger, err := staging.NewStaging(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(ledger.Close)
		wb, err := writeback.New(writeback.Options{
			Policy:                  writeback.PolicyWriteBack,
			SyncMode:                "interactive",
			Client:                  cl,
			Staging:                 ledger,
			RemoteCommitWaitTimeout: 200 * time.Millisecond,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		t.Cleanup(func() {
			holdPut.Store(false)
			close(releasePut)
		})

		binding := &layerMountBinding{}
		hook := &round9UnlinkHook{
			Writeback: wb,
			afterAcquire: func() {
				// Flip binding.fenced BEFORE AbandonLayerForRollback runs so Step 2
				// invokes abortUnlink() while wb.rolledBack is still false!
				binding.fenceMu.Lock()
				binding.fenced.Store(true)
				binding.fenceMu.Unlock()
			},
		}
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{hook, terminal}, DefaultPermissions: true})
		require.NoError(t, err)
		f.layerFence = binding

		ino := lookupFacade(t, f, "held")
		hdr := gofuse.InHeader{NodeId: ino}
		var openOut gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &openOut))
		_, st := f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: openOut.Fh}, []byte("fenced-v1"))
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: hdr, Fh: openOut.Fh}))

		require.Equal(t, gofuse.EIO, f.Unlink(nil, rootHeader(), "held"))
		meta, ok := ledger.Pending.GetMeta("/held")
		require.True(t, ok)
		require.Equal(t, staging.PendingConflict, meta.Kind, "stripped entry must be preserved as PendingConflict")
		require.True(t, ledger.Shadow.Has("/held"))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: openOut.Fh})
	})

	t.Run("hardlink_survivor_fenced_before_commit_marks_source_conflict_no_split_brain", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/held", []byte("v0"))
		b.Seed("/survivor", []byte("v0"))
		var holdPut atomic.Bool
		holdPut.Store(true)
		releasePut := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Method == http.MethodPut || r.Method == http.MethodPost) && holdPut.Load() {
				select {
				case <-r.Context().Done():
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-releasePut:
				}
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		ledger, err := staging.NewStaging(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(ledger.Close)
		wb, err := writeback.New(writeback.Options{
			Policy:                  writeback.PolicyWriteBack,
			SyncMode:                "interactive",
			Client:                  cl,
			Staging:                 ledger,
			RemoteCommitWaitTimeout: 200 * time.Millisecond,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		t.Cleanup(func() {
			holdPut.Store(false)
			close(releasePut)
		})

		binding := &layerMountBinding{}
		binding.OnSelfFence(wb.AbandonLayerForRollback)
		hook := &round9UnlinkHook{
			Writeback: wb,
			beforeCommit: func() {
				binding.triggerSelfFence("lease lost between step2 and step3")
			},
		}
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{hook, terminal}, DefaultPermissions: true})
		require.NoError(t, err)
		f.layerFence = binding

		ino := lookupFacade(t, f, "held")
		hdr := gofuse.InHeader{NodeId: ino}
		var openOut gofuse.OpenOut
		require.Equal(t, gofuse.OK, f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: syscall.O_RDWR}, &openOut))
		require.True(t, f.inodes.AddAlias(ino, "/survivor", "", 2, false, 2, time.Now()))
		_, st := f.Write(nil, &gofuse.WriteIn{InHeader: hdr, Fh: openOut.Fh}, []byte("hardlink-fenced"))
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: hdr, Fh: openOut.Fh}))

		require.Equal(t, gofuse.EIO, f.Unlink(nil, rootHeader(), "held"))
		meta, ok := ledger.Pending.GetMeta("/held")
		require.True(t, ok)
		require.Equal(t, staging.PendingConflict, meta.Kind)
		require.True(t, ledger.Shadow.Has("/held"))
		require.False(t, ledger.Pending.HasPending("/survivor"))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: hdr, Fh: openOut.Fh})
	})

	t.Run("closed_local_staged_unlink_fenced_during_stat_preserves_recovery_data", func(t *testing.T) {
		b := testbackend.NewBackend()
		var (
			fenceOnStat atomic.Bool
			holdPut     atomic.Bool
			binding     = &layerMountBinding{}
			releasePut  = make(chan struct{})
		)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Method == http.MethodPut || r.Method == http.MethodPost) && holdPut.Load() {
				select {
				case <-r.Context().Done():
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-releasePut:
				}
			}
			if (r.Method == http.MethodGet || r.Method == http.MethodHead) && fenceOnStat.CompareAndSwap(true, false) {
				binding.triggerSelfFence("fenced during non-open unlink stat")
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		cl := client.New(srv.URL, "")
		cl.SetSmallFileThresholdForTests(1 << 20)
		ledger, err := staging.NewStaging(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(ledger.Close)
		wb, err := writeback.New(writeback.Options{
			Policy:                  writeback.PolicyWriteBack,
			SyncMode:                "interactive",
			Client:                  cl,
			Staging:                 ledger,
			RemoteCommitWaitTimeout: 200 * time.Millisecond,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		t.Cleanup(func() {
			holdPut.Store(false)
			close(releasePut)
		})
		binding.OnSelfFence(wb.AbandonLayerForRollback)
		terminal, err := core.New(cl, core.Options{})
		require.NoError(t, err)
		f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, DefaultPermissions: true})
		require.NoError(t, err)
		f.layerFence = binding

		var cout gofuse.CreateOut
		ci := &gofuse.CreateIn{InHeader: *rootHeader(), Flags: syscall.O_CREAT | syscall.O_RDWR | syscall.O_EXCL, Mode: 0o644}
		require.Equal(t, gofuse.OK, f.Create(nil, ci, "local_only", &cout))
		holdPut.Store(true)
		chdr := gofuse.InHeader{NodeId: cout.NodeId}
		_, st := f.Write(nil, &gofuse.WriteIn{InHeader: chdr, Fh: cout.Fh}, []byte("must-survive-fence"))
		require.Equal(t, gofuse.OK, st)
		require.Equal(t, gofuse.OK, f.Fsync(nil, &gofuse.FsyncIn{InHeader: chdr, Fh: cout.Fh}))
		f.Release(nil, &gofuse.ReleaseIn{InHeader: chdr, Fh: cout.Fh})

		// Dispatch OpUnlink directly so f.layerFenced() top-level check doesn't
		// short-circuit before StatCtx triggers self-fence mid-flight.
		fenceOnStat.Store(true)
		_, derr := f.dispatchErr(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: "/local_only", Ino: 1})
		require.Error(t, derr)
		require.True(t, ledger.Pending.HasPending("/local_only"))
		require.True(t, ledger.Shadow.Has("/local_only"))
	})

	t.Run("mount_switch_pending_target_stop_failure_returns_recovering", func(t *testing.T) {
		sw, _, _, _ := viewSwitchFixture(t)
		dec, _, err := sw.journal.Decide(
			"op_stop_fail",
			sw.journal.Snapshot().Generation,
			mountstate.SwitchBinding{LayerID: "layer_01j00000000000000000000001", ReadOnly: true},
		)
		require.NoError(t, err)

		failFS, err := New(Options{
			Extensions: []fs.Handler{&failingStopHandler{
				HandlerFunc: func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) { return next(ctx, op) },
				stopErr:     errors.New("injected pendingTarget stop failure"),
			}},
		})
		require.NoError(t, err)

		sw.mu.Lock()
		sw.pendingTarget = &mountRuntime{fs: failFS}
		resp := sw.markRevokedLocked(dec, errors.New("lease revoked"))
		sw.mu.Unlock()

		require.Equal(t, mountcontrol.CodeSwitchRecovering, resp.Code)
		require.Contains(t, resp.Error, "injected pendingTarget stop failure")
	})
}
