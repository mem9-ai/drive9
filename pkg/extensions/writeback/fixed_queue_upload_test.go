package writeback

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func fixedQueueRuntime(t *testing.T, c *client.Client, dir string) (*Writeback, *staging.Staging) {
	t.Helper()
	ledger, err := staging.NewStagingWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(Options{Client: c, Staging: ledger, RemoteRoot: "/team", Policy: PolicyWriteBack, UploadConcurrency: 1})
	if err != nil {
		ledger.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop(context.Background()); ledger.Close() })
	return w, ledger
}
func stageFixedQueueImage(t *testing.T, l *staging.Staging, data []byte, base int64, mode bool) *CommitEntry {
	t.Helper()
	kind := staging.PendingNew
	if base > 0 {
		kind = staging.PendingOverwrite
	}
	if err := l.Shadow.WriteFull("/file", data, base); err != nil {
		t.Fatal(err)
	}
	if err := l.Shadow.Sync("/file"); err != nil {
		t.Fatal(err)
	}
	gen, err := l.Pending.PutWithBaseRevAndMode("/file", int64(len(data)), kind, base, 0, mode)
	if err != nil {
		t.Fatal(err)
	}
	return &CommitEntry{Path: "/file", Size: int64(len(data)), BaseRev: base, Kind: kind, Mode: 0, HasMode: mode, PendingIndexGen: gen, ShadowGen: l.Shadow.ActiveGeneration("/file"), ShadowSpill: true}
}
func TestFixedQueueActualSyncAndWorker(t *testing.T) {
	for _, worker := range []bool{false, true} {
		for _, size := range []int{0, (4 << 20) + 7} {
			t.Run(map[bool]string{true: "worker", false: "sync"}[worker]+"/"+map[bool]string{true: "empty", false: "multi"}[size == 0], func(t *testing.T) {
				api := &fixedCacheAPI{t: t, size: int64(size)}
				server := httptest.NewServer(http.HandlerFunc(api.serve))
				defer server.Close()
				c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
				w, l := fixedQueueRuntime(t, c, t.TempDir())
				api.journal = l.Journal
				if err := w.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				entry := stageFixedQueueImage(t, l, bytes.Repeat([]byte("x"), size), 0, true)
				var revision atomic.Int64
				w.queue.OnSuccess = func(e *CommitEntry, rev int64) {
					revision.Store(rev)
					if l.Shadow.Has(e.Path) || l.Pending.HasPending(e.Path) {
						t.Error("callback preceded cleanup")
					}
				}
				if worker {
					w.queue.ConfigureBatchWrite(time.Millisecond, 4, 16<<20)
					if err := w.queue.Enqueue(entry); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := w.queue.WaitIdle(ctx); err != nil {
						t.Fatal(err)
					}
				} else if err := w.queue.CommitNow(context.Background(), entry); err != nil {
					t.Fatal(err)
				}
				if revision.Load() != 8 || l.Shadow.Has("/file") || l.Pending.HasPending("/file") {
					t.Fatal("not settled", revision.Load())
				}
				records, err := l.Journal.PendingFixedUploads()
				if err != nil || len(records) != 0 {
					t.Fatal(records, err)
				}
				if size > 0 && api.transfers.Load() != 2 {
					t.Fatal("did not stream both parts", api.transfers.Load())
				}
			})
		}
	}
}
func TestFixedQueueStartReplaysOriginalWithoutSource(t *testing.T) {
	for _, phase := range []string{"complete", "mode", "cleanup", "empty"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			size := int64(6)
			if phase == "empty" {
				size = 0
			}
			api := &fixedCacheAPI{t: t, size: size}
			api.failComplete.Store(phase == "complete" || phase == "empty")
			api.failMode.Store(phase == "mode")
			server := httptest.NewServer(http.HandlerFunc(api.serve))
			defer server.Close()
			c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			w, l := fixedQueueRuntime(t, c, dir)
			api.journal = l.Journal
			entry := stageFixedQueueImage(t, l, bytes.Repeat([]byte("x"), int(size)), 0, true)
			blocker := filepath.Join(l.Pending.Dir(), staging.HashPath("/file")+".meta")
			if phase == "cleanup" {
				api.afterComplete = func() {
					if err := os.Remove(blocker); err != nil && !errors.Is(err, os.ErrNotExist) {
						t.Error(err)
					}
					if err := os.Mkdir(blocker, 0700); err != nil {
						t.Error(err)
					}
					if err := os.WriteFile(filepath.Join(blocker, "block"), nil, 0600); err != nil {
						t.Error(err)
					}
				}
			}
			if err := w.queue.CommitNow(context.Background(), entry); err == nil {
				t.Fatal("unknown outcome succeeded")
			}
			if err := w.queue.WaitIdle(context.Background()); err == nil {
				t.Fatal("pending failure reported idle success")
			}
			if unlock, err := w.lockFixedNamespace("/file"); !errors.Is(err, syscall.EAGAIN) {
				if unlock != nil {
					unlock()
				}
				t.Fatal(err)
			}
			first := api.completeIDs[0]
			_ = w.Stop(context.Background())
			l.Close()
			if phase == "cleanup" {
				if err := os.RemoveAll(blocker); err != nil {
					t.Fatal(err)
				}
			}
			api.afterComplete = nil
			api.failComplete.Store(false)
			api.failMode.Store(false)
			fresh, l2 := fixedQueueRuntime(t, c, dir)
			api.journal = l2.Journal
			if err := fresh.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if l2.Pending.HasPending("/file") || l2.Shadow.Has("/file") {
				t.Fatal("old local image survived")
			}
			records, err := l2.Journal.PendingFixedUploads()
			if err != nil || len(records) != 0 {
				t.Fatal(records, err)
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			for _, id := range api.completeIDs {
				if id != first {
					t.Fatal("changed completion identity")
				}
			}
			wantInit := 1
			if size == 0 {
				wantInit = 0
			}
			if len(api.initIDs) != wantInit {
				t.Fatal("reinitialized on recovery", api.initIDs)
			}
			if len(api.modeIDs) > 1 && api.modeIDs[0] != api.modeIDs[len(api.modeIDs)-1] {
				t.Fatal("changed mode identity")
			}
		})
	}
}
func TestFixedQueueRecoveryPreservesNewerOwner(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
	api.failComplete.Store(true)
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	w, l := fixedQueueRuntime(t, c, t.TempDir())
	api.journal = l.Journal
	old := stageFixedQueueImage(t, l, []byte("before"), 0, false)
	if err := w.queue.CommitNow(context.Background(), old); err == nil {
		t.Fatal("unknown completion succeeded")
	}
	newer := stageFixedQueueImage(t, l, []byte("newer bytes"), 7, false)
	api.failComplete.Store(false)
	// Recovery settles only the immutable old publication, without fresh upload.
	if err := w.recoverFixedPublications(context.Background()); err != nil {
		t.Fatal(err)
	}
	if meta, ok := l.Pending.GetMeta("/file"); !ok || meta.Generation != newer.PendingIndexGen {
		t.Fatal(meta, ok)
	}
	if got, err := l.Shadow.ReadAll("/file"); err != nil || string(got) != "newer bytes" {
		t.Fatal(string(got), err)
	}
	if api.transfers.Load() != 1 {
		t.Fatal("read changed payload")
	}
}

func TestFixedQueueStartupPreservesLaterMetadataFromWAL(t *testing.T) {
	dir := t.TempDir()
	api := &fixedCacheAPI{t: t, size: 6}
	api.failComplete.Store(true)
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	w, l := fixedQueueRuntime(t, c, dir)
	api.journal = l.Journal
	entry := stageFixedQueueImage(t, l, []byte("before"), 0, false)
	if err := w.queue.CommitNow(context.Background(), entry); err == nil {
		t.Fatal("unknown completion succeeded")
	}
	gen, err := l.Pending.PutWithBaseRevAndMode("/file", 6, staging.PendingOverwrite, 7, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Stop(context.Background())
	l.Close()
	api.failComplete.Store(false)
	api.failInit.Store(true)
	api.expected = 7
	fresh, l2 := fixedQueueRuntime(t, c, dir)
	api.journal = l2.Journal
	// This is also called by mount before building the writeback extension.
	if err := l2.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fresh.queue.WaitIdle(ctx); err == nil {
		t.Fatal("newer pending owner disappeared")
	}
	if meta, ok := l2.Pending.GetMeta("/file"); !ok || meta.Generation != gen || !meta.HasMode {
		t.Fatal(meta, ok)
	}
	if got, err := l2.Shadow.ReadAll("/file"); err != nil || string(got) != "before" {
		t.Fatal(string(got), err)
	}
	records, err := l2.Journal.PendingFixedUploads()
	if err != nil || len(records) != 0 {
		t.Fatal(records, err)
	}
}

func TestFixedQueueCanceledWhileNamespaceLockedDoesNotDeadlock(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	w, l := fixedQueueRuntime(t, c, t.TempDir())
	api.journal = l.Journal
	entry := stageFixedQueueImage(t, l, []byte("before"), 0, false)
	q := w.queue
	q.fixedNamespaceMu.Lock()
	done := make(chan struct{})
	go func() { q.commitFixedWorker(entry); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		q.mu.Lock()
		ready := entry.cancelCommit != nil
		if ready {
			entry.canceled = true
			entry.cancelCommit()
		}
		q.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			q.fixedNamespaceMu.Unlock()
			t.Fatal("worker did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		q.fixedNamespaceMu.Unlock()
		t.Fatal("cancellation waited for namespace unlock")
	}
	q.fixedNamespaceMu.Unlock()
	if !l.Pending.HasPending("/file") || !l.Shadow.Has("/file") {
		t.Fatal("cancellation dropped data")
	}
	if len(api.initIDs) != 0 {
		t.Fatal("canceled request uploaded")
	}
}

func TestFixedQueueSettlesPriorCacheBeforePublishingShadow(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
	api.failComplete.Store(true)
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(Options{Client: c, Staging: ledger, WriteCache: cache, RemoteRoot: "/team", Policy: PolicyWriteBack})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop(context.Background()) }()
	api.journal = ledger.Journal
	cacheFixedImage(t, cache, []byte("before"), false)
	if err := w.uploader.UploadSync(context.Background(), "/file"); err == nil {
		t.Fatal("unknown completion succeeded")
	}
	entry := stageFixedQueueImage(t, ledger, []byte("newest"), 7, false)
	api.failComplete.Store(false)
	api.expected = 7
	if w.queue.fixedNamespaceMu != &w.uploader.fixedNamespaceMu {
		t.Fatal("owners have different namespace locks")
	}
	if err := w.queue.CommitNow(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	if len(api.initIDs) != 2 || len(api.completeIDs) != 3 || api.completeIDs[0] != api.completeIDs[1] || api.completeIDs[1] == api.completeIDs[2] {
		t.Fatal("lost owner handoff", api.initIDs, api.completeIDs)
	}
	if cache.MetaGeneration("/file") != 0 || ledger.Pending.HasPending("/file") || ledger.Shadow.Has("/file") {
		t.Fatal("unfinished owner cleanup")
	}
}

func TestFixedQueueNamespaceCannotPassUnknownCompletion(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
	api.failComplete.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	api.beforeComplete = func() { close(entered); <-release }
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	w, l := fixedQueueRuntime(t, c, t.TempDir())
	api.journal = l.Journal
	entry := stageFixedQueueImage(t, l, []byte("before"), 0, false)
	completed := make(chan error, 1)
	go func() { completed <- w.queue.CommitNow(context.Background(), entry) }()
	<-entered
	namespace := make(chan error, 1)
	go func() {
		unlock, err := w.lockFixedNamespace("/file")
		if unlock != nil {
			unlock()
		}
		namespace <- err
	}()
	select {
	case err := <-namespace:
		close(release)
		t.Fatal("namespace passed active publication", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-completed; err == nil {
		t.Fatal("failed completion succeeded")
	}
	select {
	case err := <-namespace:
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("namespace did not return")
	}
}

func TestFixedQueueMainKeepsConditionalPUTAndNoPublicationRecords(t *testing.T) {
	for _, worker := range []bool{false, true} {
		t.Run(map[bool]string{true: "worker", false: "sync"}[worker], func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != "PUT" || r.URL.Path != "/v1/fs/team/file" || r.Header.Get("X-Dat9-Expected-Revision") != "7" || r.Header.Get(viewprotocol.ViewHeader) != "" {
					t.Error("changed main request", r.Method, r.URL.Path)
				}
				_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
			}))
			defer server.Close()
			c := newTestClient(server.URL)
			w, l := fixedQueueRuntime(t, c, t.TempDir())
			entry := stageFixedQueueImage(t, l, []byte("before"), 7, false)
			entry.ShadowSpill = false
			var rev atomic.Int64
			w.queue.OnSuccess = func(_ *CommitEntry, r int64) { rev.Store(r) }
			if worker {
				if err := w.queue.Enqueue(entry); err != nil {
					t.Fatal(err)
				}
				w.queue.DrainAll()
			} else {
				if err := w.queue.CommitNow(context.Background(), entry); err != nil {
					t.Fatal(err)
				}
			}
			if requests.Load() != 1 || rev.Load() != 8 || l.Pending.HasPending("/file") || l.Shadow.Has("/file") {
				t.Fatal("main commit changed", requests.Load(), rev.Load())
			}
			records, err := l.Journal.PendingFixedUploads()
			if err != nil || len(records) != 0 {
				t.Fatal(records, err)
			}
			entries, err := os.ReadDir(filepath.Join(l.Pending.Dir(), "..", "shadow"))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".fixed-upload") {
					t.Fatal("main created a fixed identity")
				}
			}
		})
	}
}

// A queue WAL can coexist with the .dat/.meta pair left by an interrupted
// cache-to-queue handoff. Start must fix ownership before replay removes pending.
func TestFixedQueueStartRetiresOnlySupersededCache(t *testing.T) {
	for _, scenario := range []string{"stale", "new_after_snapshot", "new_before_restart", "cleanup_failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			api := &fixedCacheAPI{t: t, size: 6}
			api.failComplete.Store(true)
			ts := httptest.NewServer(http.HandlerFunc(api.serve))
			defer ts.Close()
			c, _ := client.New(ts.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			open := func() (*Writeback, *staging.Staging) {
				l, err := staging.NewStagingWithQuota(filepath.Join(dir, "ledger"), 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				w, err := New(Options{Client: c, Staging: l, WriteCacheDir: filepath.Join(dir, "cache"), RemoteRoot: "/team", Policy: PolicyWriteBack, UploadConcurrency: 1})
				if err != nil {
					l.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = w.Stop(context.Background()); l.Close() })
				api.journal = l.Journal
				return w, l
			}
			put := func(w *Writeback, data string) uint64 {
				if err := w.wbCache.PutWithBaseRevAndMode("/file", []byte(data), int64(len(data)), staging.PendingNew, 0, 0, false); err != nil {
					t.Fatal(err)
				}
				m, _ := w.wbCache.GetMeta("/file")
				return m.Generation
			}
			w, l := open()
			oldGen := put(w, "stale!")
			entry := stageFixedQueueImage(t, l, []byte("queued"), 0, false)
			if err := w.queue.CommitNow(context.Background(), entry); err == nil {
				t.Fatal("completion loss succeeded")
			}
			if scenario == "new_before_restart" {
				gen := put(w, "newer cache")
				if !w.wbCache.MarkSupersedesPendingGen("/file", entry.PendingIndexGen, gen) {
					t.Fatal("mark newer cache")
				}
			}
			_ = w.Stop(context.Background())
			l.Close()
			fresh, l2 := open()
			api.failComplete.Store(false)
			if scenario == "new_after_snapshot" {
				api.beforeComplete = func() { put(fresh, "newer cache") }
			}
			if scenario == "cleanup_failure" {
				// Meta deletion succeeds, but payload removal fails. No completion
				// replay or pending retirement may precede checked cache cleanup.
				blocked := fresh.wbCache.DatFile("/file")
				if err := os.Remove(blocked); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(blocked, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(blocked, "block"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				if err := fresh.Start(context.Background()); err == nil {
					t.Fatal("cleanup failure ignored")
				}
				if !l2.Pending.HasPending("/file") || !l2.Shadow.Has("/file") {
					t.Fatal("removed recovery owner on cleanup failure")
				}
				records, err := l2.Journal.PendingFixedUploads()
				if err != nil || len(records) != 1 {
					t.Fatal(records, err)
				}
				if len(api.completeIDs) != 1 {
					t.Fatal("replayed before cleanup succeeded")
				}
				_ = fresh.Stop(context.Background())
				l2.Close()
				if err := os.RemoveAll(blocked); err != nil {
					t.Fatal(err)
				}
				fresh, l2 = open()
			}
			if err := fresh.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := fresh.queue.WaitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			newer := strings.HasPrefix(scenario, "new_")
			if err := fresh.uploader.WaitIdle(ctx); (err != nil) != newer {
				t.Fatal("only a retained newer cache should remain unresolved", err)
			}
			if l2.Pending.HasPending("/file") || l2.Shadow.Has("/file") {
				t.Fatal("queue image not retired")
			}
			records, err := l2.Journal.PendingFixedUploads()
			if err != nil || len(records) != 0 {
				t.Fatal(records, err)
			}
			m, data, exists := fresh.wbCache.GetMetaAndView("/file")
			if newer {
				if !exists || m.Generation <= oldGen || string(data) != "newer cache" {
					t.Fatal("newer cache lost", m, string(data), exists)
				}
			} else if exists {
				t.Fatal("stale cache survived", m)
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			if len(api.initIDs) != 1 || api.transfers.Load() != 1 || len(api.completeIDs) != 2 || api.completeIDs[0] != api.completeIDs[1] {
				t.Fatal("recovery uploaded stale cache or changed identity", api.initIDs, api.completeIDs)
			}
		})
	}
}
