package writeback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type fixedCacheAPI struct {
	t                                *testing.T
	journal                          *staging.Journal
	size                             int64
	expected                         int64
	mu                               sync.Mutex
	initIDs, completeIDs, modeIDs    []string
	transfers                        atomic.Int32
	failInit, failComplete, failMode atomic.Bool
	afterComplete                    func()
	beforeComplete                   func()
	modeStatus                       atomic.Int32
}

func (a *fixedCacheAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := r.Header.Get(viewprotocol.OperationHeader)
	if r.Header.Get(viewprotocol.ViewHeader) != "a" || r.Header.Get(viewprotocol.WorkHeader) != "work" {
		a.t.Error("lost view binding")
	}
	switch r.URL.Path {
	case "/v1/views/a/v2/uploads/initiate":
		a.initIDs = append(a.initIDs, id)
		var req struct {
			Path     string
			Size     int64 `json:"total_size"`
			Revision int64 `json:"expected_revision"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path != "/team/file" || req.Size != a.size || req.Revision != a.expected {
			a.t.Error(req, err)
		}
		if a.failInit.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "upload", "content_layout": "extent", "part_size": 4 << 20, "total_parts": (a.size-1)/(4<<20) + 1, "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "checksum_contract": map[string]any{"required": true, "supported": []string{"SHA-256"}}})
	case "/v1/views/a/v2/uploads/upload/presign":
		a.transfers.Add(1)
		var req struct {
			Number   int `json:"part_number"`
			Size     int64
			Checksum struct{ Value string }
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			a.t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(client.PartURL{Number: req.Number, Size: min(int64(4<<20), a.size-int64(req.Number-1)*(4<<20)), ChecksumSHA256: req.Checksum.Value, SkipUpload: true, ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)})
	case "/v1/views/a/v2/uploads/upload/complete":
		a.completeIDs = append(a.completeIDs, id)
		if a.beforeComplete != nil {
			a.beforeComplete()
		}
		records, err := a.journal.PendingFixedUploads()
		if err != nil || len(records) != 1 || records[0].OperationID != id || a.journal.DurableSeq() < records[0].Seq {
			a.t.Error("completion precedes durable identity", len(records), a.journal.DurableSeq(), err)
		}
		if a.failComplete.Load() {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: id, View: client.FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 7, Size: a.size})
		if a.afterComplete != nil {
			a.afterComplete()
		}
	case "/v1/views/a/v1/fs/team/file":
		if r.Method == "PUT" {
			a.completeIDs = append(a.completeIDs, id)
			records, err := a.journal.PendingFixedUploads()
			if a.size != 0 || r.ContentLength != 0 || r.Header.Get("X-Dat9-Expected-Revision") != strconv.FormatInt(a.expected, 10) || err != nil || len(records) != 1 || records[0].OperationID != id || a.journal.DurableSeq() < records[0].Seq {
				a.t.Error("empty write preceded original durable request", err)
			}
			if a.failComplete.Load() {
				w.WriteHeader(503)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "ok", OperationID: id, View: client.FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 7, Size: 0})
			return
		}
		if r.Method != "POST" || r.URL.RawQuery != "chmod" {
			a.t.Error("unexpected file mutation", r.Method, r.URL)
			w.WriteHeader(400)
			return
		}
		a.modeIDs = append(a.modeIDs, id)
		var req struct {
			Mode     uint32
			Resource string `json:"resource_id"`
			Revision int64  `json:"expected_revision"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Mode != 0 || req.Resource != "inode" || req.Revision != 7 {
			a.t.Error("changed chmod condition", req, err)
		}
		if status := a.modeStatus.Load(); status != 0 {
			w.WriteHeader(int(status))
			return
		}
		if a.failMode.Load() {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "operation_id": id, "view": client.FixedViewState{LayerID: "a", Generation: 3, Fence: 1}, "resource_id": "inode", "revision": 8, "size_bytes": a.size})
	default:
		a.t.Error("unexpected transfer or fallback", r.Method, r.URL)
		w.WriteHeader(400)
	}
}
func newFixedCacheUploader(t *testing.T, c *client.Client, dir string) (*WriteBackUploader, *staging.Journal, *staging.WriteBackCache) {
	t.Helper()
	journal, err := staging.NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	cache, err := staging.NewWriteBackCache(filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	u := NewWriteBackUploader(c, cache, 1, "/team")
	u.fixedJournal = journal
	t.Cleanup(func() { u.DrainAll(); _ = journal.Close() })
	return u, journal, cache
}
func cacheFixedImage(t *testing.T, cache *staging.WriteBackCache, data []byte, mode bool) {
	t.Helper()
	if err := cache.PutWithBaseRevAndMode("/file", data, int64(len(data)), staging.PendingNew, 0, 0, mode); err != nil {
		t.Fatal(err)
	}
}
func TestFixedCacheUploaderPublishesDurablyAndReturnsModeRevision(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "worker"}[async], func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), (4<<20)+5)
			api := &fixedCacheAPI{t: t, size: int64(len(data))}
			server := httptest.NewServer(http.HandlerFunc(api.serve))
			defer server.Close()
			c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			u, j, cache := newFixedCacheUploader(t, c, t.TempDir())
			api.journal = j
			cacheFixedImage(t, cache, data, true)
			var landed atomic.Int64
			u.OnSuccess = func(m staging.WriteBackMeta, rev int64, g StagingGens) {
				landed.Store(rev)
				if m.Path != "/file" || g.WriteBackGen != m.Generation {
					t.Error(m, g)
				}
			}
			if async {
				u.Submit("/file")
				if err := u.WaitIdle(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				rev, err := u.UploadSyncWithRevision(context.Background(), "/file")
				if err != nil || rev != 8 {
					t.Fatal(rev, err)
				}
			}
			if landed.Load() != 8 || cache.MetaGeneration("/file") != 0 || api.transfers.Load() != 2 {
				t.Fatal(landed.Load(), api.transfers.Load())
			}
			records, err := j.PendingFixedUploads()
			if err != nil || len(records) != 0 {
				t.Fatal(records, err)
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			if len(api.initIDs) != 1 || len(api.completeIDs) != 1 || len(api.modeIDs) != 1 || api.initIDs[0] == api.completeIDs[0] || api.modeIDs[0] == api.completeIDs[0] {
				t.Fatal("wrong phase requests")
			}
		})
	}
}

func TestFixedCacheStartReplaysCompletionAndModeAfterReopen(t *testing.T) {
	for _, phase := range []string{"complete", "chmod"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			api := &fixedCacheAPI{t: t, size: 6}
			api.failComplete.Store(phase == "complete")
			api.failMode.Store(phase == "chmod")
			server := httptest.NewServer(http.HandlerFunc(api.serve))
			defer server.Close()
			c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			u, j, cache := newFixedCacheUploader(t, c, dir)
			api.journal = j
			cacheFixedImage(t, cache, []byte("before"), true)
			if err := u.UploadSync(context.Background(), "/file"); err == nil {
				t.Fatal("unknown outcome returned success")
			}
			if cache.MetaGeneration("/file") == 0 {
				t.Fatal("lost retry data")
			}
			wb := &Writeback{opts: Options{Client: c, Staging: &staging.Staging{Journal: j}}, uploader: u}
			if reason, _ := wb.ReexecRefusal(); reason != "fixed_upload_pending" {
				t.Fatal(reason)
			}
			nextCalls := 0
			if _, err := wb.Do(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: "/file"}, func(context.Context, *fs.Op) (fs.Result, error) { nextCalls++; return fs.Result{}, nil }); !errors.Is(err, syscall.EAGAIN) || nextCalls != 0 {
				t.Fatal(err, nextCalls)
			}
			u.DrainAll()
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			fresh, j2, cache2 := newFixedCacheUploader(t, c, dir)
			api.journal = j2
			startup := &Writeback{opts: Options{Client: c, Staging: &staging.Staging{Journal: j2}}, uploader: fresh}
			// Still unavailable: Start must fail before fresh initiation or transfer.
			if err := startup.Start(context.Background()); err == nil {
				t.Fatal("Start accepted unresolved request")
			}
			api.failComplete.Store(false)
			api.failMode.Store(false)
			if err := startup.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if cache2.MetaGeneration("/file") != 0 {
				t.Fatal("old cache survived successful recovery")
			}
			if records, err := j2.PendingFixedUploads(); err != nil || len(records) != 0 {
				t.Fatal(records, err)
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			if len(api.initIDs) != 1 || api.transfers.Load() != 1 || len(api.completeIDs) != 3 {
				t.Fatal("recovery restarted transfer", len(api.initIDs), api.transfers.Load(), len(api.completeIDs))
			}
			for _, id := range api.completeIDs {
				if id != api.completeIDs[0] {
					t.Fatal("new completion ID")
				}
			}
			for _, id := range api.modeIDs {
				if id != fixedCacheModeID(api.completeIDs[0]) {
					t.Fatal("new chmod ID")
				}
			}
		})
	}
}

func TestFixedCacheUnknownInitiationKeepsIdentityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	api := &fixedCacheAPI{t: t, size: 6}
	api.failInit.Store(true)
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	u, j, cache := newFixedCacheUploader(t, c, dir)
	api.journal = j
	cacheFixedImage(t, cache, []byte("before"), false)
	if err := u.UploadSync(context.Background(), "/file"); err == nil {
		t.Fatal("initiation returned success")
	}
	u.DrainAll()
	_ = j.Close()
	fresh, j2, _ := newFixedCacheUploader(t, c, dir)
	api.journal = j2
	api.failInit.Store(false)
	if err := fresh.UploadSync(context.Background(), "/file"); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.initIDs) != 2 || api.initIDs[0] != api.initIDs[1] || api.transfers.Load() != 1 {
		t.Fatal(api.initIDs, api.transfers.Load())
	}
}

func TestFixedCacheCleanupFailureKeepsRecoveryWithoutPayload(t *testing.T) {
	dir := t.TempDir()
	api := &fixedCacheAPI{t: t, size: 6}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	u, j, cache := newFixedCacheUploader(t, c, dir)
	api.journal = j
	cacheFixedImage(t, cache, []byte("before"), false)
	dat := cache.DatFile("/file")
	api.afterComplete = func() {
		if err := os.Remove(dat); err != nil {
			t.Error(err)
		}
		if err := os.Mkdir(dat, 0700); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(dat, "blocker"), []byte("x"), 0600); err != nil {
			t.Error(err)
		}
	}
	if err := u.UploadSync(context.Background(), "/file"); err == nil {
		t.Fatal("cleanup failure returned success")
	}
	if records, err := j.PendingFixedUploads(); err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	api.afterComplete = nil
	u.DrainAll()
	_ = j.Close()
	if err := os.RemoveAll(dat); err != nil {
		t.Fatal(err)
	}
	fresh, j2, _ := newFixedCacheUploader(t, c, dir)
	api.journal = j2
	if err := fresh.recoverFixedCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if records, err := j2.PendingFixedUploads(); err != nil || len(records) != 0 {
		t.Fatal(records, err)
	}
	if api.transfers.Load() != 1 {
		t.Fatal("reread absent bytes")
	}
}

func TestFixedCacheRecoveryPreservesNewerImage(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
	api.failComplete.Store(true)
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	u, j, cache := newFixedCacheUploader(t, c, t.TempDir())
	api.journal = j
	cacheFixedImage(t, cache, []byte("before"), false)
	original := cache.MetaGeneration("/file")
	if err := u.UploadSync(context.Background(), "/file"); err == nil {
		t.Fatal("unknown completion succeeded")
	}
	cacheFixedImage(t, cache, []byte("new-data"), false)
	latest := cache.MetaGeneration("/file")
	if latest <= original {
		t.Fatal(original, latest)
	}
	api.failComplete.Store(false)
	if err := u.recoverFixedCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if data, ok := cache.Get("/file"); !ok || string(data) != "new-data" || cache.MetaGeneration("/file") != latest {
		t.Fatal("old recovery removed newer image")
	}
	if records, err := j.PendingFixedUploads(); err != nil || len(records) != 0 {
		t.Fatal(records, err)
	}
	if api.transfers.Load() != 1 {
		t.Fatal("recovery uploaded a different image")
	}
}

func TestFixedCacheStartupRejectsOtherBindingOrRemoteRoot(t *testing.T) {
	for _, mismatch := range []string{"work", "root"} {
		t.Run(mismatch, func(t *testing.T) {
			api := &fixedCacheAPI{t: t, size: 6}
			api.failComplete.Store(true)
			server := httptest.NewServer(http.HandlerFunc(api.serve))
			defer server.Close()
			c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			u, j, cache := newFixedCacheUploader(t, c, t.TempDir())
			api.journal = j
			cacheFixedImage(t, cache, []byte("before"), false)
			if err := u.UploadSync(context.Background(), "/file"); err == nil {
				t.Fatal("unknown completion succeeded")
			}
			if mismatch == "work" {
				u.client, _ = client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "new", Fence: 2})
			} else {
				u.remoteRoot = "/other"
			}
			wb := &Writeback{opts: Options{Client: u.client, Staging: &staging.Staging{Journal: j}}, uploader: u}
			if err := wb.Start(context.Background()); err == nil {
				t.Fatal("rebound old request")
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			if len(api.completeIDs) != 1 || len(api.initIDs) != 1 {
				t.Fatal("sent rebound request")
			}
		})
	}
}

func TestFixedCacheFactoryWiresJournalBeforeWorkerSubmission(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
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
	wb, err := New(Options{Client: c, Staging: ledger, WriteCache: cache, RemoteRoot: "/team", Policy: PolicyWriteBack})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	api.journal = ledger.Journal
	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cacheFixedImage(t, cache, []byte("before"), true)
	wb.uploader.Submit("/file")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.MetaGeneration("/file") != 0 {
		t.Fatal("worker did not settle image")
	}
	if reason, _ := wb.ReexecRefusal(); reason != "" {
		t.Fatal(reason)
	}
}

func TestFixedCacheNamespaceWaitsForUnknownPublication(t *testing.T) {
	for _, kind := range []fs.OpKind{fs.OpUnlink, fs.OpRename} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			api := &fixedCacheAPI{t: t, size: 6}
			api.failComplete.Store(true)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			api.beforeComplete = func() { once.Do(func() { close(entered); <-release }) }
			server := httptest.NewServer(http.HandlerFunc(api.serve))
			defer server.Close()
			c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			u, j, cache := newFixedCacheUploader(t, c, t.TempDir())
			api.journal = j
			cacheFixedImage(t, cache, []byte("oldest"), false)
			uploadDone := make(chan error, 1)
			go func() { uploadDone <- u.UploadSync(context.Background(), "/file") }()
			<-entered
			w := &Writeback{opts: Options{Client: c, RemoteRoot: "/team"}, uploader: u, wbCache: cache}
			calls := atomic.Int32{}
			mutationDone := make(chan error, 1)
			go func() {
				_, err := w.Do(context.Background(), &fs.Op{Kind: kind, Path: "/file", Path2: "/new"}, func(context.Context, *fs.Op) (fs.Result, error) { calls.Add(1); return fs.Result{}, nil })
				mutationDone <- err
			}()
			select {
			case err := <-mutationDone:
				close(release)
				t.Fatalf("namespace returned before upload: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-uploadDone; err == nil {
				t.Fatal("unknown completion accepted")
			}
			select {
			case err := <-mutationDone:
				if err == nil {
					t.Fatal("unknown write discarded")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("namespace deadlocked")
			}
			records, err := j.PendingFixedUploads()
			if err != nil || len(records) != 1 || cache.MetaGeneration("/file") == 0 || calls.Load() != 0 {
				t.Fatal("namespace changed unresolved image", len(records), calls.Load(), err)
			}
		})
	}
}

func TestFixedCacheModeConflictRetainsDataAndRecovery(t *testing.T) {
	api := &fixedCacheAPI{t: t, size: 6}
	api.modeStatus.Store(409)
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	u, j, cache := newFixedCacheUploader(t, c, t.TempDir())
	api.journal = j
	cacheFixedImage(t, cache, []byte("oldest"), true)
	calls := 0
	u.OnSuccess = func(staging.WriteBackMeta, int64, StagingGens) { calls++ }
	if err := u.UploadSync(context.Background(), "/file"); err == nil {
		t.Fatal("accepted changed file")
	}
	_, data, ok := cache.GetMetaAndView("/file")
	records, err := j.PendingFixedUploads()
	if !ok || string(data) != "oldest" || err != nil || len(records) != 1 || calls != 0 {
		t.Fatal("lost failed-mode recovery", ok, len(records), calls, err)
	}
}

func TestFixedCacheSettleFailureReopensWithoutPayload(t *testing.T) {
	dir := t.TempDir()
	api := &fixedCacheAPI{t: t, size: 6}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	u, j, cache := newFixedCacheUploader(t, c, dir)
	api.journal = j
	cacheFixedImage(t, cache, []byte("oldest"), false)
	api.afterComplete = func() {
		if err := j.Close(); err != nil {
			t.Error(err)
		}
	}
	if err := u.UploadSync(context.Background(), "/file"); err == nil {
		t.Fatal("closed journal accepted settlement")
	}
	if cache.MetaGeneration("/file") != 0 {
		t.Fatal("did not reach local cleanup")
	}
	u.DrainAll()
	api.mu.Lock()
	api.afterComplete = nil
	api.mu.Unlock()
	u2, j2, cache2 := newFixedCacheUploader(t, c, dir)
	api.mu.Lock()
	api.journal = j2
	api.mu.Unlock()
	if err := u2.recoverFixedCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	records, err := j2.PendingFixedUploads()
	if err != nil || len(records) != 0 || cache2.MetaGeneration("/file") != 0 || api.transfers.Load() != 1 {
		t.Fatal("wrong replay", len(records), api.transfers.Load(), err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.completeIDs) != 2 || api.completeIDs[0] != api.completeIDs[1] || len(api.initIDs) != 1 {
		t.Fatal("changed original request")
	}
}

func TestFixedCacheEmptyPutRecovery(t *testing.T) {
	for _, kind := range []staging.PendingKind{staging.PendingNew, staging.PendingOverwrite} {
		for _, hasMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d-mode-%t", kind, hasMode), func(t *testing.T) {
				dir := t.TempDir()
				api := &fixedCacheAPI{t: t, size: 0}
				if kind == staging.PendingOverwrite {
					api.expected = 6
				}
				api.failComplete.Store(true)
				server := httptest.NewServer(http.HandlerFunc(api.serve))
				defer server.Close()
				c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
				u, j, cache := newFixedCacheUploader(t, c, dir)
				api.journal = j
				if err := cache.PutWithBaseRevAndMode("/file", nil, 0, kind, api.expected, 0, hasMode); err != nil {
					t.Fatal(err)
				}
				if err := u.UploadSync(context.Background(), "/file"); err == nil {
					t.Fatal("unknown empty PUT accepted")
				}
				if cache.MetaGeneration("/file") == 0 {
					t.Fatal("discarded empty file metadata")
				}
				u.DrainAll()
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
				u2, j2, cache2 := newFixedCacheUploader(t, c, dir)
				api.mu.Lock()
				api.journal = j2
				api.mu.Unlock()
				api.failComplete.Store(false)
				if err := (&Writeback{opts: Options{Client: c}, uploader: u2}).Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				records, err := j2.PendingFixedUploads()
				if err != nil || len(records) != 0 || cache2.MetaGeneration("/file") != 0 || api.transfers.Load() != 0 {
					t.Fatal("bad empty recovery", records, err)
				}
				api.mu.Lock()
				defer api.mu.Unlock()
				if len(api.initIDs) != 0 || len(api.completeIDs) != 2 || api.completeIDs[0] != api.completeIDs[1] || len(api.modeIDs) != map[bool]int{false: 0, true: 1}[hasMode] {
					t.Fatal("wrong replay phases")
				}
			})
		}
	}
}
