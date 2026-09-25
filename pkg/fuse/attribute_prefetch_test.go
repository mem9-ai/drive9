package fuse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLookupSecondSiblingMissPrefetchesDirectory(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			writeMetadataPrefetchListing(t, w, 3)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	for _, name := range []string{"file.0", "file.1", "file.2"} {
		var out gofuse.EntryOut
		if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &out); status != gofuse.OK {
			t.Fatalf("Lookup %s = %v, want OK", name, status)
		}
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}
}

func TestLookupSingleMissDoesNotPrefetchDirectory(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			writeMetadataPrefetchListing(t, w, 1)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "file.0", &gofuse.EntryOut{}); status != gofuse.OK {
		t.Fatalf("Lookup = %v, want OK", status)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("LIST calls = %d, want 0", got)
	}
}

func TestSiblingMetadataPrefetchFailureFallsBackToSinglePathStat(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	for _, name := range []string{"file.0", "file.1"} {
		if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &gofuse.EntryOut{}); status != gofuse.OK {
			t.Fatalf("Lookup %s = %v, want OK", name, status)
		}
	}
	if got := headCalls.Load(); got != 2 {
		t.Fatalf("HEAD calls = %d, want 2", got)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}
}

func TestGetAttrUsesVerifiedSiblingPrefetchWithoutTrustLocalEvents(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			writeMetadataPrefetchListing(t, w, 3)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	for i := 0; i < 3; i++ {
		filePath := fmt.Sprintf("/src/file.%d", i)
		ino := fs.inodes.Lookup(filePath, false, 0, time.Unix(1, 0))
		var out gofuse.AttrOut
		if status := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); status != gofuse.OK {
			t.Fatalf("GetAttr %s = %v, want OK", filePath, status)
		}
		if got, want := out.Size, uint64(i+1); got != want {
			t.Fatalf("GetAttr %s size = %d, want %d", filePath, got, want)
		}
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}
}

func TestVerifiedSiblingPrefetchFailsClosedAfterDirectoryMutation(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "99")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "9")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			writeMetadataPrefetchListing(t, w, 3)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	for i := 0; i < 2; i++ {
		ino := fs.inodes.Lookup(fmt.Sprintf("/src/file.%d", i), false, 0, time.Unix(1, 0))
		if status := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &gofuse.AttrOut{}); status != gofuse.OK {
			t.Fatalf("GetAttr file.%d = %v, want OK", i, status)
		}
	}
	fs.dirCache.Upsert("/src", CachedFileInfo{Name: "local.txt", Size: 1, Revision: 1})
	ino := fs.inodes.Lookup("/src/file.2", false, 0, time.Unix(1, 0))
	var out gofuse.AttrOut
	if status := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); status != gofuse.OK {
		t.Fatalf("GetAttr file.2 = %v, want OK", status)
	}
	if got, want := out.Size, uint64(99); got != want {
		t.Fatalf("GetAttr file.2 size = %d, want %d", got, want)
	}
	if got := headCalls.Load(); got != 2 {
		t.Fatalf("HEAD calls = %d, want 2", got)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}
}

func TestSiblingMetadataPrefetchSingleflightsConcurrentDirectoryRefresh(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	listStarted := make(chan struct{})
	releaseList := make(chan struct{})
	var closeList sync.Once
	t.Cleanup(func() { closeList.Do(func() { close(releaseList) }) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			close(listStarted)
			<-releaseList
			writeMetadataPrefetchListing(t, w, 3)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "file.0", &gofuse.EntryOut{}); status != gofuse.OK {
		t.Fatalf("first Lookup = %v, want OK", status)
	}
	results := make(chan gofuse.Status, 2)
	go func() {
		results <- fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "file.1", &gofuse.EntryOut{})
	}()
	select {
	case <-listStarted:
	case <-time.After(time.Second):
		t.Fatal("directory prefetch did not start")
	}
	go func() {
		results <- fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "file.2", &gofuse.EntryOut{})
	}()
	key := metadataPrefetchActiveKey(t, fs.metadataPrefetch)
	waitForWaiters(t, fs.metadataPrefetch.flight, key, 1)
	closeList.Do(func() { close(releaseList) })
	for range 2 {
		if status := <-results; status != gofuse.OK {
			t.Fatalf("concurrent Lookup = %v, want OK", status)
		}
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}
}

func TestLargeDirectorySiblingPrefetchUsesBoundedBatchStat(t *testing.T) {
	var headCalls, listCalls, batchCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			writeMetadataPrefetchListing(t, w, metadataPrefetchListThreshold+1)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			batchCalls.Add(1)
			var body struct {
				Paths []string `json:"paths"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode batch stat: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(body.Paths) == 0 || len(body.Paths) > client.MaxBatchStatPaths {
				t.Errorf("batch paths = %d, want 1..%d", len(body.Paths), client.MaxBatchStatPaths)
			}
			results := make([]client.BatchStatResult, len(body.Paths))
			for i, filePath := range body.Paths {
				results[i] = client.BatchStatResult{Path: filePath, Status: http.StatusOK, Size: int64(i + 10), Revision: 2}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	for _, name := range []string{"file.0", "file.1"} {
		if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &gofuse.EntryOut{}); status != gofuse.OK {
			t.Fatalf("initial Lookup %s = %v, want OK", name, status)
		}
	}
	fs.dirCache.Invalidate("/src")
	fs.metadataPrefetch.mu.Lock()
	fs.metadataPrefetch.dirs["/src"].nextAllowed = time.Time{}
	fs.metadataPrefetch.mu.Unlock()
	for _, name := range []string{"file.2", "file.3"} {
		if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &gofuse.EntryOut{}); status != gofuse.OK {
			t.Fatalf("second Lookup %s = %v, want OK", name, status)
		}
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}
	if got := batchCalls.Load(); got != 1 {
		t.Fatalf("batch stat calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got != 2 {
		t.Fatalf("HEAD calls = %d, want 2", got)
	}
}

func TestMetadataPrefetchBatchMarkerAuthorizesOnlyRefreshedPaths(t *testing.T) {
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	request := metadataPrefetchRequest{
		parentPath:      "/src",
		mountGeneration: 3,
		epoch:           prefetch.epoch,
	}
	prefetch.rememberBatch(request, []CachedFileInfo{{Name: "fresh.go"}}, 7)
	if !prefetch.valid("/src", "/src/fresh.go", 3, 7) {
		t.Fatal("refreshed path was not authorized")
	}
	if prefetch.valid("/src", "/src/stale.go", 3, 7) {
		t.Fatal("unrefreshed path was authorized")
	}
}

func TestMetadataPrefetchLargeListingMarkerAuthorizesOnlyHotPaths(t *testing.T) {
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	request := metadataPrefetchRequest{
		parentPath:      "/src",
		mountGeneration: 3,
		epoch:           prefetch.epoch,
		hotPaths:        []string{"/src/hot.go"},
	}
	items := make([]CachedFileInfo, metadataPrefetchListThreshold+1)
	for i := range items {
		items[i].Name = fmt.Sprintf("file.%d", i)
	}
	items[0].Name = "hot.go"
	prefetch.rememberListing(request, items, 7)
	if !prefetch.valid("/src", "/src/hot.go", 3, 7) {
		t.Fatal("hot path was not authorized")
	}
	if prefetch.valid("/src", "/src/file.1", 3, 7) {
		t.Fatal("non-hot path from a large listing was authorized")
	}
}

func newMetadataPrefetchTestFS(serverURL string) *Dat9FS {
	opts := &MountOptions{Profile: MountProfileCodingAgent}
	opts.setDefaults()
	return NewDat9FS(newTestClient(serverURL), opts)
}

func writeMetadataPrefetchListing(t *testing.T, w http.ResponseWriter, count int) {
	t.Helper()
	entries := make([]client.FileInfo, count)
	for i := range entries {
		entries[i] = client.FileInfo{
			Name:     fmt.Sprintf("file.%d", i),
			Size:     int64(i + 1),
			Revision: 1,
			Mtime:    1,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"entries": entries}); err != nil {
		t.Errorf("encode listing: %v", err)
	}
}

func metadataPrefetchActiveKey(t *testing.T, prefetch *siblingMetadataPrefetch) string {
	t.Helper()
	prefetch.mu.Lock()
	defer prefetch.mu.Unlock()
	for key := range prefetch.active {
		return key
	}
	t.Fatal("no active metadata prefetch")
	return ""
}
