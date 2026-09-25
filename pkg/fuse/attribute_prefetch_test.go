package fuse

import (
	"context"
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
	rememberMetadataPrefetchDirectorySize(fs, "/src", 3)
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

func TestMetadataPrefetchEnabledAcrossProfilesWithoutSingleMissFanout(t *testing.T) {
	profiles := []string{MountProfileCodingAgent, MountProfileInteractive, MountProfileNone, MountProfileExtent, "custom"}
	for _, profile := range profiles {
		t.Run(profile, func(t *testing.T) {
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
					w.WriteHeader(http.StatusInternalServerError)
				case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
					batchCalls.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
				default:
					t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer server.Close()

			fs := newMetadataPrefetchTestFSWithProfile(server.URL, profile)
			if fs.metadataPrefetch == nil {
				t.Fatal("metadata prefetch was not initialized")
			}
			if fs.metadataPrefetch.ttl != fs.opts.DirTTL {
				t.Fatalf("prefetch TTL = %v, want profile DirTTL %v", fs.metadataPrefetch.ttl, fs.opts.DirTTL)
			}
			dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
			if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "file.go", &gofuse.EntryOut{}); status != gofuse.OK {
				t.Fatalf("Lookup = %v, want OK", status)
			}
			if got := headCalls.Load(); got != 1 {
				t.Fatalf("HEAD calls = %d, want 1", got)
			}
			if got := listCalls.Load(); got != 0 {
				t.Fatalf("LIST calls = %d, want 0", got)
			}
			if got := batchCalls.Load(); got != 0 {
				t.Fatalf("batch calls = %d, want 0", got)
			}
		})
	}
}

func TestMetadataPrefetchUnverifiedAcrossProfilesFallsBackToHead(t *testing.T) {
	profiles := []string{MountProfileCodingAgent, MountProfileInteractive, MountProfileNone, "custom"}
	for _, profile := range profiles {
		t.Run(profile, func(t *testing.T) {
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
					w.WriteHeader(http.StatusInternalServerError)
				case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
					batchCalls.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
				default:
					t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer server.Close()

			fs := newMetadataPrefetchTestFSWithProfile(server.URL, profile)
			fs.markStatCacheUnverified()
			dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
			for _, name := range []string{"a.go", "b.go"} {
				if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &gofuse.EntryOut{}); status != gofuse.OK {
					t.Fatalf("Lookup %s = %v, want OK", name, status)
				}
			}
			if got := headCalls.Load(); got != 2 {
				t.Fatalf("HEAD calls = %d, want 2", got)
			}
			if got := listCalls.Load(); got != 0 {
				t.Fatalf("LIST calls = %d, want 0", got)
			}
			if got := batchCalls.Load(); got != 0 {
				t.Fatalf("batch calls = %d, want 0", got)
			}
		})
	}
}

func TestMetadataPrefetchExtentDataPlaneFallsBackToHead(t *testing.T) {
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
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			batchCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	opts := &MountOptions{Profile: MountProfileExtent, ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	if fs.metadataPrefetch == nil {
		t.Fatal("metadata prefetch was not initialized")
	}
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	for _, name := range []string{"a.db", "b.db"} {
		if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &gofuse.EntryOut{}); status != gofuse.OK {
			t.Fatalf("Lookup %s = %v, want OK", name, status)
		}
	}
	if got := headCalls.Load(); got != 2 {
		t.Fatalf("HEAD calls = %d, want 2", got)
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("LIST calls = %d, want 0", got)
	}
	if got := batchCalls.Load(); got != 0 {
		t.Fatalf("batch calls = %d, want 0", got)
	}
}

func TestLookupUnknownDirectoryUsesBoundedBatchStat(t *testing.T) {
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
				results[i] = client.BatchStatResult{Path: filePath, Status: http.StatusOK, Size: int64(i + 1), Revision: 2}
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
			t.Fatalf("Lookup %s = %v, want OK", name, status)
		}
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("LIST calls = %d, want 0", got)
	}
	if got := batchCalls.Load(); got != 1 {
		t.Fatalf("batch stat calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
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
	rememberMetadataPrefetchDirectorySize(fs, "/src", 3)
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
	rememberMetadataPrefetchDirectorySize(fs, "/src", 3)
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

func TestOrdinaryLookupInstallKeepsSiblingPrefetchMarker(t *testing.T) {
	var headCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "40")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "4")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	installMetadataPrefetchBatch(t, fs, "/src", []CachedFileInfo{{
		Name:     "a.go",
		Size:     10,
		Revision: 2,
	}})
	aIno := fs.inodes.Lookup("/src/a.go", false, 0, time.Unix(1, 0))
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "d.go", &gofuse.EntryOut{}); status != gofuse.OK {
		t.Fatalf("Lookup d.go = %v, want OK", status)
	}

	var out gofuse.AttrOut
	if status := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: aIno}}, &out); status != gofuse.OK {
		t.Fatalf("GetAttr a.go = %v, want OK", status)
	}
	if got, want := out.Size, uint64(10); got != want {
		t.Fatalf("GetAttr a.go size = %d, want prefetched %d", got, want)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want only unprefetched d.go", got)
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
	rememberMetadataPrefetchDirectorySize(fs, "/src", 3)
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

func TestCachedAttrSnapshotCannotUseReplacementMarker(t *testing.T) {
	fs := newMetadataPrefetchTestFS("http://127.0.0.1")
	ino := fs.inodes.LookupWithIdentity("/src/file.go", "resource-old", 1, false, 10, time.Unix(10, 0))

	installMetadataPrefetchBatch(t, fs, "/src", []CachedFileInfo{{
		Name:       "file.go",
		Size:       10,
		Mtime:      time.Unix(10, 0),
		Revision:   1,
		ResourceID: "resource-old",
		Nlink:      1,
	}})
	oldSnapshot := fs.dirCache.Lookup("/src", "file.go")
	if oldSnapshot.kind != namespaceLookupPositive {
		t.Fatal("old cache snapshot is not positive")
	}

	fs.dirCache.Invalidate("/src")
	installMetadataPrefetchBatch(t, fs, "/src", []CachedFileInfo{{
		Name:       "file.go",
		Size:       20,
		Mtime:      time.Unix(20, 0),
		Revision:   2,
		ResourceID: "resource-new",
		Nlink:      1,
	}})

	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if cached, accepted := fs.cachedAttrEntryFromLookup(entry, "/src", oldSnapshot, fs.mountViewGeneration.Load()); accepted || cached != nil {
		t.Fatalf("old snapshot accepted under replacement marker: %+v", cached)
	}
	if current, ok := fs.inodes.GetEntry(ino); !ok || current.Size != 10 || current.ResourceID != "resource-old" {
		t.Fatalf("rejected snapshot mutated inode: %+v, ok=%t", current, ok)
	}
}

func TestVerifiedSiblingPrefetchIdentityReplacementFallsBackToHead(t *testing.T) {
	explicitMtime := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	replacementMtime := time.Date(2024, 5, 5, 0, 0, 0, 0, time.UTC)
	var headCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "5")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "1")
		w.Header().Set("X-Dat9-Resource-ID", "resource-new")
		w.Header().Set("X-Dat9-Nlink", "1")
		w.Header().Set("X-Dat9-Mtime", fmt.Sprintf("%d", replacementMtime.Unix()))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	ino := fs.inodes.LookupWithIdentity("/src/file.go", "resource-old", 1, false, 5, explicitMtime)
	fs.inodes.SetLocalMtime(ino, explicitMtime)
	installMetadataPrefetchBatch(t, fs, "/src", []CachedFileInfo{{
		Name:       "file.go",
		Size:       5,
		Mtime:      replacementMtime,
		Revision:   1,
		ResourceID: "resource-new",
		Nlink:      1,
	}})

	var out gofuse.AttrOut
	if status := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); status != gofuse.OK {
		t.Fatalf("GetAttr = %v, want OK", status)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := out.Mtime; got != uint64(replacementMtime.Unix()) {
		t.Fatalf("mtime = %d, want replacement mtime %d", got, replacementMtime.Unix())
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.ResourceID != "resource-new" {
		t.Fatalf("replacement identity = %+v, ok=%t", entry, ok)
	}
	if fs.inodes.HasMtimeOverride(ino) {
		t.Fatal("old identity mtime override survived HEAD fallback")
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
	rememberMetadataPrefetchDirectorySize(fs, "/src", 3)
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
	rememberMetadataPrefetchDirectorySize(fs, "/src", metadataPrefetchListThreshold+1)
	dirIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	for _, name := range []string{"file.0", "file.1"} {
		if status := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, name, &gofuse.EntryOut{}); status != gofuse.OK {
			t.Fatalf("Lookup %s = %v, want OK", name, status)
		}
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("LIST calls = %d, want 0", got)
	}
	if got := batchCalls.Load(); got != 1 {
		t.Fatalf("batch stat calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
}

func TestAcceptedCompleteListingSeedsSmallDirectoryAfterCacheExpiry(t *testing.T) {
	fs := newMetadataPrefetchTestFS("http://127.0.0.1")
	now := time.Unix(100, 0)
	fs.dirCache.now = func() time.Time { return now }
	request := fs.dirCache.BeginRequest("/src")
	fs.putDirectoryListing("/src", []CachedFileInfo{
		{Name: "a.go"},
		{Name: "b.go"},
	}, request)

	now = now.Add(fs.dirCache.ttl + time.Second)
	if _, ok := fs.dirCache.Get("/src"); ok {
		t.Fatal("directory cache remained live after TTL")
	}
	mutationGeneration := fs.dirCache.mutationGeneration("/src")
	namespaceGeneration := fs.dirCache.namespaceGeneration()
	if _, ok := fs.metadataPrefetch.beginMiss("/src/a.go", 1, mutationGeneration, namespaceGeneration); ok {
		t.Fatal("first miss activated prefetch")
	}
	prefetchRequest, ok := fs.metadataPrefetch.beginMiss("/src/b.go", 1, mutationGeneration, namespaceGeneration)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}
	if !prefetchRequest.list {
		t.Fatal("accepted complete small listing did not select LIST")
	}
}

func TestBeginMissBoundsGetAttrHotSetAndDefersSnapshot(t *testing.T) {
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	now := time.Unix(100, 0)
	prefetch.now = func() time.Time { return now }

	if _, ok := prefetch.beginMiss("/src/a.go", 1, 3, 4); ok {
		t.Fatal("first miss activated prefetch")
	}
	request, ok := prefetch.beginMiss("/src/b.go", 1, 3, 4)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}
	if len(request.hotPaths) != 2 {
		t.Fatalf("activation hot paths = %d, want 2", len(request.hotPaths))
	}
	joined, ok := prefetch.beginMiss("/src/c.go", 1, 3, 4)
	if !ok {
		t.Fatal("active refresh was not joined")
	}
	if fmt.Sprint(joined.hotPaths) != fmt.Sprint(request.hotPaths) {
		t.Fatalf("active waiter plan = %v, want shared %v", joined.hotPaths, request.hotPaths)
	}
	prefetch.finish(request)

	for i := 0; i < metadataPrefetchMaxHotPaths*4; i++ {
		if _, ok := prefetch.beginMiss(fmt.Sprintf("/src/getattr-%04d.go", i), 1, 3, 4); ok {
			t.Fatalf("cooldown miss %d activated prefetch", i)
		}
	}
	prefetch.mu.Lock()
	hotCount := len(prefetch.dirs["/src"].hot)
	prefetch.mu.Unlock()
	if hotCount != metadataPrefetchMaxHotPaths {
		t.Fatalf("hot paths = %d, want bounded %d", hotCount, metadataPrefetchMaxHotPaths)
	}
}

func TestActiveMetadataPrefetchJoinerCanExecuteSharedPlan(t *testing.T) {
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	if _, ok := prefetch.beginMiss("/src/a.go", 1, 3, 4); ok {
		t.Fatal("first miss activated prefetch")
	}
	owner, ok := prefetch.beginMiss("/src/b.go", 1, 3, 4)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}
	joiner, ok := prefetch.beginMiss("/src/c.go", 1, 3, 4)
	if !ok {
		t.Fatal("active refresh was not joined")
	}

	executed := make(chan []string, 1)
	release := make(chan struct{})
	joinerDone := make(chan struct{})
	go func() {
		defer close(joinerDone)
		_, _, _ = prefetch.flight.Do(context.Background(), joiner.key, func() ([]byte, error) {
			executed <- append([]string(nil), joiner.hotPaths...)
			<-release
			return nil, nil
		})
	}()

	var executorCalls atomic.Int32
	ownerDone := make(chan struct{})
	paths := <-executed
	go func() {
		defer close(ownerDone)
		_, _, _ = prefetch.flight.Do(context.Background(), owner.key, func() ([]byte, error) {
			executorCalls.Add(1)
			return nil, nil
		})
	}()
	waitForWaiters(t, prefetch.flight, owner.key, 1)
	close(release)
	<-joinerDone
	<-ownerDone
	prefetch.finish(owner)

	if fmt.Sprint(paths) != fmt.Sprint(owner.hotPaths) {
		t.Fatalf("joiner executor plan = %v, want shared %v", paths, owner.hotPaths)
	}
	if executorCalls.Load() != 0 {
		t.Fatal("owner callback ran after joiner had already become executor")
	}
}

func TestUnrelatedDirectoryMutationKeepsRememberedDirectorySize(t *testing.T) {
	dc := NewNamespaceCache(time.Minute, time.Second, 16)
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	mutationGeneration := dc.mutationGeneration("/src")
	namespaceGeneration := dc.namespaceGeneration()
	prefetch.rememberDirectorySize("/src", 2, mutationGeneration, namespaceGeneration)
	prefetch.rememberBatch(metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     1,
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: namespaceGeneration,
		epoch:               prefetch.epoch,
	}, []CachedFileInfo{{Name: "a.go"}}, mutationGeneration, namespaceGeneration)
	dc.Upsert("/other", CachedFileInfo{Name: "new.go"})

	mutationGeneration = dc.mutationGeneration("/src")
	namespaceGeneration = dc.namespaceGeneration()
	if !prefetch.valid("/src", "/src/a.go", 1, mutationGeneration, namespaceGeneration) {
		t.Fatal("unrelated directory mutation invalidated sibling marker")
	}
	if _, ok := prefetch.beginMiss("/src/a.go", 1, mutationGeneration, namespaceGeneration); ok {
		t.Fatal("first miss activated prefetch")
	}
	request, ok := prefetch.beginMiss("/src/b.go", 1, mutationGeneration, namespaceGeneration)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}
	if !request.list {
		t.Fatal("unrelated directory mutation discarded remembered directory size")
	}
}

func TestDirectoryMutationInvalidatesRememberedDirectorySize(t *testing.T) {
	dc := NewNamespaceCache(time.Minute, time.Second, 16)
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	prefetch.rememberDirectorySize("/src", 2, dc.mutationGeneration("/src"), dc.namespaceGeneration())
	dc.Upsert("/src", CachedFileInfo{Name: "new.go"})

	mutationGeneration := dc.mutationGeneration("/src")
	namespaceGeneration := dc.namespaceGeneration()
	if _, ok := prefetch.beginMiss("/src/a.go", 1, mutationGeneration, namespaceGeneration); ok {
		t.Fatal("first miss activated prefetch")
	}
	request, ok := prefetch.beginMiss("/src/b.go", 1, mutationGeneration, namespaceGeneration)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}
	if request.list {
		t.Fatal("same-directory mutation retained stale directory size")
	}
}

func TestAncestorInvalidationRevokesDescendantPrefetchIdentity(t *testing.T) {
	dc := NewNamespaceCache(time.Minute, time.Second, 16)
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	mutationGeneration := dc.mutationGeneration("/repo/src")
	namespaceGeneration := dc.namespaceGeneration()
	prefetch.rememberDirectorySize("/repo/src", 2, mutationGeneration, namespaceGeneration)
	prefetch.rememberBatch(metadataPrefetchRequest{
		parentPath:          "/repo/src",
		mountGeneration:     1,
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: namespaceGeneration,
		epoch:               prefetch.epoch,
	}, []CachedFileInfo{{Name: "a.go"}}, mutationGeneration, namespaceGeneration)

	dc.InvalidatePrefix("/repo")
	mutationGeneration = dc.mutationGeneration("/repo/src")
	namespaceGeneration = dc.namespaceGeneration()
	if prefetch.valid("/repo/src", "/repo/src/a.go", 1, mutationGeneration, namespaceGeneration) {
		t.Fatal("ancestor invalidation left descendant marker valid")
	}
	if _, ok := prefetch.beginMiss("/repo/src/a.go", 1, mutationGeneration, namespaceGeneration); ok {
		t.Fatal("first miss activated prefetch")
	}
	request, ok := prefetch.beginMiss("/repo/src/b.go", 1, mutationGeneration, namespaceGeneration)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}
	if request.list {
		t.Fatal("ancestor invalidation retained stale descendant directory size")
	}
}

func TestDirectoryMutationIdentityEvictionCannotMatchOldMarker(t *testing.T) {
	dc := NewNamespaceCache(time.Minute, time.Second, 2)
	first := dc.mutationGeneration("/first")
	dc.mutationGeneration("/second")
	dc.mutationGeneration("/second")
	dc.mutationGeneration("/third")
	recreated := dc.mutationGeneration("/first")
	if recreated == first {
		t.Fatalf("recreated mutation identity = %d, want newer than evicted %d", recreated, first)
	}
	if got := len(dc.mutationGenerations); got != 2 {
		t.Fatalf("tracked mutation identities = %d, want bounded 2", got)
	}
}

func TestListingReceiptExcludesResponseRejectedByLocalMutation(t *testing.T) {
	dc := NewNamespaceCache(time.Minute, time.Second, 16)
	mutationGeneration := dc.mutationGeneration("/src")
	request := dc.BeginRequest("/src")
	dc.Upsert("/src", CachedFileInfo{Name: "file.go", Size: 20, Revision: 2})
	_, receipt := dc.putListing("/src", []CachedFileInfo{{Name: "file.go", Size: 10, Revision: 1}}, request)
	if !receipt.installed {
		t.Fatal("listing was not installed")
	}
	if len(receipt.accepted) != 0 {
		t.Fatalf("accepted response items = %d, want 0", len(receipt.accepted))
	}

	prefetch := newSiblingMetadataPrefetch(time.Minute)
	prefetchRequest := metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     3,
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: receipt.namespaceGeneration,
		epoch:               prefetch.epoch,
	}
	prefetch.rememberListing(prefetchRequest, receipt)
	if prefetch.valid("/src", "/src/file.go", 3, receipt.mutationGeneration, receipt.namespaceGeneration) {
		t.Fatal("locally superseded response item received a marker")
	}
}

func TestStaleListingReceiptCannotPublishMarker(t *testing.T) {
	dc := NewNamespaceCache(time.Minute, time.Second, 16)
	older := dc.BeginRequest("/src")
	newer := dc.BeginRequest("/src")
	_, newerReceipt := dc.putListing("/src", []CachedFileInfo{{Name: "new.go", Size: 2}}, newer)
	_, staleReceipt := dc.putListing("/src", []CachedFileInfo{{Name: "old.go", Size: 1}}, older)
	if !newerReceipt.installed {
		t.Fatal("newer listing was not installed")
	}
	if staleReceipt.installed {
		t.Fatal("stale listing produced an install receipt")
	}

	prefetch := newSiblingMetadataPrefetch(time.Minute)
	prefetchRequest := metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     3,
		mutationGeneration:  newerReceipt.mutationGeneration,
		namespaceGeneration: newerReceipt.namespaceGeneration,
		epoch:               prefetch.epoch,
	}
	prefetch.rememberListing(prefetchRequest, newerReceipt)
	prefetch.rememberListing(prefetchRequest, staleReceipt)
	prefetch.mu.Lock()
	_, staleMarked := prefetch.dirs["/src"].marker.paths["/src/old.go"]
	prefetch.mu.Unlock()
	if staleMarked {
		t.Fatal("stale listing path was added to the marker")
	}
	currentMutation := dc.mutationGeneration("/src")
	if prefetch.valid("/src", "/src/old.go", 3, currentMutation, newerReceipt.namespaceGeneration) {
		t.Fatal("stale listing path received a marker")
	}
}

func TestMetadataPrefetchRequestSurvivesReadInstallBeforeRefresh(t *testing.T) {
	var batchCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs:batch-stat" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		batchCalls.Add(1)
		var body struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode batch stat: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		results := make([]client.BatchStatResult, len(body.Paths))
		for i, filePath := range body.Paths {
			results[i] = client.BatchStatResult{Path: filePath, Status: http.StatusOK, Size: int64(i + 1), Revision: 2}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	mountGeneration := fs.mountViewGeneration.Load()
	mutationGeneration := fs.dirCache.mutationGeneration("/src")
	namespaceGeneration := fs.dirCache.namespaceGeneration()
	if _, ok := fs.metadataPrefetch.beginMiss("/src/a.go", mountGeneration, mutationGeneration, namespaceGeneration); ok {
		t.Fatal("first miss activated prefetch")
	}
	request, ok := fs.metadataPrefetch.beginMiss("/src/b.go", mountGeneration, mutationGeneration, namespaceGeneration)
	if !ok {
		t.Fatal("second miss did not activate prefetch")
	}

	dirGeneration := fs.dirCache.generation("/src")
	observation := fs.dirCache.BeginRequest("/src")
	fs.dirCache.Observe("/src", CachedFileInfo{Name: "other.go", Revision: 1}, observation)
	if fs.dirCache.generation("/src") == dirGeneration {
		t.Fatal("ordinary read install did not advance directory generation")
	}
	if fs.dirCache.mutationGeneration("/src") != mutationGeneration {
		t.Fatal("ordinary read install advanced mutation generation")
	}

	if err := fs.refreshSiblingMetadata(context.Background(), request); err != nil {
		t.Fatalf("refresh after read install: %v", err)
	}
	if got := batchCalls.Load(); got != 1 {
		t.Fatalf("batch stat calls = %d, want 1", got)
	}
	if !fs.metadataPrefetch.valid("/src", "/src/a.go", mountGeneration, mutationGeneration, namespaceGeneration) {
		t.Fatal("read install prevented sibling marker publication")
	}
}

func TestMetadataPrefetchBatchBindsResultsByPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs:batch-stat" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var body struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode batch stat: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(body.Paths) != 2 {
			t.Errorf("batch paths = %d, want 2", len(body.Paths))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		results := []client.BatchStatResult{
			{Path: body.Paths[1], Status: http.StatusOK, Size: 200, Revision: 2},
			{Path: body.Paths[0], Status: http.StatusOK, Size: 100, Revision: 2},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	request := metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     fs.mountViewGeneration.Load(),
		mutationGeneration:  fs.dirCache.mutationGeneration("/src"),
		namespaceGeneration: fs.dirCache.namespaceGeneration(),
		epoch:               fs.metadataPrefetch.epoch,
		hotPaths:            []string{"/src/a.go", "/src/b.go"},
	}
	if err := fs.refreshSiblingMetadataBatch(context.Background(), request); err != nil {
		t.Fatalf("refresh batch: %v", err)
	}
	a := fs.dirCache.Lookup("/src", "a.go")
	b := fs.dirCache.Lookup("/src", "b.go")
	if a.kind != namespaceLookupPositive || a.item.Size != 100 {
		t.Fatalf("a.go = kind %v size %d, want positive size 100", a.kind, a.item.Size)
	}
	if b.kind != namespaceLookupPositive || b.item.Size != 200 {
		t.Fatalf("b.go = kind %v size %d, want positive size 200", b.kind, b.item.Size)
	}
}

func TestMetadataPrefetchBatchRejectsDuplicateResultPaths(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode batch stat: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		results := []client.BatchStatResult{
			{Path: body.Paths[0], Status: http.StatusOK, Size: 100, Revision: 2},
			{Path: body.Paths[0], Status: http.StatusOK, Size: 200, Revision: 2},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	defer server.Close()

	fs := newMetadataPrefetchTestFS(server.URL)
	request := metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     fs.mountViewGeneration.Load(),
		mutationGeneration:  fs.dirCache.mutationGeneration("/src"),
		namespaceGeneration: fs.dirCache.namespaceGeneration(),
		epoch:               fs.metadataPrefetch.epoch,
		hotPaths:            []string{"/src/a.go", "/src/b.go"},
	}
	if err := fs.refreshSiblingMetadataBatch(context.Background(), request); err != nil {
		t.Fatalf("refresh batch: %v", err)
	}
	if got := fs.dirCache.Lookup("/src", "a.go").kind; got == namespaceLookupPositive {
		t.Fatal("duplicate result path was cached")
	}
	if got := fs.dirCache.Lookup("/src", "b.go").kind; got == namespaceLookupPositive {
		t.Fatal("missing result path was cached")
	}
}

func TestMetadataPrefetchBatchMarkerAuthorizesOnlyRefreshedPaths(t *testing.T) {
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	request := metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     3,
		mutationGeneration:  7,
		namespaceGeneration: 5,
		epoch:               prefetch.epoch,
	}
	prefetch.rememberBatch(request, []CachedFileInfo{{Name: "fresh.go"}}, 7, 5)
	if !prefetch.valid("/src", "/src/fresh.go", 3, 7, 5) {
		t.Fatal("refreshed path was not authorized")
	}
	if prefetch.valid("/src", "/src/stale.go", 3, 7, 5) {
		t.Fatal("unrefreshed path was authorized")
	}
}

func TestMetadataPrefetchLargeListingMarkerAuthorizesOnlyHotPaths(t *testing.T) {
	prefetch := newSiblingMetadataPrefetch(time.Minute)
	request := metadataPrefetchRequest{
		parentPath:          "/src",
		mountGeneration:     3,
		mutationGeneration:  7,
		namespaceGeneration: 5,
		epoch:               prefetch.epoch,
		hotPaths:            []string{"/src/hot.go"},
	}
	items := make([]CachedFileInfo, metadataPrefetchListThreshold+1)
	for i := range items {
		items[i].Name = fmt.Sprintf("file.%d", i)
	}
	items[0].Name = "hot.go"
	prefetch.rememberListing(request, listingInstallReceipt{
		accepted:            items,
		childCount:          len(items),
		dirGeneration:       7,
		mutationGeneration:  7,
		namespaceGeneration: 5,
		installed:           true,
	})
	if !prefetch.valid("/src", "/src/hot.go", 3, 7, 5) {
		t.Fatal("hot path was not authorized")
	}
	if prefetch.valid("/src", "/src/file.1", 3, 7, 5) {
		t.Fatal("non-hot path from a large listing was authorized")
	}
}

func newMetadataPrefetchTestFS(serverURL string) *Dat9FS {
	return newMetadataPrefetchTestFSWithProfile(serverURL, MountProfileCodingAgent)
}

func newMetadataPrefetchTestFSWithProfile(serverURL, profile string) *Dat9FS {
	opts := &MountOptions{Profile: profile}
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

func installMetadataPrefetchBatch(t *testing.T, fs *Dat9FS, parentPath string, items []CachedFileInfo) {
	t.Helper()
	mutationGeneration := fs.dirCache.mutationGeneration(parentPath)
	namespaceGeneration := fs.dirCache.namespaceGeneration()
	observation := fs.dirCache.BeginRequest(parentPath)
	_, accepted := fs.dirCache.observeBatch(parentPath, items, observation)
	if !accepted {
		t.Fatal("metadata batch was not accepted")
	}
	request := metadataPrefetchRequest{
		parentPath:          parentPath,
		mountGeneration:     fs.mountViewGeneration.Load(),
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: namespaceGeneration,
		epoch:               fs.metadataPrefetch.epoch,
	}
	fs.metadataPrefetch.rememberBatch(request, items, mutationGeneration, namespaceGeneration)
}

func rememberMetadataPrefetchDirectorySize(fs *Dat9FS, parentPath string, childCount int) {
	fs.metadataPrefetch.rememberDirectorySize(
		parentPath,
		childCount,
		fs.dirCache.mutationGeneration(parentPath),
		fs.dirCache.namespaceGeneration(),
	)
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
