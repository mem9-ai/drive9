package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

func TestWriteBackCache_PutGetRemove(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/test/file.txt"
	data := []byte("hello world")

	// Get before Put should return false.
	if _, ok := cache.Get(path); ok {
		t.Fatal("expected Get to return false before Put")
	}

	// Put + Get
	if err := cache.Put(path, data, int64(len(data)), PendingNew); err != nil {
		t.Fatal(err)
	}
	got, ok := cache.Get(path)
	if !ok {
		t.Fatal("expected Get to return true after Put")
	}
	if string(got) != "hello world" {
		t.Fatalf("Get = %q, want %q", got, "hello world")
	}

	// GetMeta
	meta, ok := cache.GetMeta(path)
	if !ok {
		t.Fatal("expected GetMeta to return true")
	}
	if meta.Path != path {
		t.Fatalf("meta.Path = %q, want %q", meta.Path, path)
	}
	if meta.Size != int64(len(data)) {
		t.Fatalf("meta.Size = %d, want %d", meta.Size, len(data))
	}

	// Remove + Get
	cache.Remove(path)
	if _, ok := cache.Get(path); ok {
		t.Fatal("expected Get to return false after Remove")
	}
}

func TestWriteBackCache_PutWithBaseRevPersistsRevision(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.PutWithBaseRev("/existing.txt", []byte("hello"), 5, PendingOverwrite, 17); err != nil {
		t.Fatal(err)
	}

	meta, ok := cache.GetMeta("/existing.txt")
	if !ok {
		t.Fatal("expected GetMeta to return true")
	}
	if meta.Kind != PendingOverwrite {
		t.Fatalf("meta.Kind = %v, want %v", meta.Kind, PendingOverwrite)
	}
	if meta.BaseRev != 17 {
		t.Fatalf("meta.BaseRev = %d, want 17", meta.BaseRev)
	}
}

func TestWriteBackCache_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/atomic/test.txt"
	data := []byte("atomic data")

	if err := cache.Put(path, data, int64(len(data)), PendingNew); err != nil {
		t.Fatal(err)
	}

	// Verify no temp files remain.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if name := e.Name(); len(name) > 0 && name[0] == '.' {
			t.Fatalf("temp file not cleaned up: %s", name)
		}
	}

	// Verify .dat and .meta files exist.
	hash := hashPath(path)
	if _, err := os.Stat(filepath.Join(dir, hash+".dat")); err != nil {
		t.Fatalf("dat file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, hash+".meta")); err != nil {
		t.Fatalf("meta file missing: %v", err)
	}
}

func TestWriteBackCache_ListPending(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Put two entries.
	_ = cache.Put("/a.txt", []byte("aaa"), 3, PendingNew)
	_ = cache.Put("/b.txt", []byte("bbb"), 3, PendingNew)

	pending := cache.ListPending()
	if len(pending) != 2 {
		t.Fatalf("ListPending returned %d entries, want 2", len(pending))
	}

	// Map by path for easier assertion.
	byPath := map[string]PendingEntry{}
	for _, e := range pending {
		byPath[e.Meta.Path] = e
	}
	if string(byPath["/a.txt"].Data) != "aaa" {
		t.Fatalf("a.txt data = %q, want %q", byPath["/a.txt"].Data, "aaa")
	}
	if string(byPath["/b.txt"].Data) != "bbb" {
		t.Fatalf("b.txt data = %q, want %q", byPath["/b.txt"].Data, "bbb")
	}
}

func TestWriteBackCache_ListPending_CorruptMeta(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Write a corrupt .meta file.
	hash := hashPath("/corrupt.txt")
	_ = os.WriteFile(filepath.Join(dir, hash+".meta"), []byte("not json"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, hash+".dat"), []byte("data"), 0o644)

	// Write a valid entry.
	_ = cache.Put("/valid.txt", []byte("ok"), 2, PendingNew)

	pending := cache.ListPending()
	if len(pending) != 1 {
		t.Fatalf("ListPending returned %d entries, want 1 (corrupt should be skipped)", len(pending))
	}
	if pending[0].Meta.Path != "/valid.txt" {
		t.Fatalf("pending path = %q, want /valid.txt", pending[0].Meta.Path)
	}

	// Corrupt files should be cleaned up.
	if _, err := os.Stat(filepath.Join(dir, hash+".meta")); !os.IsNotExist(err) {
		t.Fatal("corrupt .meta should be removed")
	}
	if _, err := os.Stat(filepath.Join(dir, hash+".dat")); !os.IsNotExist(err) {
		t.Fatal("corrupt .dat should be removed")
	}
}

func TestWriteBackUploader_SubmitAndUpload(t *testing.T) {
	var uploadedPaths sync.Map
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			body, _ := io.ReadAll(r.Body)
			uploadedPaths.Store(r.URL.Path, string(body))
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	// Put data in cache and submit for upload.
	_ = cache.Put("/upload-test.txt", []byte("upload data"), 11, PendingNew)
	uploader.Submit("/upload-test.txt")

	// DrainAll waits for all uploads to complete.
	uploader.DrainAll()

	// Verify the data was uploaded.
	if val, ok := uploadedPaths.Load("/v1/fs/upload-test.txt"); !ok {
		t.Fatal("file was not uploaded")
	} else if val.(string) != "upload data" {
		t.Fatalf("uploaded data = %q, want %q", val, "upload data")
	}

	// Verify cache entry was removed after successful upload.
	if _, ok := cache.Get("/upload-test.txt"); ok {
		t.Fatal("cache entry should be removed after successful upload")
	}
}

func TestWriteBackUploader_RecoverPending(t *testing.T) {
	var uploadCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			uploadCount.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")

	// Simulate entries from a previous session.
	_ = cache.Put("/recover1.txt", []byte("data1"), 5, PendingNew)
	_ = cache.Put("/recover2.txt", []byte("data2"), 5, PendingNew)

	uploader := NewWriteBackUploader(c, cache, 2)
	uploader.RecoverPending()
	uploader.DrainAll()

	if n := uploadCount.Load(); n != 2 {
		t.Fatalf("expected 2 recovered uploads, got %d", n)
	}
}

func TestWriteBackUploader_UploadFailRetainsCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/fail.txt", []byte("fail data"), 9, PendingNew)
	uploader.Submit("/fail.txt")
	uploader.DrainAll()

	// Cache entry should be retained for retry on next mount.
	if _, ok := cache.Get("/fail.txt"); !ok {
		t.Fatal("cache entry should be retained after upload failure")
	}
}

func TestWriteBackUploader_PendingNewUsesCreateIfAbsent(t *testing.T) {
	var gotExpected string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/new.txt", []byte("new"), 3, PendingNew)
	uploader.Submit("/new.txt")
	uploader.DrainAll()

	if gotExpected != "0" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want %q", gotExpected, "0")
	}
}

func TestWriteBackUploader_PendingOverwriteUsesBaseRevision(t *testing.T) {
	var gotExpected string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRev("/existing.txt", []byte("edit"), 4, PendingOverwrite, 23)
	uploader.Submit("/existing.txt")
	uploader.DrainAll()

	if gotExpected != "23" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want %q", gotExpected, "23")
	}
}

func TestWriteBackUploader_PendingOverwriteWithoutBaseRevRetainsCache(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/legacy-overwrite.txt", []byte("edit"), 4, PendingOverwrite)
	uploader.Submit("/legacy-overwrite.txt")
	uploader.DrainAll()

	if putCalls.Load() != 0 {
		t.Fatalf("putCalls = %d, want 0", putCalls.Load())
	}
	if _, ok := cache.Get("/legacy-overwrite.txt"); !ok {
		t.Fatal("legacy overwrite entry should remain pending")
	}

	// Simulate next mount: recovery should continue skipping the legacy entry.
	uploader = NewWriteBackUploader(c, cache, 1)
	uploader.RecoverPending()
	uploader.DrainAll()

	if putCalls.Load() != 0 {
		t.Fatalf("putCalls after recovery = %d, want 0", putCalls.Load())
	}
}

func TestWriteBackUploader_UploadSyncLegacyOverwriteFallsBackToUnconditional(t *testing.T) {
	var gotExpected string
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls.Add(1)
			gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/legacy-sync.txt", []byte("edit"), 4, PendingOverwrite)
	if err := uploader.UploadSync(context.Background(), "/legacy-sync.txt"); err != nil {
		t.Fatalf("UploadSync: %v", err)
	}

	if putCalls.Load() != 1 {
		t.Fatalf("putCalls = %d, want 1", putCalls.Load())
	}
	if gotExpected != "" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want empty for unconditional legacy fallback", gotExpected)
	}
	if _, ok := cache.Get("/legacy-sync.txt"); ok {
		t.Fatal("legacy overwrite cache entry should be removed after UploadSync fallback")
	}
}

func TestMountHash_Deterministic(t *testing.T) {
	h1 := MountHash("https://example.com", "/mnt/data")
	h2 := MountHash("https://example.com", "/mnt/data")
	if h1 != h2 {
		t.Fatalf("MountHash not deterministic: %s != %s", h1, h2)
	}

	h3 := MountHash("https://other.com", "/mnt/data")
	if h1 == h3 {
		t.Fatal("different servers should produce different hashes")
	}
}

// TestFlush_WriteBack_SmallFile verifies that Flush() writes to local cache
// and returns immediately (no HTTP upload) when write-back is enabled.
func TestFlush_WriteBack_SmallFile(t *testing.T) {
	var httpPutCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			httpPutCalls.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create file
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "wb_test.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	// Write data
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("write-back test"))
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	putsBefore := httpPutCalls.Load()

	// Flush — should write to local cache, NOT make HTTP PUT.
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}

	putsAfter := httpPutCalls.Load()
	if putsAfter > putsBefore {
		t.Fatalf("Flush made %d HTTP PUT calls — should use write-back cache", putsAfter-putsBefore)
	}

	// Verify data is in the local cache.
	data, ok := cache.Get("/wb_test.txt")
	if !ok {
		t.Fatal("data not found in write-back cache after Flush")
	}
	if string(data) != "write-back test" {
		t.Fatalf("cached data = %q, want %q", data, "write-back test")
	}

	// Release — should submit async upload.
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	// Drain uploader and verify the HTTP PUT happened.
	uploader.DrainAll()

	if httpPutCalls.Load() == 0 {
		t.Fatal("Release + DrainAll should have triggered HTTP PUT upload")
	}

	// Cache should be cleaned up after successful upload.
	if _, ok := cache.Get("/wb_test.txt"); ok {
		t.Fatal("cache entry should be removed after successful upload")
	}
}

// TestFlush_WriteBack_Lifecycle tests the full echo "xxx" > file lifecycle
// with write-back enabled: Create → Write → Flush → Release.
func TestFlush_WriteBack_Lifecycle(t *testing.T) {
	uploadedCh := make(chan []byte, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			uploadedCh <- append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	done := make(chan struct{})
	go func() {
		defer close(done)

		var createOut gofuse.CreateOut
		st := fs.Create(nil, &gofuse.CreateIn{
			InHeader: gofuse.InHeader{NodeId: 1},
		}, "lifecycle.txt", &createOut)
		if st != gofuse.OK {
			t.Errorf("Create: %v", st)
			return
		}

		_, st = fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		}, []byte("lifecycle data\n"))
		if st != gofuse.OK {
			t.Errorf("Write: %v", st)
			return
		}

		st = fs.Flush(nil, &gofuse.FlushIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		})
		if st != gofuse.OK {
			t.Errorf("Flush: %v", st)
			return
		}

		fs.Release(nil, &gofuse.ReleaseIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		})
	}()

	// The lifecycle should complete very fast (Flush is local disk only).
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Create→Write→Flush→Release timed out")
	}

	// Drain uploader to ensure async upload completes.
	uploader.DrainAll()

	select {
	case uploaded := <-uploadedCh:
		if string(uploaded) != "lifecycle data\n" {
			t.Fatalf("uploaded = %q, want %q", uploaded, "lifecycle data\n")
		}
	case <-time.After(time.Second):
		t.Fatal("data was never uploaded after DrainAll")
	}
}

// TestFlush_WriteBack_MultipleFiles verifies write-back works correctly
// with multiple concurrent files.
func TestFlush_WriteBack_MultipleFiles(t *testing.T) {
	var uploadedFiles sync.Map
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			uploadedFiles.Store(r.URL.Path, string(body))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 4)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	const N = 10
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("multi_%d.txt", idx)
			data := fmt.Sprintf("data-%d", idx)

			var createOut gofuse.CreateOut
			st := fs.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
			}, name, &createOut)
			if st != gofuse.OK {
				t.Errorf("Create(%s): %v", name, st)
				return
			}

			_, st = fs.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
				Fh:       createOut.Fh,
			}, []byte(data))
			if st != gofuse.OK {
				t.Errorf("Write(%s): %v", name, st)
				return
			}

			st = fs.Flush(nil, &gofuse.FlushIn{
				InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
				Fh:       createOut.Fh,
			})
			if st != gofuse.OK {
				t.Errorf("Flush(%s): %v", name, st)
				return
			}

			fs.Release(nil, &gofuse.ReleaseIn{
				InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
				Fh:       createOut.Fh,
			})
		}(i)
	}

	wg.Wait()
	uploader.DrainAll()

	// Verify all files were uploaded.
	for i := 0; i < N; i++ {
		path := fmt.Sprintf("/v1/fs/multi_%d.txt", i)
		expected := fmt.Sprintf("data-%d", i)
		if val, ok := uploadedFiles.Load(path); !ok {
			t.Errorf("file %s not uploaded", path)
		} else if val.(string) != expected {
			t.Errorf("uploaded %s = %q, want %q", path, val, expected)
		}
	}
}

// TestFlush_WriteBack_WriteBetweenFlushAndRelease verifies that if a Write
// occurs between Flush and Release, the latest data is uploaded (not the
// stale cache snapshot). This is a regression test for data loss.
func TestFlush_WriteBack_WriteBetweenFlushAndRelease(t *testing.T) {
	uploadedCh := make(chan []byte, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			uploadedCh <- append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "race.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	// Write v1
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("version1"))
	if st != gofuse.OK {
		t.Fatalf("Write v1: %v", st)
	}

	// Flush — writes "version1" to cache
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}

	// Write v2 AFTER Flush (e.g. dup'd fd scenario)
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("version2-longer"))
	if st != gofuse.OK {
		t.Fatalf("Write v2: %v", st)
	}

	// Release — must upload "version2-longer", NOT stale "version1"
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	uploader.DrainAll()

	// Collect the LAST uploaded data (there might be multiple PUTs).
	var lastUploaded []byte
	for {
		select {
		case data := <-uploadedCh:
			lastUploaded = data
		default:
			goto done
		}
	}
done:
	if lastUploaded == nil {
		t.Fatal("no data was uploaded")
	}
	if string(lastUploaded) != "version2-longer" {
		t.Fatalf("uploaded = %q, want %q (stale cache data was used!)", lastUploaded, "version2-longer")
	}
}

// TestFlush_WriteBack_NoWriteBack_LargeFile verifies that files >= 10MB
// do NOT use write-back cache and follow the normal upload path.
func TestFlush_WriteBack_NoWriteBack_LargeFile(t *testing.T) {
	var httpPutCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			httpPutCalls.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create file
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "large.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	// Write data just at the threshold (10MB) — should NOT use write-back.
	bigData := make([]byte, writeBackThreshold)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}
	// Write in chunks (WriteBuffer has a max size constraint)
	chunkSize := 1 << 20 // 1MB
	for off := 0; off < len(bigData); off += chunkSize {
		end := off + chunkSize
		if end > len(bigData) {
			end = len(bigData)
		}
		_, st = fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   uint64(off),
		}, bigData[off:end])
		if st != gofuse.OK {
			t.Fatalf("Write at offset %d: %v", off, st)
		}
	}

	// Verify no write-back cache entry exists
	if _, ok := cache.Get("/large.bin"); ok {
		t.Fatal("large file should NOT be in write-back cache")
	}

	uploader.DrainAll()
}

// TestRenameFlushesWriteBack verifies that Rename with a pending write-back
// entry uses the local fast path (RenamePending), avoiding a synchronous
// upload on the vim :w critical path. The data is uploaded to the new path
// asynchronously.
func TestRenameFlushesWriteBack(t *testing.T) {
	var ops []string
	var mu sync.Mutex
	record := func(op string) {
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			record("PUT:" + r.URL.Path)
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			if r.URL.RawQuery == "rename" {
				record("RENAME")
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Simulate: file written to write-back cache (pending upload).
	_ = cache.Put("/new.txt", []byte("vim data"), 8, PendingNew)

	// Root inode (1) is pre-created by NewInodeToPath.
	fs.inodes.Lookup("/new.txt", false, 8, time.Time{})
	fs.inodes.Lookup("/target.txt", false, 0, time.Time{})

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "new.txt", "target.txt")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	uploader.DrainAll()

	mu.Lock()
	defer mu.Unlock()

	// Fast path: old cache entry is gone, data is uploaded to new path.
	if _, ok := cache.Get("/new.txt"); ok {
		t.Fatal("old cache entry should be removed after rename")
	}
	if _, ok := cache.Get("/target.txt"); ok {
		t.Fatal("new cache entry should be uploaded and cleaned after drain")
	}

	// The upload should go to /target.txt (not /new.txt), and there should be
	// no remote RENAME call since we used the local fast path.
	foundPut := false
	for _, op := range ops {
		if op == "PUT:/v1/fs/target.txt" {
			foundPut = true
		}
		if op == "RENAME" {
			t.Fatal("local fast path should not issue remote RENAME")
		}
	}
	if !foundPut {
		t.Fatalf("PUT for /target.txt not found in ops: %v", ops)
	}
}

// TestUnlinkClearsPendingWriteBack verifies that Unlink clears any pending
// write-back cache entry. For PendingNew files, the remote DELETE is skipped
// (the file never existed on the server). Cache must be cleaned regardless.
func TestUnlinkClearsPendingWriteBack(t *testing.T) {
	var deleteCalled atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			deleteCalled.Store(true)
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Put a PendingNew file in the write-back cache (never uploaded).
	_ = cache.Put("/ephemeral.txt", []byte("temp"), 4, PendingNew)

	fs.inodes.Lookup("/ephemeral.txt", false, 4, time.Time{})

	st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "ephemeral.txt")
	if st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}

	uploader.DrainAll()

	// PendingNew files skip the remote DELETE — no wasted RTT.
	if deleteCalled.Load() {
		t.Fatal("Unlink should NOT issue remote DELETE for PendingNew files")
	}

	// Cache should be cleaned.
	if _, ok := cache.Get("/ephemeral.txt"); ok {
		t.Fatal("cache entry should be removed after Unlink")
	}
}

// TestReopenAfterClose_ReadsPendingCache verifies close-to-open consistency:
// after writing and closing a file, reopening and reading it returns the
// pending write-back data (not stale server data).
func TestReopenAfterClose_ReadsPendingCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			// Return stale/empty data from server.
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			// Accept but do NOT actually store — simulates async lag.
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	// Don't start workers so the upload stays pending.
	uploader := NewWriteBackUploader(c, cache, 0)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Simulate: file data written to write-back cache (pending async upload).
	_ = cache.Put("/cto.txt", []byte("fresh data"), 10, PendingNew)

	// Simulate opening a read-only handle on the same path.
	ctoIno := fs.inodes.Lookup("/cto.txt", false, 10, time.Time{})

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ctoIno},
		Flags:    uint32(os.O_RDONLY),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ctoIno},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}

	data, _ := result.Bytes(buf)
	if string(data) != "fresh data" {
		t.Fatalf("Read = %q, want %q (close-to-open consistency broken)", string(data), "fresh data")
	}

	uploader.DrainAll()
}

// TestFsync_NoDuplicateUpload verifies that Fsync does not upload twice
// when write-back cache has already persisted the data.
func TestFsync_NoDuplicateUpload(t *testing.T) {
	var httpPutCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			httpPutCalls.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create + Write + Flush (writes to cache)
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "fsync_test.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("fsync data"))
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}

	// Fsync — should upload once via UploadSync, not twice.
	putsBefore := httpPutCalls.Load()
	st = fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}
	putsAfter := httpPutCalls.Load()

	// Exactly 1 PUT expected (UploadSync), not 2.
	if diff := putsAfter - putsBefore; diff != 1 {
		t.Fatalf("Fsync made %d PUT calls, want exactly 1 (double upload detected)", diff)
	}

	uploader.DrainAll()
}

// TestSamePathConsecutiveSaves verifies that rapid consecutive saves to the
// same path don't result in stale data overwriting fresh data, thanks to
// the generation check in uploadOne.
func TestSamePathConsecutiveSaves(t *testing.T) {
	var lastUploaded atomic.Value

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			lastUploaded.Store(string(body))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	// Put "version1" — simulates first save.
	_ = cache.Put("/race.txt", []byte("version1"), 8, PendingNew)
	uploader.Submit("/race.txt")

	// Immediately overwrite with "version2" — simulates second save.
	_ = cache.Put("/race.txt", []byte("version2"), 8, PendingNew)
	uploader.Submit("/race.txt")

	uploader.DrainAll()

	// The last uploaded value should be "version2".
	val := lastUploaded.Load()
	if val == nil {
		t.Fatal("nothing was uploaded")
	}
	if val.(string) != "version2" {
		t.Fatalf("last uploaded = %q, want %q (stale overwrite detected)", val, "version2")
	}

	// Cache should be cleaned up after the second upload.
	if _, ok := cache.Get("/race.txt"); ok {
		t.Fatal("cache should be cleared after final upload")
	}
}

// TestLookupFindsPendingWriteBack verifies that Lookup returns metadata
// for files that only exist in the write-back cache (pending upload).
func TestLookupFindsPendingWriteBack(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			// Return 404 for everything — file is only in local cache.
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, nil)

	// File only in write-back cache (not on server).
	_ = cache.Put("/pending.txt", []byte("local only"), 10, PendingNew)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "pending.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup: %v — file should be visible from write-back cache", st)
	}
	if out.Attr.Size != 10 {
		t.Fatalf("Lookup size = %d, want 10", out.Attr.Size)
	}
}

// TestOpenWritable_PreloadsFromWriteBackCache verifies that opening a file
// for writing preloads data from the write-back cache instead of the remote.
func TestOpenWritable_PreloadsFromWriteBackCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			// Return stale data size from "server"
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			// Return stale server data
			w.Write([]byte("stale"))
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 0) // no workers — keep data pending

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Fresh data in write-back cache.
	_ = cache.Put("/file.txt", []byte("fresh data"), 10, PendingNew)

	ino := fs.inodes.Lookup("/file.txt", false, 10, time.Time{})

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(os.O_RDWR),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	// Read from the writable handle — should see fresh data, not stale.
	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}

	data, _ := result.Bytes(buf)
	if string(data) != "fresh data" {
		t.Fatalf("Read = %q, want %q (writable preload used stale data)", string(data), "fresh data")
	}

	uploader.DrainAll()
}

// TestUploader_WaitPath verifies that WaitPath blocks until an in-flight
// upload completes.
func TestUploader_WaitPath(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadRelease // block until test says continue
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/inflight.txt", []byte("data"), 4, PendingNew)
	uploader.Submit("/inflight.txt")

	// Wait for the upload to actually start.
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// WaitPath should block while the upload is in flight.
	waited := make(chan struct{})
	go func() {
		uploader.WaitPath("/inflight.txt")
		close(waited)
	}()

	// Verify WaitPath is actually blocking.
	select {
	case <-waited:
		t.Fatal("WaitPath returned before upload finished")
	case <-time.After(50 * time.Millisecond):
		// Good — still blocking.
	}

	// Release the upload.
	close(uploadRelease)

	// WaitPath should return now.
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitPath did not return after upload finished")
	}

	uploader.DrainAll()
}

// TestWriteBackCache_RenamePending verifies that RenamePending moves a cache
// entry from one path to another atomically.
func TestWriteBackCache_RenamePending(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/old.txt", []byte("data"), 4, PendingNew)

	if !cache.RenamePending("/old.txt", "/new.txt") {
		t.Fatal("RenamePending returned false, expected true")
	}

	// Old path should be gone.
	if _, ok := cache.Get("/old.txt"); ok {
		t.Fatal("old path should not exist after rename")
	}

	// New path should have the data.
	data, ok := cache.Get("/new.txt")
	if !ok {
		t.Fatal("new path should exist after rename")
	}
	if string(data) != "data" {
		t.Fatalf("data = %q, want %q", data, "data")
	}

	// Meta should reflect the new path.
	meta, ok := cache.GetMeta("/new.txt")
	if !ok {
		t.Fatal("meta should exist for new path")
	}
	if meta.Path != "/new.txt" {
		t.Fatalf("meta.Path = %q, want /new.txt", meta.Path)
	}
}

// TestWriteBackCache_Generation verifies that Put assigns monotonically
// increasing generation numbers.
func TestWriteBackCache_Generation(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/gen.txt", []byte("v1"), 2, PendingNew)
	meta1, ok := cache.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after first Put")
	}

	_ = cache.Put("/gen.txt", []byte("v2"), 2, PendingNew)
	meta2, ok := cache.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after second Put")
	}

	if meta2.Generation <= meta1.Generation {
		t.Fatalf("generation did not increase: %d <= %d", meta2.Generation, meta1.Generation)
	}
}

// TestRename_PendingOverwrite_UsesSlowPath verifies that Rename falls back to
// the slow path (sync upload + remote rename) for pending-overwrite files,
// rather than using the local RenamePending fast path.
func TestRename_PendingOverwrite_UsesSlowPath(t *testing.T) {
	var ops []string
	var mu sync.Mutex
	record := func(op string) {
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			record("PUT:" + r.URL.Path)
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			if r.URL.RawQuery == "rename" {
				record("RENAME")
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Pending-overwrite: file existed on server, was edited locally.
	_ = cache.PutWithBaseRev("/existing.txt", []byte("edited data"), 11, PendingOverwrite, 7)

	fs.inodes.Lookup("/existing.txt", false, 11, time.Time{})
	fs.inodes.Lookup("/renamed.txt", false, 0, time.Time{})

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "existing.txt", "renamed.txt")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	uploader.DrainAll()

	mu.Lock()
	defer mu.Unlock()

	// Slow path: should upload oldP first (flushPendingWriteBack), then remote RENAME.
	foundPut := false
	foundRename := false
	for _, op := range ops {
		if op == "PUT:/v1/fs/existing.txt" {
			foundPut = true
		}
		if op == "RENAME" {
			foundRename = true
		}
	}
	if !foundPut {
		t.Fatalf("PendingOverwrite should upload to old path first; ops = %v", ops)
	}
	if !foundRename {
		t.Fatalf("PendingOverwrite should issue remote RENAME; ops = %v", ops)
	}
}

// TestWriteBackCache_PendingIndex verifies that the in-memory pending index
// stays in sync with disk operations.
func TestWriteBackCache_PendingIndex(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Initially empty.
	if paths := cache.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("expected 0 pending paths, got %d", len(paths))
	}

	// Put adds to index.
	_ = cache.Put("/a.txt", []byte("a"), 1, PendingNew)
	_ = cache.Put("/b.txt", []byte("b"), 1, PendingNew)
	paths := cache.ListPendingPaths()
	if len(paths) != 2 {
		t.Fatalf("expected 2 pending paths, got %d", len(paths))
	}

	// Remove updates index.
	cache.Remove("/a.txt")
	paths = cache.ListPendingPaths()
	if len(paths) != 1 {
		t.Fatalf("expected 1 pending path after Remove, got %d", len(paths))
	}
	if _, ok := paths["/b.txt"]; !ok {
		t.Fatal("/b.txt should still be pending")
	}

	// RenamePending updates index.
	cache.RenamePending("/b.txt", "/c.txt")
	paths = cache.ListPendingPaths()
	if len(paths) != 1 {
		t.Fatalf("expected 1 pending path after Rename, got %d", len(paths))
	}
	if _, ok := paths["/c.txt"]; !ok {
		t.Fatal("/c.txt should be pending after rename")
	}
	if _, ok := paths["/b.txt"]; ok {
		t.Fatal("/b.txt should NOT be pending after rename")
	}

	// Reopen cache from disk — index should be rebuilt.
	cache2, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	paths2 := cache2.ListPendingPaths()
	if len(paths2) != 1 {
		t.Fatalf("reopened cache: expected 1 pending path, got %d", len(paths2))
	}
	if _, ok := paths2["/c.txt"]; !ok {
		t.Fatal("reopened cache: /c.txt should be pending")
	}
}

// TestUnlink_PendingNew_SkipsRemoteDelete verifies that Unlink skips the
// remote DELETE for PendingNew files (never uploaded), avoiding a wasted RTT.
func TestUnlink_PendingNew_SkipsRemoteDelete(t *testing.T) {
	var deleteCalled atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			deleteCalled.Store(true)
			http.Error(w, "not found", http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// PendingNew: file created locally, never uploaded.
	_ = cache.Put("/new-only.txt", []byte("local"), 5, PendingNew)
	fs.inodes.Lookup("/new-only.txt", false, 5, time.Time{})

	st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "new-only.txt")
	if st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}

	uploader.DrainAll()

	// NO remote DELETE should be issued for PendingNew files.
	if deleteCalled.Load() {
		t.Fatal("Unlink should skip remote DELETE for PendingNew files")
	}

	// Cache must be cleaned.
	if _, ok := cache.Get("/new-only.txt"); ok {
		t.Fatal("cache entry should be removed after Unlink")
	}
}

// TestUnlink_PendingOverwrite_DeletesRemote verifies that Unlink issues a
// remote DELETE for PendingOverwrite files (the file existed on the server).
func TestUnlink_PendingOverwrite_DeletesRemote(t *testing.T) {
	var deleteCalled atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			deleteCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// PendingOverwrite: file existed on server, edited locally.
	_ = cache.PutWithBaseRev("/existing.txt", []byte("edited"), 6, PendingOverwrite, 7)
	fs.inodes.Lookup("/existing.txt", false, 6, time.Time{})

	st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "existing.txt")
	if st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}

	uploader.DrainAll()

	// Remote DELETE MUST be issued for PendingOverwrite files.
	if !deleteCalled.Load() {
		t.Fatal("Unlink should issue remote DELETE for PendingOverwrite files")
	}

	// Cache must be cleaned.
	if _, ok := cache.Get("/existing.txt"); ok {
		t.Fatal("cache entry should be removed after Unlink")
	}
}

// TestPreload_LazyLoad_SmallFile verifies that opening a small existing file
// for writing only issues StatCtx (1 RTT) during Open — NOT ReadCtx. The data
// is loaded lazily on first Read via LoadPart.
func TestPreload_LazyLoad_SmallFile(t *testing.T) {
	var statCalls, readCalls atomic.Int32
	fileData := []byte("small file content") // well under smallFileThreshold
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			statCalls.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileData)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			readCalls.Add(1)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileData)))
			w.Write(fileData)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	c := client.New(ts.URL, "")

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)

	ino := fs.inodes.Lookup("/small.txt", false, int64(len(fileData)), time.Time{})

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(os.O_RDWR),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	// After Open: StatCtx was called, but ReadCtx/GET was NOT called.
	if statCalls.Load() == 0 {
		t.Fatal("Open should issue StatCtx")
	}
	if readCalls.Load() != 0 {
		t.Fatalf("Open should NOT eagerly read data; readCalls = %d", readCalls.Load())
	}

	// First Read triggers lazy LoadPart.
	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}

	data, _ := result.Bytes(buf)
	if string(data) != string(fileData) {
		t.Fatalf("Read = %q, want %q", string(data), string(fileData))
	}

	// Now a GET should have been issued (lazy load on first read).
	if readCalls.Load() == 0 {
		t.Fatal("Read should trigger lazy LoadPart (GET)")
	}
}

// TestRmdir_CleansPendingDescendants verifies that Rmdir removes write-back
// cache entries for files under the deleted directory.
func TestRmdir_CleansPendingDescendants(t *testing.T) {
	var uploadedPaths sync.Map
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			uploadedPaths.Store(r.URL.Path, true)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "true")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create pending files under /dir/.
	_ = cache.Put("/dir/file1.txt", []byte("data1"), 5, PendingNew)
	_ = cache.Put("/dir/sub/file2.txt", []byte("data2"), 5, PendingNew)
	// Also a file NOT under /dir/ — should remain.
	_ = cache.Put("/other.txt", []byte("other"), 5, PendingNew)

	fs.inodes.Lookup("/dir", true, 0, time.Time{})

	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.OK {
		t.Fatalf("Rmdir: %v", st)
	}

	uploader.DrainAll()

	// Children should be removed from cache.
	if _, ok := cache.Get("/dir/file1.txt"); ok {
		t.Fatal("/dir/file1.txt should be removed from cache after Rmdir")
	}
	if _, ok := cache.Get("/dir/sub/file2.txt"); ok {
		t.Fatal("/dir/sub/file2.txt should be removed from cache after Rmdir")
	}

	// Unrelated file should still be present.
	if _, ok := cache.Get("/other.txt"); !ok {
		t.Fatal("/other.txt should NOT be removed by Rmdir of /dir")
	}

	// No upload attempts for the cleaned children.
	if _, loaded := uploadedPaths.Load("/v1/fs/dir/file1.txt"); loaded {
		t.Fatal("should not attempt upload for /dir/file1.txt after Rmdir")
	}
	if _, loaded := uploadedPaths.Load("/v1/fs/dir/sub/file2.txt"); loaded {
		t.Fatal("should not attempt upload for /dir/sub/file2.txt after Rmdir")
	}
}

// TestRename_Directory_MigratesPendingDescendants verifies that renaming a
// directory re-keys pending write-back entries under the old path to the new,
// and that migrated descendants are actually uploaded to the new paths.
func TestRename_Directory_MigratesPendingDescendants(t *testing.T) {
	var uploadedPaths sync.Map
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "true")
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			// Accept rename
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			uploadedPaths.Store(r.URL.Path, true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	uploader := NewWriteBackUploader(c, cache, 2)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create pending files under /olddir/.
	_ = cache.Put("/olddir/file1.txt", []byte("d1"), 2, PendingNew)
	_ = cache.PutWithBaseRev("/olddir/sub/file2.txt", []byte("d2"), 2, PendingOverwrite, 11)

	fs.inodes.Lookup("/olddir", true, 0, time.Time{})
	fs.inodes.Lookup("/newdir", true, 0, time.Time{})

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "olddir", "newdir")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	// Old paths should be gone.
	pending := cache.ListPendingPaths()
	if _, ok := pending["/olddir/file1.txt"]; ok {
		t.Fatal("/olddir/file1.txt should be migrated away")
	}
	if _, ok := pending["/olddir/sub/file2.txt"]; ok {
		t.Fatal("/olddir/sub/file2.txt should be migrated away")
	}

	// New paths should exist.
	if _, ok := pending["/newdir/file1.txt"]; !ok {
		t.Fatal("/newdir/file1.txt should exist after directory rename")
	}
	if _, ok := pending["/newdir/sub/file2.txt"]; !ok {
		t.Fatal("/newdir/sub/file2.txt should exist after directory rename")
	}

	// Data should be readable at new paths.
	data, ok := cache.Get("/newdir/file1.txt")
	if !ok || string(data) != "d1" {
		t.Fatalf("data at /newdir/file1.txt = %q, %v; want %q", string(data), ok, "d1")
	}
	data, ok = cache.Get("/newdir/sub/file2.txt")
	if !ok || string(data) != "d2" {
		t.Fatalf("data at /newdir/sub/file2.txt = %q, %v; want %q", string(data), ok, "d2")
	}

	// Drain uploader — migrated descendants should be uploaded to NEW paths.
	uploader.DrainAll()

	// Verify uploads happened at new paths, not old paths.
	if _, loaded := uploadedPaths.Load("/v1/fs/newdir/file1.txt"); !loaded {
		t.Fatal("migrated /newdir/file1.txt should be uploaded after directory rename")
	}
	if _, loaded := uploadedPaths.Load("/v1/fs/newdir/sub/file2.txt"); !loaded {
		t.Fatal("migrated /newdir/sub/file2.txt should be uploaded after directory rename")
	}
	if _, loaded := uploadedPaths.Load("/v1/fs/olddir/file1.txt"); loaded {
		t.Fatal("should NOT upload to old path /olddir/file1.txt")
	}
	if _, loaded := uploadedPaths.Load("/v1/fs/olddir/sub/file2.txt"); loaded {
		t.Fatal("should NOT upload to old path /olddir/sub/file2.txt")
	}
}

// TestFlush_SkipsRedundantCacheWrite verifies that Flush does not re-write
// the write-back cache when no new writes have occurred since the last Flush.
func TestFlush_SkipsRedundantCacheWrite(t *testing.T) {
	var putCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			putCount.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := client.New(ts.URL, "")
	// No workers — uploads stay pending so we can inspect cache writes.
	uploader := NewWriteBackUploader(c, cache, 0)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	// Create a file and write data.
	ino := fs.inodes.Lookup("/flush-dedup.txt", false, 0, time.Time{})
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE),
	}, "flush-dedup.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh := createOut.Fh

	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fh,
		Offset:   0,
		Size:     5,
	}, []byte("hello"))
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	// First Flush — should write to cache.
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush 1: %v", st)
	}

	// Verify data was cached.
	data, ok := cache.Get("/flush-dedup.txt")
	if !ok || string(data) != "hello" {
		t.Fatalf("cache after Flush 1: %q, %v", string(data), ok)
	}

	// Record cache generation after first Flush.
	meta1, _ := cache.GetMeta("/flush-dedup.txt")
	gen1 := meta1.Generation

	// Second Flush without any intervening writes — should short-circuit.
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush 2: %v", st)
	}

	// Cache generation should NOT have changed (no re-write).
	meta2, _ := cache.GetMeta("/flush-dedup.txt")
	if meta2.Generation != gen1 {
		t.Fatalf("redundant Flush re-wrote cache: gen %d → %d", gen1, meta2.Generation)
	}

	uploader.DrainAll()
}
