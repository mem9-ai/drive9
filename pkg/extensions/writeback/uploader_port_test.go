package writeback

// WriteBackCache (.dat/.meta) and WriteBackUploader rows, ported verbatim
// from v1 pkg/fuse/writeback_test.go (the small-file writeback track).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestWriteBackCache_PutGetRemove(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
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
	if err := cache.Put(path, data, int64(len(data)), staging.PendingNew); err != nil {
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
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.PutWithBaseRev("/existing.txt", []byte("hello"), 5, staging.PendingOverwrite, 17); err != nil {
		t.Fatal(err)
	}

	meta, ok := cache.GetMeta("/existing.txt")
	if !ok {
		t.Fatal("expected GetMeta to return true")
	}
	if meta.Kind != staging.PendingOverwrite {
		t.Fatalf("meta.Kind = %v, want %v", meta.Kind, staging.PendingOverwrite)
	}
	if meta.BaseRev != 17 {
		t.Fatalf("meta.BaseRev = %d, want 17", meta.BaseRev)
	}
}

func TestWriteBackCache_GetMetaUsesInMemoryIndex(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/existing.txt"
	if err := cache.PutWithBaseRev(path, []byte("hello"), 5, staging.PendingOverwrite, 17); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(cache.MetaFile(path)); err != nil {
		t.Fatal(err)
	}

	meta, ok := cache.GetMeta(path)
	if !ok {
		t.Fatal("expected GetMeta to return true from memory index")
	}
	if meta.BaseRev != 17 {
		t.Fatalf("meta.BaseRev = %d, want 17", meta.BaseRev)
	}
	if meta.Kind != staging.PendingOverwrite {
		t.Fatalf("meta.Kind = %v, want %v", meta.Kind, staging.PendingOverwrite)
	}
}

func TestWriteBackCache_GetUsesInMemorySmallDataCache(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/small.txt"
	data := []byte("hello")
	if err := cache.Put(path, data, int64(len(data)), staging.PendingNew); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(cache.DatFile(path)); err != nil {
		t.Fatal(err)
	}

	got, ok := cache.Get(path)
	if !ok {
		t.Fatal("expected Get to return true from small data cache")
	}
	if string(got) != string(data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
	view, ok := cache.GetView(path)
	if !ok {
		t.Fatal("expected getView to return true from small data cache")
	}
	if string(view) != string(data) {
		t.Fatalf("getView = %q, want %q", view, data)
	}
}

func TestWriteBackCache_LargeDataNotCachedInMemory(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/large.bin"
	data := make([]byte, staging.WriteBackInMemoryDataThreshold+1)
	if err := cache.Put(path, data, int64(len(data)), staging.PendingNew); err != nil {
		t.Fatal(err)
	}

	if cache.HasInMemoryPayload(path) {
		t.Fatal("large payload should not be retained in memory cache")
	}
}

func TestWriteBackCache_GetViewPrunesMissingLargeData(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/large.bin"
	data := make([]byte, staging.WriteBackInMemoryDataThreshold+1)
	if err := cache.Put(path, data, int64(len(data)), staging.PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cache.DatFile(path)); err != nil {
		t.Fatal(err)
	}

	if _, ok := cache.GetView(path); ok {
		t.Fatal("expected getView miss after data file removal")
	}
	if _, ok := cache.GetMeta(path); ok {
		t.Fatal("expected missing data to prune in-memory meta")
	}
	if _, err := os.Stat(cache.MetaFile(path)); !os.IsNotExist(err) {
		t.Fatal("expected missing data to prune on-disk meta")
	}
	if paths := cache.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("ListPendingPaths len = %d, want 0", len(paths))
	}
}

func TestWriteBackCache_SmallDataCacheBudgetEvictsLRU(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	cache.SetInMemoryBudget(10)

	if err := cache.Put("/a.txt", []byte("aaaaa"), 5, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put("/b.txt", []byte("bbbbb"), 5, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.GetView("/a.txt"); !ok {
		t.Fatal("expected /a.txt to be cached")
	}
	if err := cache.Put("/c.txt", []byte("ccccc"), 5, staging.PendingNew); err != nil {
		t.Fatal(err)
	}

	if cache.InMemoryUsage() > 10 {
		t.Fatalf("dataBytes = %d, want <= 10", cache.InMemoryUsage())
	}
	if !cache.HasInMemoryPayload("/a.txt") {
		t.Fatal("expected /a.txt to remain cached as most-recent entry")
	}
	if !cache.HasInMemoryPayload("/c.txt") {
		t.Fatal("expected /c.txt to remain cached as newest entry")
	}
	if cache.HasInMemoryPayload("/b.txt") {
		t.Fatal("expected /b.txt to be evicted by LRU budget")
	}
}

func TestWriteBackCache_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/atomic/test.txt"
	data := []byte("atomic data")

	if err := cache.Put(path, data, int64(len(data)), staging.PendingNew); err != nil {
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
	hash := staging.HashPath(path)
	if _, err := os.Stat(filepath.Join(dir, hash+".dat")); err != nil {
		t.Fatalf("dat file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, hash+".meta")); err != nil {
		t.Fatalf("meta file missing: %v", err)
	}
}

func TestWriteBackCache_ListPending(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Put two entries.
	_ = cache.Put("/a.txt", []byte("aaa"), 3, staging.PendingNew)
	_ = cache.Put("/b.txt", []byte("bbb"), 3, staging.PendingNew)

	pending := cache.ListPending()
	if len(pending) != 2 {
		t.Fatalf("ListPending returned %d entries, want 2", len(pending))
	}

	// Map by path for easier assertion.
	byPath := map[string]staging.PendingEntry{}
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

func TestWriteBackCache_ListPending_SkipsConflictEntries(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.Put("/ok.txt", []byte("ok"), 2, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if err := cache.PutWithBaseRev("/conflict.txt", []byte("conflict"), 8, staging.PendingConflict, 9); err != nil {
		t.Fatal(err)
	}

	pending := cache.ListPending()
	if len(pending) != 1 {
		t.Fatalf("ListPending returned %d entries, want 1", len(pending))
	}
	if pending[0].Meta.Path != "/ok.txt" {
		t.Fatalf("pending path = %q, want /ok.txt", pending[0].Meta.Path)
	}
}

func TestWriteBackCache_ListPending_CorruptMeta(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Write a corrupt .meta file.
	hash := staging.HashPath("/corrupt.txt")
	_ = os.WriteFile(filepath.Join(dir, hash+".meta"), []byte("not json"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, hash+".dat"), []byte("data"), 0o644)

	// Write a valid entry.
	_ = cache.Put("/valid.txt", []byte("ok"), 2, staging.PendingNew)

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

func TestNewWriteBackCache_PrunesMismatchedMetaPath(t *testing.T) {
	dir := t.TempDir()

	wrongPath := "/wrong.txt"
	scannedPath := "/actual.txt"
	scannedHash := staging.HashPath(scannedPath)
	meta := staging.WriteBackMeta{
		Path:       wrongPath,
		Size:       4,
		Mtime:      time.Now(),
		CreatedAt:  time.Now(),
		Generation: 3,
		Kind:       staging.PendingNew,
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, scannedHash+".meta"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, scannedHash+".dat"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if paths := cache.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("ListPendingPaths len = %d, want 0", len(paths))
	}
	if _, err := os.Stat(filepath.Join(dir, scannedHash+".meta")); !os.IsNotExist(err) {
		t.Fatal("mismatched .meta should be removed during cache load")
	}
	if _, err := os.Stat(filepath.Join(dir, scannedHash+".dat")); !os.IsNotExist(err) {
		t.Fatal("mismatched .dat should be removed during cache load")
	}
}

func TestWriteBackCache_ListPending_PrunesEmptyMetaPath(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	hash := staging.HashPath("/scanned.txt")
	meta := staging.WriteBackMeta{
		Path:       "",
		Size:       4,
		Mtime:      time.Now(),
		CreatedAt:  time.Now(),
		Generation: 2,
		Kind:       staging.PendingNew,
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hash+".meta"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hash+".dat"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put("/valid.txt", []byte("ok"), 2, staging.PendingNew); err != nil {
		t.Fatal(err)
	}

	pending := cache.ListPending()
	if len(pending) != 1 {
		t.Fatalf("ListPending returned %d entries, want 1", len(pending))
	}
	if pending[0].Meta.Path != "/valid.txt" {
		t.Fatalf("pending path = %q, want /valid.txt", pending[0].Meta.Path)
	}
	if _, err := os.Stat(filepath.Join(dir, hash+".meta")); !os.IsNotExist(err) {
		t.Fatal("empty-path .meta should be removed during reconciliation")
	}
	if _, err := os.Stat(filepath.Join(dir, hash+".dat")); !os.IsNotExist(err) {
		t.Fatal("empty-path .dat should be removed during reconciliation")
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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 2)

	// Put data in cache and submit for upload.
	_ = cache.Put("/upload-test.txt", []byte("upload data"), 11, staging.PendingNew)
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

// TestWriteBackUploader_SubmitBackpressureNeverDrops verifies Submit's
// backpressure contract: while the upload channel stays full, Submit must
// block until a worker frees space (or time out into the synchronous
// fallback) — it must never return while silently dropping the path.
func TestWriteBackUploader_SubmitBackpressureNeverDrops(t *testing.T) {
	const total = 300 // > uploadCh capacity (256) + workers, so Submit must wait
	release := make(chan struct{})
	var puts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
			<-release // hold every upload until the queue is provably saturated
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	for i := 0; i < total; i++ {
		// NoSync staging: the test only needs the entries readable by the
		// uploader, not durable.
		if _, _, err := cache.PutWithBaseRevAndModeAndLineageTimingsNoSync(fmt.Sprintf("/bp/f%03d.txt", i), []byte("x"), 1, staging.PendingNew, 0, 0, false, "", "", false); err != nil {
			t.Fatal(err)
		}
	}

	var submitted atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			submitted.Add(1)
			uploader.Submit(fmt.Sprintf("/bp/f%03d.txt", i))
		}
	}()

	// Saturation: the queue is full and the submitter has counted past what
	// capacity can hold — that Submit is inside its backpressure wait (or,
	// in the regression, already returned without enqueuing).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(uploader.uploadCh) == cap(uploader.uploadCh) && int(submitted.Load()) >= cap(uploader.uploadCh)+1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(uploader.uploadCh) != cap(uploader.uploadCh) || int(submitted.Load()) < cap(uploader.uploadCh)+1 {
		t.Fatalf("queue never saturated: submitted=%d queued=%d", submitted.Load(), len(uploader.uploadCh))
	}
	close(release)
	<-done
	uploader.DrainAll()

	if n := puts.Load(); n != total {
		t.Fatalf("uploaded %d of %d submitted paths — Submit dropped acknowledged work under backpressure", n, total)
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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)

	// Simulate entries from a previous session.
	_ = cache.Put("/recover1.txt", []byte("data1"), 5, staging.PendingNew)
	_ = cache.Put("/recover2.txt", []byte("data2"), 5, staging.PendingNew)

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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/fail.txt", []byte("fail data"), 9, staging.PendingNew)
	uploader.Submit("/fail.txt")
	uploader.DrainAll()

	// Cache entry should be retained for retry on next mount.
	if _, ok := cache.Get("/fail.txt"); !ok {
		t.Fatal("cache entry should be retained after upload failure")
	}
}

func TestWriteBackUploader_ChmodFailureRetainsCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			http.Error(w, "chmod failed", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/mode-fail.txt", []byte("data"), 4, staging.PendingNew, 0, 0o755, true)
	uploader.Submit("/mode-fail.txt")
	uploader.DrainAll()

	if _, ok := cache.Get("/mode-fail.txt"); !ok {
		t.Fatal("cache entry should be retained after chmod failure")
	}
	meta, ok := cache.GetMeta("/mode-fail.txt")
	if !ok {
		t.Fatal("cache metadata should be retained after chmod failure")
	}
	if meta.Kind != staging.PendingChmod {
		t.Fatalf("meta kind = %v, want staging.PendingChmod", meta.Kind)
	}
}

func TestWriteBackUploader_ChmodFailureRetryDoesNotReuploadData(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			if putCalls.Add(1) > 1 {
				http.Error(w, "unexpected data reupload", http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			if chmodCalls.Add(1) == 1 {
				http.Error(w, "chmod failed", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/mode-retry.txt", []byte("data"), 4, staging.PendingNew, 0, 0o755, true)
	uploader.Submit("/mode-retry.txt")
	uploader.DrainAll()

	meta, ok := cache.GetMeta("/mode-retry.txt")
	if !ok {
		t.Fatal("cache metadata should be retained after first chmod failure")
	}
	if meta.Kind != staging.PendingChmod {
		t.Fatalf("meta kind after first attempt = %v, want staging.PendingChmod", meta.Kind)
	}

	uploader.Submit("/mode-retry.txt")
	uploader.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 2 {
		t.Fatalf("chmod calls = %d, want 2", got)
	}
	if _, ok := cache.GetMeta("/mode-retry.txt"); ok {
		t.Fatal("cache metadata should be removed after chmod retry succeeds")
	}
}

// TestWriteBackUploader_PendingChmodWithMissingDat verifies that a staging.PendingChmod
// entry whose .dat file has been removed (data already uploaded) is still
// processed correctly: chmod is called and the entry is cleaned up.
// Regression test for: GetMetaAndView would prune the entry when .dat is missing,
// silently dropping the pending chmod.
func TestWriteBackUploader_PendingChmodWithMissingDat(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	// Step 1: Put a file with mode, then mark as staging.PendingChmod (simulating
	// data upload succeeded but chmod failed).
	_ = cache.PutWithBaseRevAndMode("/chmod-no-dat.txt", []byte("data"), 4, staging.PendingNew, 0, 0o755, true)
	gen := cache.MetaGeneration("/chmod-no-dat.txt")
	updated, err := cache.MarkChmodPending("/chmod-no-dat.txt", gen)
	if err != nil || !updated {
		t.Fatalf("MarkChmodPending: updated=%v err=%v", updated, err)
	}

	// Step 2: Remove the .dat file to simulate the scenario where data was
	// already uploaded and the .dat is gone.
	datPath := cache.DatFile("/chmod-no-dat.txt")
	if err := os.Remove(datPath); err != nil {
		t.Fatalf("remove .dat: %v", err)
	}

	// Also evict in-memory data cache so getViewLocked must hit disk.
	cache.DropInMemoryPayload("/chmod-no-dat.txt")

	// Step 3: Submit for upload — should call chmod and clean up.
	uploader.Submit("/chmod-no-dat.txt")
	uploader.DrainAll()

	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if _, ok := cache.GetMeta("/chmod-no-dat.txt"); ok {
		t.Fatal("cache metadata should be removed after staging.PendingChmod succeeds")
	}
}

// TestWriteBackUploader_UploadSyncPendingChmodWithMissingDat is the same
// regression test but for the UploadSync (fsync/rename) path.
func TestWriteBackUploader_UploadSyncPendingChmodWithMissingDat(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/sync-chmod-no-dat.txt", []byte("data"), 4, staging.PendingNew, 0, 0o755, true)
	gen := cache.MetaGeneration("/sync-chmod-no-dat.txt")
	updated, err := cache.MarkChmodPending("/sync-chmod-no-dat.txt", gen)
	if err != nil || !updated {
		t.Fatalf("MarkChmodPending: updated=%v err=%v", updated, err)
	}

	datPath := cache.DatFile("/sync-chmod-no-dat.txt")
	if err := os.Remove(datPath); err != nil {
		t.Fatalf("remove .dat: %v", err)
	}
	cache.DropInMemoryPayload("/sync-chmod-no-dat.txt")

	err = uploader.UploadSync(context.Background(), "/sync-chmod-no-dat.txt")
	if err != nil {
		t.Fatalf("UploadSync returned error: %v", err)
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if _, ok := cache.GetMeta("/sync-chmod-no-dat.txt"); ok {
		t.Fatal("cache metadata should be removed after staging.PendingChmod succeeds")
	}
}

func TestWriteBackUploaderSkipsDefaultModeForPendingNew(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			http.Error(w, "unexpected chmod", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/plain.txt", []byte("data"), 4, staging.PendingNew, 0, defaultRegularFileMode, true)
	uploader.Submit("/plain.txt")
	uploader.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 0 {
		t.Fatalf("chmod calls = %d, want 0", got)
	}
	if _, ok := cache.Get("/plain.txt"); ok {
		t.Fatal("cache entry should be removed after successful default-mode upload")
	}
}

func TestWriteBackUploader_UploadSyncChmodFailureReturnsErrorAndRetainsCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			http.Error(w, "chmod failed", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/mode-sync-fail.txt", []byte("data"), 4, staging.PendingNew, 0, 0o755, true)
	err := uploader.UploadSync(context.Background(), "/mode-sync-fail.txt")
	if err == nil {
		t.Fatal("UploadSync should fail when chmod fails")
	}
	if _, ok := cache.Get("/mode-sync-fail.txt"); !ok {
		t.Fatal("cache entry should be retained after UploadSync chmod failure")
	}
	meta, ok := cache.GetMeta("/mode-sync-fail.txt")
	if !ok {
		t.Fatal("cache metadata should be retained after UploadSync chmod failure")
	}
	if meta.Kind != staging.PendingChmod {
		t.Fatalf("meta kind = %v, want staging.PendingChmod", meta.Kind)
	}
	uploader.DrainAll()
}

func TestWriteBackUploader_UploadSyncUploadFailureRetainsDataPendingKind(t *testing.T) {
	var chmodCalled atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			http.Error(w, "upload failed", http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/sync-upload-fail.txt", []byte("data"), 4, staging.PendingNew, 0, 0o755, true)
	err := uploader.UploadSync(context.Background(), "/sync-upload-fail.txt")
	if err == nil {
		t.Fatal("UploadSync should fail when data upload fails")
	}
	if data, ok := cache.Get("/sync-upload-fail.txt"); !ok || string(data) != "data" {
		t.Fatalf("cache data = %q, %v; want retained data", string(data), ok)
	}
	meta, ok := cache.GetMeta("/sync-upload-fail.txt")
	if !ok {
		t.Fatal("cache metadata should be retained after upload failure")
	}
	if meta.Kind != staging.PendingNew {
		t.Fatalf("meta kind = %v, want staging.PendingNew", meta.Kind)
	}
	if chmodCalled.Load() {
		t.Fatal("chmod should not be attempted after upload failure")
	}
	uploader.DrainAll()
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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/new.txt", []byte("new"), 3, staging.PendingNew)
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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRev("/existing.txt", []byte("edit"), 4, staging.PendingOverwrite, 23)
	uploader.Submit("/existing.txt")
	uploader.DrainAll()

	if gotExpected != "23" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want %q", gotExpected, "23")
	}
}

func TestWriteBackUploader_OnSuccessInfersStreamRevision(t *testing.T) {
	var gotExpected string
	data := []byte("edit")
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != "/existing.txt" {
				t.Fatalf("initiate path = %q, want /existing.txt", req.Path)
			}
			if req.TotalSize != int64(len(data)) {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, len(data))
			}
			if req.ExpectedRevision == nil {
				t.Fatal("initiate expected_revision missing")
			}
			gotExpected = strconv.FormatInt(*req.ExpectedRevision, 10)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u1",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u1/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u1/1":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read part body: %v", err)
			}
			if !bytes.Equal(body, data) {
				t.Fatalf("part body = %q, want %q", body, data)
			}
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	uploader := NewWriteBackUploader(c, cache, 1)

	var (
		successMeta staging.WriteBackMeta
		successRev  int64
	)
	uploader.OnSuccess = func(meta staging.WriteBackMeta, committedRev int64, gens StagingGens) {
		successMeta = meta
		successRev = committedRev
	}

	_ = cache.PutWithBaseRev("/existing.txt", data, int64(len(data)), staging.PendingOverwrite, 23)
	uploader.Submit("/existing.txt")
	uploader.DrainAll()

	if gotExpected != "23" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want %q", gotExpected, "23")
	}
	if successMeta.Path != "/existing.txt" {
		t.Fatalf("OnSuccess path = %q, want /existing.txt", successMeta.Path)
	}
	if successRev != 24 {
		t.Fatalf("OnSuccess committedRev = %d, want 24 from CAS base revision", successRev)
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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/legacy-overwrite.txt", []byte("edit"), 4, staging.PendingOverwrite)
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
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/legacy-sync.txt", []byte("edit"), 4, staging.PendingOverwrite)
	committedRev, err := uploader.UploadSyncWithRevision(context.Background(), "/legacy-sync.txt")
	if err != nil {
		t.Fatalf("UploadSyncWithRevision: %v", err)
	}
	if committedRev != 0 {
		t.Fatalf("UploadSyncWithRevision committedRev = %d, want 0 for unconditional legacy fallback", committedRev)
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
