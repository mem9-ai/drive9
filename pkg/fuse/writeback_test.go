package fuse

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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/s3client"
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

func TestWriteBackCache_GetMetaUsesInMemoryIndex(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/existing.txt"
	if err := cache.PutWithBaseRev(path, []byte("hello"), 5, PendingOverwrite, 17); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(cache.metaFile(path)); err != nil {
		t.Fatal(err)
	}

	meta, ok := cache.GetMeta(path)
	if !ok {
		t.Fatal("expected GetMeta to return true from memory index")
	}
	if meta.BaseRev != 17 {
		t.Fatalf("meta.BaseRev = %d, want 17", meta.BaseRev)
	}
	if meta.Kind != PendingOverwrite {
		t.Fatalf("meta.Kind = %v, want %v", meta.Kind, PendingOverwrite)
	}
}

func TestWriteBackCache_GetUsesInMemorySmallDataCache(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/small.txt"
	data := []byte("hello")
	if err := cache.Put(path, data, int64(len(data)), PendingNew); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(cache.datFile(path)); err != nil {
		t.Fatal(err)
	}

	got, ok := cache.Get(path)
	if !ok {
		t.Fatal("expected Get to return true from small data cache")
	}
	if string(got) != string(data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
	view, ok := cache.getView(path)
	if !ok {
		t.Fatal("expected getView to return true from small data cache")
	}
	if string(view) != string(data) {
		t.Fatalf("getView = %q, want %q", view, data)
	}
}

func TestWriteBackCache_LargeDataNotCachedInMemory(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/large.bin"
	data := make([]byte, writeBackInMemoryDataThreshold+1)
	if err := cache.Put(path, data, int64(len(data)), PendingNew); err != nil {
		t.Fatal(err)
	}

	cache.mu.Lock()
	_, ok := cache.data[path]
	cache.mu.Unlock()
	if ok {
		t.Fatal("large payload should not be retained in memory cache")
	}
}

func TestWriteBackCache_GetViewPrunesMissingLargeData(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	path := "/large.bin"
	data := make([]byte, writeBackInMemoryDataThreshold+1)
	if err := cache.Put(path, data, int64(len(data)), PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cache.datFile(path)); err != nil {
		t.Fatal(err)
	}

	if _, ok := cache.getView(path); ok {
		t.Fatal("expected getView miss after data file removal")
	}
	if _, ok := cache.GetMeta(path); ok {
		t.Fatal("expected missing data to prune in-memory meta")
	}
	if _, err := os.Stat(cache.metaFile(path)); !os.IsNotExist(err) {
		t.Fatal("expected missing data to prune on-disk meta")
	}
	if paths := cache.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("ListPendingPaths len = %d, want 0", len(paths))
	}
}

func TestWriteBackCache_SmallDataCacheBudgetEvictsLRU(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	cache.dataMaxBytes = 10

	if err := cache.Put("/a.txt", []byte("aaaaa"), 5, PendingNew); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put("/b.txt", []byte("bbbbb"), 5, PendingNew); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.getView("/a.txt"); !ok {
		t.Fatal("expected /a.txt to be cached")
	}
	if err := cache.Put("/c.txt", []byte("ccccc"), 5, PendingNew); err != nil {
		t.Fatal(err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.dataBytes > cache.dataMaxBytes {
		t.Fatalf("dataBytes = %d, want <= %d", cache.dataBytes, cache.dataMaxBytes)
	}
	if _, ok := cache.data["/a.txt"]; !ok {
		t.Fatal("expected /a.txt to remain cached as most-recent entry")
	}
	if _, ok := cache.data["/c.txt"]; !ok {
		t.Fatal("expected /c.txt to remain cached as newest entry")
	}
	if _, ok := cache.data["/b.txt"]; ok {
		t.Fatal("expected /b.txt to be evicted by LRU budget")
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

func TestWriteBackCache_ListPending_SkipsConflictEntries(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.Put("/ok.txt", []byte("ok"), 2, PendingNew); err != nil {
		t.Fatal(err)
	}
	if err := cache.PutWithBaseRev("/conflict.txt", []byte("conflict"), 8, PendingConflict, 9); err != nil {
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

func TestNewWriteBackCache_PrunesMismatchedMetaPath(t *testing.T) {
	dir := t.TempDir()

	wrongPath := "/wrong.txt"
	scannedPath := "/actual.txt"
	scannedHash := hashPath(scannedPath)
	meta := WriteBackMeta{
		Path:       wrongPath,
		Size:       4,
		Mtime:      time.Now(),
		CreatedAt:  time.Now(),
		Generation: 3,
		Kind:       PendingNew,
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

	cache, err := NewWriteBackCache(dir)
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
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	hash := hashPath("/scanned.txt")
	meta := WriteBackMeta{
		Path:       "",
		Size:       4,
		Mtime:      time.Now(),
		CreatedAt:  time.Now(),
		Generation: 2,
		Kind:       PendingNew,
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
	if err := cache.Put("/valid.txt", []byte("ok"), 2, PendingNew); err != nil {
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)

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
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/fail.txt", []byte("fail data"), 9, PendingNew)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/mode-fail.txt", []byte("data"), 4, PendingNew, 0, 0o755, true)
	uploader.Submit("/mode-fail.txt")
	uploader.DrainAll()

	if _, ok := cache.Get("/mode-fail.txt"); !ok {
		t.Fatal("cache entry should be retained after chmod failure")
	}
	meta, ok := cache.GetMeta("/mode-fail.txt")
	if !ok {
		t.Fatal("cache metadata should be retained after chmod failure")
	}
	if meta.Kind != PendingChmod {
		t.Fatalf("meta kind = %v, want PendingChmod", meta.Kind)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/mode-retry.txt", []byte("data"), 4, PendingNew, 0, 0o755, true)
	uploader.Submit("/mode-retry.txt")
	uploader.DrainAll()

	meta, ok := cache.GetMeta("/mode-retry.txt")
	if !ok {
		t.Fatal("cache metadata should be retained after first chmod failure")
	}
	if meta.Kind != PendingChmod {
		t.Fatalf("meta kind after first attempt = %v, want PendingChmod", meta.Kind)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/plain.txt", []byte("data"), 4, PendingNew, 0, defaultRegularFileMode, true)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/mode-sync-fail.txt", []byte("data"), 4, PendingNew, 0, 0o755, true)
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
	if meta.Kind != PendingChmod {
		t.Fatalf("meta kind = %v, want PendingChmod", meta.Kind)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRevAndMode("/sync-upload-fail.txt", []byte("data"), 4, PendingNew, 0, 0o755, true)
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
	if meta.Kind != PendingNew {
		t.Fatalf("meta kind = %v, want PendingNew", meta.Kind)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.PutWithBaseRev("/existing.txt", []byte("edit"), 4, PendingOverwrite, 23)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	uploader := NewWriteBackUploader(c, cache, 1)

	var (
		successMeta WriteBackMeta
		successRev  int64
	)
	uploader.OnSuccess = func(meta WriteBackMeta, committedRev int64) {
		successMeta = meta
		successRev = committedRev
	}

	_ = cache.PutWithBaseRev("/existing.txt", data, int64(len(data)), PendingOverwrite, 23)
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
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/legacy-sync.txt", []byte("edit"), 4, PendingOverwrite)
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

func TestMountReadCacheHashIncludesCredential(t *testing.T) {
	base := MountReadCacheHash("https://example.com", "/mnt/data", "/", "api_key", "key-a")
	if base != MountReadCacheHash("https://example.com", "/mnt/data", "/", "api_key", "key-a") {
		t.Fatal("MountReadCacheHash should be deterministic")
	}
	if base == MountReadCacheHash("https://example.com", "/mnt/data", "/", "api_key", "key-b") {
		t.Fatal("different API keys should produce different read-cache namespaces")
	}
	if base == MountReadCacheHash("https://example.com", "/mnt/data", "/", "token", "key-a") {
		t.Fatal("different credential kinds should produce different read-cache namespaces")
	}
	if base == MountReadCacheHash("https://example.com", "/mnt/data", "/subtree", "api_key", "key-a") {
		t.Fatal("different remote roots should produce different read-cache namespaces")
	}
}

func TestTransientOverlayRootUsesCredentialScopedMountInstance(t *testing.T) {
	cacheBase := t.TempDir()
	readCacheHashA := MountReadCacheHash("https://example.com", "/mnt/data", "/", "api_key", "key-a")
	readCacheHashB := MountReadCacheHash("https://example.com", "/mnt/data", "/", "api_key", "key-b")

	rootA1 := transientOverlayRoot(cacheBase, readCacheHashA)
	rootA2 := transientOverlayRoot(cacheBase, readCacheHashA)
	rootB := transientOverlayRoot(cacheBase, readCacheHashB)

	if rootA1 == rootA2 {
		t.Fatal("transient overlay roots for separate mount instances should differ")
	}
	if !strings.Contains(rootA1, readCacheHashA) {
		t.Fatalf("transient overlay root %q should include credential-scoped read-cache hash %q", rootA1, readCacheHashA)
	}
	if strings.Contains(rootA1, readCacheHashB) || rootA1 == rootB {
		t.Fatal("different credentials should not share transient overlay roots")
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
	c := newTestClient(ts.URL)
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

func TestFlush_CloseSyncUploadsBeforeReturning(t *testing.T) {
	var putCalls atomic.Int32
	var uploaded []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			uploaded = append(uploaded[:0], body...)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(newTestClient(ts.URL), cache, 2)
	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.SetWriteBack(cache, uploader)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "close_sync.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("close-sync data"))
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Errorf("Flush: %v", st)
	}
	if putCalls.Load() != 1 {
		t.Errorf("PUT calls after Flush = %d, want 1", putCalls.Load())
	}
	if string(uploaded) != "close-sync data" {
		t.Errorf("uploaded = %q, want %q", uploaded, "close-sync data")
	}
	if _, ok := cache.Get("/close_sync.txt"); ok {
		t.Error("close-sync Flush should not leave a write-back cache entry")
	}

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	uploader.DrainAll()
	if putCalls.Load() != 1 {
		t.Errorf("PUT calls after Release = %d, want still 1", putCalls.Load())
	}
}

func TestWriteSyncUploadsBeforeWriteReturns(t *testing.T) {
	var putCalls atomic.Int32
	var uploaded []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			uploaded = append(uploaded[:0], body...)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyWriteSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "write_sync.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("write-sync data"))
	if st != gofuse.OK {
		t.Errorf("Write: %v", st)
	}
	if written != uint32(len("write-sync data")) {
		t.Errorf("written = %d, want %d", written, len("write-sync data"))
	}
	if putCalls.Load() != 1 {
		t.Errorf("PUT calls after Write = %d, want 1", putCalls.Load())
	}
	if string(uploaded) != "write-sync data" {
		t.Errorf("uploaded = %q, want %q", uploaded, "write-sync data")
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Errorf("Flush: %v", st)
	}
	if putCalls.Load() != 1 {
		t.Errorf("PUT calls after clean Flush = %d, want still 1", putCalls.Load())
	}
}

func TestWriteSyncFailureRollsBackDirtyState(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyWriteSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "write_sync_fail.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("failed data"))
	if st == gofuse.OK {
		t.Fatal("Write status = OK, want remote failure")
	}
	if written != 0 {
		t.Errorf("written = %d, want 0 on write-sync failure", written)
	}

	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle not found")
	}
	fh.Lock()
	size := int64(0)
	if fh.Dirty != nil {
		size = fh.Dirty.Size()
	}
	fh.Unlock()
	if size != 0 {
		t.Errorf("dirty buffer size after failed write-sync = %d, want 0", size)
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Errorf("Flush after failed write-sync: %v", st)
	}
	if putCalls.Load() != 1 {
		t.Errorf("PUT calls after failed Write + Flush = %d, want 1", putCalls.Load())
	}
}

func TestFlushCloseSyncRemoteFailureKeepsDirtyForRetry(t *testing.T) {
	var putCalls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			if fail.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":2}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "close_sync_retry.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("retry data")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st == gofuse.OK {
		t.Fatal("first Flush status = OK, want remote failure")
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle not found")
	}
	fh.Lock()
	dirty := fh.Dirty != nil && fh.Dirty.HasDirtyParts()
	fh.Unlock()
	if !dirty {
		t.Fatal("failed close-sync Flush cleared dirty data; want retryable dirty state")
	}

	fail.Store(false)
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("second Flush: %v", st)
	}
	if putCalls.Load() != 2 {
		t.Errorf("PUT calls = %d, want 2", putCalls.Load())
	}
}

func TestOSyncOpenPromotesWriteBackToWriteSync(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_SYNC),
	}, "osync.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle not found")
	}
	if fh.WritePolicy != WritePolicyWriteSync {
		t.Errorf("handle write policy = %v, want write-sync", fh.WritePolicy)
	}
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("osync data"))
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if putCalls.Load() != 1 {
		t.Errorf("PUT calls after O_SYNC Write = %d, want 1", putCalls.Load())
	}
}

func TestSyncOpenFlagsPromoteCloseSyncToWriteSync(t *testing.T) {
	opts := &MountOptions{WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	if got := fs.writePolicyForOpen(uint32(syscall.O_SYNC)); got != WritePolicyWriteSync {
		t.Errorf("O_SYNC policy = %v, want write-sync", got)
	}
}

func TestWriteSyncDisablesStreamingUploaderForLargeSequentialWrites(t *testing.T) {
	rec := newWriteSyncMultipartRecorder(t, "/stream_sync.bin")
	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyWriteSync}
	opts.setDefaults()
	fs := NewDat9FS(rec.client(), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "stream_sync.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle not found")
	}
	if fh.Streamer != nil {
		t.Fatal("write-sync handle should not attach StreamUploader")
	}

	chunk := bytesOf('a', int(s3client.PartSize))
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, chunk)
	if st != gofuse.OK {
		t.Fatalf("first Write: %v", st)
	}
	if written != uint32(len(chunk)) {
		t.Fatalf("first written = %d, want %d", written, len(chunk))
	}
	chunk = bytesOf('b', int(s3client.PartSize))
	written, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   uint64(s3client.PartSize),
	}, chunk)
	if st != gofuse.OK {
		t.Fatalf("second Write: %v", st)
	}
	if written != uint32(len(chunk)) {
		t.Fatalf("second written = %d, want %d", written, len(chunk))
	}

	if got := rec.initiateSizes(); len(got) != 2 || got[0] != int64(s3client.PartSize) || got[1] != 2*int64(s3client.PartSize) {
		t.Fatalf("initiate sizes = %v, want [%d %d]", got, s3client.PartSize, 2*s3client.PartSize)
	}
	if got := rec.completePartCounts(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("complete part counts = %v, want [1 2]", got)
	}
}

func TestWriteSyncCreateWithShadowStoreUploadsWrittenBytes(t *testing.T) {
	rec := newWriteSyncMultipartRecorder(t, "/shadow_sync.bin")
	rec.captureParts = true

	opts := &MountOptions{FlushDebounce: 0, WritePolicy: WritePolicyWriteSync}
	opts.setDefaults()
	fs := NewDat9FS(rec.client(), opts)

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "shadow_sync.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle not found")
	}
	if fh.ShadowSpill {
		t.Fatal("write-sync create should not enable ShadowSpill")
	}

	data := bytesOf('x', int(s3client.PartSize))
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, data)
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if written != uint32(len(data)) {
		t.Fatalf("written = %d, want %d", written, len(data))
	}

	bodies := rec.capturedPartBodies()
	if len(bodies) != 1 {
		t.Fatalf("captured S3 part bodies = %d, want 1", len(bodies))
	}
	if len(bodies[0]) != len(data) {
		t.Fatalf("captured body len = %d, want %d", len(bodies[0]), len(data))
	}
	for i, b := range bodies[0] {
		if b != 'x' {
			t.Fatalf("captured body byte %d = %q, want 'x'", i, b)
		}
	}
}

type writeSyncMultipartRecorder struct {
	t            *testing.T
	server       *httptest.Server
	wantPath     string
	mu           sync.Mutex
	initiateByID map[string]int64
	sizes        []int64
	completes    []int
	captureParts bool
	partBodies   [][]byte
}

func newWriteSyncMultipartRecorder(t *testing.T, wantPath string) *writeSyncMultipartRecorder {
	t.Helper()
	rec := &writeSyncMultipartRecorder{
		t:            t,
		wantPath:     wantPath,
		initiateByID: make(map[string]int64),
	}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+rec.wantPath && r.URL.Query().Has("chmod"):
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != rec.wantPath {
				t.Fatalf("initiate path = %q, want %q", req.Path, rec.wantPath)
			}
			rec.mu.Lock()
			id := "upload-" + strconv.Itoa(len(rec.sizes)+1)
			rec.sizes = append(rec.sizes, req.TotalSize)
			rec.initiateByID[id] = req.TotalSize
			rec.mu.Unlock()
			totalParts := int((req.TotalSize + int64(s3client.PartSize) - 1) / int64(s3client.PartSize))
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   id,
				"key":         "object-key",
				"part_size":   int64(s3client.PartSize),
				"total_parts": totalParts,
			})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign-batch")
			rec.mu.Lock()
			size := rec.initiateByID[id]
			rec.mu.Unlock()
			totalParts := int((size + int64(s3client.PartSize) - 1) / int64(s3client.PartSize))
			parts := make([]map[string]any, 0, totalParts)
			for pn := 1; pn <= totalParts; pn++ {
				partSize := int64(s3client.PartSize)
				if pn == totalParts && size%int64(s3client.PartSize) != 0 {
					partSize = size % int64(s3client.PartSize)
				}
				parts = append(parts, map[string]any{
					"number": pn,
					"url":    rec.server.URL + "/s3/" + id + "/" + strconv.Itoa(pn),
					"size":   partSize,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": parts})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign")
			var req struct {
				PartNumber int `json:"part_number"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode presign request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": req.PartNumber,
				"url":    rec.server.URL + "/s3/" + id + "/" + strconv.Itoa(req.PartNumber),
				"size":   int64(s3client.PartSize),
			})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/"):
			if rec.captureParts {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("read s3 body: %v", err)
				}
				rec.mu.Lock()
				rec.partBodies = append(rec.partBodies, body)
				rec.mu.Unlock()
			} else {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Fatalf("read s3 body: %v", err)
				}
			}
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/complete"):
			var req struct {
				Parts []struct {
					Number int    `json:"number"`
					ETag   string `json:"etag"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode complete request: %v", err)
			}
			rec.mu.Lock()
			rec.completes = append(rec.completes, len(req.Parts))
			rec.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (rec *writeSyncMultipartRecorder) client() *client.Client {
	return newTestClient(rec.server.URL)
}

func (rec *writeSyncMultipartRecorder) initiateSizes() []int64 {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]int64(nil), rec.sizes...)
}

func (rec *writeSyncMultipartRecorder) completePartCounts() []int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]int(nil), rec.completes...)
}

func (rec *writeSyncMultipartRecorder) capturedPartBodies() [][]byte {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := make([][]byte, len(rec.partBodies))
	for i, body := range rec.partBodies {
		out[i] = append([]byte(nil), body...)
	}
	return out
}

func bytesOf(b byte, n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = b
	}
	return data
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
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
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

func TestRenameInvalidatesDestinationReadCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/config.lock", false, 54, time.Now())
	fs.inodes.Lookup("/config", false, 36, time.Now())
	fs.readCache.Put("/config.lock", []byte("new config"), 1)
	fs.readCache.Put("/config", []byte("old config"), 1)

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "config.lock", "config")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	if _, ok := fs.readCache.Get("/config.lock", 0); ok {
		t.Fatal("source read cache should be invalidated after rename")
	}
	if _, ok := fs.readCache.Get("/config", 0); ok {
		t.Fatal("destination read cache should be invalidated after overwrite rename")
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
	c := newTestClient(ts.URL)
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
	c := newTestClient(ts.URL)
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
	var gotExpected atomic.Int64
	gotExpected.Store(-1)
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
			if header := r.Header.Get("X-Dat9-Expected-Revision"); header != "" {
				if rev, err := strconv.ParseInt(header, 10, 64); err == nil {
					gotExpected.Store(rev)
				}
			}
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": 1})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
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
	if gotExpected.Load() != 0 {
		t.Fatalf("Fsync expected revision = %d, want 0", gotExpected.Load())
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.BaseRev != 1 {
		t.Fatalf("handle base revision after Fsync = %d, want 1", fh.BaseRev)
	}
	if fh.IsNew {
		t.Fatal("handle should no longer be new after Fsync upload")
	}

	uploader.DrainAll()
}

func TestFsyncInteractiveStageEnqueuesBeforeReleasingPath(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "remote should not be used before queued fsync commit is processed", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncInteractive}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = &CommitQueue{
		maxPending:   4,
		shadows:      shadow,
		index:        pending,
		inFlight:     make(map[string]*CommitEntry),
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		workCh:       make(chan *CommitEntry, 8),
	}

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
	}, "fsync-stage.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("locally durable bytes")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}

	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle missing")
	}
	fh.Lock()
	retained := fh.RemoteCommitUnlock != nil
	commitReady := fh.ShadowCommitReady
	dirtySeq := fh.DirtySeq
	fh.Unlock()
	if retained {
		t.Fatal("Fsync must not retain a same-path remote commit lock until Release")
	}
	if commitReady {
		t.Fatal("Fsync should enqueue the staged commit immediately instead of deferring it until Release")
	}
	if dirtySeq != 0 {
		t.Fatalf("DirtySeq after queued Fsync = %d, want 0", dirtySeq)
	}
	if !fs.commitQueue.HasPath("/fsync-stage.bin") {
		t.Fatal("Fsync returned before the staged commit was visible in CommitQueue")
	}

	done := make(chan struct{})
	go func() {
		unlock := fs.lockWritableRemoteCommitPath("/fsync-stage.bin")
		unlock()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("same-path writer was not blocked by the queued Fsync commit")
	case <-time.After(150 * time.Millisecond):
	}

	fs.commitQueue.CancelPath("/fsync-stage.bin")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("same-path writer did not unblock after queued Fsync commit was removed")
	}
}

// TestFsyncInteractiveWALFrameDurableBeforeEnqueue pins the crash-recovery
// ordering invariant: the JournalFsync frame must be durable BEFORE the staged
// commit becomes visible to the commit queue. If the order is reversed, a fast
// worker can upload and append its JournalCommit marker first, the fsync frame
// then gets a higher Seq, and replay resurrects a path whose upload already
// completed.
//
// The test simulates a slow WAL device by holding the journal mutex: while the
// append cannot complete, the entry must not appear in the queue. Both
// interactive branches (ShadowSpill and regular staged) are covered.
func TestFsyncInteractiveWALFrameDurableBeforeEnqueue(t *testing.T) {
	t.Run("shadowspill", func(t *testing.T) { testFsyncWALFrameDurableBeforeEnqueue(t, true) })
	t.Run("regular", func(t *testing.T) { testFsyncWALFrameDurableBeforeEnqueue(t, false) })
}

func testFsyncWALFrameDurableBeforeEnqueue(t *testing.T, spill bool) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no remote traffic expected", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncInteractive}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewJournal(filepath.Join(t.TempDir(), "order.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.journal = journal
	fs.commitQueue = &CommitQueue{
		maxPending:   4,
		shadows:      shadow,
		index:        pending,
		journal:      journal,
		inFlight:     make(map[string]*CommitEntry),
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		workCh:       make(chan *CommitEntry, 8),
	}

	const remotePath = "/wal-order.bin"
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
	}, "wal-order.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("ordering invariant bytes")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if !spill {
		// Freshly created handles default to ShadowSpill; force the
		// regular staged branch (small dirty buffer materialized to the
		// shadow at fsync time).
		fh, ok := fs.fileHandles.Get(createOut.Fh)
		if !ok {
			t.Fatal("created handle missing")
		}
		fh.Lock()
		fh.ShadowSpill = false
		fh.Unlock()
	}

	// Block the WAL append; Fsync must not hand the commit to the queue
	// until the frame is on disk.
	journal.mu.Lock()
	fsyncDone := make(chan gofuse.Status, 1)
	go func() {
		fsyncDone <- fs.Fsync(nil, &gofuse.FsyncIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		})
	}()
	start := time.Now()
	for time.Since(start) < 300*time.Millisecond {
		if fs.commitQueue.HasPath(remotePath) {
			journal.mu.Unlock()
			t.Fatal("staged commit visible to the queue before its JournalFsync frame was durable")
		}
		select {
		case st := <-fsyncDone:
			journal.mu.Unlock()
			t.Fatalf("Fsync returned %v while the WAL append was blocked", st)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	journal.mu.Unlock()

	if st := <-fsyncDone; st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}
	if !fs.commitQueue.HasPath(remotePath) {
		t.Fatal("staged commit not enqueued after the WAL append completed")
	}
	var fsyncSeq uint64
	if err := journal.Replay(func(e JournalEntry) {
		if e.Op == JournalFsync && e.Path == remotePath {
			fsyncSeq = e.Seq
		}
	}); err != nil {
		t.Fatal(err)
	}
	if fsyncSeq == 0 {
		t.Fatal("JournalFsync frame missing after Fsync returned")
	}

	// The async commit marker (what onCommitSuccess appends) now necessarily
	// gets a higher Seq, so replay must NOT resurrect the committed path.
	mustAppend(t, journal, JournalEntry{Op: JournalCommit, Path: remotePath})
	mustFsync(t, journal)
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(journal, idx); err != nil {
		t.Fatal(err)
	}
	if idx.HasPending(remotePath) {
		t.Fatal("replay resurrected a committed path: commit marker did not supersede the fsync frame")
	}
}

func TestFsyncQueuedCommitSameOpenHandleWritePreservesCommittedBytes(t *testing.T) {
	const filePath = "/fsync-rewrite.bin"
	first := []byte("first")
	second := []byte("-second")
	wantFinal := append(append([]byte(nil), first...), second...)

	type uploadState struct {
		path string
		size int64
		body []byte
	}
	var (
		mu           sync.Mutex
		remote       = make(map[string][]byte)
		uploads      = make(map[string]*uploadState)
		nextUploadID int
		finalWrites  int
	)
	completeEntered := make(chan struct{})
	allowComplete := make(chan struct{})
	var completeOnce sync.Once
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "ok",
				"limits": map[string]any{"inline_threshold": 1024},
			})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode initiate: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.Path != filePath {
				t.Errorf("initiate path = %q, want %q", req.Path, filePath)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			nextUploadID++
			uploadID := fmt.Sprintf("upload-%d", nextUploadID)
			uploads[uploadID] = &uploadState{path: req.Path, size: req.TotalSize}
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   uploadID,
				"key":         "object-key",
				"part_size":   req.TotalSize,
				"total_parts": 1,
			})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign-batch")
			mu.Lock()
			state := uploads[uploadID]
			mu.Unlock()
			if state == nil {
				t.Errorf("presign unknown upload_id %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/" + uploadID + "/1",
					"size":   state.size,
				}},
			})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/"):
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/s3/"), "/")
			if len(parts) != 2 {
				t.Errorf("bad s3 path %q", r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			if uploads[parts[0]] == nil {
				t.Errorf("put unknown upload_id %q", parts[0])
			} else {
				uploads[parts[0]].body = append([]byte(nil), body...)
			}
			mu.Unlock()
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/complete"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/complete")
			mu.Lock()
			state := uploads[uploadID]
			if state == nil || len(state.body) == 0 {
				mu.Unlock()
				t.Errorf("complete before body for %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			completeOnce.Do(func() {
				close(completeEntered)
				mu.Unlock()
				<-allowComplete
				mu.Lock()
			})
			remote[state.path] = append([]byte(nil), state.body...)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
			return
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs"+filePath:
			var req struct {
				NewSize int64 `json:"new_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode patch request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.NewSize != int64(len(wantFinal)) {
				t.Errorf("patch new_size = %d, want %d", req.NewSize, len(wantFinal))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id": "patch-1",
				"part_size": req.NewSize,
				"upload_parts": []map[string]any{{
					"number":   1,
					"url":      ts.URL + "/patch/patch-1/1",
					"size":     req.NewSize,
					"read_url": ts.URL + "/patch-read",
				}},
			})
			return
		case r.Method == http.MethodGet && r.URL.Path == "/patch-read":
			mu.Lock()
			data := append([]byte(nil), remote[filePath]...)
			mu.Unlock()
			_, _ = w.Write(data)
			return
		case r.Method == http.MethodPut && r.URL.Path == "/patch/patch-1/1":
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploads["patch-1"] = &uploadState{path: filePath, size: int64(len(body)), body: append([]byte(nil), body...)}
			mu.Unlock()
			w.Header().Set("ETag", "etag-patch")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-1/complete":
			mu.Lock()
			state := uploads["patch-1"]
			if state == nil || len(state.body) == 0 {
				mu.Unlock()
				t.Errorf("patch complete before body")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote[filePath] = append([]byte(nil), state.body...)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+filePath:
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			finalWrites++
			remote[filePath] = append([]byte(nil), body...)
			revision := finalWrites + 1
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": revision})
			return
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			mu.Lock()
			data := append([]byte(nil), remote[filePath]...)
			mu.Unlock()
			if len(data) == 0 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
				var start, end int
				if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err == nil && start >= 0 && start < len(data) {
					if end >= len(data) {
						end = len(data) - 1
					}
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[start : end+1])
					return
				}
			}
			_, _ = w.Write(data)
			return
		case r.Method == http.MethodGet && r.URL.RawQuery == "list=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
			return
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncInteractive}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	queue := NewCommitQueue(c, shadow, pending, nil, 1, 8, fs.remoteRoot())
	queue.PathLock = fs.lockRemoteCommitPath
	queue.OnSuccess = fs.onCommitQueueSuccess
	queue.OnCleanup = fs.onCommitQueueCleanup
	defer queue.DrainAll()
	fs.commitQueue = queue

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     defaultRegularFileMode,
	}, strings.TrimPrefix(filePath, "/"), &createOut); st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, first); st != gofuse.OK {
		t.Fatalf("first Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}

	select {
	case <-completeEntered:
	case <-time.After(time.Second):
		t.Fatal("queued fsync commit did not reach upload complete")
	}

	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   uint64(len(first)),
		}, second)
		writeDone <- st
	}()
	select {
	case st := <-writeDone:
		t.Fatalf("second Write completed while queued fsync commit was in-flight: %v", st)
	case <-time.After(150 * time.Millisecond):
	}

	close(allowComplete)
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("second Write: %v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("second Write did not finish after queued fsync commit completed")
	}
	queue.WaitPath(filePath)
	mu.Lock()
	gotAfterFsync := append([]byte(nil), remote[filePath]...)
	mu.Unlock()
	if !bytes.Equal(gotAfterFsync, first) {
		t.Fatalf("queued fsync body = %q, want %q", gotAfterFsync, first)
	}

	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle missing")
	}
	fh.Lock()
	shadowReady := fh.ShadowReady
	shadowSpill := fh.ShadowSpill
	isNew := fh.IsNew
	fh.Unlock()
	if shadowReady || shadowSpill || isNew {
		t.Fatalf("handle after queued commit = shadow_ready:%t shadow_spill:%t is_new:%t, want all false", shadowReady, shadowSpill, isNew)
	}

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	mu.Lock()
	gotFinal := append([]byte(nil), remote[filePath]...)
	mu.Unlock()
	if !bytes.Equal(gotFinal, wantFinal) {
		t.Fatalf("final remote body = %q, want %q", gotFinal, wantFinal)
	}
}

func TestClearRemovedCommittedShadowKeepsCurrentWriteLock(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	const filePath = "/same.bin"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	fh := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Dirty:       fs.newWriteBuffer(filePath, 0, 0),
		ShadowReady: true,
		ShadowSpill: true,
	}

	unlockCurrentWrite := fs.lockHandleRemoteCommitPathLocked(fh)
	if !fs.clearRemovedCommittedShadowLocked(fh, 0, 0, false) {
		t.Fatal("clearRemovedCommittedShadowLocked returned false")
	}

	done := make(chan struct{})
	go func() {
		unlock := fs.lockWritableRemoteCommitPath(filePath)
		unlock()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("stale-shadow cleanup released the current write path lock")
	case <-time.After(150 * time.Millisecond):
	}

	unlockCurrentWrite()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("same-path waiter did not unblock after current write released the lock")
	}
}

func TestFsyncShadowSpillCommitRefreshesRevisionAndClearsPending(t *testing.T) {
	const filePath = "/workload.db-journal"
	var (
		mu                sync.Mutex
		revision          int64
		expectedRevisions []int64
		uploadSizes       = make(map[string]int64)
		uploadBodies      = make(map[string][]byte)
	)
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate" {
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode initiate request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.Path != filePath {
				t.Errorf("initiate path = %q, want %q", req.Path, filePath)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.TotalSize <= 0 {
				t.Errorf("initiate total_size = %d, want > 0", req.TotalSize)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.ExpectedRevision == nil {
				t.Error("initiate expected_revision missing")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			expectedRevisions = append(expectedRevisions, *req.ExpectedRevision)
			if *req.ExpectedRevision != revision {
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			uploadID := fmt.Sprintf("upload-%d", len(expectedRevisions))
			uploadSizes[uploadID] = req.TotalSize
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   uploadID,
				"key":         "object-key",
				"part_size":   req.TotalSize,
				"total_parts": 1,
			})
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign-batch") {
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign-batch")
			mu.Lock()
			size := uploadSizes[uploadID]
			mu.Unlock()
			if size <= 0 {
				t.Errorf("presign unknown upload_id %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/" + uploadID + "/1",
					"size":   size,
				}},
			})
			return
		}
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/") {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/s3/"), "/")
			if len(parts) != 2 {
				t.Errorf("bad s3 upload path %q", r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if len(body) == 0 {
				t.Error("multipart part body should not be empty")
			}
			mu.Lock()
			uploadBodies[parts[0]] = body
			mu.Unlock()
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/complete") {
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/complete")
			mu.Lock()
			defer mu.Unlock()
			if len(uploadBodies[uploadID]) == 0 {
				t.Errorf("complete before uploading body for %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			revision++
			w.WriteHeader(http.StatusOK)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			if len(body) == 0 {
				t.Error("direct PUT body should not be empty")
			}
			got, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				t.Errorf("expected revision header: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			expectedRevisions = append(expectedRevisions, got)
			if got != revision {
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			revision++
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": revision})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.syncMode = SyncStrict
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, strings.TrimPrefix(filePath, "/"), &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle missing")
	}
	if !fh.ShadowSpill {
		t.Fatal("created journal handle should use shadow spill")
	}

	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, []byte("first journal frame")); st != gofuse.OK {
		t.Fatalf("first Write: %v", st)
	}
	if !shadow.Has(filePath) {
		t.Fatal("shadow should contain staged journal before fsync")
	}

	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("first Fsync: %v", st)
	}
	if fh.IsNew {
		t.Fatal("handle should not remain create-if-absent after first fsync")
	}
	if fh.BaseRev != 1 {
		t.Fatalf("BaseRev after first fsync = %d, want 1", fh.BaseRev)
	}
	if pending.HasPending(filePath) {
		t.Fatal("pending index should be cleared after remote-durable fsync")
	}
	if shadow.Has(filePath) {
		t.Fatal("shadow should be cleared after remote-durable fsync")
	}
	if cached, ok := fs.readCache.Get(filePath, 1); !ok || string(cached) != "first journal frame" {
		t.Fatalf("read cache after first fsync = %q, ok=%t; want first journal frame", cached, ok)
	}

	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, []byte("second journal frame")); st != gofuse.OK {
		t.Fatalf("second Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("second Fsync: %v", st)
	}
	if fh.BaseRev != 2 {
		t.Fatalf("BaseRev after second fsync = %d, want 2", fh.BaseRev)
	}
	if cached, ok := fs.readCache.Get(filePath, 2); !ok || string(cached) != "second journal frame" {
		t.Fatalf("read cache after second fsync = %q, ok=%t; want second journal frame", cached, ok)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(expectedRevisions) != 2 || expectedRevisions[0] != 0 || expectedRevisions[1] != 1 {
		t.Fatalf("expected revisions = %v, want [0 1]", expectedRevisions)
	}
}

func TestReleaseShadowSpillFallbackAppliesPendingMode(t *testing.T) {
	const filePath = "/shadowspill-mode.bin"
	rec := newShadowSpillFallbackRecorder(t, filePath, http.StatusOK)

	fs, fhID, ino, shadow, pending := newShadowSpillFallbackFS(t, rec.client(), filePath)

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	})

	if rec.uploads() != 1 {
		t.Fatalf("uploads = %d, want 1", rec.uploads())
	}
	if got := rec.chmodModes(); len(got) != 1 || got[0] != "755" {
		t.Fatalf("chmod modes = %v, want [755]", got)
	}
	if pending.HasPending(filePath) {
		t.Fatal("pending entry should be removed after fallback upload and chmod")
	}
	if shadow.Has(filePath) {
		t.Fatal("shadow should be removed after fallback upload and chmod")
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if entry.Mode&0o777 != 0o755 {
		t.Fatalf("inode mode = %o, want 755", entry.Mode&0o777)
	}
}

func TestReleaseShadowSpillFallbackPreservesPendingModeOnChmodFailure(t *testing.T) {
	const filePath = "/shadowspill-mode-fail.bin"
	rec := newShadowSpillFallbackRecorder(t, filePath, http.StatusInternalServerError)

	fs, fhID, ino, shadow, pending := newShadowSpillFallbackFS(t, rec.client(), filePath)

	fsHandle, ok := fs.fileHandles.Get(fhID)
	if !ok {
		t.Fatal("created handle missing")
	}
	fsHandle.Lock()
	initialModeGen := fsHandle.PendingModeGen
	fsHandle.Unlock()

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	})

	if rec.uploads() != 1 {
		t.Fatalf("uploads = %d, want 1", rec.uploads())
	}
	if got := rec.chmodModes(); len(got) != 1 || got[0] != "755" {
		t.Fatalf("chmod modes = %v, want [755]", got)
	}
	if !pending.HasPending(filePath) {
		t.Fatal("pending entry should remain when fallback chmod fails")
	}
	meta, ok := pending.GetMeta(filePath)
	if !ok {
		t.Fatal("pending metadata missing after chmod failure")
	}
	if !meta.HasMode || meta.Mode&0o777 != 0o755 {
		t.Fatalf("pending mode = has:%t mode:%o, want has:true mode:755", meta.HasMode, meta.Mode&0o777)
	}
	if !shadow.Has(filePath) {
		t.Fatal("shadow should remain when fallback chmod fails")
	}
	if initialModeGen == 0 {
		t.Fatal("test setup expected pending mode generation")
	}
}

type shadowSpillFallbackRecorder struct {
	t           *testing.T
	server      *httptest.Server
	wantPath    string
	chmodStatus int
	mu          sync.Mutex
	uploadSizes map[string]int64
	uploadCount int
	modes       []string
}

func newShadowSpillFallbackRecorder(t *testing.T, wantPath string, chmodStatus int) *shadowSpillFallbackRecorder {
	t.Helper()
	rec := &shadowSpillFallbackRecorder{
		t:           t,
		wantPath:    wantPath,
		chmodStatus: chmodStatus,
		uploadSizes: make(map[string]int64),
	}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+rec.wantPath && r.URL.Query().Has("chmod"):
			var req struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode chmod: %v", err)
			}
			rec.mu.Lock()
			rec.modes = append(rec.modes, strconv.FormatUint(uint64(req.Mode&0o777), 8))
			status := rec.chmodStatus
			rec.mu.Unlock()
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			return
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+rec.wantPath:
			gotExpected, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				t.Fatalf("expected revision header: %v", err)
			}
			if gotExpected != 0 {
				t.Fatalf("expected_revision = %d, want 0", gotExpected)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read direct PUT body: %v", err)
			}
			if len(body) == 0 {
				t.Fatal("direct PUT body should not be empty")
			}
			rec.mu.Lock()
			rec.uploadCount++
			rec.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate: %v", err)
			}
			if req.Path != rec.wantPath {
				t.Fatalf("initiate path = %q, want %q", req.Path, rec.wantPath)
			}
			if req.ExpectedRevision == nil || *req.ExpectedRevision != 0 {
				t.Fatalf("expected_revision = %v, want 0", req.ExpectedRevision)
			}
			uploadID := "upload-1"
			rec.mu.Lock()
			rec.uploadSizes[uploadID] = req.TotalSize
			rec.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   uploadID,
				"key":         "object-key",
				"part_size":   req.TotalSize,
				"total_parts": 1,
			})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign-batch")
			rec.mu.Lock()
			size := rec.uploadSizes[uploadID]
			rec.mu.Unlock()
			if size <= 0 {
				t.Fatalf("unknown upload id %q", uploadID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    rec.server.URL + "/s3/" + uploadID + "/1",
					"size":   size,
				}},
			})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read part body: %v", err)
			}
			if len(body) == 0 {
				t.Fatal("multipart part body should not be empty")
			}
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/complete"):
			rec.mu.Lock()
			rec.uploadCount++
			rec.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (rec *shadowSpillFallbackRecorder) client() *client.Client {
	return newTestClient(rec.server.URL)
}

func (rec *shadowSpillFallbackRecorder) uploads() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.uploadCount
}

func (rec *shadowSpillFallbackRecorder) chmodModes() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.modes...)
}

func newShadowSpillFallbackFS(t *testing.T, c *client.Client, filePath string) (*Dat9FS, uint64, uint64, *ShadowStore, *PendingIndex) {
	t.Helper()
	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncInteractive}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	queue := NewCommitQueue(c, shadow, pending, nil, 1, 1, fs.remoteRoot())
	queue.DrainAll()
	fs.commitQueue = queue

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o755,
	}, strings.TrimPrefix(filePath, "/"), &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, bytes.Repeat([]byte("x"), 128)); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("Fsync: %v", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle missing")
	}
	fh.Lock()
	commitReady := fh.ShadowCommitReady
	hasMode := fh.HasPendingMode
	mode := fh.PendingMode & 0o777
	fh.Unlock()
	if !commitReady {
		t.Fatal("test setup expected ShadowSpill commit-ready handle")
	}
	if !hasMode || mode != 0o755 {
		t.Fatalf("test setup pending mode = has:%t mode:%o, want has:true mode:755", hasMode, mode)
	}
	return fs, createOut.Fh, createOut.NodeId, shadow, pending
}

func TestRefreshCommittedRevisionSkipsLockedSiblingHandle(t *testing.T) {
	fs := &Dat9FS{openHandles: NewOpenHandleIndex(), inodes: NewInodeToPath()}
	current := &FileHandle{Ino: 1, Path: "/same.db", Dirty: NewWriteBuffer("/same.db", 1024, 1024), BaseRev: 1}
	sibling := &FileHandle{Ino: 2, Path: "/same.db", Dirty: NewWriteBuffer("/same.db", 1024, 1024), BaseRev: 1}
	fs.openHandles.Add(current)
	fs.openHandles.Add(sibling)

	sibling.Lock()
	defer sibling.Unlock()

	done := make(chan struct{})
	go func() {
		current.Lock()
		defer current.Unlock()
		fs.markHandleRemoteCommittedLocked(current, 2)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("markHandleRemoteCommittedLocked blocked on a locked sibling handle")
	}
	if current.BaseRev != 2 {
		t.Fatalf("current BaseRev = %d, want 2", current.BaseRev)
	}
	if sibling.BaseRev != 1 {
		t.Fatalf("locked sibling BaseRev = %d, want unchanged 1", sibling.BaseRev)
	}
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
	c := newTestClient(ts.URL)
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

// TestWriteBackCache_ConcurrentPutSamePath verifies that concurrent Puts to
// the same path under the per-path lock produce a consistent final state.
func TestWriteBackCache_ConcurrentPutSamePath(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("g%d-i%d", g, i))
				_ = cache.Put("/concurrent.txt", data, int64(len(data)), PendingNew)
			}
		}()
	}
	wg.Wait()

	// After all Puts, exactly one entry should exist with valid data.
	meta, ok := cache.GetMeta("/concurrent.txt")
	if !ok {
		t.Fatal("meta should exist after concurrent Puts")
	}
	data, ok := cache.Get("/concurrent.txt")
	if !ok {
		t.Fatal("data should exist after concurrent Puts")
	}
	if int64(len(data)) != meta.Size {
		t.Fatalf("data len %d != meta size %d", len(data), meta.Size)
	}
}

// TestWriteBackCache_ConcurrentPutAndRemove verifies that concurrent Put and
// Remove on the same path don't corrupt state or leave orphan files.
func TestWriteBackCache_ConcurrentPutAndRemove(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 4
	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("put-g%d-i%d", g, i))
				_ = cache.Put("/put-rm.txt", data, int64(len(data)), PendingNew)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cache.Remove("/put-rm.txt")
			}
		}()
	}
	wg.Wait()

	// After the storm, state must be consistent: either present with valid
	// meta+data, or fully absent.
	meta, metaOK := cache.GetMeta("/put-rm.txt")
	data, dataOK := cache.Get("/put-rm.txt")
	if metaOK != dataOK {
		t.Fatalf("inconsistent: metaOK=%v dataOK=%v", metaOK, dataOK)
	}
	if metaOK && int64(len(data)) != meta.Size {
		t.Fatalf("data len %d != meta size %d", len(data), meta.Size)
	}
}

// TestWriteBackCache_ConcurrentPutAndRename verifies that concurrent Put and
// RenamePending on the same path don't produce orphan files or inconsistent state.
func TestWriteBackCache_ConcurrentPutAndRename(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Seed initial entry so RenamePending has something to work with.
	_ = cache.Put("/rename-src.txt", []byte("seed"), 4, PendingNew)

	const goroutines = 4
	const iterations = 15
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("data-%d", i))
				_ = cache.Put("/rename-src.txt", data, int64(len(data)), PendingNew)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cache.RenamePending("/rename-src.txt", "/rename-dst.txt")
				// Rename back so the next iteration has a source.
				cache.RenamePending("/rename-dst.txt", "/rename-src.txt")
			}
		}()
	}
	wg.Wait()

	// After the storm, one of src or dst should hold valid data (or both absent).
	srcMeta, srcOK := cache.GetMeta("/rename-src.txt")
	dstMeta, dstOK := cache.GetMeta("/rename-dst.txt")
	if srcOK {
		data, ok := cache.Get("/rename-src.txt")
		if !ok {
			t.Fatal("src meta present but data absent")
		}
		if int64(len(data)) != srcMeta.Size {
			t.Fatalf("src data len %d != meta size %d", len(data), srcMeta.Size)
		}
	}
	if dstOK {
		data, ok := cache.Get("/rename-dst.txt")
		if !ok {
			t.Fatal("dst meta present but data absent")
		}
		if int64(len(data)) != dstMeta.Size {
			t.Fatalf("dst data len %d != meta size %d", len(data), dstMeta.Size)
		}
	}
}

// TestWriteBackCache_RemoveIfGeneration verifies the atomic generation-checked remove.
func TestWriteBackCache_RemoveIfGeneration(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/gen-rm.txt", []byte("v1"), 2, PendingNew)
	meta1, _ := cache.GetMeta("/gen-rm.txt")
	gen1 := meta1.Generation

	// Overwrite with v2 → new generation.
	_ = cache.Put("/gen-rm.txt", []byte("v2"), 2, PendingNew)
	meta2, _ := cache.GetMeta("/gen-rm.txt")
	gen2 := meta2.Generation

	if gen2 <= gen1 {
		t.Fatalf("gen2 %d should be > gen1 %d", gen2, gen1)
	}

	// RemoveIfGeneration with stale gen should NOT remove.
	if cache.RemoveIfGeneration("/gen-rm.txt", gen1) {
		t.Fatal("RemoveIfGeneration should return false for stale generation")
	}
	if _, ok := cache.GetMeta("/gen-rm.txt"); !ok {
		t.Fatal("entry should still exist after stale RemoveIfGeneration")
	}

	// RemoveIfGeneration with current gen should remove.
	if !cache.RemoveIfGeneration("/gen-rm.txt", gen2) {
		t.Fatal("RemoveIfGeneration should return true for matching generation")
	}
	if _, ok := cache.GetMeta("/gen-rm.txt"); ok {
		t.Fatal("entry should be removed after matching RemoveIfGeneration")
	}

	// RemoveIfGeneration on nonexistent path should return false.
	if cache.RemoveIfGeneration("/nonexistent.txt", 999) {
		t.Fatal("RemoveIfGeneration should return false for nonexistent path")
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
	c := newTestClient(ts.URL)

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
	if out.Size != 10 {
		t.Fatalf("Lookup size = %d, want 10", out.Size)
	}
}

// TestOpenWritable_PreloadsFromWriteBackCache verifies that opening a file
// for writing preloads data from the write-back cache instead of the remote.
func TestOpenWritable_PreloadsFromWriteBackCache(t *testing.T) {
	var headCalls atomic.Int32
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			// Return stale data size from "server"
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			// Return stale server data
			_, _ = w.Write([]byte("stale"))
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
	c := newTestClient(ts.URL)
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
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0", got)
	}

	uploader.DrainAll()
}

func TestOpenWritable_OverwriteFromWriteBackCacheSkipsRemoteStat(t *testing.T) {
	var headCalls atomic.Int32
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			_, _ = w.Write([]byte("stale"))
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
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 0)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.SetWriteBack(cache, uploader)

	if err := cache.PutWithBaseRev("/file.txt", []byte("fresh data"), 10, PendingOverwrite, 11); err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup("/file.txt", false, 10, time.Time{})

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(os.O_RDWR),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	fh, ok := fs.fileHandles.Get(openOut.Fh)
	if !ok {
		t.Fatal("expected file handle to exist")
	}
	if fh.BaseRev != 11 {
		t.Fatalf("BaseRev = %d, want 11", fh.BaseRev)
	}
	if fh.WriteBackSeq == 0 || fh.WriteBackSeq != fh.DirtySeq {
		t.Fatalf("WriteBackSeq = %d, DirtySeq = %d, want matching non-zero snapshot seq", fh.WriteBackSeq, fh.DirtySeq)
	}

	buf := make([]byte, 32)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}
	got, _ := result.Bytes(buf)
	if string(got) != "fresh data" {
		t.Fatalf("Read = %q, want %q", string(got), "fresh data")
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0", got)
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
	c := newTestClient(ts.URL)
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

func TestWriteBackCache_ReopenPreservesGenerationCounter(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.Put("/gen.txt", []byte("v1"), 2, PendingNew); err != nil {
		t.Fatal(err)
	}
	meta1, ok := cache.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after first Put")
	}

	cache2, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache2.Put("/gen.txt", []byte("v2"), 2, PendingNew); err != nil {
		t.Fatal(err)
	}
	meta2, ok := cache2.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after reopened Put")
	}
	if meta2.Generation <= meta1.Generation {
		t.Fatalf("generation did not increase after reopen: %d <= %d", meta2.Generation, meta1.Generation)
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
	c := newTestClient(ts.URL)
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

func TestWriteBackCacheListByPrefix(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/dir/a.txt", []byte("a"), 1, PendingNew)
	_ = cache.Put("/dir/sub/b.txt", []byte("b"), 1, PendingNew)
	_ = cache.Put("/dir-other/c.txt", []byte("c"), 1, PendingNew)

	entries := cache.ListByPrefix("/dir/")
	if len(entries) != 2 {
		t.Fatalf("ListByPrefix len = %d, want 2", len(entries))
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		seen[entry.Path] = struct{}{}
	}
	if _, ok := seen["/dir/a.txt"]; !ok {
		t.Fatal("missing /dir/a.txt")
	}
	if _, ok := seen["/dir/sub/b.txt"]; !ok {
		t.Fatal("missing /dir/sub/b.txt")
	}
	if _, ok := seen["/dir-other/c.txt"]; ok {
		t.Fatal("/dir-other/c.txt should not match /dir/")
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
	c := newTestClient(ts.URL)
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

func TestUnlink_PendingNewCommitQueueUploadedDeletesRemote(t *testing.T) {
	const p = "/config.lock"
	data := []byte("lock")

	putStarted := make(chan struct{})
	allowPut := make(chan struct{})
	var putStartedOnce sync.Once
	var deleteCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putStartedOnce.Do(func() { close(putStarted) })
			<-allowPut
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
		case http.MethodDelete:
			deleteCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(p, data, 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(p, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	fs.inodes.Lookup(p, false, int64(len(data)), time.Now())

	if err := cq.Enqueue(&CommitEntry{
		Path:    p,
		Inode:   2,
		Size:    int64(len(data)),
		Kind:    PendingNew,
		BaseRev: 0,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("commit queue PUT did not start")
	}

	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "config.lock")
	}()

	select {
	case st := <-done:
		t.Fatalf("Unlink returned before in-flight commit finished: %v", st)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowPut)

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Unlink: %v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("Unlink did not finish after commit completed")
	}
	if !deleteCalled.Load() {
		t.Fatal("Unlink should delete a PendingNew file after commitQueue uploaded it")
	}
}

func TestUnlink_PendingNewCommitQueueNotEnqueuedSkipsRemoteDelete(t *testing.T) {
	const p = "/config.lock"

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

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(p, []byte("local-only"), 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(p, int64(len("local-only")), PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	fs.inodes.Lookup(p, false, int64(len("local-only")), time.Now())

	st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "config.lock")
	if st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	if deleteCalled.Load() {
		t.Fatal("Unlink should not delete remote path for PendingNew that was never enqueued")
	}
	if _, ok := pending.GetMeta(p); ok {
		t.Fatal("pending entry should be cleared after unlink")
	}
	if shadow.Has(p) {
		t.Fatal("shadow should be removed after unlink")
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
	c := newTestClient(ts.URL)
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
			_, _ = w.Write(fileData)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	c := newTestClient(ts.URL)

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
	c := newTestClient(ts.URL)
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
	allowUploadResponses := make(chan struct{})
	var allowUploadResponsesOnce sync.Once
	defer allowUploadResponsesOnce.Do(func() { close(allowUploadResponses) })

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
			<-allowUploadResponses
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
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
	allowUploadResponsesOnce.Do(func() { close(allowUploadResponses) })
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
	c := newTestClient(ts.URL)
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
