package fuse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// TestW3GrowthPatchGuard pins the safety predicate: the PATCH route is only
// taken when every part overlapping [OrigSize, size) is dirty and loaded.
func TestW3GrowthPatchGuard(t *testing.T) {
	fs := NewDat9FS(nil, &MountOptions{})
	newBuffer := func() *WriteBuffer {
		return NewWriteBuffer("/db.bin", maxPreloadSize, 100)
	}

	// Non-growth is always safe.
	fh := &FileHandle{Ino: 1, Path: "/db.bin", Dirty: newBuffer(), OrigSize: 300}
	if _, err := fh.Dirty.Write(0, make([]byte, 200)); err != nil {
		t.Fatal(err)
	}
	if !fs.growthPatchSafeLocked(fh, 200) {
		t.Fatal("non-growth must be patch-safe")
	}

	// Contiguous growth: every part in the grown region is dirty and loaded.
	fh = &FileHandle{Ino: 1, Path: "/db.bin", Dirty: newBuffer(), OrigSize: 100}
	if _, err := fh.Dirty.Write(0, make([]byte, 400)); err != nil {
		t.Fatal(err)
	}
	if !fs.growthPatchSafeLocked(fh, 400) {
		t.Fatal("contiguous growth must be patch-safe")
	}

	// Sparse growth: a pwrite beyond EOF leaves gap parts undirtied.
	fh = &FileHandle{Ino: 1, Path: "/db.bin", Dirty: newBuffer(), OrigSize: 100}
	if _, err := fh.Dirty.Write(300, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if fs.growthPatchSafeLocked(fh, 400) {
		t.Fatal("sparse growth with gap parts must not be patch-safe")
	}

	// Evicted dirty part: content is not resident.
	fh = &FileHandle{Ino: 1, Path: "/db.bin", Dirty: newBuffer(), OrigSize: 100}
	if _, err := fh.Dirty.Write(0, make([]byte, 400)); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.EvictPart(1)
	if fs.growthPatchSafeLocked(fh, 400) {
		t.Fatal("growth with an evicted dirty part must not be patch-safe")
	}
}

// TestW3FlushGrowthUsesPatchWhenCoverageComplete covers the reversed B11
// behavior: a grown file whose growth region is fully dirty and loaded takes
// the PATCH route instead of a whole-file upload.
func TestW3FlushGrowthUsesPatchWhenCoverageComplete(t *testing.T) {
	var patchReceived atomic.Bool
	var fullUpload atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patchReceived.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"upload_id":    "test-upload-w3",
				"upload_parts": []interface{}{},
				"copied_parts": []interface{}{},
			})
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/complete") {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			fullUpload.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.smallFileMax.Store(100)

	path := "/w3-growth-patch.dat"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    NewWriteBuffer(path, maxPreloadSize, 100),
		BaseRev:  5,
		OrigSize: 200,
	}
	data := make([]byte, 400)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	if !patchReceived.Load() {
		t.Fatal("growth with complete dirty coverage must use PATCH")
	}
	if fullUpload.Load() {
		t.Fatal("growth with complete dirty coverage must not use a full upload")
	}
}

// TestW3FsyncGrowthReroutesToPatch covers decision A: a strict fsync of a grown
// S3-backed file commits through the patch-capable flush path and discards the
// writeback snapshot instead of full-uploading via the writeback uploader.
func TestW3FsyncGrowthReroutesToPatch(t *testing.T) {
	var patchReceived atomic.Bool
	var fullUpload atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patchReceived.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"upload_id":    "test-upload-w3-fsync",
				"upload_parts": []interface{}{},
				"copied_parts": []interface{}{},
			})
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/complete") {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			fullUpload.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncStrict}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.smallFileMax.Store(100)

	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.SetWriteBack(cache, NewWriteBackUploader(fs.client, cache, 1))

	path := "/w3-fsync-growth.dat"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:          ino,
		Path:         path,
		Dirty:        NewWriteBuffer(path, maxPreloadSize, 100),
		BaseRev:      5,
		OrigSize:     200,
		StorageClass: storageClassS3,
	}
	data := make([]byte, 400)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fh.WriteBackSeq = fh.DirtySeq

	gen, _, err := cache.PutWithBaseRevAndModeTimings(path, data, fh.Dirty.Size(), PendingOverwrite, 5, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	fh.WriteBackGen = gen
	fhID := fs.fileHandles.Allocate(fh)

	st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID})
	if st != gofuse.OK {
		t.Fatalf("Fsync status = %v, want OK", st)
	}
	if !patchReceived.Load() {
		t.Fatal("strict fsync of a grown S3-backed file must commit via PATCH")
	}
	if fullUpload.Load() {
		t.Fatal("strict fsync of a grown S3-backed file must not full-upload via the writeback uploader")
	}
	if _, ok := cache.GetMeta(path); ok {
		t.Fatal("writeback snapshot was not discarded after the patch reroute")
	}
	fh.Lock()
	defer fh.Unlock()
	if fh.DirtySeq != 0 || fh.WriteBackSeq != 0 || fh.Dirty.HasDirtyParts() {
		t.Fatalf("handle not cleaned after patch commit: seq=%d wbSeq=%d dirty=%t", fh.DirtySeq, fh.WriteBackSeq, fh.Dirty.HasDirtyParts())
	}
}

// TestW3PatchPlanRejectedFallsBackToFullUpload covers T3.4: a patch-plan
// initiation failure (not a storage-class rejection) retries the commit as a
// full upload, records the per-handle rejection, and never recurses into
// PATCH again for this handle.
func TestW3PatchPlanRejectedFallsBackToFullUpload(t *testing.T) {
	var patchCount atomic.Int32
	var fullUpload atomic.Bool

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 50, "storage_capabilities": map[string]bool{}})
			return
		case r.Method == http.MethodPatch:
			patchCount.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "part_size 8388608 produces 10001 parts for size 8388688896, exceeds S3 limit"})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			fullUpload.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
			fullUpload.Store(true)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		default:
			t.Logf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.smallFileMax.Store(100)

	path := "/w3-plan-rejected.dat"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:          ino,
		Path:         path,
		Dirty:        NewWriteBuffer(path, maxPreloadSize, 100),
		BaseRev:      5,
		OrigSize:     200,
		StorageClass: storageClassS3,
	}
	data := make([]byte, 400)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	if patchCount.Load() != 1 {
		t.Fatalf("patch attempts = %d, want exactly 1 before fallback", patchCount.Load())
	}
	if !fullUpload.Load() {
		t.Fatal("patch plan rejection must fall back to a full upload")
	}
	if !fh.patchPlanRejected {
		t.Fatal("handle did not record the patch plan rejection")
	}
	if fh.StorageClass != storageClassS3 {
		t.Fatalf("storage class changed to %q on plan rejection", fh.StorageClass)
	}
}
