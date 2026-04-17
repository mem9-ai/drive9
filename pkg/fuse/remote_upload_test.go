package fuse

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

type multipartUploadRecorder struct {
	t                *testing.T
	server           *httptest.Server
	wantPath         string
	wantSize         int64
	wantExpected     *int64
	initiateCalls    atomic.Int32
	presignCalls     atomic.Int32
	completeCalls    atomic.Int32
	s3PutCalls       atomic.Int32
	directFilePuts   atomic.Int32
	mu               sync.Mutex
	gotUploadedBytes int64
}

func newMultipartUploadRecorder(t *testing.T, wantPath string, wantSize int64, wantExpected *int64) *multipartUploadRecorder {
	t.Helper()

	rec := &multipartUploadRecorder{
		t:            t,
		wantPath:     wantPath,
		wantSize:     wantSize,
		wantExpected: wantExpected,
	}

	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			rec.directFilePuts.Add(1)
			w.WriteHeader(http.StatusOK)
			return

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			rec.initiateCalls.Add(1)

			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != rec.wantPath {
				t.Fatalf("initiate path = %q, want %q", req.Path, rec.wantPath)
			}
			if req.TotalSize != rec.wantSize {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, rec.wantSize)
			}
			switch {
			case rec.wantExpected == nil && req.ExpectedRevision != nil:
				t.Fatalf("initiate expected_revision = %v, want nil", *req.ExpectedRevision)
			case rec.wantExpected != nil && req.ExpectedRevision == nil:
				t.Fatal("initiate expected_revision missing")
			case rec.wantExpected != nil && *req.ExpectedRevision != *rec.wantExpected:
				t.Fatalf("initiate expected_revision = %d, want %d", *req.ExpectedRevision, *rec.wantExpected)
			}

			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "upload-1",
				"key":         "object-key",
				"part_size":   int64(s3client.PartSize),
				"total_parts": 1,
			})
			return

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/upload-1/presign-batch":
			rec.presignCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{
					{
						"number": 1,
						"url":    rec.server.URL + "/s3/upload-1/1",
						"size":   rec.wantSize,
					},
				},
			})
			return

		case r.Method == http.MethodPut && r.URL.Path == "/s3/upload-1/1":
			rec.s3PutCalls.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read s3 body: %v", err)
			}
			rec.mu.Lock()
			rec.gotUploadedBytes = int64(len(body))
			rec.mu.Unlock()
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/upload-1/complete":
			rec.completeCalls.Add(1)
			var req struct {
				Parts []struct {
					Number int    `json:"number"`
					ETag   string `json:"etag"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode complete request: %v", err)
			}
			if len(req.Parts) != 1 || req.Parts[0].Number != 1 || req.Parts[0].ETag != "etag-1" {
				t.Fatalf("complete parts = %+v, want single part with etag-1", req.Parts)
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
	}))
	t.Cleanup(rec.server.Close)

	return rec
}

func (rec *multipartUploadRecorder) client() *client.Client {
	return client.New(rec.server.URL, "")
}

func TestCommitQueueLargeOverwriteUsesMultipartUpload(t *testing.T) {
	const remotePath = "/large-overwrite.bin"
	data := make([]byte, s3client.PartSize)
	for i := range data {
		data[i] = byte(i)
	}
	expectedRevision := int64(7)
	rec := newMultipartUploadRecorder(t, remotePath, int64(len(data)), &expectedRevision)

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(remotePath, data, expectedRevision); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(remotePath, int64(len(data)), PendingOverwrite, expectedRevision); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(rec.client(), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    remotePath,
		BaseRev: expectedRevision,
		Size:    int64(len(data)),
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if rec.directFilePuts.Load() != 0 {
		t.Fatalf("direct PUT count = %d, want 0", rec.directFilePuts.Load())
	}
	if rec.initiateCalls.Load() != 1 || rec.presignCalls.Load() != 1 || rec.completeCalls.Load() != 1 || rec.s3PutCalls.Load() != 1 {
		t.Fatalf("multipart flow calls = initiate:%d presign:%d complete:%d s3put:%d, want 1 each",
			rec.initiateCalls.Load(), rec.presignCalls.Load(), rec.completeCalls.Load(), rec.s3PutCalls.Load())
	}
	if pending.HasPending(remotePath) {
		t.Fatal("pending entry should be removed after multipart commit")
	}
	if shadow.Has(remotePath) {
		t.Fatal("shadow entry should be removed after multipart commit")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.gotUploadedBytes != int64(len(data)) {
		t.Fatalf("uploaded bytes = %d, want %d", rec.gotUploadedBytes, len(data))
	}
}

func TestWriteBackUploaderLargeNewFileUsesMultipartUpload(t *testing.T) {
	const remotePath = "/large-new.bin"
	data := make([]byte, s3client.PartSize)
	for i := range data {
		data[i] = byte(255 - (i % 251))
	}
	expectedRevision := int64(0)
	rec := newMultipartUploadRecorder(t, remotePath, int64(len(data)), &expectedRevision)

	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(remotePath, data, int64(len(data)), PendingNew); err != nil {
		t.Fatal(err)
	}

	uploader := NewWriteBackUploader(rec.client(), cache, 1)
	uploader.Submit(remotePath)
	uploader.DrainAll()

	if rec.directFilePuts.Load() != 0 {
		t.Fatalf("direct PUT count = %d, want 0", rec.directFilePuts.Load())
	}
	if rec.initiateCalls.Load() != 1 || rec.presignCalls.Load() != 1 || rec.completeCalls.Load() != 1 || rec.s3PutCalls.Load() != 1 {
		t.Fatalf("multipart flow calls = initiate:%d presign:%d complete:%d s3put:%d, want 1 each",
			rec.initiateCalls.Load(), rec.presignCalls.Load(), rec.completeCalls.Load(), rec.s3PutCalls.Load())
	}
	if _, ok := cache.Get(remotePath); ok {
		t.Fatal("cache entry should be removed after multipart upload")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.gotUploadedBytes != int64(len(data)) {
		t.Fatalf("uploaded bytes = %d, want %d", rec.gotUploadedBytes, len(data))
	}
}
