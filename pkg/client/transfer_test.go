package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	srvpkg "github.com/mem9-ai/dat9/pkg/server"
)

// TestWriteStreamSmallFile verifies that WriteStream sends a small file via single direct PUT.
func TestWriteStreamSmallFile(t *testing.T) {
	var writtenData []byte
	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/small.txt" {
			requestCount++
			writtenData, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	data := []byte("hello small")
	err := c.WriteStream(context.Background(), "/small.txt", bytes.NewReader(data), int64(len(data)), nil)
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}
	if requestCount != 1 {
		t.Errorf("expected 1 request, got %d", requestCount)
	}
	if !bytes.Equal(writtenData, data) {
		t.Errorf("got %q, want %q", writtenData, data)
	}
}

// TestWriteStreamLargeFile verifies the 202 + multipart upload flow.
func TestWriteStreamLargeFile(t *testing.T) {
	var mu sync.Mutex
	uploadedParts := map[int][]byte{}
	completeCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			var req uploadInitiateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if req.Path != "/large.bin" || req.TotalSize != 8 {
				http.Error(w, fmt.Sprintf("bad initiate payload: %+v", req), http.StatusBadRequest)
				return
			}
			// Return 202 with upload plan
			plan := UploadPlan{
				UploadID: "upload-123",
				Parts: []PartURL{
					{Number: 1, URL: "", Size: 5}, // URL filled below
					{Number: 2, URL: "", Size: 3},
				},
			}
			// We need the server URL for part URLs
			// Parts will be uploaded to /parts/1, /parts/2
			plan.Parts[0].URL = fmt.Sprintf("http://%s/parts/1", r.Host)
			plan.Parts[1].URL = fmt.Sprintf("http://%s/parts/2", r.Host)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(plan)

		case r.Method == http.MethodPut && r.URL.Path == "/parts/1":
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploadedParts[1] = data
			mu.Unlock()
			w.Header().Set("ETag", `"etag1"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPut && r.URL.Path == "/parts/2":
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploadedParts[2] = data
			mu.Unlock()
			w.Header().Set("ETag", `"etag2"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/upload-123/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1   // force large file path for test
	data := []byte("12345678") // 8 bytes, 2 parts (5+3)

	var progressCalls []int
	progress := func(partNum, total int, bytesUploaded int64) {
		mu.Lock()
		progressCalls = append(progressCalls, partNum)
		mu.Unlock()
	}

	err := c.WriteStream(context.Background(), "/large.bin", bytes.NewReader(data), int64(len(data)), progress)
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}

	if !bytes.Equal(uploadedParts[1], []byte("12345")) {
		t.Errorf("part 1: got %q, want %q", uploadedParts[1], "12345")
	}
	if !bytes.Equal(uploadedParts[2], []byte("678")) {
		t.Errorf("part 2: got %q, want %q", uploadedParts[2], "678")
	}
	if !completeCalled {
		t.Error("complete was not called")
	}
	if len(progressCalls) != 2 {
		t.Errorf("progress called %d times, want 2", len(progressCalls))
	}
}

func TestWriteStreamLargeFileFallsBackToLegacyInitiate(t *testing.T) {
	var usedLegacy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			http.NotFound(w, r)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/legacy.bin":
			usedLegacy.Store(true)
			if h := r.Header.Get("X-Dat9-Content-Length"); h != "8" {
				http.Error(w, "bad length", http.StatusBadRequest)
				return
			}
			plan := UploadPlan{UploadID: "legacy-upload", Parts: []PartURL{{Number: 1, URL: fmt.Sprintf("http://%s/legacy/part/1", r.Host), Size: 8}}}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(plan)
		case r.Method == http.MethodPut && r.URL.Path == "/legacy/part/1":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-upload/complete":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	if err := c.WriteStream(context.Background(), "/legacy.bin", bytes.NewReader([]byte("12345678")), 8, nil); err != nil {
		t.Fatalf("WriteStream: %v", err)
	}
	if !usedLegacy.Load() {
		t.Fatal("expected legacy initiate fallback to be used")
	}
}

func TestWriteStreamLargeFileFallsBackOnUnknownUploadAction(t *testing.T) {
	var usedLegacy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			http.Error(w, `{"error":"unknown upload action"}`, http.StatusBadRequest)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/legacy.bin":
			usedLegacy.Store(true)
			plan := UploadPlan{UploadID: "legacy-upload-400", Parts: []PartURL{{Number: 1, URL: fmt.Sprintf("http://%s/legacy400/part/1", r.Host), Size: 8}}}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(plan)
		case r.Method == http.MethodPut && r.URL.Path == "/legacy400/part/1":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-upload-400/complete":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	if err := c.WriteStream(context.Background(), "/legacy.bin", bytes.NewReader([]byte("12345678")), 8, nil); err != nil {
		t.Fatalf("WriteStream: %v", err)
	}
	if !usedLegacy.Load() {
		t.Fatal("expected legacy initiate fallback to be used")
	}
}

func TestWriteStreamLargeFileRequiresSeekableReader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("server should not be called for non-seekable large upload")
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1

	if err := c.WriteStream(context.Background(), "/large.bin", io.NopCloser(bytes.NewReader([]byte("12345678"))), 8, nil); err == nil {
		t.Fatal("expected error for non-seekable large upload")
	}
}

// TestReadStreamSmallFile verifies direct read for small files.
func TestReadStreamSmallFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("small content"))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	rc, err := c.ReadStream(context.Background(), "/small.txt")
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer func() { _ = rc.Close() }()

	data, _ := io.ReadAll(rc)
	if string(data) != "small content" {
		t.Errorf("got %q, want %q", data, "small content")
	}
}

// TestReadStreamLargeFile verifies 302 redirect follow for large files.
func TestReadStreamLargeFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			// Return 302 with presigned URL
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/presigned", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/presigned":
			_, _ = w.Write([]byte("large content from S3"))
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	rc, err := c.ReadStream(context.Background(), "/large.bin")
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer func() { _ = rc.Close() }()

	data, _ := io.ReadAll(rc)
	if string(data) != "large content from S3" {
		t.Errorf("got %q, want %q", data, "large content from S3")
	}
}

func TestReadStreamRangeLargeFileFallbackOnOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/presigned", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/presigned":
			if got := r.Header.Get("Range"); got != "bytes=5-8" {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK) // proxy ignored Range
			_, _ = w.Write([]byte("full-body"))
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	rc, err := c.ReadStreamRange(context.Background(), "/large.bin", 5, 4)
	if err != nil {
		t.Fatalf("ReadStreamRange: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	// "full-body" → skip 5 → "body" (4 bytes)
	if got := string(data); got != "body" {
		t.Errorf("got %q, want %q", got, "body")
	}
}

func TestReadStreamRangeLargeFileTreats416AsEOF(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/presigned", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/presigned":
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	rc, err := c.ReadStreamRange(context.Background(), "/large.bin", 100, 4)
	if err != nil {
		t.Fatalf("ReadStreamRange: %v", err)
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected EOF empty body, got %q", data)
	}
}

// TestResumeUpload verifies the two-step resume flow.
func TestResumeUpload(t *testing.T) {
	var mu sync.Mutex
	uploadedParts := map[int][]byte{}
	completeCalled := false
	var progressCalls [][2]int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/uploads":
			_ = json.NewEncoder(w).Encode(struct {
				Uploads []UploadMeta `json:"uploads"`
			}{
				Uploads: []UploadMeta{{
					UploadID:   "resume-456",
					PartsTotal: 3,
					Status:     "UPLOADING",
				}},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-456/resume":
			// Step 2: Return missing parts (only part 2 is missing)
			plan := UploadPlan{
				UploadID: "resume-456",
				PartSize: 4, // standard part size for this upload
				Parts: []PartURL{
					{Number: 2, URL: fmt.Sprintf("http://%s/parts/2", r.Host), Size: 4},
				},
			}
			_ = json.NewEncoder(w).Encode(plan)

		case r.Method == http.MethodPut && r.URL.Path == "/parts/2":
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploadedParts[2] = data
			mu.Unlock()
			w.Header().Set("ETag", `"etag2"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-456/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	// Full file data: 12 bytes, 3 parts of 4 bytes each
	// Part 1 (offset 0): "aaaa", Part 2 (offset 4): "bbbb", Part 3 (offset 8): "cccc"
	fullData := []byte("aaaabbbbcccc")
	progress := func(partNum, total int, bytesUploaded int64) {
		mu.Lock()
		progressCalls = append(progressCalls, [2]int{partNum, total})
		mu.Unlock()
	}

	err := c.ResumeUpload(context.Background(), "/data/big.bin",
		bytes.NewReader(fullData), int64(len(fullData)), progress)
	if err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}

	if !bytes.Equal(uploadedParts[2], []byte("bbbb")) {
		t.Errorf("part 2: got %q, want %q", uploadedParts[2], "bbbb")
	}
	if !completeCalled {
		t.Error("complete was not called")
	}
	if len(progressCalls) != 1 {
		t.Fatalf("progress called %d times, want 1", len(progressCalls))
	}
	if progressCalls[0] != [2]int{2, 3} {
		t.Fatalf("progress = %v, want [[2 3]]", progressCalls)
	}
}

func TestResumeUploadIntegrationProgressTotal(t *testing.T) {
	blobDir, err := os.MkdirTemp("", "dat9-client-blobs-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(blobDir) }()

	s3Dir, err := os.MkdirTemp("", "dat9-client-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(s3Dir) }()

	initClientTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	defer func() { _ = store.Close() }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	baseURL := "http://" + ln.Addr().String()
	s3c, err := s3client.NewLocal(s3Dir, baseURL+"/s3")
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}

	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}

	ts := httptest.NewUnstartedServer(srvpkg.New(b))
	_ = ts.Listener.Close()
	ts.Listener = ln
	ts.Start()
	defer ts.Close()

	c := New(ts.URL, "")
	data := bytes.Repeat([]byte("x"), 20<<20) // 20MB => 3 parts with 8MB part size

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/resume-int.bin", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Dat9-Content-Length", fmt.Sprintf("%d", len(data)))
	checksums, err := computePartChecksumsFromReaderAt(bytes.NewReader(data), int64(len(data)), s3client.PartSize)
	if err != nil {
		t.Fatalf("compute checksums: %v", err)
	}
	req.Header.Set("X-Dat9-Part-Checksums", strings.Join(checksums, ","))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("initiate upload: expected 202, got %d", resp.StatusCode)
	}

	var plan UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatalf("decode upload plan: %v", err)
	}
	if len(plan.Parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(plan.Parts))
	}

	req, err = http.NewRequest(http.MethodPut, plan.Parts[0].URL, bytes.NewReader(data[:int(plan.Parts[0].Size)]))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = plan.Parts[0].Size
	if plan.Parts[0].ChecksumSHA256 != "" {
		req.Header.Set("x-amz-checksum-sha256", plan.Parts[0].ChecksumSHA256)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload part 1: expected 200, got %d", resp.StatusCode)
	}

	var mu sync.Mutex
	var progressCalls [][2]int
	progress := func(partNum, total int, bytesUploaded int64) {
		mu.Lock()
		progressCalls = append(progressCalls, [2]int{partNum, total})
		mu.Unlock()
	}

	if err := c.ResumeUpload(context.Background(), "/resume-int.bin", bytes.NewReader(data), int64(len(data)), progress); err != nil {
		t.Fatalf("ResumeUpload integration: %v", err)
	}

	if len(progressCalls) != 2 {
		t.Fatalf("progress called %d times, want 2", len(progressCalls))
	}
	seen := map[int]bool{}
	for _, call := range progressCalls {
		if call[1] != 3 {
			t.Fatalf("progress total = %d, want 3; calls=%v", call[1], progressCalls)
		}
		seen[call[0]] = true
	}
	if !seen[2] || !seen[3] {
		t.Fatalf("progress part numbers = %v, want parts 2 and 3", progressCalls)
	}
}
