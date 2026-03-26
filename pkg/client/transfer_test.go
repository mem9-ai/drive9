package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
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
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/large.bin":
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
			json.NewEncoder(w).Encode(plan)

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
	c.smallFileThreshold = 1 // force large file path for test
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

// TestReadStreamSmallFile verifies direct read for small files.
func TestReadStreamSmallFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("small content"))
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	rc, err := c.ReadStream(context.Background(), "/small.txt")
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer rc.Close()

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
			w.Write([]byte("large content from S3"))
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	rc, err := c.ReadStream(context.Background(), "/large.bin")
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if string(data) != "large content from S3" {
		t.Errorf("got %q, want %q", data, "large content from S3")
	}
}

// TestResumeUpload verifies the two-step resume flow.
func TestResumeUpload(t *testing.T) {
	var mu sync.Mutex
	uploadedParts := map[int][]byte{}
	completeCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/uploads":
			// Step 1: Return upload metadata
			json.NewEncoder(w).Encode(UploadMeta{
				UploadID:   "resume-456",
				PartsTotal: 3,
				Status:     "UPLOADING",
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
			json.NewEncoder(w).Encode(plan)

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

	err := c.ResumeUpload(context.Background(), "/data/big.bin",
		bytes.NewReader(fullData), int64(len(fullData)), nil)
	if err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}

	if !bytes.Equal(uploadedParts[2], []byte("bbbb")) {
		t.Errorf("part 2: got %q, want %q", uploadedParts[2], "bbbb")
	}
	if !completeCalled {
		t.Error("complete was not called")
	}
}
