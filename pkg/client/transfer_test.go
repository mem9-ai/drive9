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
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	srvpkg "github.com/mem9-ai/dat9/pkg/server"
)

type shortUploadReader struct {
	data          []byte
	seq           *bytes.Reader
	shortOffset   int64
	mu            sync.Mutex
	readsByOffset map[int64]int
}

func newShortUploadReader(data []byte, shortOffset int64) *shortUploadReader {
	cloned := append([]byte(nil), data...)
	return &shortUploadReader{
		data:          cloned,
		seq:           bytes.NewReader(cloned),
		shortOffset:   shortOffset,
		readsByOffset: make(map[int64]int),
	}
}

func (r *shortUploadReader) Read(p []byte) (int, error) {
	return r.seq.Read(p)
}

func (r *shortUploadReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.readsByOffset[off]++
	readCount := r.readsByOffset[off]
	r.mu.Unlock()

	if len(p) == 0 {
		return 0, nil
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	if off == r.shortOffset && readCount == 2 {
		n := copy(p[:len(p)-1], r.data[off:])
		return n, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

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
	var completeCalled atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			// v2 not supported by this mock — trigger v1 fallback
			http.NotFound(w, r)

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
				PartSize: 5,
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
			completeCalled.Store(true)
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
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
	if !completeCalled.Load() {
		t.Error("complete was not called")
	}
	if len(progressCalls) != 2 {
		t.Errorf("progress called %d times, want 2", len(progressCalls))
	}
}

func TestWriteStreamV2SinglePart(t *testing.T) {
	var uploaded []byte
	var progressCalls [][2]int
	var completeReq struct {
		Parts []completePart `json:"parts"`
	}
	var sawV1 atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if req.Path != "/v2-single.bin" || req.TotalSize != 7 {
				http.Error(w, fmt.Sprintf("bad initiate payload: %+v", req), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "v2-single",
				PartSize:   11,
				TotalParts: 1,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
				},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-single/presign-batch":
			var req struct {
				Parts []struct {
					PartNumber int `json:"part_number"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if len(req.Parts) != 1 || req.Parts[0].PartNumber != 1 {
				http.Error(w, fmt.Sprintf("bad parts request: %+v", req.Parts), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{{
					Number:    1,
					URL:       fmt.Sprintf("http://%s/v2parts/1", r.Host),
					Size:      7,
					ExpiresAt: time.Now().Add(time.Minute),
				}},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v2parts/1":
			if got := r.Header.Get("x-amz-checksum-sha256"); got != "" {
				http.Error(w, "v2 upload should not send checksum header", http.StatusBadRequest)
				return
			}
			uploaded, _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"etag-v2-1"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-single/complete":
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		case strings.HasPrefix(r.URL.Path, "/v1/"):
			sawV1.Store(true)
			http.Error(w, "unexpected v1 request", http.StatusInternalServerError)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	err := c.WriteStream(context.Background(), "/v2-single.bin", bytes.NewReader([]byte("1234567")), 7,
		func(partNum, total int, bytesUploaded int64) {
			progressCalls = append(progressCalls, [2]int{partNum, total})
		})
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}
	if sawV1.Load() {
		t.Fatal("unexpected v1 fallback during v2 single-part upload")
	}
	if got := string(uploaded); got != "1234567" {
		t.Fatalf("uploaded body = %q, want %q", got, "1234567")
	}
	if len(progressCalls) != 1 || progressCalls[0] != [2]int{1, 1} {
		t.Fatalf("progress calls = %v, want [[1 1]]", progressCalls)
	}
	if len(completeReq.Parts) != 1 || completeReq.Parts[0] != (completePart{Number: 1, ETag: `"etag-v2-1"`}) {
		t.Fatalf("complete payload = %+v, want one part with etag", completeReq.Parts)
	}
}

func TestWriteStreamV2MultiPartUsesPlanPartSize(t *testing.T) {
	var mu sync.Mutex
	uploadedParts := map[int][]byte{}
	var presignReq struct {
		Parts []struct {
			PartNumber int `json:"part_number"`
		} `json:"parts"`
	}
	var completeReq struct {
		Parts []completePart `json:"parts"`
	}
	var progressCalls [][2]int
	var sawV1 atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "v2-multi",
				PartSize:   5,
				TotalParts: 3,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
				},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-multi/presign-batch":
			if err := json.NewDecoder(r.Body).Decode(&presignReq); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{
					{Number: 1, URL: fmt.Sprintf("http://%s/v2parts/1", r.Host), Size: 5, ExpiresAt: time.Now().Add(time.Minute)},
					{Number: 2, URL: fmt.Sprintf("http://%s/v2parts/2", r.Host), Size: 5, ExpiresAt: time.Now().Add(time.Minute)},
					{Number: 3, URL: fmt.Sprintf("http://%s/v2parts/3", r.Host), Size: 2, ExpiresAt: time.Now().Add(time.Minute)},
				},
			})

		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v2parts/"):
			var partNum int
			if _, err := fmt.Sscanf(r.URL.Path, "/v2parts/%d", &partNum); err != nil {
				http.Error(w, "bad part path", http.StatusBadRequest)
				return
			}
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploadedParts[partNum] = data
			mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, partNum))
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-multi/complete":
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		case strings.HasPrefix(r.URL.Path, "/v1/"):
			sawV1.Store(true)
			http.Error(w, "unexpected v1 request", http.StatusInternalServerError)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	err := c.WriteStream(context.Background(), "/v2-multi.bin", bytes.NewReader([]byte("abcdefghijkl")), 12,
		func(partNum, total int, bytesUploaded int64) {
			mu.Lock()
			progressCalls = append(progressCalls, [2]int{partNum, total})
			mu.Unlock()
		})
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}
	if sawV1.Load() {
		t.Fatal("unexpected v1 fallback during v2 multipart upload")
	}
	if len(presignReq.Parts) != 3 ||
		presignReq.Parts[0].PartNumber != 1 ||
		presignReq.Parts[1].PartNumber != 2 ||
		presignReq.Parts[2].PartNumber != 3 {
		t.Fatalf("presign batch parts = %+v, want [1 2 3]", presignReq.Parts)
	}
	if got := string(uploadedParts[1]); got != "abcde" {
		t.Fatalf("part 1 = %q, want %q", got, "abcde")
	}
	if got := string(uploadedParts[2]); got != "fghij" {
		t.Fatalf("part 2 = %q, want %q", got, "fghij")
	}
	if got := string(uploadedParts[3]); got != "kl" {
		t.Fatalf("part 3 = %q, want %q", got, "kl")
	}
	if len(completeReq.Parts) != 3 {
		t.Fatalf("complete payload has %d parts, want 3", len(completeReq.Parts))
	}
	for i, part := range completeReq.Parts {
		want := completePart{Number: i + 1, ETag: fmt.Sprintf(`"etag-%d"`, i+1)}
		if part != want {
			t.Fatalf("complete part[%d] = %+v, want %+v", i, part, want)
		}
	}
	if len(progressCalls) != 3 {
		t.Fatalf("progress calls = %v, want 3 calls", progressCalls)
	}
}

func TestWriteStreamV2RePresignsExpiredPart(t *testing.T) {
	var expiredUploads atomic.Int32
	var freshUploads atomic.Int32
	var rePresignCalls atomic.Int32
	var completeCalled atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "v2-retry",
				PartSize:   4,
				TotalParts: 1,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
				},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-retry/presign-batch":
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{{
					Number:    1,
					URL:       fmt.Sprintf("http://%s/v2parts/expired/1", r.Host),
					Size:      4,
					ExpiresAt: time.Now().Add(-time.Minute),
				}},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-retry/presign":
			rePresignCalls.Add(1)
			var req struct {
				PartNumber int `json:"part_number"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if req.PartNumber != 1 {
				http.Error(w, fmt.Sprintf("bad part number %d", req.PartNumber), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(presignedPart{
				Number:    1,
				URL:       fmt.Sprintf("http://%s/v2parts/fresh/1", r.Host),
				Size:      4,
				ExpiresAt: time.Now().Add(time.Minute),
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v2parts/expired/1":
			expiredUploads.Add(1)
			w.WriteHeader(http.StatusForbidden)

		case r.Method == http.MethodPut && r.URL.Path == "/v2parts/fresh/1":
			freshUploads.Add(1)
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "data" {
				http.Error(w, "bad fresh upload body", http.StatusBadRequest)
				return
			}
			w.Header().Set("ETag", `"etag-fresh"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-retry/complete":
			completeCalled.Store(true)
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	if err := c.WriteStream(context.Background(), "/retry.bin", bytes.NewReader([]byte("data")), 4, nil); err != nil {
		t.Fatalf("WriteStream: %v", err)
	}
	if expiredUploads.Load() != 1 {
		t.Fatalf("expired upload attempts = %d, want 1", expiredUploads.Load())
	}
	if rePresignCalls.Load() != 1 {
		t.Fatalf("re-presign calls = %d, want 1", rePresignCalls.Load())
	}
	if freshUploads.Load() != 1 {
		t.Fatalf("fresh upload attempts = %d, want 1", freshUploads.Load())
	}
	if !completeCalled.Load() {
		t.Fatal("complete was not called after re-presign retry")
	}
}

func TestWriteStreamLargeFileErrorsOnShortPartRead(t *testing.T) {
	var mu sync.Mutex
	uploadedParts := map[string][]byte{}
	var completeCalled atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			plan := UploadPlan{
				UploadID: "upload-short-read",
				PartSize: 5,
				Parts: []PartURL{
					{Number: 1, URL: fmt.Sprintf("http://%s/parts/1", r.Host), Size: 5},
					{Number: 2, URL: fmt.Sprintf("http://%s/parts/2", r.Host), Size: 3},
				},
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(plan)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/parts/"):
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			uploadedParts[r.URL.Path] = data
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/upload-short-read/complete":
			completeCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1

	err := c.WriteStream(context.Background(), "/large.bin", newShortUploadReader([]byte("12345678"), 0), 8, nil)
	if err == nil {
		t.Fatal("expected short read error")
	}
	if !strings.Contains(err.Error(), "short read for part 1") {
		t.Fatalf("expected short read error, got %v", err)
	}
	if completeCalled.Load() {
		t.Fatal("complete should not be called after short read")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, ok := uploadedParts["/parts/1"]; ok {
		t.Fatalf("short-read part should not be uploaded: got %q", uploadedParts["/parts/1"])
	}
}

func TestWriteStreamLargeFileFallsBackToLegacyInitiate(t *testing.T) {
	var usedLegacy atomic.Bool
	var sawV2 atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			sawV2.Store(true)
			http.NotFound(w, r)
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
	if !sawV2.Load() {
		t.Fatal("expected client to probe /v2 before falling back to legacy upload")
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
			var req struct {
				PartChecksums []string `json:"part_checksums"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if len(req.PartChecksums) == 0 {
				http.Error(w, "bad checksums", http.StatusBadRequest)
				return
			}
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

func TestResumeUploadFallsBackToLegacyHeader(t *testing.T) {
	var resumeCalls int
	var usedLegacy atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/uploads":
			_ = json.NewEncoder(w).Encode(struct {
				Uploads []UploadMeta `json:"uploads"`
			}{
				Uploads: []UploadMeta{{UploadID: "resume-legacy", PartsTotal: 1, Status: "UPLOADING"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-legacy/resume":
			resumeCalls++
			if resumeCalls == 1 {
				http.Error(w, `{"error":"missing X-Dat9-Part-Checksums header"}`, http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Dat9-Part-Checksums") == "" {
				http.Error(w, "missing legacy header", http.StatusBadRequest)
				return
			}
			usedLegacy.Store(true)
			_ = json.NewEncoder(w).Encode(UploadPlan{UploadID: "resume-legacy", PartSize: 8, Parts: []PartURL{{Number: 1, URL: fmt.Sprintf("http://%s/resume-legacy/part/1", r.Host), Size: 8}}})
		case r.Method == http.MethodPut && r.URL.Path == "/resume-legacy/part/1":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-legacy/complete":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.ResumeUpload(context.Background(), "/legacy-resume.bin", bytes.NewReader([]byte("12345678")), 8, nil); err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}
	if !usedLegacy.Load() {
		t.Fatal("expected legacy header fallback to be used")
	}
}

func TestResumeUploadKeepsUsingV1Endpoints(t *testing.T) {
	var uploadedPart []byte
	var completeCalled atomic.Bool
	var v2Called atomic.Bool
	queryCalls := 0
	resumeCalls := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v2/"):
			v2Called.Store(true)
			http.Error(w, "unexpected v2 request", http.StatusInternalServerError)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/uploads":
			queryCalls++
			_ = json.NewEncoder(w).Encode(struct {
				Uploads []UploadMeta `json:"uploads"`
			}{
				Uploads: []UploadMeta{{
					UploadID:   "resume-v1-only",
					PartsTotal: 2,
					Status:     "UPLOADING",
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-v1-only/resume":
			resumeCalls++
			var req uploadResumeRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if len(req.PartChecksums) == 0 {
				http.Error(w, "missing checksums", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(UploadPlan{
				UploadID: "resume-v1-only",
				PartSize: 4,
				Parts: []PartURL{
					{Number: 2, URL: fmt.Sprintf("http://%s/resume-v1-only/part/2", r.Host), Size: 4},
				},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/resume-v1-only/part/2":
			uploadedPart, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-v1-only/complete":
			completeCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	fullData := []byte("aaaabbbb")
	if err := c.ResumeUpload(context.Background(), "/resume-v1-only.bin", bytes.NewReader(fullData), int64(len(fullData)), nil); err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}

	if v2Called.Load() {
		t.Fatal("resume flow should not probe /v2 endpoints")
	}
	if queryCalls != 1 {
		t.Fatalf("query calls = %d, want 1", queryCalls)
	}
	if resumeCalls != 1 {
		t.Fatalf("resume calls = %d, want 1", resumeCalls)
	}
	if !bytes.Equal(uploadedPart, []byte("bbbb")) {
		t.Fatalf("uploaded part = %q, want %q", uploadedPart, "bbbb")
	}
	if !completeCalled.Load() {
		t.Fatal("complete was not called")
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
