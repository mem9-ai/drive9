package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestUploadBufferPoolRestoresFullLengthOnPut(t *testing.T) {
	pool := newUploadBufferPool(8, 1)

	buf, err := pool.get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(buf) != 8 {
		t.Fatalf("initial len = %d, want 8", len(buf))
	}

	pool.put(buf[:3])

	buf, err = pool.get(context.Background())
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if len(buf) != 8 {
		t.Fatalf("restored len = %d, want 8", len(buf))
	}
}

func TestUploadBufferPoolPutDropsForeignShortBuffer(t *testing.T) {
	pool := newUploadBufferPool(8, 1)

	buf, err := pool.get(context.Background())
	if err != nil {
		t.Fatalf("initial get: %v", err)
	}

	// A foreign buffer with smaller capacity should be ignored rather than
	// panicking when put() tries to restore the pool's full buffer length.
	pool.put(make([]byte, 4))
	pool.put(buf[:3])

	buf, err = pool.get(context.Background())
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if len(buf) != 8 {
		t.Fatalf("restored len = %d, want 8", len(buf))
	}
}

func TestUploadBufferPoolGetHonorsContextCancel(t *testing.T) {
	pool := newUploadBufferPool(4, 1)
	buf, err := pool.get(context.Background())
	if err != nil {
		t.Fatalf("initial get: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := pool.get(ctx); err == nil {
		t.Fatal("expected canceled get to fail")
	} else if err != context.Canceled {
		t.Fatalf("canceled get error = %v, want %v", err, context.Canceled)
	}

	pool.put(buf)
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

func TestWriteStreamWithSummarySmallFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/small-summary.txt" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	data := []byte("summary me")
	summary, err := c.WriteStreamWithSummary(context.Background(), "/small-summary.txt", bytes.NewReader(data), int64(len(data)), nil)
	if err != nil {
		t.Fatalf("WriteStreamWithSummary: %v", err)
	}
	if summary == nil {
		t.Fatal("summary is nil")
	}
	if summary.Type != "upload_summary" {
		t.Fatalf("summary.Type = %q, want upload_summary", summary.Type)
	}
	if summary.Mode != "direct_put" {
		t.Fatalf("summary.Mode = %q, want direct_put", summary.Mode)
	}
	if summary.TotalBytes != int64(len(data)) {
		t.Fatalf("summary.TotalBytes = %d, want %d", summary.TotalBytes, len(data))
	}
	if summary.DirectWriteSeconds < 0 {
		t.Fatalf("summary.DirectWriteSeconds = %f, want >= 0", summary.DirectWriteSeconds)
	}
	if summary.ElapsedSeconds < 0 {
		t.Fatalf("summary.ElapsedSeconds = %f, want >= 0", summary.ElapsedSeconds)
	}
}

func TestWriteStreamWithSummaryAndTagsSmallFileSetsTagHeaders(t *testing.T) {
	var gotTagHeaders []string
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs/tagged-small.txt" {
			http.NotFound(w, r)
			return
		}
		gotTagHeaders = append([]string(nil), r.Header.Values("X-Dat9-Tag")...)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		gotBody = append([]byte(nil), body...)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	data := []byte("hello")
	_, err := c.WriteStreamWithSummaryAndTags(context.Background(), "/tagged-small.txt", bytes.NewReader(data), int64(len(data)), nil, map[string]string{
		"topic": "cat",
		"owner": "alice",
	})
	if err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags: %v", err)
	}
	if got := string(gotBody); got != "hello" {
		t.Fatalf("uploaded body = %q, want %q", got, "hello")
	}
	if len(gotTagHeaders) != 2 {
		t.Fatalf("X-Dat9-Tag count = %d, want 2 (%v)", len(gotTagHeaders), gotTagHeaders)
	}
	if gotTagHeaders[0] != "owner=alice" || gotTagHeaders[1] != "topic=cat" {
		t.Fatalf("X-Dat9-Tag = %v, want [owner=alice topic=cat]", gotTagHeaders)
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

func TestWriteStreamWithSummaryAndTagsLegacyCompleteCarriesTags(t *testing.T) {
	var completeContentType string
	var completeReq struct {
		Tags map[string]string `json:"tags"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			http.NotFound(w, r)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(UploadPlan{
				UploadID: "legacy-tags",
				PartSize: 8,
				Parts: []PartURL{
					{Number: 1, URL: fmt.Sprintf("http://%s/legacy-tags/part/1", r.Host), Size: 8},
				},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/legacy-tags/part/1":
			w.Header().Set("ETag", `"etag-legacy-1"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-tags/complete":
			completeContentType = r.Header.Get("Content-Type")
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	_, err := c.WriteStreamWithSummaryAndTags(
		context.Background(),
		"/legacy-tags.bin",
		bytes.NewReader([]byte("12345678")),
		8,
		nil,
		map[string]string{"topic": "cat", "owner": "alice"},
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags: %v", err)
	}
	if completeContentType != "application/json" {
		t.Fatalf("complete Content-Type = %q, want application/json", completeContentType)
	}
	if completeReq.Tags["owner"] != "alice" || completeReq.Tags["topic"] != "cat" || len(completeReq.Tags) != 2 {
		t.Fatalf("complete tags = %+v, want owner/topic", completeReq.Tags)
	}
}

func TestWriteStreamWithSummaryAndTagsLegacyCompleteClearsTagsWithEmptyMap(t *testing.T) {
	var completeContentType string
	var completeReq struct {
		Tags map[string]string `json:"tags"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			http.NotFound(w, r)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(UploadPlan{
				UploadID: "legacy-clear-tags",
				PartSize: 8,
				Parts: []PartURL{{
					Number: 1,
					URL:    fmt.Sprintf("http://%s/legacy-clear-tags/part/1", r.Host),
					Size:   8,
				}},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/legacy-clear-tags/part/1":
			w.Header().Set("ETag", `"etag-legacy-clear-1"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-clear-tags/complete":
			completeContentType = r.Header.Get("Content-Type")
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	_, err := c.WriteStreamWithSummaryAndTags(
		context.Background(),
		"/legacy-clear-tags.bin",
		bytes.NewReader([]byte("12345678")),
		8,
		nil,
		map[string]string{},
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags: %v", err)
	}
	if completeContentType != "application/json" {
		t.Fatalf("complete Content-Type = %q, want application/json", completeContentType)
	}
	if completeReq.Tags == nil {
		t.Fatal("complete tags = nil, want explicit empty map")
	}
	if len(completeReq.Tags) != 0 {
		t.Fatalf("complete tags = %+v, want explicit empty map", completeReq.Tags)
	}
}

func TestWriteStreamV1WithSummaryRejectsInvalidTagsBeforeInitiate(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.writeStreamV1WithSummary(
		context.Background(),
		"/legacy-invalid-tags.bin",
		bytes.NewReader([]byte("12345678")),
		8,
		nil,
		-1,
		&UploadSummary{},
		map[string]string{"owner": string([]byte{0xff})},
		"",
	)
	if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("error = %v, want invalid UTF-8", err)
	}
	if requests != 0 {
		t.Fatalf("request count = %d, want 0", requests)
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

func TestWriteStreamWithSummaryAndTagsV2CompleteCarriesTags(t *testing.T) {
	var completeReq struct {
		Parts []completePart    `json:"parts"`
		Tags  map[string]string `json:"tags"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "v2-tags",
				PartSize:   8,
				TotalParts: 1,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
				},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-tags/presign-batch":
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{{
					Number:    1,
					URL:       fmt.Sprintf("http://%s/v2-tags/part/1", r.Host),
					Size:      8,
					ExpiresAt: time.Now().Add(time.Minute),
				}},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v2-tags/part/1":
			w.Header().Set("ETag", `"etag-v2-tags-1"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-tags/complete":
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	_, err := c.WriteStreamWithSummaryAndTags(
		context.Background(),
		"/v2-tags.bin",
		bytes.NewReader([]byte("abcdefgh")),
		8,
		nil,
		map[string]string{"topic": "cat", "owner": "alice"},
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags: %v", err)
	}
	if len(completeReq.Parts) != 1 || completeReq.Parts[0].Number != 1 {
		t.Fatalf("complete parts = %+v, want one part", completeReq.Parts)
	}
	if completeReq.Tags["owner"] != "alice" || completeReq.Tags["topic"] != "cat" || len(completeReq.Tags) != 2 {
		t.Fatalf("complete tags = %+v, want owner/topic", completeReq.Tags)
	}
}

func TestWriteStreamWithSummaryAndTagsV2CompleteClearsTagsWithEmptyMap(t *testing.T) {
	var completeReq struct {
		Parts []completePart    `json:"parts"`
		Tags  map[string]string `json:"tags"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:         "v2-clear-tags",
				PartSize:         8,
				TotalParts:       1,
				ChecksumContract: checksumContract{Supported: []string{"SHA-256"}},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-clear-tags/presign-batch":
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{{
					Number:    1,
					URL:       fmt.Sprintf("http://%s/v2-clear-tags/part/1", r.Host),
					Size:      8,
					ExpiresAt: time.Now().Add(time.Minute),
				}},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v2-clear-tags/part/1":
			w.Header().Set("ETag", `"etag-v2-clear-tags-1"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-clear-tags/complete":
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	_, err := c.WriteStreamWithSummaryAndTags(
		context.Background(),
		"/v2-clear-tags.bin",
		bytes.NewReader([]byte("abcdefgh")),
		8,
		nil,
		map[string]string{},
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummaryAndTags: %v", err)
	}
	if len(completeReq.Parts) != 1 || completeReq.Parts[0].Number != 1 {
		t.Fatalf("complete parts = %+v, want one part", completeReq.Parts)
	}
	if completeReq.Tags == nil {
		t.Fatal("complete tags = nil, want explicit empty map")
	}
	if len(completeReq.Tags) != 0 {
		t.Fatalf("complete tags = %+v, want explicit empty map", completeReq.Tags)
	}
}

func TestWriteStreamV2WithSummaryRejectsInvalidTagsBeforeInitiate(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.writeStreamV2WithSummary(
		context.Background(),
		"/v2-invalid-tags.bin",
		bytes.NewReader([]byte("abcdefgh")),
		8,
		nil,
		-1,
		&UploadSummary{},
		map[string]string{"owner": string([]byte{0xff})},
		"",
	)
	if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("error = %v, want invalid UTF-8", err)
	}
	if requests != 0 {
		t.Fatalf("request count = %d, want 0", requests)
	}
}

func TestCompleteUploadHelpersRejectInvalidTagsBeforeRequest(t *testing.T) {
	t.Run("v1", func(t *testing.T) {
		requests := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests++
			http.NotFound(w, r)
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		err := c.completeUploadWithTags(context.Background(), "legacy-invalid-tags", map[string]string{
			"owner": string([]byte{0xff}),
		})
		if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
			t.Fatalf("error = %v, want invalid UTF-8", err)
		}
		if requests != 0 {
			t.Fatalf("request count = %d, want 0", requests)
		}
	})

	t.Run("v2", func(t *testing.T) {
		requests := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests++
			http.NotFound(w, r)
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		err := c.completeUploadV2(context.Background(), "v2-invalid-tags", []completePart{{
			Number: 1,
			ETag:   `"etag-1"`,
		}}, map[string]string{
			"owner": string([]byte{0xff}),
		})
		if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
			t.Fatalf("error = %v, want invalid UTF-8", err)
		}
		if requests != 0 {
			t.Fatalf("request count = %d, want 0", requests)
		}
	})
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

func TestPresignPipelineRecorderExcludesSendBlocking(t *testing.T) {
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-blocked/presign-batch":
			requests.Add(1)
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{
					{Number: 1, URL: "http://example.invalid/1", Size: 1, ExpiresAt: time.Now().Add(time.Minute)},
					{Number: 2, URL: "http://example.invalid/2", Size: 1, ExpiresAt: time.Now().Add(time.Minute)},
					{Number: 3, URL: "http://example.invalid/3", Size: 1, ExpiresAt: time.Now().Add(time.Minute)},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	recorder := &uploadDurationRecorder{}
	presignCh := make(chan presignedPart, 1)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		c.presignPipeline(ctx, &uploadPlanV2{UploadID: "v2-blocked", TotalParts: 3}, 3, presignCh, errCh, recorder)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for len(presignCh) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("presign pipeline did not enqueue the first part")
		}
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(75 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("presign pipeline did not exit after cancellation")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("presign pipeline error = %v, want context canceled", err)
		}
	default:
		t.Fatal("expected presign pipeline to report cancellation")
	}

	if requests.Load() != 1 {
		t.Fatalf("presign batch requests = %d, want 1", requests.Load())
	}
	if got := recorder.Seconds(); got >= 0.05 {
		t.Fatalf("presign recorder included channel blocking: got %.3fs", got)
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

func TestWriteStreamConditionalV2CarriesExpectedRevision(t *testing.T) {
	var gotExpected *int64
	var completeCalled atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			gotExpected = req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "v2-cas",
				PartSize:   8,
				TotalParts: 1,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
				},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-cas/presign-batch":
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{{
					Number:    1,
					URL:       fmt.Sprintf("http://%s/v2parts/1", r.Host),
					Size:      5,
					ExpiresAt: time.Now().Add(time.Minute),
				}},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v2parts/1":
			w.Header().Set("ETag", `"etag-v2-cas"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2-cas/complete":
			completeCalled.Store(true)
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	if err := c.WriteStreamConditional(context.Background(), "/cas-v2.bin", bytes.NewReader([]byte("12345")), 5, nil, 27); err != nil {
		t.Fatalf("WriteStreamConditional: %v", err)
	}
	if gotExpected == nil || *gotExpected != 27 {
		t.Fatalf("expected_revision = %v, want 27", gotExpected)
	}
	if !completeCalled.Load() {
		t.Fatal("complete was not called")
	}
}

func TestWriteStreamConditionalLegacyFallbackCarriesExpectedRevisionHeader(t *testing.T) {
	var gotExpected string
	var sawV2 atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			sawV2.Store(true)
			http.NotFound(w, r)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			http.NotFound(w, r)

		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/legacy-cas.bin":
			gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
			plan := UploadPlan{
				UploadID: "legacy-cas",
				Parts: []PartURL{{
					Number: 1,
					URL:    fmt.Sprintf("http://%s/legacy/part/1", r.Host),
					Size:   8,
				}},
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(plan)

		case r.Method == http.MethodPut && r.URL.Path == "/legacy/part/1":
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-cas/complete":
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	if err := c.WriteStreamConditional(context.Background(), "/legacy-cas.bin", bytes.NewReader([]byte("12345678")), 8, nil, 31); err != nil {
		t.Fatalf("WriteStreamConditional: %v", err)
	}
	if !sawV2.Load() {
		t.Fatal("expected v2 probe before legacy fallback")
	}
	if gotExpected != "31" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want %q", gotExpected, "31")
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

func TestDownloadToFileWithSummaryReusesPresignedURL(t *testing.T) {
	data := bytes.Repeat([]byte("ab"), downloadChunkSize/2)
	data = append(data, []byte("tail")...)

	var readRequests atomic.Int32
	var objectRequests atomic.Int32
	var rangedRequests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			readRequests.Add(1)
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/presigned?token=fixed", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/presigned":
			objectRequests.Add(1)
			if got := r.URL.RawQuery; got != "token=fixed" {
				http.Error(w, "wrong presigned url query: "+got, http.StatusBadRequest)
				return
			}
			rangeHeader := r.Header.Get("Range")
			if !strings.HasPrefix(rangeHeader, "bytes=") {
				http.Error(w, "missing range", http.StatusBadRequest)
				return
			}
			rangedRequests.Add(1)
			var start, end int
			if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
				http.Error(w, "bad range: "+rangeHeader, http.StatusBadRequest)
				return
			}
			if start < 0 || end < start || end >= len(data) {
				http.Error(w, "range outside test data", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	localPath := filepath.Join(t.TempDir(), "large.bin")
	c := New(srv.URL, "")

	summary, err := c.DownloadToFileWithSummary(context.Background(), "/large.bin", localPath, int64(len(data)))
	if err != nil {
		t.Fatalf("DownloadToFileWithSummary: %v", err)
	}
	if summary == nil {
		t.Fatal("expected download summary for large-file path")
	}
	if got := readRequests.Load(); got != 1 {
		t.Fatalf("expected one /v1/fs request, got %d", got)
	}
	if got := objectRequests.Load(); got != 2 {
		t.Fatalf("expected two object requests, got %d", got)
	}
	if got := rangedRequests.Load(); got != 2 {
		t.Fatalf("expected two range requests, got %d", got)
	}
	if summary.Mode != "parallel_range_reuse_presigned_url" {
		t.Fatalf("summary mode = %q, want %q", summary.Mode, "parallel_range_reuse_presigned_url")
	}
	if summary.RangeCount != 2 {
		t.Fatalf("summary range_count = %d, want 2", summary.RangeCount)
	}
	if summary.Concurrency != 2 {
		t.Fatalf("summary concurrency = %d, want 2", summary.Concurrency)
	}

	downloaded, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(downloaded, data) {
		t.Fatal("downloaded file content mismatch")
	}
}

func TestDownloadToFileWithSummaryFailsWhenRangeNotHonored(t *testing.T) {
	data := bytes.Repeat([]byte("z"), downloadChunkSize+1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/presigned", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/presigned":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	localPath := filepath.Join(t.TempDir(), "large.bin")
	c := New(srv.URL, "")

	_, err := c.DownloadToFileWithSummary(context.Background(), "/large.bin", localPath, int64(len(data)))
	if err == nil {
		t.Fatal("expected strict range failure")
	}
	if !strings.Contains(err.Error(), "range request was not honored") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDownloadToFileWithSummaryFallsBackWhenReadPathDoesNotRedirect(t *testing.T) {
	data := bytes.Repeat([]byte("q"), downloadParallelThreshold+17)

	var readRequests atomic.Int32
	var objectRequests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			readRequests.Add(1)
			_, _ = w.Write(data)
		case "/s3/presigned":
			objectRequests.Add(1)
			http.Error(w, "unexpected object request", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	localPath := filepath.Join(t.TempDir(), "large.bin")
	c := New(srv.URL, "")

	summary, err := c.DownloadToFileWithSummary(context.Background(), "/large.bin", localPath, int64(len(data)))
	if err != nil {
		t.Fatalf("DownloadToFileWithSummary: %v", err)
	}
	if summary != nil {
		t.Fatalf("expected nil summary when falling back to sequential download, got %+v", summary)
	}
	if got := readRequests.Load(); got != 2 {
		t.Fatalf("expected one resolve attempt plus one sequential read, got %d requests", got)
	}
	if got := objectRequests.Load(); got != 0 {
		t.Fatalf("expected no object storage requests, got %d", got)
	}

	downloaded, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(downloaded, data) {
		t.Fatal("downloaded file content mismatch")
	}
}

func TestDownloadToFileWithSummarySmallFileUsesSequentialPath(t *testing.T) {
	var readRequests atomic.Int32
	data := []byte("small content")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/small.txt":
			readRequests.Add(1)
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	localPath := filepath.Join(t.TempDir(), "small.txt")
	c := New(srv.URL, "")

	summary, err := c.DownloadToFileWithSummary(context.Background(), "/small.txt", localPath, int64(len(data)))
	if err != nil {
		t.Fatalf("DownloadToFileWithSummary: %v", err)
	}
	if summary != nil {
		t.Fatalf("expected nil summary for small-file path, got %+v", summary)
	}
	if got := readRequests.Load(); got != 1 {
		t.Fatalf("expected one sequential read request, got %d", got)
	}

	downloaded, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(downloaded) != string(data) {
		t.Fatalf("got %q, want %q", downloaded, data)
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

func TestResumeUploadWithSummaryAndTagsSendsTagsOnComplete(t *testing.T) {
	var completeReq struct {
		Tags map[string]string `json:"tags"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/uploads":
			_ = json.NewEncoder(w).Encode(struct {
				Uploads []UploadMeta `json:"uploads"`
			}{
				Uploads: []UploadMeta{{
					UploadID:   "resume-tags",
					PartsTotal: 2,
					Status:     "UPLOADING",
				}},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-tags/resume":
			_ = json.NewEncoder(w).Encode(UploadPlan{
				UploadID: "resume-tags",
				PartSize: 4,
				Parts: []PartURL{
					{Number: 2, URL: fmt.Sprintf("http://%s/resume-tags/part/2", r.Host), Size: 4},
				},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/resume-tags/part/2":
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-tags/complete":
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.ResumeUploadWithSummaryAndTags(
		context.Background(),
		"/resume-tags.bin",
		bytes.NewReader([]byte("aaaabbbb")),
		8,
		nil,
		map[string]string{"owner": "alice", "topic": "cat"},
	)
	if err != nil {
		t.Fatalf("ResumeUploadWithSummaryAndTags: %v", err)
	}
	if completeReq.Tags["owner"] != "alice" || completeReq.Tags["topic"] != "cat" || len(completeReq.Tags) != 2 {
		t.Fatalf("complete tags = %+v, want owner/topic", completeReq.Tags)
	}
}

func TestResumeUploadWithSummaryAndTagsClearsTagsWithEmptyMap(t *testing.T) {
	var completeReq struct {
		Tags map[string]string `json:"tags"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/uploads":
			_ = json.NewEncoder(w).Encode(struct {
				Uploads []UploadMeta `json:"uploads"`
			}{Uploads: []UploadMeta{{UploadID: "resume-clear-tags", PartsTotal: 2, Status: "UPLOADING"}}})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-clear-tags/resume":
			_ = json.NewEncoder(w).Encode(UploadPlan{
				UploadID: "resume-clear-tags",
				PartSize: 4,
				Parts: []PartURL{{
					Number: 2,
					URL:    fmt.Sprintf("http://%s/resume-clear-tags/part/2", r.Host),
					Size:   4,
				}},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/resume-clear-tags/part/2":
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/resume-clear-tags/complete":
			if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
				http.Error(w, "bad complete json", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.ResumeUploadWithSummaryAndTags(
		context.Background(),
		"/resume-clear-tags.bin",
		bytes.NewReader([]byte("aaaabbbb")),
		8,
		nil,
		map[string]string{},
	)
	if err != nil {
		t.Fatalf("ResumeUploadWithSummaryAndTags: %v", err)
	}
	if completeReq.Tags == nil {
		t.Fatal("complete tags = nil, want explicit empty map")
	}
	if len(completeReq.Tags) != 0 {
		t.Fatalf("complete tags = %+v, want explicit empty map", completeReq.Tags)
	}
}

func TestResumeUploadWithSummaryAndTagsRejectsInvalidTagsBeforeRequests(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.ResumeUploadWithSummaryAndTags(
		context.Background(),
		"/resume-invalid-tags.bin",
		bytes.NewReader([]byte("aaaabbbb")),
		8,
		nil,
		map[string]string{"owner": string([]byte{0xff})},
	)
	if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("error = %v, want invalid UTF-8", err)
	}
	if requests != 0 {
		t.Fatalf("request count = %d, want 0", requests)
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
	if plan.Parts[0].ChecksumCRC32C != "" {
		req.Header.Set("x-amz-checksum-crc32c", plan.Parts[0].ChecksumCRC32C)
	} else if plan.Parts[0].ChecksumSHA256 != "" {
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
