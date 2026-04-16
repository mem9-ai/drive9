package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/s3client"
)

func TestAppendStreamCreatesMissingFile(t *testing.T) {
	var putBody []byte
	var gotExpectedRevision string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/new.txt":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/new.txt":
			gotExpectedRevision = r.Header.Get("X-Dat9-Expected-Revision")
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			putBody = append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.AppendStream(context.Background(), "/new.txt", strings.NewReader("hello"), 5, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if gotExpectedRevision != "0" {
		t.Fatalf("expected revision header = %q, want %q", gotExpectedRevision, "0")
	}
	if got := string(putBody); got != "hello" {
		t.Fatalf("created body = %q, want %q", got, "hello")
	}
}

func TestAppendStreamZeroSizeCreatesMissingFile(t *testing.T) {
	var putCalls int
	var putBody []byte
	var gotExpectedRevision string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/empty.txt":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/empty.txt":
			putCalls++
			gotExpectedRevision = r.Header.Get("X-Dat9-Expected-Revision")
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			putBody = append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.AppendStream(context.Background(), "/empty.txt", strings.NewReader(""), 0, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if putCalls != 1 {
		t.Fatalf("PUT calls = %d, want 1", putCalls)
	}
	if gotExpectedRevision != "0" {
		t.Fatalf("expected revision header = %q, want %q", gotExpectedRevision, "0")
	}
	if len(putBody) != 0 {
		t.Fatalf("created body length = %d, want 0", len(putBody))
	}
}

func TestAppendStreamFallsBackToRewriteForSmallExistingFile(t *testing.T) {
	var appendInitiateCalled bool
	var putBody []byte
	var gotExpectedRevision string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/small.txt":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/small.txt" && r.URL.Query().Has("append"):
			appendInitiateCalled = true
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "file is not S3-stored: /small.txt"})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/small.txt":
			_, _ = io.WriteString(w, "hello")
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/small.txt":
			gotExpectedRevision = r.Header.Get("X-Dat9-Expected-Revision")
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			putBody = append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.AppendStream(context.Background(), "/small.txt", strings.NewReader(" world"), 6, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if !appendInitiateCalled {
		t.Fatal("append initiate endpoint was not called")
	}
	if gotExpectedRevision != "7" {
		t.Fatalf("expected revision header = %q, want %q", gotExpectedRevision, "7")
	}
	if got := string(putBody); got != "hello world" {
		t.Fatalf("rewritten body = %q, want %q", got, "hello world")
	}
}

func TestAppendStreamRewriteUsesStreamingUploadWhenFinalFileIsLarge(t *testing.T) {
	var directPutCalled bool
	var gotExpectedRevision *int64
	var uploaded bytes.Buffer
	var completeCalled bool
	var srv *httptest.Server

	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/small.txt":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/small.txt" && r.URL.Query().Has("append"):
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "file is not S3-stored: /small.txt"})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/small.txt":
			_, _ = io.WriteString(w, "hello")
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/small.txt":
			directPutCalled = true
			w.WriteHeader(http.StatusOK)
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
			if req.Path != "/small.txt" || req.TotalSize != 11 {
				http.Error(w, "unexpected initiate payload", http.StatusBadRequest)
				return
			}
			gotExpectedRevision = req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "rewrite-v2",
				PartSize:   11,
				TotalParts: 1,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/rewrite-v2/presign-batch":
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
				http.Error(w, "unexpected parts request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{
				Parts: []presignedPart{{
					Number:    1,
					URL:       srv.URL + "/v2parts/1",
					Size:      11,
					ExpiresAt: time.Now().Add(time.Minute),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v2parts/1":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = uploaded.Write(body)
			w.Header().Set("ETag", `"etag-v2-1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/rewrite-v2/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.smallFileThreshold = 1
	if err := c.AppendStream(context.Background(), "/small.txt", strings.NewReader(" world"), 6, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if directPutCalled {
		t.Fatal("rewrite append used direct PUT instead of streaming upload")
	}
	if gotExpectedRevision == nil || *gotExpectedRevision != 7 {
		t.Fatalf("expected revision = %v, want 7", gotExpectedRevision)
	}
	if got := uploaded.String(); got != "hello world" {
		t.Fatalf("uploaded rewritten body = %q, want %q", got, "hello world")
	}
	if !completeCalled {
		t.Fatal("complete endpoint was not called")
	}
}

func TestAppendStreamFallsBackToRewriteWhenAppendActionUnsupported(t *testing.T) {
	var appendInitiateCalled bool
	var putBody []byte
	var gotExpectedRevision string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/legacy.txt":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/legacy.txt" && r.URL.Query().Has("append"):
			appendInitiateCalled = true
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unknown POST action"})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/legacy.txt":
			_, _ = io.WriteString(w, "hello")
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/legacy.txt":
			gotExpectedRevision = r.Header.Get("X-Dat9-Expected-Revision")
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			putBody = append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.AppendStream(context.Background(), "/legacy.txt", strings.NewReader(" world"), 6, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if !appendInitiateCalled {
		t.Fatal("append initiate endpoint was not called")
	}
	if gotExpectedRevision != "7" {
		t.Fatalf("expected revision header = %q, want %q", gotExpectedRevision, "7")
	}
	if got := string(putBody); got != "hello world" {
		t.Fatalf("rewritten body = %q, want %q", got, "hello world")
	}
}

func TestAppendStreamUsesFinalSizeForAdaptivePartSize(t *testing.T) {
	baseSize := int64(s3client.PartSize) * 10000
	appendSize := int64(1)
	wantPartSize := s3client.CalcAdaptivePartSize(baseSize + appendSize)
	var gotPartSize int64
	var gotExpectedRevision *int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/huge.bin":
			w.Header().Set("Content-Length", "83886080000")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "12")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/huge.bin" && r.URL.Query().Has("append"):
			var req struct {
				AppendSize       int64  `json:"append_size"`
				PartSize         int64  `json:"part_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if req.AppendSize != appendSize {
				http.Error(w, "unexpected append size", http.StatusBadRequest)
				return
			}
			gotPartSize = req.PartSize
			gotExpectedRevision = req.ExpectedRevision
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "stop"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.AppendStream(context.Background(), "/huge.bin", strings.NewReader("x"), appendSize, nil)
	if err == nil || err.Error() != "stop" {
		t.Fatalf("AppendStream error = %v, want stop", err)
	}
	if gotExpectedRevision == nil || *gotExpectedRevision != 12 {
		t.Fatalf("expected revision = %v, want 12", gotExpectedRevision)
	}
	if gotPartSize != wantPartSize {
		t.Fatalf("append part_size = %d, want %d", gotPartSize, wantPartSize)
	}
}

func TestAppendStreamUsesAppendPlanForLargeFile(t *testing.T) {
	var gotRange string
	var uploaded bytes.Buffer
	var completeCalled bool
	var srv *httptest.Server

	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/large.bin":
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "9")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/large.bin" && r.URL.Query().Has("append"):
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(AppendPlan{
				BaseSize: 10,
				PatchPlan: PatchPlan{
					UploadID: "append-123",
					PartSize: 8,
					UploadParts: []*PatchPartURL{{
						Number:      2,
						URL:         srv.URL + "/upload/2",
						Size:        8,
						Headers:     map[string]string{"X-Upload-Token": "append"},
						ExpiresAt:   "2099-01-01T00:00:00Z",
						ReadURL:     srv.URL + "/read/2",
						ReadHeaders: map[string]string{"Range": "bytes=8-9"},
					}},
					CopiedParts: []int{1},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/read/2":
			gotRange = r.Header.Get("Range")
			_, _ = io.WriteString(w, "ij")
		case r.Method == http.MethodPut && r.URL.Path == "/upload/2":
			if got := r.Header.Get("X-Upload-Token"); got != "append" {
				http.Error(w, "missing upload token", http.StatusBadRequest)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = uploaded.Write(body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/append-123/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.AppendStream(context.Background(), "/large.bin", strings.NewReader("KLMNOP"), 6, nil); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if gotRange != "bytes=8-9" {
		t.Fatalf("Range header = %q, want %q", gotRange, "bytes=8-9")
	}
	if got := uploaded.String(); got != "ijKLMNOP" {
		t.Fatalf("uploaded append part = %q, want %q", got, "ijKLMNOP")
	}
	if !completeCalled {
		t.Fatal("complete endpoint was not called")
	}
}
