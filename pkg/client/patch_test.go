package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/s3client"
)

func TestPatchFileSendsExplicitPartSize(t *testing.T) {
	var uploaded []byte
	var progressCalls [][2]int
	var completeCalled bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/patch.bin":
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if got := int64(req["new_size"].(float64)); got != 12 {
				http.Error(w, fmt.Sprintf("bad new_size %d", got), http.StatusBadRequest)
				return
			}
			dirty := req["dirty_parts"].([]any)
			if len(dirty) != 1 || int(dirty[0].(float64)) != 2 {
				http.Error(w, fmt.Sprintf("bad dirty_parts %+v", dirty), http.StatusBadRequest)
				return
			}
			if got := int64(req["part_size"].(float64)); got != 6 {
				http.Error(w, fmt.Sprintf("bad part_size %d", got), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(PatchPlan{
				UploadID: "patch-123",
				PartSize: 6,
				UploadParts: []*PatchPartURL{{
					Number:      2,
					URL:         fmt.Sprintf("http://%s/upload/2", r.Host),
					Size:        6,
					Headers:     map[string]string{"X-Upload-Token": "upload-token"},
					ReadURL:     fmt.Sprintf("http://%s/read/2", r.Host),
					ReadHeaders: map[string]string{"Range": "bytes=6-11"},
				}},
				CopiedParts: []int{1},
			})

		case r.Method == http.MethodGet && r.URL.Path == "/read/2":
			if got := r.Header.Get("Range"); got != "bytes=6-11" {
				http.Error(w, "missing range header", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte("orig!!"))

		case r.Method == http.MethodPut && r.URL.Path == "/upload/2":
			if got := r.Header.Get("X-Upload-Token"); got != "upload-token" {
				http.Error(w, "missing upload token", http.StatusBadRequest)
				return
			}
			if got := r.Header.Get("x-amz-checksum-sha256"); got == "" {
				http.Error(w, "missing checksum header", http.StatusBadRequest)
				return
			}
			uploaded, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-123/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.PatchFile(context.Background(), "/patch.bin", 12, []int{2},
		func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			if partNumber != 2 {
				t.Fatalf("partNumber = %d, want 2", partNumber)
			}
			if partSize != 6 {
				t.Fatalf("partSize = %d, want 6", partSize)
			}
			if got := string(origData); got != "orig!!" {
				t.Fatalf("origData = %q, want %q", got, "orig!!")
			}
			return []byte("merge!"), nil
		},
		func(partNum, total int, bytesUploaded int64) {
			progressCalls = append(progressCalls, [2]int{partNum, total})
		},
		WithPartSize(6))
	if err != nil {
		t.Fatalf("PatchFile: %v", err)
	}
	if got := string(uploaded); got != "merge!" {
		t.Fatalf("uploaded body = %q, want %q", got, "merge!")
	}
	if !completeCalled {
		t.Fatal("complete was not called")
	}
	if len(progressCalls) != 1 || progressCalls[0] != [2]int{2, 2} {
		t.Fatalf("progress calls = %v, want [[2 2]]", progressCalls)
	}
}

func TestPatchFileOmitsPartSizeAndAcceptsFixedBoundaryPlan(t *testing.T) {
	var uploadedLen int
	var completeCalled bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/omit.bin":
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if _, ok := req["part_size"]; ok {
				http.Error(w, "part_size must be omitted by default", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(PatchPlan{
				UploadID: "patch-omit",
				PartSize: s3client.PartSize,
				UploadParts: []*PatchPartURL{{
					Number: 1,
					URL:    fmt.Sprintf("http://%s/upload/1", r.Host),
					Size:   s3client.PartSize,
				}},
				CopiedParts: []int{2},
			})

		case r.Method == http.MethodPut && r.URL.Path == "/upload/1":
			body, _ := io.ReadAll(r.Body)
			uploadedLen = len(body)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-omit/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.PatchFile(context.Background(), "/omit.bin", s3client.PartSize+1, []int{1},
		func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			if partNumber != 1 {
				t.Fatalf("partNumber = %d, want 1", partNumber)
			}
			if partSize != s3client.PartSize {
				t.Fatalf("partSize = %d, want fixed %d", partSize, s3client.PartSize)
			}
			if len(origData) != 0 {
				t.Fatalf("origData len = %d, want 0", len(origData))
			}
			return bytes.Repeat([]byte("x"), int(partSize)), nil
		},
		nil)
	if err != nil {
		t.Fatalf("PatchFile: %v", err)
	}
	if uploadedLen != int(s3client.PartSize) {
		t.Fatalf("uploaded len = %d, want %d", uploadedLen, s3client.PartSize)
	}
	if !completeCalled {
		t.Fatal("complete was not called")
	}
}

func TestPatchFileFailsFastOnOversizePartSize(t *testing.T) {
	var uploadCalled bool
	var completeCalled bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/invalid.bin":
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			if got := int64(req["part_size"].(float64)); got != s3client.MaxPartSize+1 {
				http.Error(w, fmt.Sprintf("bad part_size %d", got), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": fmt.Sprintf("part_size %d exceeds S3 per-part limit of %d", s3client.MaxPartSize+1, s3client.MaxPartSize),
			})

		case strings.HasPrefix(r.URL.Path, "/upload/"):
			uploadCalled = true
			http.Error(w, "upload should not be attempted", http.StatusInternalServerError)

		case strings.HasPrefix(r.URL.Path, "/v1/uploads/"):
			completeCalled = true
			http.Error(w, "complete should not be attempted", http.StatusInternalServerError)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.PatchFile(context.Background(), "/invalid.bin", s3client.MaxPartSize+1, []int{1},
		func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			t.Fatal("readPart callback should not be invoked on rejected part_size")
			return nil, nil
		},
		nil,
		WithPartSize(s3client.MaxPartSize+1))
	if err == nil {
		t.Fatal("expected oversize part_size error")
	}
	if !strings.Contains(err.Error(), "exceeds S3 per-part limit") {
		t.Fatalf("error = %v, want oversize part_size message", err)
	}
	if uploadCalled {
		t.Fatal("upload should not be attempted after patch rejection")
	}
	if completeCalled {
		t.Fatal("complete should not be attempted after patch rejection")
	}
}

func TestPatchFileSendsExpectedRevision(t *testing.T) {
	var gotExpected *int64
	var completeCalled bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/cas.bin":
			var req struct {
				NewSize          int64  `json:"new_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			gotExpected = req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(PatchPlan{
				UploadID:    "patch-cas",
				PartSize:    8,
				UploadParts: []*PatchPartURL{},
				CopiedParts: []int{1},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-cas/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.PatchFile(context.Background(), "/cas.bin", 8, nil,
		func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			t.Fatal("readPart should not be called when there are no upload parts")
			return nil, nil
		},
		nil,
		WithExpectedRevision(19))
	if err != nil {
		t.Fatalf("PatchFile: %v", err)
	}
	if gotExpected == nil || *gotExpected != 19 {
		t.Fatalf("expected_revision = %v, want 19", gotExpected)
	}
	if !completeCalled {
		t.Fatal("complete was not called")
	}
}
