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
