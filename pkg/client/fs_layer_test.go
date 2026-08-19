package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUploadFSLayerFileDirectMultipart(t *testing.T) {
	const payload = "layer payload"
	var uploaded strings.Builder
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/part/") {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Dat9-Actor") != "" {
			t.Errorf("S3 received Drive9 credentials: Authorization=%q Actor=%q", r.Header.Get("Authorization"), r.Header.Get("X-Dat9-Actor"))
		}
		if r.Header.Get("X-Amz-Meta-Test") != "allowed" {
			t.Errorf("S3 allowed presigned header = %q, want allowed", r.Header.Get("X-Amz-Meta-Test"))
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read S3 part: %v", err)
			http.Error(w, "read part", http.StatusInternalServerError)
			return
		}
		uploaded.Write(data)
		w.Header().Set("ETag", `"etag-`+strings.TrimPrefix(r.URL.Path, "/part/")+`"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	var presigned, completed bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("X-Dat9-Actor") != "actor-1" {
			t.Errorf("Drive9 credentials: Authorization=%q Actor=%q", r.Header.Get("Authorization"), r.Header.Get("X-Dat9-Actor"))
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/layers/layer-1/uploads/initiate":
			var req struct {
				Path         string `json:"path"`
				TotalSize    int64  `json:"total_size"`
				BaseRevision int64  `json:"base_revision"`
				Mode         uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode initiate: %v", err)
			}
			if req.Path != "/repo/blob.bin" || req.TotalSize != int64(len(payload)) || req.BaseRevision != 7 || req.Mode != 0o640 {
				t.Errorf("initiate request = %+v", req)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "upload-1",
				PartSize:   5,
				TotalParts: 3,
			})
		case "POST /v1/layers/layer-1/uploads/upload-1/presign-batch":
			var req struct {
				Parts []struct {
					PartNumber int `json:"part_number"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode presign batch: %v", err)
			}
			if len(req.Parts) != 3 {
				t.Errorf("presign parts = %+v, want three parts", req.Parts)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			parts := make([]presignedPart, 0, len(req.Parts))
			for _, requestedPart := range req.Parts {
				partSize := int64(5)
				if requestedPart.PartNumber == 3 {
					partSize = 3
				}
				parts = append(parts, presignedPart{
					Number: requestedPart.PartNumber,
					URL:    s3.URL + "/part/" + string(rune('0'+requestedPart.PartNumber)),
					Size:   partSize,
					Headers: map[string]string{
						"authorization":   "Bearer leaked",
						"x-dAt9-aCtOr":    "actor-leaked",
						"X-Amz-Meta-Test": "allowed",
					},
				})
			}
			presigned = true
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": parts})
		case "POST /v1/layers/layer-1/uploads/upload-1/complete":
			var req struct {
				Parts []completePart `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode complete: %v", err)
			}
			if len(req.Parts) != 3 || req.Parts[0].Number != 1 || req.Parts[2].ETag != `"etag-3"` {
				t.Errorf("complete parts = %+v", req.Parts)
			}
			completed = true
			_ = json.NewEncoder(w).Encode(FSLayerEntry{
				LayerID:     "layer-1",
				Path:        "/repo/blob.bin",
				StorageType: "s3",
				SizeBytes:   int64(len(payload)),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	c := New(api.URL, "secret")
	c.SetActor("actor-1")
	entry, err := c.UploadFSLayerFile(context.Background(), "layer-1", "/repo/blob.bin", strings.NewReader(payload), int64(len(payload)), 7, 0o640, true)
	if err != nil {
		t.Fatalf("UploadFSLayerFile: %v", err)
	}
	if uploaded.String() != payload {
		t.Fatalf("uploaded data = %q, want %q", uploaded.String(), payload)
	}
	if !presigned || !completed {
		t.Fatalf("presigned=%v completed=%v, want both true", presigned, completed)
	}
	if entry.Path != "/repo/blob.bin" || entry.StorageType != "s3" {
		t.Fatalf("entry = %+v", entry)
	}
}

func TestPresignFSLayerUploadPartsAcceptsOutOfOrderResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/layers/layer-1/uploads/upload-1/presign-batch" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"parts": []presignedPart{
			{Number: 3, URL: "https://s3.example/3", Size: 3},
			{Number: 1, URL: "https://s3.example/1", Size: 5},
			{Number: 2, URL: "https://s3.example/2", Size: 5},
		}})
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	parts, err := c.presignFSLayerUploadParts(context.Background(), "layer-1", "upload-1", 1, 3, []int64{5, 5, 3})
	if err != nil {
		t.Fatalf("presignFSLayerUploadParts: %v", err)
	}
	if len(parts) != 3 || parts[0].Number != 1 || parts[1].Number != 2 || parts[2].Number != 3 {
		t.Fatalf("parts = %+v, want part numbers [1 2 3]", parts)
	}
}

func TestUploadFSLayerPartLimitsErrorBody(t *testing.T) {
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, strings.Repeat("x", 65<<10))
	}))
	defer s3.Close()

	c := New("http://drive9.test", "")
	_, err := c.uploadFSLayerPart(context.Background(), presignedPart{URL: s3.URL}, strings.NewReader("payload"), 7)
	if err == nil {
		t.Fatal("uploadFSLayerPart error = nil, want S3 error")
	}
	if len(err.Error()) > (64<<10)+32 {
		t.Fatalf("uploadFSLayerPart error length = %d, want bounded S3 response", len(err.Error()))
	}
}

func TestUploadFSLayerFileRePresignsExpiredPart(t *testing.T) {
	const payload = "abcdefghij"
	var part1Calls, part2Calls, presignCalls, abortCalls int
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read S3 body: %v", err)
		}
		switch r.URL.Path {
		case "/part/1":
			part1Calls++
			if string(data) != "abcde" {
				t.Errorf("S3 part 1 body = %q, want abcde", data)
			}
			w.Header().Set("ETag", `"etag-1"`)
		case "/part/2":
			part2Calls++
			if string(data) != "fghij" {
				t.Errorf("S3 part 2 body = %q, want fghij", data)
			}
			if part2Calls == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("ETag", `"etag-2"`)
		default:
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/layers/layer-1/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "upload-1", PartSize: 5, TotalParts: 2})
		case "/v1/layers/layer-1/uploads/upload-1/presign-batch":
			presignCalls++
			var req struct {
				Parts []struct {
					PartNumber int `json:"part_number"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode presign request: %v", err)
			}
			presigned := make([]presignedPart, 0, len(req.Parts))
			for _, requested := range req.Parts {
				presigned = append(presigned, presignedPart{Number: requested.PartNumber, URL: fmt.Sprintf("%s/part/%d", s3.URL, requested.PartNumber), Size: 5})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": presigned})
		case "/v1/layers/layer-1/uploads/upload-1/complete":
			_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: "/repo/a.bin", StorageType: "s3"})
		case "/v1/layers/layer-1/uploads/upload-1/abort":
			abortCalls++
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	c := New(api.URL, "")
	entry, err := c.UploadFSLayerFile(context.Background(), "layer-1", "/repo/a.bin", strings.NewReader(payload), int64(len(payload)), 0, 0, false)
	if err != nil {
		t.Fatalf("UploadFSLayerFile: %v", err)
	}
	if entry.StorageType != "s3" || part1Calls != 1 || part2Calls != 2 || presignCalls != 2 || abortCalls != 0 {
		t.Fatalf("entry=%+v part1Calls=%d part2Calls=%d presignCalls=%d abortCalls=%d, want s3, 1, 2, 2, 0", entry, part1Calls, part2Calls, presignCalls, abortCalls)
	}
}

func TestUploadFSLayerFileFallsBackOnlyWhenInitiateIsUnavailable(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var initiateCalls, legacyCalls int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/layers/layer-1/uploads/initiate":
					initiateCalls++
					w.WriteHeader(status)
				case "/v1/layers/layer-1/objects":
					legacyCalls++
					data, _ := io.ReadAll(r.Body)
					if string(data) != "payload" {
						t.Errorf("legacy body = %q, want payload", data)
					}
					_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: "/repo/a.bin"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			c := New(srv.URL, "")
			entry, err := c.UploadFSLayerFile(context.Background(), "layer-1", "/repo/a.bin", strings.NewReader("payload"), 7, 0, 0, false)
			if err != nil {
				t.Fatalf("UploadFSLayerFile: %v", err)
			}
			if entry.Path != "/repo/a.bin" || initiateCalls != 1 || legacyCalls != 1 {
				t.Fatalf("entry=%+v initiateCalls=%d legacyCalls=%d", entry, initiateCalls, legacyCalls)
			}
		})
	}
}

func TestUploadFSLayerFileDoesNotFallbackOnInitiateFailure(t *testing.T) {
	var legacyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/layers/layer-1/uploads/initiate":
			http.Error(w, "unavailable", http.StatusInternalServerError)
		case "/v1/layers/layer-1/objects":
			legacyCalls++
			_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: "/repo/a.bin"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	_, err := c.UploadFSLayerFile(context.Background(), "layer-1", "/repo/a.bin", strings.NewReader("payload"), 7, 0, 0, false)
	if err == nil {
		t.Fatal("UploadFSLayerFile error = nil, want initiate failure")
	}
	if legacyCalls != 0 {
		t.Fatalf("legacy calls = %d, want 0", legacyCalls)
	}
}

func TestUploadFSLayerFileAbortsAfterPartFailureWithoutFallback(t *testing.T) {
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "S3 failure", http.StatusInternalServerError)
	}))
	defer s3.Close()

	var abortCalls, legacyCalls int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/layers/layer-1/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "upload-1", PartSize: 7, TotalParts: 1})
		case "/v1/layers/layer-1/uploads/upload-1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": []presignedPart{{Number: 1, URL: s3.URL, Size: 7}}})
		case "/v1/layers/layer-1/uploads/upload-1/abort":
			abortCalls++
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/v1/layers/layer-1/objects":
			legacyCalls++
			_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: "/repo/a.bin"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	c := New(api.URL, "")
	_, err := c.UploadFSLayerFile(context.Background(), "layer-1", "/repo/a.bin", strings.NewReader("payload"), 7, 0, 0, false)
	if err == nil {
		t.Fatal("UploadFSLayerFile error = nil, want part upload failure")
	}
	if abortCalls != 1 || legacyCalls != 0 {
		t.Fatalf("abortCalls=%d legacyCalls=%d, want 1 and 0", abortCalls, legacyCalls)
	}
}

func TestUploadFSLayerFileDoesNotFallbackOrAbortAfterCompleteStarts(t *testing.T) {
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"etag-1"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	var completeCalls, abortCalls, legacyCalls int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/layers/layer-1/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "upload-1", PartSize: 7, TotalParts: 1})
		case "/v1/layers/layer-1/uploads/upload-1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": []presignedPart{{Number: 1, URL: s3.URL, Size: 7}}})
		case "/v1/layers/layer-1/uploads/upload-1/complete":
			completeCalls++
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack complete response: %v", err)
				return
			}
			_ = conn.Close()
		case "/v1/layers/layer-1/uploads/upload-1/abort":
			abortCalls++
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/v1/layers/layer-1/objects":
			legacyCalls++
			_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: "/repo/a.bin"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	c := New(api.URL, "")
	_, err := c.UploadFSLayerFile(context.Background(), "layer-1", "/repo/a.bin", strings.NewReader("payload"), 7, 0, 0, false)
	if err == nil {
		t.Fatal("UploadFSLayerFile error = nil, want ambiguous complete result")
	}
	if completeCalls != 1 || abortCalls != 0 || legacyCalls != 0 {
		t.Fatalf("completeCalls=%d abortCalls=%d legacyCalls=%d, want 1, 0, 0", completeCalls, abortCalls, legacyCalls)
	}
}

func TestReadFSLayerFileRedirectStripsCredentialsAcrossHosts(t *testing.T) {
	var destinationAuthorization, destinationActor string
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destinationAuthorization = r.Header.Get("Authorization")
		destinationActor = r.Header.Get("X-Dat9-Actor")
		_, _ = io.WriteString(w, "payload")
	}))
	defer destination.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/layers/layer-1/objects" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("X-Dat9-Actor") != "actor-1" {
			t.Errorf("source credentials: Authorization=%q Actor=%q", r.Header.Get("Authorization"), r.Header.Get("X-Dat9-Actor"))
		}
		http.Redirect(w, r, destination.URL+"/object", http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	c := New(source.URL, "secret")
	c.SetActor("actor-1")
	data, err := c.ReadFSLayerFile(context.Background(), "layer-1", "/repo/a.bin", nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("data = %q, want payload", data)
	}
	if destinationAuthorization != "" || destinationActor != "" {
		t.Fatalf("destination credentials: Authorization=%q Actor=%q, want empty", destinationAuthorization, destinationActor)
	}
}

func TestCommitFSLayerReturnsConflictBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/commit" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(FSLayerCommit{
			Status:  "conflicted",
			LayerID: "layer-1",
			Conflicts: []FSLayerCommitConflict{{
				Path:         "/repo/a.txt",
				Reason:       "base revision changed",
				BaseRevision: 3,
				WantRevision: 2,
			}},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	commit, err := c.CommitFSLayer(context.Background(), "layer-1")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("CommitFSLayer err=%v, want ErrConflict", err)
	}
	if commit == nil || commit.Status != "conflicted" || len(commit.Conflicts) != 1 {
		t.Fatalf("commit=%+v, want conflict body", commit)
	}
	if commit.Conflicts[0].Path != "/repo/a.txt" || commit.Conflicts[0].WantRevision != 2 {
		t.Fatalf("conflict=%+v, want decoded conflict details", commit.Conflicts[0])
	}
}

func TestFSLayerFileOperationsPreserveSpaceOnlyPath(t *testing.T) {
	const path = "  "
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/uploads/initiate") {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("path"); got != path {
			t.Errorf("query path = %q, want %q", got, path)
		}
		switch r.Method {
		case http.MethodPost:
			_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: path})
		case http.MethodGet:
			if strings.HasSuffix(r.URL.Path, "/objects") {
				_, _ = w.Write([]byte("payload"))
				return
			}
			_ = json.NewEncoder(w).Encode(FSLayerEntry{Path: path})
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	entry, err := c.UploadFSLayerFile(context.Background(), "layer-1", path, strings.NewReader("payload"), 7, 0, 0, false)
	if err != nil {
		t.Fatalf("UploadFSLayerFile: %v", err)
	}
	if entry.Path != path {
		t.Errorf("uploaded path = %q, want %q", entry.Path, path)
	}
	data, err := c.ReadFSLayerFile(context.Background(), "layer-1", path, nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile: %v", err)
	}
	if string(data) != "payload" {
		t.Errorf("read data = %q, want payload", data)
	}
	entry, err = c.GetFSLayerEntry(context.Background(), "layer-1", path)
	if err != nil {
		t.Fatalf("GetFSLayerEntry: %v", err)
	}
	if entry.Path != path {
		t.Errorf("entry path = %q, want %q", entry.Path, path)
	}
}

func TestCommitFSLayerConflictBodyReadError(t *testing.T) {
	c := New("http://drive9.test", "")
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/layers/layer-1/commit" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusConflict,
			Status:     "409 Conflict",
			Header:     make(http.Header),
			Body:       failingReadCloser{},
			Request:    req,
		}, nil
	})}

	commit, err := c.CommitFSLayer(context.Background(), "layer-1")
	if commit != nil {
		t.Fatalf("commit = %+v, want nil", commit)
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("CommitFSLayer err type = %T, want *StatusError", err)
	}
	if statusErr.StatusCode != http.StatusConflict {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusConflict)
	}
	if !strings.Contains(statusErr.Message, "read fs layer commit conflict body") ||
		!strings.Contains(statusErr.Message, "body read failed") {
		t.Fatalf("status message = %q, want conflict body read failure", statusErr.Message)
	}
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("body read failed")
}

func (failingReadCloser) Close() error {
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
