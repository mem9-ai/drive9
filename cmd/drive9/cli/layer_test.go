package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestUploadReaderToLayerUsesServerInlineThreshold(t *testing.T) {
	var statusCalls, entryCalls, initiateCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/status":
			statusCalls++
			_, _ = io.WriteString(w, `{"inline_threshold":4}`)
		case "HEAD /v1/fs/repo/a.bin":
			http.NotFound(w, r)
		case "POST /v1/layers/layer-1/entries":
			entryCalls++
			var req client.FSLayerEntryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode layer entry: %v", err)
			}
			if string(req.Content) != "data" {
				t.Errorf("inline content = %q, want data", req.Content)
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{Path: req.Path})
		case "POST /v1/layers/layer-1/uploads/initiate":
			initiateCalls++
			http.Error(w, "unexpected direct upload", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.Warm(context.Background())
	err := uploadReaderToLayer(context.Background(), c, "layer-1", "/repo/a.bin", strings.NewReader("data"), 4, 0o644, true)
	if err != nil {
		t.Fatalf("uploadReaderToLayer: %v", err)
	}
	if statusCalls != 1 || entryCalls != 1 || initiateCalls != 0 {
		t.Fatalf("statusCalls=%d entryCalls=%d initiateCalls=%d, want 1, 1, 0", statusCalls, entryCalls, initiateCalls)
	}
}

func TestUploadReaderToLayerUsesDirectUploadAboveServerThreshold(t *testing.T) {
	var uploaded string
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read S3 body: %v", err)
		}
		uploaded = string(data)
		w.Header().Set("ETag", `"etag-1"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	var entryCalls, initiateCalls int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/status":
			_, _ = io.WriteString(w, `{"inline_threshold":3}`)
		case "HEAD /v1/fs/repo/a.bin":
			http.NotFound(w, r)
		case "POST /v1/layers/layer-1/entries":
			entryCalls++
			http.Error(w, "unexpected inline upload", http.StatusInternalServerError)
		case "POST /v1/layers/layer-1/uploads/initiate":
			initiateCalls++
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"upload_id":"upload-1","part_size":4,"total_parts":1}`)
		case "POST /v1/layers/layer-1/uploads/upload-1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": []map[string]any{{
				"number": 1,
				"url":    s3.URL,
				"size":   4,
			}}})
		case "POST /v1/layers/layer-1/uploads/upload-1/complete":
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{Path: "/repo/a.bin", StorageType: "s3"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	c := client.New(api.URL, "")
	c.Warm(context.Background())
	err := uploadReaderToLayer(context.Background(), c, "layer-1", "/repo/a.bin", strings.NewReader("data"), 4, 0o644, true)
	if err != nil {
		t.Fatalf("uploadReaderToLayer: %v", err)
	}
	if uploaded != "data" || initiateCalls != 1 || entryCalls != 0 {
		t.Fatalf("uploaded=%q initiateCalls=%d entryCalls=%d, want data, 1, 0", uploaded, initiateCalls, entryCalls)
	}
}

func TestUploadReaderToLayerDoesNotFetchStatusOnHotPath(t *testing.T) {
	var statusCalls, entryCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/status":
			statusCalls++
			_, _ = io.WriteString(w, `{"inline_threshold":1}`)
		case "HEAD /v1/fs/repo/a.bin":
			http.NotFound(w, r)
		case "POST /v1/layers/layer-1/entries":
			entryCalls++
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{Path: "/repo/a.bin"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := uploadReaderToLayer(context.Background(), c, "layer-1", "/repo/a.bin", strings.NewReader("data"), 4, 0, false); err != nil {
		t.Fatalf("uploadReaderToLayer: %v", err)
	}
	if statusCalls != 0 || entryCalls != 1 {
		t.Fatalf("statusCalls=%d entryCalls=%d, want 0 and 1", statusCalls, entryCalls)
	}
}

func TestUploadReaderToLayerCapsInlineEntrySize(t *testing.T) {
	const size = int64(100 << 20)
	var entryCalls, initiateCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "HEAD /v1/fs/repo/large.bin":
			http.NotFound(w, r)
		case "POST /v1/layers/layer-1/entries":
			entryCalls++
			http.Error(w, "unexpected inline upload", http.StatusInternalServerError)
		case "POST /v1/layers/layer-1/uploads/initiate":
			initiateCalls++
			http.Error(w, "direct upload selected", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(128 << 20)
	err := uploadReaderToLayer(context.Background(), c, "layer-1", "/repo/large.bin", strings.NewReader("not consumed"), size, 0, false)
	if err == nil || !strings.Contains(err.Error(), "direct upload selected") {
		t.Fatalf("uploadReaderToLayer error = %v, want direct upload marker", err)
	}
	if entryCalls != 0 || initiateCalls != 1 {
		t.Fatalf("entryCalls=%d initiateCalls=%d, want 0 and 1", entryCalls, initiateCalls)
	}
}

func TestLayerCreatePrintsLayerID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers" {
			http.NotFound(w, r)
			return
		}
		var req client.FSLayerCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.BaseRootPath != "/repo" || req.Name != "task" || req.DurabilityMode != "restore-safe" {
			t.Fatalf("request = %+v", req)
		}
		if req.Tags["task"] != "auth" || req.Tags["env"] != "dev" {
			t.Fatalf("request tags = %+v", req.Tags)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(client.FSLayer{
			LayerID:        "layer-1",
			BaseRootPath:   "/repo/",
			Name:           "task",
			Tags:           map[string]string{"task": "auth", "env": "dev"},
			State:          "active",
			DurabilityMode: "restore-safe",
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error {
		return Layer(c, []string{"create", "--name", "task", "--durability", "restore-safe", "--tag", "task=auth", "--tag", "env=dev", ":/repo"})
	})
	if err != nil {
		t.Fatalf("Layer create: %v", err)
	}
	if strings.TrimSpace(out) != "layer-1" {
		t.Fatalf("stdout = %q, want layer-1", out)
	}
}

func TestCpLayerLocalUploadUsesFSLayerEntry(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "layer.txt")
	if err := os.WriteFile(localPath, []byte("layer upload"), 0o640); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	var got client.FSLayerEntryRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "HEAD /v1/fs/repo/layer.txt":
			http.NotFound(w, r)
		case "POST /v1/layers/layer-1/entries":
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatalf("decode entry request: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:   "layer-1",
				Path:      got.Path,
				Op:        got.Op,
				Kind:      got.Kind,
				SizeBytes: got.SizeBytes,
				Mode:      got.Mode,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := Cp(c, []string{"--layer", "layer-1", localPath, ":/repo/layer.txt"}); err != nil {
		t.Fatalf("Cp --layer: %v", err)
	}
	if got.Path != "/repo/layer.txt" || got.Op != "upsert" || got.Kind != "file" || string(got.Content) != "layer upload" || got.Mode != 0o640 {
		t.Fatalf("entry request = %+v", got)
	}
}

func TestSearchCommandsPassLayerParam(t *testing.T) {
	var grepLayer, findLayer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/" && r.URL.Query().Has("grep"):
			grepLayer = r.URL.Query().Get("layer")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[]`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/" && r.URL.Query().Has("find"):
			findLayer = r.URL.Query().Get("layer")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := Grep(c, []string{"--layer", "task=search", "needle", ":/repo/"}); err != nil {
		t.Fatalf("Grep --layer: %v", err)
	}
	if err := Find(c, []string{"--layer", "task=search", ":/repo/"}); err != nil {
		t.Fatalf("Find --layer: %v", err)
	}
	if grepLayer != "task=search" || findLayer != "task=search" {
		t.Fatalf("grepLayer=%q findLayer=%q, want task=search", grepLayer, findLayer)
	}
}

func TestLayerCreateRejectsDuplicateTag(t *testing.T) {
	c := client.New("http://127.0.0.1", "")
	err := Layer(c, []string{"create", "--tag", "task=auth", "--tag", "task=review", ":/repo"})
	if err == nil || !strings.Contains(err.Error(), `duplicate layer tag "task"`) {
		t.Fatalf("Layer create duplicate tag err=%v, want duplicate tag error", err)
	}
}

func TestLayerCommitPrintsResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/task=auth/commit" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(client.FSLayerCommit{
			Status:  "committed",
			LayerID: "layer-1",
			Applied: 3,
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"commit", "task=auth"}) })
	if err != nil {
		t.Fatalf("Layer commit: %v", err)
	}
	if !strings.Contains(out, "committed") || !strings.Contains(out, "layer=layer-1") || !strings.Contains(out, "applied=3") {
		t.Fatalf("stdout = %q, want commit result", out)
	}
}

func TestLayerDiffTextOutput(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/layers/layer-1/diff" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []client.FSLayerEntry{{
				EntrySeq: 1,
				Op:       "upsert",
				Kind:     "file",
				Mode:     0o644,
				Path:     "/repo/a.txt",
			}},
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"diff", "layer-1"}) })
	if err != nil {
		t.Fatalf("Layer diff: %v", err)
	}
	if !strings.Contains(out, "1") || !strings.Contains(out, "upsert") || !strings.Contains(out, "/repo/a.txt") {
		t.Fatalf("stdout = %q, want diff entry", out)
	}
}
