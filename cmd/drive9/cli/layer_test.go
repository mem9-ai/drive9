package cli

import (
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
