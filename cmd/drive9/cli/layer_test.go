package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestLayerCreatePrintsLayerID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs-layers" {
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

func TestLayerCommitPrintsResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs-layers/task=auth/commit" {
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
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs-layers/layer-1/diff" {
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
