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

func TestLayerControlCLI_V36(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a"
	const childID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b"

	var gotCreate client.LayerCreateRequest
	var gotFork client.LayerForkRequest
	var gotForce string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers":
			if err := json.NewDecoder(r.Body).Decode(&gotCreate); err != nil {
				t.Fatalf("decode create: %v", err)
			}
			_ = json.NewEncoder(w).Encode(client.LayerInfo{
				LayerID:      layerID,
				BaseRootPath: gotCreate.BaseRootPath,
				Name:         gotCreate.Name,
				Tags:         gotCreate.Tags,
				State:        "active",
				SnapGen:      4,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers":
			_ = json.NewEncoder(w).Encode(client.LayerListPage{
				Layers: []client.LayerInfo{{
					LayerID:      layerID,
					BaseRootPath: "/repo",
					Name:         "auth",
					State:        "active",
					Fence:        0,
					Tags:         map[string]string{"task": "auth"},
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/tag:task=auth":
			_ = json.NewEncoder(w).Encode(client.LayerInfo{
				LayerID:      layerID,
				BaseRootPath: "/repo",
				Name:         "auth",
				State:        "active",
				SnapGen:      4,
				Fence:        3,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/fork":
			_ = json.NewDecoder(r.Body).Decode(&gotFork)
			_ = json.NewEncoder(w).Encode(client.LayerInfo{
				LayerID:       childID,
				ParentLayerID: layerID,
				BaseRootPath:  "/repo",
				State:         "active",
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/merge":
			_ = json.NewEncoder(w).Encode(client.LayerMergeResult{
				LayerID:      layerID,
				Status:       "merged",
				AppliedPaths: 5,
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/layers/"+childID:
			gotForce = r.URL.Query().Get("force")
			_ = json.NewEncoder(w).Encode(client.LayerDeleteResult{
				LayerID: childID,
				Status:  "deleted",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "sk-test")

	out, err := captureStdoutE(t, func() error {
		return Layer(c, []string{"create", "--name", "auth", "--ttl", "1h", "--tag", "task=auth", ":/repo"})
	})
	if err != nil {
		t.Fatalf("Layer create: %v", err)
	}
	if strings.TrimSpace(out) != layerID || gotCreate.BaseRootPath != "/repo" || gotCreate.TTLSeconds != 3600 {
		t.Fatalf("create out=%q req=%+v", out, gotCreate)
	}

	out, err = captureStdoutE(t, func() error { return Layer(c, []string{"ls"}) })
	if err != nil || !strings.Contains(out, layerID) {
		t.Fatalf("Layer ls: out=%q err=%v", out, err)
	}

	out, err = captureStdoutE(t, func() error { return Layer(c, []string{"inspect", "tag:task=auth"}) })
	if err != nil || !strings.Contains(out, "snap_gen") || !strings.Contains(out, "fence") || !strings.Contains(out, "/repo") {
		t.Fatalf("Layer inspect: out=%q err=%v", out, err)
	}

	out, err = captureStdoutE(t, func() error {
		return Layer(c, []string{"fork", "--name", "child", "--ttl", "300", layerID})
	})
	if err != nil || strings.TrimSpace(out) != childID || gotFork.TTLSeconds != 300 {
		t.Fatalf("Layer fork: out=%q req=%+v err=%v", out, gotFork, err)
	}

	out, err = captureStdoutE(t, func() error { return Layer(c, []string{"merge", layerID}) })
	if err != nil || !strings.Contains(out, "merged layer="+layerID+" applied_paths=5") {
		t.Fatalf("Layer merge: out=%q err=%v", out, err)
	}

	out, err = captureStdoutE(t, func() error { return Layer(c, []string{"rm", "--force", childID}) })
	if err != nil || gotForce != "true" || !strings.Contains(out, "deleted layer="+childID) {
		t.Fatalf("Layer rm: out=%q force=%q err=%v", out, gotForce, err)
	}

	if _, err := parseLayerTags([]string{"task=1", "task=2"}); err == nil || !strings.Contains(err.Error(), `duplicate layer tag "task"`) {
		t.Fatalf("duplicate tag error = %v", err)
	}
}

func TestFSDataCommandsRouteThroughUnifiedLayerView_V36(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a"
	localPath := filepath.Join(t.TempDir(), "layer.txt")
	if err := os.WriteFile(localPath, []byte("layer upload"), 0o644); err != nil {
		t.Fatal(err)
	}

	var gotPutBody []byte
	var gotFence string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 50000})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/auth":
			_ = json.NewEncoder(w).Encode(client.LayerInfo{LayerID: layerID, BaseRootPath: "/repo", State: "active"})
		case r.Method == http.MethodHead && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/layer.txt":
			http.NotFound(w, r)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/layer.txt":
			gotFence = r.Header.Get("X-FS-Layer-Fence")
			gotPutBody, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte(`{"status":"ok","revision":1}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "sk-test")
	c.Warm(t.Context())
	if err := Cp(c, []string{"--layer", "auth", "--fence", "3", localPath, ":/repo/layer.txt"}); err != nil {
		t.Fatalf("Cp --layer --fence: %v", err)
	}
	if string(gotPutBody) != "layer upload" || gotFence != "3" {
		t.Fatalf("put body=%q fence=%q", gotPutBody, gotFence)
	}
	if err := Find(c, []string{"--layer", layerID, ":/repo/"}); err == nil || !strings.Contains(err.Error(), "unsupported in layer view") {
		t.Fatalf("Find --layer err = %v, want unsupported in layer view", err)
	}
}
