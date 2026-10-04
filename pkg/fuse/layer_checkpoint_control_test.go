package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerMountCheckpointFuncCreatesAndIndependentlyVerifies(t *testing.T) {
	var posts atomic.Int64
	var gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/checkpoints":
			posts.Add(1)
			var req client.FSLayerCheckpointRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode checkpoint request: %v", err)
			}
			if req.CheckpointID != "cp-1" {
				t.Errorf("checkpoint ID = %q", req.CheckpointID)
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{CheckpointID: "cp-1", LayerID: "layer-1", DurableSeq: 9})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp-1":
			gets.Add(1)
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{CheckpointID: "cp-1", LayerID: "layer-1", DurableSeq: 9})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	checkpoint := layerMountCheckpointFunc(client.New(server.URL, ""), &MountOptions{LayerRef: "layer-1"})
	if checkpoint == nil {
		t.Fatal("checkpoint callback = nil")
	}
	got, err := checkpoint(context.Background(), "cp-1")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got.CheckpointID != "cp-1" || got.LayerID != "layer-1" || got.DurableSeq != 9 {
		t.Fatalf("checkpoint = %+v", got)
	}
	if posts.Load() != 1 || gets.Load() != 1 {
		t.Fatalf("requests: posts=%d gets=%d", posts.Load(), gets.Load())
	}
}

func TestLayerMountCheckpointFuncReconcilesAmbiguousCreate(t *testing.T) {
	var gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost:
			http.Error(w, "ambiguous", http.StatusServiceUnavailable)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp-1":
			gets.Add(1)
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{CheckpointID: "cp-1", LayerID: "layer-1", DurableSeq: 5})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	checkpoint := layerMountCheckpointFunc(client.New(server.URL, ""), &MountOptions{LayerRef: "layer-1"})
	got, err := checkpoint(context.Background(), "cp-1")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got.DurableSeq != 5 || gets.Load() != 2 {
		t.Fatalf("checkpoint = %+v, gets=%d", got, gets.Load())
	}
}

func TestLayerMountCheckpointFuncRejectsVerificationMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		seq := int64(8)
		if r.Method == http.MethodPost {
			seq = 7
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{CheckpointID: "cp-1", LayerID: "layer-1", DurableSeq: seq})
	}))
	defer server.Close()

	checkpoint := layerMountCheckpointFunc(client.New(server.URL, ""), &MountOptions{LayerRef: "layer-1"})
	_, err := checkpoint(context.Background(), "cp-1")
	if err == nil || !strings.Contains(err.Error(), "independent read disagree") {
		t.Fatalf("checkpoint error = %v", err)
	}
}

func TestLayerMountCheckpointFuncRejectsUnsupportedMounts(t *testing.T) {
	tests := []struct {
		name string
		opts *MountOptions
	}{
		{name: "nil"},
		{name: "non-layer", opts: &MountOptions{}},
		{name: "read-only", opts: &MountOptions{LayerRef: "layer-1", ReadOnly: true}},
		{name: "checkpoint", opts: &MountOptions{LayerRef: "layer-1", CheckpointRef: "cp-0"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := layerMountCheckpointFunc(client.New("http://example.invalid", ""), tc.opts); got != nil {
				t.Fatalf("checkpoint callback = %v", got)
			}
		})
	}
}
