package git

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestRemoteOverlayRestoresAllPagesOrKeepsExistingMirror(t *testing.T) {
	for _, failLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("last_page_failure_%t", failLast), func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Query().Get("after") == "" {
					entries := make([]client.GitOverlayEntry, 200)
					for i := range entries {
						entries[i] = client.GitOverlayEntry{Path: fmt.Sprintf("f%03d", i), Content: []byte("first")}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries, "next_after": "f199"})
					return
				}
				if r.URL.Query().Get("after") != "f199" {
					t.Error("wrong continuation")
				}
				if failLast {
					w.WriteHeader(500)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.GitOverlayEntry{
					{Path: "last", Content: []byte("last-page")}, {Path: "removed", Op: "delete", Kind: "file"},
				}})
			}))
			defer ts.Close()
			g := &Git{cl: client.New(ts.URL, "test")}
			workspace := &workspace{id: "ws", overlay: map[string]overlayEntry{"existing": {Content: []byte("keep")}}}
			g.loadRemoteOverlay(context.Background(), workspace)
			if calls.Load() != 2 {
				t.Fatalf("calls=%d", calls.Load())
			}
			if failLast {
				if len(workspace.overlay) != 1 || string(workspace.overlay["existing"].Content) != "keep" {
					t.Fatal("partial list replaced existing mirror")
				}
				return
			}
			if len(workspace.overlay) != 202 {
				t.Fatalf("restored %d entries", len(workspace.overlay))
			}
			for i := 0; i < 200; i++ {
				if string(workspace.overlay[fmt.Sprintf("f%03d", i)].Content) != "first" {
					t.Fatalf("missing entry %d", i)
				}
			}
			if string(workspace.overlay["last"].Content) != "last-page" || workspace.overlay["removed"].Op != "delete" {
				t.Fatal("last-page content or whiteout missing")
			}
		})
	}
}
