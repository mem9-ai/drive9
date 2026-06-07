package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCommitFSLayerReturnsConflictBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs-layers/layer-1/commit" {
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
