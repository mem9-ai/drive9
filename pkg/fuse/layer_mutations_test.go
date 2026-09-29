package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerChmodPreservesObjectBackedContentWithoutShadow(t *testing.T) {
	for _, storage := range []string{"reference_only", "s3", "inline"} {
		t.Run(storage, func(t *testing.T) {
			entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", SizeBytes: 7, EntrySeq: 1}
			if storage == "reference_only" {
				entry.StorageRef = "s3://bucket/object"
			}
			if storage == "s3" {
				entry.StorageType = "s3"
			}
			if storage == "inline" {
				entry.Content = []byte("payload")
			}
			var posted client.FSLayerEntryRequest
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/layers/layer-1/entries" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
				}
				_ = json.NewEncoder(w).Encode(entry)
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			if err := fs.upsertLayerChmod(context.Background(), "/a", 0600); err != nil {
				t.Fatal(err)
			}
			if posted.Path != "/a" || posted.Mode != 0600 {
				t.Fatalf("wrong permission update: %+v", posted)
			}
			if storage == "inline" {
				if posted.Op != "upsert" || string(posted.Content) != "payload" {
					t.Fatalf("inline content lost: %+v", posted)
				}
			} else if posted.Op != "chmod" || len(posted.Content) != 0 {
				t.Fatalf("object-backed chmod replaced remote content: %+v", posted)
			}
		})
	}
}
