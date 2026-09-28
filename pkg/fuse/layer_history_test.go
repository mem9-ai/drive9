package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerRestoreDownloadsOnlyRequiredContent(t *testing.T) {
	for _, final := range []string{"upsert", "chmod", "whiteout", "rename"} {
		t.Run(final, func(t *testing.T) {
			var entries []client.FSLayerEntry
			for i := range 20 {
				entries = append(entries, client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte(strconv.Itoa(i)), SizeBytes: int64(len(strconv.Itoa(i))), EntrySeq: int64(i + 1)})
			}
			wantFetches := 1
			if final != "upsert" {
				entries = append(entries, client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: final, Kind: "file", EntrySeq: 21, Mode: 0640, ContentText: "/b"})
			}
			if final == "whiteout" {
				wantFetches = 0
			}
			if final == "rename" {
				entries = append(entries, client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte("after"), SizeBytes: 5, EntrySeq: 22})
				wantFetches = 2
			}
			fetches := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/layers/layer-1/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
				case "/v1/layers/layer-1/entries":
					fetches++
					seq, _ := strconv.Atoi(r.URL.Query().Get("max_seq"))
					_ = json.NewEncoder(w).Encode(entries[seq-1])
				default:
					t.Errorf("unexpected request %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadows, idx, fs); err != nil {
				t.Fatal(err)
			}
			if fetches != wantFetches {
				t.Errorf("downloads=%d, want %d", fetches, wantFetches)
			}
			if final == "whiteout" {
				if idx.HasPending("/a") || shadows.Has("/a") {
					t.Fatal("whiteout left local data")
				}
				return
			}
			want := "19"
			if final == "rename" {
				want = "after"
				data, err := shadows.ReadAll("/b")
				if err != nil || string(data) != "19" {
					t.Fatalf("rename lost earlier snapshot: %q %v", data, err)
				}
			}
			data, err := shadows.ReadAll("/a")
			if err != nil || string(data) != want {
				t.Fatalf("latest bytes=%q, err=%v", data, err)
			}
			if final == "chmod" {
				meta, _ := idx.GetMeta("/a")
				if meta == nil || meta.Mode != 0640 {
					t.Fatalf("chmod lost: %+v", meta)
				}
			}
		})
	}
}

func TestLayerRestoreKeepsShortDirtyPayloadForConflictRecovery(t *testing.T) {
	idx, shadows := newLayerShutdownState(t)
	if err := shadows.WriteFull("/a", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.Put("/a", 5, PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/v1/layers/layer-1/diff" {
			t.Errorf("dirty payload was replaced via %s", r.URL)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", EntrySeq: 2}}})
	}))
	defer ts.Close()
	fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
	if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadows, idx, fs); err != nil {
		t.Fatal(err)
	}
	if meta, ok := idx.GetMeta("/a"); !ok || meta.LayerClean || meta.Size != 5 {
		t.Fatalf("dirty metadata changed: %+v", meta)
	}
	if data, err := shadows.ReadAll("/a"); err != nil || string(data) != "x" {
		t.Fatalf("dirty recovery bytes lost: %q %v", data, err)
	}
}
