package fuse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerPublicationBaseRevision(t *testing.T) {
	for _, route := range []string{"direct", "queue", "shadow"} {
		for _, base := range []int64{-1, 0, 7} {
			t.Run(route+"/"+strconv.FormatInt(base, 10), func(t *testing.T) {
				got := int64(-99)
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/layers/layer-1/entries":
						var req client.FSLayerEntryRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
						}
						got = req.BaseRevision
					case "/v1/layers/layer-1/objects":
						got, _ = strconv.ParseInt(r.URL.Query().Get("base_revision"), 10, 64)
						_, _ = io.Copy(io.Discard, r.Body)
					default:
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer-1", Path: "/a", EntrySeq: 1})
				}))
				defer ts.Close()
				c := newTestClient(ts.URL)
				if route == "direct" {
					fs := NewDat9FS(c, &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
					if _, err := fs.upsertLayerFile(context.Background(), "/a", []byte("body"), base, 0600, true); err != nil {
						t.Fatal(err)
					}
				} else {
					_, shadows := newLayerShutdownState(t)
					if err := shadows.WriteFull("/a", []byte("body"), base); err != nil {
						t.Fatal(err)
					}
					cq := NewCommitQueue(c, shadows, nil, nil, 1, 8)
					_, err := cq.uploadLayerEntry(context.Background(), "layer-1", &CommitEntry{
						Path: "/a", BaseRev: base, Size: 4, Kind: PendingOverwrite, ShadowSpill: route == "shadow",
					}, "/a")
					if err != nil {
						t.Fatal(err)
					}
				}
				want := base
				if base < 0 {
					want = 0 // Layer-local files make no claim on a base revision.
				}
				if got != want {
					t.Fatalf("Layer base revision=%d want=%d", got, want)
				}
			})
		}
	}
}
