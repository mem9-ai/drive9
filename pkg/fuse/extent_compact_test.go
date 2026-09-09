package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

func TestExtentFuseCompactReadsStagedCacheOnGet404(t *testing.T) {
	// JuiceFS compactChunk reads the chunk cache after writeback Finish.
	// Source objects may not be on S3 yet; GET 404 must not fail compact.
	var (
		gets     atomic.Int32
		compacts atomic.Int32
		putURL   string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.RawQuery == "slices=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 3, "generation": 1,
				"slices": []map[string]any{
					{"chunk": 0, "seq": 1, "file_off": 0, "len": 4, "block_key": "blocks/a", "block_off": 0, "block_len": 4},
					{"chunk": 0, "seq": 2, "file_off": 4, "len": 4, "block_key": "blocks/b", "block_off": 0, "block_len": 4},
				},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
			var req struct {
				Ranges []struct {
					FileOff int64 `json:"file_off"`
					Len     int64 `json:"len"`
				} `json:"ranges"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			blocks := make([]map[string]any, 0, len(req.Ranges))
			for _, rng := range req.Ranges {
				blocks = append(blocks, map[string]any{
					"file_off": rng.FileOff, "len": rng.Len,
					"block_key": "blocks/compacted", "put_url": putURL,
					"headers": map[string]string{},
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
		case r.Method == http.MethodPost && r.URL.RawQuery == "compact-slices=1":
			compacts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case strings.Contains(r.URL.Path, "missing") || r.URL.Path == "/obj-a" || r.URL.Path == "/obj-b":
			gets.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	putURL = ts.URL + "/put-block"
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:        client.New(ts.URL, ""),
		opts:          &MountOptions{ExtentPaths: []string{"*"}},
		extentCache:   newExtentReadCache(1 << 20),
		extentCompact: newExtentCompactScheduler(),
	}
	fs.extentCache.putBlock("blocks/a", 0, 4, []byte("aaaa"))
	fs.extentCache.putBlock("blocks/b", 0, 4, []byte("bbbb"))

	if err := fs.compactExtentChunk(context.Background(), "/t.bin", 0, 0); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if compacts.Load() < 1 {
		t.Fatal("JuiceFS compact must commit compacted slice metadata")
	}
	if gets.Load() != 0 {
		t.Fatalf("GET 404s=%d want 0 (compact must read staged chunk cache)", gets.Load())
	}
}

func TestExtentScheduleCompactOnFiveSlices(t *testing.T) {
	var slices atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.RawQuery == "slices=1" {
			slices.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "slices": []map[string]any{},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:        client.New(ts.URL, ""),
		opts:          &MountOptions{ExtentPaths: []string{"*"}},
		extentCompact: newExtentCompactScheduler(),
	}
	fs.scheduleExtentCompact("/t.bin", []client.ChunkRowCount{{Chunk: 0, Rows: datastore.ExtentCompactReadRows}})
	deadline := time.Now().Add(2 * time.Second)
	for slices.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if slices.Load() < 1 {
		t.Fatal("JuiceFS meta.Read compact must GetSlices when a chunk has >=5 slices")
	}
}

func TestExtentScheduleWriteCompactSkipsFiveSlices(t *testing.T) {
	var slices atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.RawQuery == "slices=1" {
			slices.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "slices": []map[string]any{},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:        client.New(ts.URL, ""),
		opts:          &MountOptions{ExtentPaths: []string{"*"}},
		extentCompact: newExtentCompactScheduler(),
	}
	fs.scheduleExtentWriteCompact("/t.bin", []client.ChunkRowCount{{Chunk: 0, Rows: datastore.ExtentCompactReadRows}})
	time.Sleep(50 * time.Millisecond)
	if slices.Load() != 0 {
		t.Fatal("JuiceFS meta.Write must not compact at 5 slices")
	}
}

func TestExtentScheduleWriteCompactOn99Slices(t *testing.T) {
	var slices atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.RawQuery == "slices=1" {
			slices.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "slices": []map[string]any{},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:        client.New(ts.URL, ""),
		opts:          &MountOptions{ExtentPaths: []string{"*"}},
		extentCompact: newExtentCompactScheduler(),
	}
	fs.scheduleExtentWriteCompact("/t.bin", []client.ChunkRowCount{{Chunk: 0, Rows: 99}})
	deadline := time.Now().Add(2 * time.Second)
	for slices.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if slices.Load() < 1 {
		t.Fatal("JuiceFS meta.Write compact must GetSlices when numSlices%100==99")
	}
}

func TestExtentFuseCompactSkipsWhenChunkCacheMisses(t *testing.T) {
	var compacts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "slices=1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 3, "generation": 1,
				"slices": []map[string]any{
					{"chunk": 0, "seq": 1, "file_off": 0, "len": 4, "block_key": "blocks/a", "block_off": 0, "block_len": 4},
					{"chunk": 0, "seq": 2, "file_off": 4, "len": 4, "block_key": "blocks/b", "block_off": 0, "block_len": 4},
				},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "compact-slices=1":
			compacts.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:        client.New(ts.URL, ""),
		opts:          &MountOptions{ExtentPaths: []string{"*"}},
		extentCache:   newExtentReadCache(1 << 20),
		extentCompact: newExtentCompactScheduler(),
	}
	if err := fs.compactExtentChunk(context.Background(), "/t.bin", 0, 0); err != nil {
		t.Fatalf("compact miss should no-op, err=%v", err)
	}
	if compacts.Load() != 0 {
		t.Fatal("JuiceFS compact must not swap metadata without chunk cache bytes")
	}
}
