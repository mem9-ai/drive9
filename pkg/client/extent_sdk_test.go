package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWriteStreamDefaultsToExtentAndReportsSummary(t *testing.T) {
	partSize := int64(64)
	part1 := bytes.Repeat([]byte("a"), int(partSize))
	part2 := bytes.Repeat([]byte("a"), int(partSize)) // duplicate -> skip_upload
	part3 := bytes.Repeat([]byte("b"), 19)
	payload := append(append([]byte{}, part1...), part2...)
	payload = append(payload, part3...)

	var gotInitiate map[string]any
	var gotComplete map[string]any
	var putCount atomic.Int32
	var preCompleteRan bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			_ = json.NewDecoder(r.Body).Decode(&gotInitiate)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:   "ext-seq-1",
				PartSize:   partSize,
				TotalParts: 3,
				ChecksumContract: checksumContract{
					Supported: []string{"SHA-256"},
					Required:  true,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/ext-seq-1/presign-batch":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []presignedPart{
					{Number: 1, URL: "http://" + r.Host + "/obj/1", Size: partSize},
					{Number: 2, SkipUpload: true, Size: partSize},
					{Number: 3, URL: "http://" + r.Host + "/obj/3", Size: int64(len(part3))},
				},
			})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/obj/"):
			putCount.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/ext-seq-1/complete":
			_ = json.NewDecoder(r.Body).Decode(&gotComplete)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "test-key")
	c.SetSmallFileThresholdForTests(32)

	summary, err := c.writeStreamConditionalWithOptionsAndSummary(
		context.Background(),
		"/dataset.bin",
		bytes.NewReader(payload),
		int64(len(payload)),
		nil,
		3,
		writeOptions{
			tags:        map[string]string{"tier": "warm"},
			description: "extent upload",
		},
		"",
		func() error {
			preCompleteRan = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummary: %v", err)
	}
	if gotInitiate["layout"] != string(ContentLayoutExtent) {
		t.Fatalf("initiate layout = %v, want %q", gotInitiate["layout"], ContentLayoutExtent)
	}
	if summary.Mode != "extent_v2" {
		t.Fatalf("summary.Mode = %q, want extent_v2", summary.Mode)
	}
	if summary.TotalParts != 3 || summary.UploadedParts != 2 {
		t.Fatalf("summary parts = %d/%d, want 2/3", summary.UploadedParts, summary.TotalParts)
	}
	if got := putCount.Load(); got != 2 {
		t.Fatalf("PUT count = %d, want 2 (skip_upload should omit duplicate)", got)
	}
	if !preCompleteRan {
		t.Fatal("expected preCompleteCheck to run before complete")
	}
	tags, _ := gotComplete["tags"].(map[string]any)
	if tags["tier"] != "warm" {
		t.Fatalf("complete tags = %v, want tier=warm", gotComplete["tags"])
	}
}

func TestWriteStreamDegradesOnLegacyClassicStickySingleAndAzure(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 128)

	t.Run("legacy server ignores layout=extent and returns classic v2 plan", func(t *testing.T) {
		var initiateCalls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				initiateCalls.Add(1)
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(uploadPlanV2{
					UploadID:   "classic-1",
					Key:        "tenant/classic-1",
					PartSize:   128,
					TotalParts: 1,
				})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/classic-1/presign-batch":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"parts": []presignedPart{{Number: 1, URL: "http://" + r.Host + "/s3/1", Size: 128}},
				})
			case r.Method == http.MethodPut && r.URL.Path == "/s3/1":
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", `"e1"`)
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/classic-1/complete":
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		c.SetSmallFileThresholdForTests(32)
		summary, err := c.WriteStreamWithSummary(context.Background(), "/legacy.bin", bytes.NewReader(payload), int64(len(payload)), nil)
		if err != nil {
			t.Fatalf("WriteStreamWithSummary: %v", err)
		}
		if summary.Mode != "multipart_v2" {
			t.Fatalf("summary.Mode = %q, want multipart_v2", summary.Mode)
		}
		if got := initiateCalls.Load(); got != 1 {
			t.Fatalf("initiateCalls = %d, want 1 (should reuse already-initiated plan)", got)
		}
	})

	t.Run("existing single target returns 409 layout mismatch then falls back to classic v2", func(t *testing.T) {
		var layouts []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				layout, _ := body["layout"].(string)
				layouts = append(layouts, layout)
				if layout == string(ContentLayoutExtent) {
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"error":"content layout mismatch: the file's layout is sticky for the life of the inode"}`))
					return
				}
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(uploadPlanV2{
					UploadID:   "classic-2",
					Key:        "tenant/classic-2",
					PartSize:   128,
					TotalParts: 1,
				})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/classic-2/presign-batch":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"parts": []presignedPart{{Number: 1, URL: "http://" + r.Host + "/s3/2", Size: 128}},
				})
			case r.Method == http.MethodPut && r.URL.Path == "/s3/2":
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", `"e2"`)
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/classic-2/complete":
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		c.SetSmallFileThresholdForTests(32)
		summary, err := c.WriteStreamWithSummary(context.Background(), "/single.bin", bytes.NewReader(payload), int64(len(payload)), nil)
		if err != nil {
			t.Fatalf("WriteStreamWithSummary: %v", err)
		}
		if summary.Mode != "multipart_v2" {
			t.Fatalf("summary.Mode = %q, want multipart_v2", summary.Mode)
		}
		if len(layouts) != 2 || layouts[0] != string(ContentLayoutExtent) || layouts[1] != "" {
			t.Fatalf("initiate layouts = %v, want [extent \"\"]", layouts)
		}
	})

	t.Run("Azure store lacking SAS length bounds aborts extent upload and falls back to classic v2", func(t *testing.T) {
		var aborted atomic.Bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body["layout"] == string(ContentLayoutExtent) {
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(uploadPlanV2{
						UploadID:         "az-ext",
						PartSize:         128,
						TotalParts:       1,
						ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
					})
					return
				}
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(uploadPlanV2{
					UploadID:   "az-classic",
					Key:        "tenant/az",
					PartSize:   128,
					TotalParts: 1,
				})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-ext/presign-batch":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"store cannot enforce a declared upload length at write time"}`))
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-ext/abort":
				aborted.Store(true)
				_, _ = w.Write([]byte(`{"status":"aborted"}`))
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-classic/presign-batch":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"parts": []presignedPart{{Number: 1, URL: "http://" + r.Host + "/az/1", Size: 128}},
				})
			case r.Method == http.MethodPut && r.URL.Path == "/az/1":
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", `"az1"`)
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-classic/complete":
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		c.SetSmallFileThresholdForTests(32)
		summary, err := c.WriteStreamWithSummary(context.Background(), "/az.bin", bytes.NewReader(payload), int64(len(payload)), nil)
		if err != nil {
			t.Fatalf("WriteStreamWithSummary: %v", err)
		}
		if summary.Mode != "multipart_v2" {
			t.Fatalf("summary.Mode = %q, want multipart_v2", summary.Mode)
		}
		if !aborted.Load() {
			t.Fatal("expected failed extent session to be aborted")
		}

		// Also verify current tidbcloud/fs masked 500 "internal storage error" shape on presign-batch falls back cleanly.
		var aborted500 atomic.Bool
		srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body["layout"] == string(ContentLayoutExtent) {
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(uploadPlanV2{
						UploadID:         "az-ext-500",
						PartSize:         128,
						TotalParts:       1,
						ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
					})
					return
				}
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(uploadPlanV2{
					UploadID:   "az-classic-500",
					Key:        "tenant/az500",
					PartSize:   128,
					TotalParts: 1,
				})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-ext-500/presign-batch":
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"internal storage error"}`))
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-ext-500/abort":
				aborted500.Store(true)
				_, _ = w.Write([]byte(`{"status":"aborted"}`))
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-classic-500/presign-batch":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"parts": []presignedPart{{Number: 1, URL: "http://" + r.Host + "/az/500", Size: 128}},
				})
			case r.Method == http.MethodPut && r.URL.Path == "/az/500":
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", `"az500"`)
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/az-classic-500/complete":
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv500.Close()
		c500 := New(srv500.URL, "")
		c500.SetSmallFileThresholdForTests(32)
		sum500, err := c500.WriteStreamWithSummary(context.Background(), "/az500.bin", bytes.NewReader(payload), int64(len(payload)), nil)
		if err != nil {
			t.Fatalf("WriteStreamWithSummary(masked 500): %v", err)
		}
		if sum500.Mode != "multipart_v2" || !aborted500.Load() {
			t.Fatalf("sum500.Mode=%q aborted=%v, want multipart_v2 and true", sum500.Mode, aborted500.Load())
		}
	})
}

func TestOverwriteStickyExtentViaWriteAndSmallWriteStream(t *testing.T) {
	var directPuts atomic.Int32
	var initiatedSizes []int64
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/sticky.bin":
			directPuts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal storage error"}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/sticky.bin":
			w.Header().Set("Content-Length", "1024")
			w.Header().Set("X-Dat9-Revision", "4")
			w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["layout"] != string(ContentLayoutExtent) {
				http.Error(w, "expected layout=extent", http.StatusBadRequest)
				return
			}
			sz := int64(body["total_size"].(float64))
			mu.Lock()
			initiatedSizes = append(initiatedSizes, sz)
			id := fmt.Sprintf("ow-%d", len(initiatedSizes))
			mu.Unlock()
			if sz == 0 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"total_size must be positive"}`))
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:         id,
				PartSize:         ExtentBlockSize,
				TotalParts:       1,
				ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/sticky.bin":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(ExtentWritePlan{UploadID: "ow-trunc", NewSize: 0})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []presignedPart{{Number: 1, URL: "http://" + r.Host + "/obj/ow", Size: 5}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/obj/ow":
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
			_, _ = w.Write([]byte(`{"status":"ok","revision":5}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.SetSmallFileThresholdForTests(DefaultSmallFileThreshold)

	rev, err := c.WriteCtxConditionalWithRevision(context.Background(), "/sticky.bin", []byte("hello"), -1)
	if err != nil {
		t.Fatalf("WriteCtxConditionalWithRevision: %v", err)
	}
	if rev != 5 {
		t.Fatalf("revision = %d, want 5", rev)
	}
	if got := directPuts.Load(); got != 1 {
		t.Fatalf("directPuts = %d, want 1", got)
	}

	// Non-ReaderAt small stream with WithTargetLayout(ContentLayoutExtent) skips direct PUT completely.
	summary, err := c.WriteStreamWithSummary(
		context.Background(),
		"/sticky.bin",
		io.NopCloser(strings.NewReader("world")),
		5,
		nil,
		WithTargetLayout(ContentLayoutExtent),
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummary(targetLayout=extent): %v", err)
	}
	if summary.Mode != "extent_v2" {
		t.Fatalf("summary.Mode = %q, want extent_v2", summary.Mode)
	}
	if got := directPuts.Load(); got != 1 {
		t.Fatalf("directPuts after targetLayout=extent = %d, want 1", got)
	}

	// 0-byte explicit extent upload.
	zeroSummary, err := c.WriteStreamWithSummary(
		context.Background(),
		"/sticky.bin",
		bytes.NewReader(nil),
		0,
		nil,
		WithContentLayout(ContentLayoutExtent),
	)
	if err != nil {
		t.Fatalf("WriteStreamWithSummary(0B extent): %v", err)
	}
	if zeroSummary.Mode != "extent_v2" || zeroSummary.TotalParts != 0 {
		t.Fatalf("zeroSummary = %+v, want extent_v2 with 0 parts", zeroSummary)
	}
}

func TestAppendStreamExtentSplitsChunkBoundaryWithoutReadingExistingFile(t *testing.T) {
	baseSize := int64(ExtentChunkSize - 3)
	delta := []byte("ABCDEFG")

	var patchBody map[string]any
	var completeBody map[string]any
	var putBodies = map[string]string{}
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/ext-append.bin":
			w.Header().Set("Content-Length", fmt.Sprint(baseSize))
			w.Header().Set("X-Dat9-Revision", "8")
			w.Header().Set("X-Dat9-Resource-ID", "res-append")
			w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/ext-append.bin":
			t.Errorf("unexpected full-file GET on extent append")
			http.Error(w, "unexpected GET", http.StatusInternalServerError)
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/ext-append.bin":
			if got := r.Header.Get("X-Dat9-Expected-Resource"); got != "res-append" {
				http.Error(w, "bad expected resource: "+got, http.StatusBadRequest)
				return
			}
			_ = json.NewDecoder(r.Body).Decode(&patchBody)
			dirty, _ := patchBody["dirty_blocks"].([]any)
			blocks := make([]ExtentWritePlanBlock, 0, len(dirty))
			for i, raw := range dirty {
				d := raw.(map[string]any)
				blocks = append(blocks, ExtentWritePlanBlock{
					Chunk:    uint32(d["chunk"].(float64)),
					Off:      uint32(d["off"].(float64)),
					Size:     int64(d["size"].(float64)),
					Checksum: d["checksum"].(string),
					URL:      fmt.Sprintf("http://%s/obj/app-%d", r.Host, i),
				})
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(ExtentWritePlan{
				UploadID: "app-ext-1",
				NewSize:  int64(patchBody["new_size"].(float64)),
				Blocks:   blocks,
			})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/obj/app-"):
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			putBodies[r.URL.Path] = string(b)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/app-ext-1/complete":
			_ = json.NewDecoder(r.Body).Decode(&completeBody)
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	err := c.AppendStream(context.Background(), "/ext-append.bin", bytes.NewReader(delta), int64(len(delta)), nil)
	if err != nil {
		t.Fatalf("AppendStream(extent): %v", err)
	}
	if putBodies["/obj/app-0"] != "ABC" || putBodies["/obj/app-1"] != "DEFG" {
		t.Fatalf("putBodies = %v, want ABC and DEFG", putBodies)
	}
	parts, _ := completeBody["parts"].([]any)
	if len(parts) != 2 {
		t.Fatalf("complete parts = %d, want 2", len(parts))
	}
}

func TestDownloadToFileWithSummaryParallelExtentBlocksAndHoles(t *testing.T) {
	const holeSize = 8 << 20 // >= downloadParallelThreshold (8 MiB), zero bytes over HTTP
	head := []byte("HEAD-DATA")
	tail := []byte("TAIL-DATA")
	totalSize := int64(len(head) + holeSize + len(tail))
	var expiredOnce atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/big-ext.bin":
			if r.Header.Get("X-Dat9-Extent-Read-Plan") != "1" {
				http.Error(w, "missing read plan header", http.StatusBadRequest)
				return
			}
			rng := r.Header.Get("Range")
			w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPartialContent)
			if rng == "" || strings.HasPrefix(rng, "bytes=0-") {
				_ = json.NewEncoder(w).Encode(ExtentReadPlan{
					Size:     totalSize,
					Revision: 3,
					Blocks: []ExtentReadPlanBlock{
						{FileOffset: 0, Length: int64(len(head)), BlockOffset: 0, URL: "http://" + r.Host + "/obj/head"},
						{FileOffset: int64(len(head)), Length: holeSize, Zero: true},
					},
					NextOffset: int64(len(head) + holeSize),
				})
				return
			}
			tailURL := "http://" + r.Host + "/obj/tail-expired"
			if expiredOnce.Load() {
				tailURL = "http://" + r.Host + "/obj/tail-fresh"
			}
			_ = json.NewEncoder(w).Encode(ExtentReadPlan{
				Size:     totalSize,
				Revision: 3,
				Blocks: []ExtentReadPlanBlock{
					{FileOffset: int64(len(head) + holeSize), Length: int64(len(tail)), BlockOffset: 0, URL: tailURL},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/obj/head":
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(head)
		case r.Method == http.MethodGet && r.URL.Path == "/obj/tail-expired":
			expiredOnce.Store(true)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("expired"))
		case r.Method == http.MethodGet && r.URL.Path == "/obj/tail-fresh":
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(tail)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	c.SetSmallFileThresholdForTests(1024)
	dst := filepath.Join(t.TempDir(), "downloaded.bin")

	summary, err := c.DownloadToFileWithSummary(context.Background(), "/big-ext.bin", dst, totalSize)
	if err != nil {
		t.Fatalf("DownloadToFileWithSummary: %v", err)
	}
	if summary.Mode != "parallel_extent_blocks" {
		t.Fatalf("summary.Mode = %q, want parallel_extent_blocks", summary.Mode)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if int64(len(got)) != totalSize {
		t.Fatalf("downloaded size = %d, want %d", len(got), totalSize)
	}
	if !bytes.Equal(got[:len(head)], head) {
		t.Fatalf("head mismatch: %q", got[:len(head)])
	}
	for i, b := range got[len(head) : len(head)+holeSize] {
		if b != 0 {
			t.Fatalf("non-zero byte at hole index %d: %d", i, b)
		}
	}
	if !bytes.Equal(got[len(head)+holeSize:], tail) {
		t.Fatalf("tail mismatch: %q", got[len(head)+holeSize:])
	}
}

func TestStreamWriterAutoRetriesExtentOnLayoutMismatch(t *testing.T) {
	var seenLayouts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			layout, _ := body["layout"].(string)
			seenLayouts = append(seenLayouts, layout)
			if layout == "" {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"content layout mismatch"}`))
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{
				UploadID:         "sw-ext-1",
				PartSize:         4,
				TotalParts:       2,
				ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/sw-ext-1/presign-batch":
			var body struct {
				Parts []struct {
					PartNumber int `json:"part_number"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []presignedPart{{Number: body.Parts[0].PartNumber, URL: "http://" + r.Host + "/obj/sw", Size: 4}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/obj/sw":
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/sw-ext-1/complete":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	sw := c.NewStreamWriter(context.Background(), "/sw-ext.bin", 8)
	if err := sw.WritePart(context.Background(), 1, []byte("1234")); err != nil {
		t.Fatalf("WritePart(1): %v", err)
	}
	if err := sw.Complete(context.Background(), 2, []byte("5678")); err != nil {
		t.Fatalf("Complete(2): %v", err)
	}
	if len(seenLayouts) != 2 || seenLayouts[0] != "" || seenLayouts[1] != string(ContentLayoutExtent) {
		t.Fatalf("seenLayouts = %v, want [\"\" extent]", seenLayouts)
	}

	// Verify StatMetadataCompatCtx preserves StorageType and ContentLayout on degraded HEAD fallback.
	headSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("stat") {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Length", "8")
		w.Header().Set("X-Dat9-Revision", "2")
		w.Header().Set("X-Dat9-Storage-Type", string(StorageTypeExtent))
		w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
		w.WriteHeader(http.StatusOK)
	}))
	defer headSrv.Close()

	meta, err := New(headSrv.URL, "").StatMetadataCompatCtx(context.Background(), "/sw-ext.bin")
	if err != nil {
		t.Fatalf("StatMetadataCompatCtx: %v", err)
	}
	if !meta.Degraded || meta.StorageType != StorageTypeExtent || meta.ContentLayout != ContentLayoutExtent {
		t.Fatalf("meta = %+v, want degraded extent metadata", meta)
	}

	// Explicit invalid layout fails validation immediately.
	_, err = c.WriteStreamWithSummary(context.Background(), "/bad.bin", bytes.NewReader([]byte("x")), 1, nil, WithContentLayout("invalid"))
	if err == nil || !strings.Contains(err.Error(), `layout must be "single" or "extent"`) {
		t.Fatalf("invalid layout err = %v, want layout validation error", err)
	}
}

func TestExtentReviewRegressions(t *testing.T) {
	t.Run("extentPlanReader rejects stalled continuations, revision drift, and unbounded 403 refresh", func(t *testing.T) {
		var planFetches atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/fs/stalled.bin":
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusPartialContent)
				if r.Header.Get("Range") == "" || strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
					_ = json.NewEncoder(w).Encode(ExtentReadPlan{
						Size:       8,
						Revision:   1,
						Blocks:     []ExtentReadPlanBlock{{FileOffset: 0, Length: 4, Zero: true}},
						NextOffset: 4,
					})
					return
				}
				// Stalled continuation: NextOffset repeats 4 with no blocks.
				_ = json.NewEncoder(w).Encode(ExtentReadPlan{
					Size:       8,
					Revision:   1,
					NextOffset: 4,
				})
			case "/v1/fs/drift.bin":
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusPartialContent)
				if r.Header.Get("Range") == "" || strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
					_ = json.NewEncoder(w).Encode(ExtentReadPlan{
						Size:       8,
						Revision:   1,
						Blocks:     []ExtentReadPlanBlock{{FileOffset: 0, Length: 4, Zero: true}},
						NextOffset: 4,
					})
					return
				}
				_ = json.NewEncoder(w).Encode(ExtentReadPlan{
					Size:     8,
					Revision: 2,
					Blocks:   []ExtentReadPlanBlock{{FileOffset: 4, Length: 4, Zero: true}},
				})
			case "/v1/fs/always403.bin":
				planFetches.Add(1)
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusPartialContent)
				_ = json.NewEncoder(w).Encode(ExtentReadPlan{
					Size:     4,
					Revision: 1,
					Blocks:   []ExtentReadPlanBlock{{FileOffset: 0, Length: 4, URL: "http://" + r.Host + "/obj/403"}},
				})
			case "/obj/403":
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("expired"))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		if _, err := c.ReadCtx(context.Background(), "/stalled.bin"); err == nil || !strings.Contains(err.Error(), "did not advance") {
			t.Fatalf("stalled continuation err = %v, want did not advance", err)
		}
		if _, err := c.ReadCtx(context.Background(), "/drift.bin"); err == nil || !strings.Contains(err.Error(), "changed during read") {
			t.Fatalf("revision drift err = %v, want changed during read", err)
		}
		if _, err := c.ReadCtx(context.Background(), "/always403.bin"); err == nil {
			t.Fatal("expected repeated 403 read to fail")
		}
		if got := planFetches.Load(); got != 2 {
			t.Fatalf("planFetches on repeated 403 = %d, want 2 (initial + 1 bounded refresh)", got)
		}
	})

	t.Run("zero-size fallback sends PATCH on positive expectedRevision and creates missing file on -1", func(t *testing.T) {
		var patchSawExpectedRev int64
		var createCalls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"total_size must be positive"}`))
			case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/nonempty.bin":
				var body struct {
					NewSize          int64 `json:"new_size"`
					ExpectedRevision int64 `json:"expected_revision"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				patchSawExpectedRev = body.ExpectedRevision
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(ExtentWritePlan{UploadID: "z-patch", NewSize: 0})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/z-patch/complete":
				_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/missing-zero.bin":
				w.WriteHeader(http.StatusNotFound)
			case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/missing-zero.bin" && r.URL.Query().Get("create") == "1":
				createCalls.Add(1)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"revision":1,"resource_id":"r1","content_layout":"extent"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		rev, err := c.writeExtentBytesConditional(context.Background(), "/nonempty.bin", nil, 7, nil, "", nil, nil)
		if err != nil {
			t.Fatalf("writeExtentBytesConditional(0B, rev=7): %v", err)
		}
		if patchSawExpectedRev != 7 || rev != 8 {
			t.Fatalf("patchSawExpectedRev=%d rev=%d, want 7 and 8", patchSawExpectedRev, rev)
		}

		rev2, err := c.writeExtentBytesConditional(context.Background(), "/missing-zero.bin", nil, -1, nil, "", nil, nil)
		if err != nil {
			t.Fatalf("writeExtentBytesConditional(0B, missing, rev=-1): %v", err)
		}
		if createCalls.Load() != 1 || rev2 != 1 {
			t.Fatalf("createCalls=%d rev2=%d, want 1 and 1", createCalls.Load(), rev2)
		}
	})

	t.Run("appendExtentStream honors current seeker offset and WriteCtx forwards completed revision sink", func(t *testing.T) {
		var appended string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/seek.bin":
				w.Header().Set("Content-Length", "4")
				w.Header().Set("X-Dat9-Revision", "3")
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/seek.bin":
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(ExtentWritePlan{
					UploadID: "seek-app",
					NewSize:  8,
					Blocks:   []ExtentWritePlanBlock{{Chunk: 0, Off: 4, Size: 4, URL: "http://" + r.Host + "/obj/seek"}},
				})
			case r.Method == http.MethodPut && r.URL.Path == "/obj/seek":
				b, _ := io.ReadAll(r.Body)
				appended = string(b)
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/seek-app/complete":
				_, _ = w.Write([]byte(`{"status":"ok","revision":4}`))
			case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/sink.bin":
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"content layout mismatch"}`))
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/sink.bin":
				w.Header().Set("Content-Length", "10")
				w.Header().Set("X-Dat9-Revision", "10")
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(uploadPlanV2{
					UploadID:         "sink-up",
					PartSize:         ExtentBlockSize,
					TotalParts:       1,
					ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
				})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/sink-up/presign-batch":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"parts": []presignedPart{{Number: 1, SkipUpload: true, Size: 2}},
				})
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/sink-up/complete":
				_, _ = w.Write([]byte(`{"status":"ok","revision":11}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")
		br := bytes.NewReader([]byte("prefixTAIL"))
		if _, err := br.Seek(6, io.SeekStart); err != nil {
			t.Fatalf("Seek: %v", err)
		}
		if err := c.AppendStream(context.Background(), "/seek.bin", br, 4, nil); err != nil {
			t.Fatalf("AppendStream: %v", err)
		}
		if appended != "TAIL" {
			t.Fatalf("appended = %q, want TAIL", appended)
		}

		var sinkRev int64
		sctx := WithCompletedRevisionSink(context.Background(), &sinkRev)
		if err := c.WriteCtx(sctx, "/sink.bin", []byte("hi")); err != nil {
			t.Fatalf("WriteCtx(sink fallback): %v", err)
		}
		if sinkRev != 11 {
			t.Fatalf("sinkRev = %d, want 11", sinkRev)
		}

		// Also verify TruncateExtentConditional enforces context expected resource on same-size HEAD.
		rctx := WithExpectedResource(context.Background(), "expected-other")
		_, err := c.TruncateExtentConditional(rctx, "/seek.bin", nil, 4)
		if err != nil {
			// /seek.bin HEAD didn't set X-Dat9-Resource-ID above; test explicit mismatch on a server that returns one.
			_ = err
		}
		resSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "4")
			w.Header().Set("X-Dat9-Revision", "3")
			w.Header().Set("X-Dat9-Resource-ID", "actual-res")
			w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
			w.WriteHeader(http.StatusOK)
		}))
		defer resSrv.Close()
		_, err = New(resSrv.URL, "").TruncateExtentConditional(WithExpectedResource(context.Background(), "want-res"), "/x.bin", nil, 4)
		if !errors.Is(err, ErrResourceMismatch) {
			t.Fatalf("TruncateExtentConditional same-size resource mismatch err = %v, want ErrResourceMismatch", err)
		}
	})

	t.Run("Round 2 review guards: supplied stat resource mismatch, 0B single rejection, sink attribution, and header revision drift", func(t *testing.T) {
		var headCalls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"total_size must be positive"}`))
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/empty-single.bin":
				w.Header().Set("Content-Length", "0")
				w.Header().Set("X-Dat9-Revision", "4")
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutSingle))
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/empty-extent.bin":
				w.Header().Set("Content-Length", "0")
				w.Header().Set("X-Dat9-Revision", "6")
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/no-rev-commit.bin":
				headCalls.Add(1)
				w.Header().Set("Content-Length", "0")
				w.Header().Set("X-Dat9-Revision", "999")
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/no-rev-commit.bin":
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(ExtentWritePlan{UploadID: "nr-1", NewSize: 0})
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
				// Mirror tidbcloud/fs handleV2UploadComplete: empty parts[] is permitted
				// ONLY for extent sessions (e.g. nr-1 truncation), and rejected with 400
				// for non-extent sessions.
				var req struct {
					Parts []completePart `json:"parts"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				if len(req.Parts) == 0 && r.URL.Path != "/v2/uploads/nr-1/complete" {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":"parts must not be empty"}`))
					return
				}
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/reject-ext-create.bin":
				if r.URL.Query().Get("layout") == string(ContentLayoutExtent) {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"error":"scoped query rejected"}`))
					return
				}
				t.Errorf("unexpected classic ?create=1 fallback on rejected native extent create")
				w.WriteHeader(http.StatusInternalServerError)
			case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/setmeta-fail.bin" && r.URL.Query().Has("create"):
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"revision":1,"resource_id":"r1","content_layout":"extent"}`))
			case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/setmeta-fail.bin" && r.URL.Query().Has("setmeta"):
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"tags forbidden"}`))
			case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/setmeta-fail.bin":
				w.WriteHeader(http.StatusNotFound)
			case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/header-rev.bin":
				w.Header().Set("X-Dat9-Content-Layout", string(ContentLayoutExtent))
				if strings.HasPrefix(r.Header.Get("Range"), "bytes=4-") {
					w.Header().Set("X-Dat9-Revision", "6")
					w.WriteHeader(http.StatusPartialContent)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"size":   8,
						"blocks": []map[string]any{{"file_offset": 4, "length": 4, "zero": true}},
					})
					return
				}
				w.Header().Set("X-Dat9-Revision", "5")
				w.WriteHeader(http.StatusPartialContent)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"size":        8,
					"blocks":      []map[string]any{{"file_offset": 0, "length": 4, "zero": true}},
					"next_offset": 4,
				})
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		c := New(srv.URL, "")

		// 1. Supplied st conflicting with context expected resource must fail immediately.
		supplied := &StatResult{Size: 16, Revision: 2, ResourceID: "res-b", ContentLayout: ContentLayoutExtent}
		_, err := c.TruncateExtentConditional(WithExpectedResource(context.Background(), "res-a"), "/unused.bin", supplied, 16)
		if !errors.Is(err, ErrResourceMismatch) {
			t.Fatalf("TruncateExtentConditional(same-size supplied mismatch) err = %v, want ErrResourceMismatch", err)
		}
		_, err = c.TruncateExtentConditional(WithExpectedResource(context.Background(), "res-a"), "/unused.bin", supplied, 0)
		if !errors.Is(err, ErrResourceMismatch) {
			t.Fatalf("TruncateExtentConditional(resize supplied mismatch) err = %v, want ErrResourceMismatch", err)
		}

		// 2. 0B extent write fallback rejects an existing empty single-layout file, while allowing empty extent no-op.
		_, err = c.writeExtentBytesConditional(context.Background(), "/empty-single.bin", nil, -1, nil, "", nil, nil)
		var se *StatusError
		if !errors.As(err, &se) || se.StatusCode != http.StatusConflict {
			t.Fatalf("writeExtentBytesConditional(empty single) err = %v, want 409 StatusError", err)
		}
		var noopSink int64
		rev, err := c.writeExtentBytesConditional(WithCompletedRevisionSink(context.Background(), &noopSink), "/empty-extent.bin", nil, -1, nil, "", nil, nil)
		if err != nil || rev != 6 || noopSink != 0 {
			t.Fatalf("writeExtentBytesConditional(empty extent) rev=%d sink=%d err=%v, want rev=6 sink=0 nil", rev, noopSink, err)
		}

		// 3. 0B extent write fallback with positive expectedRevision leaves sink at 0 and skips post-commit StatCtx when commit omits revision.
		var commitSink int64
		rev, err = c.writeExtentBytesConditional(WithCompletedRevisionSink(context.Background(), &commitSink), "/no-rev-commit.bin", nil, 3, nil, "", nil, nil)
		if err != nil || rev != 0 || commitSink != 0 || headCalls.Load() != 0 {
			t.Fatalf("writeExtentBytesConditional(no-rev commit) rev=%d sink=%d headCalls=%d err=%v, want 0/0/0/nil", rev, commitSink, headCalls.Load(), err)
		}

		// 4. ReadStream populates initial plan Revision from X-Dat9-Revision header and catches drift on continuation.
		rc, err := c.ReadStream(context.Background(), "/header-rev.bin")
		if err != nil {
			t.Fatalf("ReadStream(header-rev): %v", err)
		}
		_, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr == nil || !strings.Contains(rerr.Error(), "extent file changed during read: revision 5 -> 6") {
			t.Fatalf("ReadAll(header-rev) err = %v, want revision 5 -> 6 drift error", rerr)
		}

		// 5. Unconditional 0B extent write with tags/description applies ?setmeta=1 (reporting partial outcome if setmeta fails);
		// conditional 0B rejects upfront; expectedRevision == 0 with expectedResource rejects before create;
		// rejected ?create=1&layout=extent does NOT create a classic single-layout placeholder.
		_, err = c.writeExtentBytesConditional(context.Background(), "/reject-ext-create.bin", nil, 0, nil, "", nil, nil)
		if !IsForbidden(err) {
			t.Fatalf("writeExtentBytesConditional(rejected extent create) err = %v, want 403 StatusError without classic fallback", err)
		}
		_, err = c.writeExtentBytesConditional(context.Background(), "/setmeta-fail.bin", nil, -1, map[string]string{"k": "v"}, "", nil, nil)
		if err == nil || !strings.Contains(err.Error(), "0-byte content written, tags/description not applied") {
			t.Fatalf("writeExtentBytesConditional(setmeta fail) err = %v, want partial-outcome message", err)
		}
		_, err = c.writeExtentBytesConditional(WithExpectedResource(context.Background(), "res-old"), "/create-zero.bin", nil, 0, nil, "", nil, nil)
		if !errors.Is(err, ErrResourceMismatch) {
			t.Fatalf("writeExtentBytesConditional(0B rev=0 with expectedResource) err = %v, want ErrResourceMismatch", err)
		}
		_, err = c.writeExtentBytesConditional(context.Background(), "/tagged-zero.bin", nil, 1, map[string]string{"k": "v"}, "", nil, nil)
		if err == nil || !strings.Contains(err.Error(), "conditional 0-byte extent writes do not accept tags or description") {
			t.Fatalf("writeExtentBytesConditional(conditional 0B with tags) err = %v, want explicit conditional error", err)
		}
		if cerr := c.completeUploadV2(context.Background(), "classic-up", []completePart{}, nil); cerr == nil || !strings.Contains(cerr.Error(), "parts must not be empty") {
			t.Fatalf("completeUploadV2(classic empty parts) err = %v, want parts must not be empty", cerr)
		}
	})

	t.Run("wire error classification contract pins status codes, machine codes, and server literals", func(t *testing.T) {
		if !isExtentLayoutMismatchError(&StatusError{StatusCode: http.StatusConflict, Code: extentErrCodeLayoutMismatch, Message: "reworded"}) {
			t.Fatal("expected machine code extent_layout_mismatch to match on 409")
		}
		if !isExtentLayoutMismatchError(&StatusError{StatusCode: http.StatusConflict, Message: "content layout mismatch: the file's layout is sticky for the life of the inode"}) {
			t.Fatal("expected server layout mismatch literal to match on 409")
		}
		if isExtentLayoutMismatchError(&StatusError{StatusCode: http.StatusBadRequest, Message: "content layout mismatch"}) {
			t.Fatal("must not classify non-409 as layout mismatch")
		}

		if !isExtentPresignUnsupportedError(&StatusError{StatusCode: http.StatusBadRequest, Code: extentErrCodePresignLengthUnsupported, Message: "reworded"}) {
			t.Fatal("expected machine code extent_presign_length_unsupported to match on 400")
		}
		if !isExtentPresignUnsupportedError(&StatusError{StatusCode: http.StatusBadRequest, Message: "store cannot enforce a declared upload length at write time"}) {
			t.Fatal("expected server presign length literal to match on 400")
		}
		if !isExtentPresignUnsupportedError(&StatusError{StatusCode: http.StatusInternalServerError, Message: "internal storage error"}) {
			t.Fatal("expected masked exact 500 internal storage error to match on presign-batch")
		}
		if isExtentPresignUnsupportedError(&StatusError{StatusCode: http.StatusInternalServerError, Message: "cannot enforce a declared upload length"}) {
			t.Fatal("must not classify non-exact 500 prose as presign unsupported")
		}

		if !isZeroSizeExtentInitiateError(&StatusError{StatusCode: http.StatusBadRequest, Code: extentErrCodeZeroSizeUpload, Message: "reworded"}) {
			t.Fatal("expected machine code zero_size_upload_unsupported to match on 400")
		}
		if !isZeroSizeExtentInitiateError(&StatusError{StatusCode: http.StatusBadRequest, Message: "total_size must be positive"}) {
			t.Fatal("expected total_size must be positive literal to match on 400")
		}

		if !mayBeExtentDirectWriteRejection(&StatusError{StatusCode: http.StatusInternalServerError, Message: "internal storage error"}, -1) {
			t.Fatal("expected exact 500 internal storage error to trigger stat check on overwrite")
		}
		if mayBeExtentDirectWriteRejection(&StatusError{StatusCode: http.StatusInternalServerError, Message: "internal storage error: disk full"}, -1) {
			t.Fatal("must not match extended 500 error prose")
		}
		if mayBeExtentDirectWriteRejection(&StatusError{StatusCode: http.StatusInternalServerError, Message: "internal storage error"}, 0) {
			t.Fatal("must not match 500 when expectedRevision == 0")
		}
	})
}
