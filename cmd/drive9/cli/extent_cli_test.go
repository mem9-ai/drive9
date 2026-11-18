package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestCpExtentLargeUploadAndExplicitLayoutFlag(t *testing.T) {
	dir := t.TempDir()
	smallLocal := filepath.Join(dir, "small.bin")
	largeLocal := filepath.Join(dir, "large.bin")
	if err := os.WriteFile(smallLocal, []byte("small-extent-payload"), 0o644); err != nil {
		t.Fatalf("WriteFile(small): %v", err)
	}
	largeBytes := bytes.Repeat([]byte("L"), 256)
	if err := os.WriteFile(largeLocal, largeBytes, 0o644); err != nil {
		t.Fatalf("WriteFile(large): %v", err)
	}

	var seenInitiates []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			seenInitiates = append(seenInitiates, body)
			if body["layout"] == string(client.ContentLayoutExtent) {
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"upload_id":   fmt.Sprintf("ext-%d", len(seenInitiates)),
					"part_size":   128,
					"total_parts": (int(body["total_size"].(float64)) + 127) / 128,
					"checksum_contract": map[string]any{
						"supported": []string{"SHA-256"},
						"required":  true,
					},
				})
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   fmt.Sprintf("classic-%d", len(seenInitiates)),
				"key":         "tenant/classic",
				"part_size":   256,
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			var req struct {
				Parts      []any `json:"parts"`
				PartNums   []int `json:"part_numbers"`
				StartPart  int   `json:"start_part"`
				TotalCount int   `json:"count"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			n := len(req.Parts)
			if n == 0 {
				n = len(req.PartNums)
			}
			if n == 0 {
				n = 1
			}
			parts := make([]map[string]any, 0, n)
			for i := 1; i <= n; i++ {
				parts = append(parts, map[string]any{
					"number": i,
					"url":    "http://" + r.Host + "/obj/part",
					"size":   128,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": parts})
		case r.Method == http.MethodPut && r.URL.Path == "/obj/part":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
			_, _ = w.Write([]byte(`{"status":"ok","revision":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(64)

	// 1. Default large upload selects layout=extent.
	if err := Cp(c, []string{largeLocal, ":/default-large.bin"}); err != nil {
		t.Fatalf("Cp(default large): %v", err)
	}
	if seenInitiates[0]["layout"] != string(client.ContentLayoutExtent) {
		t.Fatalf("default large initiate layout = %v, want extent", seenInitiates[0]["layout"])
	}

	// 2. Small file with --layout extent forces extent v2 instead of direct PUT.
	if err := Cp(c, []string{"--layout", "extent", smallLocal, ":/forced-extent.bin"}); err != nil {
		t.Fatalf("Cp(--layout extent small): %v", err)
	}
	if seenInitiates[1]["layout"] != string(client.ContentLayoutExtent) {
		t.Fatalf("forced extent initiate layout = %v, want extent", seenInitiates[1]["layout"])
	}

	// 3. Large file with --layout=single forces classic multipart v2.
	if err := Cp(c, []string{"--layout=single", largeLocal, ":/forced-single.bin"}); err != nil {
		t.Fatalf("Cp(--layout=single large): %v", err)
	}
	if _, hasLayout := seenInitiates[2]["layout"]; hasLayout {
		t.Fatalf("forced single initiate unexpectedly sent layout=%v", seenInitiates[2]["layout"])
	}

	// 4. Flag validation checks.
	for _, badArgs := range [][]string{
		{"--layout", "bogus", smallLocal, ":/x.bin"},
		{"--layout", "extent", ":/x.bin", filepath.Join(dir, "out.bin")},
		{"-r", "--layout", "extent", dir, ":/dir"},
		{"--append", "--layout", "extent", smallLocal, ":/x.bin"},
		{"--resume", "--layout", "extent", smallLocal, ":/x.bin"},
	} {
		if err := Cp(c, badArgs); err == nil {
			t.Fatalf("Cp(%v) succeeded, want validation error", badArgs)
		}
	}
}

func TestCpForwardsObservedTargetLayoutWithoutSpeculativeMismatchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	smallLocal := filepath.Join(dir, "small.txt")
	largeLocal := filepath.Join(dir, "large.bin")
	if err := os.WriteFile(smallLocal, []byte("tiny"), 0o644); err != nil {
		t.Fatalf("WriteFile(small): %v", err)
	}
	if err := os.WriteFile(largeLocal, bytes.Repeat([]byte("M"), 256), 0o644); err != nil {
		t.Fatalf("WriteFile(large): %v", err)
	}

	var directPuts atomic.Int32
	var conflict409s atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/existing-extent.bin":
			w.Header().Set("Content-Length", "1024")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "3")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/existing-single.bin":
			w.Header().Set("Content-Length", "1024")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "3")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutSingle))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			directPuts.Add(1)
			http.Error(w, "unexpected direct PUT on sticky extent target", http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			path, _ := body["path"].(string)
			layout, _ := body["layout"].(string)
			if path == "/existing-single.bin" && layout == string(client.ContentLayoutExtent) {
				conflict409s.Add(1)
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"content layout mismatch"}`))
				return
			}
			w.WriteHeader(http.StatusAccepted)
			if layout == string(client.ContentLayoutExtent) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"upload_id":   "ext-ow",
					"part_size":   1024,
					"total_parts": 1,
					"checksum_contract": map[string]any{
						"supported": []string{"SHA-256"},
						"required":  true,
					},
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"upload_id":   "single-ow",
					"key":         "tenant/single",
					"part_size":   1024,
					"total_parts": 1,
				})
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{"number": 1, "url": "http://" + r.Host + "/obj/1", "size": 4}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/obj/1":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"e1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
			_, _ = w.Write([]byte(`{"status":"ok","revision":4}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(64)

	// Small local file overwriting existing extent file -> zero failed direct PUTs!
	if err := Cp(c, []string{smallLocal, ":/existing-extent.bin"}); err != nil {
		t.Fatalf("Cp(small -> existing extent): %v", err)
	}
	if got := directPuts.Load(); got != 0 {
		t.Fatalf("directPuts = %d, want 0", got)
	}

	// Large local file overwriting existing single file -> zero 409 round-trips!
	if err := Cp(c, []string{largeLocal, ":/existing-single.bin"}); err != nil {
		t.Fatalf("Cp(large -> existing single): %v", err)
	}
	if got := conflict409s.Load(); got != 0 {
		t.Fatalf("conflict409s = %d, want 0", got)
	}
}

func TestCliExtentDownloadCatAppendAndStat(t *testing.T) {
	sparse := make([]byte, 16)
	copy(sparse[:4], "ABCD")
	copy(sparse[12:], "WXYZ")
	var appended string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/ext.bin":
			w.Header().Set("Content-Length", "16")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Storage-Type", string(client.StorageTypeExtent))
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/ext.bin" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"size":           16,
				"isdir":          false,
				"revision":       5,
				"content_type":   "application/octet-stream",
				"storage_type":   string(client.StorageTypeExtent),
				"content_layout": string(client.ContentLayoutExtent),
				"tags":           map[string]string{},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/ext.bin":
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPartialContent)
			if r.Header.Get("Range") == "bytes=2-13" {
				_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
					Size: 16, Revision: 5,
					Blocks: []client.ExtentReadPlanBlock{
						{FileOffset: 2, Length: 2, BlockOffset: 2, URL: "http://" + r.Host + "/obj/a"},
						{FileOffset: 4, Length: 8, Zero: true},
						{FileOffset: 12, Length: 2, BlockOffset: 0, URL: "http://" + r.Host + "/obj/b"},
					},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Size: 16, Revision: 5,
				Blocks: []client.ExtentReadPlanBlock{
					{FileOffset: 0, Length: 4, BlockOffset: 0, URL: "http://" + r.Host + "/obj/a"},
					{FileOffset: 4, Length: 8, Zero: true},
					{FileOffset: 12, Length: 4, BlockOffset: 0, URL: "http://" + r.Host + "/obj/b"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/obj/a":
			if r.Header.Get("Range") == "bytes=2-3" {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("CD"))
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("ABCD"))
		case r.Method == http.MethodGet && r.URL.Path == "/obj/b":
			if r.Header.Get("Range") == "bytes=0-1" {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("WX"))
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("WXYZ"))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/ext.bin":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.ExtentWritePlan{
				UploadID: "cli-app-1",
				NewSize:  20,
				Blocks: []client.ExtentWritePlanBlock{
					{Chunk: 0, Off: 16, Size: 4, URL: "http://" + r.Host + "/obj/app"},
				},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/obj/app":
			b, _ := io.ReadAll(r.Body)
			appended = string(b)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/cli-app-1/complete":
			_, _ = w.Write([]byte(`{"status":"ok","revision":6}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(8)

	// 1. Download via `drive9 fs cp :/ext.bin <local>`.
	dlPath := filepath.Join(t.TempDir(), "downloaded.bin")
	if err := Cp(c, []string{":/ext.bin", dlPath}); err != nil {
		t.Fatalf("Cp(download extent): %v", err)
	}
	gotDL, err := os.ReadFile(dlPath)
	if err != nil {
		t.Fatalf("ReadFile(downloaded): %v", err)
	}
	if !bytes.Equal(gotDL, sparse) {
		t.Fatalf("downloaded = %q, want %q", gotDL, sparse)
	}

	// 2. Full & ranged `drive9 fs cat`.
	var catOut bytes.Buffer
	if err := catWithWriter(c, []string{":/ext.bin"}, &catOut); err != nil {
		t.Fatalf("cat(full): %v", err)
	}
	if !bytes.Equal(catOut.Bytes(), sparse) {
		t.Fatalf("cat(full) = %q, want %q", catOut.Bytes(), sparse)
	}
	catOut.Reset()
	if err := catWithWriter(c, []string{"--offset", "2", "--length", "12", ":/ext.bin"}, &catOut); err != nil {
		t.Fatalf("cat(range): %v", err)
	}
	if !bytes.Equal(catOut.Bytes(), sparse[2:14]) {
		t.Fatalf("cat(range) = %q, want %q", catOut.Bytes(), sparse[2:14])
	}

	// 3. Append via `drive9 fs cp --append`.
	tailPath := filepath.Join(t.TempDir(), "tail.bin")
	if err := os.WriteFile(tailPath, []byte("TAIL"), 0o644); err != nil {
		t.Fatalf("WriteFile(tail): %v", err)
	}
	if err := Cp(c, []string{"--append", tailPath, ":/ext.bin"}); err != nil {
		t.Fatalf("Cp(--append extent): %v", err)
	}
	if appended != "TAIL" {
		t.Fatalf("appended = %q, want TAIL", appended)
	}

	// 4. `drive9 fs stat` text and JSON output.
	statText, err := captureStdoutE(t, func() error { return Stat(c, []string{":/ext.bin"}) })
	if err != nil {
		t.Fatalf("Stat(text): %v", err)
	}
	if !strings.Contains(statText, "content_layout: extent\n") {
		t.Fatalf("Stat(text) missing content_layout: %q", statText)
	}
	statJSON, err := captureStdoutE(t, func() error { return Stat(c, []string{"-o", "json", ":/ext.bin"}) })
	if err != nil {
		t.Fatalf("Stat(json): %v", err)
	}
	var parsed client.StatMetadataResult
	if err := json.Unmarshal([]byte(statJSON), &parsed); err != nil {
		t.Fatalf("Unmarshal stat JSON: %v", err)
	}
	if parsed.ContentLayout != client.ContentLayoutExtent || parsed.StorageType != client.StorageTypeExtent {
		t.Fatalf("parsed stat JSON = %+v, want extent layout & storage_type", parsed)
	}

	// 5. Same-identity Copy skips server-side `?copy` fast path when an explicit Layout is set.
	var copyFastPathCalled atomic.Bool
	var initiatedLayout string
	sameSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("copy"):
			copyFastPathCalled.Store(true)
			http.Error(w, "unexpected server-side copy fast path", http.StatusInternalServerError)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/src.bin":
			_, _ = w.Write([]byte("data"))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/dst.bin":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			initiatedLayout, _ = body["layout"].(string)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "copy-ext",
				"part_size":   64,
				"total_parts": 1,
				"checksum_contract": map[string]any{
					"supported": []string{"SHA-256"},
					"required":  true,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/copy-ext/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{"number": 1, "skip_upload": true, "size": 4}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/copy-ext/complete":
			_, _ = w.Write([]byte(`{"status":"ok","revision":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer sameSrv.Close()

	sameClient := client.New(sameSrv.URL, "same-key")
	sameClient.SetSmallFileThresholdForTests(64)
	b1 := newDrive9FS(sameClient)
	b2 := newDrive9FS(sameClient)
	err = Copy(
		context.Background(),
		b1,
		b2,
		Location{Kind: KindDrive9, Context: "ctx1", Path: "/src.bin", Raw: "ctx1:/src.bin"},
		Location{Kind: KindDrive9, Context: "ctx2", Path: "/dst.bin", Raw: "ctx2:/dst.bin"},
		&FileInfo{Path: "/src.bin", Size: 4},
		CopyOpts{Overwrite: true, Layout: string(client.ContentLayoutExtent)},
	)
	if err != nil {
		t.Fatalf("Copy(same identity with explicit layout): %v", err)
	}
	if copyFastPathCalled.Load() {
		t.Fatal("expected SameCopier fast path to be skipped when opts.Layout is set")
	}
	if initiatedLayout != string(client.ContentLayoutExtent) {
		t.Fatalf("initiatedLayout = %q, want extent", initiatedLayout)
	}

	// 6. Contradictory explicit --layout vs existing sticky target layout fails fast before upload,
	// and empty local file overwrites an existing extent target via the 0B truncation fallback.
	tmpDir := t.TempDir()
	emptyPath := filepath.Join(tmpDir, "empty.bin")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("WriteFile(empty): %v", err)
	}
	var zeroTruncCompleted atomic.Bool
	var setmetaReceived atomic.Bool
	strictSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/sticky-single.bin":
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutSingle))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && (r.URL.Path == "/v1/fs/sticky-extent.bin" || r.URL.Path == "/v1/fs/dir/empty.bin"):
			w.Header().Set("Content-Length", "64")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"total_size must be positive"}`))
		case r.Method == http.MethodPatch && (r.URL.Path == "/v1/fs/sticky-extent.bin" || r.URL.Path == "/v1/fs/dir/empty.bin"):
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "ext-trunc-0", "new_size": 0, "blocks": []any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/ext-trunc-0/complete":
			var body struct {
				Parts []any `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Parts) != 0 {
				http.Error(w, "expected empty parts for truncation", http.StatusBadRequest)
				return
			}
			zeroTruncCompleted.Store(true)
			_, _ = w.Write([]byte(`{"status":"ok","revision":6}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/sticky-extent.bin" && r.URL.Query().Has("setmeta"):
			setmetaReceived.Store(true)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer strictSrv.Close()

	strictClient := client.New(strictSrv.URL, "k")
	if err := Cp(strictClient, []string{"--layout=", emptyPath, ":/sticky-extent.bin"}); err == nil || !strings.Contains(err.Error(), `invalid --layout ""`) {
		t.Fatalf("Cp(--layout=) err = %v, want invalid empty layout error", err)
	}
	err = Cp(strictClient, []string{"--layout=extent", emptyPath, ":/sticky-single.bin"})
	if err == nil || !strings.Contains(err.Error(), "already has single content layout; --layout extent cannot be applied") {
		t.Fatalf("Cp(--layout=extent onto single) err = %v, want fast layout mismatch", err)
	}
	err = Cp(strictClient, []string{"--layout=single", emptyPath, ":/sticky-extent.bin"})
	if err == nil || !strings.Contains(err.Error(), "already has extent content layout; --layout single cannot be applied") {
		t.Fatalf("Cp(--layout=single onto extent) err = %v, want fast layout mismatch", err)
	}
	// Directory trailing-slash target also stats the resolved leaf and catches contradictory --layout!
	err = Cp(strictClient, []string{"--layout=single", emptyPath, ":/dir/"})
	if err == nil || !strings.Contains(err.Error(), "already has extent content layout; --layout single cannot be applied") {
		t.Fatalf("Cp(--layout=single onto :/dir/ extent leaf) err = %v, want fast layout mismatch", err)
	}
	// 0-byte upload with --tag onto existing extent target truncates to 0 and applies ?setmeta=1!
	if err := Cp(strictClient, []string{"--tag", "k=v", emptyPath, ":/sticky-extent.bin"}); err != nil {
		t.Fatalf("Cp(empty --tag onto sticky extent): %v", err)
	}
	if !zeroTruncCompleted.Load() || !setmetaReceived.Load() {
		t.Fatalf("zeroTruncCompleted=%v setmetaReceived=%v, want both true", zeroTruncCompleted.Load(), setmetaReceived.Load())
	}

	// Classic server without V2 wraps explicit --layout=extent error with retry guidance.
	classicSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer classicSrv.Close()
	nonEmpty := filepath.Join(tmpDir, "nonempty.bin")
	if err := os.WriteFile(nonEmpty, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile(nonempty): %v", err)
	}
	err = Cp(client.New(classicSrv.URL, "k"), []string{"--layout=extent", nonEmpty, ":/new.bin"})
	if err == nil || !strings.Contains(err.Error(), "--layout extent is not supported by this server (retry without --layout)") {
		t.Fatalf("Cp(--layout=extent on classic server) err = %v, want wrapped --layout guidance", err)
	}
	// drive9FS.OpenWrite rejects conflicting Layout vs TargetLayout before creating a spool tempfile.
	if _, err := newDrive9FS(strictClient).OpenWrite(
		context.Background(),
		Location{Kind: KindDrive9, Path: "/sticky-single.bin", Raw: ":/sticky-single.bin"},
		WriteOpts{Layout: string(client.ContentLayoutExtent), TargetLayout: string(client.ContentLayoutSingle)},
	); err == nil || !strings.Contains(err.Error(), "already has single content layout; --layout extent cannot be applied") {
		t.Fatalf("OpenWrite(conflicting layouts) err = %v, want fast layout mismatch before spooling", err)
	}

	// `drive9 fs stat` backfills content_layout from HEAD when GET ?stat=1 omits it on older servers.
	omittedStatSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/head-only-layout.bin" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"size":42,"isdir":false,"resource_id":"r1","revision":2,"content_type":"application/octet-stream","semantic_text":"","tags":{"env":"prod"}}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/head-only-layout.bin":
			w.Header().Set("Content-Length", "42")
			w.Header().Set("X-Dat9-Isdir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Resource-Id", "r1")
			w.Header().Set("X-Dat9-Storage-Type", "extent")
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer omittedStatSrv.Close()
	backfillOut, err := captureStdoutE(t, func() error {
		return Stat(client.New(omittedStatSrv.URL, "k"), []string{":/head-only-layout.bin"})
	})
	if err != nil {
		t.Fatalf("Stat(backfill): %v", err)
	}
	if !strings.Contains(backfillOut, "content_layout: extent\n") || !strings.Contains(backfillOut, "tags.env: prod\n") {
		t.Fatalf("Stat(backfill) output = %q, want content_layout: extent + tags", backfillOut)
	}
}
