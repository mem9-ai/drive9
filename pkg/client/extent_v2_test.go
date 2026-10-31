package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// extentV2TestServer fakes the extent content-layout v2 surface: ranged GET
// answers a block read plan over a fixed three-segment file (data, hole,
// data), PATCH/presign-batch/complete drive the write flows, and /obj hosts
// the block objects.
type extentV2TestServer struct {
	*httptest.Server
	mu sync.Mutex

	patchBodies   []map[string]any
	completes     []map[string]any
	initiates     []map[string]any
	presignBodies []map[string]any
	putBlocks     map[string][]byte
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// extentTestFile is AAAA(4) + hole(8) + BBBB(4): 16 bytes total.
var extentTestFile = func() []byte {
	b := make([]byte, 16)
	copy(b, "AAAA")
	copy(b[12:], "BBBB")
	return b
}()

func newExtentV2TestServer(t *testing.T) *extentV2TestServer {
	t.Helper()
	ts := &extentV2TestServer{putBlocks: map[string][]byte{}}
	mux := http.NewServeMux()

	// Block objects: GET honors Range (sub-block reads), PUT stores bytes.
	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/obj/"):]
		switch r.Method {
		case http.MethodGet:
			ts.mu.Lock()
			data, ok := ts.putBlocks[key]
			ts.mu.Unlock()
			if !ok {
				http.Error(w, "no such block", http.StatusNotFound)
				return
			}
			// Whole-object presign: honor a client-narrowed Range.
			if rng := r.Header.Get("Range"); rng != "" {
				var start, end int64
				if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil {
					if end >= int64(len(data)) {
						end = int64(len(data)) - 1
					}
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[start : end+1])
					return
				}
			}
			_, _ = w.Write(data)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			ts.mu.Lock()
			ts.putBlocks[key] = body
			ts.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			// Stat: the fixed file reports extent layout.
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.Header().Set("X-Dat9-Revision", "7")
			w.Header().Set("Content-Length", fmt.Sprint(len(extentTestFile)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.Header().Set("X-Dat9-Storage-Type", "extent")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPartialContent)
			_ = json.NewEncoder(w).Encode(ExtentReadPlan{
				Size:     int64(len(extentTestFile)),
				Revision: 7,
				Blocks: []ExtentReadPlanBlock{
					{FileOffset: 0, Length: 4, BlockKey: "a", BlockOffset: 0, URL: ts.URL + "/obj/a"},
					{FileOffset: 4, Length: 8, Zero: true},
					{FileOffset: 12, Length: 4, BlockKey: "b", BlockOffset: 0, URL: ts.URL + "/obj/b"},
				},
			})
		case http.MethodPatch:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			ts.mu.Lock()
			ts.patchBodies = append(ts.patchBodies, body)
			ts.mu.Unlock()
			dirty, _ := body["dirty_blocks"].([]any)
			blocks := make([]ExtentWritePlanBlock, 0, len(dirty))
			for i := range dirty {
				d := dirty[i].(map[string]any)
				blocks = append(blocks, ExtentWritePlanBlock{
					Chunk: uint32(d["chunk"].(float64)), Off: uint32(d["off"].(float64)),
					Size: int64(d["size"].(float64)), Checksum: d["checksum"].(string),
					URL: fmt.Sprintf("%s/obj/patch-%d", ts.URL, i),
				})
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(ExtentWritePlan{UploadID: "patch-1", NewSize: int64(body["new_size"].(float64)), Blocks: blocks})
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		ts.mu.Lock()
		ts.initiates = append(ts.initiates, body)
		ts.mu.Unlock()
		// Extent sessions answer an empty Key; legacy servers ignore the
		// layout field and answer a classic plan (Key != "").
		if body["layout"] != "extent" {
			http.Error(w, "expected layout=extent", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(uploadPlanV2{
			UploadID: "seq-1", PartSize: 4 << 20,
			TotalParts:       int(body["total_size"].(float64)/(4<<20)) + 1,
			ChecksumContract: checksumContract{Supported: []string{"SHA-256"}, Required: true},
		})
	})

	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/presign-batch"):
			ts.presignBodies = append(ts.presignBodies, body)
			parts, _ := body["parts"].([]any)
			out := make([]presignedPart, 0, len(parts))
			for i := range parts {
				m := parts[i].(map[string]any)
				n := int(m["part_number"].(float64))
				// The server decodes checksum as an {algorithm,value} object.
				cs := m["checksum"].(map[string]any)
				if cs["algorithm"] != "sha256" {
					http.Error(w, "bad algorithm", http.StatusBadRequest)
					return
				}
				sum := cs["value"].(string)
				out = append(out, presignedPart{Number: n, URL: fmt.Sprintf("%s/obj/seq-%d", ts.URL, n), ChecksumSHA256: sum})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
		case strings.HasSuffix(r.URL.Path, "/complete"):
			ts.completes = append(ts.completes, body)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed"})
		default:
			http.Error(w, "bad upload route", http.StatusNotFound)
		}
	})

	ts.Server = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	ts.mu.Lock()
	ts.putBlocks["a"] = extentTestFile[:4]
	ts.putBlocks["b"] = extentTestFile[12:]
	ts.mu.Unlock()
	return ts
}

func TestExtentV2ReadStreamServesPlan(t *testing.T) {
	ts := newExtentV2TestServer(t)
	cl := New(ts.URL, "")
	rc, err := cl.ReadStream(context.Background(), "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(extentTestFile) {
		t.Fatalf("ReadStream bytes = %q, want %q", got, extentTestFile)
	}
}

func TestExtentV2GetExtentReadPlan(t *testing.T) {
	ts := newExtentV2TestServer(t)
	cl := New(ts.URL, "")
	plan, err := cl.GetExtentReadPlan(context.Background(), "/big.bin", 0, 15)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Size != 16 || plan.Revision != 7 {
		t.Fatalf("plan size/rev = %d/%d", plan.Size, plan.Revision)
	}
	if len(plan.Blocks) != 3 || !plan.Blocks[1].Zero || plan.Blocks[0].BlockKey != "a" {
		t.Fatalf("unexpected blocks: %+v", plan.Blocks)
	}
}

func TestExtentV2CommitExtentPatchFlow(t *testing.T) {
	ts := newExtentV2TestServer(t)
	cl := New(ts.URL, "")
	// Dirty bytes at file offset 12..16 (chunk 0, off 12).
	dirty := []byte("CCCC")
	err := cl.CommitExtentPatch(context.Background(), "/big.bin", 16, []ExtentBlockWrite{{Chunk: 0, Off: 12, Data: dirty}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.patchBodies) != 1 {
		t.Fatalf("PATCH calls = %d", len(ts.patchBodies))
	}
	pb := ts.patchBodies[0]
	if pb["new_size"].(float64) != 16 || pb["expected_revision"].(float64) != 7 {
		t.Fatalf("PATCH body = %+v", pb)
	}
	blocks := pb["dirty_blocks"].([]any)
	if len(blocks) != 1 {
		t.Fatalf("dirty_blocks = %+v", blocks)
	}
	b0 := blocks[0].(map[string]any)
	if b0["checksum"].(string) != sha256Hex(dirty) {
		t.Fatalf("checksum = %v", b0["checksum"])
	}
	if string(ts.putBlocks["patch-0"]) != "CCCC" {
		t.Fatalf("uploaded block = %q", ts.putBlocks["patch-0"])
	}
	if len(ts.completes) != 1 {
		t.Fatalf("complete calls = %d", len(ts.completes))
	}
	cp := ts.completes[0]["parts"].([]any)
	p0 := cp[0].(map[string]any)
	// chunk=0 is omitted by omitempty; zero decodes back to chunk 0.
	getF := func(m map[string]any, k string) float64 {
		if v, ok := m[k].(float64); ok {
			return v
		}
		return 0
	}
	if getF(p0, "number") != 1 || p0["etag"].(string) != sha256Hex(dirty) ||
		getF(p0, "chunk") != 0 || getF(p0, "off") != 12 || getF(p0, "size") != 4 {
		t.Fatalf("complete part = %+v", p0)
	}
}

func TestExtentV2WriteExtentStreamSequential(t *testing.T) {
	ts := newExtentV2TestServer(t)
	cl := New(ts.URL, "")
	// 8 MiB + 4 KiB: three 4 MiB parts, the last one short.
	data := make([]byte, 8<<20|4096)
	for i := range data {
		data[i] = byte(i)
	}
	if err := cl.WriteExtentStreamConditional(context.Background(), "/new.bin", bytesReaderAt(data), int64(len(data)), nil, -1); err != nil {
		t.Fatal(err)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.initiates) != 1 || ts.initiates[0]["layout"] != "extent" {
		t.Fatalf("initiate = %+v", ts.initiates)
	}
	if ts.initiates[0]["total_size"].(float64) != float64(len(data)) {
		t.Fatalf("initiate total_size = %v", ts.initiates[0]["total_size"])
	}
	if len(ts.completes) != 1 {
		t.Fatalf("complete calls = %d", len(ts.completes))
	}
	parts := ts.completes[0]["parts"].([]any)
	if len(parts) != 3 {
		t.Fatalf("parts = %d, want 3", len(parts))
	}
	for i, p := range parts {
		m := p.(map[string]any)
		if _, hasChunk := m["chunk"]; hasChunk {
			t.Fatalf("sequential part %d carries explicit chunk", i)
		}
		start := int64(i) * (4 << 20)
		end := start + (4 << 20)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if m["etag"].(string) != sha256Hex(data[start:end]) {
			t.Fatalf("part %d etag mismatch", i)
		}
	}
	for n := 1; n <= 3; n++ {
		key := fmt.Sprintf("seq-%d", n)
		start := int64(n-1) * (4 << 20)
		end := start + (4 << 20)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if string(ts.putBlocks[key]) != string(data[start:end]) {
			t.Fatalf("part %d bytes mismatch", n)
		}
	}
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
