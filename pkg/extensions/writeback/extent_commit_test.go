package writeback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

func TestDirtyPartRangesCoalesceAndClip(t *testing.T) {
	wb := writebuffer.NewWriteBuffer("/x", 1<<30, 4096)
	wb.SetSmallFileMax(1)                                      // force part mode from the first write
	if _, err := wb.Write(0, make([]byte, 4096)); err != nil { // part 1
		t.Fatal(err)
	}
	if _, err := wb.Write(4096, make([]byte, 4096)); err != nil { // part 2
		t.Fatal(err)
	}
	if _, err := wb.Write(4*4096, make([]byte, 4096)); err != nil { // part 5
		t.Fatal(err)
	}
	got := dirtyPartRanges(wb, 5*4096)
	// Parts 1-2 coalesce; part 5 stands alone.
	want := []ByteRange{{0, 2 * 4096}, {4 * 4096, 5 * 4096}}
	if len(got) != len(want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ranges = %v, want %v", got, want)
		}
	}
	// Clip to a mid-part size.
	got = dirtyPartRanges(wb, 4*4096+100)
	if got[1].End != 4*4096+100 {
		t.Fatalf("clipped range = %v", got)
	}
}

func TestExtentDirtyBlocksCutting(t *testing.T) {
	// 12 MiB image: a large range cuts three aligned 4 MiB blocks; a small
	// append range passes through at natural bounds; two ranges landing in
	// the same aligned block dedupe to one.
	const align = client.ExtentBlockSize
	const mib = int64(1 << 20)
	image := make([]byte, 12*mib+200)
	ra := clientReaderAt(image)

	blocks, err := extentDirtyBlocks(ra, []ByteRange{{0, 12 * mib}}, 12*mib)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 {
		t.Fatalf("blocks = %d, want 3", len(blocks))
	}
	for i, b := range blocks {
		if int64(len(b.Data)) != align || int64(b.Off) != int64(i)*align {
			t.Fatalf("block %d = off %d len %d", i, b.Off, len(b.Data))
		}
	}

	blocks, err = extentDirtyBlocks(ra, []ByteRange{{12*mib + 100, 12*mib + 200}}, 12*mib+200)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || int64(blocks[0].Off) != 12*mib+100 || int64(len(blocks[0].Data)) != 100 {
		t.Fatalf("append block = %+v", blocks[0])
	}

	// Two small ranges inside one aligned 4 MiB block stay two natural
	// slices (disjoint offsets): shadow merge resolves them server-side and
	// compaction repacks the block later.
	blocks, err = extentDirtyBlocks(ra, []ByteRange{{0, 10}, {align / 2, align/2 + 10}}, align)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("small ranges produced %d blocks, want 2", len(blocks))
	}
}

func TestExtentDirtyBlocksChunkOffsets(t *testing.T) {
	// A block spanning the 64 MiB chunk boundary must be cut.
	image := make([]byte, int64(2*client.ExtentChunkSize))
	ra := clientReaderAt(image)
	blocks, err := extentDirtyBlocks(ra, []ByteRange{{client.ExtentChunkSize - 100, client.ExtentChunkSize + 100}}, int64(len(image)))
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("blocks = %d, want 2", len(blocks))
	}
	if blocks[0].Chunk != 0 || int64(blocks[0].Off) != client.ExtentChunkSize-100 {
		t.Fatalf("block 0 = chunk %d off %d", blocks[0].Chunk, blocks[0].Off)
	}
	if blocks[1].Chunk != 1 || blocks[1].Off != 0 {
		t.Fatalf("block 1 = chunk %d off %d", blocks[1].Chunk, blocks[1].Off)
	}
}

func TestZeroGrowBlockCapsAtGrowth(t *testing.T) {
	b := zeroGrowBlock(100, 101)
	if int64(len(b.Data)) != 1 || b.Off != 100 {
		t.Fatalf("zeroGrowBlock = off %d len %d", b.Off, len(b.Data))
	}
}

// extentRoutingServer fakes the three stat outcomes and records which write
// endpoint the queue hit.
type extentRoutingServer struct {
	*httptest.Server
	mu        sync.Mutex
	statKind  string // "extent" | "classic" | "missing"
	patches   int
	initiates []map[string]any
}

func newExtentRoutingServer(t *testing.T, statKind string) *extentRoutingServer {
	t.Helper()
	rs := &extentRoutingServer{statKind: statKind}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			switch rs.statKind {
			case "extent":
				w.Header().Set("X-Dat9-Content-Layout", "extent")
				w.Header().Set("X-Dat9-Revision", "9")
				w.Header().Set("Content-Length", "100")
				w.WriteHeader(http.StatusOK)
			case "classic":
				w.WriteHeader(http.StatusOK)
			default:
				http.Error(w, "not found", http.StatusNotFound)
			}
			return
		}
		if r.Method == http.MethodPatch {
			rs.mu.Lock()
			rs.patches++
			rs.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.ExtentWritePlan{
				UploadID: "u1", NewSize: 1 << 20,
				Blocks: []client.ExtentWritePlanBlock{{Chunk: 0, Off: 0, Size: 4096, Checksum: "c", URL: "http://" + r.Host + "/obj/1"}},
			})
			return
		}
		http.Error(w, "unexpected", http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		rs.mu.Lock()
		rs.initiates = append(rs.initiates, body)
		rs.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "s1", "key": "", "part_size": 4 << 20, "total_parts": 1,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
		})
	})
	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/uploads/s1/presign-batch" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []any{map[string]any{"number": 1, "url": "http://" + r.Host + "/obj/1"}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed"})
	})
	rs.Server = httptest.NewServer(mux)
	t.Cleanup(rs.Close)
	return rs
}

func extentTestEntry(data []byte) *CommitEntry {
	e := &CommitEntry{Path: "/x.bin", Size: int64(len(data)), Kind: 1, BaseRev: 9}
	e.bindPayload(data)
	return e
}

func TestUploadExtentEntryRouting(t *testing.T) {
	// Existing extent target + dirty ranges → PATCH.
	rs := newExtentRoutingServer(t, "extent")
	cq := &CommitQueue{client: client.New(rs.URL, ""), extentTakeover: true}
	entry := extentTestEntry(make([]byte, 1<<20))
	entry.DirtyRanges = []ByteRange{{0, 4096}}
	_, handled, err := cq.uploadExtentEntry(context.Background(), entry, "/x.bin", 9)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.patches != 1 || len(rs.initiates) != 0 {
		t.Fatalf("extent+dirty: patches=%d initiates=%d", rs.patches, len(rs.initiates))
	}

	// Fresh large file → sequential extent session (initiate layout=extent).
	rs2 := newExtentRoutingServer(t, "missing")
	cq2 := &CommitQueue{client: client.New(rs2.URL, ""), extentTakeover: true}
	entry2 := extentTestEntry(make([]byte, 1<<20))
	_, handled, err = cq2.uploadExtentEntry(context.Background(), entry2, "/x.bin", -1)
	if err != nil || !handled {
		t.Fatalf("fresh: handled=%v err=%v", handled, err)
	}
	rs2.mu.Lock()
	defer rs2.mu.Unlock()
	if len(rs2.initiates) != 1 || rs2.initiates[0]["layout"] != "extent" {
		t.Fatalf("fresh initiates = %+v", rs2.initiates)
	}

	// Existing classic target → not handled (classic multipart path).
	rs3 := newExtentRoutingServer(t, "classic")
	cq3 := &CommitQueue{client: client.New(rs3.URL, ""), extentTakeover: true}
	_, handled, err = cq3.uploadExtentEntry(context.Background(), extentTestEntry(make([]byte, 1<<20)), "/x.bin", 9)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("classic target must fall through to the classic path")
	}

	// Kill-switch: a queue without SetExtentTakeover never probes layout.
	rs4 := newExtentRoutingServer(t, "extent")
	cq4 := &CommitQueue{client: client.New(rs4.URL, "")}
	entry4 := extentTestEntry(make([]byte, 1<<20))
	entry4.DirtyRanges = []ByteRange{{0, 4096}}
	_, handled, err = cq4.uploadExtentEntry(context.Background(), entry4, "/x.bin", 9)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("extentTakeover=false must keep classic routing")
	}
}

type clientReaderAt []byte

func (c clientReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(c)) {
		return 0, io.EOF
	}
	n := copy(p, c[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
