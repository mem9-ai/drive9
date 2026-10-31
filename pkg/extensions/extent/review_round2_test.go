package extent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
)

// ---- Fix A regression: multi-block backing objects read the right 4 MiB ----

func TestObjectGetUsesBlockIndex(t *testing.T) {
	b := NewBridge(Config{Client: client.New("http://127.0.0.1:1", ""), RemoteRoot: "/"})
	// Stage an 8 MiB slice object: first half 'A', second half 'B'.
	const half = 4 << 20
	obj := make([]byte, 2*half)
	for i := 0; i < half; i++ {
		obj[i] = 'A'
		obj[half+i] = 'B'
	}
	sl := &stagedSlice{total: int64(len(obj)), blocks: make(map[int64][]byte)}
	sl.blocks[0] = obj[:half]
	sl.blocks[1] = obj[half:]
	const sliceID = uint64(1 << 63)
	b.mu.Lock()
	b.stagedSlices[sliceID] = sl
	b.mu.Unlock()

	s := objectStorage{bridge: b}
	// The chunk store reads block index 1 with an offset of 0 within that
	// block — that must land in the object's SECOND half.
	rc, err := s.Get(context.Background(), fmt.Sprintf("chunks/0/%d/%d_1_%d", sliceID/1000, sliceID, half), 0, 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	_, _ = rc.Read(buf)
	_ = rc.Close()
	if buf[0] != 'B' {
		t.Fatalf("block 1 read returned %q (object offset 0) — the block index is being ignored", string(buf))
	}
}

// ---- Fix B regression: Read merges plan continuations ----

func TestReadMergesPlanContinuations(t *testing.T) {
	var pages int
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(200)
		default:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(http.StatusPartialContent)
			if pages == 0 {
				pages++
				_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
					Size: 3 << 20, Revision: 4,
					Blocks: []client.ExtentReadPlanBlock{
						{FileOffset: 0, Length: 1 << 20, BlockKey: sha256HexOf([]byte("aa")), URL: ts.URL + "/obj/aa"},
					},
					NextOffset: 1 << 20,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Size: 3 << 20, Revision: 4,
				Blocks: []client.ExtentReadPlanBlock{
					{FileOffset: 2 << 20, Length: 1 << 20, BlockKey: sha256HexOf([]byte("cc")), URL: ts.URL + "/obj/cc"},
				},
			})
		}
	}))
	t.Cleanup(ts.Close)
	b := NewBridge(Config{Client: client.New(ts.URL, ""), RemoteRoot: "/"})
	var slices []meta.Slice
	if st := b.Read(meta.Background(), 1, 0, &slices); st != 0 {
		t.Fatal(st)
	}
	if len(slices) != 2 {
		t.Fatalf("slices = %d, want the continuation merged in (2)", len(slices))
	}
	if slices[0].Id == 0 || slices[1].Id == 0 || slices[0].Id == slices[1].Id {
		t.Fatalf("slice ids = %v", slices)
	}
}

// ---- Fix E regression: a failed truncation keeps its pending marker ----

func TestTruncateMarkerSurvivesFailedFlush(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(200)
		case http.MethodPatch:
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(http.StatusPartialContent)
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{Size: 4096, Revision: 5})
		}
	}))
	t.Cleanup(ts.Close)
	b := NewBridge(Config{Client: client.New(ts.URL, ""), RemoteRoot: "/"})
	ino, errno := b.ResolveLocal(context.Background(), "/t.bin")
	if errno != 0 {
		t.Fatal(errno)
	}
	if err := b.TruncateLocal(context.Background(), "/t.bin", 0); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushFile(context.Background(), ino); err == nil {
		t.Fatal("flush should fail while the server rejects the PATCH")
	}
	b.mu.Lock()
	_, stillTracked := b.truncs[ino]
	b.mu.Unlock()
	if !stillTracked {
		t.Fatal("truncate marker dropped on a failed flush — a later flush would report success over stale data")
	}
}

// ---- Fix G regression: committed slices release their staged bytes ----

func TestCommitReleasesStagedBytes(t *testing.T) {
	b := NewBridge(Config{Client: client.New("http://127.0.0.1:1", ""), RemoteRoot: "/"})
	sum := sha256HexOf([]byte("payload"))
	b.blocks.stageBytes(sum, []byte("payload"))
	b.blocks.release(sum)
	if _, ok := b.blocks.stagedBytes(sum); ok {
		t.Fatal("released block still staged")
	}
	// stageBytes is idempotent: a second stage of the same content keeps
	// the first copy (pointer-stable).
	d1 := []byte("x")
	b.blocks.stageBytes(sum, d1)
	b.blocks.stageBytes(sum, []byte("y"))
	got, ok := b.blocks.stagedBytes(sum)
	if !ok || &got[0] != &d1[0] {
		t.Fatal("stageBytes overwrote the first staged copy")
	}
}

func sha256HexOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
