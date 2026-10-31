package extent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Round-3 fix 5: a page whose cursor does not advance must error, not spin.
func TestReadRejectsNonAdvancingContinuation(t *testing.T) {
	var calls int32
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(200)
		default:
			atomic.AddInt32(&calls, 1)
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(http.StatusPartialContent)
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Size: 3 << 20, Revision: 1,
				Blocks: []client.ExtentReadPlanBlock{
					{FileOffset: 0, Length: 1 << 20, BlockKey: sha256HexOf([]byte("a")), URL: ts.URL + "/obj/a"},
				},
				NextOffset: 1 << 20,
			})
		}
	}))
	t.Cleanup(ts.Close)
	b := NewBridge(Config{Client: client.New(ts.URL, ""), RemoteRoot: "/"})
	var slices []meta.Slice
	if st := b.Read(meta.Background(), 1, 0, &slices); st == 0 {
		t.Fatal("non-advancing continuation must fail, not publish")
	}
	if atomic.LoadInt32(&calls) > 4 {
		t.Fatalf("server hammered %d times — the loop did not bound retries", calls)
	}
}

// Round-3 fix 6: budget exhaustion must error, not zero-fill the tail.
func TestReadRejectsBudgetExhaustion(t *testing.T) {
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(200)
		default:
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.WriteHeader(http.StatusPartialContent)
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Size: 3 << 20, Revision: 1,
				Blocks: []client.ExtentReadPlanBlock{
					{FileOffset: 0, Length: 1 << 20, BlockKey: sha256HexOf([]byte("a")), URL: ts.URL + "/obj/a"},
				},
				NextOffset: 1 << 20,
			})
		}
	}))
	t.Cleanup(ts.Close)
	b := NewBridge(Config{Client: client.New(ts.URL, ""), RemoteRoot: "/"})
	saved := maxPlanBlocksPerChunk
	maxPlanBlocksPerChunk = 1
	defer func() { maxPlanBlocksPerChunk = saved }()
	var slices []meta.Slice
	if st := b.Read(meta.Background(), 1, 0, &slices); st == 0 {
		t.Fatal("budget exhaustion must fail instead of publishing partial coverage")
	}
}

// Round-3 fix 1: rename rides EventWrite with RenamedTo; the trailing
// plain EventDelete must not clobber the retargeted handles.
func TestRenameEventRetargetsHandles(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA-EXTENT")
	h := newTestHandler(t, f, 1<<20)
	res, err, passed := dispatchCreate(context.Background(), h, "/big.bin", 2 /* O_RDWR */)
	if err != nil || passed {
		t.Fatalf("claim failed: %v passed=%v", err, passed)
	}
	eh := res.Handle.(*engineHandle)
	h.opts.Events.Emit(fs.Event{Kind: fs.EventWrite, Path: "/big.bin", RenamedTo: "/moved.bin"})
	h.opts.Events.Emit(fs.Event{Kind: fs.EventDelete, Path: "/big.bin"})
	h.mu.Lock()
	_, openOld := h.handles["/big.bin"]
	list, openNew := h.handles["/moved.bin"]
	h.mu.Unlock()
	if openOld || !openNew || len(list) != 1 || list[0] != eh || eh.path != "/moved.bin" {
		t.Fatalf("rename retarget failed: old=%v new=%v handle-path=%q", openOld, openNew, eh.path)
	}
	if got := h.opts.Engine.Bridge.PathsOf("/moved.bin"); got == "" {
		t.Fatal("bridge mapping did not follow the rename")
	}
}

// Round-3 fix 3: a checksum shared by two pending commits is not released
// until the last referencing commit lands.
func TestSharedChecksumSurvivesSiblingCommit(t *testing.T) {
	b := NewBridge(Config{Client: client.New("http://127.0.0.1:1", ""), RemoteRoot: "/"})
	sum := sha256HexOf([]byte("shared"))
	b.blocks.stageBytes(sum, []byte("shared"))
	inoA, inoB := meta.Ino(101), meta.Ino(102)
	w := pendingWrite{chunk: 0, off: 0, checksum: sum, size: 5}
	b.mu.Lock()
	b.pending[inoA] = []pendingWrite{w}
	b.pending[inoB] = []pendingWrite{w}
	b.mu.Unlock()
	b.releaseCommitted(inoA, []pendingWrite{w})
	if _, ok := b.blocks.stagedBytes(sum); !ok {
		t.Fatal("sibling pending commit lost its staged bytes")
	}
	// Mirror the commit path: each ino's pending entry is deleted before
	// its release (commit deletes pending[ino], then calls
	// releaseCommitted). The last referencing commit now finds refs==0.
	b.mu.Lock()
	delete(b.pending, inoA)
	delete(b.pending, inoB)
	b.mu.Unlock()
	b.releaseCommitted(inoB, []pendingWrite{w})
	if _, ok := b.blocks.stagedBytes(sum); ok {
		t.Fatal("bytes leaked after the last referencing commit")
	}
}
