package writeback

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// migrationServer models just enough of the v2 extent surface for the
// write-back plane's promotion + migration path:
//
//   - HEAD reports a classic 0-byte name until the first extent session
//     completes, then extent layout at the revision that session produced;
//   - POST /v2/uploads/initiate + presign-batch + PUT record the uploaded
//     part bytes, so a test can assert HOW MUCH was re-uploaded per fsync;
//   - PATCH /v1/fs/... records an incremental commit.
type migrationServer struct {
	*httptest.Server
	handler  http.Handler
	partSize int64

	mu       sync.Mutex
	layout   string // "classic" | "extent"
	revision int64
	size     int64
	sessions int            // whole-image extent sessions
	patches  int            // incremental PATCH commits
	parts    map[int][]byte // parts received in the LAST session
	putBytes int64          // total bytes PUT across all sessions
}

func newMigrationServer(t *testing.T) *migrationServer {
	t.Helper()
	s := &migrationServer{partSize: 4 << 20, layout: "classic", parts: make(map[int][]byte)}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"inline_threshold":1,"storage_capabilities":{"completion_identity_v1":true}}`))
	})
	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			s.mu.Lock()
			layout, rev, size := s.layout, s.revision, s.size
			s.mu.Unlock()
			if rev == 0 {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if layout == "extent" {
				w.Header().Set("X-Dat9-Content-Layout", "extent")
			}
			w.Header().Set("X-Dat9-Revision", fmt.Sprint(rev))
			w.Header().Set("Content-Length", fmt.Sprint(size))
			w.WriteHeader(http.StatusOK)
		case http.MethodPatch:
			var body struct {
				NewSize     int64 `json:"new_size"`
				DirtyBlocks []struct {
					Chunk    uint32 `json:"chunk"`
					Off      uint32 `json:"off"`
					Size     int64  `json:"size"`
					Checksum string `json:"checksum"`
				} `json:"dirty_blocks"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			// The initiator must describe every dirty block it was told
			// about, or the client rejects its own plan.
			blocks := make([]client.ExtentWritePlanBlock, 0, len(body.DirtyBlocks))
			for _, b := range body.DirtyBlocks {
				blocks = append(blocks, client.ExtentWritePlanBlock{
					Chunk: b.Chunk, Off: b.Off, Size: b.Size, Checksum: b.Checksum,
					URL: fmt.Sprintf("http://%s/obj/%d", r.Host, int(b.Chunk)*64+int(b.Off)),
				})
			}
			s.mu.Lock()
			s.patches++
			s.revision++
			if body.NewSize > 0 {
				s.size = body.NewSize
			}
			s.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.ExtentWritePlan{
				UploadID: "patch", NewSize: body.NewSize, Blocks: blocks,
			})
		case http.MethodGet:
			s.mu.Lock()
			size := s.size
			s.mu.Unlock()
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.Header().Set("Content-Length", fmt.Sprint(size))
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			TotalSize int64 `json:"total_size"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		total := int((body.TotalSize + s.partSize - 1) / s.partSize)
		s.mu.Lock()
		s.sessions++
		s.parts = make(map[int][]byte)
		s.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "s1", "key": "", "part_size": s.partSize, "total_parts": total,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
		})
	})
	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/presign-batch") {
			var body struct {
				Parts []struct {
					Number int `json:"part_number"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			out := make([]map[string]any, 0, len(body.Parts))
			for _, p := range body.Parts {
				out = append(out, map[string]any{
					"number": p.Number,
					"url":    fmt.Sprintf("http://%s/obj/%d", r.Host, p.Number),
					"size":   s.partSize,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
			return
		}
		// complete
		s.mu.Lock()
		s.layout = "extent"
		s.revision++
		s.size = 0
		for _, p := range s.parts {
			s.size += int64(len(p))
		}
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed"})
	})
	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) {
		var n int
		_, _ = fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/obj/"), "%d", &n)
		data, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.parts[n] = data
		s.putBytes += int64(len(data))
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	s.handler = mux
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func (s *migrationServer) snapshot() (layout string, rev, size int64, sessions, patches int, putBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.layout, s.revision, s.size, s.sessions, s.patches, s.putBytes
}

// newMigratingStack builds a write-back chain whose extent routes are armed
// and whose opener hands out REAL engine handles from the extent plane.
func newMigratingStack(t *testing.T, srv *migrationServer) (*fs.Chain, *Writeback, *extent.Handler) {
	t.Helper()
	eng := extent.NewEngine(extent.EngineConfig{
		Client:     client.New(srv.URL, ""),
		RemoteRoot: "/",
	})
	plane := extent.New(extent.Options{Engine: eng, Threshold: 1})
	ch, wb := newUploadStack(t, srv.handler, Options{
		Policy: PolicyWriteBack,
		// Every non-empty image is promotion-sized, so the first commit of
		// a fresh file promotes it and exercises the migration.
		ExtentTakeover: true,
		ExtentMinSize:  1,
		ExtentOpener:   plane,
	})
	return ch, wb, plane
}

// The defect B fixes: a descriptor that created a small file keeps writing
// after the file has been promoted to extent. Before the migration the
// handle stayed on this plane and re-staged + re-uploaded the WHOLE image at
// every fsync, so N fsyncs cost N full uploads. After it, the handle is the
// engine's and fsync publishes through JuiceFS.
func TestPromotionMigratesOpenHandleToExtent(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h := res.Handle
	if h == nil {
		t.Fatal("create returned no handle")
	}
	defer func() {
		if wb.queue != nil {
			wb.queue.DrainAll()
		}
	}()

	// Grow past the promotion cutoff and flush: this is what promotes the
	// remote to extent layout.
	if err := h.Write(ctx, 0, make([]byte, 1<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Wait for the promotion landing WITHOUT draining: DrainAll stops the
	// queue for good and every later fsync would fail with "queue stopped"
	// (which would make the assertions below vacuous).
	if wb.queue != nil {
		if err := wb.queue.WaitPathOutcome("/big.bin", 10*time.Second); err != nil {
			t.Fatalf("wait for promotion landing: %v", err)
		}
	}
	layout, _, _, sessionsAfterPromotion, _, _ := srv.snapshot()
	if layout != "extent" {
		t.Fatalf("remote layout = %q, want extent (the promotion never happened, so the test proves nothing)", layout)
	}
	if sessionsAfterPromotion == 0 {
		t.Fatal("no extent session: the image did not promote")
	}

	// The descriptor must now belong to the engine. Any further fsync has to
	// publish incrementally instead of opening a new whole-image session.
	before, _, _, sessionsBefore, patchesBefore, _ := srv.snapshot()
	_ = before
	for i := 0; i < 5; i++ {
		if err := h.Write(ctx, int64(i)*4096, []byte("0123456789abcdef")); err != nil {
			t.Fatalf("post-promotion write %d: %v", i, err)
		}
		if err := h.Sync(ctx); err != nil {
			t.Fatalf("post-promotion fsync %d: %v", i, err)
		}
	}
	_, _, _, sessionsAfter, patchesAfter, _ := srv.snapshot()
	if sessionsAfter != sessionsBefore {
		t.Fatalf("fsync after promotion opened %d MORE whole-image extent sessions (want 0): the open descriptor did not migrate to the engine",
			sessionsAfter-sessionsBefore)
	}
	if patchesAfter == patchesBefore {
		t.Fatalf("post-promotion fsyncs did not publish incrementally (patches %d -> %d)", patchesBefore, patchesAfter)
	}
}
