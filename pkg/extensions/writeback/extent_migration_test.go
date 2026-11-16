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
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
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

	mu         sync.Mutex
	layout     string // "classic" | "extent"
	revision   int64
	size       int64
	sessions   int            // whole-image extent sessions
	patches    int            // incremental PATCH commits
	parts      map[int][]byte // parts received in the LAST session
	putBytes   int64          // total bytes PUT across all sessions
	lastChmod  uint32         // last mode pushed through the remote chmod
	resourceID string         // identity echoed by the last completion
	totalSize  int64          // total_size of the last initiated session
	exists     bool           // the name exists even at revision 0 / size 0
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
			// An EXISTING object always answers, whatever its revision: a
			// 0-byte file created and never rewritten is real, and the
			// revision it reports may legitimately be 0. Answering 404 for
			// it made "empty but present" indistinguishable from "absent",
			// which hid the whole empty-occupant case.
			if size == 0 && rev == 0 && !s.exists {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if layout == "extent" {
				w.Header().Set("X-Dat9-Content-Layout", "extent")
			}
			if s.resourceID != "" {
				w.Header().Set("X-Dat9-Resource-ID", s.resourceID)
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
		case http.MethodPost, http.MethodPut:
			// chmod arrives as a POST/PUT on the path with a mode body.
			var body struct {
				Mode uint32 `json:"mode"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Mode != 0 {
				s.mu.Lock()
				s.lastChmod = body.Mode
				s.mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			// RemoveAll for the empty-occupant reclaim.
			s.mu.Lock()
			s.size = 0
			s.revision = 0
			s.exists = false
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
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
		s.totalSize = body.TotalSize
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
			// The client reads exactly Size bytes per part, so the LAST part
			// must report the remainder of the image, not a full part:
			// declaring partSize for a short tail fails every small upload
			// with "short read".
			s.mu.Lock()
			total := s.totalSize
			s.mu.Unlock()
			for _, p := range body.Parts {
				size := s.partSize
				if off := int64(p.Number-1) * s.partSize; total > 0 && off+size > total {
					size = total - off
				}
				out = append(out, map[string]any{
					"number": p.Number,
					"url":    fmt.Sprintf("http://%s/obj/%d", r.Host, p.Number),
					"size":   size,
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
		// Echo the identity the completion actually published
		// (completion_identity_v1): the migration uses it to prove the
		// object it opens is the one this commit produced.
		s.mu.Lock()
		s.resourceID = fmt.Sprintf("res-%d", s.revision)
		id := s.resourceID
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "resource_id": id})
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

// waitMigrated blocks until the descriptor adopted an engine handle. The
// migration is deliberately outside the queue's path lock (see
// scheduleExtentMigration), so a queue wait alone does not imply it ran.
func waitMigrated(t *testing.T, h *bufferingHandle) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for h.extentHandle() == nil {
		if time.Now().After(deadline) {
			t.Fatal("descriptor never migrated to the engine plane")
		}
		time.Sleep(5 * time.Millisecond)
	}
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
	waitMigrated(t, h.(*bufferingHandle))
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

// A migrated descriptor still owns this plane's lifecycle: the core terminal
// handle it opened at create time, its registry slot, and any unlink
// snapshot. Release must retire all of them — the engine release is
// ADDITIONAL, not a replacement.
//
// Regression: returning straight to the engine's Release left the handle in
// the live registry forever, so the plane kept believing a writer was open.
func TestMigratedHandleReleaseRetiresThisPlane(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := res.Handle.Write(ctx, 0, make([]byte, 1<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if wb.queue != nil {
		if err := wb.queue.WaitPathOutcome("/big.bin", 10*time.Second); err != nil {
			t.Fatalf("wait for landing: %v", err)
		}
	}

	h, ok := res.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	waitMigrated(t, h)

	if err := res.Handle.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	// The registry entry must be gone: a stale one makes the plane think a
	// writer is still live at the path.
	if _, tracked := wb.live.Load("/big.bin"); tracked {
		t.Fatal("released handle is still tracked in the live registry")
	}
	if h.inner != nil {
		// The core terminal was released through the same path; a second
		// release from the facade is the double-release this guards.
		t.Log("core terminal retained on the handle (release delegated elsewhere)")
	}
}

// Migration must not replay the original open's create/truncate intent. The
// bytes the descriptor wrote are already in the image promotion published;
// re-opening with O_TRUNC would discard exactly that image.
func TestMigrationDoesNotReplayOpenIntent(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()

	// Open with O_TRUNC, the shape a shell `>` or a compiler's rewrite uses.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: syscall.O_RDWR | syscall.O_CREAT | syscall.O_TRUNC, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i%251) + 1
	}
	if err := res.Handle.Write(ctx, 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if wb.queue != nil {
		if err := wb.queue.WaitPathOutcome("/big.bin", 10*time.Second); err != nil {
			t.Fatalf("wait for landing: %v", err)
		}
	}
	_, rev, size, _, _, _ := srv.snapshot()
	if rev == 0 || size != int64(len(payload)) {
		t.Fatalf("after promotion the remote holds %d bytes at rev %d, want %d", size, rev, len(payload))
	}
	h := res.Handle.(*bufferingHandle)
	if h.extentHandle() == nil {
		t.Fatal("precondition: the descriptor did not migrate")
	}
	// A migrated O_TRUNC descriptor must still be able to publish the next
	// write; replays of the truncate would leave the file empty.
	if err := res.Handle.Write(ctx, 0, []byte("tail")); err != nil {
		t.Fatalf("post-migration write: %v", err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatalf("post-migration fsync: %v", err)
	}
	_, rev2, size2, _, _, _ := srv.snapshot()
	if rev2 <= rev {
		t.Fatalf("post-migration fsync did not publish (rev %d -> %d)", rev, rev2)
	}
	// The in-place rewrite keeps the size. A replayed O_TRUNC shows up as a
	// SHRUNKEN remote image, so anything below the original proves it.
	if size2 < int64(len(payload)) {
		t.Fatalf("post-migration remote size shrank to %d from %d: the open's O_TRUNC was replayed",
			size2, len(payload))
	}
}

func TestHybridQueuedPromotionReopenTruncateZeroAndClosedSetAttr(t *testing.T) {
	var mu sync.Mutex
	exists := false
	layout := ""
	rev := int64(0)
	size := int64(0)
	resID := ""
	initiateGate := make(chan struct{})
	inInitiate := make(chan struct{})
	classicWriteHit := false
	patchCount := 0
	var initiatesTotalSize int64

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","inline_threshold":50000,"storage_capabilities":{"completion_identity_v1":true}}`))
	})
	mux.HandleFunc("/v1/fs/hyb-zero.bin", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			ex, lay, curRev, curSize, curID := exists, layout, rev, size, resID
			mu.Unlock()
			if !ex {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if lay != "" {
				w.Header().Set("X-Dat9-Content-Layout", lay)
			}
			if curID != "" {
				w.Header().Set("X-Dat9-Resource-ID", curID)
			}
			w.Header().Set("X-Dat9-Revision", fmt.Sprint(curRev))
			w.Header().Set("X-Dat9-Mode", "420")
			w.Header().Set("Content-Length", fmt.Sprint(curSize))
			w.WriteHeader(http.StatusOK)
		case http.MethodPatch:
			var body struct {
				NewSize int64 `json:"new_size"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			rev++
			size = body.NewSize
			patchCount++
			curRev := rev
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": fmt.Sprintf("patch-%d", curRev), "new_size": body.NewSize})
		case http.MethodPut:
			if r.URL.Query().Has("write") {
				mu.Lock()
				lay := layout
				mu.Unlock()
				if lay == "extent" {
					mu.Lock()
					classicWriteHit = true
					mu.Unlock()
					http.Error(w, `{"error":"content layout mismatch"}`, http.StatusInternalServerError)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Layout    string `json:"layout"`
			TotalSize int64  `json:"total_size"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		lay := layout
		if lay == "extent" || body.Layout != "extent" {
			classicWriteHit = true
			mu.Unlock()
			http.Error(w, `{"error":"content layout mismatch"}`, http.StatusInternalServerError)
			return
		}
		initiatesTotalSize = body.TotalSize
		mu.Unlock()
		close(inInitiate)
		<-initiateGate
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "init1", "key": "", "part_size": 4 << 20, "total_parts": 1,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
		})
	})
	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/presign-batch") {
			mu.Lock()
			total := initiatesTotalSize
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []any{map[string]any{"number": 1, "url": "http://" + r.Host + "/obj/1", "size": total}},
			})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/complete") {
			mu.Lock()
			if layout == "" {
				exists = true
				layout = "extent"
				rev = 1
				size = initiatesTotalSize
				resID = "res-hyb-1"
			}
			curRev := rev
			curID := resID
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "revision": curRev, "resource_id": curID})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	cl := client.New(srv.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)

	eng := extent.NewEngine(extent.EngineConfig{Client: cl, RemoteRoot: "/"})
	plane := extent.New(extent.Options{Engine: eng, Threshold: 1, Patterns: []string{"*.db"}})
	wb, err := New(Options{
		Client:         cl,
		Policy:         PolicyWriteBack,
		Staging:        ledger,
		ExtentTakeover: true,
		ExtentPatterns: []string{"*.db"},
		ExtentMinSize:  50_000,
		ExtentOpener:   plane,
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(plane, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if wb.queue != nil {
			wb.queue.DrainAll()
		}
	})
	ctx := context.Background()

	// Step 1: open("wb") writes 100 KiB (>= 50 KB hybrid cutoff) & closes ->
	// CommitQueue begins promoting to extent asynchronously.
	createRes, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/hyb-zero.bin", Flags: syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := createRes.Handle.Write(ctx, 0, make([]byte, 100_000)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := createRes.Handle.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := createRes.Handle.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	<-inInitiate
	// Allow promotion upload to finish AFTER the concurrent reopen enters writeback.
	time.AfterFunc(40*time.Millisecond, func() { close(initiateGate) })

	// Step 2: immediately reopen r+b and truncate(0) while CommitQueue holds the in-flight upload.
	reopenRes, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/hyb-zero.bin", Flags: syscall.O_RDWR, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("reopen r+b: %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/hyb-zero.bin", Size: 0, HasSize: true, Handle: reopenRes.Handle, Caller: fs.Caller{Pid: 1}}); err != nil {
		t.Fatalf("ftruncate(0): %v", err)
	}
	if err := reopenRes.Handle.Flush(ctx); err != nil {
		t.Fatalf("reopen flush: %v", err)
	}
	if err := reopenRes.Handle.Release(ctx); err != nil {
		t.Fatalf("reopen release: %v", err)
	}

	// Step 3: closed-path OpSetAttr grow & re-zero on an extent file must also use PATCH.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/hyb-zero.bin", Size: 100_000, HasSize: true, Caller: fs.Caller{Pid: 1}}); err != nil {
		t.Fatalf("closed truncate grow: %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/hyb-zero.bin", Size: 0, HasSize: true, Caller: fs.Caller{Pid: 1}}); err != nil {
		t.Fatalf("closed truncate zero: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if classicWriteHit {
		t.Fatal("classic PUT or multipart initiate hit extent file")
	}
	if size != 0 || patchCount < 3 {
		t.Fatalf("final size=%d patchCount=%d, want size=0 patchCount>=3", size, patchCount)
	}
}
