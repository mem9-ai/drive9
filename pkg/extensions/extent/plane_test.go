package extent

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// fakeV2 models the extent v2 surface the engine plane touches: stat,
// read plans, PATCH, sequential sessions, and the block objects.
type fakeV2 struct {
	*httptest.Server
	mu sync.Mutex

	extent     map[string]string // path -> content (extent layout)
	classic    map[string]string // path -> content (single layout)
	dirs       map[string]bool
	blocks     map[string][]byte // sha256 hex -> bytes
	creates    []string
	ids        map[string]string // path -> resource identity (X-Dat9-Resource-ID)
	nextID     int
	statFail   map[string]bool          // paths whose HEAD answers 500 (probe injection)
	gateOnce   map[string]chan struct{} // first HEAD for a path waits on the channel
	gateHits   map[string]int           // HEADs currently paused on a gate
	afterHead  func(path string)        // runs (under f.mu) after a HEAD was answered
	patches    int
	patchPaths []string   // every PATCH target, in order
	deletes    []string   // every DELETE target, in order
	lastPatch  *fakePatch // the open PATCH session, applied at /complete
}

// fakePatch is one initiated PATCH session: applied to the target's content
// when its upload completes, so write→read coherence is observable.
type fakePatch struct {
	path    string
	newSize int64
	dirty   []client.ExtentDirtyBlock
}

// applyPatchLocked lands the open PATCH session's dirty blocks (caller
// holds f.mu).
func (f *fakeV2) applyPatchLocked() {
	p := f.lastPatch
	f.lastPatch = nil
	content := []byte(f.extent[p.path])
	for _, d := range p.dirty {
		data, ok := f.blocks[d.Checksum]
		if !ok {
			continue
		}
		off := int64(d.Chunk)*client.ExtentChunkSize + int64(d.Off)
		end := off + int64(len(data))
		if int64(len(content)) < end {
			content = append(content, make([]byte, end-int64(len(content)))...)
		}
		copy(content[off:end], data)
	}
	if int64(len(content)) > p.newSize {
		content = content[:p.newSize]
	}
	for int64(len(content)) < p.newSize {
		content = append(content, 0)
	}
	f.extent[p.path] = string(content)
	if id := f.ids[p.path]; id != "" {
		for q, qid := range f.ids { // hardlinks share the object
			if qid == id {
				if _, isExtent := f.extent[q]; isExtent {
					f.extent[q] = string(content)
				}
			}
		}
	}
	f.blocks[sha256Hex(content)] = append([]byte(nil), content...)
}

func newFakeV2(t *testing.T) *fakeV2 {
	t.Helper()
	f := &fakeV2{extent: map[string]string{}, classic: map[string]string{}, dirs: map[string]bool{"/": true}, blocks: map[string][]byte{}, ids: map[string]string{}, statFail: map[string]bool{}, gateOnce: map[string]chan struct{}{}, gateHits: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/obj/")
		switch r.Method {
		case http.MethodGet:
			f.mu.Lock()
			data, ok := f.blocks[key]
			f.mu.Unlock()
			if !ok {
				http.Error(w, "no such block", http.StatusNotFound)
				return
			}
			if rng := r.Header.Get("Range"); rng != "" {
				var start, end int64
				if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil {
					if end >= int64(len(data)) {
						end = int64(len(data)) - 1
					}
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[start : end+1])
					return
				}
			}
			_, _ = w.Write(data)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.blocks[key] = body
			f.blocks[sha256Hex(body)] = body // content-addressed, as the server stores it
			f.mu.Unlock()
			w.WriteHeader(200)
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch r.Method {
		case http.MethodHead:
			f.mu.Lock()
			defer f.mu.Unlock()
			if ch, gated := f.gateOnce[path]; gated {
				delete(f.gateOnce, path)
				f.gateHits[path] = f.gateHits[path] + 1
				f.mu.Unlock()
				<-ch // one-shot: pause exactly one HEAD for this path
				f.mu.Lock()
			}
			if hook := f.afterHead; hook != nil {
				defer hook(path)
			}
			if f.statFail[path] {
				http.Error(w, "probe refused", http.StatusInternalServerError)
				return
			}
			switch {
			case f.dirs[path]:
				w.Header().Set("X-Dat9-IsDir", "true")
				w.WriteHeader(200)
			case f.extent[path] != "":
				w.Header().Set("X-Dat9-Content-Layout", "extent")
				w.Header().Set("X-Dat9-Revision", "3")
				w.Header().Set("X-Dat9-Resource-ID", f.ids[path])
				w.Header().Set("Content-Length", fmt.Sprint(len(f.extent[path])))
				w.WriteHeader(200)
			case f.classic[path] != "":
				w.Header().Set("X-Dat9-Resource-ID", f.ids[path])
				w.Header().Set("Content-Length", fmt.Sprint(len(f.classic[path])))
				w.WriteHeader(200)
			default:
				http.Error(w, "not found", http.StatusNotFound)
			}
		case http.MethodGet:
			f.mu.Lock()
			content := f.extent[path]
			f.mu.Unlock()
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPartialContent)
			var blocks []client.ExtentReadPlanBlock
			if content != "" {
				sum := sha256Hex([]byte(content))
				blocks = append(blocks, client.ExtentReadPlanBlock{FileOffset: 0, Length: int64(len(content)), BlockKey: sum, BlockOffset: 0, URL: f.URL + "/obj/" + sum})
			}
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{Size: int64(len(content)), Revision: 3, Blocks: blocks})
		case http.MethodPatch:
			var body struct {
				NewSize     int64                     `json:"new_size"`
				DirtyBlocks []client.ExtentDirtyBlock `json:"dirty_blocks"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			f.patches++
			f.patchPaths = append(f.patchPaths, path)
			f.lastPatch = &fakePatch{path: path, newSize: body.NewSize, dirty: body.DirtyBlocks}
			f.mu.Unlock()
			blocks := make([]client.ExtentWritePlanBlock, 0, len(body.DirtyBlocks))
			for i, d := range body.DirtyBlocks {
				blocks = append(blocks, client.ExtentWritePlanBlock{Chunk: d.Chunk, Off: d.Off, Size: d.Size, Checksum: d.Checksum, URL: fmt.Sprintf("%s/obj/%d", f.URL, i)})
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.ExtentWritePlan{UploadID: "u1", NewSize: body.NewSize, Blocks: blocks})
		case http.MethodPost:
			if _, isLink := r.URL.Query()["hardlink"]; isLink {
				// POST <dst>?hardlink=1 with X-Dat9-Hardlink-Source: like the
				// real server, an existing destination is REFUSED (409) and
				// the new name shares the source's object and identity.
				src := r.Header.Get("X-Dat9-Hardlink-Source")
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.dirs[path] || f.extent[path] != "" || f.classic[path] != "" {
					http.Error(w, "exists", http.StatusConflict)
					return
				}
				if c, ok := f.extent[src]; ok {
					f.extent[path] = c
					f.ids[path] = f.ids[src]
					w.WriteHeader(200)
					return
				}
				if c, ok := f.classic[src]; ok {
					f.classic[path] = c
					f.ids[path] = f.ids[src]
					w.WriteHeader(200)
					return
				}
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			// POST <new>?rename with X-Dat9-Rename-Source: like the real
			// server (RenameFileReplacingTarget), an existing target is
			// REPLACED — that is exactly what a shared sentinel name would
			// do to a still-open retained incarnation.
			if _, isRename := r.URL.Query()["rename"]; !isRename {
				http.Error(w, "bad post", http.StatusBadRequest)
				return
			}
			src := r.Header.Get("X-Dat9-Rename-Source")
			f.mu.Lock()
			defer f.mu.Unlock()
			if !f.move(src, path) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(200)
		case http.MethodDelete:
			f.mu.Lock()
			f.deletes = append(f.deletes, path)
			delete(f.extent, path)
			delete(f.classic, path)
			delete(f.ids, path)
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.creates = append(f.creates, body["path"].(string))
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "s1", "key": "", "part_size": 4 << 20,
			"total_parts":       int(body["total_size"].(float64)/(4<<20)) + 1,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
		})
	})
	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/presign-batch") {
			var body struct {
				Parts []struct {
					Number   int    `json:"part_number"`
					Checksum string `json:"checksum"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			out := make([]map[string]any, 0, len(body.Parts))
			for _, p := range body.Parts {
				f.mu.Lock()
				exists := len(f.blocks[p.Checksum]) > 0
				f.mu.Unlock()
				m := map[string]any{"number": p.Number, "url": fmt.Sprintf("%s/obj/%s", f.URL, p.Checksum)}
				if exists {
					m["skip_upload"] = true
				}
				out = append(out, m)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/complete") {
			var body struct {
				Parts []struct {
					Number int    `json:"number"`
					ETag   string `json:"etag"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			if strings.HasPrefix(r.URL.Path, "/v2/uploads/u1/") && f.lastPatch != nil {
				// The PATCH session: land its dirty blocks on the target.
				f.applyPatchLocked()
			} else {
				path := ""
				if len(f.creates) > 0 {
					path = f.creates[len(f.creates)-1]
				}
				var content []byte
				for _, p := range body.Parts {
					content = append(content, f.blocks[p.ETag]...)
				}
				if path != "" && len(content) > 0 {
					f.extent[path] = string(content)
				}
			}
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed"})
			return
		}
		http.Error(w, "bad upload route", http.StatusNotFound)
	})
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fakeV2) seedExtent(t *testing.T, path, content string) {
	t.Helper()
	sum := sha256Hex([]byte(content))
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extent[path] = content
	f.blocks[sum] = []byte(content)
	f.nextID++
	f.ids[path] = fmt.Sprintf("res-%d", f.nextID)
}

// move renames src onto dst, replacing any existing dst (the caller holds
// f.mu). False when src does not exist.
func (f *fakeV2) move(src, dst string) bool {
	if c, ok := f.extent[src]; ok {
		delete(f.extent, src)
		delete(f.classic, dst)
		delete(f.ids, dst)
		f.extent[dst] = c
		if id, ok := f.ids[src]; ok {
			delete(f.ids, src)
			f.ids[dst] = id // the identity moves with the object
		}
		return true
	}
	if c, ok := f.classic[src]; ok {
		delete(f.classic, src)
		delete(f.extent, dst)
		delete(f.ids, dst)
		f.classic[dst] = c
		if id, ok := f.ids[src]; ok {
			delete(f.ids, src)
			f.ids[dst] = id
		}
		return true
	}
	return false
}

// rename models a rename performed by another client: the server-side
// move the facade then reports through the event bus.
func (f *fakeV2) rename(t *testing.T, src, dst string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.move(src, dst) {
		t.Fatalf("fake rename: %s does not exist", src)
	}
}

// contentOf reports the extent content stored at path ("" when absent).
func (f *fakeV2) contentOf(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.extent[path]
}

// setDir creates (on) or removes (off) a directory entry.
func (f *fakeV2) setDir(path string, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if on {
		f.dirs[path] = true
		return
	}
	delete(f.dirs, path)
}

// remove models a deletion performed through the classic plane or by
// another client.
func (f *fakeV2) remove(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.extent, path)
	delete(f.classic, path)
	delete(f.ids, path)
}

// setAfterHead installs (or clears, with nil) a callback that runs under
// f.mu right after any HEAD was answered — the moment between a probe's
// response and the caller's next request.
func (f *fakeV2) setAfterHead(fn func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterHead = fn
}

// gateHead pauses exactly ONE future HEAD for the path until the channel
// closes (a later stat for the same path passes through).
func (f *fakeV2) gateHead(path string, ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gateOnce[path] = ch
}

// gateWaiting reports how many HEADs for the path are currently paused on
// a gate.
func (f *fakeV2) gateWaiting(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gateHits[path]
}

// setStatFail injects (on=true) or clears (on=false) HEAD failures for a
// path, modeling unreachable probes.
func (f *fakeV2) setStatFail(path string, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if on {
		f.statFail[path] = true
		return
	}
	delete(f.statFail, path)
}

func dispatchCreate(ctx context.Context, h *Handler, path string, flags uint32) (fs.Result, error, bool) {
	var nextCalled bool
	res, err := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: path, Flags: flags, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		nextCalled = true
		return fs.Result{Handle: &stubCore{}}, nil
	}))
	return res, err, nextCalled
}

type stubCore struct{}

func (stubCore) Read(ctx context.Context, off int64, dest []byte) (int, error) { return 0, io.EOF }
func (stubCore) Write(ctx context.Context, off int64, data []byte) error       { return nil }
func (stubCore) Sync(ctx context.Context) error                                { return nil }
func (stubCore) Flush(ctx context.Context) error                               { return nil }
func (stubCore) Release(ctx context.Context) error                             { return nil }

// engineCacheDir is a best-effort scratch dir for the engine's block
// cache. The engine's staging writes race t.TempDir()'s RemoveAll
// assertion ("directory not empty"); this dir is removed best-effort with
// errors ignored instead.
func engineCacheDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "extent-engine-cache-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func newTestHandler(t *testing.T, f *fakeV2, threshold int64) *Handler {
	t.Helper()
	eng := NewEngine(EngineConfig{
		Client:     client.New(f.URL, ""),
		RemoteRoot: "/",
		CacheDir:   engineCacheDir(t),
	})
	bus := fs.NewEventBus()
	return New(Options{Engine: eng, Threshold: threshold, Events: bus})
}

func TestEngineClaimsExtentFile(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA-EXTENT")
	h := newTestHandler(t, f, 1<<20)
	res, err, passed := dispatchCreate(context.Background(), h, "/big.bin", syscall.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("extent file must be claimed, not passed through")
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	dest := make([]byte, len("AAAA-EXTENT"))
	n, err := eh.Read(context.Background(), 0, dest)
	if err != nil || string(dest[:n]) != "AAAA-EXTENT" {
		t.Fatalf("read = %q, %v", dest[:n], err)
	}
}

func TestEnginePassesClassicAndUnknown(t *testing.T) {
	f := newFakeV2(t)
	f.mu.Lock()
	f.classic["/plain.txt"] = "hello"
	f.mu.Unlock()
	h := newTestHandler(t, f, 1<<20)
	if _, _, passed := dispatchCreate(context.Background(), h, "/plain.txt", syscall.O_RDWR); !passed {
		t.Fatal("classic file must pass through")
	}
	if _, _, passed := dispatchCreate(context.Background(), h, "/missing.bin", syscall.O_RDWR); !passed {
		t.Fatal("missing file must pass through")
	}
}

func TestEngineHybridThresholdLeavesFreshCreates(t *testing.T) {
	f := newFakeV2(t)
	h := newTestHandler(t, f, 1<<20) // hybrid: positive threshold
	_, _, passed := dispatchCreate(context.Background(), h, "/fresh.bin", syscall.O_RDWR|syscall.O_CREAT)
	if !passed {
		t.Fatal("hybrid mode must leave fresh creates on the classic plane")
	}
}

func TestEngineFullModeClaimsFreshCreateAndCommits(t *testing.T) {
	f := newFakeV2(t)
	h := newTestHandler(t, f, 0) // full JuiceFS mode
	res, err, passed := dispatchCreate(context.Background(), h, "/fresh.bin", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("full mode must claim the fresh create")
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	payload := make([]byte, 256<<10)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	f.mu.Lock()
	created := len(f.creates)
	f.mu.Unlock()
	if created == 0 {
		t.Fatal("engine never created the file server-side")
	}
}

func TestEngineGetAttrOverlay(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	if _, _, passed := dispatchCreate(context.Background(), h, "/big.bin", syscall.O_RDWR); passed {
		t.Fatal("extent file must be claimed")
	}
	// Record a pending local size beyond the server's 4-byte view.
	if err := h.opts.Engine.Bridge.TruncateLocal(context.Background(), "/big.bin", 4096); err != nil {
		t.Fatal(err)
	}
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/big.bin", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Attr: &fs.Attr{Size: 4}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 4096 {
		t.Fatalf("overlay size = %+v, want 4096", res.Attr)
	}
}

func TestEngineWriteSyncCommitsPerWrite(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA-EXTENT")
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncPolicy: "write-sync"})
	res, err, passed := dispatchCreate(context.Background(), h, "/big.bin", syscall.O_RDWR)
	if err != nil || passed {
		t.Fatalf("claim failed: %v passed=%v", err, passed)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("NEWBYTES")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// write-sync: the PATCH commit landed during Write, not deferred.
	f.mu.Lock()
	patches := f.patches
	f.mu.Unlock()
	if patches == 0 {
		t.Fatal("write-sync Write did not commit server-side")
	}
}

func TestEngineTruncateIntercepted(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	res, _, _ := dispatchCreate(context.Background(), h, "/big.bin", syscall.O_RDWR)
	eh := res.Handle.(*engineHandle)
	_ = eh
	passed := false
	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/big.bin", HasSize: true, Size: 8192, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		passed = true
		return fs.Result{Attr: &fs.Attr{Size: 8192}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("size-only setattr on an engine file must not reach the classic plane")
	}
	if size, _ := h.opts.Engine.LocalAttr(context.Background(), "/big.bin"); size != 8192 {
		t.Fatalf("local size = %d, want 8192", size)
	}
}
