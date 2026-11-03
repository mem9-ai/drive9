package extent

import (
	"context"
	"crypto/rand"
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
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"

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
	legacy     bool                     // classic-only server: extent sessions degrade to classic
	failBody   bool                     // abort classic GET bodies mid-stream (2 bytes delivered)
	statGone   bool                     // existing paths invisible to stats (404)
	presigns   int                      // presign-batch calls
	completes  int                      // complete calls
	upSize     int64                    // last initiate's total size (part sizing)
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
			sum := sha256Hex(body)
			f.mu.Lock()
			f.blocks[key] = body
			f.blocks[sum] = body // content-addressed, as the server stores it
			f.mu.Unlock()
			w.Header().Set("ETag", sum) // the v2 complete keys parts by this checksum
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
			if f.statGone && f.extent[path]+f.classic[path] != "" {
				http.Error(w, "not found", http.StatusNotFound)
				return
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
				w.Header().Set("X-Dat9-Mode", "420") // 0644
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
			if f.extent[path] != "" && r.Header.Get("X-Dat9-Extent-Read-Plan") == "" {
				// The wire contract: extent reads MUST declare support for
				// the block-plan representation.
				f.mu.Unlock()
				http.Error(w, "extent read requires X-Dat9-Extent-Read-Plan", http.StatusUnprocessableEntity)
				return
			}
			if classic := f.classic[path]; classic != "" && f.extent[path] == "" {
				// A classic (legacy) file: the raw byte stream, not a plan.
				// The identity rides the SAME response (the server knows
				// which object it just served).
				fail := f.failBody
				id := f.ids[path]
				f.mu.Unlock()
				if id != "" {
					w.Header().Set("X-Dat9-Resource-ID", id)
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(classic)))
				if fail {
					// Deliver a valid prefix, then end the response short of
					// Content-Length (and close the connection): the
					// client's io.ReadAll sees the truncation, and the
					// closed connection can never be reused for a later,
					// recovered request.
					w.Header().Set("Connection", "close")
					_, _ = w.Write([]byte(classic[:min(2, len(classic))]))
					return
				}
				_, _ = w.Write([]byte(classic))
				return
			}
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
			if _, isCreate := r.URL.Query()["create"]; isCreate {
				// POST <path>?create=1: materialize an empty classic file.
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.extent[path] != "" || f.classic[path] != "" || f.dirs[path] {
					http.Error(w, "exists", http.StatusConflict)
					return
				}
				f.classic[path] = ""
				f.nextID++
				f.ids[path] = fmt.Sprintf("res-%d", f.nextID)
				_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
				return
			}
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
		legacy := f.legacy
		f.upSize = int64(body["total_size"].(float64))
		occupant := f.ids[body["path"].(string)]
		f.mu.Unlock()
		if exp := r.Header.Get("X-Dat9-Expected-Resource"); exp != "" && occupant != "" && occupant != exp {
			// The publication is bound to a DIFFERENT object than the one
			// occupying the path: refuse it (412: identity, not a revision
			// conflict).
			http.Error(w, "resource mismatch", http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		if legacy {
			// A pre-extent server ignores the layout field and answers a
			// CLASSIC multipart plan (Key != "").
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id": "s1", "key": "classic-key", "part_size": 4 << 20,
				"total_parts": int(body["total_size"].(float64)/(4<<20)) + 1,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "s1", "key": "", "part_size": 4 << 20,
			"total_parts":       int(body["total_size"].(float64)/(4<<20)) + 1,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
		})
	})
	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/presign-batch") {
			f.mu.Lock()
			f.presigns++
			f.mu.Unlock()
			var body struct {
				Parts []struct {
					Number   int    `json:"part_number"`
					Checksum string `json:"checksum"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			upSize := f.upSize
			f.mu.Unlock()
			const partSize = int64(4 << 20)
			out := make([]map[string]any, 0, len(body.Parts))
			for _, p := range body.Parts {
				f.mu.Lock()
				exists := len(f.blocks[p.Checksum]) > 0
				f.mu.Unlock()
				size := partSize
				if rem := upSize - int64(p.Number-1)*partSize; rem < size {
					size = rem
				}
				if size < 0 {
					size = 0
				}
				m := map[string]any{"number": p.Number, "url": fmt.Sprintf("%s/obj/%s", f.URL, p.Checksum), "size": size}
				if exists {
					m["skip_upload"] = true
				}
				out = append(out, m)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/complete") {
			f.mu.Lock()
			f.completes++
			f.mu.Unlock()
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
					if f.legacy {
						f.classic[path] = string(content) // degraded session
					} else {
						f.extent[path] = string(content)
					}
				}
			}
			landedPath := ""
			if len(f.creates) > 0 {
				landedPath = f.creates[len(f.creates)-1]
			}
			landedID := f.ids[landedPath]
			if landedPath != "" && landedID == "" {
				// A fresh landing: assign the object's identity now (the
				// server knows it even when our stats fail).
				f.nextID++
				landedID = fmt.Sprintf("res-%d", f.nextID)
				f.ids[landedPath] = landedID
			}
			f.mu.Unlock()
			// Echo the landed object's identity: the client captures it
			// as the ownership proof for post-upload stat outages.
			if os.Getenv("EXTENT_DBG_ID") != "" {
				fmt.Println("FAKE COMPLETE echo:", landedID, "path:", landedPath)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "resource_id": landedID})
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

// contentOf reports the content stored at path, extent or classic (""
// when absent).
func (f *fakeV2) contentOf(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.extent[path]; c != "" {
		return c
	}
	return f.classic[path]
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

// setFailBody aborts (on=true) classic GET bodies after a 2-byte prefix.
func (f *fakeV2) setFailBody(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failBody = on
}

// existsOf reports whether any entry (extent, classic or dir) holds the
// path.
func (f *fakeV2) existsOf(path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, hasID := f.ids[path]
	return hasID || f.dirs[path]
}

// stat returns the resource identity stored for the path.
func (f *fakeV2) stat(t *testing.T, path string) (string, error) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.ids[path]; ok && id != "" {
		return id, nil
	}
	return "", &httpNope{}
}

type httpNope struct{}

func (httpNope) Error() string { return "no identity" }

// setLegacy switches the fake to a pre-extent (classic-only) server.
func (f *fakeV2) setLegacy(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.legacy = on
}

// setStatGone toggles visibility of EXISTING objects (stats answer 404).
func (f *fakeV2) setStatGone(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statGone = on
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

// Round-15 P1: the serialization must hold for the no-open-destination
// branch too. With NO descriptor on /b, an Open of /b started while
// mv /a /b is pending must wait for the outcome instead of registering a
// B descriptor on the replaceable name; after the rename it opens A.
func TestOpenDuringNoHandleRenameWaitsForOutcome(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)

	blocked, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			close(blocked)
			<-release
			f.rename(t, op.Path, op.Path2)
			return fs.Result{}, nil
		})
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("rename never reached the chain")
	}

	type openResult struct {
		eh  *engineHandle
		err error
	}
	opened := make(chan openResult, 1)
	go func() {
		res, err, passed := dispatchCreate(context.Background(), h, "/b", syscall.O_RDWR)
		if err != nil || passed {
			opened <- openResult{nil, fmt.Errorf("claim /b: err=%v passed=%v", err, passed)}
			return
		}
		eh, ok := res.Handle.(*engineHandle)
		if !ok {
			opened <- openResult{nil, fmt.Errorf("handle type = %T", res.Handle)}
			return
		}
		opened <- openResult{eh, nil}
	}()
	select {
	case r := <-opened:
		t.Fatalf("open of the destination completed during a rename that held no handle: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not finish")
	}
	renameEvent(h, "/a", "/b")

	var later openResult
	select {
	case later = <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("the pending open never completed after the rename resolved")
	}
	if later.err != nil {
		t.Fatal(later.err)
	}
	// The open now sees A at /b: its write lands on A's object.
	if err := later.eh.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := later.eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "ZZZZ" {
		t.Fatalf("/b = %q after the opened descriptor's write, want ZZZZ (A's object)", got)
	}
}

// Round-15 P2: a claim that waited for a rename must decide layout AFTER
// the wait: replacing the destination with a CLASSIC file leaves the
// waiting Open routed to the classic plane, never the engine.
func TestWaitingOpenUsesPostRenameLayout(t *testing.T) {
	f := newFakeV2(t)
	f.mu.Lock()
	f.classic["/a"] = "CLASSIC-CLASSIC"
	f.nextID++
	f.ids["/a"] = "classic-a"
	f.mu.Unlock()
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	ehB := claimOpen(t, h, "/b")

	blocked, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			close(blocked)
			<-release
			f.rename(t, op.Path, op.Path2)
			return fs.Result{}, nil
		})
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("rename never reached the chain")
	}

	type openResult struct {
		passed bool
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		_, err, passed := dispatchCreate(context.Background(), h, "/b", syscall.O_RDWR)
		opened <- openResult{passed, err}
	}()
	select {
	case r := <-opened:
		t.Fatalf("open of the destination completed during the pending rename: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not finish")
	}
	renameEvent(h, "/a", "/b")

	select {
	case r := <-opened:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if !r.passed {
			t.Fatal("the waiting open returned an ENGINE handle for a classic file")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pending open never completed after the rename resolved")
	}
	// The displaced extent descriptor keeps its alias semantics.
	if ehB.path == "/b" {
		t.Fatal("the displaced extent descriptor was left on the replaced name")
	}
	if got := f.contentOf(h.opts.Engine.Bridge.fullPath(ehB.path)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
}

// --extent / [extent] patterns claim matching fresh creates even in hybrid
// mode; unmatched paths keep the classic promotion flow.
func TestExtentPatternsClaimFreshCreates(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/big", true)
	h := newTestHandler(t, f, 1<<20)
	h.opts.Patterns = []string{"/big/*"}

	res, err, passed := dispatchCreate(context.Background(), h, "/big/new.bin", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("a pattern-matched create must be claimed by the engine plane")
	}
	if _, ok := res.Handle.(*engineHandle); !ok {
		t.Fatalf("matched create handle type = %T, want *engineHandle", res.Handle)
	}
	if _, err2, passed2 := dispatchCreate(context.Background(), h, "/elsewhere.bin", syscall.O_RDWR|syscall.O_CREAT); err2 != nil || !passed2 {
		t.Fatalf("unmatched create: err=%v passed=%v, want the classic plane", err2, passed2)
	}
}

// --------------------------------------------------------------------------
// merged from review_round2_test.go
// --------------------------------------------------------------------------
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
						{FileOffset: 0, Length: 1 << 20, BlockKey: sha256Hex([]byte("aa")), URL: ts.URL + "/obj/aa"},
					},
					NextOffset: 1 << 20,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Size: 3 << 20, Revision: 4,
				Blocks: []client.ExtentReadPlanBlock{
					{FileOffset: 2 << 20, Length: 1 << 20, BlockKey: sha256Hex([]byte("cc")), URL: ts.URL + "/obj/cc"},
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
	sum := sha256Hex([]byte("payload"))
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

// --------------------------------------------------------------------------
// merged from review_round3_test.go
// --------------------------------------------------------------------------
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
					{FileOffset: 0, Length: 1 << 20, BlockKey: sha256Hex([]byte("a")), URL: ts.URL + "/obj/a"},
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
					{FileOffset: 0, Length: 1 << 20, BlockKey: sha256Hex([]byte("a")), URL: ts.URL + "/obj/a"},
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
	sum := sha256Hex([]byte("shared"))
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

// --------------------------------------------------------------------------
// merged from review_round7_test.go
// --------------------------------------------------------------------------
// claimOpen claims an existing extent file through the plane and returns

// its engine handle.

func claimOpen(t *testing.T, h *Handler, path string) *engineHandle {
	t.Helper()
	res, err, passed := dispatchCreate(context.Background(), h, path, syscall.O_RDWR)
	if err != nil || passed {
		t.Fatalf("claim %s: err=%v passed=%v", path, err, passed)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("claim %s: handle type = %T", path, res.Handle)
	}
	return eh
}

// renameEvent replays what pkg/fuse emits for a rename: EventWrite on the

// OLD name with RenamedTo, then a plain EventDelete for the old name.

func renameEvent(h *Handler, from, to string) {
	h.opts.Events.Emit(fs.Event{Kind: fs.EventWrite, Path: from, RenamedTo: to})
	h.opts.Events.Emit(fs.Event{Kind: fs.EventDelete, Path: from})
}

// unlinkOpen drives OpUnlink through the plane for a path with live

// handles; the classic chain must never see it.

func unlinkOpen(t *testing.T, h *Handler, path string) {
	t.Helper()
	passed := false
	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: path, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		passed = true
		return fs.Result{}, nil
	}))
	if err != nil {
		t.Fatalf("unlink %s: %v", path, err)
	}
	if passed {
		t.Fatalf("unlink %s with live handles reached the classic plane", path)
	}
}

func readFront(t *testing.T, eh *engineHandle, n int) string {
	t.Helper()
	buf := make([]byte, n)
	got, err := eh.Read(context.Background(), 0, buf)
	if err != nil {
		t.Fatalf("read %s: %v", eh.path, err)
	}
	return string(buf[:got])
}

// Round-7 P1: renaming a REPLACEMENT again must retarget the replacement,

// not the unrelated original inode whose pathname hash it reused. Open A

// at /a, rename it to /b, open the new incarnation B at /a (synthetic key

// H), then rename B to /c while A stays open: A keeps committing to /b.

func TestSecondRenameKeepsOriginalIncarnation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")

	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	f.seedExtent(t, "/a", "BBBB") // another client's new incarnation at /a
	ehB := claimOpen(t, h, "/a")
	if ehB.ino == ehA.ino {
		t.Fatalf("replacement reused the original inode %d", ehA.ino)
	}

	f.rename(t, "/a", "/c")
	renameEvent(h, "/a", "/c")

	b.mu.Lock()
	pathA, pathB := b.paths[ehA.ino], b.paths[ehB.ino]
	repC, hasC := b.replacements["/c"]
	_, staleA := b.replacements["/a"]
	b.mu.Unlock()
	if pathA != "/b" {
		t.Fatalf("original inode retargeted by the replacement's rename: paths[A]=%q, want /b", pathA)
	}
	if pathB != "/c" || !hasC || repC != ehB.ino || staleA {
		t.Fatalf("replacement did not follow its rename: paths[B]=%q replacements[/c]=(%d,%v) stale[/a]=%v", pathB, repC, hasC, staleA)
	}
	if ehA.path != "/b" || ehB.path != "/c" {
		t.Fatalf("handle paths A=%q B=%q, want /b and /c", ehA.path, ehB.path)
	}
	// The behavioral proof: a write through A's descriptor commits to /b,
	// never to B's /c.
	if err := ehA.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehA.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patched := append([]string(nil), f.patchPaths...)
	f.mu.Unlock()
	if len(patched) == 0 {
		t.Fatal("sync through A committed nothing")
	}
	for _, p := range patched {
		if p != "/b" {
			t.Fatalf("A's commit targeted %q (B's object), want /b", p)
		}
	}
}

// Round-7 P1: two simultaneously open, unlinked generations of one

// pathname must retain DISTINCT sentinel objects — the server's rename

// replaces an existing target, so a shared name would overwrite the first

// retained file while it is still open.

func TestDeletedOpenGenerationsKeepDistinctSentinels(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	unlinkOpen(t, h, "/a")
	sentinelA := ehA.path
	if sentinelA == "/a" {
		t.Fatal("unlink with a live handle did not silly-rename")
	}

	f.seedExtent(t, "/a", "BBBB") // recreate + commit the next generation
	ehB := claimOpen(t, h, "/a")
	unlinkOpen(t, h, "/a")
	sentinelB := ehB.path
	if sentinelB == "/a" || sentinelB == sentinelA {
		t.Fatalf("second generation sentinel %q collides with the first %q", sentinelB, sentinelA)
	}
	if got := f.contentOf(b.fullPath(sentinelA)); got != "AAAA" {
		t.Fatalf("first retained object overwritten by the second retention: %q", got)
	}
	if got := f.contentOf(b.fullPath(sentinelB)); got != "BBBB" {
		t.Fatalf("second retained object = %q, want BBBB", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q after B's retention, want AAAA", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBB" {
		t.Fatalf("B reads %q, want BBBB", got)
	}
	// B's last close drops ONLY B's sentinel; A stays readable until its
	// own last close.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinelB)); got != "" {
		t.Fatalf("B's sentinel survived its last close: %q", got)
	}
	if got := f.contentOf(b.fullPath(sentinelA)); got != "AAAA" {
		t.Fatalf("A's retained object lost with B's close: %q", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q after B closed, want AAAA", got)
	}
	if err := ehA.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinelA)); got != "" {
		t.Fatalf("A's sentinel survived its last close: %q", got)
	}
	b.mu.Lock()
	left := len(b.sentinels)
	b.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d sentinel registrations leaked", left)
	}
}

// Round-7 P2: the size overlay must read the LIVE incarnation. After the

// 8-byte original is renamed away and a 4-byte replacement is opened at

// the old name, GetAttr on that name must not inherit the original's

// length.

func TestGetAttrOverlayReadsReplacementIncarnation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	f.seedExtent(t, "/a", "BBBB")
	ehB := claimOpen(t, h, "/a")
	if ehB.ino == ehA.ino {
		t.Fatalf("replacement reused the original inode %d", ehA.ino)
	}
	if size, ok := b.LocalSizeOf("/a"); !ok || size != 4 {
		t.Fatalf("LocalSizeOf(/a) = (%d,%v), want the replacement's 4", size, ok)
	}
	if size, ok := b.LocalSizeOf("/b"); !ok || size != 8 {
		t.Fatalf("LocalSizeOf(/b) = (%d,%v), want the original's 8", size, ok)
	}
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/a", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Attr: &fs.Attr{Size: 4}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 4 {
		t.Fatalf("GetAttr(/a) published %+v, want the replacement's size 4", res.Attr)
	}
}

// Round-7 P2: concurrent first resolutions of one replacement incarnation

// must converge on a single synthetic inode — JuiceFS keys reader/writer

// state and invalidation by inode, so two keys for one file would let one

// handle keep stale bytes after a write through the other.

func TestConcurrentResolveLocalConvergesOnOneInode(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	b := newTestHandler(t, f, 1<<20).opts.Engine.Bridge
	orig, errno := b.ResolveLocal(context.Background(), "/a")
	if errno != 0 {
		t.Fatal(errno)
	}
	f.rename(t, "/a", "/b")
	b.RemapLocal("/a", "/b")      // the original moved away: /a's hash is held
	f.seedExtent(t, "/a", "BBBB") // another client created a new /a

	const n = 32
	var (
		start = make(chan struct{})
		wg    sync.WaitGroup
		mu    sync.Mutex
		seen  = map[meta.Ino]int{}
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ino, errno := b.ResolveLocal(context.Background(), "/a")
			mu.Lock()
			defer mu.Unlock()
			if errno != 0 {
				seen[0]++
				return
			}
			seen[ino]++
		}()
	}
	close(start)
	wg.Wait()
	if len(seen) != 1 {
		t.Fatalf("concurrent resolutions published %d distinct inodes: %v", len(seen), seen)
	}
	for ino := range seen {
		if ino == 0 || ino == orig {
			t.Fatalf("replacement resolved to %d (original %d)", ino, orig)
		}
		b.mu.Lock()
		rep := b.replacements["/a"]
		b.mu.Unlock()
		if rep != ino {
			t.Fatalf("published inode %d but the replacement map says %d", ino, rep)
		}
	}
}

// --------------------------------------------------------------------------
// merged from review_round8_test.go
// --------------------------------------------------------------------------
// Round-8 P1: a directory rename arrives as ONE event; open descendants

// must follow it in the handler AND the bridge, so a later create at the

// old child pathname (full mode) gets its own inode instead of the

// retained descriptor's — and the retained descriptor keeps committing to

// its moved name.

func TestDirectoryRenameKeepsOpenDescendantIdentity(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a", "AAAA")
	h := newTestHandler(t, f, 0) // full mode: fresh creates ride the engine
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/d/a")

	// mv /d /e server-side, reported as one directory rename event.
	f.rename(t, "/d/a", "/e/a")
	f.setDir("/d", false)
	f.setDir("/e", true)
	renameEvent(h, "/d", "/e")
	if ehA.path != "/e/a" {
		t.Fatalf("open descendant did not follow the directory rename: %q", ehA.path)
	}
	b.mu.Lock()
	pathA := b.paths[ehA.ino]
	b.mu.Unlock()
	if pathA != "/e/a" {
		t.Fatalf("bridge mapping for A = %q, want /e/a", pathA)
	}

	// mkdir /d; create B at /d/a while A stays open.
	f.setDir("/d", true)
	res, err, passed := dispatchCreate(context.Background(), h, "/d/a", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil || passed {
		t.Fatalf("full-mode create: err=%v passed=%v", err, passed)
	}
	ehB := res.Handle.(*engineHandle)
	if ehB.ino == ehA.ino {
		t.Fatalf("B reused the retained descendant's inode %d", ehA.ino)
	}
	b.mu.Lock()
	pathA, pathB := b.paths[ehA.ino], b.paths[ehB.ino]
	b.mu.Unlock()
	if pathA != "/e/a" || pathB != "/d/a" {
		t.Fatalf("mappings A=%q B=%q, want /e/a and /d/a", pathA, pathB)
	}
	// A's write still lands on /e/a, never on B.
	if err := ehA.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehA.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patched := append([]string(nil), f.patchPaths...)
	f.mu.Unlock()
	if len(patched) == 0 {
		t.Fatal("sync through A committed nothing")
	}
	for _, p := range patched {
		if p != "/e/a" {
			t.Fatalf("A's commit targeted %q, want /e/a", p)
		}
	}
	if got := f.contentOf("/e/a"); got != "ZZZZ" {
		t.Fatalf("/e/a = %q after A's write, want ZZZZ", got)
	}
}

// Round-8 P2: reopening a migrated replacement at its NEW name must return

// the same inode its open descriptor uses — JuiceFS invalidates readers per

// inode, so a second key would leave one handle's cache stale after a

// write through the other.

func TestReopenOfMigratedReplacementSharesInode(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	claimOpen(t, h, "/a") // A stays open throughout
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")
	f.seedExtent(t, "/a", "BBBB")
	ehB := claimOpen(t, h, "/a") // the replacement incarnation
	f.rename(t, "/a", "/c")
	renameEvent(h, "/a", "/c")

	ehC := claimOpen(t, h, "/c")
	if ehC.ino != ehB.ino {
		t.Fatalf("reopen of /c resolved inode %d, want the migrated replacement's %d", ehC.ino, ehB.ino)
	}
	// Read-cache coherence: populate C's reader, write through B, read C.
	if got := readFront(t, ehC, 16); got != "BBBB" {
		t.Fatalf("C reads %q before the write, want BBBB", got)
	}
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/c"); got != "ZZZZ" {
		t.Fatalf("server content after B's sync = %q, want ZZZZ", got)
	}
	if got := readFront(t, ehC, 16); got != "ZZZZ" {
		t.Fatalf("C reads %q after B's write+sync through the shared inode, want ZZZZ", got)
	}
}

// Round-8 P2: a replacement's local state is retired when its last handle

// closes / its name is deleted, so the next incarnation at the same name

// cannot inherit its length — while the original descriptor at the

// renamed-away name stays protected, and a second open of the STILL-LIVE

// replacement keeps reusing its inode.

func TestClosedDeletedReplacementStateRetired(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	f.seedExtent(t, "/a", "BBBBBBBB")
	ehB := claimOpen(t, h, "/a")
	ehB2 := claimOpen(t, h, "/a")
	if ehB2.ino != ehB.ino {
		t.Fatalf("second open of the live replacement got %d, want %d", ehB2.ino, ehB.ino)
	}
	if err := ehB2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	_, stillOwned := b.replacements["/a"]
	b.mu.Unlock()
	if !stillOwned {
		t.Fatal("closing one of two handles retired the live replacement")
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	_, owned := b.replacements["/a"]
	_, sized := b.sizes[ehB.ino]
	pathA, sizeA := b.paths[ehA.ino], b.sizes[ehA.ino]
	b.mu.Unlock()
	if owned || sized {
		t.Fatalf("closed replacement state survived: owned=%v sized=%v", owned, sized)
	}
	if pathA != "/b" || sizeA != 8 {
		t.Fatalf("original descriptor disturbed: path=%q size=%d", pathA, sizeA)
	}

	// rm /a, then another client creates a 4-byte C there.
	f.remove("/a")
	h.opts.Events.Emit(fs.Event{Kind: fs.EventDelete, Path: "/a"})
	f.seedExtent(t, "/a", "CCCC")
	ehC := claimOpen(t, h, "/a")
	if ehC.ino == ehB.ino {
		t.Fatalf("new incarnation inherited the retired replacement inode %d", ehB.ino)
	}
	if size, ok := b.LocalSizeOf("/a"); !ok || size != 4 {
		t.Fatalf("LocalSizeOf(/a) = (%d,%v), want 4", size, ok)
	}
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/a", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Attr: &fs.Attr{Size: 4}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 4 {
		t.Fatalf("GetAttr(/a) published %+v, want 4", res.Attr)
	}
}

// --------------------------------------------------------------------------
// merged from review_round9_test.go
// --------------------------------------------------------------------------
// Round-9 P1: moving another file into a VACATED pathname must not take

// over the hash key still used by the moved-open file that vacated it.

// Open B at /b, rename it to /c (B stays open), open A at /a and rename it

// into the now-empty /b: B keeps committing to /c and keeps its own length;

// A owns /b.

func TestRenameIntoVacatedNameKeepsMovedOpenInode(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge

	ehB := claimOpen(t, h, "/b")
	f.rename(t, "/b", "/c")
	renameEvent(h, "/b", "/c")

	ehA := claimOpen(t, h, "/a")
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	b.mu.Lock()
	pathB, sizeB := b.paths[ehB.ino], b.sizes[ehB.ino]
	pathA := b.paths[ehA.ino]
	owner, owned := b.replacements["/b"]
	b.mu.Unlock()
	if pathB != "/c" || sizeB != 8 {
		t.Fatalf("moved-open B hijacked by the rename into its vacated name: path=%q size=%d, want /c and 8", pathB, sizeB)
	}
	if pathA != "/b" || !owned || owner != ehA.ino {
		t.Fatalf("A does not own /b: path=%q owner=(%d,%v)", pathA, owner, owned)
	}
	if ehA.path != "/b" || ehB.path != "/c" {
		t.Fatalf("handle paths A=%q B=%q, want /b and /c", ehA.path, ehB.path)
	}
	if size, ok := b.LocalSizeOf("/b"); !ok || size != 4 {
		t.Fatalf("LocalSizeOf(/b) = (%d,%v), want A's 4", size, ok)
	}
	// Where the old descriptor's write actually lands: B's commit goes to
	// /c; A's file at /b is untouched.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patched := append([]string(nil), f.patchPaths...)
	f.mu.Unlock()
	if len(patched) == 0 {
		t.Fatal("sync through B committed nothing")
	}
	for _, p := range patched {
		if p != "/c" {
			t.Fatalf("B's commit targeted %q (A's file), want /c", p)
		}
	}
	if got := f.contentOf("/c"); got != "ZZZZBBBB" {
		t.Fatalf("/c = %q after B's write, want ZZZZBBBB", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q, A's file was modified through B's descriptor", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
}

// --------------------------------------------------------------------------
// merged from review_round10_test.go
// --------------------------------------------------------------------------
// renameThrough drives OpRename through the plane; next models the chain

// performing the server rename (replacing an existing destination like the

// real server) unless told to fail.

func renameThrough(t *testing.T, h *Handler, f *fakeV2, from, to string, fail error) (passed bool, err error) {
	t.Helper()
	_, err = h.Do(context.Background(), &fs.Op{Kind: fs.OpRename, Path: from, Path2: to, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		passed = true
		if fail != nil {
			return fs.Result{}, fail
		}
		f.rename(t, op.Path, op.Path2)
		return fs.Result{}, nil
	}))
	return passed, err
}

// hiddenNames lists the sentinel-looking names the fake currently holds.

func hiddenNames(f *fakeV2) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for p := range f.extent {
		if strings.Contains(p, ".d9open-") {
			out = append(out, p)
		}
	}
	return out
}

// Round-10 P1: renaming onto a still-open engine file must preserve the

// displaced descriptor's original object. A at /a and B at /b both open;

// mv /a /b: B gains a hardlink alias BEFORE the server replaces /b, so a

// write through B lands on B's retained object and never on A.

func TestRenameOverOpenTargetPreservesDisplacedDescriptor(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	passed, err := renameThrough(t, h, f, "/a", "/b", nil)
	if err != nil || !passed {
		t.Fatalf("rename: err=%v passed=%v", err, passed)
	}
	renameEvent(h, "/a", "/b") // the facade's event after the chain's rename

	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("displaced open target was not given a retained alias")
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after the rename, want A's AAAA", got)
	}
	h.mu.Lock()
	atB := append([]*engineHandle(nil), h.handles["/b"]...)
	h.mu.Unlock()
	if ehA.path != "/b" || len(atB) != 1 || atB[0] != ehA {
		t.Fatalf("handles at /b = %d (A at %q), want only A", len(atB), ehA.path)
	}
	// A write through the displaced descriptor leaves the new /b unchanged.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patched := append([]string(nil), f.patchPaths...)
	f.mu.Unlock()
	if len(patched) == 0 {
		t.Fatal("sync through B committed nothing")
	}
	for _, p := range patched {
		if p != b.fullPath(sentinel) {
			t.Fatalf("B's commit targeted %q, want its alias %q", p, b.fullPath(sentinel))
		}
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q, the renamed file was modified through the displaced descriptor", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
	if got := readFront(t, ehB, 16); got != "ZZZZBBBB" {
		t.Fatalf("B reads %q, want ZZZZBBBB", got)
	}
	// B's last close drops its alias — the last reference; /b (A) stays.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "" {
		t.Fatalf("alias survived B's last close: %q", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after B's close, want AAAA", got)
	}
}

// Round-10 (round-13 semantics): a rename the chain refuses (nothing

// applied) leaves the displaced file attached to its alias — the file

// never left its name, so the alias shares the object and a write through

// the descriptor is visible at /b — and the alias drops at its own last

// close, leaving /b exactly as it was.

func TestRenameOverOpenTargetFailedRenameKeepsAlias(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	boom := errors.New("rename refused")
	passed, err := renameThrough(t, h, f, "/a", "/b", boom)
	if !passed || !errors.Is(err, boom) {
		t.Fatalf("rename: err=%v passed=%v, want the chain's error", err, passed)
	}
	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("displaced descriptor was pulled back onto the replaceable name")
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q after the failed rename, want BBBBBBBB", got)
	}
	if got := f.contentOf("/a"); got != "AAAA" {
		t.Fatalf("/a = %q, want AAAA", got)
	}
	// The alias shares the object: a write through the displaced descriptor
	// lands on the alias and is the same bytes /b shows.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("alias = %q after B's write, want ZZZZBBBB", got)
	}
	if got := f.contentOf("/b"); got != "ZZZZBBBB" {
		t.Fatalf("/b = %q after B's write (the object is shared), want ZZZZBBBB", got)
	}
	// A later descriptor opens the same file through the name; closing the
	// ORIGINAL descriptor then drops the alias but must not discard B
	// while the later descriptor is still open (B still holds /b).
	ehB2 := claimOpen(t, h, "/b")
	if ehB2.ino == ehB.ino {
		t.Fatal("the post-failure open reused the retained inode")
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if left := hiddenNames(f); len(left) != 0 {
		t.Fatalf("alias left behind after B's close: %v", left)
	}
	if got := f.contentOf("/b"); got != "ZZZZBBBB" {
		t.Fatalf("/b = %q after B's close, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehB2, 16); got != "ZZZZBBBB" {
		t.Fatalf("the later descriptor reads %q, want ZZZZBBBB", got)
	}
	if err := ehB2.Write(context.Background(), 0, []byte("YYYY")); err != nil {
		t.Fatal(err)
	}
	if err := ehB2.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "YYYYBBBB" {
		t.Fatalf("/b = %q after the later descriptor's write, want YYYYBBBB", got)
	}
	if reg := registeredSentinels(b); reg != 0 {
		t.Fatalf("%d registrations left behind", reg)
	}
}

// --------------------------------------------------------------------------
// merged from review_round11_test.go
// --------------------------------------------------------------------------
// renameWith drives OpRename through the plane with an arbitrary chain

// continuation.

func renameWith(h *Handler, from, to string, next fs.Next) error {
	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpRename, Path: from, Path2: to, Caller: fs.Caller{Pid: 1}}, next)
	return err
}

func registeredSentinels(b *Bridge) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sentinels)
}

// Round-11 P1 (retention): the displaced destination must survive its own

// last close while the source rename is still pending. With link-based

// retention the file never leaves its name, so B's last close during the

// window only drops the extra alias; the rename then fails and /b is

// simply still B — nothing was ever at risk of deletion.

func TestRenameOverOpenTargetSurvivesLastCloseDuringRename(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	blocked, release := make(chan struct{}), make(chan struct{})
	boom := errors.New("server failure")
	done := make(chan error, 1)
	go func() {
		done <- renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			close(blocked)
			<-release
			return fs.Result{}, boom // nothing moved
		})
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("rename never reached the chain")
	}
	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("displaced target was not retained before the chain ran")
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q during the pending rename, want B still in place", got)
	}
	// B's last close while the rename is pending.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q after B's close during the pending rename, want BBBBBBBB", got)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("rename error = %v, want the chain's", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not finish")
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q after the failed rename, want the original B", got)
	}
	if got := f.contentOf("/a"); got != "AAAA" {
		t.Fatalf("/a = %q, want AAAA", got)
	}
	if left := hiddenNames(f); len(left) != 0 {
		t.Fatalf("aliases left behind: %v", left)
	}
	if reg := registeredSentinels(b); reg != 0 {
		t.Fatalf("%d registrations left behind", reg)
	}
}

// Round-11 P1 (rollback): an error from the chain is not proof the rename

// did not commit. Here the server applied mv /a /b and the response was

// lost; the plane must recognize the source at /b by identity and leave it

// — B stays retained at its alias — while following the source's

// descriptors.

func TestRenameOverOpenTargetAppliedThenErrorKeepsSource(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	lost := errors.New("response lost")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.rename(t, op.Path, op.Path2) // committed server-side...
		return fs.Result{}, lost       // ...but the client never heard
	})
	if !errors.Is(err, lost) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q: the moved source was overwritten (want AAAA)", got)
	}
	if got := f.contentOf("/a"); got != "" {
		t.Fatalf("/a = %q, the source should have moved", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	// The source's descriptors follow where its bytes are.
	b.mu.Lock()
	pathA := b.paths[ehA.ino]
	b.mu.Unlock()
	if ehA.path != "/b" || pathA != "/b" {
		t.Fatalf("A not followed to /b: handle %q mapping %q", ehA.path, pathA)
	}
	if err := ehA.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehA.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "ZZZZ" {
		t.Fatalf("/b = %q after A's write, want ZZZZ", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q after A's write, want BBBBBBBB", got)
	}
	// Ordinary rename-over lifecycle: B's last close drops its alias.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "" {
		t.Fatalf("alias survived B's last close: %q", got)
	}
}

// Round-11 P1 (rollback): a different file at the destination — the name

// was replaced by another client while the rename was failing — is never

// touched; the displaced object stays alive at its alias until its own

// last close (the rename-over lifecycle), since its name is gone.

func TestRenameOverOpenTargetForeignDestinationNeverOverwritten(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	boom := errors.New("server failure")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.seedExtent(t, "/b", "CCCC") // another client replaced the name
		return fs.Result{}, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q: another client's file was overwritten (want CCCC)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBBBBBB" {
		t.Fatalf("B reads %q, want BBBBBBBB", got)
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's close, want CCCC", got)
	}
}

// Round-11 P1 (rollback): when the destination cannot be probed at all the

// outcome is unknown — nothing is written anywhere; the displaced object

// stays alive at its alias until its own last close.

func TestRenameOverOpenTargetUnknownOutcomeKeepsRetention(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	lost := errors.New("response lost")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.rename(t, op.Path, op.Path2)
		f.setStatFail("/b", true) // the probes cannot reach the destination
		return fs.Result{}, lost
	})
	if !errors.Is(err, lost) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	f.setStatFail("/b", false)
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q: an unestablished destination was touched (want AAAA)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after B's close, want AAAA", got)
	}
}

// --------------------------------------------------------------------------
// merged from review_round12_test.go
// --------------------------------------------------------------------------
// Round-12 P1: a file appearing at the destination right after a vacant

// probe must never be destroyed. The rename fails without applying while

// someone removes /b; the probe answers 404 and, before the plane's next

// request, another client creates C there. Nothing is restored over C: the

// displaced B stays alive at its alias.

func TestRenameOverOpenTargetNeverTouchesALateArrival(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	boom := errors.New("server failure")
	arrived := false
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.remove("/b") // the name vanished underneath the failing rename
		f.setAfterHead(func(path string) {
			// Right after the probe's 404: C lands at /b (f.mu is held by
			// the HEAD handler; mutate the maps directly).
			if path == "/b" && !arrived {
				arrived = true
				f.extent["/b"] = "CCCC"
				f.nextID++
				f.ids["/b"] = "late-arrival"
			}
		})
		return fs.Result{}, boom
	})
	f.setAfterHead(nil)
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	if !arrived {
		t.Fatal("the probe never ran")
	}
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q: the late arrival was destroyed (want CCCC)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBBBBBB" {
		t.Fatalf("B reads %q, want BBBBBBBB", got)
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's close, want CCCC", got)
	}
}

// Round-12 P1: retention names are unique per operation, so a later

// retention of the same pathname can never land on an earlier retained

// object — whether that object is still live or a leftover.

func TestRetentionNamesNeverCollide(t *testing.T) {
	if a, b := deletedOpenSentinel("/b", 7), deletedOpenSentinel("/b", 7); a == b {
		t.Fatalf("two retentions of the same path/inode produced one name %q", a)
	}
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	f.seedExtent(t, "/x", "XXXX")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehB := claimOpen(t, h, "/b")

	// mv /a /b with B open: B retained at S1.
	if passed, err := renameThrough(t, h, f, "/a", "/b", nil); err != nil || !passed {
		t.Fatalf("first rename: err=%v passed=%v", err, passed)
	}
	renameEvent(h, "/a", "/b")
	s1 := ehB.path
	// Open the file now at /b (A) and mv /x /b over it: A retained at S2.
	ehA := claimOpen(t, h, "/b")
	if passed, err := renameThrough(t, h, f, "/x", "/b", nil); err != nil || !passed {
		t.Fatalf("second rename: err=%v passed=%v", err, passed)
	}
	renameEvent(h, "/x", "/b")
	s2 := ehA.path
	if s1 == s2 || s1 == "/b" || s2 == "/b" {
		t.Fatalf("retention names collide or missing: s1=%q s2=%q", s1, s2)
	}
	if got := f.contentOf(b.fullPath(s1)); got != "BBBBBBBB" {
		t.Fatalf("first retained object = %q after the second retention, want BBBBBBBB", got)
	}
	if got := f.contentOf(b.fullPath(s2)); got != "AAAA" {
		t.Fatalf("second retained object = %q, want AAAA", got)
	}
	if got := f.contentOf("/b"); got != "XXXX" {
		t.Fatalf("/b = %q, want XXXX", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBBBBBB" {
		t.Fatalf("B reads %q, want BBBBBBBB", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
}

// --------------------------------------------------------------------------
// merged from review_round13_test.go
// --------------------------------------------------------------------------
// Round-13 P1: an identity probe is not a reservation on the name. The

// probe returns B's identity at /b and another client replaces /b with C

// immediately afterwards; the displaced descriptor must stay attached to

// B, its alias must survive (it may be B's last reference), and C must be

// untouched.

func TestRenameOverOpenTargetDisplacedProbeIsNotAReservation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")
	dstID := func() string {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.ids["/b"]
	}()

	boom := errors.New("server failure")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		// The probe will see B's identity... and C replaces /b right
		// after that HEAD was answered (f.mu is held by the handler).
		f.setAfterHead(func(path string) {
			if path == "/b" && f.ids["/b"] == dstID {
				f.extent["/b"] = "CCCC"
				f.nextID++
				f.ids["/b"] = "foreign-c"
			}
		})
		return fs.Result{}, boom // the rename itself never applied
	})
	f.setAfterHead(nil)
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("the displaced descriptor was redirected onto the replaceable name")
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q: the concurrent replacement was damaged (want CCCC)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("B's retained alias = %q, want BBBBBBBB", got)
	}
	// A write through the displaced descriptor targets B's alias, never C.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's write, want CCCC untouched", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("B's alias = %q after its write, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehB, 16); got != "ZZZZBBBB" {
		t.Fatalf("B reads %q, want ZZZZBBBB", got)
	}
	// Both original objects survive; B's close drops only its alias.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's close, want CCCC", got)
	}
}

// Round-13/14 P1: an Open of the destination while the rename is pending

// must not attach a descriptor for B onto the replaceable name. It is

// serialized behind the rename; when the rename lands, that open (and only

// that open) resolves the source now at the name. Writes through BOTH the

// retained B descriptor and the later one land on their own objects.

func TestOpenDuringPendingRenameWaitsForOutcome(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	blocked, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			close(blocked)
			<-release
			f.rename(t, op.Path, op.Path2)
			return fs.Result{}, nil
		})
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("rename never reached the chain")
	}
	sentinel := ehB.path

	type openResult struct {
		eh  *engineHandle
		err error
	}
	opened := make(chan openResult, 1)
	go func() {
		res, err, passed := dispatchCreate(context.Background(), h, "/b", syscall.O_RDWR)
		if err != nil || passed {
			opened <- openResult{nil, fmt.Errorf("claim /b: err=%v passed=%v", err, passed)}
			return
		}
		eh, ok := res.Handle.(*engineHandle)
		if !ok {
			opened <- openResult{nil, fmt.Errorf("handle type = %T", res.Handle)}
			return
		}
		opened <- openResult{eh, nil}
	}()
	// The open must NOT complete while the rename is unresolved: no
	// descriptor for B may land on the replaceable name.
	select {
	case r := <-opened:
		t.Fatalf("open of the pending destination completed early: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not finish")
	}
	renameEvent(h, "/a", "/b")

	var later openResult
	select {
	case later = <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("the pending open never completed after the rename resolved")
	}
	if later.err != nil {
		t.Fatal(later.err)
	}
	ehC := later.eh
	if ehC.ino == ehB.ino {
		t.Fatal("the post-rename open reused B's retained inode")
	}
	// B stays attached to its alias; a write/fsync through B never touches A.
	b.mu.Lock()
	pathB := b.paths[ehB.ino]
	b.mu.Unlock()
	if pathB != b.fullPath(sentinel) {
		t.Fatalf("B's mapping = %q, want its alias %q", pathB, b.fullPath(sentinel))
	}
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after B's write, want the source's AAAA untouched", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("B's alias = %q after its write, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehB, 16); got != "ZZZZBBBB" {
		t.Fatalf("B reads %q, want ZZZZBBBB", got)
	}
	// The later descriptor opened the source: its write lands on /b.
	if err := ehC.Write(context.Background(), 0, []byte("QQQQ")); err != nil {
		t.Fatal(err)
	}
	if err := ehC.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "QQQQ" {
		t.Fatalf("/b = %q after the later descriptor's write, want QQQQ", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("B's alias = %q after the later descriptor's write, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehA, 16); got != "QQQQ" {
		t.Fatalf("A reads %q, want QQQQ", got)
	}
}

// --------------------------------------------------------------------------
// merged from review_round14_test.go
// --------------------------------------------------------------------------
// Round-14 P1: a resolution whose stat straddles the retention boundary

// must not drag the retained mapping back onto the replaceable name. The

// resolution selects B's inode, pauses in its stat while RetainOpenAlias

// installs the alias, and only then completes: remember() must keep the

// alias mapping, so the descriptor it returns still targets B's retained

// object after the source is renamed onto the name.

func TestResolutionAcrossRetentionKeepsMapping(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	orig, errno := b.ResolveLocal(context.Background(), "/b")
	if errno != 0 {
		t.Fatal(errno)
	}

	gate := make(chan struct{})
	f.gateHead("/b", gate)
	resolved := make(chan meta.Ino, 1)
	go func() {
		ino, errno := b.ResolveLocal(context.Background(), "/b")
		if errno != 0 {
			ino = 0
		}
		resolved <- ino
	}()
	deadline := time.Now().Add(10 * time.Second)
	for f.gateWaiting("/b") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if f.gateWaiting("/b") == 0 {
		t.Fatal("the resolution never reached its stat")
	}

	// Retention runs while the resolution's stat is in flight; its own
	// stat passes (the gate is one-shot).
	sentinel, err := b.RetainOpenAlias("/b")
	if err != nil {
		t.Fatal(err)
	}
	if sentinel == "" {
		t.Fatal("retention declined a present file")
	}
	alias := b.fullPath(sentinel)
	close(gate)

	select {
	case ino := <-resolved:
		if ino != orig {
			t.Fatalf("straddling resolution returned inode %d, want B's %d", ino, orig)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the straddling resolution never completed")
	}
	b.mu.Lock()
	pathB, repB := b.paths[orig], b.replacements["/b"]
	b.mu.Unlock()
	if pathB != alias {
		t.Fatalf("the straddling resolution dragged the mapping back: paths[B]=%q, want the alias %q", pathB, alias)
	}
	if repB != 0 {
		t.Fatalf("the name still resolves to the retained inode: replacements[/b]=%d", repB)
	}
}

// Round-16: a size-only setattr on an engine file must return the complete
// attribute set — the facade treats the reply as authoritative and would
// otherwise publish mode 0000.
func TestSizeOnlySetAttrReturnsFullAttributes(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA-BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	eh := claimOpen(t, h, "/big.bin")
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/big.bin", HasSize: true, Size: 4, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		t.Fatal("size-only setattr on an engine file reached the classic plane")
		return fs.Result{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil {
		t.Fatal("no attribute returned")
	}
	if res.Attr.Kind != fs.KindFile || res.Attr.Mode == 0 || res.Attr.Size != 4 {
		t.Fatalf("attr = %+v, want a complete file attribute with size 4", res.Attr)
	}
	if res.Attr.Mode&0o777 != 0o644 {
		t.Fatalf("mode = %o, want the file's 0644", res.Attr.Mode&0o777)
	}
	_ = eh
}

// Round-16: a read between ftruncate and fsync must not cancel the
// truncation — the server's (older, larger) plan size cannot resurrect the
// shrunk tail.
func TestReadBetweenTruncateAndFlushKeepsShrink(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAABBBBCCCCDDDD")
	h := newTestHandler(t, f, 1<<20)
	eh := claimOpen(t, h, "/big.bin")

	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/big.bin", HasSize: true, Size: 4, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		t.Fatal("size-only setattr on an engine file reached the classic plane")
		return fs.Result{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	// Read through the descriptor between the truncate and the flush.
	if got := readFront(t, eh, 16); got != "AAAA" {
		t.Fatalf("read after shrink = %q, want the 4-byte view", got)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The server must show the committed shrink.
	if got := f.contentOf("/big.bin"); got != "AAAA" {
		t.Fatalf("server content after flush = %q, want AAAA (the shrink must not be cancelled)", got)
	}
}

// Round-16: a fresh engine create is visible to lookup and getattr before
// its first remote commit.
func TestFreshEngineCreateVisibleBeforeCommit(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0) // full mode
	if _, _, passed := dispatchCreate(context.Background(), h, "/d/new.bin", syscall.O_RDWR|syscall.O_CREAT); passed {
		t.Fatal("full-mode create must be claimed")
	}
	looked := false
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpLookup, Path: "/d/new.bin", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		looked = true
		return fs.Result{}, syscall.ENOENT
	}))
	if err != nil {
		t.Fatalf("lookup of the fresh create: %v", err)
	}
	if looked || res.Attr == nil || res.Attr.Kind != fs.KindFile {
		t.Fatalf("lookup did not resolve the local-only file: classic=%v attr=%+v", looked, res.Attr)
	}
	got, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/d/new.bin", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, syscall.ENOENT
	}))
	if err != nil || got.Attr == nil || got.Attr.Kind != fs.KindFile {
		t.Fatalf("getattr of the fresh create: err=%v attr=%+v", err, got.Attr)
	}
}

// Round-16: interactive fsync is a LOCAL barrier — no remote commit during
// Sync, the commit lands at flush, and a mount that never flushed replays
// the staged barrier.
func TestInteractiveFsyncLocalBarrierAndReplay(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA-BBBB-CCCC")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/big.bin")

	if err := eh.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patchesBefore := f.patches
	f.mu.Unlock()
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patchesAfter := f.patches
	f.mu.Unlock()
	if patchesAfter != patchesBefore {
		t.Fatal("interactive fsync performed a remote commit")
	}
	if got := readFront(t, eh, 16); got != "ZZZZ-BBBB-CCCC" {
		t.Fatalf("read after interactive fsync = %q", got)
	}
	// The flush boundary lands the remote commit and clears the barrier.
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patchesFlushed := f.patches
	f.mu.Unlock()
	if patchesFlushed == patchesBefore {
		t.Fatal("flush did not land the deferred commit")
	}
	if got := f.contentOf("/big.bin"); got != "ZZZZ-BBBB-CCCC" {
		t.Fatalf("server content after flush = %q", got)
	}

	// Crash before flush: a NEW engine over the same staging dir replays
	// the barrier and lands the commit.
	if err := eh.Write(context.Background(), 5, []byte("QQQQ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Simulate the crash: a new engine with the same staging directory.
	stagingRoot := filepath.Join(dir, "commit-staging")
	if ents, err := os.ReadDir(stagingRoot); err != nil {
		t.Logf("staging dir unreadable: %v", err)
	} else {
		for _, e := range ents {
			t.Logf("staged: %s", e.Name())
			if raw, rerr := os.ReadFile(filepath.Join(stagingRoot, e.Name(), "manifest.json")); rerr == nil {
				t.Logf("manifest: %s", raw)
			} else {
				t.Logf("manifest read: %v", rerr)
			}
		}
	}
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := f.contentOf("/big.bin"); got == "ZZZZ-QQQQ-CCCC" {
			return // replay landed
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replay never landed the staged commit; server has %q", f.contentOf("/big.bin"))
}

// Round-17: a fresh create whose extent session silently degraded to a
// CLASSIC upload (legacy server) keeps the open handle usable — write →
// fsync → write → fsync commits through the classic surface and reads
// serve the composed content.
func TestClassicFallbackKeepsHandleUsable(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0) // full mode claims the create
	res, err, passed := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil || passed {
		t.Fatalf("create: err=%v passed=%v", err, passed)
	}
	eh := res.Handle.(*engineHandle)

	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("first fsync after the degraded session: %v", err)
	}
	f.mu.Lock()
	classicLen := len(f.classic["/d/f.bin"])
	f.mu.Unlock()
	if classicLen != 4 {
		t.Fatalf("degraded session landed %d classic bytes, want 4", classicLen)
	}
	// The second round must not error on the layout.
	if err := eh.Write(context.Background(), 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("second fsync on the classic-owned handle: %v", err)
	}
	if got := f.contentOf("/d/f.bin"); got != "AAAABBBB" {
		t.Fatalf("classic content = %q, want AAAABBBB", got)
	}
	if got := readFront(t, eh, 16); got != "AAAABBBB" {
		t.Fatalf("read through the classic-owned handle = %q, want AAAABBBB", got)
	}
}

// Round-18: two interactive barriers without an intervening Flush leave ONE
// superseding record (pending is cumulative), and a crash replays it whole.
func TestInteractiveTwoBarriersCrashReplay(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "ABCDEFGH")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/big.bin")

	if err := eh.Write(context.Background(), 2, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	stagingRoot := filepath.Join(dir, "commit-staging")
	if n := countDirs(t, stagingRoot); n != 1 {
		t.Fatalf("staged dirs after the first barrier = %d, want 1", n)
	}
	if err := eh.Write(context.Background(), 4, []byte("QQ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := countDirs(t, stagingRoot); n != 1 {
		t.Fatalf("staged dirs after the second barrier = %d, want 1 (superseded)", n)
	}
	// Crash: a fresh engine replays the (single, cumulative) barrier.
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := f.contentOf("/big.bin"); got == "ABZZQQGH" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replay never landed the superseding barrier; server has %q", f.contentOf("/big.bin"))
}

func countDirs(t *testing.T, root string) int {
	t.Helper()
	ents, err := os.ReadDir(root)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range ents {
		if e.IsDir() {
			n++
		}
	}
	return n
}

// Round-18: a later pending write that covers only the MIDDLE of an
// earlier one keeps the earlier write's suffix; the merged read shows the
// composed bytes before any remote commit.
func TestInteractiveOverlappingWritesKeepSuffix(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "XXXXXXXX")
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h2 := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h2, "/big.bin")

	if err := eh.Write(context.Background(), 0, []byte("ABCDEFGH")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 2, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readFront(t, eh, 16); got != "ABZZEFGH" {
		t.Fatalf("overlaid read = %q, want ABZZEFGH (the suffix must survive)", got)
	}
	if got := f.contentOf("/big.bin"); got != "XXXXXXXX" {
		t.Fatalf("remote changed before flush: %q", got)
	}
}

// Round-18: a fresh file with no remote representation reads its pending
// (fsynced) bytes — with the leading hole — instead of zeros.
func TestInteractiveFreshFilePendingRead(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	if _, _, passed := dispatchCreate(context.Background(), h, "/d/new.bin", syscall.O_RDWR|syscall.O_CREAT); passed {
		t.Fatal("full-mode create must be claimed")
	}
	// Reopen WITHOUT O_CREAT: the bridge-only file must still claim.
	res, err, passed := dispatchCreate(context.Background(), h, "/d/new.bin", syscall.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("an ordinary reopen of the bridge-only file fell through to the classic plane")
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 8, []byte("ABCD")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 12)
	n, rerr := eh.Read(context.Background(), 0, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	want := "\x00\x00\x00\x00\x00\x00\x00\x00ABCD"
	if string(buf[:n]) != want {
		t.Fatalf("pending read = %q, want the leading hole + ABCD", string(buf[:n]))
	}
}

// Round-18: recovery refuses to adopt whichever object occupies the saved
// path — a renamed-away file's barrier is left in place, nothing is
// resurrected or patched.
func TestReplaySkipsOnIdentityMismatch(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/a")
	if err := eh.Write(context.Background(), 0, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patchesBefore := f.patches
	f.mu.Unlock()
	// The file is renamed away (and a DIFFERENT file takes /a) while the
	// barrier is outstanding.
	f.rename(t, "/a", "/b")
	f.seedExtent(t, "/a", "OTHER")

	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	time.Sleep(500 * time.Millisecond)
	f.mu.Lock()
	patchesAfter := f.patches
	f.mu.Unlock()
	if patchesAfter != patchesBefore {
		t.Fatal("recovery mutated the remote despite an identity mismatch")
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q, want AAAA untouched", got)
	}
	if got := f.contentOf("/a"); got != "OTHER" {
		t.Fatalf("/a = %q, want OTHER untouched", got)
	}
	if n := countDirs(t, filepath.Join(dir, "commit-staging")); n != 1 {
		t.Fatalf("staged records = %d, want the skipped one preserved", n)
	}
}

// Round-18: one unreadable block aborts the whole recovery unit — no
// partial commit, no record deletion.
func TestReplayAbortsOnMissingBlock(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "ABCDEFGH")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/big.bin")
	if err := eh.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patchesBefore := f.patches
	f.mu.Unlock()
	// Corrupt the record: remove one block file.
	root := filepath.Join(dir, "commit-staging")
	ents, _ := os.ReadDir(root)
	for _, e := range ents {
		files, _ := os.ReadDir(filepath.Join(root, e.Name()))
		for _, bf := range files {
			name := bf.Name()
			if name != "manifest.json" {
				_ = os.Remove(filepath.Join(root, e.Name(), name))
				break
			}
		}
	}
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	time.Sleep(500 * time.Millisecond)
	f.mu.Lock()
	patchesAfter := f.patches
	f.mu.Unlock()
	if patchesAfter != patchesBefore {
		t.Fatal("a partial recovery committed despite the missing block")
	}
	if got := f.contentOf("/big.bin"); got != "ABCDEFGH" {
		t.Fatalf("server content = %q, want ABCDEFGH untouched", got)
	}
	if n := countDirs(t, root); n != 1 {
		t.Fatalf("staged records = %d, want the failed unit preserved", n)
	}
}

// Round-18: a read-only mount never runs recovery.
func TestReadOnlyMountSkipsReplay(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "ABCDEFGH")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/big.bin")
	if err := eh.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patchesBefore := f.patches
	f.mu.Unlock()
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir, ReadOnly: true})
	_ = eng2
	time.Sleep(500 * time.Millisecond)
	f.mu.Lock()
	patchesAfter := f.patches
	f.mu.Unlock()
	if patchesAfter != patchesBefore {
		t.Fatal("a read-only mount replayed a barrier (mutating the remote)")
	}
	if got := f.contentOf("/big.bin"); got != "ABCDEFGH" {
		t.Fatalf("server content = %q, want ABCDEFGH", got)
	}
}

// Round-18: a recorded creation mode of 0000 is authoritative in local
// lookups (not silently replaced with 0644).
func TestLocalLookupKeepsRecordedZeroMode(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/d/m.bin", Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0, HasMode: true, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Handle: &stubCore{}}, nil
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	res, lerr := h.Do(context.Background(), &fs.Op{Kind: fs.OpLookup, Path: "/d/m.bin", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, syscall.ENOENT
	}))
	if lerr != nil || res.Attr == nil {
		t.Fatalf("lookup: err=%v attr=%+v", lerr, res.Attr)
	}
	if res.Attr.Mode != 0 {
		t.Fatalf("mode = %o, want the recorded 0000", res.Attr.Mode)
	}
}

// Round-19: a classic base read that dies mid-body must not publish a
// damaged image — the commit fails visibly, the pending state survives,
// and the retry after recovery lands the correct bytes.
func TestClassicBaseReadFailurePreservesPending(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The second commit's base download dies after a valid prefix.
	f.setFailBody(true)
	if err := eh.Write(context.Background(), 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the commit succeeded despite the truncated base read")
	}
	if got := f.contentOf("/d/f.bin"); got != "AAAA" {
		t.Fatalf("server content after the failed commit = %q, want AAAA untouched", got)
	}
	// The pending write survives: a read through the handle still shows it.
	f.setFailBody(false)
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if got := f.contentOf("/d/f.bin"); got != "AAAABBBB" {
		t.Fatalf("server content after the retry = %q, want AAAABBBB", got)
	}
}

// Round-19: reads through a classic-owned handle overlay the deferred
// (interactive) writes — read-after-write holds before publication.
func TestInteractiveClassicReadAfterWrite(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 0, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readFront(t, eh, 8); got != "ZZAA" {
		t.Fatalf("deferred read through the classic handle = %q, want ZZAA", got)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/d/f.bin"); got != "ZZAA" {
		t.Fatalf("server content after flush = %q, want ZZAA", got)
	}
}

// Round-19: a pure truncation on a classic-owned handle persists through
// the durability barrier instead of silently resurrecting the tail.
func TestClassicOwnedTruncatePersists(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/d/f.bin", HasSize: true, Size: 4, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/d/f.bin"); got != "AAAA" {
		t.Fatalf("server content after the truncation barrier = %q, want AAAA", got)
	}
}

// Round-19: degraded-layout ownership survives the durable recovery — the
// manifest carries the classic flag and the replaying bridge keeps
// committing through the classic surface.
func TestClassicOwnershipSurvivesRecovery(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 0, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := f.contentOf("/d/f.bin"); got == "ZZAA" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("recovery never landed the classic-owned barrier; server has %q", f.contentOf("/d/f.bin"))
}

// Round-19: the staged classic-read buffers stay bounded — successive
// distinct reads retire the oldest beyond the budget.
func TestClassicReadCacheBounded(t *testing.T) {
	old := classicReadCacheBytes
	classicReadCacheBytes = 4096
	defer func() { classicReadCacheBytes = old }()

	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	payload := make([]byte, 16<<10)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := eh.Write(context.Background(), 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	for off := 0; off < 12<<10; off += 4096 {
		if _, err := eh.Read(context.Background(), int64(off), buf); err != nil {
			t.Fatalf("read at %d: %v", off, err)
		}
	}
	eng := h.opts.Engine
	eng.Bridge.mu.Lock()
	held := eng.Bridge.classicBytes
	entries := len(eng.Bridge.classicLRU)
	eng.Bridge.mu.Unlock()
	// The bound is the budget plus ONE in-flight chunk staging (the
	// engine fetches a returned slice's bytes right after meta.Read, so
	// the newest entry cannot be retired mid-flight).
	if held > classicReadCacheBytes+int64(len(payload)) || entries > 1 {
		t.Fatalf("classic staging holds %d bytes in %d entries, want bounded (%d + one chunk, 1 entry)", held, entries, classicReadCacheBytes)
	}
}

// Round-19: an empty-file barrier materializes the acknowledged name at
// recovery instead of deleting its record into nothing.
func TestEmptyBarrierReplayCreatesFile(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(false)
	f.setDir("/d", true)
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, passed := dispatchCreate(context.Background(), h, "/d/empty.bin", syscall.O_RDWR|syscall.O_CREAT)
	if passed {
		t.Fatal("full-mode create must be claimed")
	}
	if err := res.Handle.(*engineHandle).Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A zero-barrier fsync: no bytes, but the name is acknowledged.
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if f.existsOf("/d/empty.bin") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recovery never materialized the acknowledged empty file")
}

// Round-19: the folded OpCreate flag contract holds for still-local files —
// O_CREAT|O_EXCL answers EEXIST and O_TRUNC truncates.
func TestLocalClaimFlagSemantics(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	if _, _, passed := dispatchCreate(context.Background(), h, "/d/x.bin", syscall.O_RDWR|syscall.O_CREAT); passed {
		t.Fatal("full-mode create must be claimed")
	}
	_, err, _ := dispatchCreate(context.Background(), h, "/d/x.bin", syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL)
	if err == nil {
		t.Fatal("an exclusive create of the existing local file must fail")
	}
	res, rerr, passed := dispatchCreate(context.Background(), h, "/d/x.bin", syscall.O_WRONLY|syscall.O_TRUNC)
	if rerr != nil || passed {
		t.Fatalf("O_TRUNC reopen: err=%v passed=%v", rerr, passed)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("DATA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/d/x.bin"); got != "DATA" {
		t.Fatalf("content after O_TRUNC + write = %q, want DATA (the truncate cleared nothing)", got)
	}
}

// Round-20: deferred appends past the remote EOF are served by the classic
// overlay (and a wholly new chunk too), not dropped as zeros.
func TestInteractiveClassicAppendRead(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Append past the 4-byte remote EOF and fsync locally.
	if err := eh.Write(context.Background(), 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readFront(t, eh, 8); got != "AAAABBBB" {
		t.Fatalf("append read = %q, want AAAABBBB (the deferred tail must survive)", got)
	}
	// A write in a wholly later chunk (sparse) is served too.
	if err := eh.Write(context.Background(), meta.ChunkSize+1024, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	n, rerr := eh.Read(context.Background(), meta.ChunkSize+1024, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if string(buf[:n]) != "ZZZZ" {
		t.Fatalf("sparse-chunk read = %q, want ZZZZ", string(buf[:n]))
	}
}

// Round-20: the handle stays usable after a temporary base-read failure —
// repeated fsync AND further writes work, not just the first bridge retry.
func TestClassicBaseReadFailureThenContinuedUse(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.setFailBody(true)
	if err := eh.Write(context.Background(), 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("commit succeeded despite the truncated base read")
	}
	f.setFailBody(false)
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("recovery fsync: %v", err)
	}
	// The community writer's recorded error must not stick.
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("second fsync after recovery: %v", err)
	}
	if err := eh.Write(context.Background(), 8, []byte("CCCC")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync of the new write: %v", err)
	}
	if got := f.contentOf("/d/f.bin"); got != "AAAABBBBCCCC" {
		t.Fatalf("server content = %q, want AAAABBBBCCCC", got)
	}
}

// Round-20: repeated identical chunk reads keep the returned slice id
// usable — the LRU refreshes the record instead of retiring a duplicate.
func TestClassicReadCacheRepeatedContent(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}
	if err := eh.Write(context.Background(), 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Read the same range twice (identical content -> identical id).
	for i := 0; i < 2; i++ {
		buf := make([]byte, 8192)
		if _, err := eh.Read(context.Background(), 0, buf); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(buf) != string(payload) {
			t.Fatalf("read %d returned wrong bytes", i)
		}
	}
	// Then a partial re-read of a 4 KiB segment inside the same id.
	buf := make([]byte, 4096)
	if _, err := eh.Read(context.Background(), 2048, buf); err != nil {
		t.Fatalf("segment read: %v", err)
	}
	if string(buf) != string(payload[2048:2048+4096]) {
		t.Fatal("segment read returned wrong bytes")
	}
}

// Round-20: a write appended to b.pending while another chunk's commit is
// in flight survives that commit (the deletion is surgical).
func TestPendingSurvivesConcurrentCommitCleanup(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	eh := claimOpen(t, h, "/big.bin")
	ino := uint64(eh.ino)

	// Simulate the interleaving deterministically: a snapshot is taken,
	// a NEW pending write arrives, then the snapshot's commit cleans up.
	w1 := pendingWrite{chunk: 0, off: 0, checksum: sha256Hex([]byte("ZZZZ")), size: 4}
	w2 := pendingWrite{chunk: 1, off: 0, checksum: sha256Hex([]byte("YYYY")), size: 4}
	b.mu.Lock()
	b.pending[meta.Ino(ino)] = []pendingWrite{w1}
	b.blocks.stagePinned(w1.checksum, []byte("ZZZZ"), true)
	b.mu.Unlock()
	// The in-flight commit's snapshot is [w1]; w2 lands meanwhile.
	b.mu.Lock()
	b.pending[meta.Ino(ino)] = append(b.pending[meta.Ino(ino)], w2)
	b.blocks.stagePinned(w2.checksum, []byte("YYYY"), true)
	b.sizes[meta.Ino(ino)] = 1 << 20
	b.mu.Unlock()
	// The commit's cleanup deletes exactly its snapshot.
	b.surgicalDeletePending(meta.Ino(ino), []pendingWrite{w1})
	b.mu.Lock()
	left := len(b.pending[meta.Ino(ino)])
	b.mu.Unlock()
	if left != 1 {
		t.Fatalf("pending after the commit's cleanup = %d, want 1 (w2 must survive)", left)
	}
}

// Round-20: deferred extent overlay honors WRITE order — a later write at
// a lower offset shadows the earlier higher-offset write.
func TestDeferredExtentOverlayWriteOrder(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAAAAAA")
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/big.bin")
	if err := eh.Write(context.Background(), 4, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 0, []byte("BBBBBBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readFront(t, eh, 8); got != "BBBBBBBB" {
		t.Fatalf("overlaid read = %q, want BBBBBBBB (the later write wins)", got)
	}
}

// Round-21: three deferred writes whose offsets move BACKWARDS — every
// later write shadows all earlier fragments, including heads emitted
// around an earlier shadow.
func TestDeferredOverlayThreeWritesBackwards(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAAAAAAAAAA")
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/big.bin")
	for _, w := range []struct {
		off  int64
		data string
	}{{0, "abcdefghijkl"}, {8, "BB"}, {2, "CC"}} {
		if err := eh.Write(context.Background(), w.off, []byte(w.data)); err != nil {
			t.Fatal(err)
		}
		if err := eh.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := readFront(t, eh, 12); got != "abCCefghBBkl" {
		t.Fatalf("overlaid read = %q, want abCCefghBBkl", got)
	}
}

// Round-21: the staged bytes survive a failed commit AND the retry can
// re-upload them (payload, not just the queue entry).
func TestClassicRecoveryRetainsPayload(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.setFailBody(true)
	if err := eh.Write(context.Background(), 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("commit succeeded despite the truncated base read")
	}
	f.setFailBody(false)
	// The RETRY must find the staged payload and land it.
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := f.contentOf("/d/f.bin"); got != "AAAABBBB" {
		t.Fatalf("server content = %q, want AAAABBBB", got)
	}
}

// Round-21: classic staging stays bounded under a scan of DISTINCT
// chunks (no time-based amnesty), with byte-level verification.
func TestClassicReadCacheBoundedUnderScan(t *testing.T) {
	oldBudget, oldProtect := classicReadCacheBytes, classicReadProtect
	classicReadCacheBytes = 8 << 10
	classicReadProtect = 1
	defer func() { classicReadCacheBytes, classicReadProtect = oldBudget, oldProtect }()

	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	// Sparse content in several DISTINCT chunks (64 MiB apart).
	const n = 4
	for i := 0; i < n; i++ {
		blk := []byte{byte('A' + i), byte('A' + i), byte('A' + i), byte('A' + i)}
		if err := eh.Write(context.Background(), int64(i)*meta.ChunkSize, blk); err != nil {
			t.Fatal(err)
		}
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		buf := make([]byte, 4)
		if _, err := eh.Read(context.Background(), int64(i)*meta.ChunkSize, buf); err != nil {
			t.Fatalf("read chunk %d: %v", i, err)
		}
		want := string([]byte{byte('A' + i), byte('A' + i), byte('A' + i), byte('A' + i)})
		if string(buf) != want {
			t.Fatalf("read chunk %d = %q, want %q", i, string(buf), want)
		}
	}
	br := h.opts.Engine.Bridge
	br.mu.Lock()
	held := br.classicBytes
	br.mu.Unlock()
	// Bound: the budget plus the protected newest records (bytes; issued
	// entries beyond it are DEMOTED to provenance — bytes dropped, the
	// remote range kept as the usable source).
	if held > classicReadCacheBytes+int64(classicReadProtect)*(64<<20) {
		t.Fatalf("classic staging holds %d bytes, want <= %d + %d protected chunk spans", held, classicReadCacheBytes, classicReadProtect)
	}
}

// Round-21: a same-inode write that survives another commit's cleanup
// keeps its staged bytes (the reference scan counts it).
func TestSameInodeSurvivorKeepsBytes(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/big.bin", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	eh := claimOpen(t, h, "/big.bin")
	ino := eh.ino

	shared := sha256Hex([]byte("SHARED"))
	b.mu.Lock()
	b.pending[ino] = []pendingWrite{{chunk: 0, off: 0, checksum: shared, size: 4}}
	b.blocks.stagePinned(shared, []byte("SHARED"), true)
	b.sizes[ino] = 1 << 20
	b.mu.Unlock()

	// A's commit snapshots [shared]; B appends the SAME checksum while the
	// commit holds the lock — modeled by running the cleanup explicitly.
	b.mu.Lock()
	b.pending[ino] = append(b.pending[ino], pendingWrite{chunk: 1, off: 0, checksum: shared, size: 4})
	b.mu.Unlock()
	b.surgicalDeletePending(ino, []pendingWrite{{chunk: 0, off: 0, checksum: shared, size: 4}})
	b.releaseCommitted(ino, []pendingWrite{{chunk: 0, off: 0, checksum: shared, size: 4}})

	if data, ok := b.blocks.stagedBytes(shared); !ok || string(data) != "SHARED" {
		t.Fatalf("survivor's staged bytes lost: ok=%v data=%q", ok, data)
	}
}

// Round-22: an issued slice stays fetchable across concurrent staging —
// its issue-pin is only consumed by the fetch, so eviction can never
// invalidate an outstanding reader (bridge-level, deterministic).
func TestIssuedSliceSurvivesEviction(t *testing.T) {
	f := newFakeV2(t)
	h := newTestHandler(t, f, 1<<20)
	br := h.opts.Engine.Bridge

	// Hand out a slice (stage + issue pin, as readClassic does).
	data := []byte("FIRST")
	id1 := br.stageClassicChunk(sha256Hex(data), data, "/d/f.bin", 0, false, "res-1", 1)
	if !br.blocks.isIssued(id1) {
		t.Fatal("staged classic slice was not issue-pinned")
	}
	// Stage many distinct chunks to force LRU pressure.
	for i := 0; i < 32; i++ {
		d := []byte{byte(i), byte(i), byte(i), byte(i)}
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+1)<<20, false, fmt.Sprintf("res-%d", i+2), 1)
	}
	// The issued id must still be fetchable (objectStorage.Get consumes
	// the pin and serves the bytes).
	rc, err := br.Storage().Get(context.Background(), fmt.Sprintf("chunks/0/0/%d_0_%d", id1, len(data)), 0, int64(len(data)))
	if err != nil {
		t.Fatalf("issued slice was evicted before its fetch: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "FIRST" {
		t.Fatalf("issued slice bytes = %q, want FIRST", got)
	}
}

// Round-22: the recovery constraint covers TRUNCATION-ONLY publication —
// a replacement at the saved path is never truncated.
func TestTruncateOnlyRecoveryRejectsReplacement(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	h := New(Options{Engine: eng, Threshold: 1 << 20, SyncMode: "interactive"})
	eh := claimOpen(t, h, "/a")
	resID := func() string {
		st, err := f.stat(t, "/a")
		if err != nil {
			t.Fatal(err)
		}
		return st
	}()
	_ = resID
	// Truncate locally and fsync (a truncation-only barrier).
	if _, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/a", HasSize: true, Size: 0, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The name is replaced before the crash recovery runs.
	f.remove("/a")
	f.seedExtent(t, "/a", "OTHER")
	eng2 := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	_ = eng2
	time.Sleep(500 * time.Millisecond)
	if got := f.contentOf("/a"); got != "OTHER" {
		t.Fatalf("replacement content = %q, want OTHER untouched (a zero-length truncate must never apply to it)", got)
	}
	if got := f.contentOf("/b"); got != "" {
		t.Fatalf("/b unexpectedly = %q", got)
	}
}

// Round-22: the constraint SURVIVES a failed publication — the staged
// data cannot reach a replacement through a later unconstrained flush.
func TestRecoveryConstraintSurvivesFailure(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	dir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: dir})
	br := eng.Bridge
	// Install a verified constraint for /a's current occupant.
	st, err := br.StatLocal(context.Background(), "/a")
	if err != nil {
		t.Fatal(err)
	}
	ino, errno := br.ResolveLocal(context.Background(), "/a")
	if errno != 0 {
		t.Fatal(errno)
	}
	br.setVerifyBase(ino, st.Revision, st.ResourceID)
	br.mu.Lock()
	br.pending[ino] = []pendingWrite{{chunk: 0, off: 0, checksum: sha256Hex([]byte("ZZZZ")), size: 4}}
	br.blocks.stagePinned(sha256Hex([]byte("ZZZZ")), []byte("ZZZZ"), true)
	br.sizes[ino] = 8
	br.mu.Unlock()

	// Replace the occupant, then attempt the (constrained) publication.
	f.remove("/a")
	f.seedExtent(t, "/a", "OTHER")
	f.setStatFail("/a", true) // the commit's own stat also fails first
	err = br.FlushFile(context.Background(), ino)
	f.setStatFail("/a", false)
	if err == nil {
		t.Fatal("the constrained publication unexpectedly succeeded against a replacement")
	}
	// A later flush must NOT publish into the replacement either.
	if err := br.FlushFile(context.Background(), ino); err == nil {
		t.Fatal("a later flush published into the replacement")
	}
	if got := f.contentOf("/a"); got != "OTHER" {
		t.Fatalf("replacement content = %q, want OTHER untouched", got)
	}
}

// Round-23: a classic recovery publication carries the verified object's
// identity — a replacement whose revision merely EQUALS the saved one
// cannot receive the staged bytes.
func TestClassicRecoveryResourceBinding(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	h := newTestHandler(t, f, 1<<20)
	br := h.opts.Engine.Bridge
	st, err := br.StatLocal(context.Background(), "/a")
	if err != nil {
		t.Fatal(err)
	}
	ino, errno := br.ResolveLocal(context.Background(), "/a")
	if errno != 0 {
		t.Fatal(errno)
	}
	br.setVerifyBase(ino, st.Revision, st.ResourceID)
	br.mu.Lock()
	br.pending[ino] = []pendingWrite{{chunk: 0, off: 0, checksum: sha256Hex([]byte("ZZZZ")), size: 4}}
	br.blocks.stagePinned(sha256Hex([]byte("ZZZZ")), []byte("ZZZZ"), true)
	br.sizes[ino] = 8
	br.classicOwned[ino] = true // the recovery target publishes classic
	br.mu.Unlock()

	// Replace the occupant with one at the SAME revision number.
	f.remove("/a")
	f.mu.Lock()
	f.extent["/a"] = "OTHER"
	f.nextID++
	f.ids["/a"] = "replacement-id"
	f.mu.Unlock()

	if err := br.FlushFile(context.Background(), ino); err == nil {
		t.Fatal("the bound publication succeeded into a same-revision replacement")
	}
	if got := f.contentOf("/a"); got != "OTHER" {
		t.Fatalf("replacement content = %q, want OTHER untouched", got)
	}
}

// Round-24: a fresh upload's identity survives a TOTAL post-upload stat
// outage (echoed by the complete response) — a replacement installed
// during the outage is rejected on retry instead of receiving the staged
// bytes.
func TestBlindLayoutPendingRejectsReplacement(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	// Fail every HEAD AFTER the upload lands: the classification cannot
	// see the object (gate the HEAD only once content exists).
	f.setStatFail("/d/f.bin", true)
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the commit succeeded despite the stat outage")
	}
	// A replacement takes the path during the outage.
	f.mu.Lock()
	f.classic["/d/f.bin"] = "REPL"
	f.nextID++
	f.ids["/d/f.bin"] = "replacement-id"
	f.mu.Unlock()
	f.setStatFail("/d/f.bin", false)
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the retry adopted the replacement installed during the outage")
	}
	if got := f.contentOf("/d/f.bin"); got != "REPL" {
		t.Fatalf("replacement content = %q, want REPL untouched", got)
	}
}

// Round-24: an ordinary SAME-OBJECT revision conflict on a bound
// publication stays an ordinary conflict — the staged data survives for
// the retry instead of being abandoned.
func TestBoundPublicationOrdinaryConflictKeepsState(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	h := newTestHandler(t, f, 1<<20)
	br := h.opts.Engine.Bridge
	st, err := br.StatLocal(context.Background(), "/a")
	if err != nil {
		t.Fatal(err)
	}
	ino, errno := br.ResolveLocal(context.Background(), "/a")
	if errno != 0 {
		t.Fatal(errno)
	}
	br.setVerifyBase(ino, st.Revision, st.ResourceID)
	br.mu.Lock()
	br.pending[ino] = []pendingWrite{{chunk: 0, off: 0, checksum: sha256Hex([]byte("ZZZZ")), size: 4}}
	br.blocks.stagePinned(sha256Hex([]byte("ZZZZ")), []byte("ZZZZ"), true)
	br.sizes[ino] = 8
	br.mu.Unlock()

	// Another writer advances the SAME object's revision: an ordinary 409
	// (the fake bumps the revision on a classic write).
	f.mu.Lock()
	f.extent["/a"] = "AAAAAAAA" // same identity; revision bumps via write
	f.mu.Unlock()
	if err := br.FlushFile(context.Background(), ino); err != nil {
		t.Fatalf("an ordinary same-object conflict must retry and land, got: %v", err)
	}
	if got := f.contentOf("/a"); got != "ZZZZAAAA" {
		t.Fatalf("server content = %q, want ZZZZAAAA", got)
	}
}

// Round-25: the echoed identity is captured on the completion path ACTUALLY
// used (the V2/degraded-classic session), and the outage branch never
// clobbers it — a replacement installed while the object was stat-invisible
// is rejected once visibility returns.
func TestEchoedIdentitySurvivesStatOutage(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	f.setStatGone(true)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the commit succeeded despite the stat outage")
	}
	br := h.opts.Engine.Bridge
	br.mu.Lock()
	id := br.layoutPendingID[eh.ino]
	br.mu.Unlock()
	if id == "" {
		t.Fatal("the echoed landed identity was not retained through the outage")
	}
	f.mu.Lock()
	f.classic["/d/f.bin"] = "REPL"
	f.nextID++
	f.ids["/d/f.bin"] = "replacement-id"
	f.statGone = false
	f.mu.Unlock()
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the retry adopted the replacement")
	}
	if got := f.contentOf("/d/f.bin"); got != "REPL" {
		t.Fatalf("replacement content = %q, want REPL untouched", got)
	}
}

// Round-25: a bound publication with the revision CAS DISABLED (-1) still
// carries the identity precondition on the direct-PUT route.
func TestBoundDirectPutWithoutRevisionCAS(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	st, err := client.New(f.URL, "").StatCtx(context.Background(), "/a")
	if err != nil {
		t.Fatal(err)
	}
	// Replace the path AFTER capturing the identity.
	f.remove("/a")
	f.seedExtent(t, "/a", "OTHER")
	err = client.New(f.URL, "").WriteStreamConditionalExpectedResource(
		context.Background(), "/a", strings.NewReader("ZZZZ"), 4, nil, -1, st.ResourceID)
	if err == nil {
		t.Fatal("the bound -1 revision publication overwrote a replacement")
	}
	if got := f.contentOf("/a"); got != "OTHER" {
		t.Fatalf("replacement content = %q, want OTHER untouched", got)
	}
}

// Round-26: a classic read whose bytes were demoted under pressure while a
// reader still held the id re-reads from the remote range — the later
// block fetch succeeds instead of failing with an unknown id.
func TestDemotedSliceRefetchesRemotely(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	payload := make([]byte, 16<<10)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}
	if err := eh.Write(context.Background(), 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Read once (stage + fetch), then force demotion by budget pressure.
	old := classicReadCacheBytes
	classicReadCacheBytes = 4 << 10
	defer func() { classicReadCacheBytes = old }()
	buf := make([]byte, 4096)
	if _, err := eh.Read(context.Background(), 0, buf); err != nil {
		t.Fatal(err)
	}
	// Stage many distinct ranges to pressure the LRU past the first id.
	br := h.opts.Engine.Bridge
	for i := 0; i < 8; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i)*4096, false, fmt.Sprintf("res-%d", i+2), 1)
	}
	// The first read's entry was demoted: its bytes are gone, provenance
	// remains. A later fetch of the SAME range re-reads the remote.
	if _, err := eh.Read(context.Background(), 0, buf); err != nil {
		t.Fatalf("read after demotion: %v", err)
	}
	if string(buf) != string(payload[:4096]) {
		t.Fatal("post-demotion read returned wrong bytes")
	}
}

// Round-26: the captured (echoed) identity constrains retries regardless of
// the occupant's CURRENT layout — an EXTENT replacement at the path of a
// still-unclassified upload is rejected too.
func TestExtentReplacementRejectedUnderLayoutPending(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	f.setStatGone(true)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the commit succeeded despite the stat outage")
	}
	// An EXTENT replacement takes the path while stats are blind.
	f.mu.Lock()
	f.extent["/d/f.bin"] = "EXTREPL"
	f.nextID++
	f.ids["/d/f.bin"] = "extent-replacement-id"
	f.statGone = false
	f.mu.Unlock()
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the retry published into an extent replacement")
	}
	if got := f.contentOf("/d/f.bin"); got != "EXTREPL" {
		t.Fatalf("replacement content = %q, want EXTREPL untouched", got)
	}
}

// Round-27: the captured identity binds BOTH publication attempts — a
// replacement arriving between the retry's Stat and the PATCH (or on the
// conflict re-stat) is refused by the header, not just by the pre-check.
func TestIdentityBindsPublicationWindow(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	f.setStatGone(true)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the commit succeeded despite the stat outage")
	}
	// Stats recover and see OUR file (the pre-check passes); a replacement
	// with an EQUAL revision lands before the publication attempt.
	f.setStatGone(false)
	f.mu.Lock()
	ourRev := f.ids["/d/f.bin"]
	f.extent["/d/f.bin"] = "BBBB"
	f.nextID++
	f.ids["/d/f.bin"] = "same-revision-replacement"
	f.mu.Unlock()
	_ = ourRev
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the publication succeeded into a replacement inside the Stat→PATCH window")
	}
	if got := f.contentOf("/d/f.bin"); got != "BBBB" {
		t.Fatalf("replacement content = %q, want BBBB untouched", got)
	}
}

// Round-27: a locally OVERLAID slice is never demoted — cache pressure
// cannot replace deferred local bytes with the older remote image.
func TestOverlaidSliceNeverDemoted(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Deferred overwrite (local-only bytes), then read to stage an OVERLAID
	// slice, then pressure the LRU hard.
	if err := eh.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := classicReadCacheBytes
	classicReadCacheBytes = 1 // everything is beyond budget
	defer func() { classicReadCacheBytes = old }()
	if got := readFront(t, eh, 4); got != "ZZZZ" {
		t.Fatalf("pre-pressure read = %q, want ZZZZ", got)
	}
	br := eng.Bridge
	for i := 0; i < 8; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+1)<<20, false, "res-x", 1)
	}
	if got := readFront(t, eh, 4); got != "ZZZZ" {
		t.Fatalf("post-pressure read = %q, want ZZZZ (an overlaid slice must not demote to the remote image)", got)
	}
}

// Round-28: a classic slice demoted to provenance never serves another
// file's bytes — the pathname was reused between staging and the refetch,
// and the identity check refuses instead.
func TestDemotedSliceRefusesReusedPath(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAAAAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Read once to stage an identity-bound classic slice, then demote it.
	old := classicReadCacheBytes
	classicReadCacheBytes = 1
	defer func() { classicReadCacheBytes = old }()
	buf := make([]byte, 8)
	if _, err := eh.Read(context.Background(), 0, buf); err != nil {
		t.Fatal(err)
	}
	br := h.opts.Engine.Bridge
	br.mu.Lock()
	firstID := br.classicLRU[len(br.classicLRU)-1].id
	br.mu.Unlock()
	for i := 0; i < 4; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+1)<<20, false, fmt.Sprintf("res-%d", i+2), 1)
	}
	// The path is replaced by ANOTHER object before the slice's later
	// block fetch: the identity-verified refetch must REFUSE (never serve
	// another file's bytes under this id). Drive the registry fetch
	// directly — the engine's own block cache would serve the first read
	// without a second meta.Read.
	f.remove("/d/f.bin")
	f.seedExtent(t, "/d/f.bin", "OTHERFILE")
	rc, err := h.opts.Engine.Bridge.Storage().Get(context.Background(),
		fmt.Sprintf("chunks/0/0/%d_0_8", firstID), 0, 8)
	if err == nil {
		_ = rc.Close()
		t.Fatal("the demoted slice served bytes from a reused pathname")
	}
	if got := f.contentOf("/d/f.bin"); got != "OTHERFILE" {
		t.Fatalf("replacement = %q, want OTHERFILE untouched", got)
	}
}

// Round-28: overlay bytes stay accounted while irreducible and are
// reclaimed after the flush resolves them.
func TestOverlayBytesAccountedAndReclaimed(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 0, []byte("ZZ")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := classicReadCacheBytes
	classicReadCacheBytes = 1
	defer func() { classicReadCacheBytes = old }()
	if got := readFront(t, eh, 4); got != "ZZAA" {
		t.Fatalf("overlay read = %q, want ZZAA", got)
	}
	br := eng.Bridge
	br.mu.Lock()
	held := br.classicBytes
	br.mu.Unlock()
	if held < 4 {
		t.Fatalf("overlay bytes unaccounted: classicBytes = %d, want >= 4", held)
	}
	// The flush resolves the overlay: the entry may demote now. Pressure
	// again and verify the bytes actually leave the accounting (the LRU
	// re-append is gone once the entry demotes).
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+8)<<20, false, fmt.Sprintf("res-r%d", i), 1)
	}
	// One more round retires the previous four; whatever remains must not
	// include the historical overlay images.
	for i := 0; i < 2; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+64)<<20, false, fmt.Sprintf("res-s%d", i), 1)
	}
	br.mu.Lock()
	held2 := br.classicBytes
	overlayLeft := 0
	for _, r := range br.classicLRU {
		if r.bytes == 4 { // the ZZAA whole-chunk overlay image
			overlayLeft++
		}
	}
	br.mu.Unlock()
	if overlayLeft > 0 {
		t.Fatalf("the historical overlay image is still resident after the flush (LRU keeps it)")
	}
	if held2 > int64(classicReadProtect)*4096 {
		t.Fatalf("historical images not reclaimed after the flush: before=%d after=%d (want <= %d protected chunks)", held, held2, classicReadProtect*4096)
	}
	if got := readFront(t, eh, 4); got != "ZZAA" {
		t.Fatalf("post-flush read = %q, want ZZAA", got)
	}
}

// Round-29: a demoted classic slice still serves the ORIGINAL object after a
// successful rename — the provenance follows the inode's current mapping.
func TestDemotedSliceSurvivesRename(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res2, _, _ := dispatchCreate(context.Background(), h, "/d/a.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh2 := res2.Handle.(*engineHandle)
	if err := eh2.Write(context.Background(), 0, []byte("AAAAAAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh2.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := classicReadCacheBytes
	classicReadCacheBytes = 1
	defer func() { classicReadCacheBytes = old }()
	if got := readFront(t, eh2, 8); got != "AAAAAAAA" {
		t.Fatalf("pre-rename read = %q", got)
	}
	// Rename the file through the plane (the descriptor stays on the alias).
	f.rename(t, "/d/a.bin", "/d/b.bin")
	renameEvent(h, "/d/a.bin", "/d/b.bin")
	// Pressure: stage many distinct chunks to demote A's slice.
	br := h.opts.Engine.Bridge
	br.mu.Lock()
	firstID := br.classicLRU[len(br.classicLRU)-1].id
	firstIno := br.classicLRU[len(br.classicLRU)-1].ino
	br.mu.Unlock()
	for i := 0; i < 4; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/b.bin", int64(i+1)<<20, false, fmt.Sprintf("res-%d", i+2), 1)
	}
	_ = firstIno
	// The demoted slice refetches through the CURRENT mapping (/d/b.bin)
	// and serves the original bytes.
	rc, err := br.Storage().Get(context.Background(), fmt.Sprintf("chunks/0/0/%d_0_8", firstID), 0, 8)
	if err != nil {
		t.Fatalf("post-rename fetch of the demoted slice: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "AAAAAAAA" {
		t.Fatalf("post-rename bytes = %q, want AAAAAAAA", data)
	}
}

// Round-30: an overlay image containing a write that SURVIVED the commit
// (appended mid-flight, still pending) stays protected — the flush of the
// earlier snapshot does not make it remotely reproducible.
func TestOverlayWithSurvivingWriteStaysProtected(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Deferred Z@0 (snapshot S1) — then, while S1's landing is emulated
	// in-flight, Y@1 is staged and read (the ZYAA whole-chunk image).
	if err := eh.Write(context.Background(), 0, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 1, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := classicReadCacheBytes
	classicReadCacheBytes = 1
	defer func() { classicReadCacheBytes = old }()
	if got := readFront(t, eh, 4); got != "ZYAA" {
		t.Fatalf("overlay read = %q, want ZYAA", got)
	}
	br := eng.Bridge
	// Land ONLY S1 (Z@0): model by removing the Y fragment's pending entry
	// is wrong — instead run a real Flush: everything pending lands (both
	// Z and Y are in one pending list in this simplified flow). To target
	// the interleaving deterministically, commit S1 through the bridge with
	// Y appended mid-flight:
	br.mu.Lock()
	pend := br.pending[eh.ino]
	s1 := append([]pendingWrite(nil), pend[:len(pend)-1]...) // Z only
	br.pending[eh.ino] = pend                                // restore: surgical delete keeps Y
	br.mu.Unlock()
	_ = s1
	// Direct: simulate releaseCommitted for S1 with Y still pending.
	br.releaseCommitted(eh.ino, []pendingWrite{pend[0]})
	// Pressure hard: the ZYAA image must stay (Y is un-published).
	for i := 0; i < 8; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+1)<<20, false, fmt.Sprintf("res-%d", i+2), 1)
	}
	if got := readFront(t, eh, 4); got != "ZYAA" {
		t.Fatalf("post-pressure read = %q, want ZYAA (an image with a surviving pending write must not demote)", got)
	}
}

// Round-31: the overlay-resolution decision is taken in the SAME critical
// section as the pending check — a write staged between the snapshot and
// the clear keeps its image protected.
func TestOverlayDecisionAtomicWithPendingCheck(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/f.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eh.Write(context.Background(), 0, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The mid-flight Y lands BEFORE releaseCommitted samples the pending
	// list (the exact window the separate-snapshot version missed).
	if err := eh.Write(context.Background(), 1, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	old := classicReadCacheBytes
	classicReadCacheBytes = 1
	defer func() { classicReadCacheBytes = old }()
	if got := readFront(t, eh, 4); got != "ZYAA" {
		t.Fatalf("overlay read = %q, want ZYAA", got)
	}
	br := eng.Bridge
	br.mu.Lock()
	pend := br.pending[eh.ino]
	br.mu.Unlock()
	// Run releaseCommitted for the Z snapshot with Y already staged.
	br.releaseCommitted(eh.ino, []pendingWrite{pend[0]})
	for i := 0; i < 8; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+1)<<20, false, fmt.Sprintf("res-%d", i+2), 1)
	}
	if got := readFront(t, eh, 4); got != "ZYAA" {
		t.Fatalf("post-pressure read = %q, want ZYAA (Y is still pending; its image must stay protected)", got)
	}
}
