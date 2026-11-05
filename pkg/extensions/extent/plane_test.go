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

	readEchoOff      bool   // classic GETs omit the identity echo (redirect shape); HEADs keep it
	completeEcho     bool   // complete responses echo the landed identity (default on)
	readEchoOverride string // classic GETs echo this instead of the stored id
	readBodyOverride string // classic GETs serve these bytes instead of the stored content
	// afterComplete runs (under f.mu) right after a completion landed its
	// bytes: the window between our upload landing and the classification
	// Stat, where another client can replace the name.
	afterComplete func(path string)
	// createEcho makes the create response echo the created object's
	// identity (as a server implementing the echo contract does).
	createEcho bool
	// noIdentityCapability makes /v1/status withhold completion_identity_v1,
	// like a server that predates the contract.
	noIdentityCapability bool
	// afterCreate runs (under f.mu) right after a create landed: the window
	// a replacement can occupy before any follow-up stat.
	afterCreate func(path string)
	// partialPlan makes the read plan for a path represent only the first
	// N bytes of each block (a truncated tail), modeling an object whose
	// content block is referenced over a shorter range.
	partialPlan map[string]partialPlanShape
}

// fakeTotalParts is the multipart part count for a size: ceil(size/part),
// never truncation.
func fakeTotalParts(size, part float64) int {
	return int((size + part - 1) / part)
}

// partialPlanShape overrides one block's represented range in the read plan.
type partialPlanShape struct {
	Length      int64
	BlockOffset int64
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
	f := &fakeV2{extent: map[string]string{}, classic: map[string]string{}, dirs: map[string]bool{"/": true}, blocks: map[string][]byte{}, ids: map[string]string{}, statFail: map[string]bool{}, gateOnce: map[string]chan struct{}{}, gateHits: map[string]int{}, completeEcho: true, partialPlan: map[string]partialPlanShape{}, createEcho: true}
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
	// /v1/status advertises the negotiated contracts. completion_identity_v1
	// is what the bridge requires before publishing a fresh create, so the
	// fake must advertise it exactly like a current server does.
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenant_id": "t",
			"status":    "active",
			"storage_capabilities": map[string]any{
				"completion_identity_v1": !f.noIdentityCapability,
			},
		})
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
			if f.statGone && (f.extent[path] != "" || f.hasClassic(path)) {
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
			case f.hasClassic(path):
				w.Header().Set("X-Dat9-Resource-ID", f.ids[path])
				// The real server reports a revision for every layout (the
				// classic CAS base is read from here), not only for extent.
				w.Header().Set("X-Dat9-Revision", fmt.Sprint(f.rev(path)))
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
			if classic := f.classic[path]; f.hasClassic(path) && f.extent[path] == "" {
				// A classic (legacy) file: the raw byte stream, not a plan.
				// The identity rides the SAME response (the server knows
				// which object it just served).
				fail := f.failBody
				id := f.ids[path]
				echoOff, echoOverride := f.readEchoOff, f.readEchoOverride
				bodyOverride := f.readBodyOverride
				f.mu.Unlock()
				if echoOverride != "" {
					w.Header().Set("X-Dat9-Resource-ID", echoOverride)
				} else if id != "" && !echoOff {
					w.Header().Set("X-Dat9-Resource-ID", id)
				}
				if bodyOverride != "" {
					classic = bodyOverride
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
			// Per-block plan, like the real server: each 4 MiB piece is
			// keyed by ITS OWN content, so two files sharing a first block
			// are distinguishable only by the rest of the plan.
			f.mu.Lock()
			shape, partial := f.partialPlan[path]
			f.mu.Unlock()
			for off := 0; off < len(content); off += int(client.ExtentBlockSize) {
				end := off + int(client.ExtentBlockSize)
				if end > len(content) {
					end = len(content)
				}
				sum := sha256Hex([]byte(content[off:end]))
				if partial && off == 0 {
					blocks = append(blocks, client.ExtentReadPlanBlock{FileOffset: 0, Length: shape.Length, BlockKey: sum, BlockOffset: shape.BlockOffset, URL: f.URL + "/obj/" + sum})
					continue
				}
				blocks = append(blocks, client.ExtentReadPlanBlock{FileOffset: int64(off), Length: int64(end - off), BlockKey: sum, BlockOffset: 0, URL: f.URL + "/obj/" + sum})
			}
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{Size: int64(len(content)), Revision: 3, Blocks: blocks})
		case http.MethodPatch:
			var body struct {
				NewSize     int64                     `json:"new_size"`
				DirtyBlocks []client.ExtentDirtyBlock `json:"dirty_blocks"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			// The wire contract mirrors initiate: a PATCH bound to an
			// expected resource refuses (412) when the path's occupant is
			// a different object — the identity check real servers run at
			// the extent-maintenance layer.
			if exp := r.Header.Get("X-Dat9-Expected-Resource"); exp != "" {
				f.mu.Lock()
				occupant := f.ids[path]
				f.mu.Unlock()
				if occupant != "" && occupant != exp {
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = w.Write([]byte(`{"error":"resource mismatch"}`))
					return
				}
			}
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
				if f.extent[path] != "" || f.hasClassic(path) || f.dirs[path] {
					http.Error(w, "exists", http.StatusConflict)
					return
				}
				f.classic[path] = ""
				f.nextID++
				f.ids[path] = fmt.Sprintf("res-%d", f.nextID)
				createdID := f.ids[path]
				echo := f.createEcho
				if hook := f.afterCreate; hook != nil {
					hook(path)
				}
				f.mu.Unlock()
				resp := map[string]any{"revision": 1}
				if echo {
					resp["resource_id"] = createdID
				}
				_ = json.NewEncoder(w).Encode(resp)
				f.mu.Lock()
				return
			}
			if _, isLink := r.URL.Query()["hardlink"]; isLink {
				// POST <dst>?hardlink=1 with X-Dat9-Hardlink-Source: like the
				// real server, an existing destination is REFUSED (409) and
				// the new name shares the source's object and identity.
				src := r.Header.Get("X-Dat9-Hardlink-Source")
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.dirs[path] || f.extent[path] != "" || f.hasClassic(path) {
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
		case http.MethodPut:
			// Direct classic write: the small-payload publication, and the
			// degraded classic surface commitClassic uses. The real server
			// answers with the new revision and size.
			body, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			if exp := r.Header.Get("X-Dat9-Expected-Resource"); exp != "" {
				if cur := f.ids[path]; cur != "" && cur != exp {
					f.mu.Unlock()
					http.Error(w, "resource mismatch", http.StatusPreconditionFailed)
					return
				}
			}
			f.classic[path] = string(body)
			delete(f.extent, path)
			if f.ids[path] == "" {
				f.nextID++
				f.ids[path] = fmt.Sprintf("res-%d", f.nextID)
			}
			rev := f.rev(path)
			f.mu.Unlock()
			w.Header().Set("X-Dat9-Revision", fmt.Sprint(rev))
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": rev, "size_bytes": int64(len(body))})
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
				"total_parts": fakeTotalParts(body["total_size"].(float64), 4<<20),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "s1", "key": "", "part_size": 4 << 20,
			// CEIL, like the real server: a size one byte past a part
			// boundary needs the extra part, or the tail is silently lost.
			"total_parts":       fakeTotalParts(body["total_size"].(float64), 4<<20),
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
			if hook := f.afterComplete; hook != nil {
				hook(landedPath)
			}
			f.mu.Unlock()
			// Echo the landed object's identity: the client captures it
			// as the ownership proof for post-upload stat outages.
			if os.Getenv("EXTENT_DBG_ID") != "" {
				fmt.Println("FAKE COMPLETE echo:", landedID, "path:", landedPath)
			}
			completion := map[string]any{"status": "completed", "revision": f.completes}
			if f.completeEcho {
				completion["resource_id"] = landedID
			}
			_ = json.NewEncoder(w).Encode(completion)
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

// hasClassic reports whether a classic object exists at path, INCLUDING a
// zero-byte one (an empty classic file is a real object, not an absence).
// The caller holds f.mu.
func (f *fakeV2) hasClassic(path string) bool {
	_, ok := f.classic[path]
	return ok
}

// rev reports the revision the server would announce for a classic object
// (extent paths use a fixed 3). The caller holds f.mu.
func (f *fakeV2) rev(path string) int64 {
	if _, ok := f.classic[path]; ok {
		return int64(f.completes) + 1 // any stable positive base works for CAS
	}
	return 3
}

// setAfterCreate installs a callback that runs under f.mu right after a
// create landed.
func (f *fakeV2) setAfterCreate(fn func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterCreate = fn
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

// setAfterComplete installs (or clears) a callback that runs under f.mu
// right after a completion landed its bytes.
func (f *fakeV2) setAfterComplete(fn func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterComplete = fn
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

// Round-32: the headerless classic refetch fallback binds the BODY it
// publishes. The post-stat re-read's own echo is checked, and an echo-less
// re-read is verified against the staged slice's content checksum — a
// replacement's bytes never publish under the staged id.
func TestHeaderlessFallbackBindsBody(t *testing.T) {
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
	// Stage an identity-bound classic slice, then demote it under pressure.
	old := classicReadCacheBytes
	classicReadCacheBytes = 1
	defer func() { classicReadCacheBytes = old }()
	buf := make([]byte, 8)
	if _, err := eh.Read(context.Background(), 0, buf); err != nil {
		t.Fatal(err)
	}
	br := h.opts.Engine.Bridge
	br.mu.Lock()
	rec := br.classicLRU[len(br.classicLRU)-1]
	firstID, wantRes := rec.id, rec.resID
	br.mu.Unlock()
	if wantRes == "" {
		t.Fatal("staged slice carries no identity")
	}
	for i := 0; i < 4; i++ {
		d := make([]byte, 4096)
		_, _ = rand.Read(d)
		br.stageClassicChunk(sha256Hex(d), d, "/d/f.bin", int64(i+1)<<20, false, fmt.Sprintf("res-h%d", i+2), 1)
	}
	// The refetch GET is headerless (redirect shape): the stat fence alone
	// no longer publishes anything — the re-read must be bound itself.
	f.mu.Lock()
	f.readEchoOff = true
	f.mu.Unlock()
	key := fmt.Sprintf("chunks/0/0/%d_0_8", firstID)

	// (1) the re-read carries a DIFFERENT identity echo: refuse.
	f.mu.Lock()
	f.readEchoOverride = "res-OTHER"
	f.mu.Unlock()
	if rc, err := br.Storage().Get(context.Background(), key, 0, 8); err == nil {
		_ = rc.Close()
		t.Fatal("fallback published bytes under a mismatched re-read identity")
	}

	// (2) the re-read is echo-less but serves a replacement's content: the
	// content checksum must refuse it.
	f.mu.Lock()
	f.readEchoOverride = ""
	f.readBodyOverride = "BBBBBBBB"
	f.mu.Unlock()
	if rc, err := br.Storage().Get(context.Background(), key, 0, 8); err == nil {
		_ = rc.Close()
		t.Fatal("fallback published unverified replacement bytes")
	}

	// (3) an echo-less re-read whose bytes hash to the staged image: serve.
	f.mu.Lock()
	f.readBodyOverride = ""
	f.mu.Unlock()
	rc, err := br.Storage().Get(context.Background(), key, 0, 8)
	if err != nil {
		t.Fatalf("verified fallback refused: %v", err)
	}
	got, gerr := io.ReadAll(rc)
	_ = rc.Close()
	if gerr != nil {
		t.Fatal(gerr)
	}
	if string(got) != "AAAAAAAA" {
		t.Fatalf("fallback body = %q, want AAAAAAAA", got)
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

// Round-60: an echo-less completion binds NOTHING, even when the occupant's
// whole image matches byte for byte. Content is not identity — a byte-identical
// replacement is a different object, and adopting its ID would let this
// descriptor's later writes overwrite it under its own revision. The refined
// requirement is that the server echoes the landed identity; without one the
// create is refused rather than adopted.
func TestEcholessCompletionBindsNothing(t *testing.T) {
	f := newFakeV2(t)
	f.completeEcho = false
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/n.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("an echo-less completion must not bind the occupant, however identical its bytes")
	}
	if got := f.contentOf("/d/n.bin"); got != "AAAA" {
		t.Fatalf("content = %q, want the upload's own bytes", got)
	}
	bridge := h.opts.Engine.Bridge
	if got := bridge.publicationIdentity(eh.ino); got != "" {
		t.Fatalf("bound %q from an echo-less completion", got)
	}
	// Retrying keeps refusing: the pending state persists for the next
	// attempt rather than silently succeeding once the Stat recovers.
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the refusal must persist across retries")
	}
}

// The classic mirror: no echo and a classic occupant refuses on every
// retry — the descriptor never adopts it.
func TestEcholessClassicOccupantRefusedOnRetries(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true)
	f.mu.Lock()
	f.completeEcho = false
	f.mu.Unlock()
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/c.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("an echo-less completion over a classic occupant must refuse")
	}
	// Retrying keeps refusing; nothing is adopted.
	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("the refusal must persist across retries")
	}
}

// Round-47: with no completion echo, an extent occupant that replaced OUR
// landing BEFORE the classification Stat must not be adopted — its read
// plan holds none of our content-addressed blocks, so the binding is
// refused and the replacement is never written through our descriptor.
func TestEcholessReplacementBeforeClassificationRefuses(t *testing.T) {
	f := newFakeV2(t)
	f.completeEcho = false
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	// Interactive staging: Write only stages; the commit (and its
	// classification) happens at the flush boundary, after the replacement.
	h.opts.Engine.Bridge.EnableDeferredCommits()
	res, _, _ := dispatchCreate(context.Background(), h, "/d/n.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	// Another client replaces the name with a DIFFERENT extent object
	// before our classification Stat runs.
	f.remove("/d/n.bin")
	f.seedExtent(t, "/d/n.bin", "OTHER!")
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("an echo-less flush over a pre-classification replacement must refuse")
	}
	if got := f.contentOf("/d/n.bin"); got != "OTHER!" {
		t.Fatalf("replacement = %q, want OTHER! untouched", got)
	}
}

// Round-48: a durable interactive barrier must follow its inode across a
// rename. The manifest persists the pathname at staging time; a rename
// updates only memory, so without retargeting, replay after a crash would
// find the saved name pointing at nothing (or at a replacement) and the
// acknowledged bytes would be stranded. The retarget rewrites the record
// durably at the rename boundary.
func TestStagedBarrierFollowsRename(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE-PAYLOAD")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(1)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("NEW!"))
	br.StageBlockForTest(sum, []byte("NEW!"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 4)
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	manifestPath := func() string {
		entries, err := os.ReadDir(br.cfg.StagingDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() {
				return filepath.Join(br.cfg.StagingDir, e.Name(), "manifest.json")
			}
		}
		t.Fatal("no staged manifest")
		return ""
	}
	readPath := func() string {
		raw, err := os.ReadFile(manifestPath())
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return m.Path
	}
	if got := readPath(); got != "/d/a.bin" {
		t.Fatalf("staged manifest path = %q, want /d/a.bin", got)
	}
	// Rename: the durable record must be retargeted before the syscall
	// is acknowledged.
	br.RemapLocal("/d/a.bin", "/d/b.bin")
	if got := readPath(); got != "/d/b.bin" {
		t.Fatalf("after rename, manifest path = %q, want /d/b.bin (record stranded at the old name)", got)
	}
}

// Test-only bridge accessors: drive the durable-barrier path directly,
// without an engine handle, so the test pins the manifest contract.
func (b *Bridge) RememberPathForTest(ino meta.Ino, path string) {
	b.mu.Lock()
	full := b.fullPath(path)
	b.paths[ino] = full
	b.replacements[full] = ino // the descriptor owns the name
	b.mu.Unlock()
}

func (b *Bridge) StageBlockForTest(sum string, data []byte) {
	b.blocks.stagePinned(sum, data, true)
}

func (b *Bridge) AddPendingForTest(ino meta.Ino, chunk, off uint32, sum string, size int64, total int64) {
	b.mu.Lock()
	b.pending[ino] = append(b.pending[ino], pendingWrite{chunk: chunk, off: off, checksum: sum, size: size})
	b.sizes[ino] = total
	b.mu.Unlock()
}

// Round-49: with JuiceFS's native writeback armed, blocks are staged to the
// engine's disk cache and uploaded by the chunk store asynchronously; the
// meta side must wait for them before committing (the server verifies every
// referenced block). A write+flush through a writeback engine must land the
// same bytes as the direct path.
func TestNativeWritebackCommitWaitsForStagedBlocks(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{
		Client:                 client.New(f.URL, ""),
		RemoteRoot:             "/",
		CacheDir:               engineCacheDir(t),
		CacheSize:              64 << 20,
		Writeback:              true,
		WritebackThresholdSize: int(client.ExtentBlockSize) + 1,
	})
	if !eng.Bridge.NativeWritebackForTest() {
		t.Fatal("native writeback was not armed on the bridge")
	}
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/w.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("WRITEBACK")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("writeback flush: %v", err)
	}
	if got := f.contentOf("/d/w.bin"); got != "WRITEBACK" {
		t.Fatalf("content = %q, want WRITEBACK", got)
	}
}

// Round-50: the engine must actually USE JuiceFS's read cache and readahead
// for whole blocks. The zero-value chunk config left CacheFullBlock false
// (a full 4 MiB block was never written to the disk cache) and Readahead 0
// (no readahead at all) — the extent plane silently ran without the
// upstream data path. Pin the upstream mount defaults.
func TestEngineUsesUpstreamCacheDefaults(t *testing.T) {
	got := upstreamChunkDefaultsForTest(4 << 20)
	if !got.CacheFullBlock {
		t.Fatal("CacheFullBlock must be true (upstream default: cache-partial-only off)")
	}
	if !got.OSCache {
		t.Fatal("OSCache must be true (upstream keeps the OS page cache)")
	}
	if want := 8 * (4 << 20); got.Readahead != want {
		t.Fatalf("Readahead = %d, want %d (8 blocks, upstream default)", got.Readahead, want)
	}
	if got.Prefetch != 1 {
		t.Fatalf("Prefetch = %d, want 1 (upstream default)", got.Prefetch)
	}
	// BufferSize is not merely a write throttle: the data reader derives
	// its GLOBAL read-ahead budget from it (80%), so an unbounded value
	// grants the mount an enormous in-memory read budget. Keep it at the
	// upstream 300 MiB shape.
	if got.BufferSize != 300<<20 {
		t.Fatalf("BufferSize = %d, want %d (upstream default 300 MiB)", got.BufferSize, 300<<20)
	}
	// The engine calls SelfCheck with an EMPTY uuid: the cache directory is
	// already per-mount scoped, so upstream's UUID namespace segment would
	// only add a redundant level (.../<scope>/extent/<uuid>).
	conf := got
	conf.CacheSize = 64 << 20
	dir := filepath.Join(engineCacheDir(t), "extent")
	conf.CacheDir = dir
	conf.SelfCheck("")
	if conf.CacheDir != dir {
		t.Fatalf("SelfCheck cache dir = %q, want it unchanged at %q", conf.CacheDir, dir)
	}
	// SelfCheck must still normalize everything else.
	if conf.CacheEviction == "" {
		t.Fatal("SelfCheck must default the eviction policy")
	}
}

// Round-50: closing a descriptor must land its pending bytes. VFS.Release
// only flushes the engine writer into the bridge; without the bridge's
// landing the last close would leave the file's data unpublished (no later
// callback exists on the FUSE path), breaking the close-time durability the
// classic plane owns.
func TestReleaseLandsPendingBytes(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/r.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("CLOSED")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := f.contentOf("/d/r.bin"); got != "CLOSED" {
		t.Fatalf("content after release = %q, want CLOSED (release must land pending bytes)", got)
	}
}

// Round-51: a shared content block does NOT establish object identity. Our
// echo-less create lands block[0]=X; another client then replaces the name
// with an object whose block[0] is also X but whose tail differs. Content
// addressing cannot tell them apart by one block, so the whole-image
// comparison must refuse — otherwise this descriptor would adopt the
// replacement and PATCH its size and bytes over the distinct tail.
func TestEcholessSharedBlockIsNotIdentity(t *testing.T) {
	f := newFakeV2(t)
	f.completeEcho = false
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	// Interactive staging: our image is fully built, then the replacement
	// lands between our completion and the classification Stat.
	h.opts.Engine.Bridge.EnableDeferredCommits()
	payload := strings.Repeat("A", int(client.ExtentBlockSize)) + "OUR-TAIL"
	res, _, _ := dispatchCreate(context.Background(), h, "/d/n.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	f.setAfterComplete(func(path string) {
		if path != "/d/n.bin" {
			return
		}
		// A foreign object that SHARES our first block but has its own
		// tail (a different object: a later write must never land on it).
		f.extent[path] = strings.Repeat("A", int(client.ExtentBlockSize)) + "FOREIGN"
		f.nextID++
		f.ids[path] = fmt.Sprintf("res-%d", f.nextID)
	})
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("a shared first block must not authorize adoption of a foreign object")
	}
	if got := f.contentOf("/d/n.bin"); !strings.HasSuffix(got, "FOREIGN") {
		t.Fatalf("replacement = %q..., want its own tail untouched", got[len(got)-8:])
	}
}

// Round-53: an empty classic occupant can never be attributed to our create
// (the create call returns no resource identity), so the commit must neither
// delete it NOR publish into it. Refusing is the only option that leaves a
// foreign object untouched — "avoid the DELETE" was not enough, because a
// classic write would still have overwritten that object's contents.
func TestUnattributedEmptyOccupantRefusedNotWritten(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	h.opts.Engine.Bridge.EnableDeferredCommits()
	res, _, _ := dispatchCreate(context.Background(), h, "/d/e.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("OURS")); err != nil {
		t.Fatal(err)
	}
	// Another client holds the name as an EMPTY classic file.
	f.mu.Lock()
	f.classic["/d/e.bin"] = ""
	f.nextID++
	f.ids["/d/e.bin"] = fmt.Sprintf("res-%d", f.nextID)
	foreignID := f.ids["/d/e.bin"]
	f.mu.Unlock()
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("an unattributed occupant must be refused, not written into")
	}
	f.mu.Lock()
	gotID := f.ids["/d/e.bin"]
	content, stillClassic := f.classic["/d/e.bin"]
	dels := 0
	for _, d := range f.deletes {
		if d == "/d/e.bin" {
			dels++
		}
	}
	f.mu.Unlock()
	if dels != 0 {
		t.Fatalf("the occupant was deleted %d time(s)", dels)
	}
	if !stillClassic || gotID != foreignID {
		t.Fatalf("occupant identity changed (%q -> %q, classic=%v)", foreignID, gotID, stillClassic)
	}
	if content != "" {
		t.Fatalf("occupant content = %q, want it untouched (never published into)", content)
	}
}

// Round-51: a directory rename must retarget the durable barrier records of
// its DESCENDANTS. Each child holds its own inode, so retargeting only the
// renamed directory's inode leaves an open /d/a/p's manifest naming a path
// that no longer holds the object; a crash before Flush would strand the
// fsync-acknowledged bytes.
func TestStagedBarrierFollowsDirectoryRename(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	childIno := meta.Ino(77)
	br.RememberPathForTest(childIno, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(childIno, 0, 0, sum, 4, 8)
	if err := br.StagePendingLocal(childIno); err != nil {
		t.Fatal(err)
	}
	if got := stagedManifestPath(t, br); got != "/d/a.bin" {
		t.Fatalf("staged path = %q, want /d/a.bin", got)
	}
	// Rename the DIRECTORY: one event, one RemapLocal for /d → /e.
	br.RemapLocal("/d", "/e")
	if got := stagedManifestPath(t, br); got != "/e/a.bin" {
		t.Fatalf("descendant manifest path after dir rename = %q, want /e/a.bin", got)
	}
}

// Round-51: a rename must FAIL when the durable intent cannot be persisted.
// Acknowledging it would leave the barrier naming a name that no longer
// holds the object, stranding the fsync-acknowledged bytes forever.
func TestRenameRefusedWhenIntentCannotPersist(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(11)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	// Make the staging tree unwritable: the manifest cannot be rewritten.
	stagingDir := br.cfg.StagingDir
	if err := os.Chmod(stagingDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stagingDir, 0o700) })
	if err := br.StageRenameIntentLocal("/d/a.bin", "/d/b.bin"); err == nil {
		t.Skip("the platform allowed the manifest rewrite; the refusal is covered by TestRenameIntentPrecedesRemoteRename")
	}
	// With the intent failing, the remote rename must not be issued: the
	// stub below performs no rename itself but observes the operation.
	h := New(Options{Engine: eng, Threshold: 0})
	var chainRan bool
	_, err, handled := h.renameWithDurableIntent(context.Background(),
		&fs.Op{Kind: fs.OpRename, Path: "/d/a.bin", Path2: "/d/b.bin"},
		func(context.Context, *fs.Op) (fs.Result, error) {
			chainRan = true
			return fs.Result{}, nil
		})
	if !handled {
		t.Fatal("the plane must claim the rename it refused")
	}
	if err == nil {
		t.Fatal("the rename must be refused when the durable intent cannot be written")
	}
	if chainRan {
		t.Fatal("the remote rename ran despite the failed durable intent")
	}
	_ = os.Chmod(stagingDir, 0o700)
}

// Round-51: the intent must be DURABLE BEFORE the remote rename, so a crash
// on either side of it leaves the barrier recoverable under one of the two
// names.
func TestRenameIntentPrecedesRemoteRename(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(21)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	h := New(Options{Engine: eng, Threshold: 0})
	var atChainTime string
	_, err, _ := h.renameWithDurableIntent(context.Background(),
		&fs.Op{Kind: fs.OpRename, Path: "/d/a.bin", Path2: "/d/b.bin"},
		func(context.Context, *fs.Op) (fs.Result, error) {
			// Observe the record exactly when the chain would rename.
			if recs, err := br.scanStaged(); err == nil && len(recs) == 1 {
				for _, c := range recs[0].m.candidatePaths() {
					if c == "/d/b.bin" {
						atChainTime = c
					}
				}
			}
			return fs.Result{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if atChainTime != "/d/b.bin" {
		t.Fatalf("intent at rename time = %q, want /d/b.bin (must precede the remote rename)", atChainTime)
	}
	if got := stagedManifestPath(t, br); got != "/d/b.bin" {
		t.Fatalf("settled manifest path = %q, want /d/b.bin", got)
	}
	if rec := onlyStaged(t, br); rec.m.Path != "/d/b.bin" || len(rec.m.NextPaths) != 0 {
		t.Fatalf("settled record = path %q / candidates %v, want path /d/b.bin and none pending",
			rec.m.Path, rec.m.candidatePaths())
	}
}

// Round-52: a not-handled helper result means only that the helper did
// nothing — never that the rename landed. When the destination has an open
// handle that is still local-only, RetainOpenAlias has no remote object to
// retain and declines (empty sentinel, not handled); the wrapper must run the
// chain itself instead of reporting success with the source untouched.
func TestNotHandledRenameRunsTheChain(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/src.bin", "SOURCE")
	// The destination exists only locally: an open engine handle with no
	// remote object yet, which is exactly the not-handled shape.
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/local.bin", syscall.O_RDWR|syscall.O_CREAT)
	if res.Handle == nil {
		t.Fatal("the local-only create must be claimed by the engine")
	}
	var chainRan bool
	_, err, handled := h.renameWithDurableIntent(context.Background(),
		&fs.Op{Kind: fs.OpRename, Path: "/d/src.bin", Path2: "/d/local.bin"},
		func(context.Context, *fs.Op) (fs.Result, error) {
			chainRan = true
			return fs.Result{}, nil
		})
	if !handled {
		t.Fatal("the plane must claim the rename")
	}
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if !chainRan {
		t.Fatal("a not-handled helper result must fall through to the chain, not report success")
	}
}

// Round-52: a plan block that references only a PREFIX of our staged content
// (same key, same start offset, shorter Length) followed by holes must not
// pass the identity check. Otherwise a same-size replacement with a
// truncated tail would be bound as our upload.
func TestImageProofRejectsPartialBlockReference(t *testing.T) {
	f := newFakeV2(t)
	f.completeEcho = false
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	h.opts.Engine.Bridge.EnableDeferredCommits()
	block := strings.Repeat("Q", 4096)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/n.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte(block)); err != nil {
		t.Fatal(err)
	}
	f.setAfterComplete(func(path string) {
		if path != "/d/n.bin" {
			return
		}
		// Same object key, same start offset, same logical size — but the
		// plan represents only the first byte of the block, then a hole.
		f.extent[path] = block
		f.partialPlan[path] = struct {
			Length      int64
			BlockOffset int64
		}{Length: 1, BlockOffset: 0}
	})
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("a partial reference to the same content must not be bound as our upload")
	}
}

// stagedManifestPath reads the (single) staged record's saved pathname.
func stagedManifestPath(t *testing.T, br *Bridge) string {
	t.Helper()
	recs, err := br.scanStaged()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("staged records = %d, want 1", len(recs))
	}
	return recs[0].m.Path
}

// Round-53: an empty flush must not materialize a classic name while the
// descriptor can still write. If it did, the next write would face an
// occupant it can neither attribute nor delete — and on a sparse write the
// classic fallback would materialize the whole logical image (one byte at
// 2 GiB would allocate 2 GiB), OOMing the mount and leaving the file off the
// extent path.
func TestEmptyFlushKeepsSparseWritesOnExtentPath(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/p.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	// Flush while empty: no name is materialized, so no classic occupant can
	// block (or be mistaken for) the later write.
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("empty flush: %v", err)
	}
	if n := f.hashParsedLayout("/d/p.bin"); n != "" {
		t.Fatalf("an intermediate empty flush materialized a %s object; it must stay local until close", n)
	}
	// A one-byte sparse write at 2 GiB must stay on the extent path: no
	// whole-image materialization, no classic fallback.
	const farOff = int64(2) << 30
	if err := eh.Write(context.Background(), farOff, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("sparse write after an empty flush must land on the extent path: %v", err)
	}
	if n := f.hashParsedLayout("/d/p.bin"); n != "extent" {
		t.Fatalf("layout = %q, want extent (a classic fallback would materialize the sparse image)", n)
	}
	// The bridge's logical size must cover the byte written at 2 GiB. (The
	// fake reconstructs block bytes rather than a sparse map, so the exact
	// file length is asserted on the client view, not its buffer.)
	br := h.opts.Engine.Bridge
	br.mu.Lock()
	size := br.sizes[eh.ino]
	br.mu.Unlock()
	if size != farOff+1 {
		t.Fatalf("logical size = %d, want %d", size, farOff+1)
	}
}

// Round-53: the name is still materialized at the LAST close, so `touch`
// keeps working.
func TestEmptyFileMaterializesAtClose(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/t.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if n := f.hashParsedLayout("/d/t.bin"); n != "classic" {
		t.Fatalf("layout after close = %q, want classic (touch must materialize the name)", n)
	}
}

// hashParsedLayout reports the fake's stored layout for a path ("", "extent"
// or "classic").
func (f *fakeV2) hashParsedLayout(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.extent[path] != "" {
		return "extent"
	}
	if f.hasClassic(path) {
		return "classic"
	}
	return ""
}

// Round-52: an error from the chain does NOT prove the remote rename failed
// to commit — a committed rename can lose its response. The durable
// destination must therefore SURVIVE an ambiguous outcome, so recovery
// accepts the staged object at every name it may hold.
func TestAmbiguousRenameKeepsIntent(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(31)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	h := New(Options{Engine: eng, Threshold: 0})
	boom := errors.New("remote rename response lost")
	_, err, _ := h.renameWithDurableIntent(context.Background(),
		&fs.Op{Kind: fs.OpRename, Path: "/d/a.bin", Path2: "/d/b.bin"},
		func(context.Context, *fs.Op) (fs.Result, error) { return fs.Result{}, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's failure", err)
	}
	rec := onlyStaged(t, br)
	if !rec.hasCandidate("/d/b.bin") {
		t.Fatalf("ambiguous rename lost the destination: candidates = %v", rec.m.candidatePaths())
	}
	// The PRE-rename name must remain a candidate too — the rename may not
	// have committed.
	if rec.m.Path != "/d/a.bin" {
		t.Fatalf("saved path = %q, want /d/a.bin still", rec.m.Path)
	}
}

// Round-53: a second rename must ADD its destination, not replace the
// previous one. Until B -> C commits, the object may still be at B, so
// recovery has to try it.
func TestSecondRenameKeepsPreviousCandidate(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(41)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	// First rename: A -> B, committed but not settled (cleanup failed).
	if err := br.StageRenameIntentLocal("/d/a.bin", "/d/b.bin"); err != nil {
		t.Fatal(err)
	}
	br.ClearRenameIntentLocal("/d/a.bin", "/d/b.bin")
	// Second rename: B -> C, staged but NOT committed yet.
	if err := br.StageRenameIntentLocal("/d/b.bin", "/d/c.bin"); err != nil {
		t.Fatal(err)
	}
	rec := onlyStaged(t, br)
	// The object is still at B, so B must remain a candidate even though C
	// was just recorded.
	if !rec.hasCandidate("/d/b.bin") {
		t.Fatalf("second rename dropped the still-current name: candidates = %v", rec.m.candidatePaths())
	}
	if !rec.hasCandidate("/d/c.bin") {
		t.Fatalf("second rename did not record its destination: candidates = %v", rec.m.candidatePaths())
	}
	// And a definitive failure of that RPC leaves the record replayable at B.
	br.ClearRenameIntentLocal("/d/b.bin", "/d/c.bin")
	if !onlyStaged(t, br).hasCandidate("/d/b.bin") {
		t.Fatal("a failed second rename must leave the object's current name replayable")
	}
}

// Round-52: a barrier staged WHILE a rename is in flight must inherit its
// destination, since StagePendingLocal does not run under the path locks.
func TestConcurrentStageInheritsInFlightIntent(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(51)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)
	if err := br.StageRenameIntentLocal("/d/a.bin", "/d/b.bin"); err != nil {
		t.Fatal(err)
	}
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	if !onlyStaged(t, br).hasCandidate("/d/b.bin") {
		t.Fatalf("mid-rename barrier missed the in-flight destination: %v", onlyStaged(t, br).m.candidatePaths())
	}
}

// onlyStaged returns the single staged record.
func onlyStaged(t *testing.T, br *Bridge) stagedRecord {
	t.Helper()
	recs, err := br.scanStaged()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("staged records = %d, want 1", len(recs))
	}
	return recs[0]
}

// hasCandidate reports whether p is one of the record's candidate names.
func (r stagedRecord) hasCandidate(p string) bool {
	for _, c := range r.m.candidatePaths() {
		if c == p {
			return true
		}
	}
	return false
}

// Round-54: a directory rename must record each DESCENDANT's own
// destination, not the directory root. Staging /a -> /b for an object at
// /a/file must record /b/file: recording /b leaves recovery probing a
// directory and the missing old name while the bytes sit at /b/file.
func TestDirRenameCandidateKeepsDescendantSuffix(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/a", true)
	f.seedExtent(t, "/a/file.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(61)
	br.RememberPathForTest(ino, "/a/file.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatal(err)
	}
	// The directory rename arrives as one intent for /a.
	if err := br.StageRenameIntentLocal("/a", "/b"); err != nil {
		t.Fatal(err)
	}
	rec := onlyStaged(t, br)
	if !rec.hasCandidate("/b/file.bin") {
		t.Fatalf("descendant destination missing: candidates = %v, want /b/file.bin", rec.m.candidatePaths())
	}
	if rec.hasCandidate("/b") {
		t.Fatalf("the directory ROOT was recorded as a file candidate: %v", rec.m.candidatePaths())
	}
	if !rec.hasCandidate("/a/file.bin") {
		t.Fatal("the pre-rename name must stay a candidate until the rename is confirmed")
	}
}

// Round-55: a STRICT fsync of a fresh empty file must be REMOTE-durable, not
// merely local. `--durability=fsync` promises the drive9 server has the file
// before fsync returns (docs/design/fuse-durability-policy.md), so a local
// barrier — or, with no staging directory, nothing at all — does not satisfy
// it. The name must exist remotely, and the descriptor must remain writable
// afterwards because the created object is bound as its publication base.
func TestStrictFsyncOfEmptyFileIsRemoteDurable(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "strict"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/e.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	if n := f.hashParsedLayout("/d/e.bin"); n != "classic" {
		t.Fatalf("strict fsync left no remote object (layout %q); --durability=fsync must be remote-durable", n)
	}
	// Bound, not merely created: a write through the same descriptor still
	// works and lands on the bound object.
	if err := eh.Write(context.Background(), 0, []byte("AFTER-FSYNC")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("the descriptor became unwritable after a strict fsync: %v", err)
	}
	if got := f.contentOf("/d/e.bin"); got != "AFTER-FSYNC" {
		t.Fatalf("content = %q, want AFTER-FSYNC", got)
	}
}

// Round-55: the same strict fsync must work with NO staging directory: there
// is no local place to record anything, so only a remote copy can make the
// acknowledgment true.
func TestStrictFsyncOfEmptyFileWithoutStagingDir(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	// No CacheDir at all: no staging directory exists.
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	if eng.Bridge.StagingDirForTest() != "" {
		t.Fatal("precondition: this engine must have no staging directory")
	}
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "strict"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/n.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	if n := f.hashParsedLayout("/d/n.bin"); n != "classic" {
		t.Fatalf("with no staging dir a strict fsync must still create the remote file (layout %q)", n)
	}
}

// Round-54: an intervening ordinary Flush must not delete the durable
// barrier that an earlier interactive fsync wrote. An empty file has nothing
// staged to make redundant; clearing here left it with neither a remote name
// nor a local record.
func TestInterveningFlushKeepsEmptyBarrier(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/e.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	if recs, _ := eng.Bridge.scanStaged(); len(recs) != 1 {
		t.Fatalf("precondition: staged barriers = %d, want 1", len(recs))
	}
	// An ordinary flush follows (libc does this between fsync and close).
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if recs, _ := eng.Bridge.scanStaged(); len(recs) != 1 {
		t.Fatalf("the intervening flush deleted the only durable record (barriers = %d)", len(recs))
	}
	// And the file still materializes at close.
	if err := eh.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if n := f.hashParsedLayout("/d/e.bin"); n != "classic" {
		t.Fatalf("layout after close = %q, want classic", n)
	}
}

// Round-54: closing ONE of several descriptors must not materialize the
// empty name. Another handle is still open and may write; an unattributed
// placeholder created by the first close would make its next write fail.
func TestFirstCloseOfTwoKeepsHandleWritable(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res1, _, _ := dispatchCreate(context.Background(), h, "/d/m.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh1 := res1.Handle.(*engineHandle)
	// A second descriptor for the same file.
	res2, _, _ := dispatchCreate(context.Background(), h, "/d/m.bin", syscall.O_RDWR)
	eh2, ok := res2.Handle.(*engineHandle)
	if !ok {
		t.Skipf("the second open was not claimed by the engine (%T)", res2.Handle)
	}
	// Close the FIRST one: it is not the last close, so no name may appear.
	if err := eh1.Release(context.Background()); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if n := f.hashParsedLayout("/d/m.bin"); n != "" {
		t.Fatalf("the first of two closes materialized a %s object; the remaining handle would be blocked", n)
	}
	// The surviving descriptor must still be able to write.
	if err := eh2.Write(context.Background(), 0, []byte("STILL-OK")); err != nil {
		t.Fatal(err)
	}
	if err := eh2.Flush(context.Background()); err != nil {
		t.Fatalf("the surviving handle became unwritable: %v", err)
	}
	if got := f.contentOf("/d/m.bin"); got != "STILL-OK" {
		t.Fatalf("content = %q, want STILL-OK", got)
	}
}

// StagingDirForTest exposes the configured staging directory.
func (b *Bridge) StagingDirForTest() string { return b.cfg.StagingDir }

// Round-55: two overlapping closes must still produce exactly one owner of
// the final publication. A plain "is another handle registered?" check lets
// both releases observe the other and BOTH decline, so the empty file is
// never materialized even though both closes report success.
//
// The race is forced deterministically: each Release is held at the point
// between "decide" and "act" until both have decided.
func TestOverlappingClosesMaterializeOnce(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res1, _, _ := dispatchCreate(context.Background(), h, "/d/o.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh1 := res1.Handle.(*engineHandle)
	res2, _, _ := dispatchCreate(context.Background(), h, "/d/o.bin", syscall.O_RDWR)
	eh2, ok := res2.Handle.(*engineHandle)
	if !ok {
		t.Skipf("the second open was not claimed by the engine (%T)", res2.Handle)
	}
	// Both handles are registered for the same path (dispatchCreate does it).
	h.mu.Lock()
	n := len(h.handles["/d/o.bin"])
	h.mu.Unlock()
	if n != 2 {
		t.Fatalf("registered handles = %d, want 2", n)
	}
	// Claim the final close from both concurrently. The implementation marks
	// the caller under h.mu before counting, so the outcome is total even
	// when the two run at once.
	var got [2]bool
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); got[0] = h.claimFinalClose(eh1) }()
	go func() { defer wg.Done(); got[1] = h.claimFinalClose(eh2) }()
	wg.Wait()
	if !got[0] && !got[1] {
		t.Fatal("neither overlapping close claimed the final publication")
	}
	if got[0] && got[1] {
		t.Fatal("both overlapping closes claimed the final publication")
	}
}

// Round-56: a rename that completes ENTIRELY inside a staging run's window
// must not strand the staged bytes. The run snapshots its path and pauses;
// the rename registers, commits, settles and retires its registration; the
// run resumes, finds no registration and would otherwise persist only the
// OLD name. In-flight tracking defers that retirement, and the run also
// adopts the inode's current mapping.
func TestRenameCompletedInsideStageWindowStillRecorded(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(71)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)

	resumed := make(chan struct{})
	paused := make(chan struct{})
	br.stagingPause = func() {
		br.stagingPause = nil
		close(paused)
		<-resumed
	}
	done := make(chan error, 1)
	go func() { done <- br.StagePendingLocal(ino) }()
	<-paused

	// The whole rename runs to completion inside that window: registration,
	// remote rename, settlement, and retirement.
	h := New(Options{Engine: eng, Threshold: 0})
	_, err, _ := h.renameWithDurableIntent(context.Background(),
		&fs.Op{Kind: fs.OpRename, Path: "/d/a.bin", Path2: "/d/b.bin"},
		func(context.Context, *fs.Op) (fs.Result, error) {
			f.mu.Lock()
			f.move("/d/a.bin", "/d/b.bin")
			f.mu.Unlock()
			br.RemapLocal("/d/a.bin", "/d/b.bin")
			return fs.Result{}, nil
		})
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	// The rename has fully committed and settled at this point. Its
	// registration retirement was DEFERRED because this staging run is still
	// in flight — that is precisely the protection under test.
	close(resumed)
	if err := <-done; err != nil {
		t.Fatalf("staging: %v", err)
	}
	rec := onlyStaged(t, br)
	if !rec.hasCandidate("/d/b.bin") {
		t.Fatalf("the staged record lost the committed rename: candidates = %v", rec.m.candidatePaths())
	}
	// With the staging run finished, the deferred retirement has now run.
	if br.hasPendingRenameForTest("/d/a.bin") {
		t.Fatal("the deferred registration retirement never happened")
	}
}

// hasPendingRenameForTest reports whether a rename is registered for path.
func (b *Bridge) hasPendingRenameForTest(path string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.pendingRenames[path]
	return ok
}

// Round-59: a rename that completes BETWEEN the staging run's disk read and
// its manifest replacement must not be lost. Only the manifest lock makes
// that window safe — a write-then-reverify cannot, because the read-back
// returns exactly the bytes the replacement just wrote.
//
// The rename is driven from a hook INSIDE the lock, i.e. exactly in the
// window the lock has to protect.
func TestRenameDuringStagePublishIsMerged(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a.bin", "BASE")
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	ino := meta.Ino(81)
	br.RememberPathForTest(ino, "/d/a.bin")
	sum := sha256Hex([]byte("PEND"))
	br.StageBlockForTest(sum, []byte("PEND"))
	br.AddPendingForTest(ino, 0, 0, sum, 4, 8)

	// The concurrent writer: a rename landing while the publish holds the
	// lock. It writes a NEW destination into the record.
	done := make(chan struct{})
	br.manifestPause = func() {
		br.manifestPause = nil
		go func() {
			defer close(done)
			if err := br.StageRenameIntentLocal("/d/a.bin", "/d/b.bin"); err != nil {
				t.Errorf("stage rename intent: %v", err)
			}
		}()
		// Give the rename every chance to run: it MUST block on the lock.
		time.Sleep(150 * time.Millisecond)
	}
	if err := br.StagePendingLocal(ino); err != nil {
		t.Fatalf("staging: %v", err)
	}
	<-done

	rec := onlyStaged(t, br)
	if !rec.hasCandidate("/d/b.bin") {
		t.Fatalf("the publish lost a rename that committed inside its lock window: candidates = %v", rec.m.candidatePaths())
	}
}

// Round-59: when the server echoes the created object's identity, the
// binding MUST use it — never a follow-up Stat, whose answer a concurrent
// replacement can turn into a stranger's identity.
func TestCreateEchoBindsWithoutStat(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	// A replacement lands between the create and any Stat the client might
	// have issued. With the echo, that replacement must never be adopted.
	f.setAfterCreate(func(path string) {
		if path != "/d/e.bin" {
			return
		}
		f.nextID++
		f.ids[path] = fmt.Sprintf("res-%d", f.nextID) // a different object
	})
	eng := h.opts.Engine
	res, _, _ := dispatchCreate(context.Background(), h, "/d/e.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	// The bound identity must be the one the CREATE echoed, not the
	// replacement's.
	bound := eng.Bridge.publicationIdentity(eh.ino)
	if bound == "" {
		t.Fatal("no identity bound after an echoing create")
	}
	f.mu.Lock()
	after := f.ids["/d/e.bin"]
	f.mu.Unlock()
	if bound == after {
		t.Fatalf("bound the replacement's identity %q instead of the created object", after)
	}
}

// A local-only engine create (Mknod does not touch the server) can race
// another client's create at the same path. Sync then finds the name already
// materialized — but by an object THIS descriptor never created. Binding that
// occupant would make every later write publish against a stranger's object,
// so the call must refuse instead.
func TestSyncRefusesForeignOccupantOfLocalCreate(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "strict"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/r.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	// Another client creates the same path first.
	f.seedExtent(t, "/d/r.bin", "FOREIGN")
	f.mu.Lock()
	foreignID := f.ids["/d/r.bin"]
	f.mu.Unlock()

	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("strict fsync must not adopt an occupant this descriptor did not create")
	}
	if got := eng.Bridge.publicationIdentity(eh.ino); got != "" {
		t.Fatalf("bound the foreign object %q as this descriptor's publication target", got)
	}
	f.mu.Lock()
	after := f.ids["/d/r.bin"]
	f.mu.Unlock()
	if after != foreignID {
		t.Fatalf("foreign occupant changed (%q -> %q)", foreignID, after)
	}
}

// An ordinary open of an EXISTING empty extent file must synchronize: the
// object predates this handle, so there is nothing to materialize and nothing
// to attribute. The fresh-create refusal must not reach it.
func TestStrictSyncOfExistingEmptyExtentFile(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	// A CLASSIC empty object: `hasClassic` true, size 0 — the shape an
	// existing zero-length extent-plane file presents to nameMaterialized.
	f.mu.Lock()
	f.classic["/d/empty.bin"] = ""
	f.nextID++
	f.ids["/d/empty.bin"] = fmt.Sprintf("res-%d", f.nextID)
	f.mu.Unlock()
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	br := eng.Bridge
	// An ORDINARY open: the bridge knows the path/size/revision but did not
	// create the inode (no freshCreate marker, no binding) — exactly what
	// ResolveLocal + GetAttr leave behind.
	ino := meta.Ino(901)
	br.RememberPathForTest(ino, "/d/empty.bin")
	br.mu.Lock()
	br.sizes[ino] = 0
	br.mu.Unlock()
	if br.isFreshCreate(ino) {
		t.Fatal("precondition: an ordinary open must not be a fresh create")
	}
	if !br.nameMaterialized(context.Background(), "/d/empty.bin") {
		t.Fatal("precondition: the empty object must exist remotely")
	}
	if err := br.MaterializeEmpty(context.Background(), ino); err != nil {
		t.Fatalf("fsync of an existing empty extent file must succeed: %v", err)
	}
}

// Truncating an existing nonempty extent file to zero publishes the
// truncation; Sync must not then turn that completed publication into an
// error. Scope note: the fake reports zero-length extent content as ABSENT, so
// this exercises the shape a real server reports only partially — the
// existing-empty guard itself is covered directly by
// TestStrictSyncOfExistingEmptyExtentFile.
func TestStrictSyncAfterTruncateToZero(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/t.bin", "PAYLOAD")
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "strict"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/t.bin", syscall.O_RDWR)
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Skipf("the existing file was not claimed by the engine (%T)", res.Handle)
	}
	if errno := eng.VFS.Truncate(eh.vctx, eh.ino, 0, 0, nil); errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	if err := eng.Bridge.TruncateLocal(context.Background(), "/d/t.bin", 0); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync after a successful truncate-to-zero must succeed: %v", err)
	}
}

// freshCreate describes the CURRENT incarnation only. Inodes are derived from
// pathnames, so a closed file's marker surviving into a later file at the same
// path would make that file's Sync believe it created an occupant it never
// created — and refuse to synchronize it.
func TestFreshCreateMarkerDoesNotSurviveReuse(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	br := eng.Bridge
	// A local create marks the path-derived inode, then the descriptor closes.
	br.mu.Lock()
	first := inoFor(br.fullPath("/d/reuse.bin"))
	br.freshCreate[first] = true
	br.mu.Unlock()
	br.ForgetPath("/d/reuse.bin")
	br.mu.Lock()
	_, stillFresh := br.freshCreate[first]
	br.mu.Unlock()
	if stillFresh {
		t.Fatal("freshCreate survived the incarnation's retirement; a reused path would inherit it")
	}
	// The reused path is an ordinary open: an existing empty occupant must be
	// synchronizable.
	f.mu.Lock()
	f.classic["/d/reuse.bin"] = ""
	f.nextID++
	f.ids["/d/reuse.bin"] = fmt.Sprintf("res-%d", f.nextID)
	f.mu.Unlock()
	ino := inoFor(br.fullPath("/d/reuse.bin"))
	br.RememberPathForTest(ino, "/d/reuse.bin")
	br.mu.Lock()
	br.sizes[ino] = 0
	br.mu.Unlock()
	if err := br.MaterializeEmpty(context.Background(), ino); err != nil {
		t.Fatalf("a reopened path must not inherit the previous incarnation's create marker: %v", err)
	}
}

// Round-60: a session that DEGRADES to the classic layout is usable again once
// the server echoes the landed identity — the echo is layout-independent, so
// the client never needs the (rejected) whole-image proof to attribute a
// classic landing.
func TestClassicFallbackBindsCompletionEcho(t *testing.T) {
	f := newFakeV2(t)
	f.setLegacy(true) // the initiate answers a CLASSIC plan
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/c.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("CLASSIC")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("a classic-degraded landing with an identity echo must be usable: %v", err)
	}
	if got := f.contentOf("/d/c.bin"); got != "CLASSIC" {
		t.Fatalf("content = %q, want CLASSIC", got)
	}
	if got := h.opts.Engine.Bridge.publicationIdentity(eh.ino); got == "" {
		t.Fatal("the completion echo must bind the landed identity")
	}
	// And the descriptor stays writable through the classic surface. The
	// rewrite is the SAME length, so it fully replaces the image rather than
	// partially overwriting it.
	if err := eh.Write(context.Background(), 0, []byte("SECOND!")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if got := f.contentOf("/d/c.bin"); got != "SECOND!" {
		t.Fatalf("content after rewrite = %q, want SECOND!", got)
	}
}

// The capability boundary must be established BEFORE publishing: a server that
// does not advertise completion_identity_v1 cannot give this descriptor an
// attributable identity afterwards, and refusing only after the upload would
// leave a published file whose descriptor is permanently unusable.
func TestPublishRefusedBeforeUploadWithoutIdentityCapability(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.noIdentityCapability = true
	h := newTestHandler(t, f, 0)
	res, _, _ := dispatchCreate(context.Background(), h, "/d/cap.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("DATA")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Flush(context.Background()); err == nil {
		t.Fatal("a server without the identity contract must be refused")
	}
	// Nothing must have been published: the whole point is that the data never
	// reaches the remote namespace under an unattributable identity.
	f.mu.Lock()
	_, classic := f.classic["/d/cap.bin"]
	_, extent := f.extent["/d/cap.bin"]
	completes := f.completes
	f.mu.Unlock()
	if classic || extent || completes != 0 {
		t.Fatalf("published anyway: classic=%v extent=%v completes=%d", classic, extent, completes)
	}
}

// The empty-create path reaches MaterializeEmpty with NO pending slices, so
// the commit-path preflight never runs for it. Without its own check, an
// older server's strict fsync would publish the empty file first and only
// then fail — the exact ordering the preflight exists to prevent.
func TestEmptyStrictSyncRefusedBeforeCreate(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.noIdentityCapability = true
	cacheDir := engineCacheDir(t)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: cacheDir})
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "strict"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/e.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)

	if err := eh.Sync(context.Background()); err == nil {
		t.Fatal("strict fsync must refuse before creating an unattributable empty file")
	}
	f.mu.Lock()
	_, classic := f.classic["/d/e.bin"]
	_, extent := f.extent["/d/e.bin"]
	f.mu.Unlock()
	if classic || extent {
		t.Fatalf("the empty file was published anyway: classic=%v extent=%v", classic, extent)
	}
}

// The operator's number is ONE allowance for the plane: it is split between
// JuiceFS's chunk cache and the durable staging tree, and the two must sum to
// it. Handing both the same N (the previous shape) let the plane retain 2N.
func TestCacheAllowanceIsSplitWithStaging(t *testing.T) {
	for _, total := range []int64{8 << 20, 1024 << 20, 8 << 30} {
		eng := NewEngine(EngineConfig{
			Client: client.New("http://127.0.0.1:1", ""), RemoteRoot: "/",
			CacheDir: engineCacheDir(t), CacheSize: uint64(total), CacheSizeConfigured: true,
		})
		staging := eng.Bridge.StagingBudgetForTest()
		juice := eng.JuiceCacheSizeForTest()
		if staging <= 0 {
			t.Fatalf("total=%d: staging budget = %d, want a positive reserve", total, staging)
		}
		if got := staging + juice; got != total {
			t.Fatalf("total=%d: staging(%d) + juicefs(%d) = %d, want exactly the configured allowance", total, staging, juice, got)
		}
	}
}

// Budget pressure must REFUSE the new fsync and leave every earlier
// acknowledged barrier untouched: each record is a durability promise already
// made to a caller, so it cannot be revoked to make room for a later write.
func TestStagingBudgetRefusesRatherThanEvictingAcknowledgedBarriers(t *testing.T) {
	const total = 8 << 20
	f := newFakeV2(t)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{
		Client: client.New(f.URL, ""), RemoteRoot: "/",
		CacheDir: engineCacheDir(t), CacheSize: total, CacheSizeConfigured: true,
	})
	br := eng.Bridge
	budget := br.StagingBudgetForTest()
	if budget <= 1 {
		t.Fatalf("staging budget = %d, too small for this test", budget)
	}
	stageFile := func(i int) error {
		ino := meta.Ino(300 + i)
		path := fmt.Sprintf("/d/s%d.bin", i)
		f.seedExtent(t, path, "")
		br.RememberPathForTest(ino, path)
		// Each barrier is sized to fill roughly half the reserve, so the
		// second one cannot fit beside the first.
		data := make([]byte, budget/2)
		for j := range data {
			data[j] = byte(i)
		}
		sum := sha256Hex(data)
		br.StageBlockForTest(sum, data)
		br.AddPendingForTest(ino, 0, 0, sum, int64(len(data)), int64(len(data)))
		return br.StagePendingLocal(ino)
	}

	if err := stageFile(0); err != nil {
		t.Fatalf("first fsync must succeed: %v", err)
	}
	entries, err := os.ReadDir(br.cfg.StagingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("barriers after the first fsync = %d, want 1", len(entries))
	}
	firstDir := entries[0].Name()

	if err := stageFile(1); err == nil {
		t.Fatal("a fsync that would exceed the split allowance must fail, not silently evict")
	}
	entries, err = os.ReadDir(br.cfg.StagingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != firstDir {
		t.Fatalf("the acknowledged barrier was disturbed: entries=%v want [%s]", entries, firstDir)
	}
}

// CacheSize == 0 is an explicit memory-only choice: no durable staging tree
// may be armed behind the operator's back — and, crucially, fsync must still
// keep its promise. With no barrier available the mount stays STRICT, so an
// interactive sync publishes remotely instead of returning success for bytes
// that exist only in memory.
func TestExplicitZeroCacheSizeDisablesStaging(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{
		Client: client.New(f.URL, ""), RemoteRoot: "/",
		CacheDir: engineCacheDir(t), CacheSize: 0, CacheSizeConfigured: true,
	})
	if eng.Bridge.StagingDirForTest() != "" {
		t.Fatalf("staging dir = %q, want none for an explicit CacheSize=0", eng.Bridge.StagingDirForTest())
	}
	// Interactive mode, but with no barrier to defer onto: fsync must land
	// remotely rather than acknowledge memory-only data.
	h := New(Options{Engine: eng, Threshold: 0, SyncMode: "interactive"})
	res, _, _ := dispatchCreate(context.Background(), h, "/d/z.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, []byte("NOCACHE")); err != nil {
		t.Fatal(err)
	}
	if err := eh.Sync(context.Background()); err != nil {
		t.Fatalf("fsync must still be durable without a disk cache: %v", err)
	}
	if got := f.contentOf("/d/z.bin"); got != "NOCACHE" {
		t.Fatalf("content after fsync = %q, want NOCACHE published remotely (not memory-only)", got)
	}
}

// StagingBudgetForTest exposes the reserve carved out of the cache allowance.
func (b *Bridge) StagingBudgetForTest() int64 { return b.cfg.StagingBudget }

// JuiceCacheSizeForTest exposes the chunk store's share of the allowance.
func (e *Engine) JuiceCacheSizeForTest() int64 { return e.juiceCacheSize }

// StagingBytesForTest exposes the retained-bytes counter.
func (b *Bridge) StagingBytesForTest() int64 { return b.stagingBytes.Load() }
