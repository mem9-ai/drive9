// Package testbackend provides a shared in-memory fake drive9 backend
// speaking the fs HTTP wire-contract subset used by client-side tests:
// HEAD stat headers, ?list=1, Range GETs, conditional PUTs, multipart
// sessions, namespace-mutation POSTs, DELETE, plus git-workspace,
// fs-layer, and fault-injection endpoints. Each test wires its own
// instance behind an httptest.Server.
package testbackend

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Node is one in-memory filesystem entry.
type Node struct {
	Data      []byte
	IsDir     bool
	Mode      uint32 // full mode, including S_IFMT for symlinks
	Rev       int64
	Mtime     int64
	ExtentIno uint64 // nonzero: stat advertises content_layout=extent

	// ContentLayout overrides the stat-advertised layout ("append_log",
	// "single", "" = none). Seeded by tests to model server-side append-log
	// objects.
	ContentLayout string
}

// LayerEntry is one recorded fs-layer entry (the layers subtree is a log,
// not part of the base namespace).
type LayerEntry struct {
	Path        string
	Op          string
	Kind        string
	Mode        uint32
	Content     []byte
	ContentText string
	// StorageType marks object-uploaded bodies ("s3"): entry fetches reply
	// it and omit the inline content, forcing the stream fetch.
	StorageType string
	Seq         int64
}

// Backend implements http.Handler with the drive9 fs wire contract subset:
// HEAD stat headers, ?list=1, Range GETs, conditional PUTs, create/mkdir/
// rename/symlink/hardlink/chmod POSTs, DELETE, and a forced-conflict knob
// for compare-and-set retry paths.
type Backend struct {
	mu      sync.Mutex
	files   map[string]*Node
	nextRev int64
	// ForceConflict makes the next N conditional writes to a path answer
	// 409, simulating a concurrent revision bump.
	forceConflict map[string]int
	// failDelete counts down injected DELETE failures per path.
	failDelete map[string]int
	requests   map[string]int
	// AppendUnsupported makes ?append-log answer the unsupported code,
	// forcing callers onto their full-rewrite fallback.
	AppendUnsupported bool
	// PatchUnsupported makes PATCH /v1/fs answer the not-S3-stored code,
	// forcing callers onto their full-rewrite fallback.
	PatchUnsupported bool
	// layerLog records fs-layer entries per layer ID; layerSeq is its cursor.
	layerLog map[string][]LayerEntry
	layerSeq int64
	// layerEvents holds synthetic events (rollbacks) injected by tests,
	// appended after the entry-derived write events.
	layerEvents map[string][]LayerEntry
	// checkpoints maps checkpoint ID → durable seq bound.
	checkpoints map[string]int64
	// layerStates maps layer ID → state for the bare layer GET.
	layerStates map[string]string
	// gitWorkspaces seeds GET /v1/git-workspaces; gitOverlay is the overlay
	// log per workspace (path → entry).
	gitWorkspaces []map[string]any
	gitOverlay    map[string]map[string]map[string]any
	// gitStates/gitPacks back the git-state checkpoint endpoints.
	gitStates map[string][]byte
	gitPacks  map[string][][]byte
	// layerEntryDelay slows layer-entry POSTs (test knob).
	layerEntryDelay time.Duration
	// uploads tracks v2 multipart sessions (initiate → parts → complete).
	uploads   map[string]*uploadState
	uploadSeq int64
}

type uploadState struct {
	path      string
	parts     map[int][]byte
	partSize  int64
	totalSize int64
}

// partSizeFor returns the byte size of a 1-based part number.
func (us *uploadState) partSizeFor(num int) int64 {
	size := us.totalSize - int64(num-1)*us.partSize
	if size > us.partSize {
		return us.partSize
	}
	if size < 0 {
		return 0
	}
	return size
}

func NewBackend() *Backend {
	return &Backend{
		files:         map[string]*Node{"/": {IsDir: true, Mode: 0o755}},
		nextRev:       10, // fixtures use revs 1..9
		forceConflict: map[string]int{},
		failDelete:    map[string]int{},
		layerLog:      map[string][]LayerEntry{},
		layerEvents:   map[string][]LayerEntry{},
		uploads:       map[string]*uploadState{},
		layerStates:   map[string]string{},
		checkpoints:   map[string]int64{},
		requests:      map[string]int{},
		gitStates:     map[string][]byte{},
		gitPacks:      map[string][][]byte{},
	}
}

// Seed installs a file fixture (perms-only mode).
func (b *Backend) Seed(path string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &Node{Data: data, Mode: 0o644, Rev: 1, Mtime: 1}
}

// SeedDir installs a directory fixture.
// SeedHardlink plants a second path sharing the node's content, matching
// the server's hardlink resource-sharing behavior for tests.
func (b *Backend) SeedHardlink(src, dst string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n, ok := b.files[src]; ok {
		// Hardlinks share one resource (one Node): a write through either
		// name is visible through both, matching the server.
		b.files[dst] = n
	}
}

// SeedSymlink plants a symlink node whose target rides the node data.
func (b *Backend) SeedSymlink(path, target string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &Node{Data: []byte(target), Mode: uint32(syscall.S_IFLNK) | 0o777}
}

func (b *Backend) SeedDir(path string, mode uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &Node{IsDir: true, Mode: mode, Rev: 1, Mtime: 1}
}

// SeedWithLayout seeds a file whose stats advertise a content layout.
func (b *Backend) SeedWithLayout(path string, data []byte, layout string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &Node{Data: append([]byte(nil), data...), Mode: 0o644, Rev: 1, Mtime: 1, ContentLayout: layout}
}

// Node returns a snapshot of one entry.
func (b *Backend) Node(path string) (Node, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.files[path]
	if !ok {
		return Node{}, false
	}
	return Node{Data: append([]byte(nil), n.Data...), IsDir: n.IsDir, Mode: n.Mode, Rev: n.Rev, Mtime: n.Mtime}, true
}

// Snapshot returns a deep copy of all entries (path → node).
func (b *Backend) Snapshot() map[string]Node {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]Node, len(b.files))
	for p, n := range b.files {
		out[p] = Node{Data: append([]byte(nil), n.Data...), IsDir: n.IsDir, Mode: n.Mode, Rev: n.Rev, Mtime: n.Mtime}
	}
	return out
}

// SeedGitWorkspace installs one workspace for GET /v1/git-workspaces.
func (b *Backend) SeedGitWorkspace(id, root string) {
	b.SeedGitWorkspaceFull(id, root, "", "")
}

// SeedGitWorkspaceFull seeds a workspace with repo URL and head commit
// (git-state restore needs the object source).
func (b *Backend) SeedGitWorkspaceFull(id, root, repoURL, headCommit string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gitWorkspaces = append(b.gitWorkspaces, map[string]any{
		"workspace_id": id, "root_path": root, "status": "active", "workspace_kind": "standard",
		"repo_url": repoURL, "head_commit": headCommit,
	})
}

// GitState returns the stored checkpoint content for one workspace.
func (b *Backend) GitState(id string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.gitStates[id]
	return c, ok
}

// GitPacks returns the stored object packs for one workspace.
func (b *Backend) GitPacks(id string) [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]byte(nil), b.gitPacks[id]...)
}

// GitOverlay returns the recorded overlay entries for one workspace.
func (b *Backend) GitOverlay(id string) map[string]map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]map[string]any{}
	for k, v := range b.gitOverlay[id] {
		out[k] = v
	}
	return out
}

// SetCheckpoint pins a checkpoint's durable seq bound.
// LayerStateForTest reports the tracked state of a layer.
func (b *Backend) LayerStateForTest(id string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.layerStates[id]
	return st, ok
}

// SetLayerState overrides the state a bare layer GET reports.
func (b *Backend) SetLayerState(id, state string) {
	b.mu.Lock()
	b.layerStates[id] = state
	b.mu.Unlock()
}

func (b *Backend) SetCheckpoint(id string, seq int64) {
	b.mu.Lock()
	b.checkpoints[id] = seq
	b.mu.Unlock()
}

// LayerLog returns the recorded fs-layer entries for one layer ID.
func (b *Backend) LayerLog(layerID string) []LayerEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]LayerEntry(nil), b.layerLog[layerID]...)
}

// Requests returns the per "METHOD path" request counts observed so far.
func (b *Backend) Requests() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]int, len(b.requests))
	for k, v := range b.requests {
		out[k] = v
	}
	return out
}

// SeedExtent installs a file whose stat advertises the extent layout with
// the given extent object-store inode.
func (b *Backend) SeedExtent(path string, data []byte, extentIno uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &Node{Data: data, Mode: 0o644, Rev: 1, Mtime: 1, ExtentIno: extentIno}
}

// DelayLayerEntriesForTest slows layer-entry POSTs (mutation-lock rows).
func (b *Backend) DelayLayerEntriesForTest(d time.Duration) {
	b.mu.Lock()
	b.layerEntryDelay = d
	b.mu.Unlock()
}

// ReplaceWithClassic swaps an extent-seeded file for a classic
// single-layout node (identity/layout change rows).
func (b *Backend) ReplaceWithClassic(path string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &Node{Data: data, Mode: 0o644, Rev: 2, Mtime: 2}
}

// ForceConflict arms N forced 409s for conditional writes to path.
func (b *Backend) ForceConflict(path string, n int) {
	b.mu.Lock()
	b.forceConflict[path] = n
	b.mu.Unlock()
}

// ForceDeleteFailure injects n failing DELETE responses for path.
func (b *Backend) ForceDeleteFailure(path string, n int) {
	b.mu.Lock()
	b.failDelete[path] = n
	b.mu.Unlock()
}

// AppendLayerEvent injects a synthetic layer event (e.g. rollback) for the
// watcher tests.
func (b *Backend) AppendLayerEvent(layerID, op string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.layerSeq++
	b.layerEvents[layerID] = append(b.layerEvents[layerID], LayerEntry{Op: op, Seq: b.layerSeq})
}

func (b *Backend) writeErr(w http.ResponseWriter, code int, msg string) {
	b.writeErrCode(w, code, msg, "")
}

func (b *Backend) writeErrCode(w http.ResponseWriter, code int, msg, errCode string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	body := map[string]string{"error": msg}
	if errCode != "" {
		body["code"] = errCode
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (b *Backend) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type listEntry struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	IsDir     bool   `json:"isDir"`
	Revision  int64  `json:"revision"`
	Mtime     int64  `json:"mtime"`
	Mode      uint32 `json:"mode"`
	HasMode   bool   `json:"hasMode"`
	ExtentIno uint64 `json:"extent_ino,omitempty"`
}

func (b *Backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	b.mu.Lock()
	b.requests[key]++
	b.mu.Unlock()
	// v2 multipart uploads
	if strings.HasPrefix(r.URL.Path, "/v2/uploads/") {
		rest := strings.TrimPrefix(r.URL.Path, "/v2/uploads/")
		switch {
		case rest == "initiate" && r.Method == http.MethodPost:
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			b.mu.Lock()
			b.uploadSeq++
			id := fmt.Sprintf("up-%d", b.uploadSeq)
			partSize := int64(16 << 20)
			total := req.TotalSize
			nParts := (total + partSize - 1) / partSize
			if nParts < 1 {
				nParts = 1
			}
			b.uploads[id] = &uploadState{path: req.Path, parts: map[int][]byte{}, partSize: partSize, totalSize: total}
			b.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			b.writeJSON(w, map[string]any{"upload_id": id, "key": req.Path, "part_size": partSize, "total_parts": nParts, "resumable": false})
			return
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			id, tail := rest[:i], rest[i+1:]
			abs := func(u string) string {
				if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
					return u
				}
				return "http://" + r.Host + u
			}
			switch {
			case tail == "presign" && r.Method == http.MethodPost:
				var req struct {
					PartNumber int `json:"part_number"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				b.mu.Lock()
				us := b.uploads[id]
				b.mu.Unlock()
				if us == nil {
					b.writeErr(w, http.StatusNotFound, "no such upload")
					return
				}
				b.writeJSON(w, map[string]any{
					"number": req.PartNumber,
					"url":    abs("/v2/uploads/" + id + "/part/" + strconv.Itoa(req.PartNumber)),
					"size":   us.partSizeFor(req.PartNumber),
				})
				return
			case tail == "presign-batch" && r.Method == http.MethodPost:
				b.mu.Lock()
				us := b.uploads[id]
				b.mu.Unlock()
				if us == nil {
					b.writeErr(w, http.StatusNotFound, "no such upload")
					return
				}
				var req struct {
					Parts []struct {
						PartNumber int `json:"part_number"`
					} `json:"parts"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				urls := make([]map[string]any, 0, len(req.Parts))
				for _, pt := range req.Parts {
					urls = append(urls, map[string]any{
						"number": pt.PartNumber,
						"url":    abs("/v2/uploads/" + id + "/part/" + strconv.Itoa(pt.PartNumber)),
						"size":   us.partSizeFor(pt.PartNumber),
					})
				}
				b.writeJSON(w, map[string]any{"parts": urls})
				return
			case tail == "complete" && r.Method == http.MethodPost:
				b.mu.Lock()
				us := b.uploads[id]
				delete(b.uploads, id)
				b.mu.Unlock()
				if us == nil {
					b.writeErr(w, http.StatusNotFound, "no such upload")
					return
				}
				var req struct {
					Parts []struct {
						Number int    `json:"number"`
						ETag   string `json:"etag"`
					} `json:"parts"`
					ChecksumSHA256 string `json:"checksum_sha256"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				var image []byte
				for _, pt := range req.Parts {
					b.mu.Lock()
					image = append(image, us.parts[pt.Number]...)
					b.mu.Unlock()
				}
				b.mu.Lock()
				rev := b.nextRev
				b.nextRev++
				n := b.files[us.path]
				if n == nil {
					n = &Node{Mode: 0o644}
					b.files[us.path] = n
				}
				n.Data = image
				n.Rev = rev
				n.Mtime = rev
				b.mu.Unlock()
				b.writeJSON(w, map[string]any{"status": "ok", "revision": rev})
				return
			case tail == "abort" && r.Method == http.MethodPost:
				b.mu.Lock()
				delete(b.uploads, id)
				b.mu.Unlock()
				w.WriteHeader(http.StatusOK)
				return
			}
			if strings.HasPrefix(tail, "part/") && r.Method == http.MethodPut {
				num, _ := strconv.Atoi(strings.TrimPrefix(tail, "part/"))
				body, _ := io.ReadAll(r.Body)
				b.mu.Lock()
				us := b.uploads[id]
				if us != nil {
					us.parts[num] = body
				}
				b.mu.Unlock()
				if us == nil {
					b.writeErr(w, http.StatusNotFound, "no such upload")
					return
				}
				etag := fmt.Sprintf("etag-%d", num)
				w.Header().Set("ETag", etag)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(etag))
				return
			}
		}
		b.writeErr(w, http.StatusNotFound, "upload endpoint not found")
		return
	}
	// git workspaces: list + overlay log
	if strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/") && strings.HasSuffix(r.URL.Path, "/git-state") && r.Method == http.MethodPost {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/"), "/git-state")
		var req struct {
			StorageType    string `json:"storage_type"`
			ChecksumSHA256 string `json:"checksum_sha256"`
			SizeBytes      int64  `json:"size_bytes"`
			Content        []byte `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		b.mu.Lock()
		b.gitStates[id] = req.Content
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"workspace_id": id, "storage_type": req.StorageType})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/") && strings.HasSuffix(r.URL.Path, "/git-state") && r.Method == http.MethodGet {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/"), "/git-state")
		b.mu.Lock()
		content, ok := b.gitStates[id]
		b.mu.Unlock()
		if !ok {
			b.writeErr(w, http.StatusNotFound, "git state not found")
			return
		}
		b.writeJSON(w, map[string]any{"workspace_id": id, "storage_type": "tar.gz-no-objects", "content": content})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/") && strings.HasSuffix(r.URL.Path, "/object-packs") && r.Method == http.MethodPost {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/"), "/object-packs")
		var req struct {
			Content []byte `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		b.mu.Lock()
		b.gitPacks[id] = append(b.gitPacks[id], req.Content)
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"pack_id": fmt.Sprintf("p%d", len(b.gitPacks[id]))})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/") && strings.HasSuffix(r.URL.Path, "/object-packs") && r.Method == http.MethodGet {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/"), "/object-packs")
		b.mu.Lock()
		packs := b.gitPacks[id]
		b.mu.Unlock()
		out := make([]map[string]any, 0, len(packs))
		for i, p := range packs {
			out = append(out, map[string]any{"pack_id": fmt.Sprintf("p%d", i+1), "size_bytes": int64(len(p))})
		}
		b.writeJSON(w, map[string]any{"packs": out})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/") && strings.Contains(r.URL.Path, "/object-packs/") && r.Method == http.MethodGet {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/")
		id := rest[:strings.IndexByte(rest, '/')]
		packID := strings.TrimPrefix(rest[strings.IndexByte(rest, '/'):], "/object-packs/")
		b.mu.Lock()
		packs := b.gitPacks[id]
		b.mu.Unlock()
		idx := 0
		_, _ = fmt.Sscanf(strings.TrimPrefix(packID, "p"), "%d", &idx)
		if idx < 1 || idx > len(packs) {
			b.writeErr(w, http.StatusNotFound, "pack not found")
			return
		}
		b.writeJSON(w, map[string]any{"pack_id": packID, "content": packs[idx-1], "size_bytes": int64(len(packs[idx-1]))})
		return
	}
	if r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet {
		b.mu.Lock()
		out := make([]map[string]any, 0, len(b.gitWorkspaces))
		out = append(out, b.gitWorkspaces...)
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"workspaces": out})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/") {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/")
		if i := strings.IndexByte(rest, '/'); i >= 0 && rest[i+1:] == "overlay" {
			id := rest[:i]
			switch r.Method {
			case http.MethodPost:
				var req struct {
					Path    string `json:"path"`
					Op      string `json:"op"`
					Kind    string `json:"kind"`
					Mode    string `json:"mode"`
					Content []byte `json:"content"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				b.mu.Lock()
				if b.gitOverlay == nil {
					b.gitOverlay = map[string]map[string]map[string]any{}
				}
				if b.gitOverlay[id] == nil {
					b.gitOverlay[id] = map[string]map[string]any{}
				}
				entry := map[string]any{"workspace_id": id, "path": req.Path, "op": req.Op, "kind": req.Kind, "mode": req.Mode, "content": req.Content}
				b.gitOverlay[id][req.Path] = entry
				b.mu.Unlock()
				b.writeJSON(w, entry)
				return
			case http.MethodGet:
				b.mu.Lock()
				var entries []map[string]any
				for _, e := range b.gitOverlay[id] {
					entries = append(entries, e)
				}
				b.mu.Unlock()
				b.writeJSON(w, map[string]any{"entries": entries})
				return
			}
		}
	}
	// checkpoints: GET /v1/layer-checkpoints/{id} (durable_seq bounds replay)
	if strings.HasPrefix(r.URL.Path, "/v1/layer-checkpoints/") && r.Method == http.MethodGet {
		id := strings.TrimPrefix(r.URL.Path, "/v1/layer-checkpoints/")
		b.mu.Lock()
		seq := b.layerSeq
		if cp, ok := b.checkpoints[id]; ok {
			seq = cp
		}
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"checkpoint_id": id, "layer_id": "L1", "durable_seq": seq})
		return
	}
	// fs-layer subtree: POST /v1/layers/{id}/entries, GET /v1/layers/{id}/diff
	if strings.HasPrefix(r.URL.Path, "/v1/layers/") {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/layers/")
		id := rest
		if i := strings.IndexByte(rest, '/'); i < 0 && r.Method == http.MethodDelete {
			b.mu.Lock()
			b.layerStates[id] = "abandoned"
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
		if i := strings.IndexByte(rest, '/'); i < 0 && r.Method == http.MethodGet {
			b.mu.Lock()
			state := b.layerStates[id]
			b.mu.Unlock()
			if state == "" {
				state = "active"
			}
			b.writeJSON(w, map[string]any{"layer_id": id, "base_root_path": "/", "state": state, "name": ""})
			return
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			id = rest[:i]
			tail := rest[i+1:]
			switch {
			case tail == "entries" && r.Method == http.MethodPost:
				var req struct {
					Path        string `json:"path"`
					Op          string `json:"op"`
					Kind        string `json:"kind"`
					Mode        uint32 `json:"mode"`
					Content     []byte `json:"content"`
					ContentText string `json:"content_text"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				b.mu.Lock()
				delay := b.layerEntryDelay
				b.layerSeq++
				e := LayerEntry{Path: req.Path, Op: req.Op, Kind: req.Kind, Mode: req.Mode, Content: req.Content, ContentText: req.ContentText, Seq: b.layerSeq}
				b.layerLog[id] = append(b.layerLog[id], e)
				b.mu.Unlock()
				if delay > 0 {
					time.Sleep(delay)
				}
				b.writeJSON(w, map[string]any{"layer_id": id, "path": req.Path, "op": req.Op, "seq": e.Seq})
				return
			case tail == "commit" && r.Method == http.MethodPost:
				b.mu.Lock()
				b.layerStates[id] = "committed"
				b.mu.Unlock()
				b.writeJSON(w, map[string]any{"status": "committed", "layer_id": id, "applied": 0})
				return
			case tail == "fork" && r.Method == http.MethodPost:
				var req struct {
					LayerID      string `json:"layer_id"`
					Name         string `json:"name"`
					CheckpointID string `json:"checkpoint_id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				child := req.LayerID
				if child == "" {
					child = "layer-forked"
				}
				b.mu.Lock()
				parent := b.layerStates[id]
				b.mu.Unlock()
				if parent == "committed" || parent == "abandoned" || parent == "conflicted" {
					b.writeErr(w, http.StatusConflict, "parent layer is "+parent)
					return
				}
				b.mu.Lock()
				b.layerStates[child] = "active"
				b.mu.Unlock()
				b.writeJSON(w, map[string]any{"layer_id": child, "base_root_path": "/", "state": "active", "parent_layer_id": id, "depth": 1})
				return
			case tail == "checkpoints" && r.Method == http.MethodPost:
				var req struct {
					CheckpointID string `json:"checkpoint_id"`
					Label        string `json:"label"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				cp := req.CheckpointID
				if cp == "" {
					cp = "ckpt-auto"
				}
				b.mu.Lock()
				b.layerSeq++
				b.checkpoints[cp] = b.layerSeq
				b.mu.Unlock()
				b.writeJSON(w, map[string]any{"checkpoint_id": cp, "layer_id": id, "durable_seq": 0, "label": req.Label})
				return
			case tail == "events" && r.Method == http.MethodGet:
				since := int64(0)
				if v := r.URL.Query().Get("since"); v != "" {
					since, _ = strconv.ParseInt(v, 10, 64)
				}
				b.mu.Lock()
				log := append([]LayerEntry(nil), b.layerLog[id]...)
				extra := append([]LayerEntry(nil), b.layerEvents[id]...)
				b.mu.Unlock()
				type ev struct {
					Seq int64  `json:"seq"`
					Op  string `json:"op"`
				}
				out := []ev{}
				for _, e := range log {
					if e.Seq > since {
						out = append(out, ev{Seq: e.Seq, Op: "write"})
					}
				}
				for _, e := range extra {
					if e.Seq > since {
						out = append(out, ev{Seq: e.Seq, Op: e.Op})
					}
				}
				b.writeJSON(w, map[string]any{"events": out})
				return
			case tail == "entries" && r.Method == http.MethodGet:
				path := r.URL.Query().Get("path")
				maxSeq := int64(-1)
				if v := r.URL.Query().Get("max_seq"); v != "" {
					maxSeq, _ = strconv.ParseInt(v, 10, 64)
				}
				b.mu.Lock()
				log := append([]LayerEntry(nil), b.layerLog[id]...)
				b.mu.Unlock()
				var found *LayerEntry
				for i := range log {
					e := &log[i]
					if e.Path != path || (maxSeq >= 0 && e.Seq > maxSeq) {
						continue
					}
					if found == nil || e.Seq > found.Seq {
						found = e
					}
				}
				if found == nil {
					b.writeErr(w, http.StatusNotFound, "layer endpoint not found")
					return
				}
				m := map[string]any{
					"layer_id": id, "path": found.Path, "op": found.Op, "kind": found.Kind,
					"mode": found.Mode, "size_bytes": int64(len(found.Content)), "entry_seq": found.Seq,
					"content_text": found.ContentText,
				}
				if found.StorageType == "s3" {
					m["storage_type"] = "s3"
				} else {
					m["content"] = found.Content
				}
				b.writeJSON(w, m)
				return
			case tail == "objects" && r.Method == http.MethodPost:
				path := r.URL.Query().Get("path")
				mode := uint32(0o644)
				if v := r.URL.Query().Get("mode"); v != "" {
					parsed, _ := strconv.ParseInt(v, 8, 32)
					mode = uint32(parsed)
				}
				baseRev := int64(0)
				if v := r.URL.Query().Get("base_revision"); v != "" {
					baseRev, _ = strconv.ParseInt(v, 10, 64)
				}
				body, rerr := io.ReadAll(r.Body)
				if rerr != nil {
					b.writeErr(w, http.StatusBadRequest, "read body")
					return
				}
				b.mu.Lock()
				b.layerSeq++
				e := LayerEntry{Path: path, Op: "upsert", Kind: "file", Mode: mode, Content: body, StorageType: "s3", Seq: b.layerSeq}
				b.layerLog[id] = append(b.layerLog[id], e)
				b.mu.Unlock()
				b.writeJSON(w, map[string]any{"layer_id": id, "path": path, "op": "upsert", "entry_seq": e.Seq, "storage_type": "s3", "size_bytes": int64(len(body)), "base_revision": baseRev})
				return
			case tail == "objects" && r.Method == http.MethodGet:
				path := r.URL.Query().Get("path")
				maxSeq := int64(-1)
				if v := r.URL.Query().Get("max_seq"); v != "" {
					maxSeq, _ = strconv.ParseInt(v, 10, 64)
				}
				b.mu.Lock()
				log := append([]LayerEntry(nil), b.layerLog[id]...)
				b.mu.Unlock()
				var found *LayerEntry
				for i := range log {
					e := &log[i]
					if e.Path != path || e.Op != "upsert" || e.Kind != "file" || (maxSeq >= 0 && e.Seq > maxSeq) {
						continue
					}
					if found == nil || e.Seq > found.Seq {
						found = e
					}
				}
				if found == nil {
					b.writeErr(w, http.StatusNotFound, "layer object not found")
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.WriteHeader(http.StatusOK)
				if len(found.Content) > 0 {
					_, _ = w.Write(found.Content)
				} else {
					_, _ = w.Write([]byte(found.ContentText))
				}
				return
			case tail == "diff" && r.Method == http.MethodGet:
				b.mu.Lock()
				log := append([]LayerEntry(nil), b.layerLog[id]...)
				b.mu.Unlock()
				maxSeq := int64(-1)
				if v := r.URL.Query().Get("max_seq"); v != "" {
					maxSeq, _ = strconv.ParseInt(v, 10, 64)
				}
				out := make([]map[string]any, 0, len(log))
				for _, e := range log {
					if maxSeq >= 0 && e.Seq > maxSeq {
						continue
					}
					m := map[string]any{"layer_id": id, "path": e.Path, "op": e.Op, "kind": e.Kind, "mode": e.Mode, "entry_seq": e.Seq}
					if len(e.Content) > 0 {
						m["content"] = e.Content
					}
					if e.ContentText != "" {
						m["content_text"] = e.ContentText
					}
					out = append(out, m)
				}
				b.writeJSON(w, map[string]any{"entries": out})
				return
			}
		}
		if r.Method == http.MethodGet && id != "" && !strings.Contains(rest, "/") {
			b.writeJSON(w, map[string]any{"layer_id": id, "state": "active"})
			return
		}
		b.writeErr(w, http.StatusNotFound, "layer endpoint not found")
		return
	}

	// Batch endpoints: POST /v1/fs:batch-stat and /v1/fs:batch-read-small
	// mirror the server's per-path result arrays.
	if r.URL.Path == "/v1/fs:batch-stat" && r.Method == http.MethodPost {
		var req struct {
			Paths []string `json:"paths"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		results := make([]map[string]any, 0, len(req.Paths))
		b.mu.Lock()
		for _, p := range req.Paths {
			n := b.files[p]
			if n == nil {
				results = append(results, map[string]any{"path": p, "status": 404, "error": "not found"})
				continue
			}
			m := map[string]any{"path": p, "status": 200, "size": int64(len(n.Data)), "isDir": n.IsDir, "revision": n.Rev}
			if n.Mode != 0 {
				m["mode"] = n.Mode
				m["hasMode"] = true
			}
			results = append(results, m)
		}
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"results": results})
		return
	}
	if r.URL.Path == "/v1/fs:batch-read-small" && r.Method == http.MethodPost {
		var req struct {
			Paths    []string `json:"paths"`
			MaxBytes int64    `json:"max_bytes"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		type result struct {
			Path     string `json:"path"`
			Status   int    `json:"status"`
			Error    string `json:"error,omitempty"`
			Data     []byte `json:"data,omitempty"`
			Size     int64  `json:"size,omitempty"`
			Revision int64  `json:"revision,omitempty"`
		}
		results := make([]result, 0, len(req.Paths))
		b.mu.Lock()
		for _, p := range req.Paths {
			n := b.files[p]
			switch {
			case n == nil:
				results = append(results, result{Path: p, Status: 404, Error: "not found"})
			case n.IsDir:
				results = append(results, result{Path: p, Status: 400, Error: "is a directory"})
			case req.MaxBytes > 0 && int64(len(n.Data)) > req.MaxBytes:
				results = append(results, result{Path: p, Status: 413, Error: "too large"})
			default:
				results = append(results, result{Path: p, Status: 200, Data: n.Data, Size: int64(len(n.Data)), Revision: n.Rev})
			}
		}
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"results": results})
		return
	}

	// Tenant status: a large inline threshold keeps small writes on the
	// conditional-PUT path instead of the multipart v2 upload protocol.
	// Patch completes share the plain uploads complete endpoint: the
	// client lands the final object there after uploading its dirty parts.
	if strings.HasPrefix(r.URL.Path, "/v1/uploads/") && strings.HasSuffix(r.URL.Path, "/complete") && r.Method == http.MethodPost {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/uploads/"), "/complete")
		b.mu.Lock()
		us := b.uploads[id]
		delete(b.uploads, id)
		b.mu.Unlock()
		if us == nil {
			b.writeErr(w, http.StatusNotFound, "no such upload")
			return
		}
		b.mu.Lock()
		nums := make([]int, 0, len(us.parts))
		for pn := range us.parts {
			nums = append(nums, pn)
		}
		sort.Ints(nums)
		image := make([]byte, 0, us.totalSize)
		for _, pn := range nums {
			image = append(image, us.parts[pn]...)
		}
		if int64(len(image)) > us.totalSize {
			image = image[:us.totalSize]
		}
		rev := b.nextRev
		b.nextRev++
		n := b.files[us.path]
		if n == nil {
			n = &Node{Mode: 0o644}
			b.files[us.path] = n
		}
		n.Data = image
		n.Rev = rev
		n.Mtime = rev
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"status": "ok", "revision": rev})
		return
	}
	if r.URL.Path == "/v1/status" && r.Method == http.MethodGet {
		b.writeJSON(w, map[string]any{
			"status":               "ok",
			"max_upload_bytes":     1 << 30,
			"inline_threshold":     1 << 20,
			"storage_capabilities": map[string]any{"append_log_v1": true},
		})
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if p == "" {
		p = "/"
	}
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPatch:
		var req struct {
			NewSize          int64  `json:"new_size"`
			DirtyParts       []int  `json:"dirty_parts"`
			PartSize         int64  `json:"part_size"`
			ExpectedRevision *int64 `json:"expected_revision"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		b.mu.Lock()
		n := b.files[p]
		if n == nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if b.PatchUnsupported {
			b.mu.Unlock()
			b.writeErr(w, http.StatusBadRequest, "file is not S3-stored: db9")
			return
		}
		if req.ExpectedRevision != nil && *req.ExpectedRevision != n.Rev {
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "revision conflict")
			return
		}
		partSize := req.PartSize
		if partSize <= 0 {
			partSize = 16 << 20
		}
		b.uploadSeq++
		id := fmt.Sprintf("patch-%d", b.uploadSeq)
		us := &uploadState{path: p, parts: map[int][]byte{}, partSize: partSize, totalSize: req.NewSize}
		// Seed every part of the new layout from the current object: a
		// part the client uploads overwrites its seed; the rest assemble
		// as copies at complete time.
		newParts := (req.NewSize + partSize - 1) / partSize
		if newParts < 1 {
			newParts = 1
		}
		dirtySet := make(map[int]bool, len(req.DirtyParts))
		for _, pn := range req.DirtyParts {
			dirtySet[pn] = true
		}
		var copied []int
		for pn := int64(1); pn <= newParts; pn++ {
			lo := (pn - 1) * partSize
			hi := lo + partSize
			if hi > int64(len(n.Data)) {
				hi = int64(len(n.Data))
			}
			if lo < hi {
				us.parts[int(pn)] = append([]byte(nil), n.Data[lo:hi]...)
			}
			if !dirtySet[int(pn)] {
				copied = append(copied, int(pn))
			}
		}
		b.uploads[id] = us
		host := r.Host
		b.mu.Unlock()
		uploadParts := make([]map[string]any, 0, len(req.DirtyParts))
		for _, pn := range req.DirtyParts {
			uploadParts = append(uploadParts, map[string]any{
				"number":     pn,
				"url":        "http://" + host + "/v2/uploads/" + id + "/part/" + strconv.Itoa(pn),
				"size":       us.partSizeFor(pn),
				"expires_at": time.Now().Add(time.Hour).Format(time.RFC3339),
			})
		}
		w.WriteHeader(http.StatusAccepted)
		b.writeJSON(w, map[string]any{
			"upload_id":    id,
			"part_size":    partSize,
			"upload_parts": uploadParts,
			"copied_parts": copied,
		})
		return
	case r.Method == http.MethodHead:
		b.mu.Lock()
		n := b.files[p]
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		w.Header().Set("X-Dat9-IsDir", strconv.FormatBool(n.IsDir))
		w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(n.Rev, 10))
		w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(n.Mode), 10))
		w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(n.Mtime, 10))
		// Mirror the real server's placement decision: objects at or
		// above the advertised inline threshold live in object storage.
		storageType := "db9"
		if int64(len(n.Data)) >= 1<<20 {
			storageType = "s3"
		}
		w.Header().Set("X-Dat9-Storage-Type", storageType)
		if n.ContentLayout != "" {
			w.Header().Set("X-Dat9-Content-Layout", n.ContentLayout)
		}
		if ino := n.ExtentIno; ino != 0 {
			w.Header().Set("X-Dat9-Content-Layout", "extent")
			w.Header().Set("X-Dat9-Extent-Ino", strconv.FormatUint(ino, 10))
		}
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodGet && q.Has("list"):
		b.mu.Lock()
		n := b.files[p]
		prefix := "/"
		if p != "/" {
			prefix = p + "/"
		}
		var out []listEntry
		seen := map[string]bool{}
		dirExists := n != nil && n.IsDir
		if !dirExists {
			// implicit directory: no node seeded, but files live below it
			for cp := range b.files {
				if strings.HasPrefix(cp, prefix) {
					dirExists = true
					break
				}
			}
		}
		if dirExists {
			for cp, child := range b.files {
				if cp == p || !strings.HasPrefix(cp, prefix) {
					continue
				}
				rest := cp[len(prefix):]
				name := rest
				isDir := child.IsDir
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					name, isDir = rest[:i], true
				}
				if name == "" || seen[name] {
					continue
				}
				seen[name] = true
				mode := child.Mode
				if mode == 0 {
					mode = 0o644
				}
				out = append(out, listEntry{Name: name, Size: int64(len(child.Data)), IsDir: isDir, Revision: child.Rev, Mtime: child.Mtime, Mode: mode, HasMode: true, ExtentIno: child.ExtentIno})
			}
		}
		b.mu.Unlock()
		if n == nil && !dirExists {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		b.writeJSON(w, map[string]any{"entries": out})

	case r.Method == http.MethodGet:
		b.mu.Lock()
		n := b.files[p]
		b.mu.Unlock()
		if n == nil || n.IsDir {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			lo, hi, ok := parseRange(rng, int64(len(n.Data)))
			if !ok {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi-1, len(n.Data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(n.Data[lo:hi])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(n.Data)

	case r.Method == http.MethodPut:
		body := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			if _, err := io.ReadFull(r.Body, body); err != nil {
				b.writeErr(w, http.StatusBadRequest, "read body")
				return
			}
		}
		expected := r.Header.Get("X-Dat9-Expected-Revision")
		b.mu.Lock()
		n := b.files[p]
		if expected == "0" && n != nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "already exists")
			return
		}
		if expected != "" && expected != "0" {
			want, _ := strconv.ParseInt(expected, 10, 64)
			if n == nil || n.Rev != want {
				b.mu.Unlock()
				b.writeErr(w, http.StatusConflict, "revision mismatch")
				return
			}
		}
		if b.forceConflict[p] > 0 {
			b.forceConflict[p]--
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "forced conflict")
			return
		}
		rev := b.nextRev
		b.nextRev++
		if n == nil {
			n = &Node{Mode: 0o644}
			b.files[p] = n
		}
		n.Data = body
		n.Rev = rev
		n.Mtime = rev
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"revision": rev})

	case r.Method == http.MethodDelete:
		b.mu.Lock()
		if b.failDelete[p] > 0 {
			b.failDelete[p]--
			b.mu.Unlock()
			b.writeErr(w, http.StatusServiceUnavailable, "delete unavailable")
			return
		}
		n := b.files[p]
		delete(b.files, p)
		if q.Get("recursive") == "1" || q.Get("recursive") == "true" {
			prefix := p + "/"
			for cp := range b.files {
				if strings.HasPrefix(cp, prefix) {
					delete(b.files, cp)
				}
			}
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		w.WriteHeader(http.StatusOK)

	case q.Has("append-log"):
		tail := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			if _, err := io.ReadFull(r.Body, tail); err != nil {
				b.writeErr(w, http.StatusBadRequest, "read tail")
				return
			}
		}
		expRev, _ := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
		expSize, _ := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Size"), 10, 64)
		b.mu.Lock()
		n := b.files[p]
		if n == nil && expRev == 0 && expSize == 0 {
			// append-log creating a brand-new file
			rev := b.nextRev + 1
			b.nextRev = rev
			n = &Node{Data: append([]byte(nil), tail...), Mode: 0o644, Rev: rev, Mtime: rev}
			b.files[p] = n
			size := int64(len(n.Data))
			b.mu.Unlock()
			b.writeJSON(w, map[string]any{"revision": rev, "size_bytes": size})
			return
		}
		rebased := func() {
			b.mu.Unlock()
			b.writeErrCode(w, http.StatusConflict, "append_log_rebased", "append_log_rebased")
		}
		if n == nil || n.IsDir {
			rebased()
			return
		}
		if n.Rev != expRev || n.Rev <= 0 {
			rebased()
			return
		}
		if int64(len(n.Data)) != expSize {
			rebased()
			return
		}
		if b.AppendUnsupported {
			b.mu.Unlock()
			b.writeErrCode(w, http.StatusConflict, "append_log_unsupported", "append_log_unsupported")
			return
		}
		n.Data = append(append([]byte(nil), n.Data...), tail...)
		b.nextRev++
		n.Rev = b.nextRev
		n.Mtime = n.Rev
		rev := n.Rev
		size := int64(len(n.Data))
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"revision": rev, "size_bytes": size})

	case q.Has("create"):
		b.mu.Lock()
		if b.files[p] != nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "already exists")
			return
		}
		rev := b.nextRev
		b.nextRev++
		b.files[p] = &Node{Mode: 0o644, Rev: rev, Mtime: rev}
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"revision": rev})

	case q.Has("mkdir"):
		mode := uint32(0o755)
		if m := q.Get("mode"); m != "" {
			if v, err := strconv.ParseUint(m, 10, 32); err == nil {
				mode = uint32(v)
			}
		}
		b.mu.Lock()
		rev := b.nextRev
		b.nextRev++
		if b.files[p] == nil {
			b.files[p] = &Node{IsDir: true, Mode: mode, Rev: rev, Mtime: rev}
		}
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case q.Has("rename"):
		src := r.Header.Get("X-Dat9-Rename-Source")
		b.mu.Lock()
		n := b.files[src]
		if n != nil {
			delete(b.files, src)
			b.files[p] = n
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "source not found")
			return
		}
		w.WriteHeader(http.StatusOK)

	case q.Has("symlink"):
		var body struct {
			Target string `json:"target"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		b.mu.Lock()
		rev := b.nextRev
		b.nextRev++
		b.files[p] = &Node{Data: []byte(body.Target), Mode: uint32(syscall.S_IFLNK) | 0o777, Rev: rev, Mtime: rev}
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case q.Has("hardlink"):
		src := r.Header.Get("X-Dat9-Hardlink-Source")
		b.mu.Lock()
		n := b.files[src]
		if n != nil {
			b.files[p] = n
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "source not found")
			return
		}
		w.WriteHeader(http.StatusOK)

	case q.Has("chmod"):
		var body struct {
			Mode uint32 `json:"mode"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		b.mu.Lock()
		if n := b.files[p]; n != nil {
			n.Mode = n.Mode&uint32(syscall.S_IFMT) | (body.Mode & 0o777)
		}
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	default:
		b.writeErr(w, http.StatusBadRequest, "unhandled "+r.Method+" "+r.URL.RequestURI())
	}
}

func parseRange(rng string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(rng, "bytes=") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	lo, err1 := strconv.ParseInt(parts[0], 10, 64)
	hi, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || lo < 0 || hi < lo {
		return 0, 0, false
	}
	if lo >= size {
		return 0, 0, false
	}
	if hi >= size {
		hi = size - 1
	}
	return lo, hi + 1, true
}
