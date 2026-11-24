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
	Data  []byte
	IsDir bool
	Mode  uint32 // full mode, including S_IFMT for symlinks
	Rev   int64
	Mtime int64
	// ResourceID is the object identity the server reports as
	// X-Dat9-Resource-ID: it names the OBJECT, so it survives revisions,
	// writes and renames, while a replacement created at the same path is
	// a different object with a different id. Assigned on first stat.
	ResourceID string

	// ContentLayout overrides the stat-advertised layout ("append_log",
	// "single", "" = none). Seeded by tests to model server-side append-log
	// objects.
	ContentLayout string

	// Tags is the user-owned tag set (`?setmeta=1` replaces it wholesale).
	Tags map[string]string
}

// Backend implements http.Handler with the drive9 fs wire contract subset:
// HEAD stat headers, ?list=1, Range GETs, conditional PUTs, create/mkdir/
// rename/symlink/hardlink/chmod POSTs, DELETE, and a forced-conflict knob
// for compare-and-set retry paths.
type Backend struct {
	mu         sync.Mutex
	files      map[string]*Node
	nextRev    int64
	nextObject int64
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
	// gitWorkspaces seeds GET /v1/git-workspaces; gitOverlay is the overlay
	// log per workspace (path → entry).
	gitWorkspaces []map[string]any
	gitOverlay    map[string]map[string]map[string]any
	// gitStates/gitPacks back the git-state checkpoint endpoints.
	gitStates map[string][]byte
	gitPacks  map[string][][]byte
	// uploads tracks v2 multipart sessions (initiate → parts → complete).
	uploads   map[string]*uploadState
	uploadSeq int64
}

type uploadState struct {
	path      string
	parts     map[int][]byte
	partSize  int64
	totalSize int64
	// expected carries the initiate's CAS revision. Only the create-only case
	// (0) is enforced, by complete, facing an existing row with a 409; a
	// positive revision is recorded and ignored — see the comment at the
	// enforcement site for why this fake cannot verify one.
	expected *int64
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
		uploads:       map[string]*uploadState{},
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

// SeedWithTags installs a file fixture carrying user tags.
func (b *Backend) SeedWithTags(path string, data []byte, tags map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := &Node{Data: append([]byte(nil), data...), Mode: 0o644, Rev: 1, Mtime: 1}
	if len(tags) > 0 {
		n.Tags = map[string]string{}
		for k, v := range tags {
			n.Tags[k] = v
		}
	}
	b.files[path] = n
}

// SeedDir installs a directory fixture.
// SeedHardlink plants a second path sharing the node's content, matching
// the server's hardlink resource-sharing behavior for tests.
// SetNodeMode applies a remote chmod to a seeded node (test knob).
func (b *Backend) SetNodeMode(path string, mode uint32) {
	b.mu.Lock()
	if n, ok := b.files[path]; ok {
		n.Mode = mode
	}
	b.mu.Unlock()
}

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

// Remove drops a fixture path, modeling another writer deleting it. Tests use
// it to build races where a request in flight outlives the path it targets.
func (b *Backend) Remove(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.files, path)
}

// ObjectID returns the path's object identity, assigning one on first use
// exactly as a stat would (X-Dat9-Resource-ID). Tests that assert on a cache
// key built from an open handle's pinned identity need it.
func (b *Backend) ObjectID(path string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.files[path]
	if n == nil {
		return ""
	}
	if n.ResourceID == "" {
		b.nextObject++
		n.ResourceID = "res-" + strconv.FormatInt(b.nextObject, 10)
	}
	return n.ResourceID
}

// SharesObject reports whether two paths name the SAME object (a hardlink or
// an alias), not merely equal bytes. Tests use it to prove a copy produced an
// independent object instead of an alias.
func (b *Backend) SharesObject(a, c string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	na, ca := b.files[a]
	nb, cb := b.files[c]
	return ca && cb && na == nb
}

// Node returns a snapshot of one entry.
func (b *Backend) Node(path string) (Node, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.files[path]
	if !ok {
		return Node{}, false
	}
	return Node{Data: append([]byte(nil), n.Data...), IsDir: n.IsDir, Mode: n.Mode, Rev: n.Rev, Mtime: n.Mtime, ResourceID: n.ResourceID, Tags: cloneTags(n.Tags)}, true
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

// SeedGitWorktree registers a LINKED worktree workspace: kind, common
// workspace id, and the gitdir path relative to the common repository.
func (b *Backend) SeedGitWorktree(id, root, repoURL, headCommit, commonID, gitDirRel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gitWorkspaces = append(b.gitWorkspaces, map[string]any{
		"workspace_id": id, "root_path": root, "status": "active", "workspace_kind": "linked",
		"repo_url": repoURL, "head_commit": headCommit,
		"common_workspace_id": commonID, "gitdir_rel": gitDirRel,
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

// ForceConflict arms N forced 409s for conditional writes to path.
// forceConflictLocked consumes one forced-conflict request for path.
// Caller holds b.mu.
func (b *Backend) forceConflictLocked(path string) bool {
	if b.forceConflict[path] > 0 {
		b.forceConflict[path]--
		return true
	}
	return false
}

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
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	IsDir    bool   `json:"isDir"`
	Revision int64  `json:"revision"`
	Mtime    int64  `json:"mtime"`
	Mode     uint32 `json:"mode"`
	HasMode  bool   `json:"hasMode"`
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
			b.uploads[id] = &uploadState{path: req.Path, parts: map[int][]byte{}, partSize: partSize, totalSize: total, expected: req.ExpectedRevision}
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
				b.mu.Lock()
				forced := b.forceConflictLocked(us.path)
				b.mu.Unlock()
				if forced {
					// The forced-conflict test knob reaches the multipart
					// track too: complete 409s once per request, exactly
					// like the direct-PUT handler (same locked accessor —
					// handlers run concurrently).
					b.writeErr(w, http.StatusConflict, "forced conflict")
					return
				}
				var image []byte
				for _, pt := range req.Parts {
					b.mu.Lock()
					image = append(image, us.parts[pt.Number]...)
					b.mu.Unlock()
				}
				b.mu.Lock()
				// The server re-checks the initiate's compare-and-set
				// precondition inside the completion transaction, so a
				// destination created or rewritten while the body streams
				// still loses. Only the create-only case (expectedRevision 0)
				// is modeled here: this fake hands out revisions from one
				// shared counter (fixtures use 1..9, writes continue from 10),
				// not the server's per-file numbering, so a positive expected
				// revision is not something it could verify without also
				// changing the revision space every other suite builds on.
				if us.expected != nil && *us.expected == 0 && b.files[us.path] != nil {
					b.mu.Unlock()
					b.writeErr(w, http.StatusConflict, "already exists")
					return
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
	if id, ok := strings.CutPrefix(r.URL.Path, "/v1/layers/"); ok {
		if r.Method == http.MethodGet && id != "" && !strings.Contains(id, "/") {
			b.writeJSON(w, map[string]any{"layer_id": id, "state": "active"})
		} else {
			b.writeErr(w, http.StatusNotFound, "layer endpoint not found")
		}
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
		// Snapshot the node's fields under the lock: a concurrent PUT on
		// the same path rewrites n.Data after its own lock, and reading
		// the fields here unlocked is a data race under -race (and served
		// torn header/body pairs).
		b.mu.Lock()
		n := b.files[p]
		var isDir bool
		var dataLen int
		var rev, mtime int64
		var mode uint32
		var contentLayout string
		var resourceID string
		var nlink int
		if n != nil {
			isDir = n.IsDir
			dataLen = len(n.Data)
			rev, mtime, mode = n.Rev, n.Mtime, n.Mode
			contentLayout = n.ContentLayout
			// Hardlinks share one Node: report the link count the way the
			// server does, so callers can distinguish an alias from a lone
			// name (a rename-style replace breaks the alias; a write does not).
			for _, other := range b.files {
				if other == n {
					nlink++
				}
			}
			if n.ResourceID == "" {
				// The real server reports the object's stable identity on
				// every stat; the lazy assignment keeps the fake's seeds
				// (which predate object identity) from colliding.
				b.nextObject++
				n.ResourceID = "res-" + strconv.FormatInt(b.nextObject, 10)
			}
			resourceID = n.ResourceID
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		w.Header().Set("X-Dat9-IsDir", strconv.FormatBool(isDir))
		w.Header().Set("Content-Length", strconv.Itoa(dataLen))
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
		w.Header().Set("X-Dat9-Resource-ID", resourceID)
		if nlink > 1 {
			w.Header().Set("X-Dat9-Nlink", strconv.Itoa(nlink))
		}
		w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(mode), 10))
		w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(mtime, 10))
		// Mirror the real server's placement decision: objects at or
		// above the advertised inline threshold live in object storage.
		storageType := "db9"
		if int64(dataLen) >= 1<<20 {
			storageType = "s3"
		}
		w.Header().Set("X-Dat9-Storage-Type", storageType)
		if contentLayout != "" {
			w.Header().Set("X-Dat9-Content-Layout", contentLayout)
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
				out = append(out, listEntry{Name: name, Size: int64(len(child.Data)), IsDir: isDir, Revision: child.Rev, Mtime: child.Mtime, Mode: mode, HasMode: true})
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
		// Snapshot the slice header under the lock: PUTs replace the
		// field (never mutating the old backing array), so serving the
		// captured bytes after unlocking is race-free while reading the
		// field unlocked was not.
		b.mu.Lock()
		n := b.files[p]
		var data []byte
		if n != nil {
			data = n.Data
		}
		b.mu.Unlock()
		if n == nil || n.IsDir {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			lo, hi, ok := parseRange(rng, int64(len(data)))
			if !ok {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi-1, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[lo:hi])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)

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
		expRev, _ := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
		b.mu.Lock()
		if b.failDelete[p] > 0 {
			b.failDelete[p]--
			b.mu.Unlock()
			b.writeErr(w, http.StatusServiceUnavailable, "delete unavailable")
			return
		}
		n := b.files[p]
		if n == nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if q.Get("kind") == "file" && n.IsDir {
			b.mu.Unlock()
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if expRev > 0 && n.Rev != expRev {
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "revision conflict")
			return
		}
		delete(b.files, p)
		b.mu.Unlock()
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
			if n.IsDir {
				// The real server moves a directory's whole subtree; the
				// harness must too, or a rename test would 404 every child.
				prefix := strings.TrimSuffix(src, "/") + "/"
				moved := make(map[string]*Node)
				for k, v := range b.files {
					if strings.HasPrefix(k, prefix) {
						moved[strings.TrimSuffix(p, "/")+"/"+strings.TrimPrefix(k, prefix)] = v
						delete(b.files, k)
					}
				}
				for k, v := range moved {
					b.files[k] = v
				}
			}
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

	case q.Has("setmeta"):
		var body struct {
			Tags        *map[string]string `json:"tags"`
			Description *string            `json:"description"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		b.mu.Lock()
		n := b.files[p]
		if n == nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if body.Tags != nil {
			if len(*body.Tags) == 0 {
				n.Tags = nil
			} else {
				n.Tags = map[string]string{}
				for k, v := range *body.Tags {
					n.Tags[k] = v
				}
			}
		}
		b.mu.Unlock()
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

// cloneTags copies a tag map (nil stays nil).
func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}
