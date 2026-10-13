// The shadow view: which layer (or checkpoint snapshot, or none) the mount
// presents, its marker roots, and the in-memory routing index built by
// walking them.
package layer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// viewMode classifies the active shadow.
type viewMode uint8

const (
	viewLive viewMode = iota
	viewLayer
	viewCheckpoint
)

func (m viewMode) String() string {
	switch m {
	case viewLayer:
		return "layer"
	case viewCheckpoint:
		return "checkpoint"
	default:
		return "live"
	}
}

// view is one shadow configuration; checkout swaps the pointer.
type view struct {
	mode       viewMode
	layerID    string
	checkpoint string
	readOnly   bool
	// roots are server-absolute marker prefixes ("" in live mode).
	treeRoot     string
	blackRoot    string
	blackDirRoot string
	casRoot      string
	attrsRoot    string
	// abandoned fails later mutations after a rollback event. Atomic:
	// the rollback poll sets it while request goroutines gate on it
	// through unsynchronized view references.
	abandoned atomic.Bool
	// lastSeq is the event-log cursor for the rollback poll loop.
	lastSeq int64

	idx *viewIndex
}

func newLiveView() *view {
	return &view{mode: viewLive, idx: newViewIndex()}
}

func layerRootPath(layerID string) string {
	return layersNamespace + "/" + layerID
}

// treePathOf maps a server-absolute path to its shadow file under the
// active tree root ("/" maps to the root itself, without a trailing
// slash).
func (v *view) treePathOf(serverAbsPath string) string {
	if serverAbsPath == "/" {
		return v.treeRoot
	}
	return v.treeRoot + serverAbsPath
}

// entryKind classifies an indexed path. Tree entries carry no file/symlink
// distinction: the real attribute flows from the chain on every rewrite.
type entryKind uint8

const (
	ekTree entryKind = iota
	ekBlackout
	ekAttrs
)

// blackoutInfo decodes a blackout marker. File and directory whiteouts
// live in separate roots (blackouts/ and blackout-dirs/ — one path cannot
// be both a marker file and a marker directory), so the marker body
// carries only the deleted base's revision; this codec matches
// pkg/layerfs exactly.
type blackoutInfo struct {
	isDir   bool
	baseRev int64
	hasBase bool
}

func encodeBlackout(info blackoutInfo) []byte {
	if info.hasBase {
		return []byte(strconv.FormatInt(info.baseRev, 10))
	}
	return []byte("-")
}

func decodeBlackout(content []byte) blackoutInfo {
	text := strings.TrimSpace(string(content))
	info := blackoutInfo{}
	if text != "" && text != "-" {
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			info.baseRev = n
			info.hasBase = true
		}
	}
	return info
}

// attrsPatch decodes an attrs/<path> marker. ModeSet distinguishes
// chmod(000) — mode zero with ModeSet true — from an absent mode field.
type attrsPatch struct {
	Mode    uint32 `json:"mode"`
	ModeSet bool   `json:"mode_set,omitempty"`
	Mtime   int64  `json:"mtime,omitempty"` // unix seconds; 0 = unchanged
}

func decodeAttrs(content []byte, out *attrsPatch) error {
	return json.Unmarshal(content, out)
}

type indexEntry struct {
	kind      entryKind
	isDir     bool   // tree entries: directory classification from the walk
	isSymlink bool   // tree entries: symlink classification from the walk
	mode      uint32 // tree: permission bits; attrs: the patched mode
	blackout  blackoutInfo
	patch     attrsPatch
}

// viewIndex routes view paths: blackout (hide), tree (rewrite), attrs
// (patch), or classic (pass through).
type viewIndex struct {
	mu      sync.RWMutex
	entries map[string]*indexEntry // view path -> entry
}

func newViewIndex() *viewIndex {
	return &viewIndex{entries: map[string]*indexEntry{}}
}

func (ix *viewIndex) get(p string) (*indexEntry, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	e, ok := ix.entries[p]
	return e, ok
}

func (ix *viewIndex) put(p string, e *indexEntry) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.entries[p] = e
}

func (ix *viewIndex) remove(p string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	delete(ix.entries, p)
}

// removeIfKind drops an entry only while it still has the given kind (a
// stale blackout at a path that now holds moved tree content must not
// clear it).
func (ix *viewIndex) removeIfKind(p string, kind entryKind) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if e, ok := ix.entries[p]; ok && e.kind == kind {
		delete(ix.entries, p)
	}
}

// remapPrefix moves every entry below oldPrefix to newPrefix and returns
// the moved view paths.
func (ix *viewIndex) remapPrefix(oldPrefix, newPrefix string) []string {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var moved []string
	for p, e := range ix.entries {
		switch {
		case p == oldPrefix:
			delete(ix.entries, p)
			ix.entries[newPrefix] = e
			moved = append(moved, newPrefix)
		case strings.HasPrefix(p, oldPrefix+"/"):
			np := newPrefix + p[len(oldPrefix):]
			delete(ix.entries, p)
			ix.entries[np] = e
			moved = append(moved, np)
		}
	}
	return moved
}

// childrenOf lists the direct children of dir known to the index (any kind).
func (ix *viewIndex) childrenOf(dir string) map[string]*indexEntry {
	prefix := "/"
	if dir != "/" {
		prefix = strings.TrimRight(dir, "/") + "/"
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := map[string]*indexEntry{}
	for p, e := range ix.entries {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		name := p[len(prefix):]
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		out[name] = e
	}
	return out
}

// isBlackedOut reports whether p itself or any ancestor dir is blacked out.
func (ix *viewIndex) isBlackedOut(p string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.blackedOutLocked(p)
}

func (ix *viewIndex) blackedOutLocked(p string) bool {
	if e, ok := ix.entries[p]; ok && e.kind == ekBlackout {
		return true
	}
	for {
		parent := strings.TrimSuffix(pathutil.ParentPath(p), "/")
		if parent == "" || parent == p {
			return false
		}
		if e, ok := ix.entries[parent]; ok && e.kind == ekBlackout && e.blackout.isDir {
			return true
		}
		p = parent
	}
}

// pathMapper converts between view paths and server-absolute paths.
type pathMapper struct {
	serverToView func(string) (string, bool)
	viewToServer func(string) string
}

// buildViewIndex walks the marker roots of v (through the data-plane
// seam) and materializes the routing index.
func buildViewIndex(ctx context.Context, dio layerIO, v *view, pm pathMapper) (*viewIndex, error) {
	ix := newViewIndex()
	if v.mode == viewLive {
		return ix, nil
	}
	walkRoot := func(root string, apply func(viewPath string, info ioEntry)) error {
		if root == "" {
			return nil
		}
		return walkRemote(ctx, dio, root, func(serverPath string, info ioEntry) {
			// serverPath is absolute under the marker root; strip it
			// before mapping through the remote root.
			rel := strings.TrimPrefix(serverPath, root)
			vp, ok := pm.serverToView(rel)
			if !ok || vp == "/" || vp == "" {
				return
			}
			apply(vp, info)
		})
	}
	if err := walkRoot(v.treeRoot, func(vp string, info ioEntry) {
		ix.put(vp, &indexEntry{kind: ekTree, isDir: info.IsDir, isSymlink: info.IsSymlink, mode: info.Mode & 0o777})
	}); err != nil {
		return nil, err
	}
	// A listed marker whose read fails must fail the walk closed:
	// treating it as absent would misroute the view (and mis-anchor
	// commits). A vanished marker (lost delete race) is benign.
	var readErr error
	readMarker := func(root, vp string) ([]byte, bool) {
		content, err := dio.Read(ctx, root+pm.viewToServer(vp))
		if err != nil {
			if !fs.IsNotFound(err) {
				readErr = err
			}
			return nil, false
		}
		return content, true
	}
	if err := walkRoot(v.attrsRoot, func(vp string, info ioEntry) {
		if info.IsDir {
			return
		}
		if content, ok := readMarker(v.attrsRoot, vp); ok {
			var patch attrsPatch
			if err := decodeAttrs(content, &patch); err == nil {
				ix.put(vp, &indexEntry{kind: ekAttrs, patch: patch})
			}
		}
	}); err != nil {
		return nil, err
	}
	seed := func(info blackoutInfo) *indexEntry {
		return &indexEntry{kind: ekBlackout, blackout: info}
	}
	if err := walkRoot(v.blackRoot, func(vp string, info ioEntry) {
		if info.IsDir {
			return
		}
		if content, ok := readMarker(v.blackRoot, vp); ok {
			ix.put(vp, seed(decodeBlackout(content)))
		}
	}); err != nil {
		return nil, err
	}
	if err := walkRoot(v.blackDirRoot, func(vp string, info ioEntry) {
		if info.IsDir {
			return
		}
		if content, ok := readMarker(v.blackDirRoot, vp); ok {
			bl := decodeBlackout(content)
			bl.isDir = true
			ix.put(vp, seed(bl))
		}
	}); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, fmt.Errorf("read layer marker: %w", readErr)
	}
	return ix, nil
}

// walkRemote lists a directory tree recursively through the data-plane
// seam, invoking fn for every entry below root (absolute paths).
func walkRemote(ctx context.Context, dio layerIO, root string, fn func(serverPath string, info ioEntry)) error {
	var walk func(dir string) error
	walk = func(dir string) error {
		infos, err := dio.List(ctx, dir)
		if err != nil {
			if fs.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("list %s: %w", dir, err)
		}
		for _, info := range infos {
			child := pathutil.JoinPath(dir, info.Name)
			fn(child, info)
			if info.IsDir {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(root)
}
