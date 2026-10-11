// The handler: view transformation for workspace paths. Every op inside
// a registered workspace root resolves through three layers — the dirty
// mirror, the clean tree, then the classic namespace — by rewriting the
// op's path into the store and dispatching next, or by landing mirror
// mutations as plain store writes.
package git

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitws"
)

// Resolution layers, strongest opinion first.
const (
	layerClassic = iota // untracked path: the classic namespace via next
	layerTree           // tracked clean path: tree/<commit>/<rel> via next
	layerOverlay        // mirrored path: overlay/<rel> via next
	layerHidden         // blackout: the path is deleted in this view
)

// resolution is the routing decision for one workspace-relative path.
type resolution struct {
	layer     int
	storePath string // valid for layerTree and layerOverlay
	node      overlayNode
	ent       gitws.TreeEntry
	hasEnt    bool
}

func (r resolution) tracked() bool { return r.hasEnt }

// kindOfEntry maps a tree-index kind string onto the fs vocabulary.
func kindOfEntry(kind string) fs.Kind {
	switch kind {
	case "dir":
		return fs.KindDir
	case "symlink":
		return fs.KindSymlink
	default:
		return fs.KindFile
	}
}

// parseMode decodes an octal mode string ("100644") to its permission bits.
func parseMode(m string) uint32 {
	var mode uint32
	for _, c := range m {
		if c < '0' || c > '7' {
			return 0
		}
		mode = mode<<3 | uint32(c-'0')
	}
	return mode & 0o777
}

// attr assembles the view attribute for a resolved overlay or tree node.
func (r resolution) attr() fs.Attr {
	if r.layer == layerOverlay {
		mode := r.node.mode
		if mode == 0 {
			mode = 0o644
			switch r.node.kind {
			case fs.KindDir:
				mode = 0o755
			case fs.KindSymlink:
				mode = 0o777
			}
		}
		return fs.Attr{Kind: r.node.kind, Mode: mode, Size: r.node.size}
	}
	kind := kindOfEntry(r.ent.Kind)
	mode := parseMode(r.ent.Mode)
	if mode == 0 {
		mode = 0o644
		switch kind {
		case fs.KindDir:
			mode = 0o755
		case fs.KindSymlink:
			mode = 0o777
		}
	}
	return fs.Attr{Kind: kind, Mode: mode, Size: r.ent.Size}
}

// resolveIn routes one workspace-relative path through the mirror, the
// tracked index, and finally the classic namespace. The tracked entry
// rides along on every layer: a mirrored or blacked-out path still needs
// its tracked-ness to decide blackout placement and O_CREAT revival.
func (g *Git) resolveIn(w *workspace, rel string) resolution {
	var ent gitws.TreeEntry
	hasEnt := false
	if e, ok := w.isTracked(rel); ok {
		ent, hasEnt = e, true
	}
	if node, hit := w.lookup(rel); hit {
		if node.blackout {
			return resolution{layer: layerHidden, ent: ent, hasEnt: hasEnt}
		}
		return resolution{layer: layerOverlay, storePath: gitws.OverlayPath(w.id, rel), node: node, ent: ent, hasEnt: hasEnt}
	}
	if hasEnt {
		return resolution{layer: layerTree, storePath: gitws.TreePath(w.id, w.head(), rel), ent: ent, hasEnt: true}
	}
	return resolution{layer: layerClassic}
}

func (g *Git) emit(path string) {
	if g.bus != nil {
		g.bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: path})
	}
}

// isStoreNamespace keeps the /.drive9/git-workspaces store itself a
// pass-through: control-plane writes (CLI registration, hydrate uploads)
// must never be re-routed by the view they feed.
func isStoreNamespace(path string) bool {
	return path == gitws.Root || strings.HasPrefix(path, gitws.Root+"/")
}

// observeGitMetaMutation debounces a .git state checkpoint when git's own
// file traffic flows through (git sits outside localize, so it observes
// the mutations before the local overlay absorbs them).
func (g *Git) observeGitMetaMutation(op *fs.Op) {
	switch op.Kind {
	case fs.OpWrite, fs.OpCreate, fs.OpUnlink, fs.OpRmdir, fs.OpRename, fs.OpSetAttr, fs.OpMkdir, fs.OpSymlink:
		g.scheduleGitStateCheckpoint(op.Path)
	}
}

func (g *Git) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if isStoreNamespace(op.Path) {
		return next(ctx, op)
	}
	g.discover(ctx)
	w, rel, ok := g.workspaceFor(op.Path)
	if !ok {
		return next(ctx, op)
	}
	if rel == ".git" || strings.HasPrefix(rel, ".git/") {
		// .git of a registered workspace is routing-local (localize via
		// IsDeclaredLocalPath); its mutations only feed the checkpoint
		// machine.
		g.observeGitMetaMutation(op)
		return next(ctx, op)
	}
	if rel == "" {
		return g.doWorkspaceRoot(ctx, op, w, next)
	}

	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		res := g.resolveIn(w, rel)
		switch res.layer {
		case layerHidden:
			return fs.Result{}, fs.ErrNotFound
		case layerOverlay, layerTree:
			attr := res.attr()
			return fs.Result{Attr: &attr}, nil
		}
		return next(ctx, op)

	case fs.OpReadlink:
		res := g.resolveIn(w, rel)
		switch res.layer {
		case layerHidden:
			return fs.Result{}, fs.ErrNotFound
		case layerOverlay:
			if res.node.kind == fs.KindSymlink {
				if res.node.target != "" {
					return fs.Result{Data: []byte(res.node.target)}, nil
				}
				store := g.storeView()
				if store == nil {
					return fs.Result{}, fs.ErrUnsupported
				}
				target, err := store.Readlink(ctx, res.storePath)
				if err != nil {
					return fs.Result{}, err
				}
				res.node.target = target
				w.set(rel, res.node)
				return fs.Result{Data: []byte(target)}, nil
			}
		case layerTree:
			if kindOfEntry(res.ent.Kind) == fs.KindSymlink {
				store := g.storeView()
				if store == nil {
					return fs.Result{}, fs.ErrUnsupported
				}
				target, err := store.Readlink(ctx, res.storePath)
				if err != nil {
					return fs.Result{}, err
				}
				return fs.Result{Data: []byte(target)}, nil
			}
		}
		return next(ctx, op)

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		res := g.resolveIn(w, rel)
		switch res.layer {
		case layerHidden:
			return fs.Result{}, fs.ErrNotFound
		case layerOverlay, layerTree:
			return next(ctx, &fs.Op{Kind: fs.OpRead, Path: res.storePath, Offset: op.Offset, Limit: op.Limit})
		}
		return next(ctx, op)

	case fs.OpList:
		return g.listMerged(ctx, w, op.Path, rel, next)

	case fs.OpWrite:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		res := g.resolveIn(w, rel)
		if res.layer == layerClassic {
			return next(ctx, op)
		}
		// Mirror write: merge at op.Offset over the current image — the
		// kernel delivers large writes as many 128KB chunks, and each
		// chunk must land over the full image, not replace it.
		off := op.Offset
		if off < 0 {
			off = 0
		}
		img := mergeAt(g.currentImage(ctx, w, rel, res), off, op.Data)
		mode := res.attr().Mode
		if mode == 0 {
			mode = 0o644
		}
		if err := g.landOverlayFile(ctx, w, rel, img, mode); err != nil {
			return fs.Result{}, err
		}
		g.emit(op.Path)
		return fs.Result{Written: uint32(len(op.Data))}, nil

	case fs.OpCreate:
		res := g.resolveIn(w, rel)
		if hasWriteIntent(op.Flags) {
			switch {
			case res.layer == layerOverlay && res.node.kind == fs.KindDir,
				res.layer == layerTree && kindOfEntry(res.ent.Kind) == fs.KindDir:
				return fs.Result{}, syscall.EISDIR
			case res.layer == layerOverlay || res.layer == layerTree,
				res.layer == layerHidden && res.tracked():
				h, cerr := g.claimWorkspaceOpen(w, op.Path, rel, op.Flags, op.Mode)
				if cerr != nil {
					return fs.Result{}, cerr
				}
				if op.Flags&uint32(syscall.O_CREAT) != 0 && res.layer != layerOverlay {
					h.mu.Lock()
					h.dirty = true
					h.mu.Unlock()
				}
				g.trackWorkspace(op.Path, h)
				attr := h.Attr()
				return fs.Result{Handle: h, Attr: &attr}, nil
			}
			return next(ctx, op)
		}
		switch res.layer {
		case layerHidden:
			return fs.Result{}, fs.ErrNotFound
		case layerOverlay, layerTree:
			// Plain open: rewrite into the store so the data plane (and
			// its read caches) serves the mirrored/tree content.
			op.Path = res.storePath
			return next(ctx, op)
		}
		return next(ctx, op)

	case fs.OpMkdir:
		res := g.resolveIn(w, rel)
		switch res.layer {
		case layerOverlay, layerTree:
			return fs.Result{}, syscall.EEXIST
		case layerHidden:
			if res.tracked() {
				mode := op.Mode & 0o777
				if mode == 0 {
					mode = 0o755
				}
				if err := g.landOverlayDir(ctx, w, rel, mode); err != nil {
					return fs.Result{}, err
				}
				g.emit(op.Path)
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode}}, nil
			}
		}
		return next(ctx, op)

	case fs.OpSymlink:
		res := g.resolveIn(w, rel)
		switch {
		case res.layer == layerOverlay || res.layer == layerTree:
			return fs.Result{}, syscall.EEXIST
		case res.layer == layerHidden && res.tracked():
			if err := g.landOverlaySymlink(ctx, w, rel, op.Target); err != nil {
				return fs.Result{}, err
			}
			g.emit(op.Path)
			return fs.Result{}, nil
		}
		return next(ctx, op)

	case fs.OpUnlink, fs.OpRmdir:
		// POSIX unlink-while-open: the delete hides the name now; an open
		// claimed handle keeps its image and never publishes.
		if h := g.handleFor(op.Path); h != nil && op.Kind == fs.OpUnlink {
			h.markUnlinked()
		}
		res := g.resolveIn(w, rel)
		switch res.layer {
		case layerHidden:
			return fs.Result{}, nil
		case layerOverlay:
			if err := g.removeOverlayNode(ctx, w, rel, res.node); err != nil {
				return fs.Result{}, err
			}
			if res.tracked() {
				if err := g.setBlackout(ctx, w, rel); err != nil {
					return fs.Result{}, err
				}
			} else {
				w.drop(rel)
			}
			g.emit(op.Path)
			return fs.Result{}, nil
		case layerTree:
			if err := g.setBlackout(ctx, w, rel); err != nil {
				return fs.Result{}, err
			}
			g.emit(op.Path)
			return fs.Result{}, nil
		}
		return next(ctx, op)

	case fs.OpRename:
		if err := g.renameView(ctx, op, w, rel, next); err != nil {
			return fs.Result{}, err
		}
		if h := g.handleFor(op.Path); h != nil {
			if w2, rel2, ok := g.workspaceFor(op.Path2); ok && w2 == w {
				h.retarget(op.Path2, rel2)
			}
		}
		g.emit(op.Path)
		g.emit(op.Path2)
		return fs.Result{}, nil

	case fs.OpSetAttr:
		if h := g.handleFor(op.Path); h != nil {
			if op.Mode != 0 {
				h.SetMode(op.Mode)
			}
			if op.Size >= 0 {
				if err := h.Truncate(op.Size); err != nil {
					return fs.Result{}, err
				}
			}
			attr := h.Attr()
			return fs.Result{Attr: &attr}, nil
		}
		res := g.resolveIn(w, rel)
		if res.layer == layerOverlay {
			userPath := op.Path
			op.Path = res.storePath
			res2, err := next(ctx, op)
			if err != nil {
				return res2, err
			}
			node := res.node
			if op.Mode != 0 {
				node.mode = op.Mode & 0o777
			}
			if op.Size >= 0 {
				node.size = op.Size
			}
			w.set(rel, node)
			g.emit(userPath)
			return res2, nil
		}
		return next(ctx, op)
	}
	return next(ctx, op)
}

// doWorkspaceRoot serves ops on the workspace root itself: the root is
// always a directory while the record exists; its listing is the merged
// view; everything else falls through to the classic namespace.
func (g *Git) doWorkspaceRoot(ctx context.Context, op *fs.Op, w *workspace, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		attr := fs.Attr{Kind: fs.KindDir, Mode: 0o755}
		return fs.Result{Attr: &attr}, nil
	case fs.OpList:
		return g.listMerged(ctx, w, op.Path, "", next)
	}
	return next(ctx, op)
}

// currentImage reads the effective content a mirror write merges over.
func (g *Git) currentImage(ctx context.Context, w *workspace, rel string, res resolution) []byte {
	store := g.storeView()
	if store == nil {
		return nil
	}
	switch res.layer {
	case layerOverlay, layerTree:
		data, err := store.Read(ctx, res.storePath, 0, 0)
		if err != nil {
			return nil // not hydrated yet: nothing below the write
		}
		return data
	}
	return nil
}

// renameView moves content within one workspace. Mirrored sources move
// through the store (a server-side rename for mirror-to-mirror); tracked
// clean sources copy once into the mirror and black out the origin;
// untracked sources stay in the classic namespace unless the destination
// is shadowed, in which case they copy into the mirror.
func (g *Git) renameView(ctx context.Context, op *fs.Op, w *workspace, rel string, next fs.Next) error {
	w2, rel2, ok := g.workspaceFor(op.Path2)
	if !ok || w2 != w {
		res := g.resolveIn(w, rel)
		if res.layer == layerClassic || res.layer == layerHidden && !res.tracked() {
			_, err := next(ctx, op)
			return err
		}
		return fmt.Errorf("git: rename crosses workspace roots")
	}
	resOld := g.resolveIn(w, rel)
	resNew := g.resolveIn(w, rel2)
	store := g.storeView()
	if store == nil {
		return fs.ErrUnsupported
	}

	switch resOld.layer {
	case layerHidden:
		return fs.ErrNotFound

	case layerOverlay:
		if err := store.EnsureParent(ctx, storeDirOf(gitws.OverlayPath(w.id, rel2))); err != nil {
			return err
		}
		if err := store.Rename(ctx, gitws.OverlayPath(w.id, rel), gitws.OverlayPath(w.id, rel2), fs.RenameOptions{}); err != nil {
			return err
		}
		if err := g.clearBlackout(ctx, w, rel2); err != nil {
			return err
		}
		node := resOld.node
		w.drop(rel)
		w.set(rel2, node)
		if resOld.tracked() {
			return g.setBlackout(ctx, w, rel)
		}
		return nil

	case layerTree:
		if kindOfEntry(resOld.ent.Kind) == fs.KindDir {
			return fmt.Errorf("git: directory rename of clean tree content is not supported")
		}
		data, err := store.Read(ctx, resOld.storePath, 0, 0)
		if err != nil && !fs.IsNotFound(err) {
			return err
		}
		mode := resOld.attr().Mode
		if err := g.landOverlayFile(ctx, w, rel2, data, mode); err != nil {
			return err
		}
		return g.setBlackout(ctx, w, rel)

	default: // classic source
		if resNew.layer == layerClassic || resNew.layer == layerHidden && !resNew.tracked() {
			_, err := next(ctx, op)
			return err
		}
		// Destination is shadowed by the mirror/tree view: copy the
		// classic content in, then remove the classic source.
		readRes, err := next(ctx, &fs.Op{Kind: fs.OpRead, Path: op.Path})
		if err != nil && !fs.IsNotFound(err) {
			return err
		}
		var mode uint32 = 0o644
		if resNew.layer == layerOverlay || resNew.layer == layerTree {
			mode = resNew.attr().Mode
		}
		if err := g.landOverlayFile(ctx, w, rel2, readRes.Data, mode); err != nil {
			return err
		}
		if _, err := next(ctx, &fs.Op{Kind: fs.OpUnlink, Path: op.Path}); err != nil && !fs.IsNotFound(err) {
			return err
		}
		return nil
	}
}

// listMerged merges the classic listing, the clean tree listing, and the
// dirty mirror (upserts add/replace, blackouts hide).
func (g *Git) listMerged(ctx context.Context, w *workspace, dir, rel string, next fs.Next) (fs.Result, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
	if err != nil && !fs.IsNotFound(err) {
		return res, err
	}
	out := map[string]fs.Entry{}
	for _, e := range res.Entries {
		out[e.Name] = e
	}
	store := g.storeView()
	if store != nil {
		trackedDir := rel == ""
		if rel != "" {
			_, trackedDir = w.isTracked(rel)
		}
		if trackedDir {
			treeEntries, terr := store.List(ctx, gitws.TreePath(w.id, w.head(), rel))
			if terr != nil && !fs.IsNotFound(terr) {
				return fs.Result{}, terr
			}
			for _, e := range treeEntries {
				out[e.Name] = e
			}
		}
	}
	prefix := ""
	if rel != "" {
		prefix = rel + "/"
	}
	for rel2, node := range w.snapshotOverlay() {
		if prefix != "" && !strings.HasPrefix(rel2, prefix) {
			continue
		}
		name := strings.TrimPrefix(rel2, prefix)
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		if node.blackout {
			delete(out, name)
			continue
		}
		mode := node.mode
		if mode == 0 {
			mode = 0o644
			if node.kind == fs.KindDir {
				mode = 0o755
			}
		}
		out[name] = fs.Entry{Name: name, Attr: fs.Attr{Kind: node.kind, Mode: mode, Size: node.size}}
	}
	entries := make([]fs.Entry, 0, len(out))
	for _, e := range out {
		entries = append(entries, e)
	}
	return fs.Result{Entries: entries}, nil
}

// storeDirOf returns the slash-parent of a store path.
func storeDirOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		return p[:i]
	}
	return "/"
}

// mergeAt overlays data at off over old, zero-filling any gap (the buffer
// merge the write path performed before publishing one image).
func mergeAt(old []byte, off int64, data []byte) []byte {
	end := off + int64(len(data))
	if end <= int64(len(old)) {
		out := append([]byte(nil), old...)
		copy(out[off:], data)
		return out
	}
	out := make([]byte, end)
	copy(out, old)
	copy(out[off:], data)
	return out
}
