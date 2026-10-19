// The handler: op routing against each workspace's overlay, overlay
// persistence, rename, and the merged directory listing.
package git

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func kindOf(e overlayEntry) fs.Kind {
	switch e.Kind {
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

func (g *Git) emit(path string) {
	if g.bus != nil {
		g.bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: path})
	}
}

func (g *Git) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	g.ensure(ctx)
	w, rel, ok := g.workspaceFor(op.Path)
	if !ok {
		// A workspace registered after this mount started (git clone
		// --fast runs the CLI registration server-side) is invisible to
		// the start-time list; re-list on the miss, throttled. Dormant
		// mounts arm on .git-segment ops only (v1): ordinary FS traffic
		// on a workspace-free root never lists.
		g.mu.RLock()
		armed := g.armed
		g.mu.RUnlock()
		if pathHasGitDirSegment(op.Path) && !armed {
			// A .git-segment op may arm the dormant plane (v1 G2), but
			// only when an arm source actually suggests workspaces exist:
			// local markers, or root-relevant index entries. G0's dormant
			// .git activity on a workspace-free root stays at zero lists.
			g.tryArmFromLocalSignals()
			g.maybeArmFromIndex(ctx)
			g.mu.RLock()
			armed = g.armed
			g.mu.RUnlock()
		}
		if armed {
			g.reloadIfStale(ctx)
		}
		w, rel, ok = g.workspaceFor(op.Path)
		if !ok {
			return next(ctx, op)
		}
	}

	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		e, hit := w.lookup(rel)
		if !hit {
			// Fast-clone view (v1 gitEntry): clean files resolve from the
			// HEAD tree before the base view.
			if n, tok := w.treeLookup(rel); tok {
				if n.Kind == "dir" || n.Kind == "symlink" || n.ObjectSHA != "" {
					attr := treeAttr(n, w.cleanTreeMtime())
					// An unknown size resolves once via cat-file and is
					// remembered on the node: re-resolving per stat let a
					// single failed exec flap the served size and pin git
					// status entries as modified.
					if n.SizeBytes < 0 && (n.Kind == "file" || n.Kind == "symlink") && n.ObjectSHA != "" {
						attr.Size = g.blobSize(w, n.ObjectSHA)
						w.noteTreeSize(rel, attr.Size)
					}
					return fs.Result{Attr: &attr}, nil
				}
			}
			return next(ctx, op)
		}
		if e.Op == "whiteout" {
			return fs.Result{}, fs.ErrNotFound
		}
		kind := kindOf(e)
		mode := parseMode(e.Mode)
		if mode == 0 {
			mode = 0o644
			switch kind {
			case fs.KindDir:
				mode = 0o755
			case fs.KindSymlink:
				mode = 0o777
			}
		}
		size := int64(len(e.Content))
		if e.remote {
			size = e.size
		}
		return fs.Result{Attr: &fs.Attr{Kind: kind, Mode: mode, Size: size}}, nil

	case fs.OpReadlink:
		if e, hit := w.lookup(rel); hit {
			if e.Op == "whiteout" {
				return fs.Result{}, fs.ErrNotFound
			}
			if e.Kind == "symlink" {
				return fs.Result{Data: e.Content}, nil
			}
		} else if n, tok := w.treeLookup(rel); tok && n.Kind == "symlink" {
			if data, err := g.materializeBlob(w, n.ObjectSHA); err == nil {
				return fs.Result{Data: data}, nil
			}
		}
		return next(ctx, op)

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		if e, hit := w.lookup(rel); hit {
			if e.Op == "whiteout" {
				return fs.Result{}, fs.ErrNotFound
			}
			if e.remote {
				// Storage-backed overlay payload: the base view serves it
				// until a local write mirrors inline content.
				return next(ctx, op)
			}
			data := e.Content
			if op.Offset > 0 || op.Limit > 0 {
				off := op.Offset
				if off < 0 {
					off = 0
				}
				if off >= int64(len(data)) {
					return fs.Result{}, nil
				}
				end := int64(len(data))
				if op.Limit > 0 && off+op.Limit < end {
					end = off + op.Limit
				}
				data = data[off:end]
			}
			return fs.Result{Data: data}, nil
		}
		// Fast-clone view: materialize the blob from the restored local
		// .git (v1 materializeGitBlob).
		if n, tok := w.treeLookup(rel); tok && n.Kind == "file" && n.ObjectSHA != "" {
			if data, err := g.materializeBlob(w, n.ObjectSHA); err == nil {
				if op.Offset > 0 || op.Limit > 0 {
					off := op.Offset
					if off < 0 {
						off = 0
					}
					if off >= int64(len(data)) {
						return fs.Result{}, nil
					}
					end := int64(len(data))
					if op.Limit > 0 && off+op.Limit < end {
						end = off + op.Limit
					}
					data = data[off:end]
				}
				return fs.Result{Data: data}, nil
			}
		}
		return next(ctx, op)

	case fs.OpList:
		return g.listMerged(ctx, w, op.Path, next)

	case fs.OpWrite:
		// Merge at op.Offset over the current image: the kernel delivers
		// large writes as many 128KB chunks — publishing each chunk as the
		// whole overlay content would keep only the last one.
		img := op.Data
		off := op.Offset
		if off < 0 {
			off = 0
		}
		if off != 0 || op.Handle != nil || op.Fh != 0 {
			if e, hit := w.lookup(rel); hit && e.Op == "upsert" && e.Kind == "file" {
				img = mergeAt(e.Content, off, op.Data)
			} else if off > 0 {
				img = mergeAt(nil, off, op.Data)
			}
		}
		mode := "100644"
		if e, hit := w.lookup(rel); hit && e.Op == "upsert" && e.Mode != "" {
			mode = e.Mode
		}
		if err := g.putOverlay(ctx, w, rel, overlayEntry{Op: "upsert", Kind: "file", Content: img, Mode: mode}); err != nil {
			return fs.Result{}, err
		}
		g.emit(op.Path)
		return fs.Result{Written: uint32(len(op.Data))}, nil

	case fs.OpCreate:
		if hasWriteIntent(op.Flags) {
			e, hit := w.lookup(rel)
			// A whiteout is a miss for a create: unlink+recreate (git's
			// checkout_entry) must land the new file in the overlay —
			// falling through to the base plane would create it remotely
			// with a default 0644 mode under a still-active whiteout.
			whiteoutCreate := hit && e.Op == "whiteout" && op.Flags&uint32(syscall.O_CREAT) != 0
			if !hit || (e.Op == "upsert" && e.Kind == "file") || whiteoutCreate {
				h, cerr := g.claimWorkspaceOpen(w, op.Path, rel, op.Flags, op.Mode)
				if cerr != nil {
					return fs.Result{}, cerr
				}
				if op.Flags&uint32(syscall.O_CREAT) != 0 && (!hit || whiteoutCreate) {
					h.mu.Lock()
					h.dirty = true
					h.mu.Unlock()
				}
				g.trackWorkspace(op.Path, h)
				attr := h.Attr()
				return fs.Result{Handle: h, Attr: &attr}, nil
			}
		}
		if op.Flags&uint32(syscall.O_CREAT) == 0 {
			// plain open: overlay files report their overlay attr
			if e, hit := w.lookup(rel); hit {
				if e.Op == "whiteout" {
					return fs.Result{}, fs.ErrNotFound
				}
				mode := parseMode(e.Mode)
				if mode == 0 {
					mode = 0o644
				}
				mtime := e.mtime
				if mtime.IsZero() {
					mtime = w.cleanTreeMtime()
				}
				return fs.Result{Attr: &fs.Attr{Kind: kindOf(e), Mode: mode, Size: int64(len(e.Content)), Mtime: mtime}}, nil
			}
			// A clean tree file has no base-plane entry: a read-only
			// open that fell through would wrap a 404 remote handle and
			// every read EIOs. Claim it and serve the materialized blob.
			if n, tok := w.treeLookup(rel); tok && n.Kind == "file" && n.ObjectSHA != "" {
				h := &gitHandle{g: g, w: w, path: op.Path, rel: rel, flags: op.Flags,
					treeSHA: n.ObjectSHA, mode: parseMode(n.Mode)}
				if h.mode == 0 {
					h.mode = 0o644
				}
				g.trackWorkspace(op.Path, h)
				attr := h.Attr()
				return fs.Result{Handle: h, Attr: &attr}, nil
			}
			return next(ctx, op)
		}
		if err := g.putOverlay(ctx, w, rel, overlayEntry{Op: "upsert", Kind: "file", Content: nil, Mode: "100644"}); err != nil {
			return fs.Result{}, err
		}
		g.emit(op.Path)
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: op.Mode & 0o777}}, nil

	case fs.OpMkdir:
		mode := "40755"
		if err := g.putOverlay(ctx, w, rel, overlayEntry{Op: "upsert", Kind: "dir", Mode: mode}); err != nil {
			return fs.Result{}, err
		}
		g.emit(op.Path)
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0o755}}, nil

	case fs.OpSymlink:
		if err := g.putOverlay(ctx, w, rel, overlayEntry{Op: "upsert", Kind: "symlink", Content: []byte(op.Target), Mode: "120777"}); err != nil {
			return fs.Result{}, err
		}
		g.emit(op.Path)
		return fs.Result{}, nil

	case fs.OpUnlink, fs.OpRmdir:
		// POSIX unlink-while-open: the delete hides the name now; an open
		// claimed handle keeps its image and never publishes — EVERY
		// matching descriptor, not just the per-path newest (an older
		// fd's later sync would overwrite a recreated object with the
		// deleted image).
		var marked []*gitHandle
		if op.Kind == fs.OpUnlink {
			g.openHandles.Range(func(key, _ any) bool {
				if h, hok := key.(*gitHandle); hok {
					h.mu.Lock()
					path := h.path
					h.mu.Unlock()
					if path == op.Path {
						h.markUnlinked()
						marked = append(marked, h)
					}
				}
				return true
			})
		}
		if err := g.putOverlay(ctx, w, rel, overlayEntry{Op: "whiteout", Kind: "file"}); err != nil {
			// The whiteout did NOT land — the name still exists, and the
			// failed unlink must not silently orphan every marked fd (a
			// later Sync would skip it as anonymous and drop the write).
			for _, h := range marked {
				h.mu.Lock()
				h.unlinked = false
				h.mu.Unlock()
			}
			return fs.Result{}, err
		}
		g.emit(op.Path)
		return fs.Result{}, nil

	case fs.OpRename:
		if err := g.renameOverlay(ctx, op.Path, op.Path2); err != nil {
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
		h := g.handleFor(op.Path)
		// FATTR_FH names a specific descriptor: resolve THAT handle —
		// the per-path newest claim is a different fd after a second
		// open, and truncating it acknowledged nothing on the named one.
		// The chain is walked through UnwrapHandle decorators (the read
		// cache wraps handles); a bare type assert missed every wrapped
		// descriptor.
		if own := gitHandleOf(op.Handle); own != nil {
			h = own
		}
		if h != nil {
			if op.HasMode {
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
		handled := false
		if op.HasMode {
			if e, hit := w.lookup(rel); hit && e.Op == "upsert" {
				e.Mode = fmt.Sprintf("%o", 0o100000|op.Mode&0o777)
				if err := g.putOverlay(ctx, w, rel, e); err != nil {
					return fs.Result{}, err
				}
				g.emit(op.Path)
				handled = true
			}
		}
		if op.Size >= 0 {
			if e, hit := w.lookup(rel); hit && e.Op == "upsert" && e.Kind == "file" {
				nextImg := make([]byte, op.Size)
				copy(nextImg, e.Content)
				e.Content = nextImg
				if err := g.putOverlay(ctx, w, rel, e); err != nil {
					return fs.Result{}, err
				}
				g.emit(op.Path)
				handled = true
			}
		}
		if handled {
			// The mutation completed against the overlay and the file lives
			// ONLY there: forwarding to next replays it against the ordinary
			// filesystem, whose ENOENT then failed an operation that had
			// already succeeded.
			if e, hit := w.lookup(rel); hit && e.Kind == "file" {
				mode := uint32(0o644)
				if m, err := strconv.ParseUint(strings.TrimPrefix(e.Mode, "100000"), 8, 32); err == nil {
					mode = uint32(m)
				}
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: int64(len(e.Content))}}, nil
			}
			return fs.Result{}, nil
		}
	}
	return next(ctx, op)
}

// putOverlay persists one overlay entry server-side then mirrors it
// locally.
// gitHandleOf walks decorator wrappers (UnwrapHandle) to the git
// extension's own handle; nil when the chain holds none.
func gitHandleOf(h any) *gitHandle {
	for range 8 {
		if gh, ok := h.(*gitHandle); ok {
			return gh
		}
		u, ok := h.(interface{ UnwrapHandle() fs.Handle })
		if !ok {
			return nil
		}
		inner := u.UnwrapHandle()
		if inner == nil {
			return nil
		}
		h = inner
	}
	return nil
}

func (g *Git) putOverlay(ctx context.Context, w *workspace, rel string, e overlayEntry) error {
	// Path-addressed writes hand us op.Data — a slice of go-fuse's POOLED
	// request buffer, recycled for the next request (a READDIR's dirent
	// bytes) the moment this one completes. The entry (and the local
	// mirror) must own a copy or the stored image mutates in place.
	if len(e.Content) > 0 {
		e.Content = append([]byte(nil), e.Content...)
	}
	_, err := g.cl.PutGitOverlayEntry(ctx, w.id, client.GitOverlayEntryRequest{
		Path:    rel,
		Op:      e.Op,
		Kind:    e.Kind,
		Mode:    e.Mode,
		Content: e.Content,
	})
	if err != nil {
		return err
	}
	// Our own write stamps a fresh landed time: the rewrite's served stat
	// must advance past whatever the index last recorded (a rewrite with
	// an unchanged mtime stays racily clean and git re-hashes forever).
	e.mtime = time.Now()
	w.set(rel, e)
	return nil
}

// renameOverlay moves overlay content: the source overlay entry is
// re-upserted at the destination and deleted at the source; base content
// is fetched once and re-upserted.
func (g *Git) renameOverlay(ctx context.Context, oldPath, newPath string) error {
	wOld, relOld, ok := g.workspaceFor(oldPath)
	if !ok {
		return nil
	}
	wNew, relNew, ok := g.workspaceFor(newPath)
	if !ok || wNew != wOld {
		return fmt.Errorf("git: rename crosses workspace roots")
	}
	if e, hit := wOld.lookup(relOld); hit {
		if e.Op == "whiteout" {
			return fs.ErrNotFound
		}
		if e.Kind == "dir" && wOld.hasEntriesUnder(relOld+"/") {
			// A NON-EMPTY overlay directory: mv's recursive-copy fallback
			// owns subtree moves. Copying just the directory entry and
			// whiteouting the source reported success while dropping
			// every child from the new name.
			return syscall.EXDEV
		}
		if err := g.putOverlay(ctx, wNew, relNew, e); err != nil {
			return err
		}
		return g.putOverlay(ctx, wOld, relOld, overlayEntry{Op: "whiteout", Kind: e.Kind})
	}
	// base content moves through one fetch + upsert
	st, err := g.cl.StatCtx(ctx, oldPath)
	if err != nil {
		return err
	}
	if st.IsDir {
		return fmt.Errorf("git: directory rename of base content is not supported")
	}
	data, err := g.cl.ReadCtx(ctx, oldPath)
	if err != nil {
		return err
	}
	mode := "100644"
	if st.HasMode {
		mode = fmt.Sprintf("%o", 0o100000|st.Mode&0o777)
	}
	if err := g.putOverlay(ctx, wNew, relNew, overlayEntry{Op: "upsert", Kind: "file", Content: data, Mode: mode}); err != nil {
		return err
	}
	return g.putOverlay(ctx, wOld, relOld, overlayEntry{Op: "whiteout", Kind: "file"})
}

// listMerged overlays the workspace's children on the base listing.
func (g *Git) listMerged(ctx context.Context, w *workspace, dir string, next fs.Next) (fs.Result, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
	if err != nil && !fs.IsNotFound(err) {
		return res, err
	}
	out := map[string]fs.Entry{}
	for _, e := range res.Entries {
		out[e.Name] = e
	}
	prefixRel := strings.Trim(strings.TrimPrefix(strings.TrimPrefix(dir, w.rootPath()), "/"), "/")
	// Compute the stable clean-tree time BEFORE taking the workspace read
	// lock: cleanTreeMtime re-acquires it, and Go's RWMutex deadlocks a
	// recursive reader as soon as one writer queues behind the outer hold.
	cleanMtime := w.cleanTreeMtime()
	w.mu.RLock()
	for rel, e := range w.overlay {
		if prefixRel != "" && !strings.HasPrefix(rel, prefixRel+"/") {
			continue
		}
		name := strings.TrimPrefix(rel, prefixRel+"/")
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		if e.Op == "whiteout" {
			delete(out, name)
			continue
		}
		kind := kindOf(e)
		mode := parseMode(e.Mode)
		if mode == 0 {
			if kind == fs.KindDir {
				mode = 0o755
			} else {
				mode = 0o644
			}
		}
		size := int64(len(e.Content))
		if e.remote {
			size = e.size
		}
		mtime := e.mtime
		if mtime.IsZero() {
			mtime = cleanMtime
		}
		out[name] = fs.Entry{Name: name, Attr: fs.Attr{Kind: kind, Mode: mode, Size: size, Mtime: mtime}}
	}
	// Fast-clone view (v1 listGitDir): tree children the base and overlay
	// do not already cover appear from the HEAD tree.
	type unresolved struct {
		rel  string
		sha  string
		name string
	}
	var pending []unresolved
	for rel, n := range w.tree {
		if prefixRel != "" && !strings.HasPrefix(rel, prefixRel+"/") {
			continue
		}
		name := strings.TrimPrefix(rel, prefixRel+"/")
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		if _, covered := out[name]; covered {
			continue
		}
		attr := treeAttr(n, cleanMtime)
		// Listing rows feed READDIRPLUS, whose attrs the kernel applies
		// directly to the inode: an unresolved blobless size (placeholder
		// 0) retires a known-good i_size and every later read returns
		// empty. Resolve unknown file/symlink sizes once via cat-file —
		// OUTSIDE the workspace read lock: noteTreeSize writes.
		if n.SizeBytes < 0 && (n.Kind == "file" || n.Kind == "symlink") && n.ObjectSHA != "" {
			pending = append(pending, unresolved{rel: rel, sha: n.ObjectSHA, name: name})
		}
		out[name] = fs.Entry{Name: name, Attr: attr}
	}
	w.mu.RUnlock()
	for _, u := range pending {
		size := g.blobSize(w, u.sha)
		w.noteTreeSize(u.rel, size)
		if e, ok := out[u.name]; ok && size > 0 {
			e.Attr.Size = size
			out[u.name] = e
		}
	}
	entries := make([]fs.Entry, 0, len(out))
	for _, e := range out {
		entries = append(entries, e)
	}
	return fs.Result{Entries: entries}, nil
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
