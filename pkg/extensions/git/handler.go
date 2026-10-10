// The handler: op routing against each workspace's overlay, overlay
// persistence, rename, and the merged directory listing.
package git

import (
	"context"
	"fmt"
	"strings"
	"syscall"

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
		return next(ctx, op)
	}

	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		e, hit := w.lookup(rel)
		if !hit {
			return next(ctx, op)
		}
		if e.Op == "delete" {
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
		return fs.Result{Attr: &fs.Attr{Kind: kind, Mode: mode, Size: int64(len(e.Content))}}, nil

	case fs.OpReadlink:
		if e, hit := w.lookup(rel); hit {
			if e.Op == "delete" {
				return fs.Result{}, fs.ErrNotFound
			}
			if e.Kind == "symlink" {
				return fs.Result{Data: e.Content}, nil
			}
		}
		return next(ctx, op)

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		if e, hit := w.lookup(rel); hit {
			if e.Op == "delete" {
				return fs.Result{}, fs.ErrNotFound
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
			if e, hit := w.lookup(rel); !hit || (e.Op == "upsert" && e.Kind == "file") {
				h, cerr := g.claimWorkspaceOpen(w, op.Path, rel, op.Flags, op.Mode)
				if cerr != nil {
					return fs.Result{}, cerr
				}
				if op.Flags&uint32(syscall.O_CREAT) != 0 && !hit {
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
				if e.Op == "delete" {
					return fs.Result{}, fs.ErrNotFound
				}
				mode := parseMode(e.Mode)
				if mode == 0 {
					mode = 0o644
				}
				return fs.Result{Attr: &fs.Attr{Kind: kindOf(e), Mode: mode, Size: int64(len(e.Content))}}, nil
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
		// claimed handle keeps its image and never publishes.
		if h := g.handleFor(op.Path); h != nil && op.Kind == fs.OpUnlink {
			h.markUnlinked()
		}
		if err := g.putOverlay(ctx, w, rel, overlayEntry{Op: "delete", Kind: "file"}); err != nil {
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
		if op.Mode != 0 {
			if e, hit := w.lookup(rel); hit && e.Op == "upsert" {
				e.Mode = fmt.Sprintf("100%04o", op.Mode&0o777)
				if err := g.putOverlay(ctx, w, rel, e); err != nil {
					return fs.Result{}, err
				}
				g.emit(op.Path)
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
			}
		}
	}
	return next(ctx, op)
}

// putOverlay persists one overlay entry server-side then mirrors it
// locally.
func (g *Git) putOverlay(ctx context.Context, w *workspace, rel string, e overlayEntry) error {
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
		if e.Op == "delete" {
			return fs.ErrNotFound
		}
		if err := g.putOverlay(ctx, wNew, relNew, e); err != nil {
			return err
		}
		return g.putOverlay(ctx, wOld, relOld, overlayEntry{Op: "delete", Kind: e.Kind})
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
		mode = fmt.Sprintf("100%04o", st.Mode&0o777)
	}
	if err := g.putOverlay(ctx, wNew, relNew, overlayEntry{Op: "upsert", Kind: "file", Content: data, Mode: mode}); err != nil {
		return err
	}
	return g.putOverlay(ctx, wOld, relOld, overlayEntry{Op: "delete", Kind: "file"})
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
	prefixRel := strings.Trim(strings.TrimPrefix(strings.TrimPrefix(dir, w.root), "/"), "/")
	w.mu.RLock()
	for rel, e := range w.overlay {
		if prefixRel != "" && !strings.HasPrefix(rel, prefixRel+"/") {
			continue
		}
		name := strings.TrimPrefix(rel, prefixRel+"/")
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		if e.Op == "delete" {
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
		out[name] = fs.Entry{Name: name, Attr: fs.Attr{Kind: kind, Mode: mode, Size: int64(len(e.Content))}}
	}
	w.mu.RUnlock()
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
