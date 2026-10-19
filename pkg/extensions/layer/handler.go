// The handler: op routing against the overlay and the merged directory
// listing.
package layer

import (
	"context"
	"errors"
	"io"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Do routes through do and maps the rollback error to ESTALE: after a
// rollback event the mount must fail writes with ESTALE until remount.
func (l *Layer) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	res, err := l.do(ctx, op, next)
	if err != nil && errors.Is(err, errLayerRolledBack) {
		return res, syscall.ESTALE
	}
	return res, err
}

func (l *Layer) do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		kind, mode, _, content, ok := l.lookup(op.Path)
		if !ok {
			// Namespace-only directories exist while any overlay entry
			// lives below them.
			if l.impliedDir(op.Path) {
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0o755, Mtime: time.Now()}}, nil
			}
			// A whiteouted ANCESTOR deletes the whole subtree from the
			// base namespace: falling through would resurrect renamed or
			// deleted parents' children (a directory rename left /d
			// whiteouted while the base /d/f still resolved).
			if l.maskedByWhiteout(op.Path) {
				return fs.Result{}, fs.ErrNotFound
			}
			res, err := next(ctx, op)
			if err != nil {
				return res, err
			}
			// A mode-only override on a base-backed file must stay
			// visible: the base's stat still reports its own mode, and a
			// chmod(0600) then answered 0644 (review).
			if m, hit := l.baseFileMode(op.Path); hit && res.Attr != nil && res.Attr.Kind == fs.KindFile {
				res.Attr.Mode = m
			}
			return res, err
		}
		switch kind {
		case "whiteout":
			return fs.Result{}, fs.ErrNotFound
		case "dir":
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode, Mtime: time.Now()}}, nil
		case "symlink":
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindSymlink, Mode: mode & 0o777}}, nil
		default:
			size := int64(len(content))
			if rs, ok := l.remoteSize(op.Path); ok {
				size = rs
			}
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size}}, nil
		}

	case fs.OpReadlink:
		if kind, _, t, _, ok := l.lookup(op.Path); ok && kind == "symlink" {
			return fs.Result{Data: []byte(t)}, nil
		}
		return next(ctx, op)

	case fs.OpList:
		return l.listMerged(ctx, op.Path, next)

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		kind, _, _, content, ok := l.lookup(op.Path)
		if ok && kind == "file" {
			if content == nil && func() bool { _, rok := l.remoteSize(op.Path); return rok }() {
				// An uploaded-but-unmaterialized layer image: hydrate it
				// once — per-read streaming re-downloaded the whole object
				// for every FUSE read (and an empty reply hid it).
				if err := l.hydrateRemote(ctx, op.Path); err != nil {
					return fs.Result{}, err
				}
				_, _, _, content, _ = l.lookup(op.Path)
			}
			data := content
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
		if ok && kind == "whiteout" {
			return fs.Result{}, fs.ErrNotFound
		}
		return next(ctx, op)

	case fs.OpCreate:
		// Checkpoint mounts are read-only for WRITES only: plain read opens
		// must serve the restored overlay (v1). The blanket gate rejected
		// even O_RDONLY opens with EROFS, so every checkpoint-restore read
		// failed while the listing (OpList) worked.
		if l.readOnly && (hasWriteIntent(op.Flags) || op.Flags&uint32(syscall.O_CREAT) != 0 || op.Flags&uint32(syscall.O_TRUNC) != 0) {
			return fs.Result{}, syscall.EROFS
		}
		if hasWriteIntent(op.Flags) {
			// Claim the data plane: writes buffer locally and land as one
			// layer entry per durability boundary. A create materializes
			// at the boundary, not at open.
			h, err := l.claimWithMode(op.Path, op.Flags, op.Mode&0o777, op.HasMode)
			if err != nil {
				return fs.Result{}, err
			}
			if op.Flags&uint32(syscall.O_CREAT) != 0 && h.isNew {
				h.mu.Lock()
				h.dirty = true
				h.mu.Unlock()
			}
			l.liveStore(op.Path, h)
			attr := h.Attr()
			return fs.Result{Handle: h, Attr: &attr}, nil
		}
		if op.Flags&uint32(syscall.O_CREAT) == 0 {
			// plain read-only open: overlay files report their overlay attr
			if kind, mode, _, content, ok := l.lookup(op.Path); ok {
				switch kind {
				case "whiteout":
					return fs.Result{}, fs.ErrNotFound
				case "file":
					return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: int64(len(content))}}, nil
				}
			}
			return next(ctx, op)
		}
		if err := l.upsertFile(ctx, op.Path, nil, 0, op.Mode); err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: op.Mode & 0o777}}, nil

	case fs.OpWrite:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		baseRev := int64(0)
		data := op.Data
		// merge with the overlay's current image
		if kind, _, _, content, ok := l.lookup(op.Path); ok && kind == "file" {
			merged := append([]byte(nil), content...)
			end := op.Offset + int64(len(op.Data))
			if end > int64(len(merged)) {
				grown := make([]byte, end)
				copy(grown, merged)
				merged = grown
			}
			copy(merged[op.Offset:], op.Data)
			data = merged
		} else if _, serr := l.cl.StatCtx(ctx, l.remotePath(op.Path)); serr == nil {
			if base, rerr := l.cl.ReadCtx(ctx, l.remotePath(op.Path)); rerr == nil {
				merged := append([]byte(nil), base...)
				end := op.Offset + int64(len(op.Data))
				if end > int64(len(merged)) {
					grown := make([]byte, end)
					copy(grown, merged)
					merged = grown
				}
				copy(merged[op.Offset:], op.Data)
				data = merged
			}
		}
		mode := op.Mode
		if mode == 0 {
			mode = 0o644
		}
		if err := l.upsertFile(ctx, op.Path, data, baseRev, mode); err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Written: uint32(len(op.Data))}, nil

	case fs.OpMkdir:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		mode := op.Mode & 0o777
		if mode == 0 {
			mode = 0o755
		}
		if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{Path: l.remotePath(op.Path), Op: "mkdir", Kind: "dir", Mode: mode}); err != nil {
			return fs.Result{}, err
		}
		l.markLayerDir(op.Path, mode)
		l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode}}, nil

	case fs.OpSymlink:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{
			Path: l.remotePath(op.Path), Op: "symlink", Kind: "symlink",
			ContentText: op.Target, SizeBytes: int64(len(op.Target)),
			Mode: uint32(syscall.S_IFLNK) | 0o777,
		}); err != nil {
			return fs.Result{}, err
		}
		l.markLayerSymlink(op.Path, op.Target, 0)
		l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
		return fs.Result{}, nil

	case fs.OpLink:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		// v1 upsertLayerHardlink: the target is ALWAYS a layer entry —
		// overlay content, an object-backed overlay image (hydrated
		// first: its lookup content is nil until read, and publishing
		// that as empty zeroed the link), or the base object read into
		// the layer. Falling through to the base plane's hardlink for
		// base sources mutated the shared base namespace from uncommitted
		// layer state (visible to other mounts, beyond rollback).
		data, mode, ok := l.linkSourceImage(ctx, op.Path)
		if !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		m := mode
		if m == 0 {
			m = 0o644
		}
		if err := l.upsertFile(ctx, op.Path2, data, 0, m); err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: m, Size: int64(len(data))}}, nil

	case fs.OpUnlink, fs.OpRmdir:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		// Emptiness is judged over the MERGED view: overlay children
		// (minus whiteouted ones) plus real base children. The overlay-only
		// check both hid base children (rmdir of a dir holding base files
		// succeeded and whiteouted the whole directory) and counted
		// whiteouted children as presence (an emptied dir stayed
		// ENOTEMPTY forever).
		if op.Kind == fs.OpRmdir && l.mergedDirHasChildren(ctx, op.Path, next) {
			return fs.Result{}, syscall.ENOTEMPTY
		}
		unlock := l.mutLocks.lock(op.Path)
		defer unlock()
		// POSIX unlink-while-open: the whiteout hides the name now, the
		// open handle keeps serving its buffered image and never
		// publishes. EVERY matching descriptor goes anonymous — marking
		// only the per-path newest left an older sibling free to upsert
		// the whiteouted name back at its next sync.
		var marked []*layerHandle
		markPathHandles := func() {
			l.regMu.Lock()
			defer l.regMu.Unlock()
			l.handles.Range(func(key, _ any) bool {
				if h, hok := key.(*layerHandle); hok {
					h.mu.Lock()
					path, released := h.path, h.released
					h.mu.Unlock()
					if !released && path == op.Path {
						h.markUnlinked()
						marked = append(marked, h)
					}
				}
				return true
			})
		}
		if op.Kind == fs.OpUnlink {
			markPathHandles()
		}
		kind := "file"
		if op.Kind == fs.OpRmdir {
			kind = "dir"
		}
		if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{Path: l.remotePath(op.Path), Op: "whiteout", Kind: kind}); err != nil {
			// The whiteout did NOT land: the name still exists, and a
			// failed unlink must not silently orphan every marked fd —
			// their later commits stay bound to the live name.
			for _, h := range marked {
				h.mu.Lock()
				h.unlinked = false
				h.mu.Unlock()
			}
			return fs.Result{}, err
		}
		if op.Kind == fs.OpUnlink {
			// A writable open can register between the sweep and the
			// whiteout landing (opens take regMu, but the HTTP window is
			// outside it): re-sweep so late arrivals are anonymized too —
			// their next sync would otherwise upsert the deleted name
			// back. Opens after the whiteout exists are rejected at claim
			// time by the whiteout lookup.
			markPathHandles()
		}
		l.markLayerWhiteout(op.Path)
		l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
		return fs.Result{}, nil

	case fs.OpRename:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		unlockOld := l.mutLocks.lock(op.Path)
		unlockNew := l.mutLocks.lock(op.Path2)
		defer unlockNew()
		defer unlockOld()
		if err := l.renameLayer(ctx, op.Path, op.Path2); err != nil {
			return fs.Result{}, err
		}
		// EVERY open descriptor follows the rename, not just the
		// per-path newest slot: an older sibling left bound to the old
		// name recreated it at its next sync (writing `A` through the
		// first of two descriptors, renaming, then syncing landed
		// `Aello` back on the old name). A DIRECTORY rename also moves
		// every open CHILD file (their commits would otherwise land
		// under the dead parent path).
		prefix := strings.TrimSuffix(op.Path, "/") + "/"
		l.handles.Range(func(key, _ any) bool {
			if h, hok := key.(*layerHandle); hok {
				h.mu.Lock()
				path, released := h.path, h.released
				h.mu.Unlock()
				if released {
					return true
				}
				switch {
				case path == op.Path:
					h.retarget(op.Path2)
				case strings.HasPrefix(path, prefix):
					h.retarget(op.Path2 + strings.TrimPrefix(path, op.Path))
				}
			}
			return true
		})
		l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
		l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path2})
		return fs.Result{}, nil

	case fs.OpSetAttr:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		// Both SETATTR halves must see the authoritative image: an
		// uploaded-but-unmaterialized entry answered as an empty file
		// (chmod published a zero-byte image; a second truncate zeroed
		// the prefix).
		if err := l.hydrateRemote(ctx, op.Path); err != nil {
			return fs.Result{}, err
		}
		h := l.handleFor(op.Path)
		// FATTR_FH names a specific descriptor: resolve THAT handle —
		// the per-path newest claim is a different fd after a second
		// open, and truncating it acknowledged nothing on the named one.
		// The chain is walked through UnwrapHandle decorators (the read
		// cache wraps handles; localize wraps again) — a bare type
		// assert missed every wrapped descriptor and fell back to the
		// newest claim.
		if own := layerHandleOf(op.Handle); own != nil {
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
		if op.HasMode {
			kind, _, _, _, ok := l.lookup(op.Path)
			if ok && kind == "file" {
				// v1: a chmod of a path this layer already upserted
				// merges into that upsert's mode — a standalone chmod
				// entry has no base to describe, and the diff/restore
				// lose the content (the smoke's umask-then-chmod create).
				unlock := l.mutLocks.lock(op.Path)
				_, _, _, content, _ := l.lookup(op.Path)
				err := l.upsertFileLocked(ctx, op.Path, content, 0, op.Mode&0o777)
				unlock()
				if err != nil {
					return fs.Result{}, err
				}
				l.markLayerFileMode(op.Path, op.Mode)
				l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
				return l.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path}, next)
			}
			if !ok {
				// Base-only file: record a mode-only override — the base
				// object stays the content source (registering an empty
				// overlay image would make every read return zero bytes).
				if st, serr := l.cl.StatCtx(ctx, l.remotePath(op.Path)); serr == nil && st != nil && !st.IsDir {
					if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{Path: l.remotePath(op.Path), Op: "chmod", Kind: "file", Mode: op.Mode & 0o777}); err != nil {
						return fs.Result{}, err
					}
					l.markLayerBaseFileMode(op.Path, op.Mode)
					l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
					return l.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path}, next)
				}
			}
			entryKind := "file"
			if !ok {
				if st, err := l.cl.StatCtx(ctx, l.remotePath(op.Path)); err == nil && st != nil && st.IsDir {
					entryKind = "dir"
				}
			} else if kind == "dir" {
				entryKind = "dir"
			} else if kind == "symlink" {
				entryKind = "symlink"
			}
			if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{Path: l.remotePath(op.Path), Op: "chmod", Kind: entryKind, Mode: op.Mode & 0o777}); err != nil {
				return fs.Result{}, err
			}
			switch {
			case ok && kind == "dir":
				l.markLayerDir(op.Path, op.Mode)
			case ok && kind == "symlink":
				_, _, tgt, _, _ := l.lookup(op.Path)
				l.markLayerSymlink(op.Path, tgt, op.Mode)
			default:
				l.markLayerFileMode(op.Path, op.Mode)
			}
			l.emit(fs.Event{Kind: fs.EventInvalidated, Path: op.Path})
		}
		if op.Size >= 0 {
			if kind, mode, _, content, ok := l.lookup(op.Path); ok && kind == "file" {
				if err := l.truncateFile(ctx, op.Path, content, op.Size, mode); err != nil {
					return fs.Result{}, err
				}
			} else if st, serr := l.cl.StatCtx(ctx, l.remotePath(op.Path)); serr == nil && st != nil && !st.IsDir {
				// Base-only file (v1 applyRemoteTruncate): a truncation
				// still records a layer image at the requested size —
				// returning the unchanged attrs silently dropped it.
				mode := uint32(0o644)
				if m, ok := l.baseFileMode(op.Path); ok {
					mode = m
				}
				kept := int64(0)
				if st.Size < op.Size {
					kept = st.Size
				} else {
					kept = op.Size
				}
				content := []byte(nil)
				var lazyBase baseRangeReader
				if kept > 0 {
					if kept <= l.inlineMax() {
						data, rerr := l.fetchBaseRange(ctx, op.Path, 0, kept)
						if rerr != nil {
							// The retained prefix IS the new entry's content:
							// a failed read must abort the truncation —
							// publishing the nil content would persist zero-
							// filled bytes over the real prefix (main's
							// applyRemoteTruncate rejected the failure).
							return fs.Result{}, rerr
						}
						content = data
					} else {
						// Above the inline limit the retained prefix
						// streams from the base object — a nil content
						// here zero-filled the first kept bytes (review).
						rp := l.remotePath(op.Path)
						lazyBase = func(bctx context.Context, off, length int64) (io.Reader, error) {
							return l.cl.ReadStreamRange(bctx, rp, off, length)
						}
					}
				}
				if err := l.truncateFileWithBase(ctx, op.Path, content, op.Size, mode, lazyBase); err != nil {
					return fs.Result{}, err
				}
			}
		}
		return l.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path}, next)
	}
	return next(ctx, op)
}

// dirHasOverlayChildren ports v1 layerDirHasOverlayChildren: any overlay
// entry (file, dir, symlink, whiteout, or live handle) directly under dir
// makes the directory non-empty for rmdir.
// layerHandleOf walks decorator wrappers (UnwrapHandle) to the layer's
// own handle; nil when the chain holds none.
func layerHandleOf(h any) *layerHandle {
	for range 8 {
		if lh, ok := h.(*layerHandle); ok {
			return lh
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

// mergedDirHasChildren reports whether the directory has any VISIBLE
// child in the merged layer+base view: overlay children survive unless
// whiteouted, and base children count when the overlay does not replace
// or delete them.
func (l *Layer) mergedDirHasChildren(ctx context.Context, dir string, next fs.Next) bool {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	direct := func(p string) (string, bool) {
		if !strings.HasPrefix(p, prefix) {
			return "", false
		}
		name := strings.TrimPrefix(p, prefix)
		return name, name != "" && !strings.Contains(name, "/")
	}
	l.mu.RLock()
	visible := map[string]bool{}
	for p := range l.files {
		if n, ok := direct(p); ok {
			visible[n] = true
		}
	}
	for p := range l.dirs {
		if n, ok := direct(p); ok {
			visible[n] = true
		}
	}
	for p := range l.symlinks {
		if n, ok := direct(p); ok {
			visible[n] = true
		}
	}
	for p := range l.whiteouts {
		if n, ok := direct(p); ok {
			// A whiteout deletes the name outright — it is not presence.
			delete(visible, n)
		}
	}
	l.mu.RUnlock()
	if len(visible) > 0 {
		return true
	}
	// Base children that the overlay neither replaced nor deleted.
	res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
	if err != nil {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, e := range res.Entries {
		p := prefix + e.Name
		if _, wh := l.whiteouts[p]; wh {
			continue
		}
		if _, f := l.files[p]; f {
			continue
		}
		if _, d := l.dirs[p]; d {
			continue
		}
		if _, sl := l.symlinks[p]; sl {
			continue
		}
		return true
	}
	return false
}

// linkSourceImage resolves a hardlink source's full image inside the
// layer: overlay bytes, a hydrated object-backed overlay entry, or the
// base object itself.
func (l *Layer) linkSourceImage(ctx context.Context, path string) ([]byte, uint32, bool) {
	kind, mode, _, content, ok := l.lookup(path)
	switch {
	case ok && kind == "whiteout":
		return nil, 0, false
	case ok && kind == "file" && content == nil:
		// Object-backed entry not yet materialized: hydrate, then read.
		if _, rok := l.remoteSize(path); rok {
			if err := l.hydrateRemote(ctx, path); err != nil {
				return nil, 0, false
			}
			_, mode, _, content, ok = l.lookup(path)
			if !ok {
				return nil, 0, false
			}
		}
		return append([]byte(nil), content...), mode, true
	case ok && kind == "file":
		return append([]byte(nil), content...), mode, true
	}
	// Base-plane source: read the object into the layer.
	st, err := l.cl.StatCtx(ctx, l.remotePath(path))
	if err != nil || st == nil || st.IsDir {
		return nil, 0, false
	}
	data, err := l.cl.ReadCtx(ctx, l.remotePath(path))
	if err != nil {
		return nil, 0, false
	}
	m := st.Mode & 0o777
	if m == 0 {
		m = 0o644
	}
	return data, m, true
}

// maskedByWhiteout reports whether any ancestor of p (or p itself) is
// whiteouted in the overlay — the base namespace must not answer through
// a deleted parent.
func (l *Layer) maskedByWhiteout(p string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for {
		if _, hit := l.whiteouts[p]; hit {
			return true
		}
		slash := strings.LastIndexByte(p, '/')
		if slash <= 0 {
			return false
		}
		p = p[:slash]
	}
}

func (l *Layer) dirHasOverlayChildren(dir string) bool {
	prefix := strings.TrimRight(dir, "/") + "/"
	has := func(path string) bool {
		if !strings.HasPrefix(path, prefix) {
			return false
		}
		name := strings.TrimPrefix(path, prefix)
		return name != "" && !strings.Contains(name, "/")
	}
	l.mu.RLock()
	for p := range l.whiteouts {
		if has(p) {
			l.mu.RUnlock()
			return true
		}
	}
	for p := range l.files {
		if has(p) {
			l.mu.RUnlock()
			return true
		}
	}
	for p := range l.dirs {
		if has(p) {
			l.mu.RUnlock()
			return true
		}
	}
	for p := range l.symlinks {
		if has(p) {
			l.mu.RUnlock()
			return true
		}
	}
	for p := range l.contents {
		if has(p) {
			l.mu.RUnlock()
			return true
		}
	}
	l.mu.RUnlock()
	found := false
	l.live.Range(func(key, _ any) bool {
		if path, ok := key.(string); ok && has(path) {
			found = true
			return false
		}
		return true
	})
	return found
}

// listMerged overlays this layer's children on the base listing.
func (l *Layer) listMerged(ctx context.Context, dir string, next fs.Next) (fs.Result, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
	if err != nil && !fs.IsNotFound(err) {
		return res, err
	}
	out := map[string]fs.Entry{}
	for _, e := range res.Entries {
		out[e.Name] = e
	}
	prefix := strings.TrimRight(dir, "/") + "/"
	l.mu.RLock()
	add := func(path string, mode uint32, kind fs.Kind, size int64) {
		if !strings.HasPrefix(path, prefix) {
			return
		}
		name := strings.TrimPrefix(path, prefix)
		if name == "" || strings.Contains(name, "/") {
			return
		}
		out[name] = fs.Entry{Name: name, Attr: fs.Attr{Kind: kind, Mode: mode, Size: size}}
	}
	for p := range l.whiteouts {
		if strings.HasPrefix(p, prefix) {
			name := strings.TrimPrefix(p, prefix)
			if name != "" && !strings.Contains(name, "/") {
				delete(out, name)
			}
		}
	}
	for p, m := range l.dirs {
		add(p, m, fs.KindDir, 0)
	}
	for p, st := range l.symlinks {
		add(p, st.Mode&0o777, fs.KindSymlink, int64(len(st.Target)))
	}
	for p, m := range l.files {
		sz := int64(len(l.contents[p]))
		if rs, ok := l.remoteSizes[p]; ok {
			sz = rs
		}
		add(p, m, fs.KindFile, sz)
	}
	l.mu.RUnlock()
	entries := make([]fs.Entry, 0, len(out))
	for _, e := range out {
		entries = append(entries, e)
	}
	return fs.Result{Entries: entries}, nil
}

// fetchBaseRange reads a byte range of the base object (small images only;
// larger truncations stream through truncateFile's reader path).
func (l *Layer) fetchBaseRange(ctx context.Context, path string, off, length int64) ([]byte, error) {
	rc, err := l.cl.ReadStreamRange(ctx, l.remotePath(path), off, length)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}
