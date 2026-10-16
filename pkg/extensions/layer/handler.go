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
		// v1 upsertLayerHardlink: a hardlink whose source lives in the
		// overlay copies the image to the target as a layer upsert — the
		// base plane cannot hardlink overlay content. Base-plane sources
		// fall through to the base plane's hardlink.
		if kind, mode, _, content, ok := l.lookup(op.Path); ok && kind == "file" {
			data := append([]byte(nil), content...)
			m := mode
			if m == 0 {
				m = 0o644
			}
			if err := l.upsertFile(ctx, op.Path2, data, 0, m); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: m, Size: int64(len(data))}}, nil
		}
		return next(ctx, op)

	case fs.OpUnlink, fs.OpRmdir:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		// v1 layerDirHasOverlayChildren: a non-empty layer directory must
		// refuse rmdir with ENOTEMPTY before the whiteout lands — the
		// overlay entries would otherwise live on under a hidden name.
		if op.Kind == fs.OpRmdir && l.dirHasOverlayChildren(op.Path) {
			return fs.Result{}, syscall.ENOTEMPTY
		}
		unlock := l.mutLocks.lock(op.Path)
		defer unlock()
		// POSIX unlink-while-open: the whiteout hides the name now, the
		// open handle keeps serving its buffered image and never publishes.
		if h := l.handleFor(op.Path); h != nil && op.Kind == fs.OpUnlink {
			h.markUnlinked()
		}
		kind := "file"
		if op.Kind == fs.OpRmdir {
			kind = "dir"
		}
		if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{Path: l.remotePath(op.Path), Op: "whiteout", Kind: kind}); err != nil {
			return fs.Result{}, err
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
		if h := l.handleFor(op.Path); h != nil {
			h.retarget(op.Path2)
		}
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
		if h := l.handleFor(op.Path); h != nil {
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
