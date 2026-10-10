// The handler: op routing against the overlay and the merged directory
// listing.
package layer

import (
	"context"
	"errors"
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
			return next(ctx, op)
		}
		switch kind {
		case "whiteout":
			return fs.Result{}, fs.ErrNotFound
		case "dir":
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode, Mtime: time.Now()}}, nil
		case "symlink":
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindSymlink, Mode: mode & 0o777}}, nil
		default:
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: int64(len(content))}}, nil
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
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
		}
		if hasWriteIntent(op.Flags) {
			// Claim the data plane: writes buffer locally and land as one
			// layer entry per durability boundary. A create materializes
			// at the boundary, not at open.
			h, err := l.claim(op.Path, op.Flags, op.Mode&0o777)
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

	case fs.OpUnlink, fs.OpRmdir:
		if l.readOnly {
			return fs.Result{}, syscall.EROFS
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
		if h := l.handleFor(op.Path); h != nil {
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
			kind, _, _, _, ok := l.lookup(op.Path)
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
				nextImg := make([]byte, op.Size)
				copy(nextImg, content)
				if err := l.upsertFile(ctx, op.Path, nextImg, 0, mode); err != nil {
					return fs.Result{}, err
				}
			}
		}
		return l.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path}, next)
	}
	return next(ctx, op)
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
		add(p, m, fs.KindFile, int64(len(l.contents[p])))
	}
	l.mu.RUnlock()
	entries := make([]fs.Entry, 0, len(out))
	for _, e := range out {
		entries = append(entries, e)
	}
	return fs.Result{Entries: entries}, nil
}
