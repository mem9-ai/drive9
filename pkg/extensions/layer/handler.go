// The handler: routes every namespace op against the shadow view —
// blackout hides, tree rewrites, attrs patches, everything else passes
// through to the classic view. Mutations are copy-on-write.
package layer

import (
	"context"
	"errors"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Do routes through do and maps the rollback error to ESTALE: after a
// rollback event the mount must fail writes with ESTALE until remount or
// checkout.
func (l *Layer) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	res, err := l.do(ctx, op, next)
	if err != nil && errors.Is(err, errLayerRolledBack) {
		return res, syscall.ESTALE
	}
	return res, err
}

// resKind classifies how a view path resolves.
type resKind uint8

const (
	resClassic resKind = iota
	resTree
	resAttrs
	resBlackout
)

func (l *Layer) resolve(v *view, p string) (resKind, *indexEntry) {
	if v.idx.isBlackedOut(p) {
		return resBlackout, nil
	}
	if e, ok := v.idx.get(p); ok {
		switch e.kind {
		case ekBlackout:
			return resBlackout, e
		case ekTree:
			return resTree, e
		case ekAttrs:
			return resAttrs, e
		}
	}
	return resClassic, nil
}

// isReservedPath reports the /.drive9 namespace, which never participates
// in the shadow (it is reserved, and the layer roots live inside it).
func isReservedPath(p string) bool {
	return p == "/.drive9" || strings.HasPrefix(p, "/.drive9/")
}

func (l *Layer) do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if isReservedPath(op.Path) || (op.Path2 != "" && isReservedPath(op.Path2)) {
		return next(ctx, op)
	}
	l.mu.RLock()
	v := l.cur
	l.mu.RUnlock()
	if v.mode == viewLive {
		return next(ctx, op)
	}

	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		return l.attrOp(ctx, v, op, next)

	case fs.OpReadlink:
		switch res, _ := l.resolve(v, op.Path); res {
		case resBlackout:
			return fs.Result{}, fs.ErrNotFound
		case resTree:
			rop := *op
			rop.Path = v.treePathOf(l.serverAbs(op.Path))
			return next(ctx, &rop)
		}
		return next(ctx, op)

	case fs.OpList:
		return l.listMerged(ctx, v, op.Path, next)

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		switch res, _ := l.resolve(v, op.Path); res {
		case resBlackout:
			return fs.Result{}, fs.ErrNotFound
		case resTree:
			rop := *op
			rop.Path = v.treePathOf(l.serverAbs(op.Path))
			return next(ctx, &rop)
		case resAttrs:
			return next(ctx, op)
		}
		return next(ctx, op)

	case fs.OpCreate:
		if err := v.mutationGate(op.Flags&uint32(syscall.O_CREAT) != 0 || hasWriteIntent(op.Flags)); err != nil {
			return fs.Result{}, err
		}
		return l.createOp(ctx, v, op, next)

	case fs.OpWrite:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.writeOp(ctx, v, op, next)

	case fs.OpMkdir:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.mkdirOp(ctx, v, op, next)

	case fs.OpSymlink:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.symlinkOp(ctx, v, op, next)

	case fs.OpMknod:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.mknodOp(ctx, v, op, next)

	case fs.OpUnlink:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.unlinkOp(ctx, v, op.Path, false, next)

	case fs.OpRmdir:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.unlinkOp(ctx, v, op.Path, true, next)

	case fs.OpRename:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.renameOp(ctx, v, op, next)

	case fs.OpLink:
		if err := v.mutationGate(true); err != nil {
			return fs.Result{}, err
		}
		return l.linkOp(ctx, v, op, next)

	case fs.OpSetAttr:
		if err := v.mutationGate(op.Mode != 0 || op.Size >= 0 || !op.Mtime.IsZero()); err != nil {
			return fs.Result{}, err
		}
		return l.setAttrOp(ctx, v, op, next)

	case fs.OpAccess:
		switch res, _ := l.resolve(v, op.Path); res {
		case resBlackout:
			return fs.Result{}, fs.ErrNotFound
		case resTree:
			rop := *op
			rop.Path = v.treePathOf(l.serverAbs(op.Path))
			return next(ctx, &rop)
		}
		return next(ctx, op)

	case fs.OpGetXattr, fs.OpListXattr:
		switch res, _ := l.resolve(v, op.Path); res {
		case resBlackout:
			return fs.Result{}, fs.ErrNotFound
		case resTree:
			rop := *op
			rop.Path = v.treePathOf(l.serverAbs(op.Path))
			return next(ctx, &rop)
		}
		return next(ctx, op)
	}
	return next(ctx, op)
}

// mutationGate rejects mutations on read-only (checkpoint) and rolled-back
// views. wants reports whether the op mutates (plain read opens pass).
func (v *view) mutationGate(wants bool) error {
	if !wants {
		return nil
	}
	if v.abandoned {
		return errLayerRolledBack
	}
	if v.readOnly {
		return syscall.EROFS
	}
	return nil
}

func (l *Layer) attrOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch res, e := l.resolve(v, op.Path); res {
	case resBlackout:
		return fs.Result{}, fs.ErrNotFound
	case resTree:
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(op.Path))
		return next(ctx, &rop)
	case resAttrs:
		res, err := next(ctx, op)
		if err != nil || res.Attr == nil {
			return res, err
		}
		patched := *res.Attr
		if e.patch.Mode != 0 {
			patched.Mode = e.patch.Mode
		}
		if e.patch.Mtime != 0 {
			patched.Mtime = unixToTime(e.patch.Mtime)
		}
		res.Attr = &patched
		return res, nil
	}
	return next(ctx, op)
}

// listMerged merges the classic listing with the layer's tree children,
// hiding blacked-out names and patching attrs-only entries.
func (l *Layer) listMerged(ctx context.Context, v *view, dir string, next fs.Next) (fs.Result, error) {
	if v.idx.isBlackedOut(dir) {
		return fs.Result{}, fs.ErrNotFound
	}
	res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
	if err != nil && !fs.IsNotFound(err) {
		return res, err
	}
	out := map[string]fs.Entry{}
	for _, e := range res.Entries {
		out[e.Name] = e
	}
	// Tree children shadow classic names.
	if v.treeRoot != "" {
		tres, terr := next(ctx, &fs.Op{Kind: fs.OpList, Path: v.treePathOf(l.serverAbs(dir))})
		if terr == nil {
			for _, e := range tres.Entries {
				out[e.Name] = e
			}
		} else if !fs.IsNotFound(terr) {
			return fs.Result{}, terr
		}
	}
	for name, e := range v.idx.childrenOf(dir) {
		switch e.kind {
		case ekBlackout:
			delete(out, name)
		case ekTree:
			if _, seen := out[name]; !seen {
				kind := fs.KindFile
				switch {
				case e.isDir:
					kind = fs.KindDir
				case e.isSymlink:
					kind = fs.KindSymlink
				}
				out[name] = fs.Entry{Name: name, Attr: fs.Attr{Kind: kind, Mode: e.mode}}
			}
		case ekAttrs:
			if cur, seen := out[name]; seen {
				patched := cur.Attr
				if e.patch.Mode != 0 {
					patched.Mode = e.patch.Mode
				}
				cur.Attr = patched
				out[name] = cur
			}
		}
	}
	entries := make([]fs.Entry, 0, len(out))
	for _, e := range out {
		entries = append(entries, e)
	}
	return fs.Result{Entries: entries}, nil
}

// createOp opens (or creates) a view path with write intent: tree-backed
// paths open their tree file directly, classic-backed paths materialize a
// copy first (content copied unless O_TRUNC discards it), fresh paths are
// created in the tree.
func (l *Layer) createOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p := op.Path
	res, e := l.resolve(v, p)
	write := hasWriteIntent(op.Flags) || op.Flags&uint32(syscall.O_CREAT) != 0
	if !write {
		// Plain read opens never materialize anything.
		switch res {
		case resBlackout:
			return fs.Result{}, fs.ErrNotFound
		case resTree:
			rop := *op
			rop.Path = v.treePathOf(l.serverAbs(p))
			return next(ctx, &rop)
		}
		return next(ctx, op)
	}

	switch res {
	case resBlackout:
		if op.Flags&uint32(syscall.O_CREAT) == 0 {
			return fs.Result{}, fs.ErrNotFound
		}
		// Recreate over a blackout: restamp the CAS base (the classic file
		// may still exist under the shadow), drop the marker, then create.
		unlock := l.mutLocks.lock(p)
		defer unlock()
		baseRev, hasBase, err := l.classicBase(ctx, v, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if err := l.writeCasMarker(ctx, v, p, baseRev, hasBase); err != nil {
			return fs.Result{}, err
		}
		if err := l.removeBlackoutMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		rop.Flags = op.Flags | uint32(syscall.O_CREAT|syscall.O_TRUNC)
		rres, rerr := next(ctx, &rop)
		if rerr != nil {
			return rres, rerr
		}
		v.idx.put(p, &indexEntry{kind: ekTree, mode: op.Mode & 0o777})
		return rres, nil

	case resTree:
		if e.isDir {
			return fs.Result{}, syscall.EISDIR
		}
		if op.Flags&uint32(syscall.O_EXCL) != 0 && op.Flags&uint32(syscall.O_CREAT) != 0 {
			return fs.Result{}, syscall.EEXIST
		}
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		rres, rerr := next(ctx, &rop)
		if rerr != nil {
			return rres, rerr
		}
		return rres, nil

	case resAttrs:
		// A metadata-only shadow still needs the content underneath.
		unlock := l.mutLocks.lock(p)
		defer unlock()
		attr, aerr := l.statClassic(ctx, next, p)
		if aerr != nil {
			return fs.Result{}, aerr
		}
		if err := l.materializeClassic(ctx, v, next, p, attr, op.Flags&uint32(syscall.O_TRUNC) != 0); err != nil {
			return fs.Result{}, err
		}
		if err := l.removeAttrsMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
		v.idx.put(p, &indexEntry{kind: ekTree, mode: op.Mode & 0o777})
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		rop.Flags = op.Flags &^ uint32(syscall.O_CREAT|syscall.O_EXCL)
		rres, rerr := next(ctx, &rop)
		if rerr != nil {
			return rres, rerr
		}
		return rres, nil
	}

	// Classic or fresh: O_EXCL against a classic file must fail without
	// materializing anything.
	attr, statErr := l.statClassic(ctx, next, p)
	if statErr != nil {
		return fs.Result{}, statErr
	}
	if attr != nil {
		if op.Flags&uint32(syscall.O_EXCL) != 0 && op.Flags&uint32(syscall.O_CREAT) != 0 {
			return fs.Result{}, syscall.EEXIST
		}
		if attr.IsDir() {
			return fs.Result{}, syscall.EISDIR
		}
	} else if op.Flags&uint32(syscall.O_CREAT) == 0 {
		return fs.Result{}, fs.ErrNotFound
	}
	unlock := l.mutLocks.lock(p)
	defer unlock()
	if attr != nil {
		if err := l.materializeClassic(ctx, v, next, p, attr, op.Flags&uint32(syscall.O_TRUNC) != 0); err != nil {
			return fs.Result{}, err
		}
	} else if err := l.writeCasMarker(ctx, v, p, 0, false); err != nil {
		return fs.Result{}, err
	}
	v.idx.put(p, &indexEntry{kind: ekTree, mode: op.Mode & 0o777})
	rop := *op
	rop.Path = v.treePathOf(l.serverAbs(p))
	rop.Flags = op.Flags &^ uint32(syscall.O_EXCL)
	rres, rerr := next(ctx, &rop)
	if rerr != nil {
		return rres, rerr
	}
	return rres, nil
}

// writeOp is the handleless write path: routes to the tree, materializing
// a classic base first when needed.
func (l *Layer) writeOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	res, _ := l.resolve(v, op.Path)
	switch res {
	case resBlackout:
		return fs.Result{}, fs.ErrNotFound
	case resTree, resAttrs:
		unlock := l.mutLocks.lock(op.Path)
		defer unlock()
		if res == resAttrs {
			attr, aerr := l.statClassic(ctx, next, op.Path)
			if aerr != nil {
				return fs.Result{}, aerr
			}
			if err := l.materializeClassic(ctx, v, next, op.Path, attr, false); err != nil {
				return fs.Result{}, err
			}
			if err := l.removeAttrsMarker(ctx, v, op.Path); err != nil {
				return fs.Result{}, err
			}
			v.idx.put(op.Path, &indexEntry{kind: ekTree})
		}
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(op.Path))
		return next(ctx, &rop)
	}
	// Classic base (or fresh): materialize then write.
	unlock := l.mutLocks.lock(op.Path)
	defer unlock()
	attr, aerr := l.statClassic(ctx, next, op.Path)
	if aerr != nil {
		return fs.Result{}, aerr
	}
	if attr != nil {
		if err := l.materializeClassic(ctx, v, next, op.Path, attr, false); err != nil {
			return fs.Result{}, err
		}
	} else if err := l.writeCasMarker(ctx, v, op.Path, 0, false); err != nil {
		return fs.Result{}, err
	}
	v.idx.put(op.Path, &indexEntry{kind: ekTree})
	rop := *op
	rop.Path = v.treePathOf(l.serverAbs(op.Path))
	return next(ctx, &rop)
}

func (l *Layer) mkdirOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p := op.Path
	res, _ := l.resolve(v, p)
	if res == resBlackout {
		// mkdir over a blackout recreates the directory in the layer.
		unlock := l.mutLocks.lock(p)
		defer unlock()
		baseRev, hasBase, err := l.classicBase(ctx, v, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if err := l.writeCasMarker(ctx, v, p, baseRev, hasBase); err != nil {
			return fs.Result{}, err
		}
		if err := l.removeBlackoutMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
		if err := l.io.EnsureParent(ctx, parentTreeDir(v, l.serverAbs(p))); err != nil {
			return fs.Result{}, err
		}
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		rres, rerr := next(ctx, &rop)
		if rerr != nil {
			return rres, rerr
		}
		mode := op.Mode & 0o777
		if mode == 0 {
			mode = 0o755
		}
		v.idx.put(p, &indexEntry{kind: ekTree, isDir: true, mode: mode})
		return rres, nil
	}
	if res == resTree || res == resAttrs {
		return fs.Result{}, syscall.EEXIST
	}
	// Reject creation over an existing classic entry (the classic mkdir
	// path would mutate the base view).
	if attr, err := l.statClassic(ctx, next, p); err == nil && attr != nil {
		return fs.Result{}, syscall.EEXIST
	}
	unlock := l.mutLocks.lock(p)
	defer unlock()
	if err := l.writeCasMarker(ctx, v, p, 0, false); err != nil {
		return fs.Result{}, err
	}
	if err := l.io.EnsureParent(ctx, parentTreeDir(v, l.serverAbs(p))); err != nil {
		return fs.Result{}, err
	}
	rop := *op
	rop.Path = v.treePathOf(l.serverAbs(p))
	rres, rerr := next(ctx, &rop)
	if rerr != nil {
		return rres, rerr
	}
	mode := op.Mode & 0o777
	if mode == 0 {
		mode = 0o755
	}
	v.idx.put(p, &indexEntry{kind: ekTree, isDir: true, mode: mode})
	return rres, nil
}

func (l *Layer) symlinkOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p := op.Path
	res, _ := l.resolve(v, p)
	if res == resTree || res == resAttrs {
		return fs.Result{}, syscall.EEXIST
	}
	if res == resBlackout {
		unlock := l.mutLocks.lock(p)
		defer unlock()
		baseRev, hasBase, err := l.classicBase(ctx, v, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if err := l.writeCasMarker(ctx, v, p, baseRev, hasBase); err != nil {
			return fs.Result{}, err
		}
		if err := l.removeBlackoutMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
	} else if attr, err := l.statClassic(ctx, next, p); err == nil && attr != nil {
		return fs.Result{}, syscall.EEXIST
	} else {
		unlock := l.mutLocks.lock(p)
		defer unlock()
		if err := l.writeCasMarker(ctx, v, p, 0, false); err != nil {
			return fs.Result{}, err
		}
	}
	if err := l.io.EnsureParent(ctx, parentTreeDir(v, l.serverAbs(p))); err != nil {
		return fs.Result{}, err
	}
	rop := *op
	rop.Path = v.treePathOf(l.serverAbs(p))
	rres, rerr := next(ctx, &rop)
	if rerr != nil {
		return rres, rerr
	}
	v.idx.put(p, &indexEntry{kind: ekTree, isSymlink: true, mode: 0o777})
	return rres, nil
}

func (l *Layer) mknodOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p := op.Path
	res, _ := l.resolve(v, p)
	if res == resTree || res == resAttrs {
		return fs.Result{}, syscall.EEXIST
	}
	if res == resBlackout {
		unlock := l.mutLocks.lock(p)
		defer unlock()
		baseRev, hasBase, err := l.classicBase(ctx, v, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if err := l.writeCasMarker(ctx, v, p, baseRev, hasBase); err != nil {
			return fs.Result{}, err
		}
		if err := l.removeBlackoutMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
	} else if attr, err := l.statClassic(ctx, next, p); err == nil && attr != nil {
		return fs.Result{}, syscall.EEXIST
	} else {
		unlock := l.mutLocks.lock(p)
		defer unlock()
		if err := l.writeCasMarker(ctx, v, p, 0, false); err != nil {
			return fs.Result{}, err
		}
	}
	if err := l.io.EnsureParent(ctx, parentTreeDir(v, l.serverAbs(p))); err != nil {
		return fs.Result{}, err
	}
	rop := *op
	rop.Path = v.treePathOf(l.serverAbs(p))
	rres, rerr := next(ctx, &rop)
	if rerr != nil {
		return rres, rerr
	}
	v.idx.put(p, &indexEntry{kind: ekTree, mode: op.Mode & 0o777})
	return rres, nil
}

// hasWriteIntent reports an open that can produce writes.
func hasWriteIntent(flags uint32) bool {
	return flags&uint32(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT) != 0
}
