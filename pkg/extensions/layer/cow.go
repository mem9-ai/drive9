// Copy-on-write materialization and the namespace mutations: every change
// lands under the layer's tree with its markers, never in the classic view.
package layer

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// pumpChunkBytes bounds the CoW copy buffer.
const pumpChunkBytes = 1 << 20

// parentTreeDir is the tree directory that must exist before a tree
// mkdir/symlink/mknod or rename destination lands in it (server
// mkdir/symlink/rename do not auto-create parents, unlike create).
func parentTreeDir(v *view, serverPath string) string {
	return v.treePathOf(pathutil.ParentPath(serverPath))
}

// statClassic stats the classic view (through the chain, so the read cache
// serves it); nil, nil when the path is absent.
func (l *Layer) statClassic(ctx context.Context, next fs.Next, p string) (*fs.Attr, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: p})
	if fs.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return res.Attr, nil
}

// classicBase resolves the commit-CAS anchor of a path: the classic
// revision when the base exists, absent otherwise.
func (l *Layer) classicBase(ctx context.Context, v *view, next fs.Next, p string) (int64, bool, error) {
	attr, err := l.statClassic(ctx, next, p)
	if err != nil || attr == nil {
		return 0, false, err
	}
	return attr.Rev, true, nil
}

// writeCasMarker stamps the commit-CAS anchor for a tree path BEFORE its
// content materializes (marker-first is crash-coherent: an unstamped tree
// file applies unconstrained at commit).
func (l *Layer) writeCasMarker(ctx context.Context, v *view, p string, baseRev int64, hasBase bool) error {
	content := "0"
	if hasBase {
		content = strconv.FormatInt(baseRev, 10)
	}
	return l.io.Write(ctx, v.casRoot+l.serverAbs(p), []byte(content))
}

// moveCasMarker restamps the anchor after a rename (destination CAS).
func (l *Layer) moveCasMarker(ctx context.Context, v *view, oldP, newP string, baseRev int64, hasBase bool) error {
	if err := l.removeMarkerAt(ctx, v.casRoot, oldP); err != nil {
		return err
	}
	return l.writeCasMarker(ctx, v, newP, baseRev, hasBase)
}

// writeBlackoutMarker hides a path (and, for dirs, its subtree) from the
// view. File and dir whiteouts live in separate roots (see view.go).
func (l *Layer) writeBlackoutMarker(ctx context.Context, v *view, p string, info blackoutInfo) error {
	root := v.blackRoot
	if info.isDir {
		root = v.blackDirRoot
	}
	return l.io.Write(ctx, root+l.serverAbs(p), encodeBlackout(info))
}

// removeBlackoutMarker clears either flavor of whiteout at p (recreates
// do not care which root held it).
func (l *Layer) removeBlackoutMarker(ctx context.Context, v *view, p string) error {
	if err := l.removeMarkerAt(ctx, v.blackRoot, p); err != nil {
		return err
	}
	return l.removeMarkerAt(ctx, v.blackDirRoot, p)
}

func (l *Layer) removeAttrsMarker(ctx context.Context, v *view, p string) error {
	return l.removeMarkerAt(ctx, v.attrsRoot, p)
}

func (l *Layer) removeMarkerAt(ctx context.Context, root, p string) error {
	if root == "" {
		return nil
	}
	if err := l.io.Delete(ctx, root+l.serverAbs(p)); err != nil && !fs.IsNotFound(err) {
		return err
	}
	return nil
}

// materializeClassic CoWs a classic base into the tree: CAS marker first,
// then the content pumped through the chain (bounded memory; the write
// plane owns buffering and durability). truncate skips the content pump —
// the caller's own O_TRUNC create materializes the empty image.
func (l *Layer) materializeClassic(ctx context.Context, v *view, next fs.Next, p string, attr *fs.Attr, truncate bool) error {
	if attr == nil {
		return nil
	}
	if attr.IsDir() {
		return syscall.EISDIR
	}
	if err := l.writeCasMarker(ctx, v, p, attr.Rev, true); err != nil {
		return err
	}
	if truncate {
		return nil
	}
	return l.pumpClassic(ctx, v, next, p, p, attr)
}

// pumpClassic streams the classic file's content into the tree shadow of
// dstP (== p for a plain CoW, the rename destination otherwise) through
// the ordinary write pipeline.
func (l *Layer) pumpClassic(ctx context.Context, v *view, next fs.Next, p, dstP string, attr *fs.Attr) error {
	mode := attr.Mode & 0o777
	if mode == 0 {
		mode = 0o644
	}
	treePath := v.treePathOf(l.serverAbs(dstP))
	res, err := next(ctx, &fs.Op{Kind: fs.OpCreate, Path: treePath, Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC), Mode: mode})
	if err != nil {
		return err
	}
	h := res.Handle
	if h == nil {
		return fs.ErrUnsupported
	}
	rc, err := l.io.ReadStream(ctx, l.serverAbs(p))
	if err != nil {
		_ = h.Release(ctx)
		return err
	}
	var werr error
	var off int64
	buf := make([]byte, pumpChunkBytes)
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			if werr = h.Write(ctx, off, buf[:n]); werr != nil {
				break
			}
			off += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			werr = rerr
			break
		}
	}
	_ = rc.Close()
	flushErr := h.Flush(ctx)
	relErr := h.Release(ctx)
	if werr != nil {
		return werr
	}
	if flushErr != nil {
		return flushErr
	}
	return relErr
}

// copyClassicDir copies a classic directory subtree into the tree shadow
// (rename of a classic directory unfolds into copy + blackout).
func (l *Layer) copyClassicDir(ctx context.Context, v *view, next fs.Next, srcP, dstP string) error {
	mkdirTree := func(treeDir string, mode uint32) error {
		if res, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: treeDir}); err == nil && res.Attr != nil && res.Attr.IsDir() {
			return nil
		}
		if err := l.io.EnsureParent(ctx, pathutil.ParentPath(treeDir)); err != nil {
			return err
		}
		_, err := next(ctx, &fs.Op{Kind: fs.OpMkdir, Path: treeDir, Mode: mode})
		if err != nil && fs.IsNotFound(err) {
			err = nil
		}
		return err
	}
	var copy func(dir string) error
	copy = func(dir string) error {
		res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
		if err != nil {
			if fs.IsNotFound(err) {
				return nil
			}
			return err
		}
		for _, e := range res.Entries {
			child := pathutil.JoinPath(dir, e.Name)
			if e.Attr.IsDir() {
				mode := e.Attr.Mode & 0o777
				if mode == 0 {
					mode = 0o755
				}
				if err := mkdirTree(v.treePathOf(l.serverAbs(child)), mode); err != nil {
					return err
				}
				v.idx.put(child, &indexEntry{kind: ekTree, isDir: true, mode: mode})
				if err := copy(child); err != nil {
					return err
				}
				continue
			}
			dstChild := dstP + child[len(srcP):]
			if err := l.pumpClassic(ctx, v, next, child, dstChild, &e.Attr); err != nil {
				return err
			}
			v.idx.put(child, &indexEntry{kind: ekTree, mode: e.Attr.Mode & 0o777})
		}
		return nil
	}
	mode := uint32(0o755)
	if err := mkdirTree(v.treePathOf(l.serverAbs(dstP)), mode); err != nil {
		return err
	}
	return copy(srcP)
}

// unlinkOp hides a path: blackout marker first, then the tree entry goes.
// POSIX unlink-while-open is served by the write plane's own deferred
// machinery on the tree path.
func (l *Layer) unlinkOp(ctx context.Context, v *view, p string, isDir bool, next fs.Next) (fs.Result, error) {
	res, e := l.resolve(v, p)
	if res == resBlackout {
		return fs.Result{}, fs.ErrNotFound
	}
	if res == resTree && e.isDir != isDir {
		if isDir {
			return fs.Result{}, syscall.ENOTDIR
		}
		return fs.Result{}, syscall.EISDIR
	}
	if isDir && res != resBlackout {
		// The merged view must be empty before a directory goes away.
		lres, lerr := l.listMerged(ctx, v, p, next)
		if lerr != nil && !fs.IsNotFound(lerr) {
			return fs.Result{}, lerr
		}
		if len(lres.Entries) > 0 {
			return fs.Result{}, syscall.ENOTEMPTY
		}
	}
	unlock := l.mutLocks.lock(p)
	defer unlock()
	attr, err := l.statClassic(ctx, next, p)
	if err != nil {
		return fs.Result{}, err
	}
	if attr == nil && res == resClassic {
		// Nothing in the view: unlink/rmdir of a missing path is ENOENT,
		// not a fresh blackout.
		return fs.Result{}, fs.ErrNotFound
	}
	info := blackoutInfo{isDir: isDir}
	if attr != nil {
		info.baseRev, info.hasBase = attr.Rev, true
	}
	if err := l.writeBlackoutMarker(ctx, v, p, info); err != nil {
		return fs.Result{}, err
	}
	if res == resTree {
		op := &fs.Op{Kind: fs.OpUnlink, Path: v.treePathOf(l.serverAbs(p))}
		if isDir {
			op.Kind = fs.OpRmdir
		}
		if _, err := next(ctx, op); err != nil && !fs.IsNotFound(err) {
			return fs.Result{}, err
		}
	}
	if res == resAttrs {
		if err := l.removeAttrsMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
	}
	v.idx.put(p, &indexEntry{kind: ekBlackout, blackout: info})
	return fs.Result{}, nil
}

// renameOp moves a view path: tree sources rename atomically server-side;
// classic sources copy into the tree and blackout the origin. The
// destination keeps a fresh CAS anchor.
func (l *Layer) renameOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p, q := op.Path, op.Path2
	resP, eP := l.resolve(v, p)
	if resP == resBlackout {
		return fs.Result{}, fs.ErrNotFound
	}
	if resP == resTree && eP.isDir {
		return l.renameTreeEntry(ctx, v, op, next, true)
	}
	var attrP *fs.Attr
	switch resP {
	case resTree:
		// The shared tail records the destination's tree entry; the index
		// already carries the source mode.
		attrP = &fs.Attr{Mode: eP.mode}
	case resClassic:
		st, err := l.statClassic(ctx, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if st == nil {
			return fs.Result{}, fs.ErrNotFound
		}
		if st.IsDir() {
			return l.renameClassicDir(ctx, v, op, next, st)
		}
		attrP = st
	case resAttrs:
		st, err := l.statClassic(ctx, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if st == nil {
			return fs.Result{}, fs.ErrNotFound
		}
		if eP.patch.Mode != 0 {
			st.Mode = eP.patch.Mode
		}
		attrP = st
	}
	// File or symlink: the destination's classic anchor is stamped fresh
	// (an existing view entry at q is simply shadowed over).
	unlockPair := l.mutLocks.lockPair(p, q)
	defer unlockPair()
	baseQ, hasQ, err := l.classicBase(ctx, v, next, q)
	if err != nil {
		return fs.Result{}, err
	}
	if resP == resTree {
		if err := l.retreeRename(ctx, v, next, p, q); err != nil {
			return fs.Result{}, err
		}
	} else {
		if resP == resAttrs {
			if err := l.removeAttrsMarker(ctx, v, p); err != nil {
				return fs.Result{}, err
			}
		}
		if attrP.Mode&0o777 == 0 {
			attrP.Mode = 0o644
		}
		if err := l.pumpClassic(ctx, v, next, p, q, attrP); err != nil {
			return fs.Result{}, err
		}
	}
	// Index first (subtree move, then the tree entry at the destination),
	// so finishRename's blackout of the origin is not itself remapped.
	v.idx.remapPrefix(p, q)
	v.idx.put(q, &indexEntry{kind: ekTree, mode: attrP.Mode & 0o777})
	if err := l.finishRename(ctx, v, next, p, q, baseQ, hasQ, false); err != nil {
		return fs.Result{}, err
	}
	return fs.Result{}, nil
}

// renameTreeEntry renames a tree directory (server-side atomic rename of
// the tree path, plus bookkeeping).
func (l *Layer) renameTreeEntry(ctx context.Context, v *view, op *fs.Op, next fs.Next, isDir bool) (fs.Result, error) {
	p, q := op.Path, op.Path2
	unlockPair := l.mutLocks.lockPair(p, q)
	defer unlockPair()
	baseQ, hasQ, err := l.classicBase(ctx, v, next, q)
	if err != nil {
		return fs.Result{}, err
	}
	if err := l.retreeRename(ctx, v, next, p, q); err != nil {
		return fs.Result{}, err
	}
	v.idx.remapPrefix(p, q)
	if err := l.finishRename(ctx, v, next, p, q, baseQ, hasQ, isDir); err != nil {
		return fs.Result{}, err
	}
	return fs.Result{}, nil
}

// retreeRename performs the server-side atomic rename inside the tree.
func (l *Layer) retreeRename(ctx context.Context, v *view, next fs.Next, p, q string) error {
	if err := l.io.EnsureParent(ctx, parentTreeDir(v, l.serverAbs(q))); err != nil {
		return err
	}
	op := &fs.Op{Kind: fs.OpRename, Path: v.treePathOf(l.serverAbs(p)), Path2: v.treePathOf(l.serverAbs(q))}
	_, err := next(ctx, op)
	return err
}

// finishRename applies the shared rename bookkeeping: blackout the origin
// when a classic base exists there, clear the destination's blackout, restamp
// the destination CAS, and move any attrs patch.
func (l *Layer) finishRename(ctx context.Context, v *view, next fs.Next, p, q string, baseQ int64, hasQ bool, isDir bool) error {
	attrP, err := l.statClassic(ctx, next, p)
	if err != nil {
		return err
	}
	if attrP != nil {
		info := blackoutInfo{isDir: isDir || attrP.IsDir(), baseRev: attrP.Rev, hasBase: true}
		if err := l.writeBlackoutMarker(ctx, v, p, info); err != nil {
			return err
		}
		v.idx.put(p, &indexEntry{kind: ekBlackout, blackout: info})
	} else {
		v.idx.remove(p)
		if err := l.removeMarkerAt(ctx, v.casRoot, p); err != nil {
			return err
		}
		if err := l.removeMarkerAt(ctx, v.attrsRoot, p); err != nil {
			return err
		}
	}
	if err := l.moveCasMarker(ctx, v, p, q, baseQ, hasQ); err != nil {
		return err
	}
	if err := l.removeBlackoutMarker(ctx, v, q); err != nil {
		return err
	}
	v.idx.removeIfKind(q, ekBlackout)
	// Move an attrs-only patch along with the content.
	if content, rerr := l.io.Read(ctx, v.attrsRoot+l.serverAbs(p)); rerr == nil {
		if werr := l.io.Write(ctx, v.attrsRoot+l.serverAbs(q), content); werr != nil {
			return werr
		}
		_ = l.removeAttrsMarker(ctx, v, p)
	}
	return nil
}

// renameClassicDir copies a classic directory subtree to the tree and
// blackouts the origin.
func (l *Layer) renameClassicDir(ctx context.Context, v *view, op *fs.Op, next fs.Next, st *fs.Attr) (fs.Result, error) {
	p, q := op.Path, op.Path2
	unlockPair := l.mutLocks.lockPair(p, q)
	defer unlockPair()
	if err := l.copyClassicDir(ctx, v, next, p, q); err != nil {
		return fs.Result{}, err
	}
	// copyClassicDir indexed the copied children under their source paths;
	// remap them below the destination before the blackout lands.
	v.idx.remapPrefix(p, q)
	v.idx.put(q, &indexEntry{kind: ekTree, isDir: true, mode: st.Mode & 0o777})
	if err := l.finishRename(ctx, v, next, p, q, 0, false, true); err != nil {
		return fs.Result{}, err
	}
	return fs.Result{}, nil
}

// linkOp hard-links within the tree, materializing a classic source first.
func (l *Layer) linkOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p, q := op.Path, op.Path2
	resP, _ := l.resolve(v, p)
	if resP == resBlackout {
		return fs.Result{}, fs.ErrNotFound
	}
	unlockPair := l.mutLocks.lockPair(p, q)
	defer unlockPair()
	if resP != resTree {
		attr, err := l.statClassic(ctx, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if attr == nil {
			return fs.Result{}, fs.ErrNotFound
		}
		if attr.IsDir() {
			return fs.Result{}, syscall.EPERM
		}
		if err := l.materializeClassic(ctx, v, next, p, attr, false); err != nil {
			return fs.Result{}, err
		}
		v.idx.put(p, &indexEntry{kind: ekTree, mode: attr.Mode & 0o777})
	}
	baseQ, hasQ, err := l.classicBase(ctx, v, next, q)
	if err != nil {
		return fs.Result{}, err
	}
	if err := l.writeCasMarker(ctx, v, q, baseQ, hasQ); err != nil {
		return fs.Result{}, err
	}
	if err := l.removeBlackoutMarker(ctx, v, q); err != nil {
		return fs.Result{}, err
	}
	rop := *op
	rop.Path = v.treePathOf(l.serverAbs(p))
	rop.Path2 = v.treePathOf(l.serverAbs(q))
	rres, rerr := next(ctx, &rop)
	if rerr != nil {
		return rres, rerr
	}
	v.idx.remove(q)
	v.idx.put(q, &indexEntry{kind: ekTree, mode: 0o644})
	return rres, nil
}

// setAttrOp routes attribute changes: tree paths take them directly,
// classic paths take an attrs marker for mode/mtime and a full CoW for
// truncation.
func (l *Layer) setAttrOp(ctx context.Context, v *view, op *fs.Op, next fs.Next) (fs.Result, error) {
	p := op.Path
	res, e := l.resolve(v, p)
	switch res {
	case resBlackout:
		return fs.Result{}, fs.ErrNotFound
	case resTree:
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		rres, rerr := next(ctx, &rop)
		if rerr == nil && (op.HasMode || op.Mode != 0) {
			v.idx.put(p, &indexEntry{kind: ekTree, isDir: e.isDir, isSymlink: e.isSymlink, mode: op.Mode & 0o777})
		}
		return rres, rerr
	case resAttrs:
		if op.Size < 0 {
			patch := e.patch
			if op.HasMode || op.Mode != 0 {
				patch.Mode = op.Mode & 0o7777
				patch.ModeSet = true
			}
			if err := writeAttrsMarker(ctx, l.io, v, l.serverAbs(p), patch); err != nil {
				return fs.Result{}, err
			}
			v.idx.put(p, &indexEntry{kind: ekAttrs, patch: patch})
			return l.attrOp(ctx, v, &fs.Op{Kind: fs.OpGetAttr, Path: p}, next)
		}
		// Truncation needs the content: materialize, then truncate.
		unlock := l.mutLocks.lock(p)
		defer unlock()
		attr, err := l.statClassic(ctx, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if err := l.materializeClassic(ctx, v, next, p, attr, op.Size == 0); err != nil {
			return fs.Result{}, err
		}
		if op.Size == 0 {
			if err := l.ensureTreeFile(ctx, v, next, p, attr); err != nil {
				return fs.Result{}, err
			}
		}
		if err := l.removeAttrsMarker(ctx, v, p); err != nil {
			return fs.Result{}, err
		}
		v.idx.put(p, &indexEntry{kind: ekTree})
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		return next(ctx, &rop)
	}
	// Classic base.
	if op.Size < 0 && (op.HasMode || op.Mode != 0 || !op.Mtime.IsZero() || op.HasSetUID || op.HasSetGID) {
		patch := attrsPatch{}
		if op.HasMode || op.Mode != 0 {
			patch.Mode = op.Mode & 0o7777
			patch.ModeSet = true
		}
		if !op.Mtime.IsZero() {
			patch.Mtime = op.Mtime.Unix()
		}
		unlock := l.mutLocks.lock(p)
		defer unlock()
		if err := writeAttrsMarker(ctx, l.io, v, l.serverAbs(p), patch); err != nil {
			return fs.Result{}, err
		}
		v.idx.put(p, &indexEntry{kind: ekAttrs, patch: patch})
		return l.attrOp(ctx, v, &fs.Op{Kind: fs.OpGetAttr, Path: p}, next)
	}
	if op.Size >= 0 {
		unlock := l.mutLocks.lock(p)
		defer unlock()
		attr, err := l.statClassic(ctx, next, p)
		if err != nil {
			return fs.Result{}, err
		}
		if attr == nil {
			return fs.Result{}, fs.ErrNotFound
		}
		if err := l.materializeClassic(ctx, v, next, p, attr, op.Size == 0); err != nil {
			return fs.Result{}, err
		}
		if op.Size == 0 {
			if err := l.ensureTreeFile(ctx, v, next, p, attr); err != nil {
				return fs.Result{}, err
			}
		}
		v.idx.put(p, &indexEntry{kind: ekTree, mode: attr.Mode & 0o777})
		rop := *op
		rop.Path = v.treePathOf(l.serverAbs(p))
		return next(ctx, &rop)
	}
	// Owner-only changes are mount-local at the facade; nothing lands here.
	return fs.Result{}, fs.ErrUnsupported
}

// ensureTreeFile materializes an empty tree shadow for a zero-length
// truncate: materializeClassic skips the pump when the content is
// discarded, but a SetAttr still needs a tree file to truncate.
func (l *Layer) ensureTreeFile(ctx context.Context, v *view, next fs.Next, p string, attr *fs.Attr) error {
	treePath := v.treePathOf(l.serverAbs(p))
	if res, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: treePath}); err == nil && res.Attr != nil {
		return nil
	} else if !fs.IsNotFound(err) {
		return err
	}
	mode := uint32(0o644)
	if attr != nil && attr.Mode&0o777 != 0 {
		mode = attr.Mode & 0o777
	}
	if err := l.io.EnsureParent(ctx, parentTreeDir(v, l.serverAbs(p))); err != nil {
		return err
	}
	res, err := next(ctx, &fs.Op{Kind: fs.OpCreate, Path: treePath, Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC), Mode: mode})
	if err != nil {
		return err
	}
	if h := res.Handle; h != nil {
		if err := h.Flush(ctx); err != nil {
			_ = h.Release(ctx)
			return err
		}
		return h.Release(ctx)
	}
	return nil
}

// writeAttrsMarker stores a metadata-only patch marker.
func writeAttrsMarker(ctx context.Context, dio layerIO, v *view, serverPath string, patch attrsPatch) error {
	content, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return dio.Write(ctx, v.attrsRoot+serverPath, content)
}

func unixToTime(sec int64) time.Time {
	if sec <= 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}
