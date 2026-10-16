// Overlay persistence through the client's FSLayer entry API.
package layer

import (
	"context"
	"fmt"
	"io"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func (l *Layer) upsertEntry(ctx context.Context, req client.FSLayerEntryRequest) error {
	if l.mu.RLock(); l.abandoned {
		l.mu.RUnlock()
		return errLayerRolledBack
	} else {
		l.mu.RUnlock()
	}
	_, err := l.cl.UpsertFSLayerEntry(ctx, l.layerRef, req)
	return err
}

func (l *Layer) upsertFile(ctx context.Context, path string, data []byte, baseRev int64, mode uint32) error {
	unlock := l.mutLocks.lock(path)
	defer unlock()
	return l.upsertFileLocked(ctx, path, data, baseRev, mode)
}

// upsertFileLocked is upsertFile without the per-path lock — callers that
// already hold the mutation locks (rename) use it directly.
func (l *Layer) upsertFileLocked(ctx context.Context, path string, data []byte, baseRev int64, mode uint32) error {
	if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{
		Path:         l.remotePath(path),
		Op:           "upsert",
		Kind:         "file",
		Content:      data,
		Mode:         mode & 0o777,
		BaseRevision: baseRev,
		SizeBytes:    int64(len(data)),
	}); err != nil {
		return err
	}
	l.mu.Lock()
	if !l.abandoned {
		delete(l.remoteSizes, path)
		l.files[path] = mode & 0o777
		l.contents[path] = append([]byte(nil), data...)
		delete(l.whiteouts, path)
		delete(l.dirs, path)
		delete(l.symlinks, path)
	}
	l.mu.Unlock()
	l.emit(fs.Event{Kind: fs.EventInvalidated, Path: path})
	return nil
}

// renameLayer moves a path within the layer. Symlinks re-upsert as symlink
// entries; directories (overlay or base) move as namespace entries with no
// payload; files move their image — overlay content directly, base content
// fetched once — with the destination's base revision as the upsert CAS.
func (l *Layer) renameLayer(ctx context.Context, oldPath, newPath string) error {
	oldRemote, newRemote := l.remotePath(oldPath), l.remotePath(newPath)
	if kind, mode, target, _, ok := l.lookup(oldPath); ok && kind == "symlink" {
		if mode == 0 {
			mode = uint32(syscall.S_IFLNK) | 0o777
		}
		if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{
			Path: newRemote, Op: "symlink", Kind: "symlink",
			ContentText: target, SizeBytes: int64(len(target)), Mode: mode,
		}); err != nil {
			return err
		}
		l.markLayerSymlink(newPath, target, mode)
		return l.whiteoutPath(ctx, oldPath, "file")
	}

	kind, mode, _, content, ok := l.lookup(oldPath)
	if ok && kind == "whiteout" {
		return fs.ErrNotFound
	}
	if ok && kind == "dir" || !ok && l.remotePathIsDir(ctx, oldRemote) {
		dirMode := mode
		if !ok || dirMode == 0 {
			dirMode = 0o755
			if st, err := l.cl.StatCtx(ctx, oldRemote); err == nil {
				dirMode = st.Mode & 0o777
			}
		}
		if st, err := l.cl.StatCtx(ctx, newRemote); err == nil {
			if st.IsDir {
				return fmt.Errorf("layer: rename target %s is a directory", newPath)
			}
			return fmt.Errorf("layer: rename target %s exists", newPath)
		} else if !client.IsNotFound(err) {
			return err
		}
		if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{
			Path: oldRemote, Op: "rename", Kind: "dir",
			ContentText: newRemote, Mode: dirMode,
		}); err != nil {
			return err
		}
		l.markLayerDir(newPath, dirMode)
		l.markLayerWhiteout(oldPath)
		return nil
	}

	// The uploaded layer image outranks the base object: hydrate it
	// first, or the rename copies the pre-truncation base and
	// resurrects the tail.
	if err := l.hydrateRemote(ctx, oldPath); err != nil {
		return err
	}
	if _, _, _, hydrated, hok := l.lookup(oldPath); hok {
		content, ok = hydrated, true
	}
	data, baseMode := content, mode
	if !ok || len(content) == 0 {
		st, err := l.cl.StatCtx(ctx, oldRemote)
		if err != nil {
			return err
		}
		if data, err = l.cl.ReadCtx(ctx, oldRemote); err != nil {
			return err
		}
		baseMode = st.Mode & 0o777
	}
	targetBaseRev := int64(0)
	if st, err := l.cl.StatCtx(ctx, newRemote); err == nil {
		if st.IsDir {
			return fmt.Errorf("layer: rename target %s is a directory", newPath)
		}
		targetBaseRev = st.Revision
	} else if !client.IsNotFound(err) {
		return err
	}
	if err := l.upsertFileLocked(ctx, newPath, data, targetBaseRev, baseMode); err != nil {
		return err
	}
	return l.whiteoutPath(ctx, oldPath, "file")
}

// remotePathIsDir answers whether the base object at remote is a directory.
func (l *Layer) remotePathIsDir(ctx context.Context, remote string) bool {
	st, err := l.cl.StatCtx(ctx, remote)
	return err == nil && st != nil && st.IsDir
}

func (l *Layer) whiteoutPath(ctx context.Context, path, kind string) error {
	if err := l.upsertEntry(ctx, client.FSLayerEntryRequest{Path: l.remotePath(path), Op: "whiteout", Kind: kind}); err != nil {
		return err
	}
	l.markLayerWhiteout(path)
	return nil
}

// truncateFile streams a truncate of an overlay file (v1
// applyLayerLargeTruncate + remote_upload.truncateReaderAt): only the kept
// prefix is read, and the zero extension is synthesized by the reader — a
// multi-gigabyte truncate never materializes in memory.
// baseRangeReader lazily serves the retained base prefix of a large
// truncation: the kept bytes stream from the base object instead of being
// zero-filled (a nil kept-prefix above the inline limit corrupted the
// visible prefix with zeros — review).
type baseRangeReader func(ctx context.Context, off, length int64) (io.Reader, error)

func (l *Layer) truncateFile(ctx context.Context, path string, content []byte, newSize int64, mode uint32) error {
	return l.truncateFileWithBase(ctx, path, content, newSize, mode, nil)
}

// truncateFileWithBase is truncateFile with a lazy reader for the retained
// base prefix, used when the kept range exceeds the inline limit.
func (l *Layer) truncateFileWithBase(ctx context.Context, path string, content []byte, newSize int64, mode uint32, base baseRangeReader) error {
	unlock := l.mutLocks.lock(path)
	defer unlock()
	kept := content
	if int64(len(kept)) > newSize {
		kept = kept[:newSize]
	}
	m := mode
	if m == 0 {
		m = 0o644
	}
	if newSize <= l.inlineMax() {
		img := make([]byte, newSize)
		copy(img, kept)
		return l.upsertFileLocked(ctx, path, img, 0, m)
	}
	// Above the inline limit the retained prefix must stream: inline bytes
	// first, then the base object's range, zeros only for the extension.
	keptLen := int64(len(kept))
	if base != nil && kept == nil {
		keptLen = newSize // retain the whole requested size from the base
	}
	if _, err := l.cl.UploadFSLayerFile(ctx, l.layerRef, l.remotePath(path), &truncateReaderAt{kept: kept, keptLen: keptLen, size: newSize, base: base, ctx: ctx}, newSize, 0, m, true); err != nil {
		return err
	}
	l.mu.Lock()
	if !l.abandoned {
		l.files[path] = m & 0o777
		// The uploaded image is the layer's stored object; a materialized
		// prefix here (or nil) would publish a partial/empty file as the
		// complete overlay image — GetAttr served 0 and reads returned
		// empty. Record the logical size and stream the object on read.
		l.remoteSizes[path] = newSize
		delete(l.contents, path)
		delete(l.whiteouts, path)
	}
	l.mu.Unlock()
	l.emit(fs.Event{Kind: fs.EventInvalidated, Path: path})
	return nil
}

// truncateReaderAt serves a truncated image without materializing the
// zero extension (ports v1 remoteTruncateReaderAt).
type truncateReaderAt struct {
	kept    []byte
	keptLen int64 // logical length of the retained prefix (kept bytes + streamed base)
	size    int64
	pos     int64
	base    baseRangeReader
	ctx     context.Context
}

func (r *truncateReaderAt) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	n := len(p)
	if r.pos+int64(n) > r.size {
		n = int(r.size - r.pos)
	}
	kept := int64(0)
	if r.pos < int64(len(r.kept)) {
		kept = int64(copy(p[:n], r.kept[r.pos:]))
	}
	// The remainder of the retained prefix streams from the base object;
	// only bytes beyond the prefix read as zeros (the extension).
	if kept < int64(n) && r.base != nil && r.pos+kept < r.keptLen {
		want := int64(n) - kept
		if end := r.keptLen - (r.pos + kept); want > end {
			want = end
		}
		if want > 0 {
			rc, err := r.base(r.ctx, r.pos+kept, want)
			if err != nil {
				return int(kept), err
			}
			m, rerr := io.ReadFull(rc, p[kept:kept+want])
			if c, ok := rc.(io.Closer); ok {
				_ = c.Close()
			}
			if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
				return int(kept) + m, rerr
			}
			kept += int64(m)
		}
	}
	for i := kept; i < int64(n); i++ {
		p[i] = 0
	}
	r.pos += int64(n)
	return n, nil
}
