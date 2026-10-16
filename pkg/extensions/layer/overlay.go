// The in-memory overlay: whiteouts, file modes, dirs, and symlinks, plus
// the lookup that resolves a path against it.
package layer

import (
	"context"
	"io"
	"strings"
	"syscall"
)

// markLayerWhiteout / markLayerFileMode / markLayerDir / markLayerSymlink
// mutate the overlay, including the abandoned guard: a racing write that
// completed on the server just before a rollback cleared the overlay must
// not re-populate it.
func (l *Layer) markLayerWhiteout(p string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.abandoned {
		return
	}
	l.whiteouts[p] = struct{}{}
	delete(l.files, p)
	delete(l.dirs, p)
	delete(l.symlinks, p)
	delete(l.contents, p)
	delete(l.remoteSizes, p)
}

func (l *Layer) markLayerFileMode(p string, mode uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.abandoned {
		return
	}
	l.files[p] = mode & 0o777
	delete(l.whiteouts, p)
	delete(l.dirs, p)
	delete(l.symlinks, p)
}

// markLayerBaseFileMode records a mode-only override for a file the layer
// has no content image of: the base object stays the content source, so
// the overlay must not answer reads with an empty image.
func (l *Layer) markLayerBaseFileMode(p string, mode uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.abandoned {
		return
	}
	l.baseModes[p] = mode & 0o777
	delete(l.whiteouts, p)
	delete(l.dirs, p)
	delete(l.symlinks, p)
	delete(l.files, p)
	delete(l.remoteSizes, p)
	delete(l.contents, p)
}

// baseFileMode reports a recorded mode-only override.
func (l *Layer) baseFileMode(p string) (uint32, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	m, ok := l.baseModes[p]
	return m, ok
}

// remoteSize reports the logical size of an uploaded-but-unmaterialized
// layer image (large truncates): reads stream the stored object.
func (l *Layer) remoteSize(p string) (int64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	s, ok := l.remoteSizes[p]
	return s, ok
}

// hydrateRemote materializes an uploaded-but-unmaterialized layer image
// into the overlay mirror, once. Every mutation path (reopen, chmod,
// rename, a second truncate) treats contents as the authoritative image;
// serving those an absent entry corrupted or resurrected content, and
// per-read streaming re-downloaded the whole object for every FUSE read.
// The image already lives server-side; this streams it back once and
// retires the remote marker.
func (l *Layer) hydrateRemote(ctx context.Context, path string) error {
	l.mu.RLock()
	_, hasContent := l.contents[path]
	_, remote := l.remoteSizes[path]
	l.mu.RUnlock()
	if !remote || hasContent {
		return nil
	}
	rc, err := l.cl.ReadFSLayerFileStream(ctx, l.layerRef, l.remotePath(path), nil)
	if err != nil {
		return err
	}
	data, rerr := io.ReadAll(rc)
	cerr := rc.Close()
	if rerr != nil {
		return rerr
	}
	if cerr != nil {
		return cerr
	}
	l.mu.Lock()
	if !l.abandoned {
		l.contents[path] = data
		delete(l.remoteSizes, path)
	}
	l.mu.Unlock()
	return nil
}

func (l *Layer) markLayerDir(p string, mode uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.abandoned {
		return
	}
	l.dirs[p] = mode & 0o777
	delete(l.whiteouts, p)
	delete(l.files, p)
	delete(l.symlinks, p)
	delete(l.contents, p)
	delete(l.remoteSizes, p)
}

func (l *Layer) markLayerSymlink(p, target string, mode uint32) {
	if mode == 0 {
		mode = uint32(syscall.S_IFLNK) | 0o777
	} else if mode&uint32(syscall.S_IFMT) == 0 {
		mode = uint32(syscall.S_IFLNK) | (mode & 0o777)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.abandoned {
		return
	}
	l.symlinks[p] = symlinkState{Target: target, Mode: mode}
	delete(l.whiteouts, p)
	delete(l.files, p)
	delete(l.dirs, p)
	delete(l.contents, p)
	delete(l.remoteSizes, p)
}

// lookup resolves a path against the overlay; ok reports an overlay hit.
func (l *Layer) lookup(p string) (kind string, mode uint32, target string, content []byte, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if _, hit := l.whiteouts[p]; hit {
		return "whiteout", 0, "", nil, true
	}
	if m, hit := l.dirs[p]; hit {
		return "dir", m, "", nil, true
	}
	if st, hit := l.symlinks[p]; hit {
		return "symlink", st.Mode, st.Target, nil, true
	}
	if m, hit := l.files[p]; hit {
		c := l.contents[p]
		return "file", m, "", append([]byte(nil), c...), true
	}
	return "", 0, "", nil, false
}

// impliedDir reports whether any overlay entry lives strictly below p:
// namespace-only directories exist because a deeper entry exists.
func (l *Layer) impliedDir(p string) bool {
	prefix := strings.TrimRight(p, "/") + "/"
	if prefix == "" {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	under := func(m map[string]struct{}) bool {
		for path := range m {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}
		return false
	}
	if under(l.whiteouts) {
		return true
	}
	for path := range l.dirs {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	for path := range l.symlinks {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	for path := range l.files {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// LookupContentForTest returns the live overlay content of a path.
func (l *Layer) LookupContentForTest(p string) ([]byte, bool) {
	kind, _, _, content, ok := l.lookup(p)
	if !ok || kind != "file" {
		return nil, false
	}
	return content, true
}
