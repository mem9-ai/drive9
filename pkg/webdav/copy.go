package webdav

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
	"golang.org/x/net/webdav"
)

// copyFastPath serves the common WebDAV COPY — a regular file onto a
// non-collection destination — without x/net/webdav's pre-delete.
//
// x/net's copyFiles removes an existing destination BEFORE it writes the
// replacement, so any failure in between loses the old content. It also
// buffers the destination in memory. Here the destination is written through
// the same staged, committed writer a PUT uses (bounded memory, one atomic
// commit), so the old content survives until the new revision lands, and a
// failed copy leaves it untouched.
//
// Everything this path cannot prove safe is delegated to x/net/webdav
// unchanged: collections and their Depth semantics, lock tokens in an If
// header, a collection destination, a non-regular source, cross-host
// destinations and malformed requests. The fast path only answers when the
// outcome is the x/net one plus the two properties above.
type copyFastPath struct {
	fs     *fileSystem
	client *client.Client
	prefix string
	locks  webdav.LockSystem
}

// serve attempts the fast path and reports whether it handled the request.
func (cp *copyFastPath) serve(w http.ResponseWriter, r *http.Request) bool {
	if cp == nil || cp.fs == nil || cp.client == nil || cp.locks == nil {
		return false
	}
	// An If header carries lock tokens whose condition parsing belongs to
	// x/net/webdav; delegate instead of reimplementing it.
	if r.Header.Get("If") != "" {
		return false
	}
	// x/net rejects a Depth other than 0 or infinity BEFORE touching the
	// destination; delegate so such a request keeps answering 400.
	if depth := r.Header.Get("Depth"); depth != "" && depth != "0" && !strings.EqualFold(depth, "infinity") {
		return false
	}
	dest := r.Header.Get("Destination")
	if dest == "" {
		return false
	}
	u, err := url.Parse(dest)
	if err != nil || (u.Host != "" && u.Host != r.Host) {
		return false // x/net answers 400 / 502
	}
	srcLocal, ok := cp.localPath(r.URL.Path)
	if !ok {
		return false
	}
	dstLocal, ok := cp.localPath(u.Path)
	if !ok {
		return false
	}
	if srcLocal == dstLocal {
		return false // x/net answers 403
	}
	ctx := r.Context()
	srcRemote := cp.fs.remotePath(srcLocal)
	dstRemote := cp.fs.remotePath(dstLocal)

	srcStat, err := cp.client.StatCtx(ctx, srcRemote)
	if err != nil {
		return false // 404 / 500 by x/net
	}
	if srcStat.IsDir || isSpecialOrSymlink(srcStat) {
		return false
	}

	// Lock parity with x/net's confirmLocks: without an If header it takes a
	// temporary zero-depth lock on the destination BEFORE looking at it, which
	// fails when another client holds a conflicting lock (a locked
	// destination answers 423, not 412).
	now := time.Now()
	token, err := cp.locks.Create(now, webdav.LockDetails{Root: dstLocal, Duration: -1, ZeroDepth: true})
	if err != nil {
		if errors.Is(err, webdav.ErrLocked) {
			writeWebDAVStatus(w, webdav.StatusLocked)
			return true
		}
		logger.Error(ctx, "webdav copy lock failed", zap.String("path", dstLocal), zap.Error(err))
		writeWebDAVStatus(w, http.StatusInternalServerError)
		return true
	}
	defer func() { _ = cp.locks.Unlock(now, token) }()

	created := false
	dstStat, dstErr := cp.client.StatCtx(ctx, dstRemote)
	switch {
	case dstErr != nil:
		if !client.IsNotFound(dstErr) {
			return false
		}
		created = true
	case dstStat.IsDir:
		return false
	case r.Header.Get("Overwrite") == "F":
		writeWebDAVStatus(w, http.StatusPreconditionFailed)
		return true
	case dstStat.Nlink > 1:
		// The destination shares its object with another name: writing in
		// place would change that other path too. x/net replaces the name
		// (RemoveAll + create), which is the POSIX copy behaviour.
		return false
	case isSymlink(dstStat):
		// Writing through the name would overwrite the link's stored target
		// and leave a symlink where a regular copy is expected; x/net removes
		// the name and creates a regular file.
		return false
	}

	src, err := cp.client.ReadStream(ctx, srcRemote)
	if err != nil {
		logCopyFailure(ctx, srcLocal, dstLocal, err)
		if client.IsNotFound(err) {
			writeWebDAVStatus(w, http.StatusNotFound)
		} else {
			writeWebDAVStatus(w, http.StatusInternalServerError)
		}
		return true
	}
	defer func() { _ = src.Close() }()

	file, err := cp.fs.OpenFile(ctx, dstLocal, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(srcStat.Mode&0o777))
	if err != nil {
		// Mirror x/net: a missing destination parent is 409, anything else
		// is 403.
		if os.IsNotExist(err) {
			writeWebDAVStatus(w, http.StatusConflict)
		} else {
			writeWebDAVStatus(w, http.StatusForbidden)
		}
		return true
	}
	dst, ok := file.(*writeFile)
	if !ok {
		_ = file.Close()
		return false
	}
	if created && r.Header.Get("Overwrite") == "F" {
		// The destination was absent when we checked; commit create-only so a
		// concurrent API writer that takes the name during the copy is NOT
		// clobbered (the server rejects a commit whose expected revision 0 no
		// longer matches).
		dst.expectedRevision = 0
	}
	if _, err := io.Copy(dst, src); err != nil {
		// A COPY has no declared length, so Close would commit whatever
		// arrived: abort instead, leaving the old destination in place.
		dst.Abort()
		logCopyFailure(ctx, srcLocal, dstLocal, err)
		writeWebDAVStatus(w, http.StatusInternalServerError)
		return true
	}
	if err := dst.Close(); err != nil {
		logCopyFailure(ctx, srcLocal, dstLocal, err)
		if dst.expectedRevision == 0 && isRevisionConflict(err) {
			// The destination appeared while the copy streamed: the
			// Overwrite: F promise no longer holds.
			writeWebDAVStatus(w, http.StatusPreconditionFailed)
			return true
		}
		writeWebDAVStatus(w, putFailureStatus(err))
		return true
	}

	// Dead properties follow the resource (x/net's copyProps); a replaced
	// destination must not keep its old ones.
	props := cp.fs.deadProps()
	props.deleteTree(dstRemote)
	props.copyTree(srcRemote, dstRemote)

	// A REPLACED destination is a fresh resource under COPY semantics (x/net
	// removes the name before creating the replacement), so the old object's
	// user tags must go with it: left behind, they make exact tag queries
	// match content that no longer exists. A created destination never had
	// any. Tags are drive9 metadata, not WebDAV properties, so a copy does not
	// carry the source's set — same as x/net, which copies dead props only.
	if !created {
		if err := cp.client.SetMetadataCtx(ctx, dstRemote, client.SetMetadataOptions{Tags: map[string]string{}}); err != nil {
			logger.Warn(ctx, "webdav copy kept the replaced destination's tags",
				zap.String("path", dstLocal), zap.Error(err))
		}
	}

	if created {
		writeWebDAVStatus(w, http.StatusCreated)
	} else {
		writeWebDAVStatus(w, http.StatusNoContent)
	}
	return true
}

// isRevisionConflict reports a compare-and-set refusal from the API plane. The
// commit error has already passed through the bridge's os-level mapping, which
// turns a 409 into os.ErrExist, so both shapes must count.
func isRevisionConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, client.ErrConflict) || errors.Is(err, os.ErrExist) {
		return true
	}
	var statusErr *client.StatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict
}

// localPath strips the mount prefix from a request path. It refuses a path
// outside the prefix so the caller delegates to x/net's own handling.
func (cp *copyFastPath) localPath(p string) (string, bool) {
	if cp.prefix != "" {
		root := strings.TrimSuffix(cp.prefix, "/")
		if p != root && !strings.HasPrefix(p, root+"/") {
			return "", false
		}
		p = strings.TrimPrefix(p, root)
	}
	if p == "" {
		p = "/"
	}
	return normPath(p), true
}

// isSpecialOrSymlink reports a source the byte copy must not follow (x/net
// copies through FileSystem.OpenFile, whose semantics for these differ).
func isSpecialOrSymlink(st *client.StatResult) bool {
	if st == nil || !st.HasMode {
		return false
	}
	switch st.Mode & 0o170000 {
	case 0, 0o100000, 0o040000: // regular, dir (already handled), no type
		return false
	default:
		return true
	}
}

// isSymlink reports a symlink destination, whose dentry x/net replaces.
func isSymlink(st *client.StatResult) bool {
	return st != nil && st.HasMode && st.Mode&0o170000 == 0o120000
}

// writeWebDAVStatus answers like x/net/webdav: the status plus its text,
// except 204 which carries no body.
func writeWebDAVStatus(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
	if code != http.StatusNoContent {
		_, _ = w.Write([]byte(http.StatusText(code)))
	}
}

// logCopyFailure records a failed server-side copy without the presigned URLs
// a transport error may carry.
func logCopyFailure(ctx context.Context, src, dst string, err error) {
	logger.Error(ctx, "webdav copy failed",
		zap.String("src", src),
		zap.String("dst", dst),
		zap.String("error", redactURLCredentials(err.Error())),
	)
}
