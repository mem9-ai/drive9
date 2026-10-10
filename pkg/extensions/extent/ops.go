package extent

// VFS stat/create helpers: attribute projection through the JuiceFS
// session and creation under the extent plane's parent inode.

import (
	"context"
	"path"
	"strings"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// statViaVFS projects one VFS.GetAttr on the resolved inode, mapped
// through juiceTypeToStatMode.
func (e *Extent) statViaVFS(ctx context.Context, path string, ino uint64) (fs.Result, error) {
	v := e.vfs()
	if v == nil {
		return fs.Result{}, syscall.EIO
	}
	entry, errn := v.GetAttr(e.jfsCtx(callerFrom(ctx)), jfsmeta.Ino(ino), 0)
	if errn != 0 || entry == nil || entry.Attr == nil {
		if errn == syscall.ENOENT {
			e.missRemember(path)
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{}, errno(errn)
	}
	attr := attrFromJuice(entry.Attr)
	e.keepOpenWALIndexSize(path, &attr)
	e.rememberExtentIno(path, ino)
	return fs.Result{Attr: &attr}, nil
}

// sqliteWALIndexMinSize is the size a still-open sqlite -shm file's mapping
// needs: an open shared-memory index reports 0 in the metadata but the
// kernel mapping requires the full index, so stat it at this floor.
const sqliteWALIndexMinSize int64 = 32768

// keepOpenWALIndexSize gives a still-open sqlite -shm file the size its
// mapping needs. See sqliteWALIndexMinSize.
func (e *Extent) keepOpenWALIndexSize(path string, attr *fs.Attr) {
	if attr.Size > 0 || !isSQLiteWALIndexPath(path) {
		return
	}
	if !e.hasLive(path) {
		return
	}
	attr.Size = sqliteWALIndexMinSize
}

// isSQLiteWALIndexPath reports a "<db>-shm" path (never a bare "-shm").
func isSQLiteWALIndexPath(localPath string) bool {
	name := path.Base(localPath)
	if name == "-shm" || !strings.HasSuffix(name, "-shm") {
		return false
	}
	return strings.TrimSuffix(name, "-shm") != ""
}

func attrFromJuice(a *jfsmeta.Attr) fs.Attr {
	attr := fs.Attr{
		Mode:     juiceTypeToStatMode(a.Typ, a.Mode) & 0o777,
		Size:     int64(a.Length),
		Mtime:    time.Unix(int64(a.Mtime), int64(a.Mtimensec)),
		Nlink:    a.Nlink,
		Uid:      a.Uid,
		Gid:      a.Gid,
		HasOwner: true,
	}
	switch a.Typ {
	case jfsmeta.TypeDirectory:
		attr.Kind = fs.KindDir
	case jfsmeta.TypeSymlink:
		attr.Kind = fs.KindSymlink
	default:
		attr.Kind = fs.KindFile
	}
	return attr
}

// createInVFS resolves the parent chain in the JuiceFS namespace (the
// mount root maps to the tenant's extent subtree), creates the file with
// the caller's mode, and returns its inode.
func (e *Extent) createInVFS(ctx context.Context, op *fs.Op) (uint64, bool) {
	v := e.vfs()
	if v == nil {
		return 0, false
	}
	jctx := e.jfsCtx(op.Caller)
	parent, errn := e.ensureParent(jctx, pathutil.DirOf(op.Path))
	if errn != 0 {
		return 0, false
	}
	mode := uint16(op.Mode & 0o777)
	if mode == 0 {
		mode = 0o644
	}
	entry, _, errn := v.Create(jctx, parent, pathutil.BaseName(op.Path), mode, 0, op.Flags)
	if errn != 0 || entry == nil {
		return 0, false
	}
	ino := uint64(entry.Inode)
	e.rememberExtentIno(op.Path, ino)
	return ino, true
}

// ensureParent walks from the extent mount root, creating missing
// directories (mkdir -p semantics in the JuiceFS namespace), and returns
// the parent inode.
func (e *Extent) ensureParent(jctx vfs.LogContext, dirPath string) (jfsmeta.Ino, syscall.Errno) {
	v := e.vfs()
	if v == nil {
		return 0, syscall.EIO
	}
	root := jfsmeta.RootInode
	if dirPath == "/" || dirPath == "" {
		return root, 0
	}
	segs := splitPath(dirPath)
	cur := root
	for _, seg := range segs {
		entry, errn := v.Lookup(jctx, cur, seg)
		if errn == syscall.ENOENT {
			newEntry, mkErrn := v.Mkdir(jctx, cur, seg, 0o755, 0)
			if mkErrn != 0 && mkErrn != syscall.EEXIST {
				return 0, mkErrn
			}
			if mkErrn == 0 && newEntry != nil {
				cur = newEntry.Inode
				continue
			}
			entry, errn = v.Lookup(jctx, cur, seg)
			if errn != 0 || entry == nil {
				return 0, errn
			}
		} else if errn != 0 || entry == nil {
			return 0, errn
		}
		cur = entry.Inode
	}
	return cur, 0
}

func splitPath(p string) []string {
	var out []string
	for _, seg := range splitSlash(p) {
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
}

func splitSlash(p string) []string {
	var out []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			if i > start {
				out = append(out, p[start:i])
			}
			start = i + 1
		}
	}
	if start < len(p) {
		out = append(out, p[start:])
	}
	return out
}
