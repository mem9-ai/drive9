// Package localize routes policy-declared local-only subtrees to a
// mount-scoped on-disk overlay: operations on those paths are answered
// entirely from the overlay (a remote copy at the same path stays
// invisible while the overlay owns it), while everything else passes to
// the rest of the chain unchanged.
//
// Three planes cooperate: a routing Policy (pattern lists, with a
// gitignore-aware gate consulted through the chain below), the persistent
// Overlay backing store, and a transient plane for SQLite WAL-index
// sidecars that exists only while their database is open.
package localize

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Options configures the overlay router.
type Options struct {
	// LocalRoot anchors the persistent overlay backing store
	// (<LocalRoot>/overlay); blank disables local routing.
	LocalRoot                   string
	LocalOnlyPatterns           []string
	LocalGitignoreAwarePatterns []string
	RemoteOnlyPatterns          []string

	// Profile names the mount profile. With ApplyBuiltinDefaults, the
	// profile's built-in local-only defaults (coding-agent: node_modules,
	// .venv; gitignore-aware: target) are merged ahead of the given lists —
	// v1 NewLocalPolicy semantics. The CLI resolves effective profiles
	// itself and leaves ApplyBuiltinDefaults false.
	Profile              string
	ApplyBuiltinDefaults bool

	// DeclarativeLocal marks view paths that another component has declared
	// local (structural routing outside the pattern lists). It composes
	// with the pattern lists as an OR.
	DeclarativeLocal func(viewPath string) bool

	// TransientRoot anchors the transient sidecar plane; blank disables it.
	TransientRoot string
}

// Localize is the overlay router: local-classified paths are claimed and
// served from the overlay, everything else passes to the next handler.
type Localize struct {
	opts      Options
	overlay   *Overlay
	policy    *Policy
	transient *Overlay

	openDBMu sync.Mutex
	openDBs  map[string]int
	unlinked map[string]bool

	innerMu    sync.Mutex
	repoRoots  map[string]string
	ignoreSets map[string]*ignoreRuleset
}

// token is an open overlay file: the terminal data-plane handle for a
// local claim. Reads, writes and syncs go straight to the backing file;
// Release closes it and retires the open-reference accounting.
type token struct {
	file *os.File
	path string
}

// New builds the overlay router. The persistent backing root is created
// eagerly when the policy is active; a transient-plane root that cannot be
// created disables just that plane (sidecars then stay on their classified
// route).
func New(opts Options) (*Localize, error) {
	f := &Localize{
		opts:       opts,
		overlay:    NewOverlay(opts.LocalRoot),
		policy:     NewPolicy(opts.Profile, opts.LocalOnlyPatterns, opts.LocalGitignoreAwarePatterns, opts.RemoteOnlyPatterns, opts.ApplyBuiltinDefaults),
		openDBs:    map[string]int{},
		unlinked:   map[string]bool{},
		repoRoots:  map[string]string{},
		ignoreSets: map[string]*ignoreRuleset{},
		transient:  NewOverlay(opts.TransientRoot),
	}
	if f.overlay != nil && f.policy.Enabled() {
		if err := f.overlay.EnsureRoot(); err != nil {
			return nil, fmt.Errorf("localize: overlay root: %w", err)
		}
	}
	if f.transient != nil {
		if err := f.transient.EnsureRoot(); err != nil {
			f.transient = nil
		}
	}
	return f, nil
}

// Do serves one operation against the overlay view. A local-classified
// path is answered entirely from the overlay; everything else passes to
// next. Handle-bound operations on a foreign handle pass through too: the
// claim that opened the handle owns the data plane. Directory fsync is the
// exception — it arrives with the directory-stream handle, which is not a
// data-plane handle, and the backing directory still needs its flush.
func (f *Localize) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if op.Handle == nil && op.Fh != 0 && op.Kind != fs.OpFsync {
		// A handle-bound op normally belongs to the layer that opened the
		// handle — but only for paths this overlay does not claim: the
		// kernel re-asks fh-addressed GETATTR/SETATTR once the attr TTL
		// expires (fstat on a still-open fd), and the inner chain would
		// stat a remote that never saw the file (ENOENT). Claimed paths
		// fall through to the switch, which serves them by path from the
		// overlay. FLUSH keeps passing through (its errors surface on
		// close).
		if op.Kind == fs.OpFlush || op.Path == "" ||
			f.overlayFor(ctx, fs.HandlerFilesystem(next), op.Path, false) == nil {
			return next(ctx, op)
		}
	}
	inner := fs.HandlerFilesystem(next)
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		// LOOKUP claims with the same StatLocal shadow as GETATTR (v1
		// serves LOOKUP of a LocalOnly path from the overlay): without
		// the claim a re-lookup of an overlay-only file leaks to the
		// remote and returns ENOENT after the entry TTL.
		st, claimed, err := f.StatLocal(ctx, inner, op.Path)
		if err != nil {
			return fs.Result{}, err
		}
		if claimed {
			return fs.Result{Attr: &st}, nil
		}

	case fs.OpReadlink:
		if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
			target, err := o.Readlink(op.Path)
			if err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Data: []byte(target)}, nil
		}

	case fs.OpList:
		if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
			entries, err := o.ReadDir(op.Path)
			if err != nil {
				return fs.Result{}, err
			}
			out := make([]fs.Entry, 0, len(entries))
			for _, e := range entries {
				info, err := e.Info()
				if err != nil {
					return fs.Result{}, err
				}
				childPath := joinDir(op.Path, e.Name())
				// A child that does not itself route local belongs to the
				// remote view at its own path and must not be listed here.
				if !f.classifiesLocal(ctx, inner, childPath, info.IsDir()) && f.transientFor(childPath, info.IsDir()) == nil {
					continue
				}
				out = append(out, fs.Entry{Name: e.Name(), Attr: attrOfLocalInfo(info)})
			}
			return fs.Result{Entries: out}, nil
		}

	case fs.OpCreate:
		if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
			// Flags pass through untouched so the caller's access mode and
			// O_EXCL/O_TRUNC semantics hold on the backing file: a plain
			// read-only open of a missing file reports ENOENT, it does not
			// materialize an empty file. O_APPEND is the exception: the
			// kernel already resolves an append write's offset (EOF at
			// submission) before the WRITE arrives, and Go's os.File
			// forbids WriteAt on O_APPEND descriptors — open the backing
			// file without it and keep writing at the kernel offsets.
			flags := op.Flags &^ uint32(syscall.O_APPEND)
			file, err := o.OpenFile(op.Path, flags, op.Mode&0o7777)
			if err != nil {
				return fs.Result{}, err
			}
			f.noteDBOpen(op.Path)
			var st *fs.Attr
			if info, err := file.Stat(); err == nil {
				attr := attrOfLocalInfo(info)
				st = &attr
			}
			return fs.Result{Handle: &token{file: file, path: op.Path}, Attr: st}, nil
		}

	case fs.OpRead:
		if h, ok := op.Handle.(*token); ok {
			if op.Limit <= 0 {
				b, err := io.ReadAll(h.file)
				if err != nil && !errors.Is(err, io.EOF) {
					return fs.Result{}, err
				}
				return fs.Result{Data: b}, nil
			}
			buf := make([]byte, op.Limit)
			n, err := h.file.ReadAt(buf, op.Offset)
			if err != nil && !errors.Is(err, io.EOF) {
				return fs.Result{}, err
			}
			return fs.Result{Data: buf[:n]}, nil
		}
		if op.Handle == nil {
			if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
				fh, err := o.OpenFile(op.Path, syscall.O_RDONLY, 0)
				if err != nil {
					return fs.Result{}, err
				}
				data, rerr := io.ReadAll(fh)
				cerr := fh.Close()
				if rerr != nil && !errors.Is(rerr, io.EOF) {
					return fs.Result{}, rerr
				}
				if cerr != nil {
					return fs.Result{}, cerr
				}
				if op.Offset > 0 || op.Limit > 0 {
					off := op.Offset
					if off < 0 {
						off = 0
					}
					if off >= int64(len(data)) {
						return fs.Result{Data: nil}, nil
					}
					end := int64(len(data))
					if op.Limit > 0 && off+op.Limit < end {
						end = off + op.Limit
					}
					data = data[off:end]
				}
				return fs.Result{Data: data}, nil
			}
		}

	case fs.OpWrite:
		if h, ok := op.Handle.(*token); ok {
			if _, err := h.file.WriteAt(op.Data, op.Offset); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}
		if op.Handle == nil {
			if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
				mode := op.Mode
				if mode == 0 {
					// A path-addressed write creates the file when it does
					// not exist yet.
					mode = 0o644
				}
				fh, err := o.OpenFile(op.Path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC, mode)
				if err != nil {
					return fs.Result{}, err
				}
				_, werr := fh.Write(op.Data)
				cerr := fh.Close()
				if werr != nil {
					return fs.Result{}, werr
				}
				if cerr != nil {
					return fs.Result{}, cerr
				}
				return fs.Result{}, nil
			}
		}

	case fs.OpFlush, fs.OpFsync:
		if h, ok := op.Handle.(*token); ok {
			if err := h.file.Sync(); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}
		if op.Handle == nil {
			if o := f.overlayFor(ctx, inner, op.Path, true); o != nil {
				if err := o.SyncDir(op.Path); err != nil {
					return fs.Result{}, err
				}
				return fs.Result{}, nil
			}
		}

	case fs.OpRelease:
		if h, ok := op.Handle.(*token); ok {
			_ = h.file.Close()
			f.noteDBClose(h.path)
			return fs.Result{}, nil
		}

	case fs.OpMkdir:
		if o := f.overlayFor(ctx, inner, op.Path, true); o != nil {
			mode := op.Mode & 0o7777
			if err := o.Mkdir(op.Path, mode); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode}}, nil
		}

	case fs.OpSymlink:
		if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
			if err := o.Symlink(op.Target, op.Path); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}

	case fs.OpUnlink:
		if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
			info, err := o.Lstat(op.Path)
			if err != nil {
				return fs.Result{}, err
			}
			if info.IsDir() {
				return fs.Result{}, syscall.EISDIR
			}
			if err := o.Remove(op.Path); err != nil {
				return fs.Result{}, err
			}
			f.noteUnlinked(op.Path)
			return fs.Result{}, nil
		}

	case fs.OpRmdir:
		if o := f.overlayFor(ctx, inner, op.Path, true); o != nil {
			info, err := o.Lstat(op.Path)
			if err != nil {
				return fs.Result{}, err
			}
			if !info.IsDir() {
				return fs.Result{}, syscall.ENOTDIR
			}
			if err := o.Remove(op.Path); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}

	case fs.OpSetAttr:
		if o := f.overlayFor(ctx, inner, op.Path, false); o != nil {
			if !op.Mtime.IsZero() {
				if err := f.chtimes(o, op.Path, op.Mtime); err != nil {
					return fs.Result{}, err
				}
			}
			if op.HasMode {
				if err := o.Chmod(op.Path, op.Mode&0o7777); err != nil {
					return fs.Result{}, err
				}
			}
			if op.Size >= 0 {
				if err := o.Truncate(op.Path, op.Size); err != nil {
					return fs.Result{}, err
				}
			}
			return fs.Result{}, nil
		}

	case fs.OpRename:
		if op.Path == op.Path2 {
			return fs.Result{}, nil
		}
		// A directory source re-classifies with a directory hint: ignore
		// rules can distinguish a directory from a same-named file.
		dirHint := false
		if f.overlay != nil {
			if info, err := f.overlay.Lstat(op.Path); err == nil && info.IsDir() {
				dirHint = true
			}
		}
		oldLocal := f.classifiesLocal(ctx, inner, op.Path, dirHint)
		newLocal := f.classifiesLocal(ctx, inner, op.Path2, dirHint)
		if oldLocal || newLocal {
			if oldLocal != newLocal {
				return fs.Result{}, syscall.EXDEV
			}
			if err := f.overlay.Rename(op.Path, op.Path2); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}
		// Remote rename: keep the backing coherent around the commit —
		// reject a destination whose invisible backing holds real local
		// entries first, then move a backing subtree along with the commit.
		if err := f.pruneRenameDestParents(ctx, inner, op.Path, op.Path2); err != nil {
			return fs.Result{}, err
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		if err := f.renameOverlaySubtree(op.Path, op.Path2); err != nil {
			// The remote namespace has committed; retry once, and a
			// persistent failure still fails the reply so the orphaned
			// subtree is reported rather than silently kept at the stale
			// path.
			if err := f.renameOverlaySubtree(op.Path, op.Path2); err != nil {
				return fs.Result{}, err
			}
		}
		return res, nil

	case fs.OpLink:
		srcLocal := f.classifiesLocal(ctx, inner, op.Path, false)
		dstLocal := f.classifiesLocal(ctx, inner, op.Path2, false)
		if srcLocal || dstLocal {
			if srcLocal != dstLocal {
				return fs.Result{}, syscall.EXDEV
			}
			if err := f.overlay.Link(op.Path, op.Path2); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}
	}
	return next(ctx, op)
}

// AttachDeclarativeLocal wires a structural local-routing declaration
// (assembly time, before the first dispatch): the git extension declares
// the .git of registered or pending workspaces local so clone and
// checkpoint traffic never round-trips the classic namespace.
func (f *Localize) AttachDeclarativeLocal(declared func(viewPath string) bool) {
	f.opts.DeclarativeLocal = declared
}

// StatLocal stats a local-classified path from the overlay. A path that
// routes local is claimed even when nothing exists in the overlay yet —
// the error (typically ENOENT) is the reply, and a remote copy at the same
// path stays invisible while the overlay owns it.
func (f *Localize) StatLocal(ctx context.Context, inner fs.Filesystem, viewPath string) (st fs.Attr, claimed bool, err error) {
	o := f.overlayFor(ctx, inner, viewPath, false)
	if o == nil {
		return fs.Attr{}, false, nil
	}
	info, err := o.Lstat(viewPath)
	if err != nil {
		return fs.Attr{}, true, err
	}
	return attrOfLocalInfo(info), true, nil
}

// ReexecRefusal reports overlay state a process re-exec would strand
// (fs.ReexecGater). The persistent overlay is process-local by
// construction, so an enabled overlay alone refuses; the transient sidecar
// plane must have drained (a live SQLite -shm has no recovery across
// exec), and a transient root that cannot be read refuses because the
// count cannot be established.
func (f *Localize) ReexecRefusal() (string, int) {
	if f.overlay != nil {
		return "user/profile local overlay is enabled", 1
	}
	if n := f.transientEntryCount(); n != 0 {
		if n < 0 {
			return "transient local overlay root cannot be read", 1
		}
		return "transient local overlay has entries", n
	}
	return "", 0
}

// classifiesLocal reports whether policy routes viewPath to the overlay.
// The claim is classification-only (no backing-existence probe). dirHint
// feeds ignore-rule evaluation only.
func (f *Localize) classifiesLocal(ctx context.Context, inner fs.Filesystem, viewPath string, dirHint bool) bool {
	if f.overlay == nil || reservedViewPath(viewPath) || !f.policy.Enabled() {
		return false
	}
	oracle := func(viewPath string, dirHint bool) (bool, bool) {
		return f.gitIgnoreCheck(ctx, inner, viewPath, dirHint)
	}
	if f.policy.Classify(viewPath, oracle, dirHint) == LayerLocalOnly {
		return true
	}
	return f.opts.DeclarativeLocal != nil && f.opts.DeclarativeLocal(viewPath)
}

// chtimes applies a timestamp update to an overlay file. The kernel flushes
// inode times from writeback with the file handle, and that flush can
// arrive after the name is gone: SQLite unlinks its WAL index (-shm) while
// the descriptor is still open. POSIX keeps the inode alive while a handle
// is open, so a timestamp update for a path removed under a live handle is
// already complete in memory — ENOENT there is swallowed instead of
// failing the flush (a failed time flush marks the page mapping and
// surfaces as EIO on the next close).
func (f *Localize) chtimes(o *Overlay, viewPath string, mtime time.Time) error {
	err := o.Chtimes(viewPath, mtime)
	if err == nil || !errors.Is(err, os.ErrNotExist) || !f.wasUnlinked(viewPath) {
		return err
	}
	return nil
}

// overlayFor returns the overlay that should serve viewPath: the transient
// sidecar plane first, then the policy-classified local plane.
func (f *Localize) overlayFor(ctx context.Context, inner fs.Filesystem, viewPath string, dirHint bool) *Overlay {
	if o := f.transientFor(viewPath, dirHint); o != nil {
		return o
	}
	if f.classifiesLocal(ctx, inner, viewPath, dirHint) {
		return f.overlay
	}
	return nil
}

func (t *token) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	return t.file.ReadAt(dest, off)
}

func (t *token) Write(ctx context.Context, off int64, data []byte) error {
	_, err := t.file.WriteAt(data, off)
	return err
}

func (t *token) Sync(ctx context.Context) error {
	return t.file.Sync()
}

func (t *token) Flush(ctx context.Context) error {
	return t.file.Sync()
}

func (t *token) Release(ctx context.Context) error {
	return t.file.Close()
}

// attrOfLocalInfo converts backing-file metadata into reply attributes;
// the file type rides in Kind.
func attrOfLocalInfo(info iofs.FileInfo) fs.Attr {
	kind := fs.KindFile
	if info.IsDir() {
		kind = fs.KindDir
	} else if info.Mode()&iofs.ModeSymlink != 0 {
		kind = fs.KindSymlink
	}
	return fs.Attr{
		Kind:  kind,
		Mode:  uint32(info.Mode().Perm()),
		Size:  info.Size(),
		Mtime: info.ModTime(),
	}
}

// reservedViewPath reports whether the path belongs to the reserved
// control namespace, which never routes to a local plane.
func reservedViewPath(viewPath string) bool {
	return viewPath == "/.drive9" || strings.HasPrefix(viewPath, "/.drive9/")
}
