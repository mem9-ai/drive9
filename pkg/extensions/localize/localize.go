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
	"strconv"
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

	// localSpecials records metadata-only nodes (FIFO/CHR/BLK/SOCK) created
	// through this overlay: the backing store cannot hold them, so lookups
	// and unlinks answer from the record instead of falling through to a
	// plane that only knows the remote view (a local-only node_modules
	// FIFO disappeared from lookups right after mkfifo).
	specialMu sync.Mutex
	// specialNode maps every recorded path (aliases included) to a shared
	// node id; specialAttrs holds the node's authoritative metadata. A
	// per-path attr copy let a chmod on one hardlink alias diverge from
	// its siblings (and the stale copy then wrote back through Lookup).
	specialNode  map[string]string
	specialAttrs map[string]fs.Attr
	// specialNextID mints monotonic node ids. A pathname is NOT a stable
	// identity: unlink(a) then create a again must not overwrite the node
	// a surviving alias (b) still references.
	specialNextID uint64
}

// token is an open overlay file: the terminal data-plane handle for a
// local claim. Reads, writes and syncs go straight to the backing file;
// Release closes it and retires the open-reference accounting.
type token struct {
	file *os.File
	path string
	// onRelease runs the owning router's open-reference accounting exactly
	// once from Release — the facade releases claimed files by calling
	// Handle.Release directly, so the OpRelease handler never runs and
	// the router's openDBs accounting leaked every opened pathname.
	onRelease func(path string)
}

// New builds the overlay router. The persistent backing root is created
// eagerly when the policy is active; a transient-plane root that cannot be
// created disables just that plane (sidecars then stay on their classified
// route).
func New(opts Options) (*Localize, error) {
	f := &Localize{
		opts:         opts,
		overlay:      NewOverlay(opts.LocalRoot),
		policy:       NewPolicy(opts.Profile, opts.LocalOnlyPatterns, opts.LocalGitignoreAwarePatterns, opts.RemoteOnlyPatterns, opts.ApplyBuiltinDefaults),
		openDBs:      map[string]int{},
		unlinked:     map[string]bool{},
		repoRoots:    map[string]string{},
		ignoreSets:   map[string]*ignoreRuleset{},
		specialNode:  map[string]string{},
		specialAttrs: map[string]fs.Attr{},
		transient:    NewOverlay(opts.TransientRoot),
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
		if attr, ok := f.localSpecial(op.Path); ok {
			return fs.Result{Attr: &attr}, nil
		}
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
			// Metadata-only local nodes (FIFO/device) live in
			// localSpecials, not in the backing directory: a readdir that
			// skipped them listed the parent as empty although lookups
			// resolved the child.
			out = append(out, f.localSpecialEntries(op.Path)...)
			return fs.Result{Entries: out}, nil
		}
		// A remotely routed directory may still hold LOCAL children (a
		// *.tmp file under a remote-routed parent): merge every overlay
		// child that itself routes local into the lower listing (v1
		// mergeLocalDirEntries merged eligible children regardless of the
		// parent's route — skipping them hid readable files from
		// readdir).
		if res, err := next(ctx, op); err == nil {
			// The local overlay owns any name it holds (v1's
			// mergeOverlayDirEntries REPLACED matching rows): keeping the
			// remote row listed a stale size/type through READDIRPLUS
			// while lookup answered the local file.
			seen := make(map[string]struct{}, len(res.Entries))
			merged := make([]fs.Entry, 0, len(res.Entries))
			// The parent itself routes remote, but its LOCAL children ride
			// the shared overlay: list the overlay's view of this directory
			// and merge every child that itself routes local (v1's
			// mergeLocalDirEntries ran regardless of the parent's route).
			if o := f.overlay; o != nil {
				if extras, err := o.ReadDir(op.Path); err == nil {
					for _, e := range extras {
						info, ierr := e.Info()
						if ierr != nil {
							continue
						}
						childPath := joinDir(op.Path, e.Name())
						if !f.classifiesLocal(ctx, inner, childPath, info.IsDir()) && f.transientFor(childPath, info.IsDir()) == nil {
							continue
						}
						// Replace a remote row with the local plane's
						// attributes (the overlay owns this name): record
						// the name so the remote pass drops its shadowed
						// row — the set starts EMPTY (a bare delete never
						// matched), so same-named remote entries listed
						// twice and READDIRPLUS exposed the conflict.
						seen[e.Name()] = struct{}{}
						merged = append(merged, fs.Entry{Name: e.Name(), Attr: attrOfLocalInfo(info)})
					}
				}
			}
			// Transient-overlay children join the SAME merge (v1's
			// mergeLocalDirEntries walked both overlays): a -shm sidecar
			// created through transient storage was name-addressable but
			// invisible to fresh listings of its remote-routed parent.
			if f.transient != nil {
				if extras, terr := f.transient.ReadDir(op.Path); terr == nil {
					for _, e := range extras {
						info, ierr := e.Info()
						if ierr != nil {
							continue
						}
						if _, dup := seen[e.Name()]; dup {
							continue
						}
						seen[e.Name()] = struct{}{}
						merged = append(merged, fs.Entry{Name: e.Name(), Attr: attrOfLocalInfo(info)})
					}
				}
			}
			for _, e := range f.localSpecialEntries(op.Path) {
				if _, dup := seen[e.Name]; dup {
					continue
				}
				seen[e.Name] = struct{}{}
				merged = append(merged, e)
			}
			for _, e := range res.Entries {
				if _, shadowed := seen[e.Name]; shadowed {
					continue
				}
				merged = append(merged, e)
			}
			res.Entries = merged
			return res, nil
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
			return fs.Result{Handle: &token{file: file, path: op.Path, onRelease: f.noteDBClose}, Attr: st}, nil
		}

		// A database opened through the REMOTE route still needs its open
		// reference held for the HANDLE's lifetime (this fall-through runs
		// only after the local-claim and transient branches declined):
		// the transient -shm routing consults the mount-wide registry (v1
		// consulted it too) — an unregistered remote /db.sqlite left the
		// WAL index persisting to the remote backend.
		if op.Flags&(uint32(syscall.O_CREAT)|uint32(syscall.O_RDWR)|uint32(syscall.O_WRONLY)) != 0 {
			f.noteDBOpen(op.Path)
			res, err := next(ctx, op)
			if err != nil || res.Handle == nil {
				f.noteDBClose(op.Path)
				return res, err
			}
			res.Handle = &dbAccountingHandle{inner: res.Handle, path: op.Path, close: f.noteDBClose}
			return res, nil
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

	case fs.OpMknod:
		if f.overlayFor(ctx, inner, op.Path, false) == nil {
			return next(ctx, op)
		}
		if _, exists := f.localSpecial(op.Path); exists {
			return fs.Result{}, syscall.EEXIST
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		if metadataOnlyLocalMode(op.Mode) {
			f.createLocalSpecial(op.Path, fs.Attr{Kind: fs.KindFile, Mode: op.Mode, Rdev: op.Rdev, Nlink: 1})
		}
		return res, nil

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
		if _, ok := f.localSpecial(op.Path); ok {
			node, _ := f.localSpecialNodeID(op.Path)
			// A metadata-only local node owns no backing file: let the
			// specials plane drop its record, then retire ours (a duplicate
			// mkfifo must not answer EEXIST afterwards).
			res, err := next(ctx, op)
			if err != nil && !fs.IsNotFound(err) {
				return fs.Result{}, err
			}
			f.forgetLocalSpecial(op.Path)
			// A surviving hardlink keeps the node: re-read its link count
			// from the specials plane (the shared attr still said 2).
			f.refreshLocalSpecialNode(ctx, next, node)
			return res, nil
		}
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
			if f.hasLocalSpecialUnder(op.Path) {
				// The backing directory is empty but the local specials
				// records are real children: removing the parent would
				// leave them addressable under a deleted directory.
				return fs.Result{}, syscall.ENOTEMPTY
			}
			if err := o.Remove(op.Path); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}

	case fs.OpSetAttr:
		if _, ok := f.localSpecial(op.Path); ok {
			// The backing file does not exist: the specials plane owns the
			// record (mode/owner/times). Forward, then re-read the record so
			// later lookups answer the settled attrs.
			res, err := next(ctx, op)
			if err != nil {
				return fs.Result{}, err
			}
			if got, gerr := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path}); gerr == nil && got.Attr != nil {
				f.recordLocalSpecial(op.Path, *got.Attr)
			}
			return res, nil
		}
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
			// Metadata-only nodes under the source live in TWO registries:
			// our localSpecials mirror and the specials plane's records.
			// Move the records through the specials handler per node (its
			// exact-record path stays local, so the remote namespace is not
			// touched — a forwarding rename is not an option for a
			// local-classified path), then migrate our mirror.
			olds := f.localSpecialNodes(op.Path)
			if err := f.overlay.Rename(op.Path, op.Path2); err != nil {
				return fs.Result{}, err
			}
			for _, child := range olds {
				from := child
				to := op.Path2
				if from != op.Path {
					to = op.Path2 + from[len(op.Path):]
				}
				// RegistryOnly: the specials plane moves its record without
				// probing or deleting the remote target (its ordinary rename
				// unlinks whatever squats on the name).
				if _, err := next(ctx, &fs.Op{Kind: fs.OpRename, Path: from, Path2: to, RegistryOnly: true}); err != nil {
					// The backing directory already moved; a record that did
					// not follow would answer at the old path. Report it
					// rather than leaving the two registries divergent.
					return fs.Result{}, err
				}
			}
			f.migrateLocalSpecialPrefix(op.Path, op.Path2)
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
		// The rename may have moved local-only special nodes under a REMOTE
		// parent: the specials plane's records follow the remote rename,
		// so our index must follow too or the two split.
		f.migrateLocalSpecialPrefix(op.Path, op.Path2)
		return res, nil

	case fs.OpLink:
		srcLocal := f.classifiesLocal(ctx, inner, op.Path, false)
		dstLocal := f.classifiesLocal(ctx, inner, op.Path2, false)
		if srcLocal || dstLocal {
			if srcLocal != dstLocal {
				return fs.Result{}, syscall.EXDEV
			}
			if attr, ok := f.localSpecial(op.Path); ok {
				// The specials plane's target probe reaches only the REMOTE
				// view: a local backing file or a recorded special on the
				// destination must refuse the link here (the remote 404
				// proved nothing about the local namespace).
				if _, lerr := f.overlay.Lstat(op.Path2); lerr == nil {
					return fs.Result{}, syscall.EEXIST
				}
				if _, ok := f.localSpecial(op.Path2); ok {
					return fs.Result{}, syscall.EEXIST
				}
				// A metadata-only node has no backing file: the specials
				// plane owns the record and creates the alias itself (its
				// OpLink registers the alias without touching the remote
				// namespace).
				res, err := next(ctx, &fs.Op{Kind: fs.OpLink, Path: op.Path, Path2: op.Path2})
				if err != nil {
					return fs.Result{}, err
				}
				if got, gerr := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path2}); gerr == nil && got.Attr != nil {
					attr = *got.Attr
				}
				f.recordLocalSpecial(op.Path, attr)
				f.linkLocalSpecial(op.Path2, op.Path)
				return res, nil
			}
			if err := f.overlay.Link(op.Path, op.Path2); err != nil {
				return fs.Result{}, err
			}
			// Reply with the link's REAL attributes: an empty result made
			// the facade answer size=0/0644/nlink=1 for an existing file.
			if attr, claimed, serr := f.StatLocal(ctx, inner, op.Path2); serr == nil && claimed {
				return fs.Result{Attr: &attr}, nil
			}
			return fs.Result{}, nil
		}
	}
	return next(ctx, op)
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
	err := t.file.Close()
	if t.onRelease != nil {
		t.onRelease(t.path)
	}
	return err
}

// dbAccountingHandle holds the router's open reference for the lifetime
// of a REMOTELY routed database descriptor: its Release retires the
// reference the transient -shm routing consults.
type dbAccountingHandle struct {
	inner fs.Handle
	path  string
	close func(path string)
}

func (h *dbAccountingHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	return h.inner.Read(ctx, off, dest)
}

func (h *dbAccountingHandle) Write(ctx context.Context, off int64, data []byte) error {
	return h.inner.Write(ctx, off, data)
}

func (h *dbAccountingHandle) Sync(ctx context.Context) error { return h.inner.Sync(ctx) }

// UnwrapHandle exposes the wrapped handle: FD-named operations
// (FATTR_FH SetAttr, unlink survivor retargeting) walk the decorator
// chain to the descriptor's OWN write-plane handle — without this, the
// resolution fell back to a per-path lookup and mutated whichever
// object currently owns the NAME (a recreated file truncated through
// the old descriptor).
func (h *dbAccountingHandle) UnwrapHandle() fs.Handle { return h.inner }

func (h *dbAccountingHandle) Flush(ctx context.Context) error { return h.inner.Flush(ctx) }

func (h *dbAccountingHandle) Release(ctx context.Context) error {
	err := h.inner.Release(ctx)
	h.close(h.path)
	return err
}

// metadataOnlyLocalMode matches the specials plane's node classes (FIFO,
// character/block device, socket): nodes the local backing store cannot
// represent.
func metadataOnlyLocalMode(mode uint32) bool {
	switch mode & syscall.S_IFMT {
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		return true
	}
	return false
}

func (f *Localize) recordLocalSpecial(path string, attr fs.Attr) {
	f.specialMu.Lock()
	f.recordLocalSpecialLocked(path, attr)
	f.specialMu.Unlock()
}

func (f *Localize) recordLocalSpecialLocked(path string, attr fs.Attr) {
	if f.specialNode == nil {
		f.specialNode = map[string]string{}
	}
	if f.specialAttrs == nil {
		f.specialAttrs = map[string]fs.Attr{}
	}
	node, ok := f.specialNode[path]
	if !ok {
		node = f.nextSpecialNodeLocked()
		f.specialNode[path] = node
	}
	f.specialAttrs[node] = attr
}

func (f *Localize) nextSpecialNodeLocked() string {
	f.specialNextID++
	return "s" + strconv.FormatUint(f.specialNextID, 10)
}

// createLocalSpecial registers a NEWLY created node under a fresh identity —
// even when the pathname was used before (a live alias may still reference
// the old node).
func (f *Localize) createLocalSpecial(path string, attr fs.Attr) {
	f.specialMu.Lock()
	if f.specialNode == nil {
		f.specialNode = map[string]string{}
	}
	if f.specialAttrs == nil {
		f.specialAttrs = map[string]fs.Attr{}
	}
	node := f.nextSpecialNodeLocked()
	f.specialNode[path] = node
	f.specialAttrs[node] = attr
	f.specialMu.Unlock()
}

// linkLocalSpecial makes dst an alias of src's node: both paths then answer
// the SAME authoritative attr.
func (f *Localize) linkLocalSpecial(dst, src string) {
	f.specialMu.Lock()
	if f.specialNode == nil {
		f.specialNode = map[string]string{}
	}
	node, ok := f.specialNode[src]
	if !ok {
		node = src
		f.specialNode[src] = node
	}
	f.specialNode[dst] = node
	f.specialMu.Unlock()
}

// localSpecialEntries lists the metadata-only children recorded directly
// under dir.
func (f *Localize) localSpecialEntries(dir string) []fs.Entry {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	f.specialMu.Lock()
	defer f.specialMu.Unlock()
	var out []fs.Entry
	for p, node := range f.specialNode {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		name := strings.TrimPrefix(p, prefix)
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		out = append(out, fs.Entry{Name: name, Attr: f.specialAttrs[node]})
	}
	return out
}

// localSpecialNodes lists the recorded special paths at p or below it.
func (f *Localize) localSpecialNodes(p string) []string {
	prefix := strings.TrimSuffix(p, "/") + "/"
	f.specialMu.Lock()
	defer f.specialMu.Unlock()
	var out []string
	for path := range f.specialNode {
		if path == p || strings.HasPrefix(path, prefix) {
			out = append(out, path)
		}
	}
	return out
}

// migrateLocalSpecialPrefix moves the recorded special nodes of a renamed
// path (a directory's descendants included) to the new prefix.
func (f *Localize) migrateLocalSpecialPrefix(oldPath, newPath string) {
	oldPrefix := strings.TrimSuffix(oldPath, "/") + "/"
	newPrefix := strings.TrimSuffix(newPath, "/") + "/"
	f.specialMu.Lock()
	defer f.specialMu.Unlock()
	moves := make(map[string]string)
	for p, node := range f.specialNode {
		switch {
		case p == oldPath:
			moves[newPath] = node
			delete(f.specialNode, p)
		case strings.HasPrefix(p, oldPrefix):
			moves[newPrefix+strings.TrimPrefix(p, oldPrefix)] = node
			delete(f.specialNode, p)
		}
	}
	for p, node := range moves {
		f.specialNode[p] = node
	}
}

// hasLocalSpecialUnder reports whether any local special node lives in the
// directory's subtree.
func (f *Localize) hasLocalSpecialUnder(dir string) bool {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	f.specialMu.Lock()
	defer f.specialMu.Unlock()
	for p := range f.specialNode {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

func (f *Localize) localSpecial(path string) (fs.Attr, bool) {
	f.specialMu.Lock()
	defer f.specialMu.Unlock()
	node, ok := f.specialNode[path]
	if !ok {
		return fs.Attr{}, false
	}
	attr, ok := f.specialAttrs[node]
	return attr, ok
}

// localSpecialNodeID returns the shared node id behind path.
func (f *Localize) localSpecialNodeID(path string) (string, bool) {
	f.specialMu.Lock()
	defer f.specialMu.Unlock()
	node, ok := f.specialNode[path]
	return node, ok
}

// refreshLocalSpecialNode re-reads a surviving node's authoritative attr
// (link count included) from the specials plane after one alias was
// removed.
func (f *Localize) refreshLocalSpecialNode(ctx context.Context, next fs.Next, node string) {
	if node == "" {
		return
	}
	f.specialMu.Lock()
	var survivor string
	for p, n := range f.specialNode {
		if n == node {
			survivor = p
			break
		}
	}
	_, alive := f.specialAttrs[node]
	f.specialMu.Unlock()
	if survivor == "" || !alive {
		return
	}
	got, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: survivor})
	if err != nil || got.Attr == nil {
		return
	}
	f.specialMu.Lock()
	if _, ok := f.specialAttrs[node]; ok {
		f.specialAttrs[node] = *got.Attr
	}
	f.specialMu.Unlock()
}

func (f *Localize) forgetLocalSpecial(path string) {
	f.specialMu.Lock()
	node, ok := f.specialNode[path]
	delete(f.specialNode, path)
	if ok {
		// The node's metadata goes away with its LAST alias; a sibling
		// hardlink keeps it.
		shared := false
		for _, n := range f.specialNode {
			if n == node {
				shared = true
				break
			}
		}
		if !shared {
			delete(f.specialAttrs, node)
		}
	}
	f.specialMu.Unlock()
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
		Mode:  localFileMode(info.Mode()),
		Size:  info.Size(),
		Mtime: info.ModTime(),
	}
}

// reservedViewPath reports whether the path belongs to the reserved
// control namespace, which never routes to a local plane.
func reservedViewPath(viewPath string) bool {
	return viewPath == "/.drive9" || strings.HasPrefix(viewPath, "/.drive9/")
}
