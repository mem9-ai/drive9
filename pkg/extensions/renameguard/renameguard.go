// Package renameguard enforces the POSIX rename preflight: the kernel does not enforce permission or
// emptiness rules on the server side, so renames are validated (EACCES/
// EPERM via sticky parents, EISDIR/ENOTDIR on target class, ENOTEMPTY on
// non-empty target directories, EINVAL into one's own subtree) before the
// mutation reaches the chain below. Attribute lookups dispatch through the
// inner chain, so lazy-create overlays answer for not-yet-committed files.
package renameguard

import (
	"context"
	"path"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
)

const (
	stickyPermissionBit    uint32 = 0o1000
	defaultRegularFileMode uint32 = 0o644
)

// Options configures the guard.
type Options struct {
	// Uid/Gid identify the mount owner. Renamed entries resolve to this
	// ownership (the mount carries no per-caller identity table),
	// which is what the sticky-bit comparison tests against.
	Uid uint32
	Gid uint32
	// OwnerOf resolves a path's ownership from the mount's local inode
	// table (v1's authority for chown — the backend has no owner verb and
	// remote stats report the mount actor). The sticky-bit comparison must
	// test the LOCAL owner: a chain stat racing listing invalidation or
	// replica lag reports a foreign owner and EPERMs a rename the local
	// view allows (rename/09). Nil falls back to the chain's Attr owner.
	OwnerOf func(path string) (uid, gid uint32, hasUID, hasGID bool)
}

// RenameGuard is the fs.Handler enforcing the preflight on OpRename.
type RenameGuard struct {
	opts Options
}

func New(opts Options) (*RenameGuard, error) {
	return &RenameGuard{opts: opts}, nil
}

// SetOwnerLookup wires the facade's inode-table owner probe after assembly.
func (g *RenameGuard) SetOwnerLookup(fn func(path string) (uid, gid uint32, hasUID, hasGID bool)) {
	g.opts.OwnerOf = fn
}

func (g *RenameGuard) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if op.Kind != fs.OpRename {
		return next(ctx, op)
	}
	if err := g.preflight(ctx, op, next); err != nil {
		return fs.Result{}, err
	}
	return next(ctx, op)
}

type pathInfo struct {
	path      string
	exists    bool
	mode      uint32
	hasMode   bool
	isDir     bool
	isSymlink bool
	uid       uint32
	gid       uint32
}

// modeOrDefault assumes unknown modes assume 0755 dirs / 0644 files.
func (i pathInfo) modeOrDefault() uint32 {
	if i.hasMode {
		return i.mode
	}
	if i.isDir {
		return uint32(syscall.S_IFDIR) | 0o755
	}
	return uint32(syscall.S_IFREG) | defaultRegularFileMode
}

// stat resolves one path's class/ownership through the inner chain; a
// missing path is a valid answer (exists=false), any other error fails the
// preflight.
func (g *RenameGuard) stat(ctx context.Context, next fs.Next, p string) (pathInfo, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: p})
	if err != nil {
		if fs.IsNotFound(err) {
			return pathInfo{path: p}, nil
		}
		return pathInfo{path: p}, err
	}
	if res.Attr == nil {
		return pathInfo{path: p}, nil
	}
	uid, gid := g.opts.Uid, g.opts.Gid
	if g.opts.OwnerOf != nil {
		// The mount's local ownership is the sticky-comparison authority
		// (v1's inode table): it reflects chown/chmod the chain's stat view
		// may not have seen yet.
		if luid, lgid, hasUID, hasGID := g.opts.OwnerOf(p); hasUID || hasGID {
			if hasUID {
				uid = luid
			}
			if hasGID {
				gid = lgid
			}
		} else if res.Attr.HasOwner {
			uid, gid = res.Attr.Uid, res.Attr.Gid
		}
	} else if res.Attr.HasOwner {
		// The stat carries the entry's own identity (facade stampOwner,
		// specials records): class checks and sticky comparisons test
		// against it, falling back to the mount identity only when the
		// chain had no owner to report.
		uid, gid = res.Attr.Uid, res.Attr.Gid
	}
	return pathInfo{
		path:      p,
		exists:    true,
		mode:      res.Attr.Mode,
		hasMode:   res.Attr.Mode != 0,
		isDir:     res.Attr.IsDir(),
		isSymlink: res.Attr.IsSymlink() || res.Attr.Mode&syscall.S_IFMT == syscall.S_IFLNK,
		uid:       uid,
		gid:       gid,
	}, nil
}

// checkParentAccess checks W|X on the parent directory by permission
// class (owner/group/other; group membership reduces to the mount gid —
// no supplementary groups here).
func checkParentAccess(caller fs.Caller, ownerUid, ownerGid uint32, parent pathInfo) error {
	if !parent.exists {
		return syscall.ENOENT
	}
	if caller.Uid == 0 {
		return nil
	}
	mode := parent.modeOrDefault() & 0o777
	shift := uint32(0) // other class
	if caller.Uid == ownerUid {
		shift = 6
	} else if caller.Gid == ownerGid {
		shift = 3
	}
	const want = uint32(0o2 | 0o1) // W_OK | X_OK
	if mode>>shift&want != want {
		return syscall.EACCES
	}
	return nil
}

// checkSticky enforces the sticky bit: on a sticky directory the caller
// must own either the directory or the moved entry (root bypasses).
func checkSticky(callerUid uint32, parent, victim pathInfo) error {
	if parent.modeOrDefault()&stickyPermissionBit == 0 {
		return nil
	}
	if callerUid == 0 || callerUid == parent.uid || callerUid == victim.uid {
		return nil
	}
	return syscall.EPERM
}

func parentOf(p string) string {
	dir := path.Dir(p)
	if dir == "." || dir == "" {
		return "/"
	}
	return dir
}

// preflight validates the rename in POSIX check order.
func (g *RenameGuard) preflight(ctx context.Context, op *fs.Op, next fs.Next) error {
	oldInfo, err := g.stat(ctx, next, op.Path)
	if err != nil {
		return err
	}
	if !oldInfo.exists {
		return syscall.ENOENT
	}
	if oldInfo.isDir && strings.HasPrefix(op.Path2, strings.TrimSuffix(op.Path, "/")+"/") {
		return syscall.EINVAL
	}

	oldParent, newParent := parentOf(op.Path), parentOf(op.Path2)
	oldParentInfo, err := g.stat(ctx, next, oldParent)
	if err != nil {
		return err
	}
	if !oldParentInfo.exists || !oldParentInfo.isDir {
		return syscall.ENOENT
	}
	newParentInfo := oldParentInfo
	if newParent != oldParent {
		newParentInfo, err = g.stat(ctx, next, newParent)
		if err != nil {
			return err
		}
		// A symlinked new parent is ELOOP even before the existence
		// check reports ENOENT.
		if newParentInfo.exists && newParentInfo.isSymlink {
			return syscall.ELOOP
		}
		if !newParentInfo.exists || !newParentInfo.isDir {
			return syscall.ENOENT
		}
	}

	caller := op.Caller
	if err := checkParentAccess(caller, oldParentInfo.uid, oldParentInfo.gid, oldParentInfo); err != nil {
		return err
	}
	if newParent != oldParent {
		if err := checkParentAccess(caller, newParentInfo.uid, newParentInfo.gid, newParentInfo); err != nil {
			return err
		}
	}
	if err := checkSticky(caller.Uid, oldParentInfo, oldInfo); err != nil {
		return err
	}
	// Renaming through a symlinked old parent is ELOOP, checked after
	// the sticky test.
	if oldParentInfo.exists && oldParentInfo.isSymlink {
		return syscall.ELOOP
	}

	newInfo, err := g.stat(ctx, next, op.Path2)
	if err != nil {
		return err
	}
	if newInfo.exists {
		switch {
		case oldInfo.isDir && !newInfo.isDir:
			return syscall.ENOTDIR
		case !oldInfo.isDir && newInfo.isDir:
			return syscall.EISDIR
		case oldInfo.isDir && newInfo.isDir:
			empty, err := g.dirEmpty(ctx, next, op.Path2)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		if err := checkSticky(caller.Uid, newParentInfo, newInfo); err != nil {
			return err
		}
	}
	return nil
}

// dirEmpty reports whether a target directory is empty in the chain's
// listing.
func (g *RenameGuard) dirEmpty(ctx context.Context, next fs.Next, p string) (bool, error) {
	res, err := next(ctx, &fs.Op{Kind: fs.OpList, Path: p})
	if err != nil {
		if fs.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	for _, e := range res.Entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		return false, nil
	}
	return true, nil
}
