package localize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// pruneBackingDirs recursively removes empty, remote-classified backing
// directories under viewPath. The walk opens every component without
// following symlinks so nothing outside the backing tree can redirect the
// cleanup, and directory-relative removal refuses a directory that acquired
// a concurrent child.
func (f *Localize) pruneBackingDirs(inner fs.Filesystem, viewPath string) error {
	abs, err := f.overlay.abs(viewPath)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(f.overlay.root, abs)
	if err != nil {
		return err
	}
	const flags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Open(f.overlay.root, flags, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(fd), f.overlay.root)
	defer func() { _ = dir.Close() }()
	for _, name := range strings.Split(rel, string(os.PathSeparator)) {
		childFD, err := unix.Openat(int(dir.Fd()), name, flags, 0)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		_ = dir.Close()
		dir = os.NewFile(uintptr(childFD), filepath.Join(dir.Name(), name))
	}
	var prune func(parent *os.File, parentPath string) error
	prune = func(parent *os.File, parentPath string) error {
		names, err := parent.Readdirnames(-1)
		if err != nil {
			return err
		}
		for _, name := range names {
			childPath := joinDir(parentPath, name)
			// Real local-classified content (or a live sidecar) refuses the
			// replacement; only remote-classified scaffolding is removable.
			if f.classifiesLocal(inner, childPath, true) || f.transientFor(childPath, true) != nil {
				return syscall.ENOTEMPTY
			}
			childFD, err := unix.Openat(int(parent.Fd()), name, flags, 0)
			if os.IsNotExist(err) {
				continue
			}
			if errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
				return syscall.ENOTEMPTY
			}
			if err != nil {
				return err
			}
			child := os.NewFile(uintptr(childFD), filepath.Join(parent.Name(), name))
			err = prune(child, childPath)
			_ = child.Close()
			if err != nil {
				return err
			}
			if err := unix.Unlinkat(int(parent.Fd()), name, unix.AT_REMOVEDIR); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		return nil
	}
	return prune(dir, viewPath)
}

// pruneRenameDestParents removes invisible, empty backing directories from
// a remote rename's destination before the rename commits, and rejects the
// rename with ENOTEMPTY when the backing there holds real local-classified
// entries the replacement would orphan. Files, symlinks and visible
// directories are never removed. The remote probes are spent only when the
// backing actually holds something at the destination — the no-backing
// case is the common one and stays probe-free.
func (f *Localize) pruneRenameDestParents(ctx context.Context, inner fs.Filesystem, oldP, newP string) error {
	if f.overlay == nil {
		return nil
	}
	if _, err := f.overlay.Lstat(newP); err != nil {
		return nil
	}
	oldAttr, err := inner.GetAttr(ctx, oldP)
	if err != nil {
		return err
	}
	if oldAttr.Kind != fs.KindDir {
		return nil
	}
	if st, err := inner.GetAttr(ctx, newP); err == nil && st.Kind != fs.KindDir {
		return nil
	}
	if err := f.pruneBackingDirs(inner, newP); err != nil {
		if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
			return syscall.ENOTEMPTY
		}
		return err
	}
	return nil
}

// renameOverlaySubtree moves a backing subtree from oldP to newP around a
// remote rename. A remote-classified directory can still hold local-only
// descendants (a remotely created staging directory containing
// node_modules), and the directory's own classification says nothing about
// its children — the backing subtree must move with the rename or those
// files stay orphaned at the stale path. Missing or empty backing
// directories are a no-op: the backing root contains implicit parent
// directories created for local files, and moving an empty one is
// pointless. Overlapping source and destination trees are left alone.
func (f *Localize) renameOverlaySubtree(oldP, newP string) error {
	if f == nil || f.overlay == nil {
		return nil
	}
	if strings.HasPrefix(newP, oldP+"/") || strings.HasPrefix(oldP, newP+"/") {
		return nil
	}
	info, err := f.overlay.Lstat(oldP)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return nil
	}
	entries, err := f.overlay.ReadDir(oldP)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	// rename(2) atomically replaces an empty destination directory and
	// rejects a non-empty one, which preserves both trees if a new child
	// raced in.
	return f.overlay.Rename(oldP, newP)
}
