package fuse

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/dat9/pkg/pathutil"
)

// LocalOverlay stores local-only subtrees under a mount-scoped root. It is a
// backend, not a symlink layer: callers still operate on Drive9 local paths,
// while the backend maps those paths to localRoot/overlay.
type LocalOverlay struct {
	root string
}

func NewLocalOverlay(localRoot string) *LocalOverlay {
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return nil
	}
	return &LocalOverlay{root: filepath.Join(localRoot, "overlay")}
}

func (o *LocalOverlay) EnsureRoot() error {
	if o == nil {
		return nil
	}
	return os.MkdirAll(o.root, 0o755)
}

func (o *LocalOverlay) abs(localPath string) (string, error) {
	if o == nil {
		return "", syscall.EIO
	}
	canonical, err := pathutil.Canonicalize(localPath)
	if err != nil {
		return "", err
	}
	rel := strings.TrimPrefix(canonical, "/")
	if rel == "" {
		return o.root, nil
	}
	return filepath.Join(o.root, filepath.FromSlash(rel)), nil
}

func (o *LocalOverlay) Lstat(localPath string) (fs.FileInfo, error) {
	abs, err := o.abs(localPath)
	if err != nil {
		return nil, err
	}
	return os.Lstat(abs)
}

func (o *LocalOverlay) Mkdir(localPath string, mode uint32) error {
	abs, err := o.abs(localPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	return os.Mkdir(abs, fs.FileMode(mode&0o777))
}

func (o *LocalOverlay) OpenFile(localPath string, flags uint32, mode uint32) (*os.File, error) {
	abs, err := o.abs(localPath)
	if err != nil {
		return nil, err
	}
	if flags&syscall.O_CREAT != 0 {
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(abs, int(flags), fs.FileMode(mode&0o777))
}

func (o *LocalOverlay) Symlink(target, localPath string) error {
	abs, err := o.abs(localPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, abs)
}

func (o *LocalOverlay) Link(oldPath, newPath string) error {
	oldAbs, err := o.abs(oldPath)
	if err != nil {
		return err
	}
	newAbs, err := o.abs(newPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(newAbs), 0o755); err != nil {
		return err
	}
	return os.Link(oldAbs, newAbs)
}

func (o *LocalOverlay) Readlink(localPath string) (string, error) {
	abs, err := o.abs(localPath)
	if err != nil {
		return "", err
	}
	return os.Readlink(abs)
}

func (o *LocalOverlay) Remove(localPath string) error {
	abs, err := o.abs(localPath)
	if err != nil {
		return err
	}
	return os.Remove(abs)
}

func (o *LocalOverlay) Rename(oldPath, newPath string) error {
	oldAbs, err := o.abs(oldPath)
	if err != nil {
		return err
	}
	newAbs, err := o.abs(newPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(newAbs), 0o755); err != nil {
		return err
	}
	return os.Rename(oldAbs, newAbs)
}

func (o *LocalOverlay) Chmod(localPath string, mode uint32) error {
	abs, err := o.abs(localPath)
	if err != nil {
		return err
	}
	return os.Chmod(abs, fs.FileMode(mode&0o777))
}

func (o *LocalOverlay) Truncate(localPath string, size int64) error {
	abs, err := o.abs(localPath)
	if err != nil {
		return err
	}
	return os.Truncate(abs, size)
}

func (o *LocalOverlay) Chtimes(localPath string, mtime time.Time) error {
	abs, err := o.abs(localPath)
	if err != nil {
		return err
	}
	return os.Chtimes(abs, mtime, mtime)
}

func (o *LocalOverlay) ReadDir(localPath string) ([]localOverlayEntry, error) {
	abs, err := o.abs(localPath)
	if err != nil {
		return nil, err
	}
	items, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}
	entries := make([]localOverlayEntry, 0, len(items))
	for _, item := range items {
		info, err := item.Info()
		if err != nil {
			return nil, err
		}
		entries = append(entries, localOverlayEntry{Name: item.Name(), Info: info})
	}
	return entries, nil
}

type localOverlayEntry struct {
	Name string
	Info fs.FileInfo
}

func entryFromLocalInfo(localPath string, info fs.FileInfo) *InodeEntry {
	mode, hasMode, isDir := inodeModeFromFileInfo(info)
	return &InodeEntry{
		Path:    localPath,
		IsDir:   isDir,
		Size:    info.Size(),
		Mtime:   info.ModTime(),
		Mode:    mode,
		HasMode: hasMode,
	}
}

func dirEntryFromLocalInfo(name string, ino uint64, info fs.FileInfo) DirEntry {
	mode, hasMode, isDir := inodeModeFromFileInfo(info)
	return DirEntry{
		Name:        name,
		Ino:         ino,
		Mode:        dirEntryMode(isDir, hasMode, mode),
		Size:        info.Size(),
		Mtime:       info.ModTime(),
		AttrMode:    mode,
		HasMode:     hasMode,
		IsDir:       isDir,
		HasMetadata: true,
	}
}

func inodeModeFromFileInfo(info fs.FileInfo) (mode uint32, hasMode bool, isDir bool) {
	if info == nil {
		return 0, false, false
	}
	perm := uint32(info.Mode().Perm())
	switch {
	case info.IsDir():
		return perm, true, true
	case info.Mode()&fs.ModeSymlink != 0:
		return uint32(syscall.S_IFLNK) | perm, true, false
	default:
		return perm, true, false
	}
}

func localErrToFuseStatus(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	if errors.Is(err, os.ErrNotExist) {
		return gofuse.ENOENT
	}
	if errors.Is(err, os.ErrPermission) {
		return gofuse.EACCES
	}
	if errors.Is(err, os.ErrExist) {
		return gofuse.Status(syscall.EEXIST)
	}
	if errors.Is(err, io.EOF) {
		return gofuse.OK
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return gofuse.Status(errno)
	}
	return gofuse.EIO
}
