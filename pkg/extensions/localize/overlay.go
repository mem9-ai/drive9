package localize

import (
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// Overlay is the on-disk backing store for local-classified subtrees. It is
// a backend, not a symlink layer: callers keep operating on view paths
// while the overlay maps them to <root>/<view path>. A nil *Overlay is a
// valid disabled overlay (operations fail with EIO through abs).
type Overlay struct {
	root string
}

// NewOverlay returns an overlay rooted at <localRoot>/overlay, or nil for a
// blank localRoot (local routing disabled).
func NewOverlay(localRoot string) *Overlay {
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return nil
	}
	return &Overlay{root: filepath.Join(localRoot, "overlay")}
}

// Chmod changes the backing file's permission (and setuid/setgid/sticky)
// bits for viewPath.
func (o *Overlay) Chmod(viewPath string, mode uint32) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	return os.Chmod(abs, posixFileMode(mode))
}

// Chtimes sets the backing file's access and modification times.
func (o *Overlay) Chtimes(viewPath string, mtime time.Time) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	return os.Chtimes(abs, mtime, mtime)
}

// EnsureRoot creates the backing root directory.
func (o *Overlay) EnsureRoot() error {
	if o == nil {
		return nil
	}
	return os.MkdirAll(o.root, 0o755)
}

// Link creates a hard link between two view paths.
func (o *Overlay) Link(oldPath, newPath string) error {
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

// Lstat stats the backing file without following a trailing symlink.
func (o *Overlay) Lstat(viewPath string) (iofs.FileInfo, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return nil, err
	}
	return os.Lstat(abs)
}

// Mkdir creates the backing directory, materializing implicit parents.
func (o *Overlay) Mkdir(viewPath string, mode uint32) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	return os.Mkdir(abs, posixFileMode(mode))
}

// OpenFile opens (and with O_CREAT, creates) the backing file, materializing
// implicit parent directories for creation.
func (o *Overlay) OpenFile(viewPath string, flags uint32, mode uint32) (*os.File, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return nil, err
	}
	if flags&syscall.O_CREAT != 0 {
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(abs, int(flags), posixFileMode(mode))
}

// ReadDir lists the backing directory.
func (o *Overlay) ReadDir(viewPath string) ([]os.DirEntry, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return nil, err
	}
	return os.ReadDir(abs)
}

// Readlink reads the symlink target at viewPath.
func (o *Overlay) Readlink(viewPath string) (string, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return "", err
	}
	return os.Readlink(abs)
}

// Remove unlinks a file, or removes an empty directory (os.Remove falls
// back to rmdir when the path is a directory; a non-empty one reports
// ENOTEMPTY from the syscall).
func (o *Overlay) Remove(viewPath string) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	return os.Remove(abs)
}

// Rename moves a backing path. rename(2) atomically replaces an existing
// empty destination directory and rejects a non-empty one, which preserves
// both trees when a child races in.
func (o *Overlay) Rename(oldPath, newPath string) error {
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

// Root reports the backing root path ("" for a disabled overlay).
func (o *Overlay) Root() string {
	if o == nil {
		return ""
	}
	return o.root
}

// Symlink creates a backing symlink at viewPath pointing to target.
func (o *Overlay) Symlink(target, viewPath string) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, abs)
}

// SyncDir flushes the backing directory entry for viewPath (fsync on the
// open directory) so renames and unlinks inside it reach disk.
func (o *Overlay) SyncDir(viewPath string) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	dir, err := os.Open(abs)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

// Truncate resizes the backing file.
func (o *Overlay) Truncate(viewPath string, size int64) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	return os.Truncate(abs, size)
}

func (o *Overlay) abs(viewPath string) (string, error) {
	if o == nil {
		return "", syscall.EIO
	}
	canonical, err := pathutil.Canonicalize(viewPath)
	if err != nil {
		return "", err
	}
	rel := strings.TrimPrefix(canonical, "/")
	if rel == "" {
		return o.root, nil
	}
	return filepath.Join(o.root, filepath.FromSlash(rel)), nil
}

// posixFileMode maps a POSIX mode word to an io/fs FileMode, carrying the
// setuid, setgid and sticky bits through.
func posixFileMode(mode uint32) iofs.FileMode {
	fm := iofs.FileMode(mode & 0o777)
	if mode&0o4000 != 0 {
		fm |= iofs.ModeSetuid
	}
	if mode&0o2000 != 0 {
		fm |= iofs.ModeSetgid
	}
	if mode&0o1000 != 0 {
		fm |= iofs.ModeSticky
	}
	return fm
}

// localFileMode is posixFileMode's inverse for reply attributes:
// FileMode.Perm() masks the three special bits, so a successful
// chmod(04755/01777) read back as 0755/0777 and every Lookup/GetAttr/
// listing dropped the marks the overlay had just stored (renameguard's
// sticky preflight then saw a non-sticky parent).
func localFileMode(fm iofs.FileMode) uint32 {
	mode := uint32(fm.Perm())
	if fm&iofs.ModeSetuid != 0 {
		mode |= 0o4000
	}
	if fm&iofs.ModeSetgid != 0 {
		mode |= 0o2000
	}
	if fm&iofs.ModeSticky != 0 {
		mode |= 0o1000
	}
	return mode
}
