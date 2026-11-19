package localize

import (
	"errors"
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
	// quota bounds the backing tree's bytes and the host filesystem's free
	// space (nil disables both). The local overlay writes straight to the
	// host disk, so this is the only guard between a local-only subtree and
	// a full disk.
	quota *overlayQuota
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

// SetQuota installs the overlay's byte quota and free-space ratio. A
// non-positive limit with a non-positive ratio leaves the overlay unbounded.
func (o *Overlay) SetQuota(limit int64, freeRatio float64) error {
	if o == nil {
		return nil
	}
	q, err := newOverlayQuota(o.root, limit, freeRatio)
	if err != nil {
		return err
	}
	o.quota = q
	return nil
}

// Quota reports the overlay's quota (nil when disabled).
func (o *Overlay) Quota() *overlayQuota {
	if o == nil {
		return nil
	}
	return o.quota
}

// ChargeGrowth reserves growth for a TRUNCATING path write, which has no base a
// booking could settle against (the file is about to be reset): the caller
// releases the unlanded remainder. ENOSPC means the quota or the free-space
// floor refused.
func (o *Overlay) ChargeGrowth(viewPath string, growth int64) error {
	return o.withQuota(viewPath, func(q *overlayQuota, _ string) error {
		return q.charge(growth)
	})
}

// AdmitFileWrite reserves the space a write through an OPEN descriptor needs.
// The size comes from the descriptor and the reservation is keyed by the file's
// identity, so a rename between admission and write cannot move the accounting
// to whatever now sits at the old path.
func (o *Overlay) AdmitFileWrite(f *os.File, viewPath string, end, writeLen int64) (writeReservation, error) {
	var res writeReservation
	err := o.withQuota(viewPath, func(q *overlayQuota, rel string) error {
		var qerr error
		res, qerr = q.admitFileWrite(f, rel, end, writeLen)
		return qerr
	})
	return res, err
}

// SettleWrite settles a write's booking against the file's resulting size. The
// reservation carries the descriptor for handle writes, so settlement cannot be
// redirected by a rename; a path write is settled from the backing path.
func (o *Overlay) SettleWrite(res writeReservation) {
	if o == nil || o.quota == nil {
		return
	}
	o.quota.settleWrite(res)
}

// RetirePath retires a name's bookings against size — the size the file had AT
// the mutation boundary — and records baseline, the size it has after the
// mutation: 0 when the mutation also released the name's bytes (unlink, a
// truncating open), the post-resize size for a truncate, or the moved size for a
// rename. A writer retired here that lands afterwards is charged only the growth
// beyond baseline.
func (o *Overlay) RetirePath(viewPath string, size, baseline int64) {
	if o == nil || o.quota == nil {
		return
	}
	if rel, err := o.rel(viewPath); err == nil {
		o.quota.retirePath(rel, size, baseline)
	}
}

// RetireFile retires the bookings for a file whose identity is known (an
// unlink or rename holds the FileInfo): the inode-keyed booking an admission
// made through any hardlink alias is retired together with the name-indexed
// ones.
func (o *Overlay) RetireFile(info iofs.FileInfo, key, viewPath string, size, baseline int64) {
	if o == nil || o.quota == nil {
		return
	}
	rel, err := o.rel(viewPath)
	if err != nil {
		rel = ""
	}
	if key == "" && info != nil {
		if id, ok := fileReservationKey(info); ok {
			key = id
		}
	}
	o.quota.retireIdentity(key, rel, size, baseline)
}

// RetireMutation retires the writers of viewPath after a mutation LANDED:
// the name-indexed bookings plus the inode booking an admission through a
// different hardlink alias would have created. size is the file's size at the
// mutation boundary, baseline the size it has after it.
func (o *Overlay) RetireMutation(viewPath string, size, baseline int64) {
	if o == nil || o.quota == nil {
		return
	}
	var info iofs.FileInfo
	if fi, err := o.Lstat(viewPath); err == nil {
		info = fi
	}
	o.RetireFile(info, "", viewPath, size, baseline)
}

// RetirePathNow retires a name's bookings against its CURRENT on-disk size
// (baseline = that same size: the bytes stay counted).
func (o *Overlay) RetirePathNow(viewPath string) {
	if o == nil || o.quota == nil {
		return
	}
	size := int64(0)
	if rel, err := o.rel(viewPath); err == nil {
		if cur, serr := statBackingSize(o.root, rel); serr == nil {
			size = cur
		}
	}
	o.RetirePath(viewPath, size, size)
}

// rel resolves viewPath to its backing-relative path.
func (o *Overlay) rel(viewPath string) (string, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return "", err
	}
	return filepath.Rel(o.root, abs)
}

// AdmitResize reserves (or returns) the space a truncate to newSize needs.
func (o *Overlay) AdmitResize(viewPath string, newSize int64) (resizeReservation, error) {
	var res resizeReservation
	err := o.withQuota(viewPath, func(q *overlayQuota, rel string) error {
		var qerr error
		res, qerr = q.resize(rel, newSize)
		return qerr
	})
	return res, err
}

// ReconcileResize undoes a reservation whose truncate then failed: the file is
// unchanged, so admitted growth is returned and released shrinkage is counted
// again (forced — the bytes are still on disk).
func (o *Overlay) ReconcileResize(res resizeReservation) {
	if o == nil || o.quota == nil {
		return
	}
	o.quota.release(res.admitted)
	if res.released > 0 {
		o.quota.forceAdmit(res.released)
	}
}

// withQuota resolves viewPath to its backing-relative path and runs fn when a
// quota is armed.
func (o *Overlay) withQuota(viewPath string, fn func(*overlayQuota, string) error) error {
	if o == nil || o.quota == nil {
		return nil
	}
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(o.root, abs)
	if err != nil {
		return err
	}
	return fn(o.quota, rel)
}

// Size reports the backing file's current size (0 when it does not exist).
// It never touches the quota; the callers that need to give bytes back read
// the size BEFORE the operation that frees them, then call ReleaseBytes.
func (o *Overlay) Size(viewPath string) (int64, error) {
	if o == nil {
		return 0, syscall.EIO
	}
	abs, err := o.abs(viewPath)
	if err != nil {
		return 0, err
	}
	rel, err := filepath.Rel(o.root, abs)
	if err != nil {
		return 0, err
	}
	return statBackingSize(o.root, rel)
}

// ReleaseBytes returns n bytes of already-freed backing storage to the quota
// (a truncating open, an unlink, or an admitted write that failed).
func (o *Overlay) ReleaseBytes(n int64) {
	if o == nil || o.quota == nil {
		return
	}
	o.quota.release(n)
}

// Chmod changes the backing file's permission (and setuid/setgid/sticky)
// bits for viewPath.
func (o *Overlay) Chmod(viewPath string, mode uint32) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	err = os.Chmod(abs, posixFileMode(mode))
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Chmod(rel, posixFileMode(mode))
}

// Chtimes sets the backing file's access and modification times.
func (o *Overlay) Chtimes(viewPath string, mtime time.Time) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	err = os.Chtimes(abs, mtime, mtime)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Chtimes(rel, mtime, mtime)
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
	if err := o.ensureParent(newAbs); err != nil {
		return err
	}
	err = os.Link(oldAbs, newAbs)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, oldRel, err := o.openPathRoot(oldAbs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	newRel, err := filepath.Rel(o.root, newAbs)
	if err != nil {
		return err
	}
	return root.Link(oldRel, newRel)
}

// Lstat stats the backing file without following a trailing symlink.
func (o *Overlay) Lstat(viewPath string) (iofs.FileInfo, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(abs)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return info, err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return root.Lstat(rel)
}

// Mkdir creates the backing directory, materializing implicit parents.
func (o *Overlay) Mkdir(viewPath string, mode uint32) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	if err := o.ensureParent(abs); err != nil {
		return err
	}
	err = os.Mkdir(abs, posixFileMode(mode))
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Mkdir(rel, posixFileMode(mode))
}

// OpenFile opens (and with O_CREAT, creates) the backing file, materializing
// implicit parent directories for creation.
func (o *Overlay) OpenFile(viewPath string, flags uint32, mode uint32) (*os.File, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return nil, err
	}
	if flags&syscall.O_CREAT != 0 {
		if err := o.ensureParent(abs); err != nil {
			return nil, err
		}
	}
	return o.openFile(abs, int(flags), posixFileMode(mode))
}

func (o *Overlay) openFile(abs string, flags int, mode os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(abs, flags, mode)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return file, err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return root.OpenFile(rel, flags, mode)
}

// ReadDir lists the backing directory.
func (o *Overlay) ReadDir(viewPath string) ([]os.DirEntry, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(abs)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return entries, err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return iofs.ReadDir(root.FS(), rel)
}

// Readlink reads the symlink target at viewPath.
func (o *Overlay) Readlink(viewPath string) (string, error) {
	abs, err := o.abs(viewPath)
	if err != nil {
		return "", err
	}
	target, err := os.Readlink(abs)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return target, err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return "", err
	}
	defer func() { _ = root.Close() }()
	return root.Readlink(rel)
}

// Remove unlinks a file, or removes an empty directory (os.Remove falls
// back to rmdir when the path is a directory; a non-empty one reports
// ENOTEMPTY from the syscall).
func (o *Overlay) Remove(viewPath string) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	err = os.Remove(abs)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Remove(rel)
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
	if err := o.ensureParent(newAbs); err != nil {
		return err
	}
	// A rename REPLACES an existing destination name (POSIX), and the
	// replaced file's bytes are freed on disk: give them back to the quota
	// once the rename succeeded. A rename of one hardlink over another name of
	// the same inode frees nothing, so the identities are compared first.
	replacedBytes, releaseDest := int64(0), false
	srcSize, srcKnown := int64(0), false
	dstSize, dstKnown := int64(0), false
	if o.quota != nil {
		if srcInfo, serr := os.Lstat(oldAbs); serr == nil && srcInfo.Mode().IsRegular() {
			srcSize, srcKnown = srcInfo.Size(), true
		}
		if dstInfo, derr := os.Lstat(newAbs); derr == nil && dstInfo.Mode().IsRegular() {
			dstSize, dstKnown = dstInfo.Size(), true
			sameInode := false
			if srcInfo, serr := os.Lstat(oldAbs); serr == nil {
				sameInode = os.SameFile(srcInfo, dstInfo)
			}
			// The replaced NAME goes away, but the inode's bytes only leave
			// the quota when no other hardlink keeps them: releasing on a
			// shared inode would under-count the surviving aliases.
			if !sameInode && backingLinkCount(dstInfo) <= 1 {
				replacedBytes, releaseDest = dstInfo.Size(), true
			}
		}
	}
	err = os.Rename(oldAbs, newAbs)
	if errors.Is(err, syscall.ENAMETOOLONG) {
		err = o.renameLongPath(oldAbs, newAbs)
	}
	if err == nil {
		if releaseDest {
			o.ReleaseBytes(replacedBytes)
		}
		// Each name's bookings are retired against THAT NAME's pre-rename size:
		// the source name is gone (its writers were measured at srcSize) and the
		// replaced destination's writers were measured at dstSize (0 when
		// nothing was replaced). Using the source's size for the destination
		// over-refunded by dstSize-srcSize.
		if srcKnown {
			o.RetirePath(oldPath, srcSize, srcSize)
		}
		if dstKnown {
			// The destination's bytes were released above only when nothing else
			// keeps the inode; otherwise they stay charged, so its retired
			// writers must not be re-charged for them either.
			dstBaseline := int64(0)
			if !releaseDest {
				dstBaseline = dstSize
			}
			o.RetirePath(newPath, dstSize, dstBaseline)
		} else if !srcKnown {
			o.RetirePathNow(newPath)
		}
	}
	return err
}

// openPathRoot resolves long backing paths relative to the overlay directory.
// Root walks individual components on Unix, so adding the local-root prefix
// cannot turn a valid remote path into an ENAMETOOLONG overlay probe. It still
// reports invalid components and I/O errors, and keeps resolution in the root.
func (o *Overlay) openPathRoot(abs string) (*os.Root, string, error) {
	rel, err := filepath.Rel(o.root, abs)
	if err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(o.root)
	return root, rel, err
}

func (o *Overlay) renameLongPath(oldAbs, newAbs string) error {
	root, oldRel, err := o.openPathRoot(oldAbs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	newRel, err := filepath.Rel(o.root, newAbs)
	if err != nil {
		return err
	}
	return root.Rename(oldRel, newRel)
}

func (o *Overlay) ensureParent(abs string) error {
	err := os.MkdirAll(filepath.Dir(abs), 0o755)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.MkdirAll(filepath.Dir(rel), 0o755)
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
	if err := o.ensureParent(abs); err != nil {
		return err
	}
	err = os.Symlink(target, abs)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	root, rel, err := o.openPathRoot(abs)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return root.Symlink(target, rel)
}

// SyncDir flushes the backing directory entry for viewPath (fsync on the
// open directory) so renames and unlinks inside it reach disk.
func (o *Overlay) SyncDir(viewPath string) error {
	abs, err := o.abs(viewPath)
	if err != nil {
		return err
	}
	dir, err := o.openFile(abs, os.O_RDONLY, 0)
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
	err = os.Truncate(abs, size)
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		return err
	}
	file, err := o.openFile(abs, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Truncate(size)
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

// backingLinkCount reports the backing file's hardlink count (1 when the
// platform does not expose it on FileInfo.Sys).
func backingLinkCount(info iofs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink > 0 {
		return uint64(st.Nlink)
	}
	return 1
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
