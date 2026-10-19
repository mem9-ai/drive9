package fs

import (
	"context"
	"io"
	"net"

	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Sentinel errors of the chain. Handlers return them (or errors wrapping
// them); StatusOf maps each onto its canonical kernel errno.
var (
	// ErrNotFound reports a missing path.
	ErrNotFound = errors.New("fs: not found")
	// ErrUnsupported reports an operation the stack cannot serve.
	ErrUnsupported = errors.New("fs: operation not supported")
	// ErrNoCore reports chain dispatch running past the last handler —
	// a terminal handler called next.
	ErrNoCore = errors.New("fs: terminal handler called next")
)

// Kind classifies a filesystem node.
type Kind uint8

// Node kinds.
const (
	KindFile Kind = iota
	KindDir
	KindSymlink
)

// String returns the kind's name ("file", "dir", "symlink").
func (k Kind) String() string {
	switch k {
	case KindDir:
		return "dir"
	case KindSymlink:
		return "symlink"
	default:
		return "file"
	}
}

// Attr is the unified node-attribute vocabulary the facade and every
// handler exchange.
type Attr struct {
	Kind  Kind      // node classification
	Mode  uint32    // permission bits
	Size  int64     // file size in bytes
	Rev   int64     // server revision token (compare-and-set identity); 0 = unknown
	Mtime time.Time // modification time

	// Identity/ownership. Nlink/Rdev ride server stats when the backend
	// reports them (zero = unknown); Uid/Gid apply only when HasOwner is
	// set (the server sends no owner — chown is mount-local). Atime falls
	// back to Mtime at the facade.
	Nlink    uint32
	Uid      uint32
	Gid      uint32
	HasOwner bool
	Rdev     uint32
	Atime    time.Time
	// ExtentIno is the object-store inode the server reports for
	// extent-layout listing rows (0 = not an extent object). A listing row
	// is the only layout signal readdir gets: when the row carries the
	// inode the facade stamps it; when it does not, nothing may be invented
	// and stale routing must be cleared.
	ExtentIno uint64
	// ResourceID is the server's object identity for the path: two paths
	// sharing one are hardlinks of the same object and share a FUSE inode
	// (v1 LookupWithIdentity). Empty = unknown.
	ResourceID string
}

// IsDir reports whether the attribute describes a directory.
func (a Attr) IsDir() bool { return a.Kind == KindDir }

// IsSymlink reports whether the attribute describes a symlink.
func (a Attr) IsSymlink() bool { return a.Kind == KindSymlink }

// Entry is one name in a directory listing.
type Entry struct {
	Name string
	Attr Attr
	// Ino pins the FUSE inode a directory snapshot resolved the entry to.
	// Zero = unresolved; the consumer resolves it (the facade fills it in
	// when a listing snapshot is taken). A pinned inode keeps every
	// READDIRPLUS page of one enumeration on the identity the first page
	// saw, so a mid-enumeration replacement cannot serve mixed rows (the
	// #918 snapshot fence).
	Ino uint64
}

// Caller identifies the originating process of a kernel request.
type Caller struct {
	Uid uint32
	Gid uint32
	Pid uint32
}

// CallerFrom extracts the requesting credentials from a kernel request
// header.
func CallerFrom(h gofuse.InHeader) Caller {
	return Caller{Uid: h.Uid, Gid: h.Gid, Pid: h.Pid}
}

// OpKind identifies a chain operation. Kinds that map one-to-one onto a
// kernel FUSE opcode carry that opcode's value (the frozen FUSE ABI — the
// same numbers go-fuse keeps in its unexported opcode table), so facade
// translation is a direct mapping and logs align with kernel opcodes.
// Kinds that merge several kernel opcodes or are synthesized by the chain
// start at 0x1000; kernel opcodes stay below 100 and go-fuse reserves
// 100-104 for notifications, so the two ranges never collide. Kinds are
// declared in kernel opcode order, with the synthesized kinds after them.
// Protocol lifecycle opcodes (INIT, INTERRUPT, FORGET, NOTIFY_REPLY,
// DESTROY) never enter the vocabulary; the facade owns them.
type OpKind uint32

// Kernel-mapped kinds in FUSE ABI order.
const (
	OpLookup        OpKind = 1  // FUSE_LOOKUP
	OpGetAttr       OpKind = 3  // FUSE_GETATTR
	OpSetAttr       OpKind = 4  // FUSE_SETATTR
	OpReadlink      OpKind = 5  // FUSE_READLINK
	OpSymlink       OpKind = 6  // FUSE_SYMLINK
	OpMknod         OpKind = 8  // FUSE_MKNOD
	OpMkdir         OpKind = 9  // FUSE_MKDIR
	OpUnlink        OpKind = 10 // FUSE_UNLINK
	OpRmdir         OpKind = 11 // FUSE_RMDIR
	OpLink          OpKind = 13 // FUSE_LINK
	OpRead          OpKind = 15 // FUSE_READ
	OpWrite         OpKind = 16 // FUSE_WRITE
	OpStatFs        OpKind = 17 // FUSE_STATFS
	OpRelease       OpKind = 18 // FUSE_RELEASE
	OpFsync         OpKind = 20 // FUSE_FSYNC
	OpSetXattr      OpKind = 21 // FUSE_SETXATTR
	OpGetXattr      OpKind = 22 // FUSE_GETXATTR
	OpListXattr     OpKind = 23 // FUSE_LISTXATTR
	OpRemoveXattr   OpKind = 24 // FUSE_REMOVEXATTR
	OpFlush         OpKind = 25 // FUSE_FLUSH
	OpGetLk         OpKind = 31 // FUSE_GETLK
	OpSetLk         OpKind = 32 // FUSE_SETLK
	OpSetLkw        OpKind = 33 // FUSE_SETLKW
	OpAccess        OpKind = 34 // FUSE_ACCESS
	OpFallocate     OpKind = 43 // FUSE_FALLOCATE
	OpLseek         OpKind = 46 // FUSE_LSEEK
	OpCopyFileRange OpKind = 47 // FUSE_COPY_FILE_RANGE
)

// Chain-synthesized kinds (private range, see OpKind).
const (
	OpList   OpKind = 0x1000 // folds FUSE_READDIR / FUSE_READDIRPLUS into one full-entry listing
	OpCreate OpKind = 0x1001 // folds FUSE_OPEN / FUSE_CREATE into one open-or-create op
	OpRename OpKind = 0x1002 // folds FUSE_RENAME / FUSE_RENAME2 into one rename op
)

// String returns the operation's name — the kernel opcode name for
// kernel-mapped kinds, the folded name for synthesized kinds.
func (k OpKind) String() string {
	switch k {
	case OpLookup:
		return "lookup"
	case OpGetAttr:
		return "getattr"
	case OpSetAttr:
		return "setattr"
	case OpReadlink:
		return "readlink"
	case OpSymlink:
		return "symlink"
	case OpMknod:
		return "mknod"
	case OpMkdir:
		return "mkdir"
	case OpUnlink:
		return "unlink"
	case OpRmdir:
		return "rmdir"
	case OpLink:
		return "link"
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpStatFs:
		return "statfs"
	case OpRelease:
		return "release"
	case OpFsync:
		return "fsync"
	case OpSetXattr:
		return "setxattr"
	case OpGetXattr:
		return "getxattr"
	case OpListXattr:
		return "listxattr"
	case OpRemoveXattr:
		return "removexattr"
	case OpFlush:
		return "flush"
	case OpGetLk:
		return "getlk"
	case OpSetLk:
		return "setlk"
	case OpSetLkw:
		return "setlkw"
	case OpAccess:
		return "access"
	case OpFallocate:
		return "fallocate"
	case OpLseek:
		return "lseek"
	case OpCopyFileRange:
		return "copy_file_range"
	case OpList:
		return "list"
	case OpCreate:
		return "create"
	case OpRename:
		return "rename"
	}
	return fmt.Sprintf("op(%d)", uint32(k))
}

// Op is the operation value that travels the namespace chain: one struct
// covers every OpKind, with fields repurposed per kind.
type Op struct {
	Kind   OpKind // the operation
	Path   string // canonical absolute target path
	Caller Caller // originating process of the kernel request
	Handle any    // handle token claimed at OpCreate, carried on data-plane ops

	// Driver-side identity of the kernel request, used to rebuild exact
	// execution below the rewriting layers; extensions route on Path only.
	// Zero/nil on sub-ops issued through a view, where the core synthesizes
	// identity from the path.
	Ino    uint64 // source kernel inode
	Ino2   uint64 // destination kernel inode (rename/link)
	Fh     uint64 // kernel file handle
	Kernel any    // the raw kernel request the facade translated
	Rev    int64  // compare-and-set base revision for writes; -1 = unconditional

	// Kind-dependent payloads: which fields apply depends on OpKind.
	Offset int64  // read/write/fallocate start offset
	Limit  int64  // read length limit
	Size   int64  // setattr size or allocation size
	Mode   uint32 // create/setattr mode
	// HasMode distinguishes a chmod(000) — Mode == 0 with HasMode true —
	// from an absent mode on OpSetAttr. Create paths always set it. Zero
	// as a sentinel silently skipped the permission revocation.
	HasMode bool
	// HasSize marks an explicitly requested truncation in a combined
	// SETATTR (Size's zero value is ambiguous otherwise).
	HasSize bool
	Flags   uint32    // OpenFlags for OpCreate
	Target  string    // symlink target
	Rdev    uint32    // mknod device number (specials carry it in their records)
	Path2   string    // destination path (rename/link/copy_file_range)
	Data    []byte    // write payload or xattr value
	Mtime   time.Time // setattr modification time
	// Setattr owner change (v1 chown): pointer semantics — nil/absent
	// means "do not change"; &^uint32(0) is the kernel's preserve sentinel,
	// normalized away by the facade before dispatch.
	SetUID    uint32
	SetGID    uint32
	HasSetUID bool
	HasSetGID bool

	// Extended vocabulary payloads.
	Name    string   // xattr name for Op{Get,Set,Remove}Xattr
	Mask    uint32   // OpAccess mask
	Fh2     uint64   // destination handle identity for OpCopyFileRange
	Offset2 int64    // OpCopyFileRange destination offset
	Lock    FileLock // OpGetLk / OpSetLk / OpSetLkw payload
}

// Result is the operation reply; which fields are meaningful depends on
// the OpKind.
type Result struct {
	Attr      *Attr     // OpLookup / OpGetAttr reply
	Entries   []Entry   // OpList reply
	Data      []byte    // OpRead / OpReadlink reply
	Handle    Handle    // OpCreate claim: head of the handle decorator chain
	NodeId    uint64    // engine-assigned kernel inode, when the terminal owns identity (bridge mode); 0 = caller allocates
	Rev       int64     // post-commit revision (OpCreate / OpWrite replies)
	Fh        uint64    // driver-side handle id
	OpenFlags uint32    // normalized open flags of the claimed handle
	Written   uint32    // byte count for OpWrite / OpCopyFileRange
	Offset    int64     // OpLseek result offset
	Lock      *FileLock // OpGetLk reply
	Stat      *FsStat   // OpStatFs reply
}

// WriteOptions parameterizes Filesystem.Write.
type WriteOptions struct {
	ExpectedRev int64  // compare-and-set base revision; 0 = unconditional
	Mode        uint32 // mode applied when the write creates the file
}

// AttrPatch is the partial-attribute update payload for setattr; nil
// fields are left unchanged.
type AttrPatch struct {
	Mode  *uint32
	Size  *int64
	Mtime *time.Time
}

// RenameOptions carries rename flags; the path-addressed Filesystem view
// supports only plain rename.
type RenameOptions struct {
	NoReplace bool // fail instead of overwriting an existing destination
	Exchange  bool // atomically swap the two paths instead of moving
}

// IsZero reports whether no flags are set.
func (o RenameOptions) IsZero() bool { return !o.NoReplace && !o.Exchange }

// FileLock describes one POSIX lock range for OpGetLk / OpSetLk / OpSetLkw.
type FileLock struct {
	Start int64  // byte offset, inclusive
	End   int64  // byte offset, exclusive; 0 extends to EOF
	Type  uint32 // F_RDLCK / F_WRLCK / F_UNLCK
	Pid   uint32
	Owner uint64 // lock owner identity supplied by the kernel
	// Flock marks FUSE_LK_FLOCK requests (flock(2) semantics).
	Flock bool
}

// FsStat is the mount-wide statfs payload for OpStatFs; the fields mirror
// statfs(2).
type FsStat struct {
	BlockSize uint64 // fundamental block size
	Blocks    uint64 // total data blocks
	Free      uint64 // free blocks
	BAvail    uint64 // free blocks for unprivileged users
	Files     uint64 // total file nodes
	FFree     uint64 // free file nodes
	NameLen   uint32 // maximum file name length
}

// OpenFlagDropKernelCache is a facade-reserved reply bit: the data plane
// requires the kernel to drop the page cache at close (close-to-open
// invalidation, e.g. JuiceFS KeepCache=false). The facade strips the bit
// and must not OR FOPEN_KEEP_CACHE back on; a plain zero reply cannot
// express this ("no opinion" and "drop" collapse).
const OpenFlagDropKernelCache uint32 = 1 << 31

// OpenFlags is the normalized open/create flag set for OpCreate and
// Filesystem.Open.
type OpenFlags uint32

// Open intent flags; exactly one of FlagRead/FlagWrite accompanies a
// create.
const (
	FlagRead OpenFlags = 1 << iota
	FlagWrite
	FlagCreate
	FlagExcl
	FlagTruncate
	FlagAppend
)

// Uint32 returns the flag set as a plain integer.
func (f OpenFlags) Uint32() uint32 { return uint32(f) }

// StatusError carries an already-decided kernel status as an error, for
// producers that know the errno before the facade translates.
type StatusError struct {
	Path   string        // optional path, included in the error text when set
	Status gofuse.Status // the kernel status
}

// StatusErrorOf wraps a kernel status as an error.
func StatusErrorOf(st gofuse.Status) error { return &StatusError{Status: st} }

// Error implements error.
func (e *StatusError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("fs: %s: %s", e.Path, e.Status.String())
	}
	return "fs: " + e.Status.String()
}

// IsNotFound reports whether err represents a missing path: the sentinel,
// os.ErrNotExist, or an ENOENT status.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return true
	}
	var se *StatusError
	return errors.As(err, &se) && se.Status == gofuse.ENOENT
}

// StatusOf normalizes any chain error into a kernel status: sentinels and
// os errors map to their canonical errnos, StatusError and syscall.Errno
// unwrap directly, and everything else is EIO.
func StatusOf(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	// Exact errno unwrap first (v1 localErrToFuseStatus order): the os
	// sentinels are broad — os.ErrExist also matches ENOTEMPTY/ENOTDIR and
	// os.ErrPermission matches EPERM — so sentinel checks must not collapse
	// a precise errno carried in the chain.
	var se *StatusError
	if errors.As(err, &se) {
		return se.Status
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return gofuse.Status(errno)
	}
	switch {
	case IsNotFound(err):
		return gofuse.ENOENT
	case errors.Is(err, os.ErrPermission):
		return gofuse.EACCES
	case errors.Is(err, os.ErrExist):
		return gofuse.Status(syscall.EEXIST)
	case errors.Is(err, ErrUnsupported):
		return gofuse.Status(syscall.ENOSYS)
	case errors.Is(err, io.EOF):
		// v1 localErrToFuseStatus: a short read is success, not an error.
		return gofuse.OK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// v1 httpToFuseStatus: cancellation is retryable (EINTR/EAGAIN
		// class), never EIO — raw cancel errors reach the facade from the
		// legacy-interruptible commit paths.
		return gofuse.Status(syscall.EAGAIN)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		// v1: network timeouts are retryable.
		return gofuse.Status(syscall.EAGAIN)
	}
	return gofuse.EIO
}

func isExist(err error) bool {
	if errors.Is(err, os.ErrExist) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && errno == syscall.EEXIST {
		return true
	}
	var se *StatusError
	return errors.As(err, &se) && se.Status == gofuse.Status(syscall.EEXIST)
}
