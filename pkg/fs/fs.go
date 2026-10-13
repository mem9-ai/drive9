package fs

import (
	"context"
	"fmt"
	"strings"
)

// Filesystem is the path-addressed API over a chain segment, used for
// sub-operations issued from handlers and tools rather than kernel
// dispatch.
type Filesystem interface {
	Read(ctx context.Context, path string, off, limit int64) ([]byte, error)
	Write(ctx context.Context, path string, data []byte, o WriteOptions) error

	GetAttr(ctx context.Context, path string) (Attr, error)
	SetAttr(ctx context.Context, path string, p AttrPatch) error

	List(ctx context.Context, dir string) ([]Entry, error)
	Mkdir(ctx context.Context, path string, mode uint32) error
	Rmdir(ctx context.Context, path string) error

	Open(ctx context.Context, path string, flags OpenFlags) (Handle, error)
	Rename(ctx context.Context, oldPath, newPath string, o RenameOptions) error

	Link(ctx context.Context, oldPath, newPath string) error
	Unlink(ctx context.Context, path string) error
	Symlink(ctx context.Context, target, path string) error
	Readlink(ctx context.Context, path string) (string, error)

	EnsureParent(ctx context.Context, dir string) error
}

// HandlerFilesystem adapts a chain-dispatch func (typically a captured
// next, or a view rooted below a handler) into the path-addressed
// Filesystem API.
type HandlerFilesystem func(ctx context.Context, op *Op) (Result, error)

// NewEmptyFilesystem returns a Filesystem that answers every operation
// with ErrNotFound.
func NewEmptyFilesystem() Filesystem {
	return HandlerFilesystem(func(ctx context.Context, op *Op) (Result, error) {
		return Result{}, ErrNotFound
	})
}

// Read implements Filesystem.
func (f HandlerFilesystem) Read(ctx context.Context, path string, off, limit int64) ([]byte, error) {
	res, err := f(ctx, &Op{Kind: OpRead, Path: path, Offset: off, Limit: limit})
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}

// Write implements Filesystem.
func (f HandlerFilesystem) Write(ctx context.Context, path string, data []byte, o WriteOptions) error {
	rev := o.ExpectedRev
	if rev == 0 {
		rev = -1
	}
	_, err := f(ctx, &Op{Kind: OpWrite, Path: path, Data: data, Mode: o.Mode, Rev: rev})
	return err
}

// GetAttr implements Filesystem.
func (f HandlerFilesystem) GetAttr(ctx context.Context, path string) (Attr, error) {
	res, err := f(ctx, &Op{Kind: OpGetAttr, Path: path})
	if err != nil {
		return Attr{}, err
	}
	if res.Attr == nil {
		return Attr{}, fmt.Errorf("fs: getattr %s: no attr", path)
	}
	return *res.Attr, nil
}

// SetAttr implements Filesystem.
func (f HandlerFilesystem) SetAttr(ctx context.Context, path string, p AttrPatch) error {
	op := &Op{Kind: OpSetAttr, Path: path, Size: -1}
	if p.Mode != nil {
		op.Mode, op.HasMode = *p.Mode, true
	}
	if p.Size != nil {
		op.Size = *p.Size
	}
	if p.Mtime != nil {
		op.Mtime = *p.Mtime
	}
	_, err := f(ctx, op)
	return err
}

// List implements Filesystem.
func (f HandlerFilesystem) List(ctx context.Context, dir string) ([]Entry, error) {
	res, err := f(ctx, &Op{Kind: OpList, Path: dir})
	if err != nil {
		return nil, err
	}
	return res.Entries, nil
}

// Mkdir implements Filesystem.
func (f HandlerFilesystem) Mkdir(ctx context.Context, path string, mode uint32) error {
	_, err := f(ctx, &Op{Kind: OpMkdir, Path: path, Mode: mode})
	return err
}

// Rmdir implements Filesystem.
func (f HandlerFilesystem) Rmdir(ctx context.Context, path string) error {
	_, err := f(ctx, &Op{Kind: OpRmdir, Path: path})
	return err
}

// Rename implements Filesystem.
func (f HandlerFilesystem) Rename(ctx context.Context, oldPath, newPath string, o RenameOptions) error {
	if !o.IsZero() {
		return ErrUnsupported
	}
	_, err := f(ctx, &Op{Kind: OpRename, Path: oldPath, Path2: newPath})
	return err
}

// Link implements Filesystem.
func (f HandlerFilesystem) Link(ctx context.Context, oldPath, newPath string) error {
	_, err := f(ctx, &Op{Kind: OpLink, Path: oldPath, Path2: newPath})
	return err
}

// Unlink implements Filesystem.
func (f HandlerFilesystem) Unlink(ctx context.Context, path string) error {
	_, err := f(ctx, &Op{Kind: OpUnlink, Path: path})
	return err
}

// Symlink implements Filesystem.
func (f HandlerFilesystem) Symlink(ctx context.Context, target, path string) error {
	_, err := f(ctx, &Op{Kind: OpSymlink, Path: path, Target: target})
	return err
}

// Readlink implements Filesystem.
func (f HandlerFilesystem) Readlink(ctx context.Context, path string) (string, error) {
	res, err := f(ctx, &Op{Kind: OpReadlink, Path: path})
	if err != nil {
		return "", err
	}
	return string(res.Data), nil
}

// EnsureParent implements Filesystem: it walks dir's ancestors and creates
// any missing directory (mode 0755), refusing when an ancestor exists as a
// non-directory.
func (f HandlerFilesystem) EnsureParent(ctx context.Context, dir string) error {
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" || dir == "/" {
		return nil
	}
	segments := strings.Split(strings.TrimPrefix(dir, "/"), "/")
	cur := ""
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		cur = cur + "/" + seg
		if st, err := f.GetAttr(ctx, cur); err == nil {
			if !st.IsDir() {
				return fmt.Errorf("fs: ensure parent %s: %s is not a directory", dir, cur)
			}
			continue
		} else if !IsNotFound(err) {
			return err
		}
		if _, err := f(ctx, &Op{Kind: OpMkdir, Path: cur, Mode: 0o755}); err != nil && !isExist(err) {
			return err
		}
	}
	return nil
}

// Open implements Filesystem.
func (f HandlerFilesystem) Open(ctx context.Context, path string, flags OpenFlags) (Handle, error) {
	res, err := f(ctx, &Op{Kind: OpCreate, Path: path, Flags: uint32(flags)})
	if err != nil {
		return nil, err
	}
	return handle{fs: f, path: path, token: res.Handle}, nil
}

// handle adapts a handle token claimed at OpCreate into the Handle
// interface by routing the data operations back through the op chain.
type handle struct {
	fs    HandlerFilesystem
	path  string
	token Handle
}

func (h handle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	res, err := h.fs(ctx, &Op{Kind: OpRead, Path: h.path, Handle: h.token, Offset: off, Limit: int64(len(dest))})
	if err != nil {
		return 0, err
	}
	return copy(dest, res.Data), nil
}

func (h handle) Write(ctx context.Context, off int64, data []byte) error {
	_, err := h.fs(ctx, &Op{Kind: OpWrite, Path: h.path, Handle: h.token, Offset: off, Data: data})
	return err
}

func (h handle) Sync(ctx context.Context) error {
	_, err := h.fs(ctx, &Op{Kind: OpFsync, Path: h.path, Handle: h.token})
	return err
}

func (h handle) Flush(ctx context.Context) error {
	_, err := h.fs(ctx, &Op{Kind: OpFlush, Path: h.path, Handle: h.token})
	return err
}

func (h handle) Release(ctx context.Context) error {
	_, err := h.fs(ctx, &Op{Kind: OpRelease, Path: h.path, Handle: h.token})
	return err
}
