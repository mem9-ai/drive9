package fs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Port of v1's TestErrnoAlignmentLocalErrors: local (overlay) errors map to
// their canonical kernel errnos through fs.StatusOf — directly, wrapped in
// *os.PathError / *os.LinkError, and double-wrapped in fmt.Errorf chains.
func TestStatusOfLocalErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want gofuse.Status
	}{
		{"nil", nil, gofuse.OK},
		{"eof", io.EOF, gofuse.OK},
		{"permission_sentinel", os.ErrPermission, gofuse.EACCES},
		{"exist_sentinel", os.ErrExist, gofuse.Status(syscall.EEXIST)},
		{"not_exist_sentinel", os.ErrNotExist, gofuse.ENOENT},
		{"unsupported_sentinel", ErrUnsupported, gofuse.Status(syscall.ENOSYS)},
		{"unknown", errors.New("unknown local error"), gofuse.EIO},
	}
	for _, errno := range []syscall.Errno{syscall.ENOTEMPTY, syscall.EPERM, syscall.EACCES, syscall.EEXIST, syscall.ENOENT, syscall.ENOTDIR, syscall.EISDIR, syscall.EXDEV, syscall.EFBIG, syscall.EDQUOT, syscall.ENOSPC, syscall.EIO} {
		cases = append(cases, struct {
			name string
			err  error
			want gofuse.Status
		}{errno.Error(), errno, gofuse.Status(errno)})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			variants := []error{c.err}
			if c.err != nil {
				variants = append(variants,
					&os.PathError{Op: "remove", Path: "/local", Err: c.err},
					&os.LinkError{Op: "rename", Old: "/old", New: "/new", Err: c.err},
					fmt.Errorf("outer: %w", &os.PathError{Op: "remove", Path: "/local", Err: c.err}),
				)
			}
			for _, variant := range variants {
				if got := StatusOf(variant); got != c.want {
					t.Errorf("StatusOf(%v) = %v, want %v", variant, got, c.want)
				}
			}
		})
	}
}
