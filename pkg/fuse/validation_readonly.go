package fuse

import (
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Namespace mutations are rejected before reaching any extension, including
// localize and xattr. Handle writes and mount-local setattr are gated before
// their fast paths in the facade itself.
func validationMutation(op *fs.Op) bool {
	switch op.Kind {
	case fs.OpCreate:
		return op.Flags&syscall.O_ACCMODE != syscall.O_RDONLY || op.Flags&(syscall.O_CREAT|syscall.O_TRUNC|syscall.O_APPEND) != 0
	case fs.OpAccess:
		return op.Mask&2 != 0 // W_OK
	case fs.OpWrite, fs.OpSetAttr, fs.OpSymlink, fs.OpMknod, fs.OpMkdir,
		fs.OpUnlink, fs.OpRmdir, fs.OpLink, fs.OpRename, fs.OpSetXattr,
		fs.OpRemoveXattr, fs.OpFallocate, fs.OpCopyFileRange:
		return true
	default:
		return false
	}
}
