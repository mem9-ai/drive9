package fuse

import (
	"context"
	"encoding/json"
	"runtime"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type extentXattrResponse struct {
	Errno int      `json:"errno"`
	Value []byte   `json:"value"`
	Names []string `json:"names"`
}

func extentXattrStatus(errno int) gofuse.Status {
	if errno == 0 {
		return gofuse.OK
	}
	// The extent protocol carries Linux errno numbers. A Darwin client also
	// accepts a locally produced ENODATA during tests, but the kernel-facing
	// answer for a missing xattr must be ENOATTR on that platform.
	if errno == int(syscall.ENODATA) || (runtime.GOOS != "linux" && errno == 61) {
		return gofuse.ENOATTR
	}
	return gofuse.Status(errno)
}

func (fs *Dat9FS) extentXattrInode(nodeID uint64, path string) (uint64, bool) {
	if fs == nil || fs.client == nil || fs.inodes == nil {
		return 0, false
	}
	if _, ok := fs.inodes.GetEntry(nodeID); !ok {
		return 0, false
	}
	return fs.resolveJuiceIno(nodeID, path)
}

func (fs *Dat9FS) callExtentXattr(ctx context.Context, op string, request any) (extentXattrResponse, gofuse.Status) {
	var response extentXattrResponse
	raw, err := fs.client.ExtentMeta(ctx, op, request)
	if err != nil {
		return response, gofuse.EIO
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return response, gofuse.EIO
	}
	return response, extentXattrStatus(response.Errno)
}

func (fs *Dat9FS) extentGetXattr(ctx context.Context, ino uint64, name string) ([]byte, gofuse.Status) {
	response, status := fs.callExtentXattr(ctx, "get_xattr", map[string]any{
		"inode": ino,
		"name":  name,
	})
	return response.Value, status
}

func (fs *Dat9FS) extentListXattr(ctx context.Context, ino uint64) ([]string, gofuse.Status) {
	response, status := fs.callExtentXattr(ctx, "list_xattr", map[string]any{
		"inode": ino,
	})
	return response.Names, status
}

func (fs *Dat9FS) extentSetXattr(ctx context.Context, ino uint64, name string, value []byte, flags uint32) gofuse.Status {
	_, status := fs.callExtentXattr(ctx, "set_xattr", map[string]any{
		"inode": ino,
		"name":  name,
		"value": value,
		"flags": flags,
	})
	return status
}

func (fs *Dat9FS) extentRemoveXattr(ctx context.Context, ino uint64, name string) gofuse.Status {
	_, status := fs.callExtentXattr(ctx, "remove_xattr", map[string]any{
		"inode": ino,
		"name":  name,
	})
	return status
}
