package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

const fuseOverlayOverrideStatXattr = "user.fuseoverlayfs.override_stat"

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

// classicFuseOverlayStatXattr returns the stat override fuse-overlayfs 1.14
// requires when xattr_permissions=2. A file created through the ordinary
// Drive9 API has classic content and therefore no JuiceFS inode on which the
// durable extent xattr protocol can store this private attribute. Without a
// value, fuse-overlayfs rejects stat/open with ENODATA before Drive9 can serve
// the file bytes.
//
// This compatibility value is deliberately limited to mounts that negotiated
// the exact extent xattr v1 contract, and to regular classic files. It is
// recomputed from the entry's current FUSE stat on every fresh mount; it does
// not mask an extent-xattr failure and it never changes file data or size.
func (fs *Dat9FS) classicFuseOverlayStatXattr(nodeID uint64, name string) ([]byte, bool) {
	if fs == nil || fs.opts == nil || !fs.opts.RequireExtentXattrV1 || name != fuseOverlayOverrideStatXattr || fs.inodes == nil {
		return nil, false
	}
	entry, ok := fs.inodes.GetEntry(nodeID)
	if !ok || entry.ExtentIno != 0 || !entryIsRegularFile(entry) {
		return nil, false
	}
	var attr gofuse.Attr
	fs.fillAttr(entry, &attr)
	return []byte(fmt.Sprintf("%d:%d:%o", attr.Uid, attr.Gid, attr.Mode)), true
}
