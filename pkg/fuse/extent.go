package fuse

import (
	"path"
	"strings"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

// juiceFSAttrTimeout is JuiceFS mount --attr-cache / --entry-cache default
// (cmd/flags.go clientFlags(1.0) → 1.0s). Timeout 0 made every kernel
// getattr a VFS.GetAttr HTTP call; sqlite mptest --wait all is 10s.
const juiceFSAttrTimeout = time.Second

func extentAttrTimeout(entry *InodeEntry) time.Duration {
	if entry != nil && !entry.IsDir && (entry.Nlink > 1 || len(entry.Paths) > 1) {
		return 0
	}
	return juiceFSAttrTimeout
}

func juiceTypeToStatMode(typ uint8, perm uint16) uint32 {
	kind := uint32(syscall.S_IFREG)
	switch typ {
	case jfsmeta.TypeDirectory:
		kind = uint32(syscall.S_IFDIR)
	case jfsmeta.TypeSymlink:
		kind = uint32(syscall.S_IFLNK)
	case jfsmeta.TypeFIFO:
		kind = uint32(syscall.S_IFIFO)
	case jfsmeta.TypeBlockDev:
		kind = uint32(syscall.S_IFBLK)
	case jfsmeta.TypeCharDev:
		kind = uint32(syscall.S_IFCHR)
	case jfsmeta.TypeSocket:
		kind = uint32(syscall.S_IFSOCK)
	}
	return kind | (uint32(perm) & posixPermissionModeMask)
}

func (fh *FileHandle) isExtent() bool {
	return fh != nil && (fh.extentIno != 0 || fh.extentFh != 0)
}

func (fs *Dat9FS) extentEnabled() bool {
	return fs != nil && fs.opts != nil && len(fs.opts.ExtentPaths) > 0
}

func (fs *Dat9FS) shouldUseExtentPath(localPath string) bool {
	if fs == nil || fs.opts == nil {
		return false
	}
	if strings.HasSuffix(localPath, "/") {
		return false
	}
	for _, pat := range fs.opts.ExtentPaths {
		if matchExtentPattern(pat, localPath) {
			return true
		}
	}
	return false
}

func matchExtentPattern(pattern, filePath string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	name := path.Base(filePath)
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		return strings.HasSuffix(strings.ToLower(name), strings.ToLower(pattern[1:]))
	}
	if strings.Contains(pattern, "*") {
		ok, _ := path.Match(path.Base(pattern), name)
		return ok
	}
	return strings.EqualFold(name, pattern)
}
