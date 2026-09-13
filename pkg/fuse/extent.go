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

// clampExtentTTL keeps the mount's configured cache lifetimes as an upper
// bound: the extent attribute timeout exists so a JuiceFS-backed file is not
// re-validated through the VFS on every getattr, but a mount configured with a
// shorter TTL must not have it silently lengthened. The glob also matches
// classic files, and honoring the user's setting is what keeps "extent mount"
// from meaning "slower, differently cached mount" for every file on it.
//
// This is a plain min, so the extent layer's own zero — hardlinked or
// multi-path inodes, which must be revalidated because a rename could leave a
// stale alias (see extentAttrTimeout) — stays zero. Never clamp a deliberate 0
// up to the mount default.
func clampExtentTTL(ttl, configured time.Duration) time.Duration {
	if configured < ttl {
		return configured
	}
	return ttl
}

// extentAttrTimeoutFor is extentAttrTimeout bounded by the mount's own AttrTTL,
// for the replies that answer with a single timeout (GetAttr/SetAttr). The
// entry-cache reply in fillEntryOut clamps EntryTTL and AttrTTL itself.
func (fs *Dat9FS) extentAttrTimeoutFor(entry *InodeEntry) time.Duration {
	ttl := extentAttrTimeout(entry)
	if fs == nil || fs.opts == nil {
		return ttl
	}
	return clampExtentTTL(ttl, fs.opts.AttrTTL)
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

// extentDiscoveryEnabled reports whether this mount may spend an extra RPC to
// discover that a path is an extent object it did not create itself.
//
// Only mounts that opted into the extent data plane (--extent patterns or the
// extent profile) pay for discovery. A mount without them still serves an
// extent file whenever the server already reported ContentLayout/ExtentIno in
// a stat or lookup reply, because that costs nothing; it just never probes.
// Without this gate every standard mount would issue an extent credential
// request, a JuiceFS lookup, or a HEAD per getattr/open/unlink/rename, which
// breaks the standard path's remote-call budget (and its --durability
// contract).
func (fs *Dat9FS) extentDiscoveryEnabled() bool {
	return fs.extentEnabled() || fs.extentVFS() != nil
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
