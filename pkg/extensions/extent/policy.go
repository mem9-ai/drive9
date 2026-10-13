// Policy core of the extent plane: creation-declaration matching, the
// large-file claim decision, and the JuiceFS mode mapping.
package extent

import (
	"path"
	"strings"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// juiceFSAttrTimeout is the JuiceFS mount --attr-cache / --entry-cache
// default (cmd/flags.go clientFlags(1.0) → 1.0s). Timeout 0 made every
// kernel getattr a VFS.GetAttr HTTP call; sqlite mptest --wait all is 10s.
const juiceFSAttrTimeout = time.Second

// clampExtentTTL keeps the mount's configured cache lifetimes as an upper
// bound: the extent attribute timeout exists so a JuiceFS-backed file is
// not re-validated through the VFS on every getattr, but a mount configured
// with a shorter TTL must not have it silently lengthened.
//
// This is a plain min, so a deliberate zero — hardlinked or multi-path
// inodes, which must be revalidated because a rename could leave a stale
// alias — stays zero. Never clamp a deliberate 0 up to the mount default.
func clampExtentTTL(ttl, configured time.Duration) time.Duration {
	if configured < ttl {
		return configured
	}
	return ttl
}

const posixPermissionModeMask uint32 = 0o7777

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

// ShouldUseExtentPath reports whether localPath matches one of the mount's
// extent declarations.
func ShouldUseExtentPath(extentPaths []string, localPath string) bool {
	if strings.HasSuffix(localPath, "/") {
		return false
	}
	for _, pat := range extentPaths {
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

// writeIntent reports whether the open flags carry write access.
func writeIntent(flags uint32) bool {
	return flags&(uint32(syscall.O_WRONLY)|uint32(syscall.O_RDWR)) != 0
}

// claimFreshWriter reports whether the create flags describe the fresh
// truncate-writer the large-file takeover claims: a create with truncation
// and write intent, no append (an appender's offset discipline belongs to
// the plane that owns the existing bytes) and no per-write sync demand.
func claimFreshWriter(flags uint32) bool {
	return writeIntent(flags) &&
		flags&uint32(syscall.O_CREAT) != 0 &&
		flags&uint32(syscall.O_TRUNC) != 0 &&
		flags&uint32(syscall.O_APPEND) == 0 &&
		!hasSyncOpenFlag(flags)
}

// claimsLargeFile reports whether the default takeover claims this create:
// enabled, a fresh truncate-writer, not already declared for the extent
// plane (the declaration claims from byte zero), and not exempted to a
// plane inside this one (the append-log declarations).
func (e *Extent) claimsLargeFile(op *fs.Op) bool {
	if !e.largeFileEnabled() || !claimFreshWriter(op.Flags) {
		return false
	}
	if ShouldUseExtentPath(e.opts.ExtentPaths, op.Path) {
		return false
	}
	if e.opts.LargeFileExempt != nil && e.opts.LargeFileExempt(op.Path) {
		return false
	}
	return true
}

// extentOpenFlags maps JuiceFS close-to-open invalidation: the inode keeps
// its cache only when the server said so.
func extentOpenFlags(keepCache bool) uint32 {
	if keepCache {
		return 0x20 // gofuse.FOPEN_KEEP_CACHE
	}
	return 0
}

// juiceWriteOffset bumps O_APPEND kernel writes (which can arrive with
// off=0) to the VFS length.
func juiceWriteOffset(off, length uint64, appendMode bool) uint64 {
	if !appendMode || off != 0 {
		return off
	}
	if length > off {
		return length
	}
	return off
}
