package fuse

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// ReexecProtocolVersion is the wire protocol version for the reexec handshake.
const ReexecProtocolVersion = 1

// ReexecRefusal describes a single reason why reexec was refused.
type ReexecRefusal struct {
	Gate   string `json:"gate"`
	Reason string `json:"reason"`
	Count  int    `json:"count,omitempty"`
}

// ReexecPreflightResult holds the complete preflight check output.
type ReexecPreflightResult struct {
	OK       bool            `json:"ok"`
	Refusals []ReexecRefusal `json:"refusals,omitempty"`
}

// reexecGuard prevents concurrent reexec attempts.
type reexecGuard struct {
	mu     sync.Mutex
	active bool
}

func (g *reexecGuard) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active {
		return false
	}
	g.active = true
	return true
}

func (g *reexecGuard) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active = false
}

// ReexecPreflight runs every gate against the facade state. It does NOT
// acquire the guard or install a quiesce barrier (caller's job).
func (f *FS) ReexecPreflight() ReexecPreflightResult {
	var refusals []ReexecRefusal
	refuse := func(gate, reason string, count int) {
		refusals = append(refusals, ReexecRefusal{Gate: gate, Reason: reason, Count: count})
	}

	if n := f.handles.len(); n > 0 {
		refuse("file_handles", "open file handles exist", n)
	}
	if n := f.dirs.len(); n > 0 {
		refuse("dir_handles", "open directory handles exist", n)
	}
	if n := f.nonRootKernelLookupCount(); n > 0 {
		refuse("inode_nlookup", "non-root inodes have positive kernel lookup refs", n)
	}
	if n := f.heldLockCount(); n > 0 {
		refuse("fuse_locks", "FUSE locks are held", n)
	}
	// Extensions hold their own in-memory state (session xattrs, lazy
	// buffers); ask each through the framework's ReexecGater capability.
	for _, h := range f.chain.Handlers() {
		if g, ok := h.(fs.ReexecGater); ok {
			if reason, n := g.ReexecRefusal(); n > 0 {
				refuse("extension_state", reason, n)
			}
		}
	}
	return ReexecPreflightResult{OK: len(refusals) == 0, Refusals: refusals}
}

// nonRootKernelLookupCount sums positive lookup refs below the root.
func (f *FS) nonRootKernelLookupCount() int {
	n := 0
	f.inodes.mu.RLock()
	for ino, e := range f.inodes.byInode {
		if ino == 1 {
			continue
		}
		if e.Nlookup > 0 {
			n += int(e.Nlookup)
		}
	}
	f.inodes.mu.RUnlock()
	return n
}

// heldLockCount reports held POSIX locks (v1: locks map length).
func (f *FS) heldLockCount() int {
	return f.locks.count()
}

var (
	_ = os.Getpid
	_ = json.Marshal
)
