package fs

import "context"

import (
	"time"
)

// Context bundles the shared state every plugin composes against. It is
// constructed once at mount and handed to plugin constructors — no
// registry, no runtime lookup; a plugin's constructor signature is its
// dependency declaration. Naming convention: the framework Context is
// referenced as fctx/*fs.Context to stay distinct from ctx context.Context.
type Context struct {
	// Inodes is the identity authority: inode↔path resolution plus the
	// mutation epochs that gate cross-plugin handoffs. Implemented by the
	// facade.
	Inodes InodeTable
	// Locks is the shared lock discipline for cross-plugin path work and
	// handle-serialized data operations. Implemented by the facade.
	Locks LockTable
	// Staging is the single source of truth for locally staged mutations;
	// nil when the mount runs without a buffered write plane.
	Staging StagingLedger
	// Notify invalidates kernel caches. Implemented by the facade.
	Notify Notifier
	// Events is the typed event bus shared by plugins and the facade; its
	// methods are nil-safe.
	Events *EventBus
	// DirCache is the mount's directory-listing cache. The facade
	// installs a default TTL cache; owners may install a richer
	// implementation through the slot.
	DirCache DirListingCache
	// Bypass arms short per-inode kernel-cache bypass windows; nil leaves
	// the slot empty.
	Bypass CacheBypassArmer
}

// flushOwnerKey carries the FUSE FLUSH lock owner across the handle
// boundary: extensions that own POSIX record locks need it at close time.
type flushOwnerKey struct{}

// WithFlushOwner attaches the kernel's FLUSH lock owner to the context.
func WithFlushOwner(ctx context.Context, owner uint64) context.Context {
	return context.WithValue(ctx, flushOwnerKey{}, owner)
}

// FlushOwnerOf extracts the FLUSH lock owner (0 = none carried).
func FlushOwnerOf(ctx context.Context) uint64 {
	if v, ok := ctx.Value(flushOwnerKey{}).(uint64); ok {
		return v
	}
	return 0
}

// InodeTable is the identity authority shared by the kernel facade and the
// plugins: inode↔path resolution plus the mutation epochs that gate
// cross-plugin handoffs.
type InodeTable interface {
	// Ino returns the current inode for path, if known.
	Ino(path string) (uint64, bool)
	// Path resolves an inode back to its canonical path.
	Path(ino uint64) (string, bool)
	// Epoch returns the current mutation epoch of a path.
	Epoch(path string) uint64
	// NextEpoch advances the epoch and returns the new token. Whichever
	// layer publishes new authoritative state for a path (dirty write
	// staged, remote commit landed, view switched) advances it.
	NextEpoch(path string) uint64
}

// LockTable is the shared lock discipline. PathLock serializes
// cross-plugin work on one path (upload vs cleanup, staging vs mutation);
// HandleLock serializes data operations on one open handle chain — only
// the outermost handle decorator acquires it, inner layers assume it held.
type LockTable interface {
	// PathLock exclusively locks one path and returns its unlock func.
	PathLock(path string) func()
	// HandleLock exclusively locks one open handle chain and returns its
	// unlock func.
	HandleLock(fh uint64) func()
}

// StagedKind classifies a staged mutation for the ledger.
type StagedKind uint8

// Staged mutation classes.
const (
	StagedNew      StagedKind = iota // fresh file, no remote base
	StagedMutation                   // in-place update of an existing remote object
	StagedDelete                     // tombstone for an existing remote object
)

// StagedMeta is the metadata recorded alongside staged bytes.
type StagedMeta struct {
	Kind    StagedKind // mutation class
	Mode    uint32     // file mode at staging time
	Size    int64      // staged content size in bytes
	BaseRev int64      // remote revision the staging was based on
}

// StagingLedger is the single source of truth for locally staged mutations
// (shadow bytes, pending metadata, journal). The buffered write plane
// stages into it, durability drains it, recovery replays it.
type StagingLedger interface {
	// Stage records data and meta under path at epoch. A later Stage for
	// the same path supersedes an earlier one at an older epoch.
	Stage(path string, epoch uint64, data []byte, meta StagedMeta) error
	// Take atomically removes and returns the staged state for a path.
	// ok=false when nothing is staged or epoch is no longer current.
	Take(path string, epoch uint64) (data []byte, meta StagedMeta, ok bool)
}

// CacheBypassArmer flags an inode whose next open must bypass the kernel
// page cache (implemented by the gofuse facade): Linux can serve stale
// pages after a truncate-first sync commit even across a successful
// InodeNotify.
type CacheBypassArmer interface {
	// ArmBypass flags path's next open to bypass the kernel page cache.
	// rev/size carry the authoritative post-commit state; reason names the
	// arming cause in diagnostics.
	ArmBypass(path string, rev, size int64, reason string)
}

// Notifier invalidates kernel caches; implemented by the gofuse facade.
type Notifier interface {
	// InvalidateEntry drops the kernel's cached dentry for parent/name.
	InvalidateEntry(parentIno uint64, name string)
	// InvalidateContent drops the kernel's cached attributes and data
	// for ino (size = the authoritative size afterwards).
	InvalidateContent(ino uint64, size uint64)
	// InvalidateAll flushes all kernel caches for the mount (view switch).
	InvalidateAll()
}

// DirEntryInfo is one cached directory-listing entry. The optional
// identity fields (ResourceID, ownership) let listing-prefetch machinery
// match entries across generations; leave them zero when unknown.
type DirEntryInfo struct {
	Name       string    // entry base name
	IsDir      bool      // directory classification
	Size       int64     // file size in bytes
	Mode       uint32    // permission bits; valid only when HasMode
	HasMode    bool      // Mode was reported
	Mtime      time.Time // modification time (zero = unknown)
	Rev        int64     // server revision token (0 = unknown)
	ResourceID string    // server-side resource identity for cross-generation matching
	HasUID     bool      // Uid was reported
	Uid        uint32    // reported owner uid
	HasGID     bool      // Gid was reported
	Gid        uint32    // reported owner gid
	// ExtentIno is the object-store inode for extent-layout entries;
	// zero elsewhere.
	ExtentIno uint64
	// LocalMutation marks a row this mount itself wrote (create, write,
	// landed commit, chown). Such a row is this process's own state even
	// on a multi-server deployment, so a stat may serve it without
	// revalidation; listing-observed rows leave it false and revalidate.
	LocalMutation bool
	// FreshUntil is the listing-cache TTL deadline for this entry. It is
	// meaningful only inside the listing cache that set it; producers and
	// consumers outside caching leave it zero.
	FreshUntil time.Time
	// Nlink is the server-reported link count (0 = unknown).
	Nlink uint32
}

// LookupKind classifies the outcome of a namespace lookup answered from
// directory-listing state (the Context.DirCache slot).
type LookupKind uint8

// Namespace lookup outcomes.
const (
	LookupNone         LookupKind = iota // no usable listing state
	LookupPositive                       // name present and fresh
	LookupNegative                       // name known absent (fresh tombstone)
	LookupCompleteMiss                   // listing complete, name absent
	LookupSessionMiss                    // listing created this session, name absent
	LookupPartialMiss                    // listing incomplete, name simply unknown
)

// DirListingCache caches directory listings for the facade: readdir
// restarts consult it, fresh creates upsert into it, and removals drop
// the affected listings. Owners install an implementation through the
// Context slot; nil disables listing caching.
type DirListingCache interface {
	// Get returns the cached listing for dir, if fresh.
	Get(dir string) ([]DirEntryInfo, bool)
	// Put stores a full listing for dir.
	Put(dir string, entries []DirEntryInfo)
	// Upsert merges one entry into the cached listing of parent.
	Upsert(parent string, entry DirEntryInfo)
	// Invalidate drops the cached listing of dir.
	Invalidate(dir string)
	// InvalidatePrefix drops the cached listings of dir and its subtree.
	InvalidatePrefix(dir string)
	// InvalidateAll drops every cached listing.
	InvalidateAll()
}
