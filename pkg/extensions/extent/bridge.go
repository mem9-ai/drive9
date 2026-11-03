// Package extent serves extent-layout files through the community JuiceFS
// engine (vfs + chunk store) running on the extent content-layout v2 wire
// protocol, while classic files keep the drive9 planes untouched.
//
// Routing (see Handler in plane.go): existing content_layout=extent files
// are claimed on open; with Threshold == 0 (full JuiceFS mode) every fresh
// create is claimed too; a positive threshold leaves creation to the
// classic write plane, whose commit queue promotes large images to extent
// layout. The extension sits above the write plane, so claimed handles
// bypass drive9's writeback — the engine's own buffering, block cache and
// prefetch own the file. Compaction and block GC stay server-side.
//
// Internally the package is the engine bridge: this file implements
// meta.Meta against the v2 server.
//
// The bridge implements JuiceFS's meta.Meta against the v2 server: the
// namespace maps onto the classic REST API, chunk reads translate block
// read plans into JuiceFS slice lists, and slice commits become PATCH
// dirty-block sessions. Block bytes never touch drive9-server — the
// object.ObjectStorage handed to the chunk store stages writes in memory
// and uploads them through presigned URLs at commit time, and serves reads
// from plan URLs (Range-narrowed) with synthesized zero blocks for holes.
//
// The Meta interface contains unexported members (getBase, and signatures
// naming unexported types), so no out-of-package type can implement it
// directly. The bridge therefore embeds a nil meta.Meta — it satisfies the
// interface at compile time — and overrides every method the engine can
// actually reach. Anything not overridden panics through the nil embed,
// which the tests would surface immediately.
//
// Compaction and block GC are deliberately absent: they run in the server's
// tenant worker.
package extent

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
)

// Config wires the bridge.
type Config struct {
	// Client is the mount's API client.
	Client *client.Client
	// RemoteRoot is the namespace subtree the bridge serves (the JuiceFS
	// root inode maps onto it).
	RemoteRoot string
	// StagingDir arms the interactive-durability barrier: pending blocks
	// are spilled there at fsync and re-committed by a later mount when a
	// remote commit never landed. Empty disables the barrier.
	StagingDir string
}

const (
	rootIno      = meta.Ino(1)
	blockSizeMiB = 4 // must match the server-pinned extent block size
)

// Bridge implements meta.Meta against the drive9 v2 server.
type Bridge struct {
	meta.Meta // nil embed: satisfies unexported interface members; never called

	cfg    Config
	format meta.Format

	mu           sync.Mutex
	nextID       uint64 // provisional slice-id allocator (high bit set)
	stagedSlices map[uint64]*stagedSlice
	// blocks maps deterministic ids (content sha256, or the zero-block ids)
	// to their URL registry entry.
	blocks *blockRegistry
	// classicLRU bounds the staging bytes retained for classic-owned
	// reads (oldest-first retirement beyond classicReadCacheBytes).
	classicLRU   []classicRef
	classicBytes int64

	// layoutPending marks a fresh upload of OURS whose landed layout
	// could not be classified (stat outage) — proof of ownership that a
	// foreign replacement can never carry. verifyBase carries an
	// identity-verified recovery's expected revision/resource, consumed
	// by the next commit as its CAS base.
	// commitErr remembers the last landing failure for an inode so the
	// flush/close boundary (FlushFile) surfaces it — Bridge.Write itself
	// returns success to keep the community writer clean (its sticky
	// fileWriter.err otherwise stranded the handle after recovery).
	commitErr map[meta.Ino]error

	layoutPending map[meta.Ino]bool
	// layoutPendingID is the resource identity of OUR landed upload for
	// an inode whose classification failed: adopting the classic surface
	// is only valid while the occupant is still THAT object.
	layoutPendingID map[meta.Ino]string
	verifyBase      map[meta.Ino]verifyBase

	// classicOwned marks files whose fresh extent session silently
	// degraded to a CLASSIC upload on a legacy server: the handle stays
	// owned by this bridge, so commits and reads keep serving it through
	// the classic wire surface instead of erroring on the layout.
	classicOwned map[meta.Ino]bool

	// deferred (interactive durability): meta.Write keeps slices pending
	// locally instead of committing remotely at every writer flush; reads
	// overlay the pending state and the remote commit lands at the
	// flush/close boundary or the next mount's replay.
	deferred atomic.Bool
	// commitLocks serializes per-file commit lifecycles (FlushFile).
	commitMu    sync.Mutex
	commitLocks map[meta.Ino]*sync.Mutex

	// paths caches inode → remote path; inode values are content-stable
	// hashes of the path so lookups survive restarts of the bridge.
	paths   map[meta.Ino]string
	sizes   map[meta.Ino]int64 // local view of file length (fresh writes)
	revs    map[meta.Ino]int64 // last known server revision
	pending map[meta.Ino][]pendingWrite
	truncs  map[meta.Ino]int64 // truncate-only growth awaiting the next commit
	// modes remembers fresh-create modes until the first commit applies
	// them server-side (the sequential create itself has no mode field).
	modes map[meta.Ino]uint32
	// sentinels tracks the hidden keep-alive names THIS bridge created
	// (silly-rename); only registered names may be deleted at last close —
	// a user filename that merely contains ".d9open-" is never ours.
	sentinels map[string]meta.Ino // sentinel LOCAL path -> held engine ino
	// moved records engine inodes whose pathname changed (rename) or was
	// unlinked (sentinel) while a descriptor still holds them; a NEW
	// incarnation at the old path must not reuse the key. Keyed by the
	// FULL-path-derived inode the engine actually uses — mount-root
	// independent.
	moved map[meta.Ino]string // old engine ino -> new full path
	// replacements maps a full path to the synthetic inode allocated for
	// its current live incarnation when the path-derived key is held.
	replacements map[string]meta.Ino
	// nextHeldIno allocates synthetic inode keys for replacements created
	// at a path whose original engine ino is still held by a live
	// deleted-open descriptor (path-derived keys would collide).
	nextHeldIno  meta.Ino
	sessionCount int32
}

// heldInoBase is the synthetic-inode allocation base: far above the
// path-derived FNV space the engine uses, far below the JuiceFS special
// range.
const heldInoBase = meta.Ino(1) << 40

type pendingWrite struct {
	chunk    uint32
	off      uint32
	checksum string // sha256 of the staged block object
	size     int64  // visible length
}

// NewBridge builds the meta.Meta implementation.
func NewBridge(cfg Config) *Bridge {
	if cfg.RemoteRoot == "" {
		cfg.RemoteRoot = "/"
	}
	root := strings.TrimSuffix(cfg.RemoteRoot, "/")
	if root == "" {
		root = "/"
	}
	br := &Bridge{
		cfg:             cfg,
		format:          meta.Format{Name: "drive9", UUID: "drive9-extent-v2", Storage: "drive9-presign", BlockSize: blockSizeMiB << 20, TrashDays: 0, DirStats: false},
		nextID:          uint64(1) << 63,
		stagedSlices:    make(map[uint64]*stagedSlice),
		blocks:          newBlockRegistry(),
		paths:           map[meta.Ino]string{rootIno: root},
		sizes:           make(map[meta.Ino]int64),
		revs:            make(map[meta.Ino]int64),
		pending:         make(map[meta.Ino][]pendingWrite),
		truncs:          make(map[meta.Ino]int64),
		modes:           make(map[meta.Ino]uint32),
		sentinels:       make(map[string]meta.Ino),
		nextHeldIno:     heldInoBase,
		moved:           make(map[meta.Ino]string),
		replacements:    make(map[string]meta.Ino),
		commitLocks:     make(map[meta.Ino]*sync.Mutex),
		classicOwned:    make(map[meta.Ino]bool),
		layoutPending:   make(map[meta.Ino]bool),
		layoutPendingID: make(map[meta.Ino]string),
		commitErr:       make(map[meta.Ino]error),
		verifyBase:      make(map[meta.Ino]verifyBase),
	}
	// Provenance refetch resolves through the inode's CURRENT mapping:
	// open files move under renames and retention aliases, and a stale
	// plan path belongs to a replacement file or nothing at all.
	br.blocks.currentPath = func(ino meta.Ino) (string, bool) {
		br.mu.Lock()
		defer br.mu.Unlock()
		p, ok := br.paths[ino]
		return p, ok
	}
	return br
}

// verifyBase is an identity-verified recovery's publication constraint.
type verifyBase struct {
	rev   int64
	resID string
}

// classicRef is one staged classic chunk read on the bounded LRU.
type classicRef struct {
	sum   string
	id    uint64
	bytes int64
	at    time.Time // staged time; inside the grace window it is immune
}

// newBridgeFinish installs cross-referencing hooks that need the assembled
// Bridge (the block registry resolves provenance inodes through the
// bridge's CURRENT path mappings — open files move under renames and
// retention aliases).

// Storage returns the object backend for the chunk store.
func (b *Bridge) Storage() objectStorage {
	return objectStorage{bridge: b}
}

// Format returns the handcrafted volume format.
func (b *Bridge) VolumeFormat() meta.Format { return b.format }

// ---- inode/path mapping ----

func (b *Bridge) remotePath(ino meta.Ino) (string, bool) {
	if ino == rootIno {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.paths[rootIno], true
	}
	b.mu.Lock()
	p, ok := b.paths[ino]
	b.mu.Unlock()
	return p, ok
}

// inoFor derives a stable inode for a remote path.
func inoFor(path string) meta.Ino {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	v := h.Sum64()
	// Stay out of the engine's special-inode range (IsSpecialNode) and the
	// reserved low inodes: fold into [100, 2^46).
	v &= (uint64(1) << 46) - 1
	if v < 100 {
		v += 100
	}
	if v == uint64(rootIno) {
		v++
	}
	return meta.Ino(v)
}

func (b *Bridge) remember(ino meta.Ino, path string) {
	b.mu.Lock()
	if cur, ok := b.paths[ino]; ok && cur != path {
		// The inode's mapping moved while this resolution was in flight
		// (a rename retargeted it, or a retention set it on an alias):
		// keep the newer destination. Overwriting it would drag the
		// descriptor back onto a name that no longer holds the file.
		b.mu.Unlock()
		return
	}
	b.paths[ino] = path
	b.mu.Unlock()
}

// childPath joins a parent path and a name.
func childPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// ---- deterministic block ids ----

// detID maps a content sha256 to a stable slice id so the chunk store's
// derived object keys (and its persistent cache) stay content-addressed.
// Ids fold into the low 61 bits, strictly disjoint from the provisional id
// space (top bit, used by NewSlice before content exists).
func detID(sum []byte) uint64 {
	if len(sum) < 8 {
		return 0
	}
	v := binary.BigEndian.Uint64(sum[:8]) & ((uint64(1) << 61) - 1)
	if v == 0 {
		v = 1
	}
	return v
}

// blockRegistry tracks content blocks seen in read plans: their sha256,
// object URLs (with expiry), and the file provenance needed to refetch a
// plan when a URL goes stale.
type blockRegistry struct {
	mu   sync.Mutex
	m    map[uint64]*blockEntry
	pins map[string]int // checksum -> pending-commit references
	// issued tracks outstanding readers of staged classic ids: an issued
	// entry is immune from LRU retirement until its fetch consumes the pin.
	issued     map[uint64]int
	issueOrder []uint64 // handout insertion order (oldest first)
	// currentPath resolves an inode to the remote path it maps to NOW.
	currentPath func(ino meta.Ino) (string, bool)
}

type blockEntry struct {
	sha256          string
	size            int64
	url             string
	expires         time.Time
	path            string // provenance for plan refetch
	provenanceStart int64
	provenanceIno   meta.Ino // whose mapping the provenance path belongs to
	cached          []byte
}

func newBlockRegistry() *blockRegistry {
	return &blockRegistry{m: make(map[uint64]*blockEntry), pins: make(map[string]int), issued: make(map[uint64]int)}
}

// ctxOf adapts a meta.Context to a context.Context (meta.Context embeds
// context.Context).
func ctxOf(c meta.Context) context.Context {
	if c == nil {
		return context.Background()
	}
	return c
}
