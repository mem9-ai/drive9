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
	"os"
	"path/filepath"
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
	// NativeWriteback declares that the chunk store runs JuiceFS's own
	// writeback pipeline: blocks are staged to the disk cache and handed
	// to the store's asynchronous uploader, so objectStorage.Put may run
	// AFTER the writer's Finish returned. The meta side then waits
	// (bounded) for a slice's blocks to land before committing it, since
	// the server verifies every referenced block at commit time.
	NativeWriteback bool
	// WritebackWait bounds that wait (0 uses the upload timeout).
	WritebackWait time.Duration
	// CacheDir is the engine cache root; staged slices are streamed here.
	CacheDir string
	// SpoolBudget bounds on-disk staging spool bytes carved out of CacheSize.
	SpoolBudget int64
	// FreeSpaceRatio is the minimum filesystem free-space ratio for disk spooling.
	FreeSpaceRatio float64
	// MaxUpload mirrors the chunk store's upload concurrency so memory-only
	// staging never blocks all upload workers simultaneously.
	MaxUpload int
}

const (
	rootIno      = meta.Ino(1)
	blockSizeMiB = 4 // must match the server-pinned extent block size
)

// Bridge implements meta.Meta against the drive9 v2 server.
type Bridge struct {
	meta.Meta // nil embed: satisfies unexported interface members; never called

	cfg       Config
	clientPtr atomic.Pointer[client.Client]
	format    meta.Format

	mu           sync.Mutex
	nextID       uint64 // provisional slice-id allocator (high bit set)
	stagedSlices map[uint64]*stagedSlice
	// blocks maps deterministic ids (content sha256, or the zero-block ids)
	// to their URL registry entry.
	blocks *blockRegistry

	// commitErr remembers the last landing failure for an inode so the
	// flush/close boundary (FlushFile) surfaces it — Bridge.Write itself
	// returns success to keep the community writer clean (its sticky
	// fileWriter.err otherwise stranded the handle after recovery).
	commitErr map[meta.Ino]error
	// closedRetained records closed handles whose final publication failed so
	// background retry and Drain keep retrying and only forget the inode once
	// all uncommitted state has landed.
	closedRetained map[meta.Ino]string
	onRetireClosed func(localPath string, ino meta.Ino)

	layoutPending map[meta.Ino]bool
	// freshCreate marks inodes this bridge CREATED locally: their first
	// publication must establish an attributable identity before writing
	// any pathname occupant (an unbound fresh descriptor must not PATCH
	// whoever happens to hold the name).
	freshCreate map[meta.Ino]bool
	// layoutPendingID is the resource identity of OUR landed upload for
	// an inode, carried forward as X-Dat9-Expected-Resource on PATCH commits.
	layoutPendingID map[meta.Ino]string
	verifyBase      map[meta.Ino]verifyBase
	// emptyPlaceholders records the FULL remote paths this bridge
	// materialized as zero-byte classic files when talking to an older server
	// that predates native 0-byte extent creation (?create=1&layout=extent),
	// together with the created revision and resource identity.
	emptyPlaceholders   map[string]verifyBase
	emptyPlaceholderOrd []string // FIFO for the bounded-recency cap below
	// commitLocks serializes per-file commit lifecycles (FlushFile).
	commitMu    sync.Mutex
	commitLocks map[meta.Ino]*sync.Mutex

	// locks is the mount-local advisory lock table (POSIX byte ranges and
	// BSD flock). In-memory and mount-scoped on purpose: see locks.go.
	locks *lockTable

	// paths caches inode → remote path; inode values are content-stable
	// hashes of the path so lookups survive restarts of the bridge.
	paths map[meta.Ino]string
	sizes map[meta.Ino]int64 // local view of file length (fresh writes)
	// buffered is the end offset of bytes the engine's writer is holding but
	// has not published yet. It is NOT a commit size: the engine buffers a
	// chunk until it fills or the file is flushed, and until then nothing
	// reaches the server. It exists because stat must already reflect those
	// bytes — POSIX grows the file on write(2), and a stat that answers "0
	// bytes" makes the KERNEL reset its own i_size to zero and serve every
	// following read as EOF. SQLite writes page 1, stats, then re-reads the
	// schema cookie; an EOF there reads as "no database" and the next
	// statement in the same sqlite3_exec batch fails with "no such table".
	buffered map[meta.Ino]int64
	revs     map[meta.Ino]int64 // newest revision known to be published
	pubSizes map[meta.Ino]int64 // remote size corresponding to revs[ino]
	// revAt records when THIS mount last published an inode. Only then can a
	// read legitimately disagree with the server (replica lag); a revision
	// recorded from someone else's write, or a publication minutes ago, gives
	// a read nothing to wait for.
	revAt    map[meta.Ino]time.Time
	pending  map[meta.Ino][]pendingWrite
	truncs   map[meta.Ino]int64  // truncate-only growth awaiting the next commit
	truncSeq map[meta.Ino]uint64 // monotonic counter fencing b.truncs cleanup
	retryRunning bool
	retryStop    chan struct{}
	retryOnce    sync.Once
	retryWg      sync.WaitGroup
	fetchWg      sync.WaitGroup
	// modes and mtimes remember local-only create/setattr values until the
	// first remote publication applies them server-side.
	modes  map[meta.Ino]uint32
	mtimes map[meta.Ino]time.Time
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
	if cfg.CacheDir != "" {
		if entries, err := os.ReadDir(cfg.CacheDir); err == nil {
			for _, ent := range entries {
				if strings.HasPrefix(ent.Name(), "extent-stage-") || strings.HasPrefix(ent.Name(), "extent-spill-") {
					_ = os.Remove(filepath.Join(cfg.CacheDir, ent.Name()))
				}
			}
		}
	}
	br := &Bridge{
		cfg:               cfg,
		format:            meta.Format{Name: "drive9", UUID: "drive9-extent-v2", Storage: "drive9-presign", BlockSize: blockSizeMiB << 20, TrashDays: 0, DirStats: false},
		nextID:            uint64(1) << 63,
		stagedSlices:      make(map[uint64]*stagedSlice),
		blocks:            newBlockRegistry(),
		paths:             map[meta.Ino]string{rootIno: root},
		sizes:             make(map[meta.Ino]int64),
		revs:              make(map[meta.Ino]int64),
		pubSizes:          make(map[meta.Ino]int64),
		revAt:             make(map[meta.Ino]time.Time),
		buffered:          make(map[meta.Ino]int64),
		pending:           make(map[meta.Ino][]pendingWrite),
		truncs:            make(map[meta.Ino]int64),
		truncSeq:          make(map[meta.Ino]uint64),
		retryStop:         make(chan struct{}),
		modes:             make(map[meta.Ino]uint32),
		mtimes:            make(map[meta.Ino]time.Time),
		sentinels:         make(map[string]meta.Ino),
		nextHeldIno:       heldInoBase,
		moved:             make(map[meta.Ino]string),
		replacements:      make(map[string]meta.Ino),
		commitLocks:       make(map[meta.Ino]*sync.Mutex),
		locks:             newLockTable(),
		layoutPending:     make(map[meta.Ino]bool),
		layoutPendingID:   make(map[meta.Ino]string),
		freshCreate:       make(map[meta.Ino]bool),
		commitErr:         make(map[meta.Ino]error),
		closedRetained:    make(map[meta.Ino]string),
		verifyBase:        make(map[meta.Ino]verifyBase),
		emptyPlaceholders: make(map[string]verifyBase),
	}
	br.clientPtr.Store(cfg.Client)
	br.blocks.cacheDir = cfg.CacheDir
	br.blocks.diskBudget = cfg.SpoolBudget
	br.blocks.freeRatio = cfg.FreeSpaceRatio
	br.blocks.maxUpload = cfg.MaxUpload
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

func (b *Bridge) client() *client.Client {
	return b.clientPtr.Load()
}

// ResetForGeneration stops any background retry loop and in-flight block
// fetches, then resets all mutable bridge state before binding the shared
// engine to a new main generation.
func (b *Bridge) ResetForGeneration(cl *client.Client) {
	if b == nil {
		return
	}
	_ = b.ShutdownCtx(context.Background())

	b.commitMu.Lock()
	b.commitLocks = make(map[meta.Ino]*sync.Mutex)
	b.commitMu.Unlock()

	root := strings.TrimSuffix(b.cfg.RemoteRoot, "/")
	if root == "" {
		root = "/"
	}

	b.clientPtr.Store(cl)

	b.mu.Lock()
	b.stagedSlices = make(map[uint64]*stagedSlice)
	b.paths = map[meta.Ino]string{rootIno: root}
	b.sizes = make(map[meta.Ino]int64)
	b.revs = make(map[meta.Ino]int64)
	b.pubSizes = make(map[meta.Ino]int64)
	b.revAt = make(map[meta.Ino]time.Time)
	b.buffered = make(map[meta.Ino]int64)
	b.pending = make(map[meta.Ino][]pendingWrite)
	b.truncs = make(map[meta.Ino]int64)
	b.truncSeq = make(map[meta.Ino]uint64)
	b.modes = make(map[meta.Ino]uint32)
	b.mtimes = make(map[meta.Ino]time.Time)
	b.sentinels = make(map[string]meta.Ino)
	b.nextHeldIno = heldInoBase
	b.moved = make(map[meta.Ino]string)
	b.replacements = make(map[string]meta.Ino)
	b.locks = newLockTable()
	b.layoutPending = make(map[meta.Ino]bool)
	b.layoutPendingID = make(map[meta.Ino]string)
	b.freshCreate = make(map[meta.Ino]bool)
	b.commitErr = make(map[meta.Ino]error)
	b.closedRetained = make(map[meta.Ino]string)
	b.onRetireClosed = nil
	b.verifyBase = make(map[meta.Ino]verifyBase)
	b.emptyPlaceholders = make(map[string]verifyBase)
	b.emptyPlaceholderOrd = nil
	b.retryRunning = false
	b.retryStop = make(chan struct{})
	b.retryOnce = sync.Once{}
	b.mu.Unlock()

	b.blocks.resetForGeneration()
}

// verifyBase is an identity-verified recovery's publication constraint.
type verifyBase struct {
	rev   int64
	resID string
}

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
// plan when a URL is stale.
type blockRegistry struct {
	mu   sync.Mutex
	m    map[uint64]*blockEntry
	pins map[string]int // checksum -> pending-commit references
	// pinSizes remembers the byte volume accounted for each pinned
	// checksum, so unpin retires exactly what pin added even if the entry
	// was released or replaced in between.
	pinSizes map[string]int64
	// pinnedBytes is the staged-byte volume currently held by pending
	// commits (checksums with pins > 0). Pinned entries are exempt from
	// the classic-read LRU, so without a bound a commit-side stall grows
	// this without limit: a 6 GiB seqwrite against a slow server OOM-killed
	// the worker at 7.58 GiB RSS. waitPinnedBudget turns that cliff into
	// write backpressure.
	pinnedBytes int64
	// stagedMemBytes tracks slice bytes currently buffered or reserved in
	// Bridge.stagedSlices (non-Writeback in-memory staging) before Bridge.Write
	// moves them into pins.
	stagedMemBytes int64
	// waitingSlices tracks slice IDs parked in waitSliceAdmission so freed
	// budget admits the lowest slice ID first instead of starving older slices.
	waitingSlices map[uint64]int
	// sliceTarget / sliceUsed / sliceWriters track per-slice headroom
	// reservation so admitting block 0 of a 4 MiB-blocked slice accounts its
	// expected 64 MiB chunk footprint immediately instead of letting MaxUpload
	// workers admit 16 concurrent slices in parallel before subsequent blocks fit.
	sliceTarget     map[uint64]int64
	sliceUsed       map[uint64]int64
	stagedDiskBytes int64
	diskBudget      int64
	freeRatio       float64
	cacheDir        string
	maxUpload       int
	// budgetCh is closed (and replaced under mu) whenever pinnedBytes or
	// stagedMemBytes drops or a waiter departs, waking budget waiters.
	budgetCh chan struct{}
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
	return &blockRegistry{
		m:             make(map[uint64]*blockEntry),
		pins:          make(map[string]int),
		pinSizes:      make(map[string]int64),
		waitingSlices: make(map[uint64]int),
		sliceTarget:   make(map[uint64]int64),
		sliceUsed:     make(map[uint64]int64),
		budgetCh:      make(chan struct{}),
	}
}

func (r *blockRegistry) resetForGeneration() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.m = make(map[uint64]*blockEntry)
	r.pins = make(map[string]int)
	r.pinSizes = make(map[string]int64)
	r.pinnedBytes = 0
	r.stagedMemBytes = 0
	r.waitingSlices = make(map[uint64]int)
	r.sliceTarget = make(map[uint64]int64)
	r.sliceUsed = make(map[uint64]int64)
	r.stagedDiskBytes = 0
	close(r.budgetCh)
	r.budgetCh = make(chan struct{})
	r.mu.Unlock()
}

// ctxOf adapts a meta.Context to a context.Context (meta.Context embeds
// context.Context).
func ctxOf(c meta.Context) context.Context {
	if c == nil {
		return context.Background()
	}
	return c
}
