package extent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/client"
)

// objectStorage adapts the chunk store onto the v2 presign data plane.
//
// Writes: the engine uploads a slice object BEFORE committing its metadata
// (vfs flushes the store first, then calls meta.Write), but v2 presigns
// blocks only inside an upload session. Put therefore stages the bytes in
// memory; the real presign+PUT happens in Bridge.commit once the session
// exists (meta.Write/Close).
//
// Reads: deterministic block ids resolve through the read-plan URL
// registry; synthesized zero blocks never leave memory.
type objectStorage struct {
	bridge *Bridge
}

const maxStageBytes = 256 << 20

func (s objectStorage) String() string { return "drive9-presign" }

func (s objectStorage) Limits() object.Limits { return object.Limits{} }

func (s objectStorage) Create(ctx context.Context) error { return nil }

// parseKey extracts the slice id and object size from a JuiceFS-derived key
// (chunks/XX/a/b/<id>_<indx>_<size>).
func parseKey(key string) (id uint64, indx int64, size int64, err error) {
	parts := strings.Split(key, "/")
	last := parts[len(parts)-1]
	fields := strings.Split(last, "_")
	if len(fields) != 3 {
		return 0, 0, 0, fmt.Errorf("unrecognized chunk key %q", key)
	}
	id, err = strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("chunk key id %q: %w", fields[0], err)
	}
	indx, err = strconv.ParseInt(fields[1], 10, 63)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("chunk key indx %q: %w", fields[1], err)
	}
	size, err = strconv.ParseInt(fields[2], 10, 63)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("chunk key size %q: %w", fields[2], err)
	}
	return id, indx, size, nil
}

func (s objectStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	id, indx, size, err := parseKey(key)
	if err != nil {
		return err
	}
	if size <= 0 || size > maxStageBytes {
		return fmt.Errorf("staged slice size %d out of bounds", size)
	}
	data, err := io.ReadAll(io.LimitReader(in, size))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("short staging read: %d of %d", len(data), size)
	}
	// One slice object may span several blocks (Size > BlockSize); the
	// store uploads them as separate keys with the same slice id. Stage
	// per block index; meta.Write concatenates in order.
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	sl := s.bridge.stagedSlices[id]
	if sl == nil {
		sl = &stagedSlice{blocks: make(map[int64][]byte)}
		s.bridge.stagedSlices[id] = sl
	}
	sl.blocks[indx] = data
	sl.total += size
	return nil
}

// stagedSlice accumulates the blocks of one not-yet-committed slice object.
type stagedSlice struct {
	blocks map[int64][]byte
	total  int64
}

// join returns the concatenated slice object bytes.
func (sl *stagedSlice) join() []byte {
	out := make([]byte, 0, sl.total)
	for i := int64(0); int64(len(out)) < sl.total; i++ {
		b, ok := sl.blocks[i]
		if !ok {
			break
		}
		out = append(out, b...)
	}
	return out
}

func (s objectStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	id, indx, size, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	// The chunk store addresses offsets WITHIN one 4 MiB block; the key's
	// block index selects which slice of a multi-block backing object is
	// being read (engine slices and server-compacted blocks span up to
	// 64 MiB). Into the joined object the byte position is therefore
	// indx*blockSize + off — using off alone made every block read the
	// object's first 4 MiB.
	objOff := int64(indx)*client.ExtentBlockSize + off
	if limit < 0 {
		limit = size - off
	}
	if limit <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	// An issued classic slice stays fetchable for its whole reader
	// lifetime: this Get ACQUIRES a pin before touching the registry (a
	// concurrent stageClassicChunk cannot retire the entry between the
	// pin and the snapshot) and releases ONE pin on exit — the handout
	// issued one pin per block, so multi-block slices stay protected
	// across their separate Gets.
	s.bridge.mu.Lock()
	sl := s.bridge.stagedSlices[id]
	s.bridge.mu.Unlock()
	if sl == nil {
		// A fetch pins for its OWN duration only (a concurrent stage
		// cannot retire the entry between the pin and the snapshot —
		// retirement checks issued under the same registry lock). The
		// HANDOUT pins are NOT consumed here: a block boundary is not the
		// end of an outstanding slice user (rSlice loads blocks one at a
		// time, and other readers may hold the same id), so clearing on a
		// block boundary would strip protection mid-slice and from other
		// readers. Handout pins drain when the entry is re-staged fresh
		// (the reader set turned over) or stay accounted until then.
		s.bridge.blocks.issuePin(id)              // transient: acquire-to-snapshot
		defer s.bridge.blocks.releaseIssuePin(id) // transient out
		s.bridge.blocks.releaseIssuePin(id)       // drain one handout pin
	}
	if sl != nil {
		staged := sl.join()
		if objOff > int64(len(staged)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		end := objOff + limit
		if end > int64(len(staged)) {
			end = int64(len(staged))
		}
		return io.NopCloser(bytes.NewReader(staged[objOff:end])), nil
	}
	return s.bridge.blocks.fetch(ctx, s.bridge.cfg.Client, id, objOff, limit)
}

// simpleObject implements object.Object for Head replies.
type simpleObject struct {
	key   string
	size  int64
	mtime time.Time
}

func (o simpleObject) Key() string          { return o.key }
func (o simpleObject) Size() int64          { return o.size }
func (o simpleObject) Mtime() time.Time     { return o.mtime }
func (o simpleObject) IsDir() bool          { return false }
func (o simpleObject) IsSymlink() bool      { return false }
func (o simpleObject) StorageClass() string { return "" }
func (o simpleObject) Status() string       { return "" }

func (s objectStorage) Head(ctx context.Context, key string) (object.Object, error) {
	_, _, size, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	return simpleObject{key: key, size: size, mtime: time.Now()}, nil
}

func (s objectStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	// Block deletion is server-owned (tenant-worker GC).
	return nil
}

func (s objectStorage) Copy(ctx context.Context, dst, src string) error {
	return fmt.Errorf("copy is not supported by the drive9 presign plane")
}

func (s objectStorage) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	return nil, false, "", fmt.Errorf("list is not supported by the drive9 presign plane")
}

func (s objectStorage) ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan object.Object, error) {
	return nil, fmt.Errorf("list is not supported by the drive9 presign plane")
}

func (s objectStorage) CreateMultipartUpload(ctx context.Context, key string) (*object.MultipartUpload, error) {
	return nil, fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) UploadPart(ctx context.Context, key string, uploadID string, num int, body []byte) (*object.Part, error) {
	return nil, fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) UploadPartCopy(ctx context.Context, key string, uploadID string, num int, srcKey string, off, size int64) (*object.Part, error) {
	return nil, fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) AbortUpload(ctx context.Context, key string, uploadID string) {}

func (s objectStorage) CompleteUpload(ctx context.Context, key string, uploadID string, parts []*object.Part) error {
	return fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) ListUploads(ctx context.Context, marker string) ([]*object.PendingPart, string, error) {
	return nil, "", nil
}

func (s objectStorage) Restore(ctx context.Context, key string, days int32) error {
	return fmt.Errorf("restore is not supported by the drive9 presign plane")
}

// ---- registry implementation ----

const urlTTL = 8 * time.Minute

func (r *blockRegistry) register(sha, url string, length int64, path string, fileOffset int64, ino meta.Ino) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	id := detID(sum)
	r.mu.Lock()
	// Refresh URL/provenance but PRESERVE cached bytes: a pending commit may
	// still need them (a sibling staged the same checksum and its upload
	// reader has not consumed it); clearing them here made a fresh sibling
	// upload zeros for real content. Bytes retire only via
	// releaseCommitted's reference-aware path.
	if e, ok := r.m[id]; ok {
		e.url = url
		e.expires = time.Now().Add(urlTTL)
		e.path = path
		e.provenanceStart = fileOffset
		e.provenanceIno = ino
		if e.size < length {
			e.size = length
		}
	} else {
		r.m[id] = &blockEntry{sha256: sha, size: length, url: url, expires: time.Now().Add(urlTTL), path: path, provenanceStart: fileOffset, provenanceIno: ino}
	}
	r.mu.Unlock()
	return id
}

// stageBytes caches block content for pending commits. Idempotent: an
// already-staged block keeps its first copy (write retries used to
// re-copy the whole slice on every attempt).
// registerLocal publishes a locally staged block (deferred write) under
// its deterministic id so the engine's reader can fetch it from staging.
func (r *blockRegistry) registerLocal(sha string, length int64) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	return detID(sum)
}

// stagePinned stores staged bytes with an optional pending-commit pin;
// pinned entries are never retired by the classic-read LRU.
func (r *blockRegistry) stagePinned(sha string, data []byte, pinned bool) uint64 {
	id := r.stageBytes(sha, data)
	if pinned {
		r.pin(sha)
	}
	return id
}

// pin marks a checksum as referenced by a pending commit.
func (r *blockRegistry) pin(sha string) {
	r.mu.Lock()
	r.pins[sha]++
	r.mu.Unlock()
}

// unpin releases a pending-commit reference.
func (r *blockRegistry) unpin(sha string) {
	r.mu.Lock()
	if r.pins[sha] > 0 {
		r.pins[sha]--
	}
	r.mu.Unlock()
}

// issuePin increments the issued-reader count for a staged id (the
// registry entry is immune from LRU retirement until fetched).
func (r *blockRegistry) issuePin(id uint64) {
	if id == 0 {
		return
	}
	r.mu.Lock()
	if r.issued[id] == 0 {
		r.issueOrder = append(r.issueOrder, id)
	}
	r.issued[id]++
	r.mu.Unlock()
}

// releaseIssuePin decrements an id's issued-reader count; true when a pin
// existed.
func (r *blockRegistry) releaseIssuePin(id uint64) bool {
	if id == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.issued[id] <= 0 {
		return false
	}
	r.issued[id]--
	if r.issued[id] == 0 {
		delete(r.issued, id)
		for i, v := range r.issueOrder {
			if v == id {
				r.issueOrder = append(r.issueOrder[:i], r.issueOrder[i+1:]...)
				break
			}
		}
	}
	return true
}

// markLocalOverlay flags an entry whose cached bytes include local-only
// writes (a deferred overlay): demotion would replace them with the older
// remote image, so the bytes are irreducible.
func (r *blockRegistry) markLocalOverlay(id uint64, ino meta.Ino) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.m[id]; ok {
		e.localOverlay = true
		if ino != 0 {
			e.provenanceIno = ino
		}
	}
}

// demoteToProvenance drops a classic entry's cached bytes while keeping
// the id fetchable: the remote range is the usable source for any
// outstanding reader's later block fetches. Entries whose bytes are NOT
// reproducible from the remote range — locally overlaid slices (deferred
// writes only exist here) or checksums a pending commit still references
// — are NOT demoted; their bytes stay (accounted by the LRU re-append in
// the caller).
func (r *blockRegistry) demoteToProvenance(id uint64, sum, path string, start int64, resID string, ino meta.Ino) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[id]
	if !ok || e.cached == nil {
		return true // nothing to keep
	}
	if r.pins[sum] > 0 {
		return false // a pending commit still references these bytes
	}
	if e.localOverlay {
		// Local-only bytes are irreducible: keep them RESIDENT (the
		// reader must see them) — the CALLER re-appends the LRU record so
		// they stay accounted, and the entry self-clears the overlay flag
		// once a flush's releaseCommitted retires its pending fragments
		// (the then-pure-remote image becomes demotable again).
		return false
	}
	if resID == "" {
		// No identity for the source: the pathname is REUSABLE (renames,
		// replacements) and cannot be trusted as this slice's source —
		// keep the bytes resident instead of risking cross-file service.
		return false
	}
	e.cached = nil
	e.path = path
	e.provenanceStart = start
	e.classicRange = e.size
	e.provResID = resID
	e.provenanceIno = ino // rename-following: fetch resolves the CURRENT mapping
	return true
}

// setProvenanceIdentity records the source object's identity on a staged
// classic entry (the demotion refetch verifies it).
func (r *blockRegistry) setProvenanceIdentity(id uint64, resID string, ino meta.Ino) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.m[id]; ok {
		e.provResID = resID
		if ino != 0 {
			e.provenanceIno = ino
		}
	}
}

// resolveOverlaysFor clears the local-overlay flag of every classic
// entry staged from one inode: after the flush landed that inode's
// pending fragments, the whole-chunk overlay images (whose checksums
// differ from the fragment checksums) match the remote object again and
// may demote.
func (r *blockRegistry) resolveOverlaysFor(ino meta.Ino) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.m {
		if e.provenanceIno == ino && e.localOverlay {
			e.localOverlay = false
		}
	}
}

// trimIssued is a no-op retained for call-site symmetry: pins belonging
// to outstanding readers are NEVER revoked by cache pressure (revoking
// them makes valid reads fail with no reconstructable source). The
// memory bound comes from the LRU accounting (issued entries stay
// counted) plus entry reuse: re-staging the same id refreshes the entry,
// and the byte budget bounds the non-issued population. A hard
// outstanding-set cap would reintroduce reader failure and is
// deliberately not implemented.
func (r *blockRegistry) trimIssued(keep int) {
	_ = keep
}

// isIssued reports whether an outstanding reader holds the staged id.
func (r *blockRegistry) isIssued(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.issued[id] > 0
}

// releaseIfUnpinned drops a staged entry nothing references — pinned by
// neither a pending commit NOR an outstanding reader (one critical
// section: the issued check and the deletion are atomic).
func (r *blockRegistry) releaseIfUnpinned(sha string) {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pins[sha] > 0 || r.issued[detID(sum)] > 0 {
		return
	}
	delete(r.pins, sha)
	delete(r.m, detID(sum))
}

func (r *blockRegistry) stageBytes(sha string, data []byte) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := detID(sum)
	if e, ok := r.m[id]; ok {
		if e.cached == nil {
			e.cached = data
		}
		return id
	}
	r.m[id] = &blockEntry{sha256: sha, size: int64(len(data)), cached: data}
	return id
}

// release drops a block's staged bytes. Called after a successful commit —
// the bytes live in object storage now, and nothing bounds the registry's
// cached copies, so a write-only workload otherwise retained everything
// it ever wrote for the mount's lifetime.
func (r *blockRegistry) release(sha string) {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return
	}
	r.mu.Lock()
	if e, ok := r.m[detID(sum)]; ok {
		e.cached = nil
	}
	r.mu.Unlock()
}

func (r *blockRegistry) stagedBytes(sha string) ([]byte, bool) {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.m[detID(sum)]; ok && e.cached != nil {
		return e.cached, true
	}
	return nil, false
}

// fetch serves a block byte range, refetching the file's read plan when the
// cached URL is missing or stale. limit < 0 means "to the end of the
// object" (the JuiceFS ObjectStorage contract).
func (r *blockRegistry) fetch(ctx context.Context, c fetchClient, id uint64, off, limit int64) (io.ReadCloser, error) {
	// Snapshot the entry under the lock; parallel fetches and stageBytes
	// mutate the same entry.
	r.mu.Lock()
	e, ok := r.m[id]
	var snap blockEntry
	if ok {
		snap = *e
	}
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("block id %d is unknown to the bridge", id)
	}
	if limit < 0 {
		limit = snap.size - off
	}
	if limit <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if snap.url != "" && time.Now().Before(snap.expires) {
		return c.FetchExtentBlockRange(ctx, snap.url, off, limit)
	}
	if snap.cached != nil {
		if off > int64(len(snap.cached)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		end := off + limit
		if end > int64(len(snap.cached)) || limit <= 0 {
			end = int64(len(snap.cached))
		}
		return io.NopCloser(bytes.NewReader(snap.cached[off:end])), nil
	}
	if snap.path == "" {
		return nil, fmt.Errorf("block %d has neither URL nor provenance", id)
	}
	path := snap.path
	if snap.provenanceIno != 0 && r.currentPath != nil {
		// The file may have been renamed or retained under an alias since
		// this plan was fetched; refresh through the inode's CURRENT
		// mapping, never the stale name.
		if cur, ok := r.currentPath(snap.provenanceIno); ok {
			path = cur
		}
	}
	if snap.classicRange > 0 {
		// Classic-read provenance: the bytes were dropped under cache
		// pressure while a reader still held the id. The refetch's proof
		// is the READ's OWN response identity (the pre-stat only fences
		// the obvious): a mismatch or absent echo errors — never wrong
		// bytes under this id.
		if snap.provResID == "" {
			return nil, fmt.Errorf("classic slice %d has no verified source", id)
		}
		rc, rerr := c.ReadStreamRange(ctx, path, snap.provenanceStart+off, limit)
		if rerr != nil {
			return nil, rerr
		}
		if got := rcHeaderResourceID(rc); got == "" {
			// Redirected (302/307) and legacy-inline reads carry no echo.
			// Fall back to an identity STAT fenced against the same
			// replacement risk as before the read: close the unverified
			// body, verify, then re-read. A mismatch errors (never wrong
			// bytes); the residual race is identical to the pre-read stat.
			_ = rc.Close()
			curID, serr := c.StatResourceID(ctx, path)
			if serr != nil || curID != snap.provResID {
				return nil, fmt.Errorf("classic slice %d source no longer holds the staged object", id)
			}
			rc, rerr = c.ReadStreamRange(ctx, path, snap.provenanceStart+off, limit)
			if rerr != nil {
				return nil, rerr
			}
		} else if got != snap.provResID {
			_ = rc.Close()
			return nil, fmt.Errorf("classic slice %d source no longer holds the staged object", id)
		}
		return rc, nil
	}
	plan, err := c.GetExtentReadPlan(ctx, path, snap.provenanceStart, snap.provenanceStart+snap.size)
	if err != nil {
		return nil, err
	}
	for _, blk := range plan.Blocks {
		if blk.BlockKey == snap.sha256 {
			r.mu.Lock()
			if cur, still := r.m[id]; still {
				cur.url = blk.URL
				cur.expires = time.Now().Add(urlTTL)
			}
			r.mu.Unlock()
			return c.FetchExtentBlockRange(ctx, blk.URL, off, limit)
		}
	}
	return nil, fmt.Errorf("block %s no longer present in the plan of %s", snap.sha256[:12], path)
}

// fetchClient is the subset of *client.Client the registry needs.
type fetchClient interface {
	FetchExtentBlockRange(ctx context.Context, url string, offset, length int64) (io.ReadCloser, error)
	GetExtentReadPlan(ctx context.Context, path string, start, end int64) (*client.ExtentReadPlan, error)
	ReadStreamRange(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
	StatResourceID(ctx context.Context, path string) (string, error)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var _ = sync.Mutex{} // keep sync import while the registry evolves
