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
	id, _, size, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	if limit < 0 {
		limit = size - off
	}
	if limit <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	s.bridge.mu.Lock()
	sl := s.bridge.stagedSlices[id]
	s.bridge.mu.Unlock()
	if sl != nil {
		staged := sl.join()
		if off > int64(len(staged)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		end := off + limit
		if end > int64(len(staged)) || limit <= 0 {
			end = int64(len(staged))
		}
		return io.NopCloser(bytes.NewReader(staged[off:end])), nil
	}
	return s.bridge.blocks.fetch(ctx, s.bridge.cfg.Client, id, off, limit)
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

func (r *blockRegistry) register(sha, url string, length int64, path string) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	id := detID(sum)
	r.mu.Lock()
	r.m[id] = &blockEntry{sha256: sha, size: length, url: url, expires: time.Now().Add(urlTTL), path: path}
	r.mu.Unlock()
	return id
}

// stageBytes caches refetched block content for pending commits.
func (r *blockRegistry) stageBytes(sha string, data []byte) {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := detID(sum)
	if e, ok := r.m[id]; ok {
		e.cached = data
	} else {
		r.m[id] = &blockEntry{sha256: sha, size: int64(len(data)), cached: data}
	}
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
	r.mu.Lock()
	e, ok := r.m[id]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("block id %d is unknown to the bridge", id)
	}
	if limit < 0 {
		limit = e.size - off
	}
	if limit <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if e.url != "" && time.Now().Before(e.expires) {
		return c.FetchExtentBlockRange(ctx, e.url, off, limit)
	}
	if e.cached != nil {
		if off > int64(len(e.cached)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		end := off + limit
		if end > int64(len(e.cached)) || limit <= 0 {
			end = int64(len(e.cached))
		}
		return io.NopCloser(bytes.NewReader(e.cached[off:end])), nil
	}
	if e.path == "" {
		return nil, fmt.Errorf("block %d has neither URL nor provenance", id)
	}
	plan, err := c.GetExtentReadPlan(ctx, e.path, e.provenanceStart, e.provenanceStart+e.size)
	if err != nil {
		return nil, err
	}
	for _, blk := range plan.Blocks {
		if blk.BlockKey == e.sha256 {
			r.mu.Lock()
			e.url = blk.URL
			e.expires = time.Now().Add(urlTTL)
			r.mu.Unlock()
			return c.FetchExtentBlockRange(ctx, blk.URL, off, limit)
		}
	}
	return nil, fmt.Errorf("block %s no longer present in the plan of %s", e.sha256[:12], e.path)
}

// fetchClient is the subset of *client.Client the registry needs.
type fetchClient interface {
	FetchExtentBlockRange(ctx context.Context, url string, offset, length int64) (io.ReadCloser, error)
	GetExtentReadPlan(ctx context.Context, path string, start, end int64) (*client.ExtentReadPlan, error)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type bytesAt []byte

func (b bytesAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

var _ = sync.Mutex{} // keep sync import while the registry evolves
