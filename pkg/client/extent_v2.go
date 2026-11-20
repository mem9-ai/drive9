package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	// extentPatchPutConcurrency bounds the parallel presigned block PUTs one
	// PATCH commit issues (the completion verifies every part, so the round
	// trips are independent). Sixteen parallel 4 MiB PUTs keep a dirty batch
	// from being paced by a single stream's latency without stampeding the
	// object store.
	extentPatchPutConcurrency = 16
	// extentBlockPutTimeout bounds one block PUT attempt so a silent TCP/TLS
	// stall against object storage cannot wedge commit locks indefinitely.
	extentBlockPutTimeout = 2 * time.Minute
)

// Extent content layout v2. The server owns the block layout (64 MiB chunks
// of content-addressed 4 MiB blocks) and all maintenance; the client speaks
// three verbs: ranged GETs return a JSON block read plan, PATCH declares
// dirty blocks, and upload sessions PUT blocks to presigned URLs. The client
// never learns object keys — only presigned URLs and content checksums.

// ExtentChunkSize mirrors the server's fixed chunk span (64 MiB). It is a
// client-side cache-granularity hint only; a plan's blocks are authoritative
// for where bytes live.
const ExtentChunkSize = int64(1 << 26)

// ExtentBlockSize mirrors the server's default block size (4 MiB). Dirty
// ranges are cut into blocks of at most this size before a patch commit.
const ExtentBlockSize = int64(4 << 20)

// Centralized extent v2 server error wire contract (verified against tidbcloud/fs/pkg/server).
// Classifiers match HTTP status + optional machine-readable StatusError.Code first,
// then fall back to these pinned server wire literals:
//   - HTTP 409 Conflict: ErrExtentLayoutMismatch ("content layout mismatch...") on
//     V2 upload initiate (server.go:5926, extent_maintenance.go:220).
//   - HTTP 400 Bad Request / 501 Not Implemented (or masked 500 "internal storage error"):
//     s3client.ErrLengthBoundedPresignUnsupported ("store cannot enforce a declared upload
//     length at write time", extent_blocks.go:268) on the initial presign-batch call.
//     Currently tidbcloud/fs handleV2PresignBatch (server.go:6076) masks it behind exact
//     500 "internal storage error" until ErrLengthBoundedPresignUnsupported is added to
//     ClientCausedSentinels, so isExtentPresignUnsupportedError accepts both the 400/501
//     code/literal and exact 500 "internal storage error" (safe: classic fallback fails
//     closed with 409 on sticky extent targets).
//   - HTTP 400 Bad Request: "total_size must be positive" on V2 upload initiate
//     (tidbcloud/fs server.go:5891, drive9 server.go:4617).
//   - HTTP 500 Internal Server Error: exact "internal storage error" (mem9-ai/drive9)
//     or "storage backend unavailable; contact support" (tidbcloud/fs server.go:8394)
//     from handleWrite's errJSONInternalStorage when direct PUT hits a sticky extent inode,
//     or from handleV2PresignBatch when s3client.ErrLengthBoundedPresignUnsupported is masked.
const (
	extentErrCodeLayoutMismatch           = "extent_layout_mismatch"
	extentErrMsgLayoutMismatch            = "content layout mismatch"
	extentErrCodePresignLengthUnsupported = "extent_presign_length_unsupported"
	extentErrMsgPresignLengthUnsupported  = "cannot enforce a declared upload length"
	extentErrCodeZeroSizeUpload           = "zero_size_upload_unsupported"
	extentErrMsgZeroSizeUpload            = "total_size must be positive"
	extentErrMsgDirectWriteInternal       = "internal storage error"
	extentErrMsgCloudBackendUnavailable   = "storage backend unavailable; contact support"
)

func isOpaqueInternalStorageMessage(msg string) bool {
	trimmed := strings.TrimSpace(msg)
	return strings.EqualFold(trimmed, extentErrMsgDirectWriteInternal) ||
		strings.EqualFold(trimmed, extentErrMsgCloudBackendUnavailable)
}

// ErrExtentNotServed reports a read response that is not an extent block
// plan (no extent content-layout header): callers fall back to the classic
// read path.
var ErrExtentNotServed = errors.New("extent read plan not served")

// ExtentReadPlanBlock is one piece of a read plan: a byte range of the file
// served from one presigned object GET, or a zero run for a hole.
type ExtentReadPlanBlock struct {
	FileOffset  int64  `json:"file_offset"`
	Length      int64  `json:"length"`
	BlockKey    string `json:"block_key,omitempty"`
	BlockOffset int64  `json:"block_offset"`
	URL         string `json:"url,omitempty"`
	Zero        bool   `json:"zero,omitempty"`
}

// ExtentReadPlan answers a ranged GET on an extent file.
type ExtentReadPlan struct {
	Size       int64                 `json:"size"`
	Revision   int64                 `json:"revision"`
	Blocks     []ExtentReadPlanBlock `json:"blocks"`
	NextOffset int64                 `json:"next_offset,omitempty"`
}

// ExtentDirtyBlock declares one dirty block of a patch: the size bytes at
// chunk*64MiB+off whose content hashes to Checksum (sha256 hex).
type ExtentDirtyBlock struct {
	Chunk    uint32 `json:"chunk"`
	Off      uint32 `json:"off"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

// ExtentWritePlanBlock is one dirty block of a patch plan and its upload URL.
type ExtentWritePlanBlock struct {
	Chunk    uint32 `json:"chunk"`
	Off      uint32 `json:"off"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
	URL      string `json:"url"`
	// Headers are the request headers the PUT must carry verbatim: the
	// server signs them into the URL (S3 checksum, GCS create-once
	// precondition) or the store requires them (Azure blob type).
	Headers map[string]string `json:"headers,omitempty"`
	// Exists reports that the block content is already in the object store;
	// the client skips the PUT and completes with the checksum alone.
	Exists bool `json:"exists"`
}

// ExtentWritePlan is the PATCH response for an extent file.
type ExtentWritePlan struct {
	UploadID         string                 `json:"upload_id"`
	NewSize          int64                  `json:"new_size"`
	ExpiresAt        string                 `json:"expires_at"`
	Blocks           []ExtentWritePlanBlock `json:"blocks"`
	ChecksumContract checksumContract       `json:"checksum_contract"`
}

// GetExtentReadPlan fetches the block read plan covering [start, end] (end
// inclusive; end < 0 means open-ended) of an extent-layout file. Non-extent
// responses fail with ErrExtentNotServed so callers can fall back to the
// classic read path.
func (c *Client) GetExtentReadPlan(ctx context.Context, path string, start, end int64) (*ExtentReadPlan, error) {
	rng := fmt.Sprintf("bytes=%d-", start)
	if end >= 0 {
		rng = fmt.Sprintf("bytes=%d-%d", start, end)
	}
	// Opt in to the block-plan representation: every plan request
	// (first page and continuations) must declare support or the server
	// answers 422.
	resp, err := c.readWithoutRedirectOptIn(ctx, path, rng, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return nil, readError(resp)
	}
	if ContentLayout(resp.Header.Get("X-Dat9-Content-Layout")) != ContentLayoutExtent {
		return nil, ErrExtentNotServed
	}
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var plan ExtentReadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode extent read plan: %w", err)
	}
	// The revision a plan represents travels in the RESPONSE HEADER, not in
	// the plan body: the server writes setFileReadHeaders (ETag,
	// X-Dat9-Resource-ID, X-Dat9-Revision) before it encodes the block plan,
	// and the plan struct itself carries only size/blocks/next_offset. Left
	// unread here, plan.Revision stays 0 on every response and any consumer
	// that treats "no revision" as "cannot be judged" accepts whatever the
	// server answered — including a plan from a replica that has not caught up
	// with a commit this mount already published.
	if plan.Revision <= 0 {
		if raw := resp.Header.Get("X-Dat9-Revision"); raw != "" {
			if rev, perr := strconv.ParseInt(raw, 10, 64); perr == nil && rev > 0 {
				plan.Revision = rev
			}
		}
	}
	return &plan, nil
}

// extentPlanReader serves an extent file sequentially from a read plan,
// fetching continuation plans via next_offset as it advances. Whole blocks
// are fetched per block URL; random-access sub-block reads belong to the
// FUSE read handle, not this streaming reader.
type extentPlanReader struct {
	ctx  context.Context
	c    *Client
	path string

	mu     sync.Mutex
	closed bool

	plan *ExtentReadPlan
	bi   int   // index into plan.Blocks
	off  int64 // logical offset served so far
	cur  io.ReadCloser
	// zeroLeft tracks the REMAINING length of the current zero run; zeros
	// are synthesized on demand instead of pre-allocated, so arbitrarily
	// large holes never cap or shift the stream.
	zeroLeft int64
	// limit bounds the total bytes the reader may serve (-1 = unbounded);
	// ranged reads must never stream past the caller's requested length.
	// rangeEnd caps continuation fetches to the original window end.
	limit         int64
	rangeEnd      int64
	refreshOffset int64
	refreshCount  int
}

func (r *extentPlanReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, fmt.Errorf("read on closed extent plan reader")
	}
	total := 0
	for len(p) > 0 {
		if r.limit == 0 {
			if total > 0 {
				return total, nil
			}
			return 0, io.EOF
		}
		if r.zeroLeft > 0 {
			n := len(p)
			if int64(n) > r.zeroLeft {
				n = int(r.zeroLeft)
			}
			if r.limit >= 0 && int64(n) > r.limit {
				n = int(r.limit)
			}
			clear(p[:n])
			r.zeroLeft -= int64(n)
			r.off += int64(n)
			if n > 0 {
				r.refreshOffset = -1
				r.refreshCount = 0
			}
			if r.limit >= 0 {
				r.limit -= int64(n)
			}
			p = p[n:]
			total += n
			continue
		}
		if r.cur != nil {
			n, err := r.cur.Read(p)
			r.off += int64(n)
			if n > 0 {
				r.refreshOffset = -1
				r.refreshCount = 0
			}
			if r.limit >= 0 {
				r.limit -= int64(n)
			}
			p = p[n:]
			total += n
			if err == io.EOF {
				_ = r.cur.Close()
				r.cur = nil
				continue
			}
			if err != nil {
				return total, err
			}
			if n > 0 {
				return total, nil
			}
			continue
		}
		if r.bi < len(r.plan.Blocks) {
			blk := r.plan.Blocks[r.bi]
			r.bi++
			if blk.Zero {
				r.zeroLeft = blk.Length
				continue
			}
			if blk.URL == "" {
				return total, fmt.Errorf("extent plan block at %d has no URL", blk.FileOffset)
			}
			if r.limit >= 0 && blk.Length > r.limit {
				// Fetch only the bytes the caller asked for.
				blk.Length = r.limit
			}
			// A block may start mid-object (a plan window starts at the
			// requested offset): narrow the presigned GET to the slice.
			rc, err := r.c.FetchExtentBlockRange(r.ctx, blk.URL, blk.BlockOffset, blk.Length)
			if IsPresignExpired(err) {
				if r.refreshOffset != blk.FileOffset {
					r.refreshOffset = blk.FileOffset
					r.refreshCount = 0
				}
				if r.refreshCount < 1 {
					r.refreshCount++
					end := r.rangeEnd
					if r.limit >= 0 {
						if lim := blk.FileOffset + r.limit - 1; end < 0 || lim < end {
							end = lim
						}
					}
					if fresh, perr := r.c.GetExtentReadPlan(r.ctx, r.path, blk.FileOffset, end); perr == nil && len(fresh.Blocks) > 0 {
						if r.plan.Revision > 0 && fresh.Revision > 0 && fresh.Revision != r.plan.Revision {
							return total, fmt.Errorf("extent file changed during read: revision %d -> %d", r.plan.Revision, fresh.Revision)
						}
						r.plan = fresh
						r.bi = 0
						continue
					}
				}
			}
			if err != nil {
				return total, err
			}
			r.cur = rc
			continue
		}
		if r.plan.NextOffset > 0 && r.plan.NextOffset < r.plan.Size {
			prevNext := r.plan.NextOffset
			end := r.rangeEnd
			if r.limit >= 0 {
				if lim := prevNext + r.limit - 1; end < 0 || lim < end {
					end = lim
				}
			}
			plan, err := r.c.GetExtentReadPlan(r.ctx, r.path, prevNext, end)
			if err != nil {
				return total, err
			}
			if r.plan.Revision > 0 && plan.Revision > 0 && plan.Revision != r.plan.Revision {
				return total, fmt.Errorf("extent file changed during read: revision %d -> %d", r.plan.Revision, plan.Revision)
			}
			if (plan.NextOffset > 0 && plan.NextOffset <= prevNext) || (len(plan.Blocks) == 0 && plan.NextOffset <= 0 && prevNext < plan.Size) {
				return total, fmt.Errorf("extent read plan continuation did not advance at offset %d", prevNext)
			}
			r.plan = plan
			r.bi = 0
			continue
		}
		if total > 0 {
			return total, nil
		}
		return 0, io.EOF
	}
	return total, nil
}

func (r *extentPlanReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.cur != nil {
		_ = r.cur.Close()
		r.cur = nil
	}
	return nil
}

// serveExtentPlan decodes a read-plan response body (already confirmed to
// carry the extent content-layout header) and serves it sequentially.
// limit bounds the bytes served (ranged reads); end caps continuation
// fetches to the caller's window. Pass limit < 0 / end < 0 for unbounded.
func (c *Client) serveExtentPlan(ctx context.Context, path string, resp *http.Response, limit, end int64) (io.ReadCloser, error) {
	defer func() { _ = resp.Body.Close() }()
	var plan ExtentReadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode extent read plan: %w", err)
	}
	if plan.Revision <= 0 {
		if revHeader := resp.Header.Get("X-Dat9-Revision"); revHeader != "" {
			if parsed, err := strconv.ParseInt(revHeader, 10, 64); err == nil && parsed > 0 {
				plan.Revision = parsed
			}
		}
	}
	return &extentPlanReader{ctx: ctx, c: c, path: path, plan: &plan, limit: limit, rangeEnd: end, refreshOffset: -1}, nil
}

// FetchExtentBlockRange fetches [offset, offset+length) of the object behind
// a read-plan block URL by narrowing the presigned GET with a Range header.
func (c *Client) FetchExtentBlockRange(ctx context.Context, blockURL string, offset, length int64) (io.ReadCloser, error) {
	if blockURL == "" {
		return nil, fmt.Errorf("extent block URL is required")
	}
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid extent block range %d+%d", offset, length)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		return resp.Body, nil
	case http.StatusOK:
		// The object server ignored Range and returned the whole object:
		// skip to the requested offset and cap at the requested length.
		if _, err := io.CopyN(io.Discard, resp.Body, offset); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("extent block range GET skip: %w", err)
		}
		return &cappedBody{rc: resp.Body, remaining: length}, nil
	default:
		body := readPresignedErrorBody(resp)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("extent block range GET: HTTP %d: %s (%w)", resp.StatusCode, string(body), errPresignExpired)
		}
		return nil, fmt.Errorf("extent block range GET: HTTP %d: %s", resp.StatusCode, string(body))
	}
}

// cappedBody limits a raw 200 response body to the requested slice length.
type cappedBody struct {
	rc        io.ReadCloser
	remaining int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.rc.Read(p)
	b.remaining -= int64(n)
	if err == io.EOF && b.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func (b *cappedBody) Close() error { return b.rc.Close() }

// InitiateExtentPatch declares the dirty blocks of an extent patch and
// returns the server's upload plan (presigned block PUT URLs, or Exists for
// content the object store already holds).
func (c *Client) InitiateExtentPatch(ctx context.Context, path string, newSize int64, dirty []ExtentDirtyBlock, expectedRevision int64) (*ExtentWritePlan, error) {
	return c.InitiateExtentPatchExpectedResource(ctx, path, newSize, dirty, expectedRevision, "")
}

// InitiateExtentPatchExpectedResource additionally binds the patch to the
// object's resource identity: the server (when it understands the header)
// rejects the publication when the path's current occupant is not that
// object, so a replacement whose revision merely happens to equal the
// saved one cannot receive the staged bytes. An empty ID disables the
// binding.
func (c *Client) InitiateExtentPatchExpectedResource(ctx context.Context, path string, newSize int64, dirty []ExtentDirtyBlock, expectedRevision int64, expectedResourceID string) (*ExtentWritePlan, error) {
	if err := validateFSPath(path); err != nil {
		return nil, err
	}
	body, err := json.Marshal(struct {
		NewSize          int64              `json:"new_size"`
		DirtyBlocks      []ExtentDirtyBlock `json:"dirty_blocks"`
		ExpectedRevision int64              `json:"expected_revision"`
	}{NewSize: newSize, DirtyBlocks: dirty, ExpectedRevision: expectedRevision})
	if err != nil {
		return nil, err
	}
	req, err := c.newFSRequest(ctx, http.MethodPatch, path, "", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if expectedResourceID == "" {
		expectedResourceID = expectedResourceOf(ctx)
	}
	if expectedResourceID != "" {
		req.Header.Set("X-Dat9-Expected-Resource", expectedResourceID)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusPreconditionFailed && expectedResourceID != "" {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%s: %w", path, ErrResourceMismatch)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		return nil, readError(resp)
	}
	var plan ExtentWritePlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode extent write plan: %w", err)
	}
	return &plan, nil
}

// ExtentBlockWrite couples one dirty block's declaration with its bytes.
type ExtentBlockWrite struct {
	Chunk uint32
	Off   uint32
	Data  []byte
}

// ChunkOffset derives the (chunk, off) pair of a file offset.
func ChunkOffset(fileOffset int64) (chunk uint32, off uint32, ok bool) {
	if fileOffset < 0 {
		return 0, 0, false
	}
	return uint32(fileOffset / ExtentChunkSize), uint32(fileOffset % ExtentChunkSize), true
}

// CommitExtentPatch uploads dirty blocks and commits the patch session in
// one call: PATCH → PUT each non-existing block → complete with explicit
// part records. expectedRevision must match the server's current revision.
// CommitExtentPatchExpectedResource uploads dirty blocks and commits the
// patch session, bound to expectedResourceID when non-empty.
func (c *Client) CommitExtentPatchExpectedResource(ctx context.Context, path string, newSize int64, blocks []ExtentBlockWrite, expectedRevision int64, expectedResourceID string) error {
	return c.commitExtentPatch(ctx, path, newSize, blocks, expectedRevision, expectedResourceID)
}

func (c *Client) CommitExtentPatch(ctx context.Context, path string, newSize int64, blocks []ExtentBlockWrite, expectedRevision int64) error {
	return c.commitExtentPatch(ctx, path, newSize, blocks, expectedRevision, "")
}

func (c *Client) commitExtentPatch(ctx context.Context, path string, newSize int64, blocks []ExtentBlockWrite, expectedRevision int64, expectedResourceID string) error {
	if newSize < 0 {
		return fmt.Errorf("extent patch requires non-negative new size")
	}
	dirty := make([]ExtentDirtyBlock, len(blocks))
	for i, b := range blocks {
		if len(b.Data) == 0 {
			return fmt.Errorf("dirty block %d is empty", i)
		}
		if int64(len(b.Data)) > ExtentChunkSize {
			return fmt.Errorf("dirty block %d exceeds the chunk span", i)
		}
		sum := sha256.Sum256(b.Data)
		dirty[i] = ExtentDirtyBlock{Chunk: b.Chunk, Off: b.Off, Size: int64(len(b.Data)), Checksum: hex.EncodeToString(sum[:])}
	}
	plan, err := c.InitiateExtentPatchExpectedResource(ctx, path, newSize, dirty, expectedRevision, expectedResourceID)
	if err != nil {
		return err
	}
	// Block objects are create-once: a PUT that finds the address already
	// taken (an earlier interrupted attempt or a concurrent writer of the
	// same bytes) is not a failure. Re-plan once — the server HEADs
	// create-once stores and marks present blocks exists — and PUT only
	// what is still missing.
	for attempt := 0; ; attempt++ {
		err := c.putExtentPlanBlocks(ctx, plan, dirty, blocks)
		if err == nil {
			break
		}
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		if attempt > 0 || !isExtentBlockConflict(err) {
			return err
		}
		if plan, err = c.InitiateExtentPatchExpectedResource(ctx, path, newSize, dirty, expectedRevision, expectedResourceID); err != nil {
			return err
		}
	}
	parts := make([]completePart, 0, len(blocks))
	for i, b := range blocks {
		parts = append(parts, completePart{
			Number: i + 1,
			ETag:   dirty[i].Checksum,
			Chunk:  b.Chunk,
			Off:    b.Off,
			Size:   int64(len(b.Data)),
		})
	}
	if err := c.completeUploadV2WithOptions(ctx, plan.UploadID, parts, nil, ""); err != nil {
		return err
	}
	return nil
}

// TruncateExtentConditional resizes an existing extent-layout file via a
// metadata-only PATCH (no block downloads or uploads), bound to the initial
// resource identity across bounded CAS retries.
func (c *Client) TruncateExtentConditional(ctx context.Context, path string, st *StatResult, newSize int64) (*StatResult, error) {
	if newSize < 0 {
		return nil, fmt.Errorf("extent truncate requires non-negative size")
	}
	var err error
	ctxResID := expectedResourceOf(ctx)
	if st == nil {
		st, err = c.StatCtx(ctx, path)
		if err != nil {
			return nil, err
		}
		if ctxResID != "" && st.ResourceID != "" && st.ResourceID != ctxResID {
			return nil, fmt.Errorf("%s: %w", path, ErrResourceMismatch)
		}
	} else if ctxResID != "" && st.ResourceID == "" {
		st.ResourceID = ctxResID
	} else if ctxResID != "" && st.ResourceID != ctxResID {
		return nil, fmt.Errorf("%s: %w", path, ErrResourceMismatch)
	}
	if st.ContentLayout != "" && st.ContentLayout != ContentLayoutExtent {
		return nil, &StatusError{StatusCode: http.StatusConflict, Message: "content layout mismatch: the file's layout is sticky for the life of the inode"}
	}
	if st.Size == newSize {
		return st, nil
	}
	callerSink := completedRevisionSinkOf(ctx)
	origResID := st.ResourceID
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 || st.Revision <= 0 {
			st, err = c.StatCtx(ctx, path)
			if err != nil {
				return nil, err
			}
			if origResID != "" && st.ResourceID != origResID {
				return nil, fmt.Errorf("%s: %w", path, ErrResourceMismatch)
			}
			if st.ContentLayout != "" && st.ContentLayout != ContentLayoutExtent {
				return nil, &StatusError{StatusCode: http.StatusConflict, Message: "content layout mismatch: the file's layout is sticky for the life of the inode"}
			}
			if st.Size == newSize {
				return st, nil
			}
		}
		if st.Revision <= 0 {
			return nil, fmt.Errorf("%s: missing revision for extent truncate", path)
		}
		var landedRev int64
		pctx := WithCompletedRevisionSink(ctx, &landedRev)
		err = c.CommitExtentPatchExpectedResource(pctx, path, newSize, nil, st.Revision, st.ResourceID)
		if err == nil {
			updated := *st
			updated.Size = newSize
			updated.ContentLayout = ContentLayoutExtent
			if landedRev > 0 {
				updated.Revision = landedRev
				if callerSink != nil {
					*callerSink = landedRev
				}
			} else if fresh, serr := c.StatCtx(ctx, path); serr == nil && fresh != nil {
				updated = *fresh
			}
			return &updated, nil
		}
		if !errors.Is(err, ErrConflict) {
			return nil, err
		}
	}
	return nil, err
}

// putExtentPlanBlocks PUTs every distinct block the plan does not already
// hold, in bounded parallel, with the headers the plan prescribes.
func (c *Client) putExtentPlanBlocks(ctx context.Context, plan *ExtentWritePlan, dirty []ExtentDirtyBlock, blocks []ExtentBlockWrite) error {
	type putJob struct {
		url     string
		headers map[string]string
		data    []byte
	}
	seenChecksum := make(map[string]struct{}, len(blocks))
	puts := make([]putJob, 0, len(blocks))
	for i, b := range blocks {
		wb := extentPlanBlockFor(plan, dirty[i])
		if wb == nil {
			return fmt.Errorf("extent patch plan lacks block %d/%d", dirty[i].Chunk, dirty[i].Off)
		}
		if wb.Exists || wb.URL == "" {
			continue
		}
		if _, dup := seenChecksum[dirty[i].Checksum]; dup {
			continue
		}
		seenChecksum[dirty[i].Checksum] = struct{}{}
		puts = append(puts, putJob{url: wb.URL, headers: wb.Headers, data: b.Data})
	}
	if len(puts) == 0 {
		return nil
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(extentPatchPutConcurrency)
	for _, job := range puts {
		g.Go(func() error {
			return c.putExtentBlock(gctx, job.url, job.headers, job.data)
		})
	}
	return g.Wait()
}

func extentPlanBlockFor(plan *ExtentWritePlan, d ExtentDirtyBlock) *ExtentWritePlanBlock {
	for i := range plan.Blocks {
		if plan.Blocks[i].Chunk == d.Chunk && plan.Blocks[i].Off == d.Off {
			return &plan.Blocks[i]
		}
	}
	return nil
}

// putExtentBlock PUTs one whole block object to its presigned URL with the
// headers the server prescribed for it: they are part of the signature (the
// S3 checksum in its base64 wire form, GCS's if-generation-match: 0) or
// required by the store (Azure's x-ms-blob-type), so the client sends
// exactly these and nothing else that could be signed. A server that
// prescribes none signs only the S3 flexible checksum, which the client can
// derive from the bytes.
func (c *Client) putExtentBlock(ctx context.Context, url string, headers map[string]string, data []byte) error {
	putCtx, cancel := context.WithTimeout(ctx, extentBlockPutTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(putCtx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if len(headers) == 0 {
		sum := sha256.Sum256(data)
		req.Header.Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
	}
	for k, v := range headers {
		if isForbiddenPresignedHeader(k) {
			continue
		}
		req.Header.Set(k, v)
	}
	req.ContentLength = int64(len(data))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body := readPresignedErrorBody(resp)
		return fmt.Errorf("extent block PUT: %w", &presignedPutStatusError{Status: resp.StatusCode, Body: string(body)})
	}
	return nil
}

// isExtentBlockConflict reports whether a block PUT failed because the
// create-once address is already taken rather than because the upload is
// broken: GCS answers the signed if-generation-match: 0 precondition with
// 412, Azure answers a Create-only SAS against an existing blob with 403
// (AuthorizationPermissionMismatch), and an S3-compatible store may answer
// 409. A 403 is also how an expired URL fails, which is why a conflict is
// resolved by re-presigning rather than by guessing: the server HEADs
// create-once stores and answers skip_upload / exists for a present object,
// and hands back a fresh URL otherwise.
func isExtentBlockConflict(err error) bool {
	if errors.Is(err, errPresignExpired) {
		return true
	}
	var se *presignedPutStatusError
	if errors.As(err, &se) {
		switch se.Status {
		case http.StatusForbidden, http.StatusConflict, http.StatusPreconditionFailed:
			return true
		}
	}
	return false
}

// errExtentPresignUnsupported reports that an extent session could not mint
// length-bounded block PUT URLs on the backing store (e.g. Azure Blob), so
// automatic callers may abort and fall back to classic V2 multipart.
var errExtentPresignUnsupported = errors.New("extent block presign unsupported on store")

// isExtentUploadPlan reports whether a V2 initiate response describes an
// extent sequential session rather than a classic multipart session. Real
// extent plans omit Key and require SHA-256 part checksums; classic servers
// (and classic test fakes) either populate Key or leave Required false.
func isExtentUploadPlan(plan *uploadPlanV2) bool {
	return plan != nil && plan.Key == "" && plan.ChecksumContract.Required
}

func isExtentLayoutMismatchError(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusConflict {
		return false
	}
	if se.Code == extentErrCodeLayoutMismatch {
		return true
	}
	return strings.Contains(strings.ToLower(se.Message), extentErrMsgLayoutMismatch)
}

func isExtentPresignUnsupportedError(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) {
		return false
	}
	if se.StatusCode == http.StatusInternalServerError {
		return isOpaqueInternalStorageMessage(se.Message)
	}
	if se.StatusCode != http.StatusBadRequest && se.StatusCode != http.StatusNotImplemented {
		return false
	}
	if se.Code == extentErrCodePresignLengthUnsupported {
		return true
	}
	return strings.Contains(strings.ToLower(se.Message), extentErrMsgPresignLengthUnsupported)
}

// mayBeExtentDirectWriteRejection reports whether a direct PUT failure may
// have been caused by an existing extent-layout occupant at path. Create-only
// requests (expectedRevision == 0) fail revision CAS with 409 before the
// layout guard runs, so a 500 there is never a layout mismatch.
func mayBeExtentDirectWriteRejection(err error, expectedRevision int64) bool {
	if isExtentLayoutMismatchError(err) {
		return true
	}
	if expectedRevision == 0 {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) && se.StatusCode == http.StatusInternalServerError {
		return isOpaqueInternalStorageMessage(se.Message)
	}
	return false
}

// WriteExtentStreamConditional uploads a whole file as an extent sequential
// session (layout=extent, 4 MiB parts). Use for fresh large files; existing
// extent files with small dirty sets should use CommitExtentPatch instead.
func (c *Client) WriteExtentStreamConditional(ctx context.Context, path string, ra io.ReaderAt, size int64, progress ProgressFunc, expectedRevision int64) error {
	if c.view.LayerID != "" || ra == nil || size <= 0 || expectedRevision < -1 {
		return errors.New("extent stream requires main, a source, positive size and valid expected revision")
	}
	plan, err := c.initiateUploadV2WithLayout(ctx, path, size, expectedRevision, "", "extent")
	if err != nil {
		return err
	}
	if plan.Key != "" || (plan.ContentLayout != "" && plan.ContentLayout != ContentLayoutExtent) {
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		return fmt.Errorf("server ignored layout=extent on %s", path)
	}
	if plan.PartSize <= 0 || plan.TotalParts <= 0 {
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		return fmt.Errorf("server returned invalid extent plan on %s", path)
	}
	if err := c.uploadExtentSequential(ctx, plan, ra, size, progress); err != nil {
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		return err
	}
	return nil
}

// uploadExtentSequential drives an extent sequential session: for each bounded
// batch of parts (32 * 4 MiB = 128 MiB max in-flight RAM), read each part once
// from ra, SHA-256 hash, presign with checksums (deduped parts return
// skip_upload), upload the non-deduped parts in parallel (16-way), and complete
// with the checksum etags.
func (c *Client) uploadExtentSequential(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt, size int64, progress ProgressFunc) error {
	return c.uploadExtentSequentialWithSummary(ctx, plan, ra, size, progress, nil, nil, nil)
}

func (c *Client) uploadExtentSequentialWithSummary(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt, size int64, progress ProgressFunc, summary *UploadSummary, tags map[string]string, preCompleteCheck func() error) error {
	const batchParts = 32
	if summary != nil {
		summary.Mode = "extent_v2"
		summary.PartSizeBytes = plan.PartSize
		summary.TotalParts = plan.TotalParts
		parallelism := extentPatchPutConcurrency
		if plan.TotalParts > 0 && parallelism > plan.TotalParts {
			parallelism = plan.TotalParts
		}
		if plan.TotalParts == 0 {
			parallelism = 1
		}
		summary.Parallelism = parallelism
	}

	parts := make([]completePart, 0, plan.TotalParts)
	var uploadedCount atomic.Int64
	var checksumDur, presignDur, uploadDur time.Duration
	var progMu sync.Mutex
	reportProgress := func(num int, bytes int64) {
		if progress == nil {
			return
		}
		progMu.Lock()
		progress(num, plan.TotalParts, bytes)
		progMu.Unlock()
	}

	for first := 1; first <= plan.TotalParts; first += batchParts {
		last := first + batchParts - 1
		if last > plan.TotalParts {
			last = plan.TotalParts
		}
		batch, hashElapsed, presignElapsed, err := c.presignExtentBatchLoadedTimed(ctx, plan, ra, size, first, last)
		checksumDur += hashElapsed
		presignDur += presignElapsed
		if err != nil {
			if first == 1 && isExtentPresignUnsupportedError(err) {
				return fmt.Errorf("%w: %w", errExtentPresignUnsupported, err)
			}
			return err
		}
		batchUploadStart := time.Now()
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(extentPatchPutConcurrency)
		for i := range batch {
			item := batch[i]
			batch[i].data = nil // drop slice header reference after worker finishes
			if item.skipUpload {
				reportProgress(item.number, 0)
				continue
			}
			g.Go(func() error {
				putCtx, cancel := context.WithTimeout(gctx, extentBlockPutTimeout)
				_, err := c.uploadOnePartV2(putCtx, item.part, item.data)
				cancel()
				skippedOnRetry := false
				if err != nil {
					if !isExtentBlockConflict(err) {
						return err
					}
					// Create-once address already taken or URL expired: re-presign
					// this single part with its already-computed checksum.
					fresh, perr := c.presignExtentChecksums(gctx, plan.UploadID, []extentPresignedBatchItem{{
						number:   item.number,
						checksum: item.checksum,
					}})
					if perr != nil {
						return perr
					}
					if fresh[0].skipUpload {
						skippedOnRetry = true
					} else {
						retryCtx, retryCancel := context.WithTimeout(gctx, extentBlockPutTimeout)
						_, rerr := c.uploadOnePartV2(retryCtx, fresh[0].part, item.data)
						retryCancel()
						if rerr != nil {
							return rerr
						}
					}
				}
				if skippedOnRetry {
					reportProgress(item.number, 0)
				} else {
					uploadedCount.Add(1)
					reportProgress(item.number, int64(len(item.data)))
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		uploadDur += time.Since(batchUploadStart)
		for _, p := range batch {
			parts = append(parts, completePart{Number: p.number, ETag: p.checksum})
		}
	}

	if summary != nil {
		summary.ChecksumSeconds = checksumDur.Seconds()
		summary.PresignSeconds = presignDur.Seconds()
		summary.UploadSeconds = uploadDur.Seconds()
		summary.UploadedParts = int(uploadedCount.Load())
	}
	if err := runPreCompleteCheck(ctx, preCompleteCheck); err != nil {
		return err
	}
	completeStart := time.Now()
	if err := c.completeUploadV2WithOptions(ctx, plan.UploadID, parts, tags, ""); err != nil {
		return err
	}
	if summary != nil {
		summary.CompleteSeconds = time.Since(completeStart).Seconds()
	}
	return nil
}

type extentPresignedBatchItem struct {
	part       presignedPart
	checksum   string
	data       []byte
	skipUpload bool
	number     int
}

// presignExtentBatchLoadedTimed reads parts [first,last] once from ra, hashes
// them concurrently, and requests their presigned URLs in one batch RPC.
func (c *Client) presignExtentBatchLoadedTimed(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt, size int64, first, last int) ([]extentPresignedBatchItem, time.Duration, time.Duration, error) {
	count := last - first + 1
	items := make([]extentPresignedBatchItem, count)
	for i := 0; i < count; i++ {
		n := first + i
		data, derr := extentPartBytes(ra, plan.PartSize, n, size)
		if derr != nil {
			return nil, 0, 0, derr
		}
		items[i] = extentPresignedBatchItem{number: n, data: data}
	}
	hashStart := time.Now()
	hg, _ := errgroup.WithContext(ctx)
	hg.SetLimit(extentPatchPutConcurrency)
	for i := range items {
		i := i
		hg.Go(func() error {
			sum := sha256.Sum256(items[i].data)
			items[i].checksum = hex.EncodeToString(sum[:])
			return nil
		})
	}
	_ = hg.Wait()
	hashDur := time.Since(hashStart)

	presignStart := time.Now()
	out, err := c.presignExtentChecksums(ctx, plan.UploadID, items)
	presignDur := time.Since(presignStart)
	return out, hashDur, presignDur, err
}

func (c *Client) presignExtentChecksums(ctx context.Context, uploadID string, items []extentPresignedBatchItem) ([]extentPresignedBatchItem, error) {
	type reqPart struct {
		Number   int             `json:"part_number"`
		Checksum presignChecksum `json:"checksum"`
	}
	body := struct {
		Parts []reqPart `json:"parts"`
	}{
		Parts: make([]reqPart, len(items)),
	}
	for i := range items {
		body.Parts[i] = reqPart{
			Number:   items[i].number,
			Checksum: presignChecksum{Algorithm: "sha256", Value: items[i].checksum},
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v2/uploads/"+uploadID+"/presign-batch", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Parts []presignedPart `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode presign-batch response: %w", err)
	}
	if len(out.Parts) != len(items) {
		return nil, fmt.Errorf("presign-batch returned %d parts, want %d", len(out.Parts), len(items))
	}
	for i := range items {
		items[i].part = out.Parts[i]
		items[i].skipUpload = out.Parts[i].SkipUpload
		if items[i].skipUpload {
			items[i].data = nil
		}
	}
	return items, nil
}

// presignChecksum is the algorithm-neutral checksum object the presign
// endpoints expect (backend.PresignChecksum on the wire).
type presignChecksum struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// extentPartBytes reads part n's bytes (1-based) from ra.
func extentPartBytes(ra io.ReaderAt, partSize int64, n int, total int64) ([]byte, error) {
	start := int64(n-1) * partSize
	if start >= total {
		return nil, fmt.Errorf("extent part %d beyond total %d", n, total)
	}
	length := partSize
	if rem := total - start; rem < length {
		length = rem
	}
	buf := make([]byte, length)
	got, err := ra.ReadAt(buf, start)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("extent part %d read: %w", n, err)
	}
	if int64(got) != length {
		return nil, fmt.Errorf("extent part %d short read: %d of %d", n, got, length)
	}
	return buf, nil
}

// writeExtentBytesConditional writes an in-memory payload (including 0 bytes)
// through the extent upload plane, returning the committed revision when the
// server echoes one. Used when the caller requests ContentLayoutExtent on a
// small file or overwrites an existing extent-layout file via direct write.
func (c *Client) writeExtentBytesConditional(ctx context.Context, path string, data []byte, expectedRevision int64, tags map[string]string, description string, summary *UploadSummary, preCompleteCheck func() error) (int64, error) {
	if err := validateTags(tags); err != nil {
		return 0, err
	}
	callerSink := completedRevisionSinkOf(ctx)
	var landedRev int64
	rctx := WithCompletedRevisionSink(ctx, &landedRev)
	size := int64(len(data))

	initiateStart := time.Now()
	plan, err := c.initiateUploadV2WithLayout(rctx, path, size, expectedRevision, description, string(ContentLayoutExtent))
	if err != nil {
		// Compatibility with servers whose V2 initiate rejects total_size <= 0.
		if size == 0 && isZeroSizeExtentInitiateError(err) {
			if (tags != nil || description != "") && (expectedRevision >= 0 || expectedResourceOf(rctx) != "") {
				return 0, fmt.Errorf("conditional 0-byte extent writes do not accept tags or description: %w", err)
			}
			rev, ferr := c.fallbackZeroSizeExtentWrite(rctx, path, expectedRevision, summary, preCompleteCheck)
			if ferr != nil {
				return 0, ferr
			}
			if tags != nil || description != "" {
				metaOpts := SetMetadataOptions{Tags: tags}
				if description != "" {
					desc := description
					metaOpts.Description = &desc
				}
				if merr := c.SetMetadataCtx(ctx, path, metaOpts); merr != nil {
					return 0, fmt.Errorf("apply metadata to %s (0-byte content written, tags/description not applied): %w", path, merr)
				}
			}
			if landedRev > 0 && callerSink != nil {
				*callerSink = landedRev
			}
			return rev, nil
		}
		return 0, err
	}
	if summary != nil {
		summary.InitiateSeconds = time.Since(initiateStart).Seconds()
	}
	if plan.Key != "" {
		c.abortUploadV2BestEffort(rctx, plan.UploadID)
		return 0, fmt.Errorf("server ignored layout=extent on %s", path)
	}
	if err := c.uploadExtentSequentialWithSummary(rctx, plan, bytes.NewReader(data), size, nil, summary, tags, preCompleteCheck); err != nil {
		c.abortUploadV2BestEffort(rctx, plan.UploadID)
		return 0, err
	}
	if landedRev > 0 && callerSink != nil {
		*callerSink = landedRev
	}
	return landedRev, nil
}

func isZeroSizeExtentInitiateError(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusBadRequest {
		return false
	}
	if se.Code == extentErrCodeZeroSizeUpload {
		return true
	}
	return strings.Contains(strings.ToLower(se.Message), extentErrMsgZeroSizeUpload)
}

func (c *Client) fallbackZeroSizeExtentWrite(ctx context.Context, path string, expectedRevision int64, summary *UploadSummary, preCompleteCheck func() error) (int64, error) {
	if err := runPreCompleteCheck(ctx, preCompleteCheck); err != nil {
		return 0, err
	}
	setZeroSummary := func() {
		if summary != nil {
			summary.Mode = "extent_v2"
			summary.TotalParts = 0
			summary.UploadedParts = 0
			summary.Parallelism = 1
		}
	}
	if expectedRevision == 0 {
		if expectedResourceOf(ctx) != "" {
			return 0, fmt.Errorf("%s: %w", path, ErrResourceMismatch)
		}
		rev, err := c.createNativeExtentFileCtx(ctx, path)
		if err != nil {
			return 0, err
		}
		if rev > 0 {
			if out := completedRevisionSinkOf(ctx); out != nil {
				*out = rev
			}
		}
		setZeroSummary()
		return rev, nil
	}
	if expectedRevision > 0 {
		var landedRev int64
		pctx := WithCompletedRevisionSink(ctx, &landedRev)
		if err := c.CommitExtentPatchExpectedResource(pctx, path, 0, nil, expectedRevision, expectedResourceOf(ctx)); err != nil {
			return 0, err
		}
		if landedRev > 0 {
			if out := completedRevisionSinkOf(ctx); out != nil {
				*out = landedRev
			}
		}
		setZeroSummary()
		return landedRev, nil
	}
	updated, err := c.TruncateExtentConditional(ctx, path, nil, 0)
	if err != nil {
		if IsNotFound(err) {
			if expectedResourceOf(ctx) != "" {
				return 0, fmt.Errorf("%s: %w", path, ErrResourceMismatch)
			}
			rev, cerr := c.createNativeExtentFileCtx(ctx, path)
			if cerr != nil {
				return 0, cerr
			}
			if rev > 0 {
				if out := completedRevisionSinkOf(ctx); out != nil {
					*out = rev
				}
			}
			setZeroSummary()
			return rev, nil
		}
		return 0, err
	}
	if updated.ContentLayout != "" && updated.ContentLayout != ContentLayoutExtent {
		return 0, &StatusError{StatusCode: http.StatusConflict, Message: "content layout mismatch: the file's layout is sticky for the life of the inode"}
	}
	setZeroSummary()
	return updated.Revision, nil
}

// createNativeExtentFileCtx issues POST ?create=1&layout=extent without falling
// back to classic ?create=1, cleaning up best-effort if a legacy server ignored
// layout=extent and created a single-layout placeholder.
func (c *Client) createNativeExtentFileCtx(ctx context.Context, path string) (int64, error) {
	req, err := c.newFSRequest(ctx, http.MethodPost, path, "?create=1&layout=extent", nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return 0, readError(resp)
	}
	var result struct {
		Revision      int64  `json:"revision"`
		ResourceID    string `json:"resource_id"`
		ContentLayout string `json:"content_layout"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("decode create extent file response: %w", err)
	}
	if ContentLayout(result.ContentLayout) != ContentLayoutExtent {
		if result.Revision > 0 || result.ResourceID != "" {
			_ = c.DeleteFileConditionalCtx(ctx, path, result.Revision, result.ResourceID)
		}
		return 0, fmt.Errorf("server does not support 0-byte extent file creation on %s", path)
	}
	return result.Revision, nil
}

type extentAppendSlice struct {
	streamOff int64
	chunk     uint32
	off       uint32
	size      int64
	checksum  string
}

// buildExtentAppendSlices tiles [baseSize, baseSize+appendSize) into dirty
// blocks that never cross a 64 MiB chunk boundary and align interior blocks on
// the 4 MiB block grid.
func buildExtentAppendSlices(baseSize, appendSize int64) ([]extentAppendSlice, error) {
	finalSize := baseSize + appendSize
	var slices []extentAppendSlice
	for fileOff := baseSize; fileOff < finalSize; {
		chunkEnd := (fileOff/ExtentChunkSize + 1) * ExtentChunkSize
		blkEnd := finalSize
		if finalSize-fileOff >= ExtentBlockSize {
			gridEnd := (fileOff/ExtentBlockSize + 1) * ExtentBlockSize
			if gridEnd < blkEnd {
				blkEnd = gridEnd
			}
		}
		if blkEnd > chunkEnd {
			blkEnd = chunkEnd
		}
		chunk, off, ok := ChunkOffset(fileOff)
		if !ok {
			return nil, fmt.Errorf("invalid append offset %d", fileOff)
		}
		slices = append(slices, extentAppendSlice{
			streamOff: fileOff - baseSize,
			chunk:     chunk,
			off:       off,
			size:      blkEnd - fileOff,
		})
		fileOff = blkEnd
	}
	return slices, nil
}

// appendExtentStream appends appendSize bytes from r onto an existing
// extent-layout file via an atomic PATCH session without reading existing file
// bytes. Memory stays bounded regardless of appendSize.
func (c *Client) appendExtentStream(ctx context.Context, path string, st *StatResult, r io.Reader, appendSize int64, progress ProgressFunc) error {
	const inMemoryAppendCap = 16 << 20
	var ra io.ReaderAt
	var advanceSeeker func()
	if readerAt, ok := r.(io.ReaderAt); ok {
		if seeker, sok := r.(io.Seeker); sok {
			if origin, serr := seeker.Seek(0, io.SeekCurrent); serr == nil && origin >= 0 {
				ra = io.NewSectionReader(readerAt, origin, appendSize)
				advanceSeeker = func() {
					_, _ = seeker.Seek(origin+appendSize, io.SeekStart)
				}
			}
		}
	}
	if ra == nil {
		if appendSize <= inMemoryAppendCap {
			buf := make([]byte, appendSize)
			if _, err := io.ReadFull(r, buf); err != nil {
				return fmt.Errorf("read append bytes: %w", err)
			}
			ra = bytes.NewReader(buf)
		} else {
			tmp, err := os.CreateTemp("", "drive9-extent-append-*")
			if err != nil {
				return fmt.Errorf("create append temp file: %w", err)
			}
			defer func() {
				_ = tmp.Close()
				_ = os.Remove(tmp.Name())
			}()
			written, err := io.Copy(tmp, io.LimitReader(r, appendSize))
			if err != nil {
				return fmt.Errorf("spool append data: %w", err)
			}
			if written != appendSize {
				return fmt.Errorf("spool append data: got %d bytes, want %d", written, appendSize)
			}
			ra = tmp
		}
	}

	slices, err := buildExtentAppendSlices(st.Size, appendSize)
	if err != nil {
		return err
	}

	// Hash all dirty blocks with bounded buffer memory before initiating PATCH.
	pool := newUploadBufferPool(ExtentBlockSize, min(extentPatchPutConcurrency, max(1, len(slices))))
	hg, hctx := errgroup.WithContext(ctx)
	hg.SetLimit(extentPatchPutConcurrency)
	for i := range slices {
		i := i
		hg.Go(func() error {
			buf, berr := pool.get(hctx)
			if berr != nil {
				return berr
			}
			defer pool.put(buf)
			data := buf[:slices[i].size]
			got, rerr := ra.ReadAt(data, slices[i].streamOff)
			if rerr != nil && rerr != io.EOF {
				return fmt.Errorf("read append block %d: %w", i+1, rerr)
			}
			if int64(got) != slices[i].size {
				return fmt.Errorf("short read for append block %d: got %d want %d", i+1, got, slices[i].size)
			}
			sum := sha256.Sum256(data)
			slices[i].checksum = hex.EncodeToString(sum[:])
			return nil
		})
	}
	if err := hg.Wait(); err != nil {
		return err
	}

	dirty := make([]ExtentDirtyBlock, len(slices))
	for i, s := range slices {
		dirty[i] = ExtentDirtyBlock{Chunk: s.chunk, Off: s.off, Size: s.size, Checksum: s.checksum}
	}
	finalSize := st.Size + appendSize
	plan, err := c.InitiateExtentPatchExpectedResource(ctx, path, finalSize, dirty, st.Revision, st.ResourceID)
	if err != nil {
		return err
	}

	var progMu sync.Mutex
	reportProgress := func(num int, bytes int64) {
		if progress == nil {
			return
		}
		progMu.Lock()
		progress(num, len(slices), bytes)
		progMu.Unlock()
	}

	for attempt := 0; ; attempt++ {
		err := c.putExtentAppendSlices(ctx, plan, slices, ra, pool, reportProgress)
		if err == nil {
			break
		}
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		if attempt > 0 || !isExtentBlockConflict(err) {
			return err
		}
		if plan, err = c.InitiateExtentPatchExpectedResource(ctx, path, finalSize, dirty, st.Revision, st.ResourceID); err != nil {
			return err
		}
	}

	parts := make([]completePart, len(slices))
	for i, s := range slices {
		parts[i] = completePart{
			Number: i + 1,
			ETag:   s.checksum,
			Chunk:  s.chunk,
			Off:    s.off,
			Size:   s.size,
		}
	}
	if err := c.completeUploadV2WithOptions(ctx, plan.UploadID, parts, nil, ""); err != nil {
		return err
	}
	if advanceSeeker != nil {
		advanceSeeker()
	}
	return nil
}

func (c *Client) putExtentAppendSlices(ctx context.Context, plan *ExtentWritePlan, slices []extentAppendSlice, ra io.ReaderAt, pool *uploadBufferPool, reportProgress func(int, int64)) error {
	type putItem struct {
		num     int
		slice   extentAppendSlice
		url     string
		headers map[string]string
	}
	seenChecksum := make(map[string]struct{}, len(slices))
	var jobs []putItem
	for i, s := range slices {
		d := ExtentDirtyBlock{Chunk: s.chunk, Off: s.off, Size: s.size, Checksum: s.checksum}
		wb := extentPlanBlockFor(plan, d)
		if wb == nil {
			return fmt.Errorf("extent patch plan lacks block %d/%d", s.chunk, s.off)
		}
		if wb.Exists || wb.URL == "" {
			reportProgress(i+1, 0)
			continue
		}
		if _, dup := seenChecksum[s.checksum]; dup {
			reportProgress(i+1, 0)
			continue
		}
		seenChecksum[s.checksum] = struct{}{}
		jobs = append(jobs, putItem{num: i + 1, slice: s, url: wb.URL, headers: wb.Headers})
	}
	if len(jobs) == 0 {
		return nil
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(extentPatchPutConcurrency)
	for _, job := range jobs {
		job := job
		g.Go(func() error {
			buf, berr := pool.get(gctx)
			if berr != nil {
				return berr
			}
			defer pool.put(buf)
			data := buf[:job.slice.size]
			got, rerr := ra.ReadAt(data, job.slice.streamOff)
			if rerr != nil && rerr != io.EOF {
				return fmt.Errorf("read append block %d: %w", job.num, rerr)
			}
			if int64(got) != job.slice.size {
				return fmt.Errorf("short read for append block %d: got %d want %d", job.num, got, job.slice.size)
			}
			if err := c.putExtentBlock(gctx, job.url, job.headers, data); err != nil {
				return err
			}
			reportProgress(job.num, job.slice.size)
			return nil
		})
	}
	return g.Wait()
}

// downloadExtentToFileParallel downloads an extent-layout file using the
// already-fetched initial read plan (plus any NextOffset continuations),
// preallocating sparse zero runs via Truncate and writing data blocks in
// parallel via WriteAt.
func (c *Client) downloadExtentToFileParallel(ctx context.Context, remotePath, localPath string, out *os.File, firstPlan *ExtentReadPlan) (*DownloadSummary, error) {
	startedAt := time.Now()
	var dataBlocks []ExtentReadPlanBlock
	cur := firstPlan
	for {
		for _, blk := range cur.Blocks {
			if blk.Zero || blk.Length <= 0 {
				continue
			}
			dataBlocks = append(dataBlocks, blk)
		}
		if cur.NextOffset <= 0 || cur.NextOffset >= cur.Size {
			break
		}
		prevNext := cur.NextOffset
		next, err := c.GetExtentReadPlan(ctx, remotePath, prevNext, -1)
		if err != nil {
			return nil, err
		}
		if firstPlan.Revision > 0 && next.Revision > 0 && next.Revision != firstPlan.Revision {
			return nil, fmt.Errorf("extent file changed during read: revision %d -> %d", firstPlan.Revision, next.Revision)
		}
		if (next.NextOffset > 0 && next.NextOffset <= prevNext) || (len(next.Blocks) == 0 && next.NextOffset <= 0 && prevNext < next.Size) {
			return nil, fmt.Errorf("extent read plan continuation did not advance at offset %d", prevNext)
		}
		cur = next
	}

	if err := out.Truncate(firstPlan.Size); err != nil {
		return nil, fmt.Errorf("preallocate %s: %w", localPath, err)
	}
	concurrency := 1
	if len(dataBlocks) > 0 {
		concurrency = min(downloadMaxConcurrency, len(dataBlocks))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)
		for _, blk := range dataBlocks {
			blk := blk
			g.Go(func() error {
				if blk.URL == "" {
					return fmt.Errorf("extent plan block at %d has no URL", blk.FileOffset)
				}
				rc, err := c.FetchExtentBlockRange(gctx, blk.URL, blk.BlockOffset, blk.Length)
				if IsPresignExpired(err) {
					fresh, perr := c.GetExtentReadPlan(gctx, remotePath, blk.FileOffset, blk.FileOffset+blk.Length-1)
					if perr != nil {
						return fmt.Errorf("refresh extent read plan for %s: %w", remotePath, perr)
					}
					if firstPlan.Revision > 0 && fresh.Revision > 0 && fresh.Revision != firstPlan.Revision {
						return fmt.Errorf("extent file changed during read: revision %d -> %d", firstPlan.Revision, fresh.Revision)
					}
					if len(fresh.Blocks) == 0 || fresh.Blocks[0].Zero || fresh.Blocks[0].URL == "" {
						return fmt.Errorf("refreshed extent read plan for %s missing data block at %d", remotePath, blk.FileOffset)
					}
					fb := fresh.Blocks[0]
					rc, err = c.FetchExtentBlockRange(gctx, fb.URL, fb.BlockOffset, blk.Length)
				}
				if err != nil {
					return err
				}
				data, readErr := io.ReadAll(rc)
				closeErr := rc.Close()
				if readErr != nil {
					return fmt.Errorf("read extent block offset=%d length=%d: %w", blk.FileOffset, blk.Length, readErr)
				}
				if closeErr != nil {
					return fmt.Errorf("close extent block offset=%d length=%d: %w", blk.FileOffset, blk.Length, closeErr)
				}
				if int64(len(data)) != blk.Length {
					return fmt.Errorf("short extent block read at offset=%d: got %d bytes, want %d", blk.FileOffset, len(data), blk.Length)
				}
				if _, err := out.WriteAt(data, blk.FileOffset); err != nil {
					return fmt.Errorf("write extent block offset=%d length=%d: %w", blk.FileOffset, blk.Length, err)
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}
	if err := out.Sync(); err != nil {
		return nil, fmt.Errorf("sync %s: %w", localPath, err)
	}
	finishedAt := time.Now()
	return &DownloadSummary{
		Type:           "download_summary",
		Mode:           "parallel_extent_blocks",
		Concurrency:    concurrency,
		ChunkSizeBytes: ExtentBlockSize,
		RangeCount:     len(dataBlocks),
		StartedAt:      startedAt,
		FinishedAt:     finishedAt,
		ElapsedSeconds: finishedAt.Sub(startedAt).Seconds(),
		RemotePath:     remotePath,
		LocalPath:      localPath,
	}, nil
}
