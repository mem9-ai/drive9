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
	"sync"
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
	resp, err := c.readWithoutRedirect(ctx, path, rng)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
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
	limit    int64
	rangeEnd int64
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
			if err != nil {
				return total, err
			}
			r.cur = rc
			continue
		}
		if r.plan.NextOffset > 0 && r.plan.NextOffset < r.plan.Size {
			end := r.rangeEnd
			if r.limit >= 0 {
				if lim := r.plan.NextOffset + r.limit - 1; end < 0 || lim < end {
					end = lim
				}
			}
			plan, err := r.c.GetExtentReadPlan(r.ctx, r.path, r.plan.NextOffset, end)
			if err != nil {
				return total, err
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
	return &extentPlanReader{ctx: ctx, c: c, path: path, plan: &plan, limit: limit, rangeEnd: end}, nil
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
	resp, err := c.do(req)
	if err != nil {
		return nil, err
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
func (c *Client) CommitExtentPatch(ctx context.Context, path string, newSize int64, blocks []ExtentBlockWrite, expectedRevision int64) error {
	if len(blocks) == 0 && newSize != 0 {
		return fmt.Errorf("extent patch requires at least one dirty block")
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
	plan, err := c.InitiateExtentPatch(ctx, path, newSize, dirty, expectedRevision)
	if err != nil {
		return err
	}
	// Block objects are create-once: a PUT that finds the address already
	// taken (a repeated block, an earlier interrupted attempt, a concurrent
	// writer of the same bytes) is not a failure. Re-plan once — the server
	// HEADs create-once stores and marks present blocks exists — and PUT only
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
		if plan, err = c.InitiateExtentPatch(ctx, path, newSize, dirty, expectedRevision); err != nil {
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

// putExtentPlanBlocks PUTs every block the plan does not already hold, in
// plan order, with the headers the plan prescribes for each URL.
func (c *Client) putExtentPlanBlocks(ctx context.Context, plan *ExtentWritePlan, dirty []ExtentDirtyBlock, blocks []ExtentBlockWrite) error {
	for i, b := range blocks {
		wb := extentPlanBlockFor(plan, dirty[i])
		if wb == nil {
			return fmt.Errorf("extent patch plan lacks block %d/%d", dirty[i].Chunk, dirty[i].Off)
		}
		if wb.Exists || wb.URL == "" {
			continue
		}
		if err := c.putExtentBlock(ctx, wb.URL, wb.Headers, b.Data); err != nil {
			return err
		}
		wb.Exists = true
	}
	return nil
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
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

// WriteExtentStreamConditional uploads a whole file as an extent sequential
// session (layout=extent, 4 MiB parts). Against a server that predates the
// layout field the session silently degrades to a classic multipart upload —
// the two flows are client-identical apart from the initiate body. Use for
// fresh large files; existing extent files with small dirty sets should use
// CommitExtentPatch instead.
func (c *Client) WriteExtentStreamConditional(ctx context.Context, path string, ra io.ReaderAt, size int64, progress ProgressFunc, expectedRevision int64) error {
	plan, err := c.initiateUploadV2WithLayout(ctx, path, size, expectedRevision, "", "extent")
	if err != nil {
		return err
	}
	if plan.Key != "" {
		// Legacy server: the layout field was ignored and a classic session
		// was created. Run the classic pipeline on the same session.
		return c.uploadClassicSession(ctx, plan, ra, progress)
	}
	if err := c.uploadExtentSequential(ctx, plan, ra, size, progress); err != nil {
		c.abortUploadV2BestEffort(ctx, plan.UploadID)
		return err
	}
	return nil
}

// uploadExtentSequential drives an extent sequential session: for each batch
// of parts, hash the part contents, presign with checksums (deduped parts
// come back skip_upload), PUT the rest, then complete with the checksum
// etags. The server derives each part's slice from its part number.
func (c *Client) uploadExtentSequential(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt, size int64, progress ProgressFunc) error {
	const batchParts = 128
	var parts []completePart
	for first := 1; first <= plan.TotalParts; first += batchParts {
		last := first + batchParts - 1
		if last > plan.TotalParts {
			last = plan.TotalParts
		}
		batch, err := c.presignExtentBatch(ctx, plan, ra, size, first, last)
		if err != nil {
			return err
		}
		for _, p := range batch {
			if p.skipUpload {
				if progress != nil {
					progress(p.number, plan.TotalParts, 0)
				}
				continue
			}
			data, derr := extentPartBytes(ra, plan.PartSize, p.number, size)
			if derr != nil {
				return derr
			}
			if _, err := c.uploadOnePartV2(ctx, p.part, data); err != nil {
				if !isExtentBlockConflict(err) {
					return err
				}
				// The create-once address is already taken — this file
				// repeats the block, an interrupted earlier session landed
				// it, or a concurrent writer did — or the URL expired.
				// Re-presign this part: the server answers skip_upload when
				// the object is present and a fresh URL otherwise.
				fresh, perr := c.presignExtentBatch(ctx, plan, ra, size, p.number, p.number)
				if perr != nil {
					return perr
				}
				if !fresh[0].skipUpload {
					if _, err := c.uploadOnePartV2(ctx, fresh[0].part, data); err != nil {
						return err
					}
				}
			}
			if progress != nil {
				progress(p.number, plan.TotalParts, int64(len(data)))
			}
		}
		for _, p := range batch {
			parts = append(parts, completePart{Number: p.number, ETag: p.checksum})
		}
	}
	return c.completeUploadV2WithOptions(ctx, plan.UploadID, parts, nil, "")
}

type extentPresignedBatchItem struct {
	part       presignedPart
	checksum   string
	skipUpload bool
	number     int
}

// presignExtentBatch hashes parts [first,last] from ra, asks the server for
// their presigned URLs (with checksums, so dedupe can answer skip_upload),
// and returns them in order.
func (c *Client) presignExtentBatch(ctx context.Context, plan *uploadPlanV2, ra io.ReaderAt, size int64, first, last int) ([]extentPresignedBatchItem, error) {
	type reqPart struct {
		Number   int             `json:"part_number"`
		Checksum presignChecksum `json:"checksum"`
	}
	body := struct {
		Parts []reqPart `json:"parts"`
	}{}
	items := make([]extentPresignedBatchItem, 0, last-first+1)
	for n := first; n <= last; n++ {
		data, derr := extentPartBytes(ra, plan.PartSize, n, size)
		if derr != nil {
			return nil, derr
		}
		sum := sha256.Sum256(data)
		items = append(items, extentPresignedBatchItem{number: n, checksum: hex.EncodeToString(sum[:])})
	}
	for i := range items {
		body.Parts = append(body.Parts, reqPart{
			Number:   items[i].number,
			Checksum: presignChecksum{Algorithm: "sha256", Value: items[i].checksum},
		})
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v2/uploads/"+plan.UploadID+"/presign-batch", bytes.NewReader(raw))
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
