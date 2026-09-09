package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	// ExtentChunkSize is the 64MiB chunk partition used by commit-slices.
	ExtentChunkSize = 1 << 26
	// ExtentMaxBlockSize is the maximum immutable block object (4MiB).
	ExtentMaxBlockSize = 4 << 20

	maxExtentCommitAttempts = 8

	extentCodeCASConflict    = "cas_conflict"
	extentCodeBlockNotLanded = "block_not_landed"
	extentCodePendingExpired = "pending_expired"
)

// errExtentFallback tells WriteStream to use the single-blob upload path.
var errExtentFallback = errors.New("extent write fallback")

// PrepareBlock is one presigned PUT target.
type PrepareBlock struct {
	FileOff   int64             `json:"file_off"`
	Len       int64             `json:"len"`
	BlockKey  string            `json:"block_key"`
	PutURL    string            `json:"put_url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// SliceOp is one commit-slices write declaration.
type SliceOp struct {
	FileOff        int64  `json:"file_off"`
	Len            int64  `json:"len"`
	BlockKey       string `json:"block_key,omitempty"`
	BlockOff       int64  `json:"block_off"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	Kind           string `json:"kind,omitempty"`
}

// ChunkRowCount is the number of slice rows in one chunk after commit.
type ChunkRowCount struct {
	Chunk int64 `json:"chunk"`
	Rows  int   `json:"rows"`
}

// CommitSlicesResult is the commit-slices response.
type CommitSlicesResult struct {
	Revision   int64           `json:"revision"`
	Generation int64           `json:"generation"`
	SizeBytes  int64           `json:"size_bytes"`
	Replayed   bool            `json:"replayed"`
	ChunkRows  []ChunkRowCount `json:"chunk_rows,omitempty"`
}

// SliceRow is one committed extent. Later seq overlays earlier coverage.
type SliceRow struct {
	InodeID        string `json:"inode_id"`
	Chunk          int64  `json:"chunk"`
	Seq            int64  `json:"seq,omitempty"`
	FileOff        int64  `json:"file_off"`
	Len            int64  `json:"len"`
	BlockKey       string `json:"block_key"`
	BlockOff       int64  `json:"block_off"`
	BlockLen       int64  `json:"block_len"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	Kind           string `json:"kind"`
	BornGen        int64  `json:"born_gen"`
}

// ExtentReadPart is one presigned ranged GET from a read-plan.
type ExtentReadPart struct {
	FileOff   int64             `json:"file_off"`
	Len       int64             `json:"len"`
	BlockKey  string            `json:"block_key"`
	BlockOff  int64             `json:"block_off"`
	GetURL    string            `json:"get_url"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// ExtentReadPlan is the client-side read recipe.
type ExtentReadPlan struct {
	Revision   int64            `json:"revision"`
	Generation int64            `json:"generation"`
	SizeBytes  int64            `json:"size_bytes"`
	Parts      []ExtentReadPart `json:"parts"`
	// ChunkRows is the raw per-chunk slice count from doRead (JuiceFS len(ss)).
	ChunkRows []ChunkRowCount `json:"chunk_rows,omitempty"`
	// Layout is JuiceFS buildSlice output cached until InvalidateChunk.
	Layout []SliceRow `json:"layout,omitempty"`
}

// ExtentCommitError is a 409/410 commit-slices failure with CAS tokens.
type ExtentCommitError struct {
	StatusCode int
	Code       string
	Message    string
	Revision   int64
	Generation int64
	SizeBytes  int64
}

func (e *ExtentCommitError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("extent commit HTTP %d", e.StatusCode)
}

// ExtentPayload is one dirty file interval to upload.
type ExtentPayload struct {
	FileOff int64
	Data    []byte
}

// FlushExtentRequest is a prepare-blocks + PUT + commit-slices flush.
type FlushExtentRequest struct {
	Path               string
	ExpectedRevision   int64
	ExpectedGeneration int64
	Payloads           []ExtentPayload
	TruncateTo         *int64
	// Holes are declarative hole ops applied before payload ops on each
	// commit-slices attempt (local truncate-down then grow).
	Holes []SliceOp
	// AfterLanded is invoked after blocks are on object storage and before
	// each commit-slices attempt, so a local journal can record op_id + ops
	// together with the CAS tokens for that attempt. truncateTo is the
	// value this attempt will send (nil on a payload CAS retry).
	AfterLanded func(opID string, ops []SliceOp, expectedRevision, expectedGeneration int64, truncateTo *int64) error
	// AfterPrepared is invoked after prepare-blocks (keys + PUT URLs) and
	// before commit, so writeback can fsync local staging.
	AfterPrepared func([]LandedBlock) error
	// AfterPut is invoked after asynchronous writeback PUTs finish.
	AfterPut func([]LandedBlock)
	// Writeback commits metadata after prepare, then PUTs asynchronously
	// (JuiceFS --writeback). Callers must keep payload bytes until AfterPut.
	Writeback bool
	// Landed skips prepare/PUT (JuiceFS sliceWriter.flushData already
	// uploaded). Commit-slices uses these blocks as-is.
	Landed []LandedBlock
	// Append is JuiceFS meta.Write: no whole-file expected-revision CAS.
	Append bool
}

// LandedBlock is one prepared immutable block ready to PUT.
type LandedBlock struct {
	Op      SliceOp
	PutURL  string
	Headers map[string]string
	Data    []byte
	Path    string
}

type landedExtentBlock struct {
	op      SliceOp
	putURL  string
	headers map[string]string
	data    []byte
	path    string
}

func exportLanded(blocks []landedExtentBlock) []LandedBlock {
	out := make([]LandedBlock, 0, len(blocks))
	for _, blk := range blocks {
		out = append(out, LandedBlock{
			Op: blk.op, PutURL: blk.putURL, Headers: blk.headers, Data: blk.data, Path: blk.path,
		})
	}
	return out
}

func importLanded(blocks []LandedBlock) []landedExtentBlock {
	out := make([]landedExtentBlock, 0, len(blocks))
	for _, blk := range blocks {
		out = append(out, landedExtentBlock{
			op: blk.Op, putURL: blk.PutURL, headers: blk.Headers, data: blk.Data, path: blk.Path,
		})
	}
	return out
}

// PrepareExtentPayloads is JuiceFS writeback Finish: prepare-blocks only.
// The caller stages bytes locally, commits metadata, and PUTs in the background.
func (c *Client) PrepareExtentPayloads(ctx context.Context, path string, payloads []ExtentPayload) ([]LandedBlock, error) {
	if c == nil {
		return nil, fmt.Errorf("prepare extent: nil client")
	}
	if path == "" {
		return nil, fmt.Errorf("prepare extent: empty path")
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	landed, err := c.prepareExtent(ctx, path, splitExtentPayloads(payloads))
	if err != nil {
		return nil, err
	}
	return exportLanded(landed), nil
}

// PutExtentPayloads is JuiceFS sliceWriter.flushData without writeback:
// prepare-blocks + PUT, no commit-slices.
func (c *Client) PutExtentPayloads(ctx context.Context, path string, payloads []ExtentPayload) ([]LandedBlock, error) {
	if c == nil {
		return nil, fmt.Errorf("put extent: nil client")
	}
	if path == "" {
		return nil, fmt.Errorf("put extent: empty path")
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	landed, err := c.prepareAndPutExtent(ctx, path, splitExtentPayloads(payloads))
	if err != nil {
		return nil, err
	}
	return exportLanded(landed), nil
}

func (c *Client) CreateFileWithLayout(ctx context.Context, path, layout string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?create=1", nil)
	if err != nil {
		return 0, err
	}
	if layout != "" {
		req.Header.Set("X-Dat9-Content-Layout", layout)
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
		Revision int64 `json:"revision"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}
	return result.Revision, nil
}

func (c *Client) PrepareBlocks(ctx context.Context, path string, fileOff, length int64, checksumSHA256 string) ([]PrepareBlock, error) {
	return c.PrepareBlockRanges(ctx, path, []SliceOp{{
		FileOff: fileOff, Len: length, ChecksumSHA256: checksumSHA256,
	}})
}

func (c *Client) PrepareBlockRanges(ctx context.Context, path string, ranges []SliceOp) ([]PrepareBlock, error) {
	payload := make([]map[string]any, 0, len(ranges))
	for _, rng := range ranges {
		payload = append(payload, map[string]any{
			"file_off":        rng.FileOff,
			"len":             rng.Len,
			"checksum_sha256": rng.ChecksumSHA256,
		})
	}
	body, err := json.Marshal(map[string]any{"ranges": payload})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?prepare-blocks=1", bytes.NewReader(body))
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
		Blocks []PrepareBlock `json:"blocks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Blocks, nil
}

func (c *Client) CommitSlices(ctx context.Context, path, opID string, expectedRevision, expectedGeneration int64, ops []SliceOp, truncateTo *int64) (*CommitSlicesResult, error) {
	return c.CommitSlicesStaged(ctx, path, opID, expectedRevision, expectedGeneration, ops, truncateTo, false, false)
}

func (c *Client) CommitSlicesRetry(ctx context.Context, path string, expectedRevision, expectedGeneration int64, ops []SliceOp, truncateTo *int64, staged, exactSize bool) (*CommitSlicesResult, error) {
	rev, gen := expectedRevision, expectedGeneration
	opID := NewExtentOpID()
	var last error
	for attempt := 0; attempt < maxExtentCommitAttempts; attempt++ {
		var result *CommitSlicesResult
		var err error
		if staged {
			result, err = c.CommitSlicesStaged(ctx, path, opID, rev, gen, ops, truncateTo, true, false)
		} else {
			result, err = c.CommitSlices(ctx, path, opID, rev, gen, ops, truncateTo)
		}
		if err == nil {
			return result, nil
		}
		last = err
		var ce *ExtentCommitError
		if errors.As(err, &ce) && ce.StatusCode == http.StatusConflict && ce.Code != extentCodeBlockNotLanded {
			// Path truncate wants the exact size. Size-only grow must not
			// shrink a concurrent winner that already committed a larger file.
			if !exactSize && truncateTo != nil && ce.SizeBytes >= *truncateTo && ce.Revision > 0 {
				return &CommitSlicesResult{Revision: ce.Revision, Generation: ce.Generation, SizeBytes: ce.SizeBytes}, nil
			}
			if ce.Revision > 0 {
				rev = ce.Revision
			}
			if ce.Generation > 0 || ce.Revision > 0 {
				gen = ce.Generation
			}
			opID = NewExtentOpID()
			continue
		}
		return nil, err
	}
	if last != nil {
		return nil, last
	}
	return nil, fmt.Errorf("commit-slices: retries exhausted")
}

func (c *Client) CommitSlicesStaged(ctx context.Context, path, opID string, expectedRevision, expectedGeneration int64, ops []SliceOp, truncateTo *int64, staged, appendSlices bool) (*CommitSlicesResult, error) {
	reqBody := map[string]any{
		"expected_revision":   expectedRevision,
		"expected_generation": expectedGeneration,
		"op_id":               opID,
		"ops":                 ops,
		"staged":              staged,
		"append":              appendSlices,
	}
	if truncateTo != nil {
		reqBody["truncate_to"] = *truncateTo
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?commit-slices=1", bytes.NewReader(body))
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
		return nil, readExtentCommitError(resp)
	}
	var out CommitSlicesResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GrowExtentLength is JuiceFS doFallocate grow: persist a larger inode
// length without appending hole slices.
func (c *Client) GrowExtentLength(ctx context.Context, path, opID string, expectedRevision, expectedGeneration, newSize int64) (*CommitSlicesResult, error) {
	reqBody := map[string]any{
		"expected_revision":   expectedRevision,
		"expected_generation": expectedGeneration,
		"op_id":               opID,
		"ops":                 []SliceOp{},
		"truncate_to":         newSize,
		"grow_length":         true,
		"staged":              true,
		"append":              true,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?commit-slices=1", bytes.NewReader(body))
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
		return nil, readExtentCommitError(resp)
	}
	var out CommitSlicesResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) CompactExtentChunk(ctx context.Context, path string, chunk int64, snapshot []SliceRow, payloads []ExtentPayload) ([]LandedBlock, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	landed, err := c.prepareAndPutExtent(ctx, path, splitExtentPayloads(payloads))
	if err != nil {
		return nil, err
	}
	if err := c.CompactSlices(ctx, path, chunk, snapshot, opsFromLanded(landed)); err != nil {
		return nil, err
	}
	return exportLanded(landed), nil
}

func (c *Client) SetContentLayout(ctx context.Context, path, layout string) error {
	body, err := json.Marshal(map[string]string{"content_layout": layout})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?setattr=1", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

func (c *Client) PutPresigned(ctx context.Context, url string, headers map[string]string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	for k, v := range headers {
		if isForbiddenPresignedHeader(k) {
			continue
		}
		req.Header.Set(k, v)
	}
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusPreconditionFailed {
		return nil
	}
	if resp.StatusCode >= 300 {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("presigned PUT HTTP %d: %s", resp.StatusCode, slurp)
	}
	return nil
}

// FlushExtent uploads dirty intervals as immutable blocks and commits them.
// Write() only dirties locally; this is the durability point (fsync/close/cp).
func (c *Client) FlushExtent(ctx context.Context, req FlushExtentRequest) (*CommitSlicesResult, error) {
	if req.Path == "" {
		return nil, fmt.Errorf("flush extent: empty path")
	}
	if len(req.Payloads) == 0 && len(req.Landed) == 0 && req.TruncateTo == nil && len(req.Holes) == 0 {
		return nil, fmt.Errorf("flush extent: nothing to commit")
	}

	payloads := splitExtentPayloads(req.Payloads)
	rev := req.ExpectedRevision
	gen := req.ExpectedGeneration
	truncateTo := req.TruncateTo
	opID := NewExtentOpID()

	var landed []landedExtentBlock
	var err error
	if len(req.Landed) > 0 {
		landed = importLanded(req.Landed)
	} else if len(payloads) > 0 {
		if req.Writeback {
			landed, err = c.prepareExtent(ctx, req.Path, payloads)
		} else {
			landed, err = c.prepareAndPutExtent(ctx, req.Path, payloads)
		}
		if err != nil {
			return nil, err
		}
	}
	ops := append(append([]SliceOp(nil), req.Holes...), opsFromLanded(landed)...)
	if req.Writeback && req.AfterPrepared != nil {
		if prepErr := req.AfterPrepared(exportLanded(landed)); prepErr != nil {
			return nil, fmt.Errorf("extent staging: %w", prepErr)
		}
	}
	if req.AfterLanded != nil {
		_ = req.AfterLanded(opID, ops, rev, gen, truncateTo)
	}

	for attempt := 0; attempt < maxExtentCommitAttempts; attempt++ {
		var result *CommitSlicesResult
		var commitErr error
		// After a successful PUT in this process the pending row is still
		// live; staged skips per-block HEAD (fio rand_rw freezeAll of ~800
		// 4k slices). Writeback commits before PUT with the same flag.
		result, commitErr = c.CommitSlicesStaged(ctx, req.Path, opID, rev, gen, ops, truncateTo, true, req.Append)
		if commitErr == nil {
			if req.Writeback && len(landed) > 0 {
				go func(blocks []landedExtentBlock, after func([]LandedBlock)) {
					_ = c.reputLanded(context.Background(), blocks)
					if after != nil {
						after(exportLanded(blocks))
					}
				}(landed, req.AfterPut)
			} else if req.AfterPut != nil {
				req.AfterPut(exportLanded(landed))
			}
			return result, nil
		}
		var ce *ExtentCommitError
		if errors.As(commitErr, &ce) {
			switch {
			case ce.StatusCode == http.StatusConflict && ce.Code == extentCodeBlockNotLanded:
				if putErr := c.reputLanded(ctx, landed); putErr != nil {
					landed, err = c.prepareAndPutExtent(ctx, req.Path, payloads)
					if err != nil {
						return nil, err
					}
					ops = append(append([]SliceOp(nil), req.Holes...), opsFromLanded(landed)...)
					opID = NewExtentOpID()
					if req.AfterLanded != nil {
						_ = req.AfterLanded(opID, ops, rev, gen, truncateTo)
					}
				}
				continue
			case ce.StatusCode == http.StatusGone || ce.Code == extentCodePendingExpired:
				if len(payloads) > 0 {
					landed, err = c.prepareAndPutExtent(ctx, req.Path, payloads)
					if err != nil {
						return nil, err
					}
					ops = append(append([]SliceOp(nil), req.Holes...), opsFromLanded(landed)...)
				}
				opID = NewExtentOpID()
				if req.AfterLanded != nil {
					_ = req.AfterLanded(opID, ops, rev, gen, truncateTo)
				}
				continue
			case ce.StatusCode == http.StatusConflict:
				// A stale truncate_to can shrink a concurrent winner's larger
				// commit. Size-only flushes return the conflict; payload
				// retries overlay without shrinking.
				if truncateTo != nil && len(payloads) == 0 {
					return nil, commitErr
				}
				truncateTo = nil
				if ce.Revision > 0 {
					rev = ce.Revision
				}
				if ce.Generation > 0 || ce.Revision > 0 {
					gen = ce.Generation
				}
				opID = NewExtentOpID()
				if req.AfterLanded != nil {
					_ = req.AfterLanded(opID, ops, rev, gen, truncateTo)
				}
				continue
			}
		}
		var se *StatusError
		if errors.As(commitErr, &se) && se.StatusCode >= 500 && se.StatusCode <= 599 {
			continue
		}
		return nil, commitErr
	}
	return nil, fmt.Errorf("flush extent: retries exhausted")
}

func (c *Client) prepareExtent(ctx context.Context, remotePath string, payloads []ExtentPayload) ([]landedExtentBlock, error) {
	ranges := make([]SliceOp, 0, len(payloads))
	pending := make(map[int64][]ExtentPayload, len(payloads))
	for _, p := range payloads {
		sum := SHA256Hex(p.Data)
		ranges = append(ranges, SliceOp{FileOff: p.FileOff, Len: int64(len(p.Data)), ChecksumSHA256: sum})
		pending[p.FileOff] = append(pending[p.FileOff], p)
	}
	blocks, err := c.PrepareBlockRanges(ctx, remotePath, ranges)
	if err != nil {
		return nil, err
	}
	landed := make([]landedExtentBlock, 0, len(blocks))
	for _, blk := range blocks {
		list := pending[blk.FileOff]
		if len(list) == 0 || int64(len(list[0].Data)) < blk.Len {
			return nil, fmt.Errorf("prepare-blocks returned unexpected offset %d", blk.FileOff)
		}
		p := list[0]
		pending[blk.FileOff] = list[1:]
		data := p.Data[:blk.Len]
		landed = append(landed, landedExtentBlock{
			op: SliceOp{
				FileOff:        blk.FileOff,
				Len:            blk.Len,
				BlockKey:       blk.BlockKey,
				BlockOff:       0,
				ChecksumSHA256: SHA256Hex(data),
			},
			putURL:  blk.PutURL,
			headers: blk.Headers,
			data:    data,
			path:    remotePath,
		})
	}
	return landed, nil
}

func (c *Client) prepareAndPutExtent(ctx context.Context, remotePath string, payloads []ExtentPayload) ([]landedExtentBlock, error) {
	landed, err := c.prepareExtent(ctx, remotePath, payloads)
	if err != nil {
		return nil, err
	}
	if err := c.reputLanded(ctx, landed); err != nil {
		return nil, err
	}
	return landed, nil
}

func (c *Client) reputLanded(ctx context.Context, landed []landedExtentBlock) error {
	if len(landed) == 0 {
		return nil
	}
	if len(landed) == 1 {
		return c.putOneLanded(ctx, landed[0])
	}
	sem := make(chan struct{}, 16)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for i := range landed {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return err
		}
		select {
		case err := <-errCh:
			wg.Wait()
			return err
		default:
		}
		blk := landed[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := c.putOneLanded(ctx, blk); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

func (c *Client) putOneLanded(ctx context.Context, blk landedExtentBlock) error {
	if blk.putURL == "" {
		if blk.path == "" || blk.op.BlockKey == "" {
			return fmt.Errorf("put extent: missing put url")
		}
		fresh, presignErr := c.PresignPutBlock(ctx, blk.path, blk.op.BlockKey, blk.op.Len, blk.op.ChecksumSHA256)
		if presignErr != nil {
			return presignErr
		}
		return c.PutPresigned(ctx, fresh.PutURL, fresh.Headers, blk.data)
	}
	if err := c.PutPresigned(ctx, blk.putURL, blk.headers, blk.data); err != nil {
		if blk.path != "" && (strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), "HTTP 400")) {
			fresh, presignErr := c.PresignPutBlock(ctx, blk.path, blk.op.BlockKey, blk.op.Len, blk.op.ChecksumSHA256)
			if presignErr != nil {
				return err
			}
			return c.PutPresigned(ctx, fresh.PutURL, fresh.Headers, blk.data)
		}
		return err
	}
	return nil
}

func (c *Client) PresignPutBlock(ctx context.Context, path, blockKey string, size int64, checksumSHA256 string) (*PrepareBlock, error) {
	body, err := json.Marshal(map[string]any{
		"block_key":       blockKey,
		"len":             size,
		"checksum_sha256": checksumSHA256,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?presign-put=1", bytes.NewReader(body))
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
	var blk PrepareBlock
	if err := json.NewDecoder(resp.Body).Decode(&blk); err != nil {
		return nil, err
	}
	return &blk, nil
}

func (c *Client) CopyFileRange(ctx context.Context, srcPath, dstPath string, srcOff, dstOff, length, expectedRevision, expectedGeneration int64) (*CommitSlicesResult, error) {
	rev, gen := expectedRevision, expectedGeneration
	var last error
	for attempt := 0; attempt < maxExtentCommitAttempts; attempt++ {
		body, err := json.Marshal(map[string]any{
			"src":                 srcPath,
			"src_off":             srcOff,
			"dst_off":             dstOff,
			"length":              length,
			"expected_revision":   rev,
			"expected_generation": gen,
		})
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(dstPath)+"?clone-range=1", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode < 300 {
			var out CommitSlicesResult
			decErr := json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()
			if decErr != nil {
				return nil, decErr
			}
			return &out, nil
		}
		commitErr := readExtentCommitError(resp)
		_ = resp.Body.Close()
		last = commitErr
		var ce *ExtentCommitError
		if errors.As(commitErr, &ce) && ce.StatusCode == http.StatusConflict && ce.Code != extentCodeBlockNotLanded {
			if ce.Revision > 0 {
				rev = ce.Revision
			}
			if ce.Generation > 0 || ce.Revision > 0 {
				gen = ce.Generation
			}
			continue
		}
		return nil, commitErr
	}
	if last != nil {
		return nil, last
	}
	return nil, fmt.Errorf("clone-range: retries exhausted")
}

func opsFromLanded(landed []landedExtentBlock) []SliceOp {
	ops := make([]SliceOp, 0, len(landed))
	for _, blk := range landed {
		ops = append(ops, blk.op)
	}
	return ops
}

func splitExtentPayloads(in []ExtentPayload) []ExtentPayload {
	var out []ExtentPayload
	for _, p := range in {
		if len(p.Data) == 0 {
			continue
		}
		for _, part := range SplitExtentRange(p.FileOff, int64(len(p.Data))) {
			rel := part.FileOff - p.FileOff
			out = append(out, ExtentPayload{
				FileOff: part.FileOff,
				Data:    p.Data[rel : rel+part.Len],
			})
		}
	}
	return out
}

// SplitExtentRange cuts a dirty interval on 4MiB and 64MiB chunk boundaries.
func SplitExtentRange(fileOff, length int64) []SliceOp {
	if length <= 0 {
		return nil
	}
	var out []SliceOp
	off := fileOff
	end := fileOff + length
	for off < end {
		chunkEnd := (off &^ (ExtentChunkSize - 1)) + ExtentChunkSize
		blockEnd := off + ExtentMaxBlockSize
		next := end
		if chunkEnd < next {
			next = chunkEnd
		}
		if blockEnd < next {
			next = blockEnd
		}
		out = append(out, SliceOp{FileOff: off, Len: next - off})
		off = next
	}
	return out
}

func (c *Client) writeExtentStream(ctx context.Context, remotePath string, r io.Reader, size int64, expectedRevision int64, summary *UploadSummary) (*UploadSummary, error) {
	stat, err := c.StatCtx(ctx, remotePath)
	rev := int64(0)
	gen := int64(0)
	remoteSize := int64(0)
	switch {
	case IsNotFound(err):
		_, createErr := c.CreateFileWithLayout(ctx, remotePath, string(ContentLayoutExtent))
		if createErr != nil {
			if isExtentDisabledError(createErr) {
				return nil, errExtentFallback
			}
			var se *StatusError
			if !errors.As(createErr, &se) || se.StatusCode != http.StatusConflict {
				return nil, createErr
			}
		}
		stat, err = c.StatCtx(ctx, remotePath)
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	}
	if stat == nil {
		return nil, fmt.Errorf("extent write: missing stat")
	}
	if stat.ContentLayout != "" && stat.ContentLayout != ContentLayoutExtent {
		return nil, errExtentFallback
	}
	rev = stat.Revision
	gen = stat.SliceGeneration
	remoteSize = stat.Size
	if expectedRevision > 0 && rev != expectedRevision {
		return nil, &StatusError{StatusCode: http.StatusConflict, Message: "revision conflict"}
	}

	var payloads []ExtentPayload
	if size > 0 {
		if ra, ok := r.(io.ReaderAt); ok {
			off := int64(0)
			for off < size {
				parts := SplitExtentRange(off, size-off)
				if len(parts) == 0 {
					break
				}
				part := parts[0]
				buf := make([]byte, part.Len)
				n, readErr := ra.ReadAt(buf, part.FileOff)
				if n < int(part.Len) {
					if readErr == nil {
						readErr = io.ErrUnexpectedEOF
					}
					if n == 0 {
						return nil, fmt.Errorf("extent write: read at %d: %w", part.FileOff, readErr)
					}
					buf = buf[:n]
				}
				payloads = append(payloads, ExtentPayload{FileOff: part.FileOff, Data: buf})
				off = part.FileOff + int64(len(buf))
			}
		} else {
			off := int64(0)
			for off < size {
				n := int64(ExtentMaxBlockSize)
				chunkEnd := (off &^ (ExtentChunkSize - 1)) + ExtentChunkSize
				if off+n > chunkEnd {
					n = chunkEnd - off
				}
				if off+n > size {
					n = size - off
				}
				buf := make([]byte, n)
				if _, readErr := io.ReadFull(r, buf); readErr != nil {
					return nil, fmt.Errorf("extent write: read at %d: %w", off, readErr)
				}
				payloads = append(payloads, ExtentPayload{FileOff: off, Data: buf})
				off += n
			}
		}
	}

	var truncateTo *int64
	if size < remoteSize || size == 0 {
		to := size
		truncateTo = &to
	}
	if len(payloads) == 0 && truncateTo == nil {
		if summary != nil {
			summary.Mode = "extent"
			summary.TotalBytes = 0
			summary.UploadedParts = 0
		}
		return finishUploadSummary(summary), nil
	}

	result, err := c.FlushExtent(ctx, FlushExtentRequest{
		Path:               remotePath,
		ExpectedRevision:   rev,
		ExpectedGeneration: gen,
		Payloads:           payloads,
		TruncateTo:         truncateTo,
	})
	if err != nil {
		if isExtentDisabledError(err) || isExtentUseCommitError(err) {
			return nil, errExtentFallback
		}
		return nil, err
	}
	if summary != nil {
		summary.Mode = "extent"
		summary.TotalBytes = size
		summary.TotalParts = len(payloads)
		summary.UploadedParts = len(payloads)
		summary.PartSizeBytes = ExtentMaxBlockSize
		_ = result
	}
	return finishUploadSummary(summary), nil
}

func isExtentDisabledError(err error) bool {
	var se *StatusError
	if errors.As(err, &se) && se.StatusCode == http.StatusBadRequest && strings.Contains(se.Message, "extent_disabled") {
		return true
	}
	return strings.Contains(err.Error(), "extent_disabled")
}

func isExtentUseCommitError(err error) bool {
	var se *StatusError
	if errors.As(err, &se) && se.StatusCode == http.StatusBadRequest {
		msg := strings.ToLower(se.Message)
		return strings.Contains(msg, "commit-slices") || strings.Contains(msg, "content_layout=extent")
	}
	return false
}

func NewExtentOpID() string {
	return ulid.Make().String()
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func readExtentCommitError(resp *http.Response) error {
	var payload struct {
		Error      string `json:"error"`
		Code       string `json:"code"`
		Revision   int64  `json:"revision"`
		Generation int64  `json:"generation"`
		SizeBytes  int64  `json:"size_bytes"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	msg := payload.Error
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusGone {
		return &ExtentCommitError{
			StatusCode: resp.StatusCode,
			Code:       payload.Code,
			Message:    msg,
			Revision:   payload.Revision,
			Generation: payload.Generation,
			SizeBytes:  payload.SizeBytes,
		}
	}
	return &StatusError{StatusCode: resp.StatusCode, Message: msg}
}
