package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

func TestPrepareBlockJSONUsesSnakeCase(t *testing.T) {
	blk := PrepareBlock{
		FileOff:   8,
		Len:       13,
		BlockKey:  "blocks/inode/ulid",
		PutURL:    "http://host.orb.internal:9009/s3/t/objects/blocks/inode/ulid",
		Headers:   map[string]string{"content-length": "13"},
		ExpiresAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	raw, err := json.Marshal(blk)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for _, key := range []string{`"file_off"`, `"len"`, `"block_key"`, `"put_url"`, `"headers"`, `"expires_at"`} {
		if !strings.Contains(s, key) {
			t.Errorf("missing %s in %s", key, s)
		}
	}
	for _, leaked := range []string{`"FileOff"`, `"PutURL"`, `"BlockKey"`} {
		if strings.Contains(s, leaked) {
			t.Errorf("Go field name leaked %s in %s", leaked, s)
		}
	}
	var decoded struct {
		PutURL  string `json:"put_url"`
		FileOff int64  `json:"file_off"`
		Len     int64  `json:"len"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.PutURL != blk.PutURL || decoded.FileOff != 8 || decoded.Len != 13 {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func extentCreateCtx() context.Context {
	return WithContentLayout(context.Background(), datastore.ContentLayoutExtent)
}

func TestExtentCreatePrepareCommitRead(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/wal/app.db-wal"

	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if !nf.File.IsExtent() {
		t.Fatalf("layout=%q storage_ref=%q, want extent", nf.File.Layout(), nf.File.StorageRef)
	}

	payload := []byte("hello-extent-block")
	sum := sha256.Sum256(payload)
	hexSum := hex.EncodeToString(sum[:])
	blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
		FileOff: 0, Len: int64(len(payload)), ChecksumSHA256: hexSum,
	}})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("blocks=%d", len(blocks))
	}
	encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
	if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(payload), int64(len(payload)), encOpts); err != nil {
		t.Fatalf("put: %v", err)
	}
	result, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision:   1,
		ExpectedGeneration: 0,
		OpID:               "op-1",
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
		}},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if result.SizeBytes != int64(len(payload)) || result.Revision != 2 || result.Generation != 1 {
		t.Fatalf("result=%+v", result)
	}

	replay, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision:   1,
		ExpectedGeneration: 0,
		OpID:               "op-1",
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
		}},
	})
	if err != nil || !replay.Replayed {
		t.Fatalf("idempotent replay: %+v %v", replay, err)
	}

	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, result.SizeBytes, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Fatalf("assembled %q want %q", buf.Bytes(), payload)
	}

	plan, err := b.ReadPlanCtx(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Assemble || plan.PresignURL != "" {
		t.Fatalf("read plan = %+v, want assemble without redirect", plan)
	}

	clientPlan, err := b.PlanExtentRead(ctx, path, 0, -1)
	if err != nil {
		t.Fatalf("PlanExtentRead: %v", err)
	}
	if clientPlan.SizeBytes != result.SizeBytes || len(clientPlan.Parts) != 1 {
		t.Fatalf("client plan=%+v", clientPlan)
	}
	if clientPlan.Parts[0].GetURL == "" || clientPlan.Parts[0].BlockKey != blocks[0].BlockKey {
		t.Fatalf("part=%+v", clientPlan.Parts[0])
	}
}

func TestExtentAppendOverlappingWriteWithoutCAS(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/overlap.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	first := []byte("AAAAAAAA")
	second := []byte("BBBB")
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID:   opID,
			Append: true,
			Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("append %s: %v", opID, err)
		}
	}
	put(first, 0, "op-a")
	put(second, 2, "op-b")
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, 8, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if got := buf.String(); got != "AABBBBAA" {
		t.Fatalf("assembled %q want AABBBBAA (later seq overlays)", got)
	}
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: 1, ExpectedGeneration: 0, OpID: "stale-cas", Append: true,
		Ops: []datastore.SliceOp{{FileOff: 0, Len: 1, Kind: datastore.SliceKindHole}},
	}); err != nil {
		t.Fatalf("append must not require matching revision: %v", err)
	}
}

func TestExtentStagedCommitAcceptsClientAllocatedBlockKeys(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/client-keys.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	key := "blocks/" + nf.File.FileID + "/client-ulid"
	payload := []byte("hello-wb")
	sum := sha256.Sum256(payload)
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		OpID: "wb-1", Append: true, Staged: true,
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: key, ChecksumSHA256: hex.EncodeToString(sum[:]),
		}},
	}); err != nil {
		t.Fatalf("staged commit without prepare: %v", err)
	}
}

func TestExtentCompactSkipsWritebackBlocksNotOnS3(t *testing.T) {
	// JuiceFS compact reads the chunk cache. Drive9 compact is server-side
	// S3; writeback CommitSlices before PUT must not copy missing objects.
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/writeback-compact.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, payload := range [][]byte{[]byte("AAAAAAAA"), []byte("BBBBBBBB")} {
		sum := sha256.Sum256(payload)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: int64(i * 8), Len: int64(len(payload)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: fmt.Sprintf("staged-%d", i), Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: int64(i * 8), Len: int64(len(payload)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("staged commit: %v", err)
		}
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.EnqueueCompactIfNeeded(ctx, nf.File.FileID, map[int64]struct{}{0: {}}, 2, time.Now().UTC()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, err = b.ProcessOneSliceCompact(ctx)
	if err == nil || !(strings.Contains(err.Error(), "not landed") || strings.Contains(err.Error(), "pending blocks")) {
		t.Fatalf("compact err=%v, want skip until objects land", err)
	}
}

func TestExtentCompactOverlappingWriteLastWins(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/overlap-compact.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("append %s: %v", opID, err)
		}
	}
	put([]byte("AAAAAAAA"), 0, "compact-a")
	put([]byte("BBBB"), 2, "compact-b")
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.EnqueueCompactIfNeeded(ctx, nf.File.FileID, map[int64]struct{}{0: {}}, 2, time.Now().UTC()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	did, err := b.ProcessOneSliceCompact(ctx)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !did {
		t.Fatal("expected a compact task")
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, 8, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if got := buf.String(); got != "AABBBBAA" {
		t.Fatalf("after compact %q want AABBBBAA", got)
	}
}

func TestCompactAndPlanMany4KPagesLastWins(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/pages.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	const page = 4096
	const pages = 16
	want := make([]byte, page*pages)
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("append %s: %v", opID, err)
		}
		copy(want[off:off+int64(len(data))], data)
	}
	for i := 0; i < pages; i++ {
		buf := bytes.Repeat([]byte{byte(i + 1)}, page)
		put(buf, int64(i*page), fmt.Sprintf("init-%d", i))
	}
	for round := 0; round < 5; round++ {
		for i := 0; i < pages; i += 2 {
			buf := bytes.Repeat([]byte{byte(0xA0 + round)}, page)
			put(buf, int64(i*page), fmt.Sprintf("r%d-p%d", round, i))
		}
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.EnqueueCompactIfNeeded(ctx, nf.File.FileID, map[int64]struct{}{0: {}}, 2, time.Now().UTC()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := b.ProcessOneSliceCompact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	var assembled bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, int64(len(want)), &assembled); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if !bytes.Equal(assembled.Bytes(), want) {
		t.Fatalf("assemble after compact diverged from last-write-wins")
	}
	for i := 0; i < pages; i++ {
		off := int64(i * page)
		plan, err := b.PlanExtentRead(ctx, path, off, off+page)
		if err != nil {
			t.Fatalf("plan page %d: %v", i, err)
		}
		got := make([]byte, page)
		for _, p := range plan.Parts {
			from := p.FileOff
			if from < off {
				from = off
			}
			to := p.FileOff + p.Len
			if to > off+page {
				to = off + page
			}
			if to <= from {
				continue
			}
			piece := make([]byte, p.Len)
			rc, err := b.s3.GetObjectRange(ctx, p.BlockKey, p.BlockOff, p.BlockOff+p.Len-1)
			if err != nil {
				t.Fatalf("get page %d: %v", i, err)
			}
			_, readErr := io.ReadFull(rc, piece)
			_ = rc.Close()
			if readErr != nil {
				t.Fatalf("read page %d: %v", i, readErr)
			}
			delta := from - p.FileOff
			copy(got[from-off:to-off], piece[delta:delta+(to-from)])
		}
		if !bytes.Equal(got, want[off:off+page]) {
			t.Fatalf("plan page %d mismatch first=%d want=%d", i, got[0], want[off])
		}
		if i > 0 && len(plan.Parts) > 0 && plan.Parts[0].BlockOff > 0 {
			if !strings.Contains(plan.Parts[0].GetURL, "range=") {
				t.Fatalf("page %d GetURL=%q, JuiceFS ReadAt must presign the block range", i, plan.Parts[0].GetURL)
			}
		}
	}
}

func TestPlanExtentReadSeesPagesDuringCompact(t *testing.T) {
	// JuiceFS doRead is one chunk blob. Racing ListSlices with compact's
	// DELETE+INSERT must not return an empty plan for a live 4K page.
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/compact-race.db"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	put := func(off int64, v byte, opID string) {
		t.Helper()
		data := bytes.Repeat([]byte{v}, 4096)
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: 4096, ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatal(err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), 4096, encOpts); err != nil {
			t.Fatal(err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: 4096, BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 6; i++ {
		put(int64(i)*4096, byte('A'+i), fmt.Sprintf("p%d", i))
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.EnqueueCompactIfNeeded(ctx, nf.File.FileID, map[int64]struct{}{0: {}}, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	empty := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 80; i++ {
			plan, planErr := b.PlanExtentRead(ctx, path, 0, 4096)
			if planErr != nil {
				continue
			}
			if plan.SizeBytes >= 4096 && len(plan.Parts) == 0 {
				empty++
			}
		}
	}()
	if _, err := b.ProcessOneSliceCompact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	<-done
	if empty > 0 {
		t.Fatalf("empty plans during compact=%d, JuiceFS chunk blob is never empty", empty)
	}
}

func TestPlanExtentReadEmitsBuildSliceHoles(t *testing.T) {
	// JuiceFS buildSlice inserts Id=0 for gaps. FUSE must see an explicit
	// hole part, not an unlabeled window that KEEP_CACHE would cache as zeros.
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/plan-holes.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	put := func(off int64, v byte, opID string) {
		t.Helper()
		data := []byte{v}
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: 1, ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatal(err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), 1, encOpts); err != nil {
			t.Fatal(err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: 1, BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	put(0, 'A', "a")
	put(2, 'C', "c")
	to := int64(3)
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		OpID: "len", Append: true, TruncateTo: &to, GrowLength: true,
	}); err != nil {
		t.Fatal(err)
	}
	plan, err := b.PlanExtentRead(ctx, path, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	var holes, data int
	for _, p := range plan.Parts {
		if p.BlockKey == "" {
			holes++
			if p.FileOff != 1 || p.Len != 1 {
				t.Fatalf("hole part=%+v want file_off=1 len=1", p)
			}
			continue
		}
		data++
	}
	if data != 2 || holes != 1 {
		t.Fatalf("parts data=%d holes=%d want 2 data + 1 hole: %+v", data, holes, plan.Parts)
	}
	st, err := b.store.LoadExtentReadState(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if st.SizeBytes != plan.SizeBytes || st.Revision != plan.Revision || st.Generation != plan.Generation {
		t.Fatalf("plan size/rev/gen=%d/%d/%d load=%d/%d/%d", plan.SizeBytes, plan.Revision, plan.Generation, st.SizeBytes, st.Revision, st.Generation)
	}
}

func TestCompactAfterGrowHoleKeepsPageWrite(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/sparse-pages.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatal(err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatal(err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	page1 := bytes.Repeat([]byte{0xA1}, 4096)
	page4 := bytes.Repeat([]byte{0xB4}, 4096)
	put(page1, 0, "p1")
	to := int64(32768)
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		OpID: "grow", Append: true, TruncateTo: &to,
	}); err != nil {
		t.Fatalf("grow: %v", err)
	}
	put(page4, 12288, "p4")
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.EnqueueCompactIfNeeded(ctx, nf.File.FileID, map[int64]struct{}{0: {}}, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ProcessOneSliceCompact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 12288, 16384, &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), page4) {
		t.Fatalf("page 4 after compact is zeros=%t, grow hole must not become a spanning data slice",
			bytes.Count(buf.Bytes(), []byte{0}) == len(buf.Bytes()))
	}
	rows, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == datastore.SliceKindData && row.FileOff == 0 && row.Len >= 32768 {
			t.Fatalf("compact emitted spanning data slice len=%d with interior holes", row.Len)
		}
	}
}

func TestFallocateGrowKeepsWrittenPageThroughCompact(t *testing.T) {
	// JuiceFS doFallocate grow does not append hole slices. A later compact
	// must not bake zeros over a page that was already written (sqlite page 5).
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/falloc-grow.db"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	page1 := bytes.Repeat([]byte("1"), 4096)
	page5 := make([]byte, 4096)
	page5[0] = 0x0d
	copy(page5[1:], bytes.Repeat([]byte("P5"), 2047))
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatal(err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatal(err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("%s: %v", opID, err)
		}
	}
	put(page1, 0, "p1")
	put(page5, 16384, "p5")
	to := int64(733184)
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		OpID: "falloc-grow", Append: true, TruncateTo: &to, GrowLength: true,
	}); err != nil {
		t.Fatalf("grow length: %v", err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == datastore.SliceKindHole && row.FileOff <= 16384 && row.FileOff+row.Len > 16384 {
			t.Fatalf("fallocate grow punched page 5: %+v", row)
		}
	}
	if err := b.store.EnqueueCompactIfNeeded(ctx, nf.File.FileID, map[int64]struct{}{0: {}}, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ProcessOneSliceCompact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 16384, 20480, &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), page5) {
		t.Fatalf("page 5 after fallocate-grow compact is zeros=%t, JuiceFS grow must not bake a hole",
			bytes.Count(buf.Bytes(), []byte{0}) == len(buf.Bytes()))
	}
}

func TestCommitSlicesTruncateToThenWriteLastWins(t *testing.T) {
	// JuiceFS Truncate then Write: a grow hole must not overlay the payload
	// in the same commit (sqlite pages 4/8 were durable zeros).
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/grow-then-write.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	page := bytes.Repeat([]byte("X"), 4096)
	sum := sha256.Sum256(page)
	hexSum := hex.EncodeToString(sum[:])
	blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
		FileOff: 12288, Len: 4096, ChecksumSHA256: hexSum,
	}})
	if err != nil {
		t.Fatal(err)
	}
	encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
	if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(page), 4096, encOpts); err != nil {
		t.Fatal(err)
	}
	to := int64(32768)
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		OpID: "grow-write", Append: true, Staged: true, TruncateTo: &to,
		Ops: []datastore.SliceOp{{
			FileOff: 12288, Len: 4096, BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
		}},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 12288, 16384, &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), page) {
		t.Fatalf("page 4 after grow+write is %d zeros=%t, JuiceFS write seq must beat truncate holes",
			len(buf.Bytes()), bytes.Count(buf.Bytes(), []byte{0}) == len(buf.Bytes()))
	}
}

func TestCommitSlicesWithoutAppendFlagStillOverlays(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/always-append.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("commit %s: %v", opID, err)
		}
	}
	put([]byte("AAAAAAAA"), 0, "aa")
	put([]byte("BBBB"), 2, "bb")
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d want 2 overlapping appends, got %+v", len(rows), rows)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, 8, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if got := buf.String(); got != "AABBBBAA" {
		t.Fatalf("assembled %q want AABBBBAA", got)
	}
}

func TestCompactKeepsWritesAfterSnapshot(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/compact-suffix.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("commit %s: %v", opID, err)
		}
	}
	put([]byte("AAAAAAAA"), 0, "pre-a")
	put([]byte("BBBB"), 2, "pre-b")
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	put([]byte("XX"), 1, "during-copy")
	merged := []byte("AABBBBAA")
	sumM := sha256.Sum256(merged)
	hexM := hex.EncodeToString(sumM[:])
	prep, err := b.PrepareBlocks(ctx, path, []PrepareRange{{FileOff: 0, Len: 8, ChecksumSHA256: hexM}})
	if err != nil {
		t.Fatal(err)
	}
	encOpts, _, _ := b.s3WriteEncryption(prep[0].BlockKey)
	if err := b.s3.PutObject(ctx, prep[0].BlockKey, bytes.NewReader(merged), 8, encOpts); err != nil {
		t.Fatal(err)
	}
	if err := b.CompactSlices(ctx, path, CompactSlicesRequest{
		Chunk:    0,
		Snapshot: snapshot,
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: 8, BlockKey: prep[0].BlockKey, ChecksumSHA256: hexM,
		}},
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, 8, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if got := buf.String(); got != "AXXBBBAA" {
		t.Fatalf("after compact+suffix %q want AXXBBBAA", got)
	}
	after, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) < 2 {
		t.Fatalf("suffix write must remain, rows=%+v", after)
	}
}

func TestPlanExtentReadEnqueuesCompactAtFiveSlices(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/read-compact.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < datastore.ExtentCompactReadRows; i++ {
		data := []byte{byte('A' + i)}
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: 0, Len: 1, ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), 1, encOpts); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: fmt.Sprintf("rc-%d", i), Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: 0, Len: 1, BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := b.PlanExtentRead(ctx, path, 0, 1); err != nil {
		t.Fatalf("plan: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var n int
	for {
		if err := b.store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM slice_compact_tasks`).Scan(&n); err != nil {
			t.Fatalf("count compact tasks: %v", err)
		}
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("JuiceFS meta.Read must enqueue compact at 5 slices")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExtentWriteDoesNotEnqueueCompactAt32(t *testing.T) {
	// JuiceFS Write compact is 99/350; counting the blob on every CONCAT
	// made wal-multiwrite --wait all miss the 1s budget.
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/write-no-count.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 32; i++ {
		data := []byte{byte(i)}
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: int64(i), Len: 1, ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), 1, encOpts); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: fmt.Sprintf("nc-%d", i), Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: int64(i), Len: 1, BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	var n int
	if err := b.store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM slice_compact_tasks`).Scan(&n); err != nil {
		t.Fatalf("count compact tasks: %v", err)
	}
	if n != 0 {
		t.Fatalf("Write must not enqueue compact at 32 slices, got %d tasks", n)
	}
}

func TestExtentTruncateAfterOverlappingAppends(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/overlap-trunc.bin"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	first := []byte("AAAAAAAA")
	second := []byte("BB")
	put := func(data []byte, off int64, opID string) {
		t.Helper()
		sum := sha256.Sum256(data)
		hexSum := hex.EncodeToString(sum[:])
		blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
			FileOff: off, Len: int64(len(data)), ChecksumSHA256: hexSum,
		}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
			OpID: opID, Append: true, Staged: true,
			Ops: []datastore.SliceOp{{
				FileOff: off, Len: int64(len(data)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
			}},
		}); err != nil {
			t.Fatalf("append %s: %v", opID, err)
		}
	}
	put(first, 0, "overlap-trunc-full")
	put(second, 0, "overlap-trunc-head")
	to := int64(4)
	result, err := b.CommitSlices(ctx, path, CommitSlicesRequest{OpID: "overlap-trunc-size", TruncateTo: &to})
	if err != nil {
		t.Fatalf("truncate overlapping slices: %v", err)
	}
	if result.SizeBytes != 4 {
		t.Fatalf("size=%d want 4", result.SizeBytes)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, 4, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if got := buf.String(); got != "BBAA" {
		t.Fatalf("assembled %q want BBAA", got)
	}
	grow := int64(8)
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{OpID: "overlap-trunc-grow", TruncateTo: &grow}); err != nil {
		t.Fatalf("grow after shrink: %v", err)
	}
	buf.Reset()
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, 8, &buf); err != nil {
		t.Fatalf("assemble grow: %v", err)
	}
	if got := buf.String(); got != "BBAA\x00\x00\x00\x00" {
		t.Fatalf("grown %q want BBAA then zeros", got)
	}
}

func TestExtentCommitTwoBlocks(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/logs/big.jsonl"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatalf("create: %v", err)
	}
	first := []byte("hello-ext")
	second := []byte("ent-tail")
	payload := append(append([]byte{}, first...), second...)
	sum1 := sha256.Sum256(first)
	sum2 := sha256.Sum256(second)
	hex1 := hex.EncodeToString(sum1[:])
	hex2 := hex.EncodeToString(sum2[:])
	blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{
		{FileOff: 0, Len: int64(len(first)), ChecksumSHA256: hex1},
		{FileOff: int64(len(first)), Len: int64(len(second)), ChecksumSHA256: hex2},
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("blocks=%d", len(blocks))
	}
	for i, chunk := range [][]byte{first, second} {
		encOpts, _, _ := b.s3WriteEncryption(blocks[i].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[i].BlockKey, bytes.NewReader(chunk), int64(len(chunk)), encOpts); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	result, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision:   1,
		ExpectedGeneration: 0,
		OpID:               "op-two",
		Ops: []datastore.SliceOp{
			{FileOff: 0, Len: int64(len(first)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hex1},
			{FileOff: int64(len(first)), Len: int64(len(second)), BlockKey: blocks[1].BlockKey, ChecksumSHA256: hex2},
		},
	})
	if err != nil {
		t.Fatalf("commit two blocks: %v", err)
	}
	if result.SizeBytes != int64(len(payload)) {
		t.Fatalf("size=%d want %d", result.SizeBytes, len(payload))
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, result.SizeBytes, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Fatalf("assembled %q want %q", buf.Bytes(), payload)
	}
}

func TestSetContentLayoutEmptyFile(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := context.Background()
	if err := b.CreateCtx(ctx, "/empty.txt"); err != nil {
		t.Fatal(err)
	}
	if err := b.SetContentLayout(ctx, "/empty.txt", datastore.ContentLayoutExtent); err != nil {
		t.Fatalf("setattr: %v", err)
	}
	nf, err := b.store.Stat(ctx, "/empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !nf.File.IsExtent() {
		t.Fatalf("layout=%q", nf.File.Layout())
	}
}

func TestIngestExtentOverwriteAndTruncate(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/logs/events.jsonl"
	first := []byte("hello-jsonl")
	if _, _, err := b.IngestExtentBytes(ctx, path, first); err != nil {
		t.Fatalf("ingest create: %v", err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if nf.File.SizeBytes != int64(len(first)) {
		t.Fatalf("size=%d want %d", nf.File.SizeBytes, len(first))
	}
	second := []byte("xy")
	if _, _, err := b.IngestExtentBytes(ctx, path, second); err != nil {
		t.Fatalf("ingest overwrite: %v", err)
	}
	nf, err = b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if nf.File.SizeBytes != int64(len(second)) {
		t.Fatalf("overwrite size=%d want %d", nf.File.SizeBytes, len(second))
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, nf.File.SizeBytes, &buf); err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), second) {
		t.Fatalf("assembled %q want %q", buf.Bytes(), second)
	}
}

func TestCommitSlicesGrowTruncatesSizeWithoutHoleRows(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/wal/grow.db"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatal(err)
	}
	payload := []byte("abc")
	sum := sha256.Sum256(payload)
	hexSum := hex.EncodeToString(sum[:])
	blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
		FileOff: 0, Len: int64(len(payload)), ChecksumSHA256: hexSum,
	}})
	if err != nil {
		t.Fatal(err)
	}
	encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
	if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(payload), int64(len(payload)), encOpts); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: 1, ExpectedGeneration: 0, OpID: "op-data",
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	to := int64(1 << 20)
	result, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: nf.File.Revision, ExpectedGeneration: nf.File.SliceGeneration,
		OpID: "op-grow", TruncateTo: &to,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SizeBytes != to {
		t.Fatalf("size=%d want %d", result.SizeBytes, to)
	}
	after, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) < len(before) {
		t.Fatalf("grow must keep data rows: before=%d after=%d", len(before), len(after))
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, to, &buf); err != nil {
		t.Fatal(err)
	}
	if int64(buf.Len()) != to {
		t.Fatalf("assembled len=%d want %d", buf.Len(), to)
	}
	if !bytes.Equal(buf.Bytes()[:len(payload)], payload) {
		t.Fatalf("prefix=%q", buf.Bytes()[:len(payload)])
	}
	if buf.Bytes()[to-1] != 0 {
		t.Fatal("grown tail must read as zero")
	}
}

func TestCompactSlicesSwapsRowsWithoutBumpRevision(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/wal/compact.db-wal"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaa")
	c := []byte("cccc")
	sumA := sha256.Sum256(a)
	sumC := sha256.Sum256(c)
	hexA := hex.EncodeToString(sumA[:])
	hexC := hex.EncodeToString(sumC[:])
	blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{
		{FileOff: 0, Len: 4, ChecksumSHA256: hexA},
		{FileOff: 4, Len: 4, ChecksumSHA256: hexC},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, payload := range [][]byte{a, c} {
		encOpts, _, _ := b.s3WriteEncryption(blocks[i].BlockKey)
		if err := b.s3.PutObject(ctx, blocks[i].BlockKey, bytes.NewReader(payload), 4, encOpts); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: 1, ExpectedGeneration: 0, OpID: "op-c1",
		Ops: []datastore.SliceOp{
			{FileOff: 0, Len: 4, BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexA},
			{FileOff: 4, Len: 4, BlockKey: blocks[1].BlockKey, ChecksumSHA256: hexC},
		},
	}); err != nil {
		t.Fatal(err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	merged := append(append([]byte{}, a...), c...)
	sumM := sha256.Sum256(merged)
	hexM := hex.EncodeToString(sumM[:])
	prep, err := b.PrepareBlocks(ctx, path, []PrepareRange{{FileOff: 0, Len: 8, ChecksumSHA256: hexM}})
	if err != nil {
		t.Fatal(err)
	}
	encOpts, _, _ := b.s3WriteEncryption(prep[0].BlockKey)
	if err := b.s3.PutObject(ctx, prep[0].BlockKey, bytes.NewReader(merged), 8, encOpts); err != nil {
		t.Fatal(err)
	}
	rows, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CompactSlices(ctx, path, CompactSlicesRequest{
		Chunk:    0,
		Snapshot: rows,
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: 8, BlockKey: prep[0].BlockKey, ChecksumSHA256: hexM,
		}},
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	nf2, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if nf2.File.Revision != nf.File.Revision {
		t.Fatalf("revision changed %d -> %d (JuiceFS compact does not change length/rev)", nf.File.Revision, nf2.File.Revision)
	}
	if nf2.File.SliceGeneration <= nf.File.SliceGeneration {
		t.Fatalf("slice_generation %d -> %d, JuiceFS InvalidateChunk needs a new layout token", nf.File.SliceGeneration, nf2.File.SliceGeneration)
	}
	after, err := b.store.ListSlicesForInode(ctx, nf.File.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0].BlockKey != prep[0].BlockKey || after[0].Len != 8 {
		t.Fatalf("after compact rows=%+v", after)
	}
}

func TestExtentWriteRejectsTagsAndDescription(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/wal/meta.db"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatal(err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	flags := filesystem.WriteFlagCreate | filesystem.WriteFlagTruncate
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, path, []byte("hi"), 0, flags, nf.File.Revision, map[string]string{"k": "v"}, ""); !errors.Is(err, ErrExtentExtraMetadata) {
		t.Fatalf("tags err=%v, want ErrExtentExtraMetadata", err)
	}
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, path, []byte("hi"), 0, flags, nf.File.Revision, nil, "desc"); !errors.Is(err, ErrExtentExtraMetadata) {
		t.Fatalf("description err=%v, want ErrExtentExtraMetadata", err)
	}
}

func TestStagedCommitMissingPendingRequiresObject(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/wal/staged.db"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatal(err)
	}
	payload := []byte("staged-block")
	sum := sha256.Sum256(payload)
	hexSum := hex.EncodeToString(sum[:])
	blocks, err := b.PrepareBlocks(ctx, path, []PrepareRange{{
		FileOff: 0, Len: int64(len(payload)), ChecksumSHA256: hexSum,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.DeletePendingBlock(ctx, blocks[0].BlockKey); err != nil {
		t.Fatal(err)
	}
	_, err = b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: 1, ExpectedGeneration: 0, OpID: "op-missing",
		Staged: true,
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
		}},
	})
	if !errors.Is(err, datastore.ErrPendingExpired) {
		t.Fatalf("missing object staged commit err=%v, want ErrPendingExpired", err)
	}

	encOpts, _, _ := b.s3WriteEncryption(blocks[0].BlockKey)
	if err := b.s3.PutObject(ctx, blocks[0].BlockKey, bytes.NewReader(payload), int64(len(payload)), encOpts); err != nil {
		t.Fatal(err)
	}
	result, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: 1, ExpectedGeneration: 0, OpID: "op-missing",
		Staged: true,
		Ops: []datastore.SliceOp{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: blocks[0].BlockKey, ChecksumSHA256: hexSum,
		}},
	})
	if err != nil {
		t.Fatalf("staged commit after PUT: %v", err)
	}
	if result.SizeBytes != int64(len(payload)) {
		t.Fatalf("size=%d want %d", result.SizeBytes, len(payload))
	}
}

func TestAssembleExtentSparseHoleStreamsZeros(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{})
	ctx := extentCreateCtx()
	path := "/wal/sparse.db"
	if err := b.CreateCtx(ctx, path); err != nil {
		t.Fatal(err)
	}
	hole := int64(2 << 20)
	result, err := b.CommitSlices(ctx, path, CommitSlicesRequest{
		ExpectedRevision: 1, ExpectedGeneration: 0, OpID: "op-hole",
		TruncateTo: &hole,
		Ops:        datastore.SplitHoleRange(0, hole),
	})
	if err != nil {
		t.Fatal(err)
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := b.AssembleExtent(ctx, nf.File.FileID, 0, result.SizeBytes, &buf); err != nil {
		t.Fatal(err)
	}
	if int64(buf.Len()) != hole {
		t.Fatalf("assembled len=%d want %d", buf.Len(), hole)
	}
	if buf.Bytes()[0] != 0 || buf.Bytes()[hole-1] != 0 {
		t.Fatal("hole bytes must be zero")
	}
}
