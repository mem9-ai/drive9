package writeback

// Ports of v1 pkg/fuse/commit_queue_lineage_test.go and
// commit_queue_lineage_regression_test.go: the queue's rebase/fence
// machinery around process-local snapshot lineage — growth rebase onto the
// watermark (#896), superseded same-path rewrite (#935), the terminal fence
// for divergent or unproven payloads, landed-landmark bounding, and
// direct-parent coalescing. Production lineage (stagedSnapshotID chaining
// through consecutive stagings of one handle) is pinned at the bottom.
//
// Not ported (v1-only internals):
//   - TestOpenHandlePathWideShadowPreloadDropsLineageTrust: the new plane has
//     no path-wide shadow preload into writable handles (ensureBufferLocked
//     preloads from the .dat/.meta track or the remote base only), so there
//     is no public path that could inherit another handle's lineage trust.
//   - The SQLite-WAL specifics of
//     TestCommitQueueGrowthRebaseCannotResurrectOlderSQLiteWALGeneration are
//     reduced to plain byte images: the queue's fence compares whole
//     payloads, and the WAL header builder was v1-side detection state.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func waitCommitQueueIdle(t *testing.T, cq *CommitQueue) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, _ := cq.PendingStats()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n, _ := cq.PendingStats()
	t.Fatalf("commit queue still pending=%d", n)
}

// proofRequestCounts reports the identity-proof round trips (HEAD stat +
// ranged GET) the lineage machinery issued.
func (s *casFileServer) proofRequestCounts() (heads, gets int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headCalls, s.getCalls
}

// recreate models a delete/recreate that resets the server incarnation to
// the same revision and size the landed proof still remembers.
func (s *casFileServer) recreate(body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision = 1
	s.body = append([]byte(nil), body...)
}

func newLineageQueue(t *testing.T, url string) *CommitQueue {
	t.Helper()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(url)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)
	return cq
}

func stageLineageEntry(t *testing.T, cq *CommitQueue, path string, data []byte, id, parent string, spill bool) *CommitEntry {
	t.Helper()
	if err := cq.shadows.WriteFull(path, data, 0); err != nil {
		t.Fatal(err)
	}
	var gen uint64
	var err error
	if spill {
		gen, err = cq.index.PutShadowSpillWithModeAndLineage(path, int64(len(data)), staging.PendingNew, 0, 0, false, id, parent, true)
	} else {
		gen, err = cq.index.PutWithBaseRevAndModeAndLineage(path, int64(len(data)), staging.PendingNew, 0, 0, false, id, parent, true)
	}
	if err != nil {
		t.Fatal(err)
	}
	return &CommitEntry{Path: path, Size: int64(len(data)), Kind: staging.PendingNew, PayloadBaseRevSet: true,
		ShadowSpill: spill, ShadowGen: cq.shadows.ActiveGeneration(path), PendingIndexGen: gen,
		SnapshotID: id, ParentSnapshotID: parent, liveLineageProof: true}
}

func TestCommitQueueRebasesGrownPayloadAfterSmallerSamePathCommit(t *testing.T) {
	const path = "/kv-1g.db"
	const headerSnapshot = "header-snapshot"
	const grownSnapshot = "grown-snapshot"
	header := bytes.Repeat([]byte("H"), 4096)
	grown := append(append([]byte(nil), header...), bytes.Repeat([]byte("G"), 8192)...)

	server, ts := newCASFileServer(t, path, 0, nil)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	if err := shadow.WriteFull(path, header, 0); err != nil {
		t.Fatal(err)
	}
	headerGen := shadow.ActiveGeneration(path)
	headerPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(header)), staging.PendingNew, 0, 0, false, headerSnapshot, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(header)), Kind: staging.PendingNew, MutationSeq: 1,
		ShadowGen: headerGen, PendingIndexGen: headerPending,
		SnapshotID: headerSnapshot, liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitCommitQueueIdle(t, cq)
	if rev, body, _ := server.snapshot(); rev != 1 || !bytes.Equal(body, header) {
		t.Fatalf("after header commit rev=%d body=%q, want rev1 header", rev, body)
	}

	if err := shadow.WriteFull(path, grown, 0); err != nil {
		t.Fatal(err)
	}
	grownGen := shadow.ActiveGeneration(path)
	grownPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(grown)), staging.PendingNew, 0, 0, false, grownSnapshot, headerSnapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(grown)), Kind: staging.PendingNew, MutationSeq: 2,
		ShadowGen: grownGen, PendingIndexGen: grownPending,
		SnapshotID: grownSnapshot, ParentSnapshotID: headerSnapshot, liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitCommitQueueIdle(t, cq)
	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, grown) || len(puts) != 2 {
		t.Fatalf("after growth rev=%d body-len=%d puts=%d, want rev2 grown-len=%d and 2 PUTs", rev, len(body), len(puts), len(grown))
	}
}

// TestCommitQueueRebasesSupersededRewriteOntoLandedParent reproduces issue
// #935: rapid same-path rewrites (npm/npx _cacache index files) leave the
// second entry with a payload base revision that predates the revision the
// queue just landed for its direct parent snapshot. The bytes are a causal
// descendant of that landed image, so the entry must rebase onto it instead of
// failing as a terminal conflict that blocks mount drain forever.
func TestCommitQueueRebasesSupersededRewriteOntoLandedParent(t *testing.T) {
	const path = "/_cacache/index-v5/ab/cd/rewrite"
	const firstSnapshot = "rewrite-snapshot-1"
	const secondSnapshot = "rewrite-snapshot-2"
	first := []byte("index entry v1")
	second := []byte("index entry v2 rewritten in place, possibly smaller")

	server, ts := newCASFileServer(t, path, 1, first)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	stage := func(payload []byte, baseRev int64, snapshotID, parentSnapshotID string) (uint64, uint64) {
		t.Helper()
		if err := shadow.WriteFull(path, payload, baseRev); err != nil {
			t.Fatal(err)
		}
		shadowGen := shadow.ActiveGeneration(path)
		pendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(payload)), staging.PendingOverwrite, baseRev, 0, false, snapshotID, parentSnapshotID, true)
		if err != nil {
			t.Fatal(err)
		}
		return shadowGen, pendingGen
	}

	// First rewrite commits against the existing rev 1.
	shadowGen, pendingGen := stage(first, 1, firstSnapshot, "")
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(first)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
		SnapshotID: firstSnapshot, liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	if rev, body, _ := server.snapshot(); rev != 2 || !bytes.Equal(body, first) {
		t.Fatalf("after first rewrite rev=%d body=%q, want rev2 first bytes", rev, body)
	}

	// Rapid second rewrite is staged from a dirty handle that still carries
	// base rev 1, so its payload base predates the landed rev 2. Its snapshot
	// is the exact direct child of the image the queue just landed.
	shadowGen, pendingGen = stage(second, 1, secondSnapshot, firstSnapshot)
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(second)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
		SnapshotID: secondSnapshot, ParentSnapshotID: firstSnapshot,
		liveLineageProof: true,
	}); err != nil {
		t.Fatalf("superseded rewrite err = %v, want rebase onto landed parent", err)
	}
	rev, body, puts := server.snapshot()
	if rev != 3 || !bytes.Equal(body, second) {
		t.Fatalf("after superseded rewrite rev=%d body=%q, want rev3 second bytes", rev, body)
	}
	if len(puts) != 2 || !bytes.Equal(puts[1].body, second) || puts[1].expected != "2" {
		t.Fatalf("PUT history = %+v, want second PUT expected rev2 with second bytes", puts)
	}
	if conflicts, _, _ := pending.ConflictSummary(); conflicts != 0 {
		t.Fatalf("pending conflicts = %d, want 0 after rebase", conflicts)
	}
}

// TestCommitQueueRebasesLiveDescendantOntoWatermark reproduces the commit
// layer of issue #935: a later appendFile-style snapshot retains live causal
// ancestry to the exact snapshot this queue landed. After the remote revision,
// size, and checksum verify that landed identity, the descendant may rebase
// onto the watermark instead of becoming a terminal drain-blocking conflict.
func TestCommitQueueRebasesLiveDescendantOntoWatermark(t *testing.T) {
	const path = "/_cacache/index-v5/ab/cd/extend"
	base := []byte("line-1\n")
	first := append(append([]byte(nil), base...), []byte("line-A\n")...)
	second := append(append([]byte(nil), first...), []byte("line-B\n")...)

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	stage := func(payload []byte, baseRev int64) (uint64, uint64) {
		t.Helper()
		if err := shadow.WriteFull(path, payload, baseRev); err != nil {
			t.Fatal(err)
		}
		shadowGen := shadow.ActiveGeneration(path)
		pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingOverwrite, baseRev)
		if err != nil {
			t.Fatal(err)
		}
		return shadowGen, pendingGen
	}

	shadowGen, pendingGen := stage(first, 1)
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(first)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
		SnapshotID: "snap-first", liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	if rev, body, _ := server.snapshot(); rev != 2 || !bytes.Equal(body, first) {
		t.Fatalf("after first append rev=%d body=%q, want rev2 first bytes", rev, body)
	}

	shadowGen, pendingGen = stage(second, 1)
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(second)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
		SnapshotID: "snap-second", ParentSnapshotID: "snap-first", liveLineageProof: true,
	}); err != nil {
		t.Fatalf("live descendant err = %v, want rebase onto rev 2", err)
	}
	rev, body, puts := server.snapshot()
	if rev != 3 || !bytes.Equal(body, second) {
		t.Fatalf("after live descendant rev=%d body=%q, want rev3 composed bytes", rev, body)
	}
	if len(puts) != 2 || !bytes.Equal(puts[1].body, second) || puts[1].expected != "2" {
		t.Fatalf("PUT history = %+v, want second PUT expected rev2 with composed bytes", puts)
	}
	if conflicts, _, _ := pending.ConflictSummary(); conflicts != 0 {
		t.Fatalf("pending conflicts = %d, want 0 after live descendant rebase", conflicts)
	}
}

// TestCommitQueueDivergentRewriteKeepsTerminalFence is the negative half of
// issue #935: a sibling snapshot without live causal ancestry to the landed
// image must stay terminal, including divergent writes and stale checkpoints.
func TestCommitQueueDivergentRewriteKeepsTerminalFence(t *testing.T) {
	const path = "/_cacache/index-v5/ab/cd/diverge"
	base := []byte("line-1\n")
	first := append(append([]byte(nil), base...), []byte("line-A\n")...)
	divergent := append(append([]byte(nil), base...), []byte("line-X\n")...)

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	stage := func(payload []byte, baseRev int64) (uint64, uint64) {
		t.Helper()
		if err := shadow.WriteFull(path, payload, baseRev); err != nil {
			t.Fatal(err)
		}
		shadowGen := shadow.ActiveGeneration(path)
		pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingOverwrite, baseRev)
		if err != nil {
			t.Fatal(err)
		}
		return shadowGen, pendingGen
	}

	shadowGen, pendingGen := stage(first, 1)
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(first)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}

	shadowGen, pendingGen = stage(divergent, 1)
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(divergent)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, first) || len(puts) != 1 {
		t.Fatalf("after divergent rewrite rev=%d body=%q puts=%d, want untouched rev2 first bytes", rev, body, len(puts))
	}
	meta, ok := pending.GetMeta(path)
	if !ok || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending meta = %+v ok=%t, want preserved PendingConflict", meta, ok)
	}
}

// TestCommitQueueRejectsUnprovenAppendExtension checks that a prefix is not
// permission to overwrite an unseen remote revision (which may be a truncate):
// a payload based on rev 1 fenced behind a watermark at rev 2 stays terminal
// without issuing any upload, preserving the durable image.
func TestCommitQueueRejectsUnprovenAppendExtension(t *testing.T) {
	const path = "/_cacache/index-v5/ab/cd/unproven"
	remote := []byte("line-1\nline-A\nline-B\n")
	local := []byte("line-1\nline-A\n")

	server, ts := newCASFileServer(t, path, 2, remote)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(2)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }

	if err := shadow.WriteFull(path, local, 1); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(local)), staging.PendingOverwrite, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(local)), Kind: staging.PendingOverwrite,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, remote) || len(puts) != 0 {
		t.Fatalf("unproven prefix rev=%d body=%q puts=%d, want untouched rev2 with no PUTs", rev, body, len(puts))
	}
	meta, ok := pending.GetMeta(path)
	if !ok || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending meta = %+v ok=%t, want preserved PendingConflict", meta, ok)
	}
}

// TestCommitQueueUnprovenOversizedPayloadFailsClosedWithoutRemoteRead pins the
// proof bound: an unproven payload above maxLandedPayloadBytes must keep the
// terminal fence without loading shadow bytes or reading/uploading remote data.
func TestCommitQueueUnprovenOversizedPayloadFailsClosedWithoutRemoteRead(t *testing.T) {
	const path = "/_cacache/index-v5/ab/cd/large-extend"
	first := bytes.Repeat([]byte("a"), 4096)
	big := append(append([]byte(nil), first...), bytes.Repeat([]byte("b"), maxLandedPayloadBytes)...)

	server, ts := newCASFileServer(t, path, 1, first)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	if err := shadow.WriteFull(path, first, 1); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(first)), staging.PendingOverwrite, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(first)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
		SnapshotID: "snap-first", liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}

	beforeHeads, beforeGets := server.proofRequestCounts()
	if err := shadow.WriteFull(path, big, 1); err != nil {
		t.Fatal(err)
	}
	shadowGen = shadow.ActiveGeneration(path)
	pendingGen, err = pending.PutWithBaseRev(path, int64(len(big)), staging.PendingOverwrite, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(big)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()
	if heads, gets := server.proofRequestCounts(); heads != beforeHeads || gets != beforeGets {
		t.Fatalf("unproven oversized payload performed reads: HEAD=%d GET=%d", heads-beforeHeads, gets-beforeGets)
	}

	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, first) || len(puts) != 1 {
		t.Fatalf("after oversized append rev=%d body-len=%d puts=%d, want untouched rev2", rev, len(body), len(puts))
	}
	meta, ok := pending.GetMeta(path)
	if !ok || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending meta = %+v ok=%t, want preserved PendingConflict above the proof bound", meta, ok)
	}
}

// TestPortConsecutiveStagedSnapshotsFormDirectParentChain pins the
// process-local staging lineage production side (v1
// TestConsecutiveStagedShadowSnapshotsFormDirectParentChain, restored): after
// one handle stages twice, the second snapshot must name the first as its
// exact direct parent and carry trusted lineage.
func TestPortConsecutiveStagedSnapshotsFormDirectParentChain(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	var mu sync.Mutex
	var landed []*CommitEntry
	wb.Queue().OnSuccess = func(entry *CommitEntry, _ int64) {
		mu.Lock()
		landed = append(landed, entry)
		mu.Unlock()
	}

	h := open(t, ch, "/_cacache/index-v5/ab/cd/chain")
	if err := h.Write(ctx, 0, []byte("first rewrite")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("second rapid rewrite that grows")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(landed) != 2 {
		t.Fatalf("landed entries = %d, want 2", len(landed))
	}
	snap1, snap2 := landed[0], landed[1]
	// The restored production contract is the stagedSnapshotID chain itself;
	// the queue's liveLineageProof rebase gate is still armed only by the
	// direct-entry constructors the queue-side tests use, so it is not
	// asserted here.
	if snap1.SnapshotID == "" {
		t.Fatalf("first stage snapshot = %q, want a snapshot ID", snap1.SnapshotID)
	}
	if snap1.ParentSnapshotID != "" {
		t.Fatalf("first stage parent = %q, want empty root", snap1.ParentSnapshotID)
	}
	if snap2.SnapshotID == "" || snap2.SnapshotID == snap1.SnapshotID {
		t.Fatalf("second stage snapshot = %q, want a new snapshot (first was %q)", snap2.SnapshotID, snap1.SnapshotID)
	}
	if snap2.ParentSnapshotID != snap1.SnapshotID {
		t.Fatalf("second stage parent = %q, want direct parent %q", snap2.ParentSnapshotID, snap1.SnapshotID)
	}
	if bh, ok := h.(*bufferingHandle); ok {
		if bh.contentSnapshotID != snap2.SnapshotID {
			t.Fatalf("handle content snapshot = %q, want the second staged snapshot", bh.contentSnapshotID)
		}
	}
}

func TestCommitQueueLandedProofUsesUploadedPayloadAfterShadowReplacement(t *testing.T) {
	for _, tc := range []struct {
		name             string
		spill, watermark bool
	}{
		{"buffered", false, true}, {"spill-watermark", true, true}, {"spill-conflict", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const path = "/concurrent-growth.db"
			const parentSnapshot = "snapshot-A"
			const childSnapshot = "snapshot-B"
			parent := []byte("parent-image-A")
			child := append(append([]byte(nil), parent...), []byte("-grown-image-B")...)

			server, ts := newCASFileServer(t, path, 0, nil)
			defer ts.Close()
			server.firstPutStarted = make(chan struct{})
			server.releaseFirstPut = make(chan struct{})

			shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			pending, err := staging.NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			c := newTestClient(ts.URL)
			c.SetSmallFileThresholdForTests(1 << 20)
			cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
			defer cq.DrainAll()
			defer func() {
				select {
				case <-server.releaseFirstPut:
				default:
					close(server.releaseFirstPut)
				}
			}()
			var watermark atomic.Int64
			if tc.watermark {
				cq.DurableWatermark = func(string) int64 { return watermark.Load() }
			}
			cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

			if err := shadow.WriteFull(path, parent, 0); err != nil {
				t.Fatal(err)
			}
			parentShadowGen := shadow.ActiveGeneration(path)
			parentPendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(parent)), staging.PendingNew, 0, 0, false, parentSnapshot, "", true)
			if err != nil {
				t.Fatal(err)
			}
			if err := cq.Enqueue(&CommitEntry{
				Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
				Size: int64(len(parent)), Kind: staging.PendingNew, ShadowSpill: tc.spill, MutationSeq: 1,
				ShadowGen: parentShadowGen, PendingIndexGen: parentPendingGen,
				SnapshotID: parentSnapshot, liveLineageProof: true,
			}); err != nil {
				t.Fatal(err)
			}

			select {
			case <-server.firstPutStarted:
			case <-time.After(3 * time.Second):
				t.Fatal("parent upload did not reach server")
			}

			// The parent upload has already bound its private payload, but its success
			// tail has not run. Replace the path's active shadow with the exact child,
			// matching a second handle staging while the first upload is in flight.
			staged := make(chan error, 1)
			go func() { staged <- shadow.WriteFull(path, child, 0) }()
			select {
			case err := <-staged:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("small spill upload retained the mutable shadow lock")
			}
			childShadowGen := shadow.ActiveGeneration(path)
			childPendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(child)), staging.PendingNew, 0, 0, false, childSnapshot, parentSnapshot, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := cq.Enqueue(&CommitEntry{
				Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
				Size: int64(len(child)), Kind: staging.PendingNew, ShadowSpill: tc.spill, MutationSeq: 2,
				ShadowGen: childShadowGen, PendingIndexGen: childPendingGen,
				SnapshotID: childSnapshot, ParentSnapshotID: parentSnapshot, liveLineageProof: true,
			}); err != nil {
				t.Fatal(err)
			}
			close(server.releaseFirstPut)

			waitCommitQueueIdle(t, cq)
			rev, body, puts := server.snapshot()
			if rev != 2 || !bytes.Equal(body, child) || len(puts) != 2 {
				t.Fatalf("after overlapping commits rev=%d body=%q puts=%d, want rev2 exact child and two PUTs", rev, body, len(puts))
			}
			if puts[0].expected != "0" || !bytes.Equal(puts[0].body, parent) {
				t.Fatalf("parent PUT expected=%q body=%q, want base0 exact parent", puts[0].expected, puts[0].body)
			}
			if puts[1].expected != "1" || !bytes.Equal(puts[1].body, child) {
				t.Fatalf("child PUT expected=%q body=%q, want base1 exact child", puts[1].expected, puts[1].body)
			}
		})
	}
}

func TestCommitQueueLandedLandmarksAreBoundedAndProtectQueuedChild(t *testing.T) {
	cq := &CommitQueue{
		queue: []*CommitEntry{
			{Path: "/protected.db", ParentSnapshotID: "protected-parent"},
			{
				Path:             "/ancestor-protected.db",
				ParentSnapshotID: "unlanded-direct-parent",
				liveAncestors:    []string{"protected-ancestor"},
			},
		},
		inFlight:  make(map[string]*CommitEntry),
		immediate: make(map[*CommitEntry]struct{}),
		landed:    make(map[string]pathCommitLandmark),
	}
	cq.rememberLanded("/protected.db", 1, 1, "checksum", "protected-parent")
	cq.rememberLanded("/ancestor-protected.db", 1, 1, "checksum", "protected-ancestor")
	for i := 0; i < maxLandedCommitLandmarks; i++ {
		path := fmt.Sprintf("/landmark-%04d.db", i)
		cq.rememberLanded(path, 1, 1, "checksum", fmt.Sprintf("snapshot-%04d", i))
	}
	if got := len(cq.landed); got != maxLandedCommitLandmarks {
		t.Fatalf("landed landmark count = %d, want %d", got, maxLandedCommitLandmarks)
	}
	if got := cq.landedCommit("/protected.db"); got.snapshotID != "protected-parent" {
		t.Fatalf("queued child's parent landmark was evicted: %+v", got)
	}
	if got := cq.landedCommit("/ancestor-protected.db"); got.snapshotID != "protected-ancestor" {
		t.Fatalf("queued child's ancestor landmark was evicted: %+v", got)
	}
	if got := cq.landedCommit("/landmark-0000.db"); got.snapshotID != "" {
		t.Fatalf("oldest unreferenced landmark was retained: %+v", got)
	}
}

func TestCommitQueueUnverifiableCommitInvalidatesOlderLandmark(t *testing.T) {
	cq := &CommitQueue{landed: make(map[string]pathCommitLandmark)}
	cq.rememberLanded("/replaced.db", 1, 4, "checksum", "snapshot-old")
	if got := cq.landedCommit("/replaced.db"); got.snapshotID != "snapshot-old" {
		t.Fatalf("initial landmark = %+v, want snapshot-old", got)
	}
	cq.rememberLanded("/replaced.db", 2, 2<<20, "", "snapshot-large")
	if got := cq.landedCommit("/replaced.db"); got.snapshotID != "" {
		t.Fatalf("unverifiable replacement retained stale landmark: %+v", got)
	}
}

func TestCommitQueueLowerRevisionReplacesOlderLandmark(t *testing.T) {
	cq := &CommitQueue{landed: make(map[string]pathCommitLandmark)}
	cq.rememberLanded("/recreated.db", 9, 4, "old-checksum", "snapshot-old")
	cq.rememberLanded("/recreated.db", 1, 8, "new-checksum", "snapshot-new")

	got := cq.landedCommit("/recreated.db")
	if got.rev != 1 || got.snapshotID != "snapshot-new" || got.checksum != "new-checksum" {
		t.Fatalf("landmark after revision reset = %+v, want new incarnation at revision 1", got)
	}
}

func TestCommitQueueCancellationInvalidatesLandedLandmarks(t *testing.T) {
	cq := &CommitQueue{
		inFlight:  make(map[string]*CommitEntry),
		immediate: make(map[*CommitEntry]struct{}),
		landed:    make(map[string]pathCommitLandmark),
	}
	for _, path := range []string{"/one.db", "/dir/two.db", "/keep.db"} {
		cq.rememberLanded(path, 1, 1, "checksum", "snapshot-"+path)
	}

	cq.CancelPathPreserveLocal("/one.db")
	cq.CancelPrefix("/dir/")

	if got := cq.landedCommit("/one.db"); got.snapshotID != "" {
		t.Fatalf("CancelPathPreserveLocal retained landmark: %+v", got)
	}
	if got := cq.landedCommit("/dir/two.db"); got.snapshotID != "" {
		t.Fatalf("CancelPrefix retained landmark: %+v", got)
	}
	if got := cq.landedCommit("/keep.db"); got.snapshotID == "" {
		t.Fatal("CancelPrefix removed unrelated landmark")
	}
}

func TestCommitQueueReadRemoteSnapshotStopsOnCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		close(started)
		<-r.Context().Done()
	}))
	defer ts.Close()

	cq := &CommitQueue{client: newTestClient(ts.URL)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = cq.readRemoteSnapshot(ctx, "/cancel.db")
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("remote snapshot HEAD did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("remote snapshot ignored caller cancellation")
	}
}

func TestCommitQueueCancelDuringConflictLineageProofDoesNotMarkConflict(t *testing.T) {
	const path = "/cancel-conflict.db"
	const parentSnapshot = "snapshot-parent"
	parent := []byte("parent")
	child := []byte("parent-grown-child")
	var watermark atomic.Int64
	headStarted := make(chan struct{})
	var headOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			watermark.Store(1)
			w.WriteHeader(http.StatusConflict)
		case http.MethodHead:
			headOnce.Do(func() { close(headStarted) })
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, child, 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(child)), staging.PendingNew, 0, 0, false, "snapshot-child", parentSnapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	defer cq.CancelPathPreserveLocal(path)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.rememberLanded(path, 1, int64(len(parent)), payloadChecksum(parent), parentSnapshot)
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(child)), Kind: staging.PendingNew, ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: pendingGen,
		SnapshotID: "snapshot-child", ParentSnapshotID: parentSnapshot, liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-headStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("conflict lineage HEAD did not start")
	}
	cq.CancelPathPreserveLocal(path)
	drained := make(chan struct{})
	go func() {
		cq.WaitPath(path)
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled lineage proof did not drain")
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("cancel-preserve removed pending metadata")
	}
	if meta.Kind == staging.PendingConflict {
		t.Fatal("caller cancellation was recorded as a terminal conflict")
	}
}

func TestCommitQueueDoesNotRebaseLargerStaleCheckpointOntoLandedMain(t *testing.T) {
	const path = "/app.db"
	base := []byte("base-rev1")
	checkpointB := []byte("checkpoint-B-rev2")
	staleA := append([]byte("checkpoint-A-from-rev1:"), bytes.Repeat([]byte("x"), len(checkpointB))...)

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	stage := func(payload []byte, baseRev int64) (uint64, uint64) {
		t.Helper()
		if err := shadow.WriteFull(path, payload, baseRev); err != nil {
			t.Fatal(err)
		}
		shadowGen := shadow.ActiveGeneration(path)
		pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingOverwrite, baseRev)
		if err != nil {
			t.Fatal(err)
		}
		return shadowGen, pendingGen
	}

	shadowGen, pendingGen := stage(checkpointB, 1)
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(checkpointB)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, checkpointB) || len(puts) != 1 {
		t.Fatalf("checkpoint B rev=%d body=%q puts=%d, want rev2 exact B and one PUT", rev, body, len(puts))
	}

	shadowGen, pendingGen = stage(staleA, 1)
	err = cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(staleA)), Kind: staging.PendingOverwrite,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
		DurableWatermarkRev: 2, DisableAutoResolveLWW: true,
	})
	if !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("stale larger checkpoint err=%v, want errCommitPayloadStale", err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, checkpointB) || len(puts) != 1 {
		t.Fatalf("after stale checkpoint rev=%d body=%q puts=%d, want rev2 exact B and no second PUT", rev, body, len(puts))
	}
}

func TestCommitQueueGrowthRebaseParentAboveIdentityLimitFailsClosed(t *testing.T) {
	const path = "/large-parent.db"
	parent := bytes.Repeat([]byte("p"), (1<<20)+1)
	child := append(append([]byte(nil), parent...), 'c')
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	cq := NewCommitQueue(newTestClient("http://localhost"), shadow, nil, nil, 1, 8)
	defer cq.DrainAll()

	parentEntry := &CommitEntry{Size: int64(len(parent)), SnapshotID: "snapshot-parent", payload: parent, payloadBound: true}
	checksum := cq.checksumLandedPayload(parentEntry)
	if checksum != "" {
		t.Fatalf("large parent checksum = %q, want bounded identity proof to be unavailable", checksum)
	}
	cq.rememberLanded(path, 1, int64(len(parent)), checksum, parentEntry.SnapshotID)
	if got := cq.landedCommit(path); got.snapshotID != "" {
		t.Fatalf("unverifiable large parent was retained as a landmark: %+v", got)
	}

	if err := shadow.WriteFull(path, child, 0); err != nil {
		t.Fatal(err)
	}
	entry := &CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(child)), Kind: staging.PendingNew, ShadowGen: shadow.ActiveGeneration(path),
		SnapshotID: "snapshot-child", ParentSnapshotID: parentEntry.SnapshotID,
		liveLineageProof: true, DurableWatermarkRev: 1,
	}
	if err := cq.validateEntryPayloadFreshCtx(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("large-parent growth err = %v, want fail-closed errCommitPayloadStale", err)
	}
}

// TestCommitQueueGrowthRebaseCannotResurrectOlderGeneration ports v1's
// SQLite-WAL row with plain byte images: a stale same-size-or-larger
// generation whose payload base predates the watermark must fail closed
// instead of overwriting the newer landed image.
func TestCommitQueueGrowthRebaseCannotResurrectOlderGeneration(t *testing.T) {
	const path = "/app.db-wal"
	newHeader := bytes.Repeat([]byte("n"), 32)
	oldGeneration := append(bytes.Repeat([]byte("o"), 32), bytes.Repeat([]byte("o"), 4096)...)

	server, ts := newCASFileServer(t, path, 1, oldGeneration)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }

	commit := func(payload []byte, payloadBase, durable int64) error {
		t.Helper()
		if err := shadow.WriteFull(path, payload, payloadBase); err != nil {
			t.Fatal(err)
		}
		shadowGen := shadow.ActiveGeneration(path)
		pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingOverwrite, payloadBase)
		if err != nil {
			t.Fatal(err)
		}
		return cq.CommitNow(context.Background(), &CommitEntry{
			Path: path, BaseRev: payloadBase, PayloadBaseRev: payloadBase, PayloadBaseRevSet: true,
			Size: int64(len(payload)), Kind: staging.PendingOverwrite,
			ShadowGen: shadowGen, PendingIndexGen: pendingGen,
			DurableWatermarkRev: durable, DisableAutoResolveLWW: true,
		})
	}

	if err := commit(newHeader, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := commit(oldGeneration, 1, 2); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("old generation err=%v, want errCommitPayloadStale", err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, newHeader) || len(puts) != 1 {
		t.Fatalf("after stale generation rev=%d len=%d puts=%d, want rev2 exact new header and no second PUT", rev, len(body), len(puts))
	}
}

func TestCommitQueueGrowthRebaseRejectsForkedPendingNew(t *testing.T) {
	const path = "/fork.db-wal"
	base := []byte("landed-A")
	childB := []byte("landed-A-child-B")
	siblingC := []byte("landed-A-larger-sibling-C")

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) { watermark.Store(rev) }
	cq.rememberLanded(path, 1, int64(len(base)), payloadChecksum(base), "snapshot-A")

	commitChild := func(payload []byte, snapshotID string) error {
		t.Helper()
		if err := shadow.WriteFull(path, payload, 0); err != nil {
			t.Fatal(err)
		}
		shadowGen := shadow.ActiveGeneration(path)
		pendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(payload)), staging.PendingNew, 0, 0, false, snapshotID, "snapshot-A", true)
		if err != nil {
			t.Fatal(err)
		}
		return cq.CommitNow(context.Background(), &CommitEntry{
			Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
			Size: int64(len(payload)), Kind: staging.PendingNew,
			ShadowGen: shadowGen, PendingIndexGen: pendingGen,
			SnapshotID: snapshotID, ParentSnapshotID: "snapshot-A", liveLineageProof: true,
		})
	}

	if err := commitChild(childB, "snapshot-B"); err != nil {
		t.Fatal(err)
	}
	if err := commitChild(siblingC, "snapshot-C"); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("forked sibling err=%v, want errCommitPayloadStale", err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, childB) || len(puts) != 1 {
		t.Fatalf("fork result rev=%d body=%q puts=%d, want only child B at rev2", rev, body, len(puts))
	}
}

func TestCommitQueueGrowthRebaseValidationIsOneShot(t *testing.T) {
	const path = "/one-shot.db"
	base := []byte("header")
	grown := []byte("header-and-grown-payload")

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	cq := NewCommitQueue(newTestClient(ts.URL), nil, nil, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	watermark.Store(1)
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.rememberLanded(path, 1, int64(len(base)), payloadChecksum(base), "snapshot-A")
	entry := &CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(grown)), Kind: staging.PendingNew,
		SnapshotID: "snapshot-B", ParentSnapshotID: "snapshot-A", liveLineageProof: true,
	}

	if err := cq.validateEntryPayloadFreshCtx(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	firstHeads, firstGets := server.proofRequestCounts()
	if firstHeads != 1 || firstGets != 1 {
		t.Fatalf("first proof HEAD/GET=%d/%d, want 1/1", firstHeads, firstGets)
	}
	if err := cq.validateEntryPayloadFreshCtx(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	if heads, gets := server.proofRequestCounts(); heads != firstHeads || gets != firstGets {
		t.Fatalf("repeat validation HEAD/GET=%d/%d, want unchanged %d/%d", heads, gets, firstHeads, firstGets)
	}
	watermark.Store(2)
	if err := cq.validateEntryPayloadFreshCtx(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("advanced watermark err=%v, want errCommitPayloadStale", err)
	}
}

func TestCommitQueueCoalescesOnlyExactQueuedParent(t *testing.T) {
	const path = "/fast-grow.db"
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndModeAndLineage(path, 3, staging.PendingNew, 0, 0, false, "snapshot-C", "snapshot-B", true)
	if err != nil {
		t.Fatal(err)
	}
	a := &CommitEntry{Path: path, Kind: staging.PendingNew, PayloadBaseRevSet: true, SnapshotID: "snapshot-A", liveLineageProof: true}
	b := &CommitEntry{Path: path, Kind: staging.PendingNew, PayloadBaseRevSet: true, SnapshotID: "snapshot-B", ParentSnapshotID: "snapshot-A", liveLineageProof: true}
	c := &CommitEntry{Path: path, Kind: staging.PendingNew, PayloadBaseRevSet: true, SnapshotID: "snapshot-C", ParentSnapshotID: "snapshot-B", PendingIndexGen: gen, liveLineageProof: true}
	cq := &CommitQueue{
		queue:        []*CommitEntry{a, b, c},
		queuedByPath: map[string]map[*CommitEntry]struct{}{path: {a: {}, b: {}, c: {}}},
		inFlight:     map[string]*CommitEntry{path: a},
		index:        pending,
		delayed:      make(map[*CommitEntry]*time.Timer),
	}

	cq.coalesceDirectParentQueuedLocked(c)
	if !b.canceled {
		t.Fatal("exact queued parent B was not canceled")
	}
	if a.canceled {
		t.Fatal("in-flight ancestor A was canceled")
	}
	if c.ParentSnapshotID != "snapshot-A" {
		t.Fatalf("child parent=%q, want compressed snapshot-A", c.ParentSnapshotID)
	}
	if len(cq.queue) != 2 || cq.queue[0] != a || cq.queue[1] != c {
		t.Fatalf("queue after compression=%v, want [A C]", cq.queue)
	}
	meta, ok := pending.GetMeta(path)
	if !ok || meta.ParentSnapshotID != "snapshot-A" {
		t.Fatalf("pending meta=%+v ok=%t, want in-memory parent snapshot-A", meta, ok)
	}
}

func TestPendingIndexRestartDropsProcessLocalLineage(t *testing.T) {
	dir := t.TempDir()
	idx, err := staging.NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutWithBaseRevAndModeAndLineage("/legacy.db", 8, staging.PendingNew, 0, 0, false, "snapshot-B", "snapshot-A", true, "snapshot-A", "snapshot-root"); err != nil {
		t.Fatal(err)
	}
	if meta, ok := idx.GetMeta("/legacy.db"); !ok || meta.SnapshotID == "" || !meta.LineageTrusted() || len(meta.LiveAncestors()) != 2 {
		t.Fatalf("live meta=%+v ok=%t, want trusted process-local lineage", meta, ok)
	}
	recovered, err := staging.NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	meta, ok := recovered.GetMeta("/legacy.db")
	if !ok {
		t.Fatal("recovered metadata missing")
	}
	if meta.SnapshotID != "" || meta.ParentSnapshotID != "" || meta.LineageTrusted() || len(meta.LiveAncestors()) != 0 {
		t.Fatalf("recovered lineage=%q/%q ancestors=%v trusted=%t, want empty/untrusted", meta.SnapshotID, meta.ParentSnapshotID, meta.LiveAncestors(), meta.LineageTrusted())
	}
}

func TestCommitQueueSameProcessGrowthDoesNotOverwriteRecreatedSameRevSize(t *testing.T) {
	const path = "/kv-1g.db"
	const headerSnapshot = "recreate-header"
	const grownSnapshot = "recreate-grown"
	header := bytes.Repeat([]byte("H"), 4096)
	grown := append(bytes.Repeat([]byte("H"), 4096), bytes.Repeat([]byte("G"), 8192)...)
	recreated := bytes.Repeat([]byte("X"), 4096)

	server, ts := newCASFileServer(t, path, 0, nil)
	defer ts.Close()

	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	var watermark atomic.Int64
	cq.DurableWatermark = func(string) int64 { return watermark.Load() }
	cq.OnSuccess = func(_ *CommitEntry, rev int64) {
		watermark.Store(rev)
	}

	if err := shadow.WriteFull(path, header, 0); err != nil {
		t.Fatal(err)
	}
	headerGen := shadow.ActiveGeneration(path)
	headerPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(header)), staging.PendingNew, 0, 0, false, headerSnapshot, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           0,
		PayloadBaseRev:    0,
		PayloadBaseRevSet: true,
		Size:              int64(len(header)),
		Kind:              staging.PendingNew,
		MutationSeq:       1,
		ShadowGen:         headerGen,
		PendingIndexGen:   headerPending,
		SnapshotID:        headerSnapshot,
		liveLineageProof:  true,
	}); err != nil {
		t.Fatal(err)
	}
	waitCommitQueueIdle(t, cq)
	server.recreate(recreated)

	if err := shadow.WriteFull(path, grown, 0); err != nil {
		t.Fatal(err)
	}
	grownGen := shadow.ActiveGeneration(path)
	grownPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(grown)), staging.PendingNew, 0, 0, false, grownSnapshot, headerSnapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           0,
		PayloadBaseRev:    0,
		PayloadBaseRevSet: true,
		Size:              int64(len(grown)),
		Kind:              staging.PendingNew,
		MutationSeq:       2,
		ShadowGen:         grownGen,
		PendingIndexGen:   grownPending,
		SnapshotID:        grownSnapshot,
		ParentSnapshotID:  headerSnapshot,
		liveLineageProof:  true,
	}); err != nil {
		t.Fatal(err)
	}
	waitCommitQueueIdle(t, cq)

	rev, body, puts := server.snapshot()
	if rev != 1 || !bytes.Equal(body, recreated) {
		t.Fatalf("remote rev=%d body-len=%d, want recreated 4KiB image at rev=1", rev, len(body))
	}
	for _, put := range puts {
		if bytes.Equal(put.body, grown) {
			t.Fatal("same-process growth overwrote a delete/recreate with the same rev and size")
		}
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending entry missing after same-process recreate conflict")
	}
	if meta.Kind != staging.PendingConflict {
		t.Fatalf("pending kind = %v, want PendingConflict", meta.Kind)
	}
}

func TestCommitQueueRecoveredGrowthFailsClosedWithoutImmutablePayload(t *testing.T) {
	const path = "/kv-1g.db"
	const headerSnapshot = "recovery-header"
	const grownSnapshot = "recovery-grown"
	header := bytes.Repeat([]byte("H"), 4096)
	grown := append(bytes.Repeat([]byte("H"), 4096), bytes.Repeat([]byte("G"), 8192)...)

	server, ts := newCASFileServer(t, path, 0, nil)
	defer ts.Close()
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	if err := shadow.WriteFull(path, header, 0); err != nil {
		t.Fatal(err)
	}
	headerGen := shadow.ActiveGeneration(path)
	headerPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(header)), staging.PendingNew, 0, 0, false, headerSnapshot, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(header)), Kind: staging.PendingNew, MutationSeq: 1,
		ShadowGen: headerGen, PendingIndexGen: headerPending,
		SnapshotID: headerSnapshot, liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitCommitQueueIdle(t, cq)
	if err := shadow.WriteFull(path, grown, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(grown)), staging.PendingNew, 0, 0, false, grownSnapshot, headerSnapshot, true); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	recovered := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	recovered.client.SetSmallFileThresholdForTests(1 << 20)
	defer recovered.DrainAll()
	recovered.RecoverPending()
	waitCommitQueueIdle(t, recovered)
	if rev, body, _ := server.snapshot(); rev != 1 || !bytes.Equal(body, header) {
		t.Fatalf("after recovery rev=%d body-len=%d, want unchanged rev1 header-len=%d", rev, len(body), len(header))
	}
	meta, ok := pending.GetMeta(path)
	if !ok || meta.Kind != staging.PendingConflict {
		t.Fatalf("recovered mutable payload meta=%+v ok=%t, want preserved PendingConflict", meta, ok)
	}
}

func TestCommitQueuePruneMultipleLandmarksProtectsAllReferenceSources(t *testing.T) {
	queued := &CommitEntry{Path: "/queued", ParentSnapshotID: "queued-parent"}
	flight := &CommitEntry{Path: "/flight", ParentSnapshotID: "flight-parent"}
	immediate := &CommitEntry{Path: "/immediate", ParentSnapshotID: "immediate-parent"}
	cq := &CommitQueue{
		queue:     []*CommitEntry{queued},
		inFlight:  map[string]*CommitEntry{flight.Path: flight},
		immediate: map[*CommitEntry]struct{}{immediate: {}},
		landed:    make(map[string]pathCommitLandmark),
	}
	for _, entry := range []*CommitEntry{queued, flight, immediate} {
		cq.landed[entry.Path] = pathCommitLandmark{snapshotID: entry.ParentSnapshotID, lastUsed: 1}
	}
	for i := 0; i < maxLandedCommitLandmarks; i++ {
		cq.landed[fmt.Sprintf("/unreferenced-%04d", i)] = pathCommitLandmark{lastUsed: uint64(i + 2)}
	}
	cq.mu.Lock()
	cq.pruneLandedLocked()
	cq.mu.Unlock()
	if len(cq.landed) != maxLandedCommitLandmarks {
		t.Fatalf("landmark count=%d, want %d", len(cq.landed), maxLandedCommitLandmarks)
	}
	for _, entry := range []*CommitEntry{queued, flight, immediate} {
		if got := cq.landed[entry.Path]; got.snapshotID != entry.ParentSnapshotID {
			t.Errorf("evicted referenced parent for %s: %+v", entry.Path, got)
		}
	}
	for i := 0; i < 3; i++ {
		if _, ok := cq.landed[fmt.Sprintf("/unreferenced-%04d", i)]; ok {
			t.Errorf("retained unreferenced landmark %d", i)
		}
	}
}

// --- Ports of commit_queue_lineage_regression_test.go ---

// Reproduce the original spill window precisely: upload has returned and
// released its generation lock, but success has not yet recorded a landmark.
func TestCommitQueueSpillLandedProofSurvivesReplacementBeforeSuccess(t *testing.T) {
	const path = "/spill-tail.db"
	server, ts := newCASFileServer(t, path, 0, nil)
	t.Cleanup(ts.Close)
	cq := newLineageQueue(t, ts.URL)
	parent := stageLineageEntry(t, cq, path, []byte("parent"), "A", "", true)
	rev, err := cq.uploadEntry(context.Background(), parent)
	if err != nil {
		t.Fatal(err)
	}
	child := stageLineageEntry(t, cq, path, []byte("parent-grown"), "B", "A", true)
	if err := cq.onCommitSuccess(parent, 0, rev); err != nil {
		t.Fatal(err)
	}
	proof := cq.landedCommit(path)
	if proof.snapshotID != "A" || proof.checksum != payloadChecksum([]byte("parent")) {
		t.Fatalf("uploaded parent proof = %+v", proof)
	}
	if meta, ok := cq.index.GetMeta(path); !ok || meta.Generation != child.PendingIndexGen {
		t.Fatal("parent cleanup removed the child generation")
	}
	child.DurableWatermarkRev = 1
	if err := cq.CommitNow(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if rev, body, _ := server.snapshot(); rev != 2 || string(body) != "parent-grown" {
		t.Fatalf("rev=%d body=%q", rev, body)
	}
}

func TestCommitQueueSpillConflictReadsHonorCancellation(t *testing.T) {
	for _, phase := range []string{"stat", "growth-read", "equal-read"} {
		t.Run(phase, func(t *testing.T) {
			const path = "/cancel-spill.db"
			payload := []byte("parent-grown")
			parentSize := len("parent")
			if phase == "equal-read" {
				parentSize = len(payload)
			}
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusConflict)
					return
				}
				if (phase == "stat" && r.Method == http.MethodHead) || (phase != "stat" && r.Method == http.MethodGet) {
					once.Do(func() { close(started) })
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Length", strconv.Itoa(parentSize))
					w.Header().Set("X-Dat9-Revision", "1")
					w.WriteHeader(http.StatusOK)
					return
				}
				http.NotFound(w, r)
			}))
			t.Cleanup(ts.Close)
			cq := newLineageQueue(t, ts.URL)
			// Cleanup must unblock the handler before draining, including on assertion failure.
			t.Cleanup(func() { close(release); cq.CancelPathPreserveLocal(path) })
			entry := stageLineageEntry(t, cq, path, payload, "B", "A", true)
			// No watermark: the actual initial PUT hits 409 and enters spill resolution.
			if err := cq.Enqueue(entry); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("conflict read did not start")
			}
			cq.CancelPathPreserveLocal(path)
			drained := make(chan struct{})
			go func() { cq.WaitPath(path); close(drained) }()
			select {
			case <-drained:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled spill read did not drain")
			}
			meta, ok := cq.index.GetMeta(path)
			if !ok || meta.Kind == staging.PendingConflict {
				t.Fatalf("canceled staging=%+v exists=%t", meta, ok)
			}
		})
	}
}

func TestCommitQueueLayerGrowthPreservesMainClaim(t *testing.T) {
	for _, spill := range []bool{false, true} {
		for _, mainParent := range []bool{false, true} {
			t.Run(fmt.Sprintf("spill=%t/main-parent=%t", spill, mainParent), func(t *testing.T) {
				const path = "/layer-growth.db"
				parentBody := []byte("header")
				childBody := []byte("header-grown")
				main := &casFileServer{t: t, path: path}
				type claim struct {
					rev  int64
					body []byte
				}
				claims := make(chan claim, 2)
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/fs"+path {
						main.serveHTTP(w, r)
						return
					}
					var got claim
					switch r.URL.Path {
					case "/v1/layers/L/entries":
						var req client.FSLayerEntryRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							http.Error(w, err.Error(), 400)
							return
						}
						got = claim{req.BaseRevision, req.Content}
					case "/v1/layers/L/objects":
						got.rev, _ = strconv.ParseInt(r.URL.Query().Get("base_revision"), 10, 64)
						got.body, _ = io.ReadAll(r.Body)
					default:
						http.NotFound(w, r)
						return // SDK fallback to /objects
					}
					claims <- got
					_ = json.NewEncoder(w).Encode(map[string]any{"layer_id": "L", "path": path, "op": "upsert", "kind": "file", "size_bytes": len(got.body)})
				}))
				t.Cleanup(ts.Close)
				cq := newLineageQueue(t, ts.URL)
				if !mainParent {
					cq.SetLayerRef("L")
				}
				parent := stageLineageEntry(t, cq, path, parentBody, "A", "", spill)
				if err := cq.CommitNow(context.Background(), parent); err != nil {
					t.Fatal(err)
				}
				if !mainParent {
					got := <-claims
					if got.rev != 0 || !bytes.Equal(got.body, parentBody) {
						t.Fatalf("parent claim=%+v", got)
					}
					if proof := cq.landedCommit(path); proof.snapshotID != "" {
						t.Fatalf("overlay invented main landmark: %+v", proof)
					}
				}
				// An existing main proof exercises post-validation CAS wiring; this
				// is not a claim that overlay uploads increment main revisions.
				cq.SetLayerRef("L")
				child := stageLineageEntry(t, cq, path, childBody, "B", "A", spill)
				wantRev := int64(0)
				if mainParent {
					wantRev = 1
					child.DurableWatermarkRev = 1
				}
				if err := cq.CommitNow(context.Background(), child); err != nil {
					t.Fatal(err)
				}
				got := <-claims
				if got.rev != wantRev || !bytes.Equal(got.body, childBody) {
					t.Fatalf("child claim rev=%d body=%q, want %d/%q", got.rev, got.body, wantRev, childBody)
				}
				if child.PayloadBaseRev != 0 {
					t.Fatal("payload audit base changed")
				}
			})
		}
	}
}

func TestCommitQueueCoalescePreservesModeClaim(t *testing.T) {
	for _, mode := range []struct {
		has  bool
		mode uint32
	}{{false, 0}, {true, 0o600}} {
		cq := newLineageQueue(t, "http://127.0.0.1")
		parent := &CommitEntry{Path: "/mode", Kind: staging.PendingNew, PayloadBaseRevSet: true, SnapshotID: "A", liveLineageProof: true, HasMode: true, Mode: 0o640}
		child := stageLineageEntry(t, cq, "/mode", []byte("grown"), "B", "A", false)
		child.HasMode, child.Mode = mode.has, mode.mode
		cq.mu.Lock()
		cq.queue = []*CommitEntry{parent, child}
		cq.rebuildQueuedIndexLocked()
		cq.coalesceDirectParentQueuedLocked(child)
		canceled := parent.canceled
		cq.queue = nil
		cq.rebuildQueuedIndexLocked()
		cq.mu.Unlock()
		if canceled || child.ParentSnapshotID != "A" {
			t.Fatal("coalesced across a different mode claim")
		}
	}
}

func TestCommitQueueSmallSpillMultipartRetainsUploadedProof(t *testing.T) {
	const path = "/multipart-parent.db"
	payload := []byte("small-parent")
	expected := int64(0)
	rec := newMultipartUploadRecorder(t, path, int64(len(payload)), &expected)
	t.Cleanup(rec.server.Close)
	cq := newLineageQueue(t, rec.server.URL)
	cq.client.SetSmallFileThresholdForTests(1) // force multipart despite the bounded image
	entry := stageLineageEntry(t, cq, path, payload, "A", "", true)
	rev, err := cq.uploadEntry(context.Background(), entry)
	if err != nil {
		t.Fatal(err)
	}
	stageLineageEntry(t, cq, path, []byte("small-parent-grown"), "B", "A", true)
	if err := cq.onCommitSuccess(entry, 0, rev); err != nil {
		t.Fatal(err)
	}
	proof := cq.landedCommit(path)
	if proof.checksum != payloadChecksum(payload) || proof.snapshotID != "A" {
		t.Fatalf("proof=%+v", proof)
	}
	if rec.completeCalls.Load() != 1 || rec.directFilePuts.Load() != 0 {
		t.Fatal("test did not use multipart completion")
	}
}

func TestCommitQueueSmallSpillChecksActualSizeBeforeBinding(t *testing.T) {
	cq := newLineageQueue(t, "http://127.0.0.1:1")
	entry := stageLineageEntry(t, cq, "/oversize", make([]byte, maxLandedPayloadBytes+1), "A", "", true)
	entry.Size = 8 // stale/mismatched queue metadata must not permit an unbounded allocation
	if _, err := cq.uploadEntry(context.Background(), entry); err == nil {
		t.Fatal("accepted mismatched spill size")
	}
	if entry.payloadBound {
		t.Fatal("bound an oversized spill image")
	}
}

func TestCommitQueueRenamedSnapshotCommitsThenAuthorizesGrowth(t *testing.T) {
	const oldPath = "/old.db"
	const newPath = "/new.db"
	server, ts := newCASFileServer(t, newPath, 0, nil)
	t.Cleanup(ts.Close)
	cq := newLineageQueue(t, ts.URL)
	parent := stageLineageEntry(t, cq, oldPath, []byte("header"), "A", "", true)
	prepared, err := cq.index.PrepareRename(oldPath, newPath)
	if err != nil || prepared == nil {
		t.Fatalf("prepare=%+v err=%v", prepared, err)
	}
	if !cq.shadows.Rename(oldPath, newPath) {
		t.Fatal("shadow rename failed")
	}
	cq.index.CommitRename(oldPath, prepared)
	// v1 bindCommitEntryToPreparedPendingMeta: the renamed entry adopts the
	// prepared pending metadata's identity.
	parent.Path = newPath
	parent.ShadowGen = cq.shadows.ActiveGeneration(newPath)
	parent.PendingIndexGen = prepared.Generation
	if prepared.SnapshotID != "" {
		parent.SnapshotID = prepared.SnapshotID
	}
	if err := cq.CommitNow(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	if proof := cq.landedCommit(oldPath); proof.snapshotID != "" {
		t.Fatal("landmark remained at old path")
	}
	child := stageLineageEntry(t, cq, newPath, []byte("header-grown"), "B", "A", true)
	child.DurableWatermarkRev = 1
	if err := cq.CommitNow(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || string(body) != "header-grown" || len(puts) != 2 {
		t.Fatalf("renamed growth rev=%d body=%q puts=%d", rev, body, len(puts))
	}
}

func TestCommitQueueCanceledIdempotentSuccessPreservesStaging(t *testing.T) {
	for _, spill := range []bool{false, true} {
		for _, cancelContext := range []bool{false, true} {
			t.Run(fmt.Sprintf("spill=%t/context=%t", spill, cancelContext), func(t *testing.T) {
				cq := newLineageQueue(t, "http://127.0.0.1:1")
				entry := stageLineageEntry(t, cq, "/canceled", []byte("equal"), "A", "", spill)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if cancelContext {
					cancel()
				} else {
					entry.canceled = true
				}
				published := false
				cq.OnUploaded = func(*CommitEntry, int64) { published = true }
				// The equality read has succeeded, but cancellation won before its
				// shared buffered/spill success tail begins.
				cq.finishIdempotentCommit(ctx, entry, 1)
				if published {
					t.Fatal("published canceled equality result")
				}
				if meta, ok := cq.index.GetMeta(entry.Path); !ok || meta.Kind == staging.PendingConflict {
					t.Fatalf("staging=%+v exists=%t", meta, ok)
				}
				if !cq.shadows.Has(entry.Path) {
					t.Fatal("removed canceled shadow")
				}
			})
		}
	}
}

func TestCommitQueueEmptyParentGrowthAcceptsRangeEOF(t *testing.T) {
	const path = "/empty-parent"
	main := &casFileServer{t: t, path: path}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" {
			rev, body, _ := main.snapshot()
			if rev == 1 && len(body) == 0 {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
		}
		main.serveHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cq := newLineageQueue(t, ts.URL)
	parent := stageLineageEntry(t, cq, path, nil, "A", "", true)
	if err := cq.CommitNow(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	child := stageLineageEntry(t, cq, path, []byte("grown"), "B", "A", true)
	child.DurableWatermarkRev = 1
	if err := cq.CommitNow(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if rev, body, _ := main.snapshot(); rev != 2 || string(body) != "grown" {
		t.Fatalf("rev=%d body=%q", rev, body)
	}
}

// An in-flight entry can remain in either queue index while its upload worker
// binds the immutable payload without cq.mu. Coalescing must reject ownership
// before reading any worker-owned fields, not merely skip the final mutation.
func TestCommitQueueCoalescerSkipsInFlightPayloadBinding(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			parent := &CommitEntry{
				Path: "/in-flight.bin", Kind: staging.PendingNew, PayloadBaseRevSet: true,
				SnapshotID: "parent", liveLineageProof: true,
			}
			child := &CommitEntry{
				Path: parent.Path, Kind: staging.PendingNew, PayloadBaseRevSet: true,
				SnapshotID: "child", ParentSnapshotID: parent.SnapshotID, liveLineageProof: true,
			}
			cq := &CommitQueue{
				queue:    []*CommitEntry{parent},
				inFlight: map[string]*CommitEntry{parent.Path: parent},
			}
			if indexed {
				cq.queuedByPath = map[string]map[*CommitEntry]struct{}{parent.Path: {parent: {}, nil: {}}}
			}
			start := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				<-start
				parent.bindPayload([]byte("uploaded snapshot"))
			}()
			close(start)
			for i := 0; i < 10000; i++ {
				cq.mu.Lock()
				cq.coalesceDirectParentQueuedLocked(child)
				cq.mu.Unlock()
			}
			<-done
			if len(cq.queue) != 1 || cq.queue[0] != parent || parent.canceled || child.ParentSnapshotID != parent.SnapshotID {
				t.Fatal("coalescing changed the in-flight parent or child lineage")
			}
		})
	}
}
