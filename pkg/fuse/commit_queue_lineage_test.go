package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommitQueueRebasesGrownPayloadAfterSmallerSamePathCommit(t *testing.T) {
	const path = "/kv-1g.db"
	const headerSnapshot = "header-snapshot"
	const grownSnapshot = "grown-snapshot"
	header := bytes.Repeat([]byte("H"), 4096)
	grown := append(append([]byte(nil), header...), bytes.Repeat([]byte("G"), 8192)...)

	server, ts := newCASFileServer(t, path, 0, nil)
	defer ts.Close()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
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
	headerPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(header)), PendingNew, 0, 0, false, headerSnapshot, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(header)), Kind: PendingNew, MutationSeq: 1,
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
	grownPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(grown)), PendingNew, 0, 0, false, grownSnapshot, headerSnapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(grown)), Kind: PendingNew, MutationSeq: 2,
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

			shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			pending, err := NewPendingIndex(t.TempDir())
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
			parentPendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(parent)), PendingNew, 0, 0, false, parentSnapshot, "", true)
			if err != nil {
				t.Fatal(err)
			}
			if err := cq.Enqueue(&CommitEntry{
				Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
				Size: int64(len(parent)), Kind: PendingNew, ShadowSpill: tc.spill, MutationSeq: 1,
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
			childPendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(child)), PendingNew, 0, 0, false, childSnapshot, parentSnapshot, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := cq.Enqueue(&CommitEntry{
				Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
				Size: int64(len(child)), Kind: PendingNew, ShadowSpill: tc.spill, MutationSeq: 2,
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
		queue:     []*CommitEntry{{Path: "/protected.db", ParentSnapshotID: "protected-parent"}},
		inFlight:  make(map[string]*CommitEntry),
		immediate: make(map[*CommitEntry]struct{}),
		landed:    make(map[string]pathCommitLandmark),
	}
	cq.rememberLanded("/protected.db", 1, 1, "checksum", "protected-parent")
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

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, child, 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(child)), PendingNew, 0, 0, false, "snapshot-child", parentSnapshot, true)
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
		Size: int64(len(child)), Kind: PendingNew, ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: pendingGen,
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
	if meta.Kind == PendingConflict {
		t.Fatal("caller cancellation was recorded as a terminal conflict")
	}
}

func TestOpenHandlePathWideShadowPreloadDropsLineageTrust(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	const path = "/generation-race.db"
	activePayload := []byte("another handle's active generation")
	if err := shadow.WriteFull(path, activePayload, 0); err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup(path, false, int64(len(activePayload)), time.Now())
	source := &FileHandle{
		Ino:               ino,
		Path:              path,
		Dirty:             fs.newWriteBuffer(path, maxPreloadSize, 0),
		IsNew:             true,
		ShadowReady:       true,
		ShadowStageGen:    0,
		ContentSnapshotID: "source-snapshot",
		LineageTrusted:    true,
	}
	if _, err := source.Dirty.Write(0, []byte("source handle bytes")); err != nil {
		t.Fatal(err)
	}
	source.DirtySeq = fs.markDirtySize(ino, source.Dirty.Size())
	fs.openHandles.Add(source)

	target := &FileHandle{
		Ino:   ino,
		Path:  path,
		Dirty: fs.newWriteBuffer(path, maxPreloadSize, 0),
	}
	if !fs.loadWritableHandleFromOpenHandleLocked(target) {
		t.Fatal("loadWritableHandleFromOpenHandleLocked returned false")
	}
	if got := target.Dirty.Bytes(); !bytes.Equal(got, activePayload) {
		t.Fatalf("preloaded bytes = %q, want path-wide active shadow %q", got, activePayload)
	}
	if target.LineageTrusted {
		t.Fatal("path-wide shadow preload retained another handle's lineage trust")
	}
}

func TestCommitQueueDoesNotRebaseLargerStaleCheckpointOntoLandedMain(t *testing.T) {
	const path = "/app.db"
	base := []byte("base-rev1")
	checkpointB := []byte("checkpoint-B-rev2")
	staleA := append([]byte("checkpoint-A-from-rev1:"), bytes.Repeat([]byte("x"), len(checkpointB))...)

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
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
		pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingOverwrite, baseRev)
		if err != nil {
			t.Fatal(err)
		}
		return shadowGen, pendingGen
	}

	shadowGen, pendingGen := stage(checkpointB, 1)
	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
		Size: int64(len(checkpointB)), Kind: PendingOverwrite,
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
		Size: int64(len(staleA)), Kind: PendingOverwrite,
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
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
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
		Size: int64(len(child)), Kind: PendingNew, ShadowGen: shadow.ActiveGeneration(path),
		SnapshotID: "snapshot-child", ParentSnapshotID: parentEntry.SnapshotID,
		liveLineageProof: true, DurableWatermarkRev: 1,
	}
	if err := cq.validateEntryPayloadFresh(entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("large-parent growth err = %v, want fail-closed errCommitPayloadStale", err)
	}
}

func TestCommitQueueGrowthRebaseCannotResurrectOlderSQLiteWALGeneration(t *testing.T) {
	const path = "/app.db-wal"
	oldHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	oldGeneration := append(append([]byte(nil), oldHeader...), bytes.Repeat([]byte("o"), 4096)...)

	server, ts := newCASFileServer(t, path, 1, oldGeneration)
	defer ts.Close()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
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
		pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingOverwrite, payloadBase)
		if err != nil {
			t.Fatal(err)
		}
		return cq.CommitNow(context.Background(), &CommitEntry{
			Path: path, BaseRev: payloadBase, PayloadBaseRev: payloadBase, PayloadBaseRevSet: true,
			Size: int64(len(payload)), Kind: PendingOverwrite,
			ShadowGen: shadowGen, PendingIndexGen: pendingGen,
			DurableWatermarkRev: durable, DisableAutoResolveLWW: true,
		})
	}

	if err := commit(newHeader, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := commit(oldGeneration, 1, 2); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("old WAL generation err=%v, want errCommitPayloadStale", err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || !bytes.Equal(body, newHeader) || len(puts) != 1 {
		t.Fatalf("after stale WAL rev=%d len=%d puts=%d, want rev2 exact new header and no second PUT", rev, len(body), len(puts))
	}
}

func TestCommitQueueGrowthRebaseRejectsForkedPendingNew(t *testing.T) {
	const path = "/fork.db-wal"
	base := []byte("landed-A")
	childB := []byte("landed-A-child-B")
	siblingC := []byte("landed-A-larger-sibling-C")

	server, ts := newCASFileServer(t, path, 1, base)
	defer ts.Close()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
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
		pendingGen, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(payload)), PendingNew, 0, 0, false, snapshotID, "snapshot-A", true)
		if err != nil {
			t.Fatal(err)
		}
		return cq.CommitNow(context.Background(), &CommitEntry{
			Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
			Size: int64(len(payload)), Kind: PendingNew,
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
		Size: int64(len(grown)), Kind: PendingNew,
		SnapshotID: "snapshot-B", ParentSnapshotID: "snapshot-A", liveLineageProof: true,
	}

	if err := cq.validateEntryPayloadFresh(entry); err != nil {
		t.Fatal(err)
	}
	firstHeads, firstGets := server.proofRequestCounts()
	if firstHeads != 1 || firstGets != 1 {
		t.Fatalf("first proof HEAD/GET=%d/%d, want 1/1", firstHeads, firstGets)
	}
	if err := cq.validateEntryPayloadFresh(entry); err != nil {
		t.Fatal(err)
	}
	if heads, gets := server.proofRequestCounts(); heads != firstHeads || gets != firstGets {
		t.Fatalf("repeat validation HEAD/GET=%d/%d, want unchanged %d/%d", heads, gets, firstHeads, firstGets)
	}
	watermark.Store(2)
	if err := cq.validateEntryPayloadFresh(entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("advanced watermark err=%v, want errCommitPayloadStale", err)
	}
}

func TestCommitQueueCoalescesOnlyExactQueuedParent(t *testing.T) {
	const path = "/fast-grow.db"
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndModeAndLineage(path, 3, PendingNew, 0, 0, false, "snapshot-C", "snapshot-B", true)
	if err != nil {
		t.Fatal(err)
	}
	a := &CommitEntry{Path: path, Kind: PendingNew, PayloadBaseRevSet: true, SnapshotID: "snapshot-A", liveLineageProof: true}
	b := &CommitEntry{Path: path, Kind: PendingNew, PayloadBaseRevSet: true, SnapshotID: "snapshot-B", ParentSnapshotID: "snapshot-A", liveLineageProof: true}
	c := &CommitEntry{Path: path, Kind: PendingNew, PayloadBaseRevSet: true, SnapshotID: "snapshot-C", ParentSnapshotID: "snapshot-B", PendingIndexGen: gen, liveLineageProof: true}
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
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutWithBaseRevAndModeAndLineage("/legacy.db", 8, PendingNew, 0, 0, false, "snapshot-B", "snapshot-A", true); err != nil {
		t.Fatal(err)
	}
	if meta, ok := idx.GetMeta("/legacy.db"); !ok || meta.SnapshotID == "" || !meta.lineageTrusted {
		t.Fatalf("live meta=%+v ok=%t, want trusted process-local lineage", meta, ok)
	}
	recovered, err := NewPendingIndex(dir)
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
	if meta.SnapshotID != "" || meta.ParentSnapshotID != "" || meta.lineageTrusted {
		t.Fatalf("recovered lineage=%q/%q trusted=%t, want empty/untrusted", meta.SnapshotID, meta.ParentSnapshotID, meta.lineageTrusted)
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

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
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
	headerPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(header)), PendingNew, 0, 0, false, headerSnapshot, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           0,
		PayloadBaseRev:    0,
		PayloadBaseRevSet: true,
		Size:              int64(len(header)),
		Kind:              PendingNew,
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
	grownPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(grown)), PendingNew, 0, 0, false, grownSnapshot, headerSnapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           0,
		PayloadBaseRev:    0,
		PayloadBaseRevSet: true,
		Size:              int64(len(grown)),
		Kind:              PendingNew,
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
	if meta.Kind != PendingConflict {
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
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
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
	headerPending, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(header)), PendingNew, 0, 0, false, headerSnapshot, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
		Size: int64(len(header)), Kind: PendingNew, MutationSeq: 1,
		ShadowGen: headerGen, PendingIndexGen: headerPending,
		SnapshotID: headerSnapshot, liveLineageProof: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitCommitQueueIdle(t, cq)
	if err := shadow.WriteFull(path, grown, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndModeAndLineage(path, int64(len(grown)), PendingNew, 0, 0, false, grownSnapshot, headerSnapshot, true); err != nil {
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
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("recovered mutable payload meta=%+v ok=%t, want preserved PendingConflict", meta, ok)
	}
}

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
