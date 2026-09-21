package fuse

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCommitQueueRejectsUnprovenWatermarkPayload(t *testing.T) {
	for _, payload := range []string{"ABCD", "AXYZ"} {
		t.Run(payload, func(t *testing.T) {
			const path = "/watermark"
			server, ts := newCASFileServer(t, path, 2, []byte("AB"))
			defer ts.Close()
			shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			cq := NewCommitQueue(newTestClient(ts.URL), shadow, nil, nil, 1, 8)
			defer cq.DrainAll()
			cq.rememberLanded(path, 2, 2, payloadChecksum([]byte("AB")), "landed")
			entry := &CommitEntry{
				Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
				Size: int64(len(payload)), Kind: PendingOverwrite, DurableWatermarkRev: 2,
				SnapshotID: "fork", ParentSnapshotID: "unrelated", liveLineageProof: true,
			}
			entry.bindPayload([]byte(payload))
			if err := cq.CommitNow(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
				t.Fatalf("unproven payload err=%v, want stale conflict", err)
			}
			if rev, body, puts := server.snapshot(); rev != 2 || string(body) != "AB" || len(puts) != 0 {
				t.Fatalf("remote rev=%d body=%q puts=%d", rev, body, len(puts))
			}
		})
	}
}

func TestLiveSnapshotAncestorsBoundedAndRecoveredFailClosed(t *testing.T) {
	parents := make([]string, maxLiveSnapshotAncestors)
	for i := range parents {
		parents[i] = "older"
	}
	got := snapshotAncestors("parent", parents)
	if len(got) != maxLiveSnapshotAncestors || got[0] != "parent" {
		t.Fatalf("bounded ancestors=%v", got)
	}
	parents[0] = "changed"
	if got[1] != "older" {
		t.Fatal("ancestor proof aliases mutable input")
	}
	entry := &CommitEntry{Kind: PendingOverwrite, BaseRev: 1, PayloadBaseRev: 1,
		PayloadBaseRevSet: true, SnapshotID: "child", ParentSnapshotID: "parent",
		liveLineageProof: true, liveAncestors: got, recovered: true}
	if entryCanRebaseOntoLandedParent(entry, pathCommitLandmark{snapshotID: "older"}) {
		t.Fatal("recovered entry retained live ancestor authority")
	}
}

func TestCommitQueueBatchDiscardsVerifiedAncestor(t *testing.T) {
	landed := &casFileServer{t: t, path: "/ancestor", revision: 2, body: []byte("AB")}
	other := &casFileServer{t: t, path: "/other"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/fs/ancestor" {
			landed.serveHTTP(w, r)
		} else {
			other.serveHTTP(w, r)
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
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	cq.rememberLanded("/ancestor", 2, 2, payloadChecksum([]byte("AB")), "B", "A")
	entries := []*CommitEntry{
		{Path: "/ancestor", BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
			Size: 1, Kind: PendingOverwrite, SnapshotID: "A", liveLineageProof: true,
			DurableWatermarkRev: 2},
		{Path: "/other", Size: 1, Kind: PendingNew},
	}
	for _, entry := range entries {
		if err := shadow.WriteFull(entry.Path, []byte("A"), entry.BaseRev); err != nil {
			t.Fatal(err)
		}
		entry.ShadowGen = shadow.ActiveGeneration(entry.Path)
		entry.PendingIndexGen, err = pending.PutWithBaseRev(entry.Path, 1, entry.Kind, entry.BaseRev)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Enter the batch boundary deterministically, without relying on a timer
	// to collect both entries before the first worker can dispatch alone.
	cq.mu.Lock()
	cq.queue = append([]*CommitEntry(nil), entries...)
	for _, entry := range entries {
		cq.inFlight[entry.Path] = entry
	}
	cq.rebuildQueuedIndexLocked()
	cq.mu.Unlock()
	cq.commitBatch(entries)
	if conflicts, _, _ := pending.ConflictSummary(); conflicts != 0 {
		t.Fatalf("batch left %d conflicts for a committed ancestor", conflicts)
	}
	if rev, body, puts := landed.snapshot(); rev != 2 || string(body) != "AB" || len(puts) != 0 {
		t.Fatalf("ancestor overwritten: rev=%d bytes=%q puts=%d", rev, body, len(puts))
	}
	if heads, gets := landed.proofRequestCounts(); heads != 1 || gets != 1 {
		t.Fatalf("landed identity was not verified: HEAD=%d GET=%d", heads, gets)
	}
	if rev, body, _ := other.snapshot(); rev != 1 || string(body) != "A" {
		t.Fatalf("unrelated entry not committed: rev=%d bytes=%q", rev, body)
	}
}

// TestPR939ReviewPendingNewPreservesCreateOnly verifies that an unrelated
// remote create cannot be authorized merely because its bytes are a prefix.
func TestPR939ReviewPendingNewPreservesCreateOnly(t *testing.T) {
	for _, remote := range []string{"A", ""} {
		name := "prefix"
		if remote == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			pr939Assert409PreservesRemote(t, PendingNew, 0, []byte("ABCD"), []byte(remote))
		})
	}
}

// TestPR939ReviewRemoteTruncationPreserved models local ABC -> ABCD while
// another writer has already truncated revision 1's ABC to A or empty at
// revision 2. Keeping a prefix does not prove that removed bytes are new.
func TestPR939ReviewRemoteTruncationPreserved(t *testing.T) {
	for _, remote := range []string{"A", ""} {
		name := "shorter_prefix"
		if remote == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			pr939Assert409PreservesRemote(t, PendingOverwrite, 1, []byte("ABCD"), []byte(remote))
		})
	}
}

// TestPR939ReviewLongerDivergent409Rejected reaches the 409 prefix predicate
// with a longer payload, unlike the existing equal/shorter-length negatives.
func TestPR939ReviewLongerDivergent409Rejected(t *testing.T) {
	pr939Assert409PreservesRemote(t, PendingOverwrite, 1, []byte("AXYZ"), []byte("AB"))
}

func pr939Assert409PreservesRemote(t *testing.T, kind PendingKind, baseRev int64, local, remote []byte) {
	t.Helper()
	const path = "/pr939-review/409"
	server, ts := newCASFileServer(t, path, 1, []byte("ABC"))
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
	if err := shadow.WriteFull(path, local, baseRev); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(local)), kind, baseRev)
	if err != nil {
		t.Fatal(err)
	}
	// A different writer has committed before this local queue observes it.
	server.mu.Lock()
	server.revision = 2
	server.body = append([]byte(nil), remote...)
	server.mu.Unlock()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	if err := cq.Enqueue(&CommitEntry{
		Path: path, BaseRev: baseRev, PayloadBaseRev: baseRev, PayloadBaseRevSet: true,
		Size: int64(len(local)), Kind: kind,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()
	rev, body, puts := server.snapshot()
	meta, ok := pending.GetMeta(path)
	heads, gets := server.proofRequestCounts()
	t.Logf("kind=%v base=%d local=%q remote-before=%q remote-after=%q revision=%d successful-PUTs=%+v HEAD=%d GET=%d pending=%+v present=%t",
		kind, baseRev, local, remote, body, rev, puts, heads, gets, meta, ok)
	if rev != 2 || !bytes.Equal(body, remote) || len(puts) != 0 {
		t.Errorf("remote was overwritten: rev=%d body=%q successful PUTs=%d; want unchanged rev=2 body=%q and zero successful PUTs", rev, body, len(puts), remote)
	}
	if !ok || meta.Kind != PendingConflict {
		t.Errorf("pending=%+v present=%t; want retained PendingConflict", meta, ok)
	}
}
