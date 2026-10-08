package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Use actual Link/Open handlers: the POST establishes the shared inode even
// when its optional confirmation HEAD cannot populate ResourceID.
func issue1023LinkWithoutConfirmationRID(t *testing.T, headProbe ...func(path string)) (*Dat9FS, uint64, *casFileServer, uint64, uint64, *atomic.Bool) {
	t.Helper()
	const original = "/empty-rid-original"
	const alias = "/empty-rid-alias"
	fs, ino, server, closeOld := reviewR2FS(t, original, false)
	cleanupKernelCacheBypassAfterTest(t, fs)
	closeOld()
	fs.inodes.SetIdentity(ino, "", 1)
	fs.inodes.UpdateMode(ino, 0o644)
	var linked, confirming, denyProof atomic.Bool
	var failedConfirm atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Get("hardlink") == "1" {
			if r.URL.Path != "/v1/fs"+alias || r.Header.Get("X-Dat9-Hardlink-Source") != original {
				t.Error("unexpected hardlink request")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			linked.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		if r.URL.Path != "/v1/fs"+original && (r.URL.Path != "/v1/fs"+alias || !linked.Load()) {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+alias && confirming.Load() {
			failedConfirm.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Method == http.MethodHead && denyProof.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Method == http.MethodHead && linked.Load() && !confirming.Load() {
			for _, probe := range headProbe {
				probe(r.URL.Path)
			}
		}
		// Initial source Open cannot populate RID. Later successful reads can
		// observe the real shared identity, rather than a fabricated local one.
		if linked.Load() {
			w.Header().Set("X-Dat9-Resource-ID", "confirmed-link-shared-resource")
			w.Header().Set("X-Dat9-Nlink", "2")
		} else {
			w.Header().Set("X-Dat9-Nlink", "1")
		}
		w.Header().Set("X-Dat9-Mode", "420")
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/v1/fs" + original
		clone.URL = &urlCopy
		server.serveHTTP(w, clone)
	}))
	t.Cleanup(ts.Close)
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.commitQueue.client = fs.client
	open := func(pid uint32) uint64 {
		var out gofuse.OpenOut
		if status := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: pid}}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &out); status != gofuse.OK {
			t.Fatalf("Open=%v", status)
		}
		return out.Fh
	}
	sourceID := open(1001)
	confirming.Store(true)
	var linkOut gofuse.EntryOut
	status := fs.Link(nil, &gofuse.LinkIn{InHeader: gofuse.InHeader{NodeId: 1}, Oldnodeid: ino}, "empty-rid-alias", &linkOut)
	confirming.Store(false)
	if status != gofuse.OK || !linked.Load() || failedConfirm.Load() == 0 {
		t.Fatalf("confirmed Link=%v linked=%t failed confirmation HEADs=%d", status, linked.Load(), failedConfirm.Load())
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.ResourceID != "" || linkOut.NodeId != ino || linkOut.Nlink != 2 || !fs.sameLinkedInode(original, ino) || !fs.sameLinkedInode(alias, ino) {
		t.Fatal("successful Link with missing advisory RID did not retain the confirmed alias identity")
	}
	peerID := open(1002)
	source, _ := fs.fileHandles.Get(sourceID)
	peer, _ := fs.fileHandles.Get(peerID)
	if source.Path != original || peer.Path != alias || source.Ino != peer.Ino {
		t.Fatalf("retained/alias paths=%q/%q inodes=%d/%d", source.Path, peer.Path, source.Ino, peer.Ino)
	}
	if entry, _ := fs.inodes.GetEntry(ino); entry.ResourceID != "" {
		t.Fatal("peer Open unexpectedly erased the missing-RID premise")
	}
	t.Cleanup(func() {
		denyProof.Store(false)
		for _, id := range []uint64{sourceID, peerID} {
			if handle, present := fs.fileHandles.Get(id); present {
				handle.Lock()
				fs.releaseHandleRemoteCommitPathLocked(handle)
				handle.Unlock()
			}
		}
	})
	return fs, ino, server, sourceID, peerID, &denyProof
}

// The readable case requires the original full-record result. The unavailable
// proof case separately requires zero acknowledgment and retained dirty state.
// Neither case weakens the original four-process/48-record acceptance matrix.
func TestIssue1023ConfirmedLinkMissingRIDRetainedAppender(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		name := "proof_readable"
		if unavailable {
			name = "proof_unavailable"
		}
		t.Run(name, func(t *testing.T) {
			fs, ino, server, sourceID, peerID, denyProof := issue1023LinkWithoutConfirmationRID(t)
			peer, _ := fs.fileHandles.Get(peerID)
			// Pending B is inherited by A, so retiring B requires remote proof
			// of A's landed descendant. The error path must preserve B's image.
			if unavailable {
				if n, status := pr939Append(fs, ino, peerID, "B"); n != 1 || status != gofuse.OK {
					t.Fatalf("initial peer append=%d/%v", n, status)
				}
			}
			if n, status := pr939Append(fs, ino, sourceID, "A"); n != 1 || status != gofuse.OK {
				t.Fatalf("source append=%d/%v", n, status)
			}
			if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID}); status != gofuse.OK {
				t.Fatalf("source Fsync=%v", status)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID})
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, present := fs.fileHandles.Get(sourceID); present {
				t.Fatal("source Release did not remove the open source")
			}
			_, remoteBefore, _ := server.snapshot()
			wantBefore := "baseA"
			if unavailable {
				wantBefore = "baseBA"
			}
			if string(remoteBefore) != wantBefore {
				t.Fatalf("source committed=%q, want %q", remoteBefore, wantBefore)
			}

			if unavailable {
				peer.Lock()
				buffer, seq, version, base := peer.Dirty, peer.DirtySeq, peer.Dirty.contentVersion, peer.BaseRev
				before := string(peer.Dirty.Bytes())
				contentID, stagedID, stagedSeq := peer.ContentSnapshotID, peer.StagedSnapshotID, peer.StagedSnapshotSeq
				ancestors := append([]string(nil), peer.contentAncestors...)
				peer.Unlock()
				denyProof.Store(true)
				n, status := pr939Append(fs, ino, peerID, "C")
				if n != 0 || status == gofuse.OK {
					t.Errorf("unproved retained append acknowledged=%d/%v, want zero bytes and failure", n, status)
				}
				peer.Lock()
				preserved := peer.Dirty == buffer && peer.DirtySeq == seq && peer.Dirty.contentVersion == version && peer.BaseRev == base &&
					string(peer.Dirty.Bytes()) == before && peer.ContentSnapshotID == contentID && peer.StagedSnapshotID == stagedID &&
					peer.StagedSnapshotSeq == stagedSeq && slices.Equal(peer.contentAncestors, ancestors)
				peer.Unlock()
				if !preserved {
					t.Error("unavailable identity proof changed the retained dirty image or its ownership")
				}
				if _, after, _ := server.snapshot(); string(after) != string(remoteBefore) {
					t.Error("rejected retained append changed remote content")
				}
				return
			}

			if n, status := pr939Append(fs, ino, peerID, "B"); n != 1 || status != gofuse.OK {
				t.Fatalf("retained peer append=%d/%v, want full successful write", n, status)
			}
			if got := pr939HandleBytes(peer); got != "baseAB" {
				t.Errorf("ACKed peer image=%q, want baseAB; stale EOF acknowledgment lost A", got)
			}
			if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: peerID}); status != gofuse.OK {
				t.Errorf("actual peer Fsync after successful write=%v", status)
			} else {
				if status := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: peerID}); status != gofuse.OK {
					t.Errorf("peer close Flush=%v", status)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: peerID})
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, body, _ := server.snapshot(); string(body) != "baseAB" {
				t.Errorf("remote acknowledged records=%q, want baseAB", body)
			}
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Errorf("pending conflicts=%d", conflicts)
			}
		})
	}
}

func TestIssue1023MissingRIDRefreshRechecksConcurrentObservations(t *testing.T) {
	for _, scenario := range []string{"different_identity", "added_alias", "advanced_floor", "same_identity_link_count"} {
		t.Run(scenario, func(t *testing.T) {
			var fs *Dat9FS
			var ino uint64
			var armed atomic.Bool
			var headCount atomic.Int32
			var mutationOnce sync.Once
			var mutated atomic.Bool
			var firstPath string
			var raisedFloor int64
			const addedPath = "/empty-rid-newmember"
			probe := func(_ string) {
				if !armed.Load() || headCount.Add(1) != 2 {
					return
				}
				mutationOnce.Do(func() {
					switch scenario {
					case "different_identity":
						// Model a newer normal stat/lookup observation. The local
						// path-to-inode bindings remain unchanged.
						fs.inodes.SetIdentityWithKind(ino, "newly-observed-resource", 5, false)
					case "added_alias":
						entry, ok := fs.inodes.GetEntry(ino)
						if !ok || !fs.inodes.AddAlias(ino, addedPath, "", 3, false, entry.Size, time.Now()) {
							t.Error("concurrent alias fixture could not attach the new member")
						}
					case "advanced_floor":
						raisedFloor = fs.latestCommittedRevision(firstPath) + 1
						// The first HEAD was already checked against the older
						// fence; the second HEAD does not observe this pathname.
						fs.recordCommittedRevisionWithSize(firstPath, raisedFloor, 6)
					case "same_identity_link_count":
						// Same proven RID is legitimate concurrent refinement;
						// an older cached nlink must not overwrite the new count.
						fs.inodes.SetIdentityWithKind(ino, "confirmed-link-shared-resource", 5, false)
					}
					mutated.Store(true)
				})
			}
			var server *casFileServer
			var sourceID, peerID uint64
			fs, ino, server, sourceID, peerID, _ = issue1023LinkWithoutConfirmationRID(t, probe)
			peer, _ := fs.fileHandles.Get(peerID)
			if n, status := pr939Append(fs, ino, sourceID, "A"); n != 1 || status != gofuse.OK {
				t.Fatalf("source append=%d/%v", n, status)
			}
			if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID}); status != gofuse.OK {
				t.Fatalf("source Fsync=%v", status)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID})
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, body, _ := server.snapshot(); string(body) != "baseA" {
				t.Fatalf("source committed=%q, want baseA", body)
			}
			entry, ok := fs.inodes.GetEntry(ino)
			if !ok || entry.ResourceID != "" {
				t.Fatal("RID was not missing before the post-commit identity probes")
			}
			paths := fs.appendReadPaths(ino)
			if len(paths) != 2 || fs.latestCommittedRevision(peer.Path) <= peer.BaseRev {
				t.Fatal("locally fenced retained alias did not require identity recovery")
			}
			firstPath = paths[0]
			peer.Lock()
			buffer, seq, version, base := peer.Dirty, peer.DirtySeq, peer.Dirty.contentVersion, peer.BaseRev
			contentID, stagedID, stagedSeq := peer.ContentSnapshotID, peer.StagedSnapshotID, peer.StagedSnapshotSeq
			ancestors := append([]string(nil), peer.contentAncestors...)
			before := string(peer.Dirty.Bytes())
			armed.Store(true)
			err := fs.refreshUnidentifiedAppendAliasesLocked(context.Background(), peer)
			preserved := peer.Dirty == buffer && peer.DirtySeq == seq && peer.Dirty.contentVersion == version && peer.BaseRev == base &&
				peer.ContentSnapshotID == contentID && peer.StagedSnapshotID == stagedID && peer.StagedSnapshotSeq == stagedSeq &&
				slices.Equal(peer.contentAncestors, ancestors) && string(peer.Dirty.Bytes()) == before
			peer.Unlock()
			armed.Store(false)
			if !mutated.Load() || headCount.Load() < 2 {
				t.Fatal("the second post-commit network HEAD did not exercise the mutation")
			}
			if !preserved {
				t.Error("identity observation changed the retained buffer or CAS ownership")
			}
			current, ok := fs.inodes.GetEntry(ino)
			if !ok {
				t.Fatal("inode disappeared during identity observation")
			}
			if scenario == "same_identity_link_count" {
				if err != nil || current.ResourceID != "confirmed-link-shared-resource" || current.Nlink != 5 {
					t.Errorf("same-RID refinement err=%v RID=%q nlink=%d, want nil/shared/5", err, current.ResourceID, current.Nlink)
				}
				return
			}
			if !errors.Is(err, syscall.EAGAIN) {
				t.Errorf("changed identity/members/floor accepted err=%v, want EAGAIN", err)
			}
			switch scenario {
			case "different_identity":
				if current.ResourceID != "newly-observed-resource" || current.Nlink != 5 {
					t.Errorf("stale HEAD replaced newer identity/count: RID=%q nlink=%d", current.ResourceID, current.Nlink)
				}
			case "added_alias":
				if current.ResourceID != "" || current.Nlink != 3 || !fs.sameLinkedInode(addedPath, ino) {
					t.Errorf("unobserved alias was promoted or lost: RID=%q nlink=%d", current.ResourceID, current.Nlink)
				}
				if proof := fs.commitQueue.landedCommit(addedPath); proof.snapshotID != "" || proof.checksum != "" {
					t.Error("unobserved new alias received a remote content proof")
				}
			case "advanced_floor":
				if current.ResourceID != "" || fs.latestCommittedRevision(firstPath) != raisedFloor {
					t.Errorf("behind-floor identity promoted or fence changed: RID=%q floor=%d want=%d", current.ResourceID, fs.latestCommittedRevision(firstPath), raisedFloor)
				}
			}
		})
	}
}
