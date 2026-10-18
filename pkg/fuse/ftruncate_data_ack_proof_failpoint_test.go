//go:build failpoint

package fuse

import (
	"slices"
	"testing"

	"github.com/pingcap/failpoint"
)

func TestWriteBackDataProofPrecedesMutationRetirement(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	hardlinkWrite(t, fs, ino, b, "H")
	child, _ := fs.fileHandles.Get(b)
	child.Lock()
	if err := fs.stageShadowLocked(child, true); err != nil {
		child.Unlock()
		t.Fatal(err)
	}
	fs.setPendingModeLocked(child, 0644, fs.nextPendingModeGen())
	err := fs.snapshotWriteBackLocked(child, true)
	child.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	meta, ok := fs.writeBack.GetMeta(child.Path)
	if !ok || !meta.ownedStagingKnown || meta.ownedStagingGens.PendingIndexGen == 0 || meta.ownedStagingGens.ShadowGen == 0 {
		t.Fatal("missing actual staged ownership for the captured upload")
	}
	point := "github.com/mem9-ai/drive9/pkg/fuse/writeBackDataCommittedProofReady"
	reached := false
	if err := failpoint.EnableCall(point, func(got *Dat9FS, uploaded WriteBackMeta, revision int64) {
		if got != fs || uploaded.SnapshotID != meta.SnapshotID {
			return
		}
		reached = true
		proof := fs.landedAppendCommit(meta.Path)
		if revision != 3 || remote() != "Hello" || proof.rev != revision || proof.snapshotID != meta.SnapshotID || proof.checksum != payloadChecksum([]byte("Hello")) || !slices.Contains(proof.ancestors, child.pendingFtruncate.Load().id) {
			t.Error("data ACK reached mutation publication without its exact landed proof")
		}
		fs.dirtyMu.Lock()
		committed := fs.mutationInodes[ino]
		fs.dirtyMu.Unlock()
		if committed.committedSeq >= meta.MutationSeq {
			t.Error("mutation watermark became visible before the proof boundary")
		}
		if fs.pendingIndex.Generation(meta.Path) != meta.ownedStagingGens.PendingIndexGen || fs.shadowStore.ActiveGeneration(meta.Path) != meta.ownedStagingGens.ShadowGen {
			t.Error("captured staging retired before the proof boundary")
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	uploader := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", OnDataCommitted: fs.onWriteBackDataCommitted, OnSuccess: fs.onWriteBackUploadSuccess, SnapshotStagingGens: fs.snapshotStagingGens}
	uploader.uploadOne(child.Path)
	if !reached || fs.ftruncateCommittedSeq(ino) < meta.MutationSeq {
		t.Fatal("did not observe proof followed by mutation publication")
	}
	if fs.pendingIndex.Generation(meta.Path) != 0 || fs.shadowStore.ActiveGeneration(meta.Path) != 0 {
		t.Fatal("data ACK did not retire its captured staging after proof publication")
	}
	if pending, ok := fs.writeBack.GetMeta(meta.Path); !ok || pending.Kind != PendingChmod {
		t.Fatal("later chmod failure lost its independent pending obligation")
	}
}
