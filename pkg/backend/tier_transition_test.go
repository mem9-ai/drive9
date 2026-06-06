package backend

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/meta"
)

func TestBackendTierTransitionInlineToS3ToInlineCleansStorage(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.storageNamespaceID = "ns-tier-transition"

	ctx := context.Background()
	path := "/tier-transition.bin"
	initialInline := deterministicTierPayload(10*1024, 0x11)
	largeS3 := deterministicTierPayload(8*1024*1024, 0x42)
	finalInline := deterministicTierPayload(10*1024, 0x7d)

	if n, err := b.Write(path, initialInline, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create inline file: %v", err)
	} else if n != int64(len(initialInline)) {
		t.Fatalf("create bytes = %d, want %d", n, len(initialInline))
	}
	assertBackendVisibleBytes(t, b, path, initialInline)
	initial := loadBackendTierState(t, b, path)
	assertInlineTierState(t, initial, initialInline)

	if n, err := b.Write(path, largeS3, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite inline -> s3: %v", err)
	} else if n != int64(len(largeS3)) {
		t.Fatalf("inline -> s3 bytes = %d, want %d", n, len(largeS3))
	}
	assertBackendVisibleBytes(t, b, path, largeS3)
	large := loadBackendTierState(t, b, path)
	assertS3TierState(t, large, largeS3)
	assertS3ObjectBytes(t, b, large.storageRef, largeS3)

	if large.fileID != initial.fileID {
		t.Fatalf("file id changed across overwrite: initial=%q large=%q", initial.fileID, large.fileID)
	}
	if large.storageRef == "" || large.storageRef == initial.storageRef {
		t.Fatalf("large storage ref = %q, initial ref = %q", large.storageRef, initial.storageRef)
	}

	if n, err := b.Write(path, finalInline, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite s3 -> inline: %v", err)
	} else if n != int64(len(finalInline)) {
		t.Fatalf("s3 -> inline bytes = %d, want %d", n, len(finalInline))
	}
	assertBackendVisibleBytes(t, b, path, finalInline)
	final := loadBackendTierState(t, b, path)
	assertInlineTierState(t, final, finalInline)

	stillReferenced, err := b.Store().HasConfirmedS3StorageRef(ctx, datastore.StorageRefHash(large.storageRef), large.storageRef)
	if err != nil {
		t.Fatalf("check old s3 reference: %v", err)
	}
	if stillReferenced {
		t.Fatalf("old s3 ref %q is still referenced by confirmed content", large.storageRef)
	}
	assertOverwriteGCCandidate(t, fake, "ns-tier-transition", large.storageRef)
}

func TestBackendTierTransitionMultipleS3OverwritesQueueAllObsoleteRefs(t *testing.T) {
	backendFS, fake := newCentralQuotaBackend(t)
	backendFS.storageNamespaceID = "ns-tier-transition-multi"

	ctx := context.Background()
	path := "/tier-transition-multi.bin"
	initialInline := deterministicTierPayload(10*1024, 0x13)
	largeA := deterministicTierPayload(8*1024*1024, 0x24)
	largeB := deterministicTierPayload(8*1024*1024, 0x35)
	finalInline := deterministicTierPayload(10*1024, 0x46)

	if _, err := backendFS.Write(path, initialInline, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create inline file: %v", err)
	}
	if _, err := backendFS.Write(path, largeA, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite inline -> s3 A: %v", err)
	}
	stateA := loadBackendTierState(t, backendFS, path)
	assertS3TierState(t, stateA, largeA)
	assertS3ObjectBytes(t, backendFS, stateA.storageRef, largeA)

	if _, err := backendFS.Write(path, largeB, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite s3 A -> s3 B: %v", err)
	}
	stateB := loadBackendTierState(t, backendFS, path)
	assertS3TierState(t, stateB, largeB)
	assertS3ObjectBytes(t, backendFS, stateB.storageRef, largeB)
	if stateB.storageRef == stateA.storageRef {
		t.Fatalf("s3 overwrite reused storage ref %q", stateB.storageRef)
	}

	if _, err := backendFS.Write(path, finalInline, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite s3 B -> inline: %v", err)
	}
	finalState := loadBackendTierState(t, backendFS, path)
	assertInlineTierState(t, finalState, finalInline)
	assertBackendVisibleBytes(t, backendFS, path, finalInline)

	for _, obsoleteRef := range []string{stateA.storageRef, stateB.storageRef} {
		stillReferenced, err := backendFS.Store().HasConfirmedS3StorageRef(ctx, datastore.StorageRefHash(obsoleteRef), obsoleteRef)
		if err != nil {
			t.Fatalf("check obsolete s3 reference %q: %v", obsoleteRef, err)
		}
		if stillReferenced {
			t.Fatalf("obsolete s3 ref %q is still referenced by confirmed content", obsoleteRef)
		}
		assertOverwriteGCCandidate(t, fake, "ns-tier-transition-multi", obsoleteRef)
	}
	assertOverwriteGCCandidateCount(t, fake, "ns-tier-transition-multi", []string{stateA.storageRef, stateB.storageRef})
}

type backendTierState struct {
	fileID            string
	storageType       datastore.StorageType
	storageRef        string
	sizeBytes         int64
	checksum          string
	contentBlob       []byte
	legacyStorageType datastore.StorageType
	legacyStorageRef  string
	legacySizeBytes   int64
	legacyChecksum    string
	legacyContentBlob []byte
}

func loadBackendTierState(t *testing.T, b *Dat9Backend, path string) backendTierState {
	t.Helper()

	ctx := context.Background()
	nf, err := b.Store().Stat(ctx, path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if nf.File == nil {
		t.Fatalf("stat %s returned no file entity", path)
	}

	state := backendTierState{
		fileID:      nf.File.FileID,
		storageType: nf.File.StorageType,
		storageRef:  nf.File.StorageRef,
		sizeBytes:   nf.File.SizeBytes,
		checksum:    nf.File.ChecksumSHA256,
		contentBlob: append([]byte(nil), nf.File.ContentBlob...),
	}

	var legacyStorageType, legacyStorageRef, legacyChecksum sql.NullString
	var legacyContentBlob []byte
	if err := b.Store().DB().QueryRowContext(ctx, `SELECT storage_type, storage_ref, size_bytes, checksum_sha256, content_blob
		FROM files WHERE file_id = ?`, state.fileID).Scan(
		&legacyStorageType, &legacyStorageRef, &state.legacySizeBytes, &legacyChecksum, &legacyContentBlob,
	); err != nil {
		t.Fatalf("read legacy files row: %v", err)
	}
	state.legacyStorageType = datastore.StorageType(legacyStorageType.String)
	state.legacyStorageRef = legacyStorageRef.String
	state.legacyChecksum = legacyChecksum.String
	state.legacyContentBlob = append([]byte(nil), legacyContentBlob...)

	return state
}

func assertInlineTierState(t *testing.T, state backendTierState, want []byte) {
	t.Helper()

	wantChecksum := sha256sum(want)
	if state.storageType != datastore.StorageDB9 {
		t.Fatalf("split storage type = %s, want %s", state.storageType, datastore.StorageDB9)
	}
	if state.storageRef != "inline" {
		t.Fatalf("split storage ref = %q, want inline", state.storageRef)
	}
	if state.sizeBytes != int64(len(want)) {
		t.Fatalf("split size = %d, want %d", state.sizeBytes, len(want))
	}
	if state.checksum != wantChecksum {
		t.Fatalf("split checksum = %q, want %q", state.checksum, wantChecksum)
	}
	if !bytes.Equal(state.contentBlob, want) {
		t.Fatalf("split content blob mismatch: got %d bytes, want %d", len(state.contentBlob), len(want))
	}
	if state.legacyStorageType != datastore.StorageDB9 {
		t.Fatalf("legacy storage type = %s, want %s", state.legacyStorageType, datastore.StorageDB9)
	}
	if state.legacyStorageRef != "inline" {
		t.Fatalf("legacy storage ref = %q, want inline", state.legacyStorageRef)
	}
	if state.legacySizeBytes != int64(len(want)) {
		t.Fatalf("legacy size = %d, want %d", state.legacySizeBytes, len(want))
	}
	if state.legacyChecksum != wantChecksum {
		t.Fatalf("legacy checksum = %q, want %q", state.legacyChecksum, wantChecksum)
	}
	if !bytes.Equal(state.legacyContentBlob, want) {
		t.Fatalf("legacy content blob mismatch: got %d bytes, want %d", len(state.legacyContentBlob), len(want))
	}
}

func assertS3TierState(t *testing.T, state backendTierState, want []byte) {
	t.Helper()

	wantChecksum := sha256sum(want)
	if state.storageType != datastore.StorageS3 {
		t.Fatalf("split storage type = %s, want %s", state.storageType, datastore.StorageS3)
	}
	if state.storageRef == "" || state.storageRef == "inline" {
		t.Fatalf("split storage ref = %q, want s3 object ref", state.storageRef)
	}
	if state.sizeBytes != int64(len(want)) {
		t.Fatalf("split size = %d, want %d", state.sizeBytes, len(want))
	}
	if state.checksum != wantChecksum {
		t.Fatalf("split checksum = %q, want %q", state.checksum, wantChecksum)
	}
	if len(state.contentBlob) != 0 {
		t.Fatalf("split content blob len = %d, want 0 for s3", len(state.contentBlob))
	}
	if state.legacyStorageType != datastore.StorageS3 {
		t.Fatalf("legacy storage type = %s, want %s", state.legacyStorageType, datastore.StorageS3)
	}
	if state.legacyStorageRef != state.storageRef {
		t.Fatalf("legacy storage ref = %q, want split ref %q", state.legacyStorageRef, state.storageRef)
	}
	if state.legacySizeBytes != int64(len(want)) {
		t.Fatalf("legacy size = %d, want %d", state.legacySizeBytes, len(want))
	}
	if state.legacyChecksum != wantChecksum {
		t.Fatalf("legacy checksum = %q, want %q", state.legacyChecksum, wantChecksum)
	}
	if len(state.legacyContentBlob) != 0 {
		t.Fatalf("legacy content blob len = %d, want 0 for s3", len(state.legacyContentBlob))
	}
}

func assertBackendVisibleBytes(t *testing.T, b *Dat9Backend, path string, want []byte) {
	t.Helper()

	info, err := b.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if info.Size != int64(len(want)) {
		t.Fatalf("visible size = %d, want %d", info.Size, len(want))
	}
	got, err := b.Read(path, 0, int64(len(want)+1))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("visible bytes mismatch: got %d bytes checksum=%s want %d bytes checksum=%s",
			len(got), sha256sum(got), len(want), sha256sum(want))
	}
}

func assertS3ObjectBytes(t *testing.T, b *Dat9Backend, storageRef string, want []byte) {
	t.Helper()

	rc, err := b.S3().GetObject(context.Background(), storageRef)
	if err != nil {
		t.Fatalf("get s3 object %q: %v", storageRef, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read s3 object %q: %v", storageRef, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("s3 object %q mismatch: got %d bytes checksum=%s want %d bytes checksum=%s",
			storageRef, len(got), sha256sum(got), len(want), sha256sum(want))
	}
}

func assertOverwriteGCCandidate(t *testing.T, fake *fakeMetaQuotaStore, namespaceID, storageRef string) {
	t.Helper()

	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, candidate := range fake.objectGCCandidates {
		if candidate.NamespaceID == namespaceID &&
			candidate.StorageRef == storageRef &&
			candidate.StorageRefHash == datastore.StorageRefHash(storageRef) &&
			candidate.Reason == meta.ObjectGCReasonOverwrite {
			return
		}
	}
	t.Fatalf("missing overwrite gc candidate for %q in namespace %q: %+v", storageRef, namespaceID, fake.objectGCCandidates)
}

func assertOverwriteGCCandidateCount(t *testing.T, fake *fakeMetaQuotaStore, namespaceID string, storageRefs []string) {
	t.Helper()

	want := make(map[string]int, len(storageRefs))
	for _, storageRef := range storageRefs {
		want[storageRef]++
	}
	got := make(map[string]int, len(storageRefs))
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, candidate := range fake.objectGCCandidates {
		if candidate.NamespaceID == namespaceID && candidate.Reason == meta.ObjectGCReasonOverwrite {
			got[candidate.StorageRef]++
		}
	}
	for storageRef, count := range want {
		if got[storageRef] != count {
			t.Fatalf("overwrite gc candidate count for %q = %d, want %d; all=%+v",
				storageRef, got[storageRef], count, fake.objectGCCandidates)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("overwrite gc candidate refs = %+v, want exactly %+v", got, want)
	}
}

func deterministicTierPayload(size int, seed byte) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte((i*31 + int(seed)) % 251)
	}
	return out
}
