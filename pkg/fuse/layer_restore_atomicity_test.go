package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

var errInjectedLayerRestoreMeta = errors.New("injected layer restore metadata durability failure")
var errInjectedLayerRestoreCommit = errors.New("injected layer restore commit-point failure")

func seedCommittedLayerCache(t *testing.T, shadows *ShadowStore, pending *PendingIndex, path string, data []byte, baseRev int64, mode uint32) WriteBackMeta {
	t.Helper()
	if err := shadows.WriteFull(path, data, baseRev); err != nil {
		t.Fatalf("seed shadow %s: %v", path, err)
	}
	gen, err := pending.PutWithBaseRevAndMode(path, int64(len(data)), PendingOverwrite, baseRev, mode, true)
	if err != nil {
		t.Fatalf("seed pending %s: %v", path, err)
	}
	if marked, err := pending.MarkLayerCommitted(path, gen, baseRev); err != nil || !marked {
		t.Fatalf("mark %s committed = (%t, %v)", path, marked, err)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatalf("seed pending %s disappeared", path)
	}
	return *meta
}

// failNextLayerRestoreMetaAfterReplacement models the hardest atomicWrite
// failure: rename installed the new .meta, but its directory fsync reported an
// error. The layer transaction must restore both the old metadata and shadow.
func failNextLayerRestoreMetaAfterReplacement(t *testing.T, pending *PendingIndex) {
	t.Helper()
	failed := false
	pending.restoreMetaWrite = func(path string, data []byte) error {
		if !failed {
			failed = true
			if err := atomicWrite(path, data); err != nil {
				return err
			}
			return errInjectedLayerRestoreMeta
		}
		return atomicWrite(path, data)
	}
	t.Cleanup(func() { pending.restoreMetaWrite = nil })
}

func assertLayerRestoreCacheState(t *testing.T, shadows *ShadowStore, pending *PendingIndex, path string, wantData []byte, wantMeta WriteBackMeta) {
	t.Helper()
	got, err := shadows.ReadAll(path)
	if err != nil {
		t.Fatalf("read shadow %s: %v", path, err)
	}
	if !bytes.Equal(got, wantData) {
		t.Fatalf("shadow %s = %q, want %q", path, got, wantData)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatalf("pending %s missing", path)
	}
	if meta.Generation != wantMeta.Generation || meta.BaseRev != wantMeta.BaseRev || meta.Size != wantMeta.Size || meta.Mode != wantMeta.Mode || meta.LayerCommitted != wantMeta.LayerCommitted {
		t.Fatalf("pending %s = %+v, want generation/base/size/mode/committed from %+v", path, meta, wantMeta)
	}
}

func reopenLayerRestoreCache(t *testing.T, shadowDir, pendingDir string) (*ShadowStore, *PendingIndex) {
	t.Helper()
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatalf("reopen pending index: %v", err)
	}
	if err := pending.RecoverFromDisk(); err != nil {
		t.Fatalf("recover pending index: %v", err)
	}
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatalf("reopen shadow store: %v", err)
	}
	t.Cleanup(shadows.Close)
	return shadows, pending
}

func assertNoLayerRestoreArtifacts(t *testing.T, dirs ...string) {
	t.Helper()
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.Contains(entry.Name(), "layer-restore") {
				t.Fatalf("unfinished layer restore artifact: %s", filepath.Join(dir, entry.Name()))
			}
		}
	}
}

func TestRestoreLayerInlineMetadataFailureRollsBackShadowAndRestart(t *testing.T) {
	const path = "/shared.txt"
	oldData := []byte("old-inline")
	newData := []byte("new-inline")
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/shared.txt", Op: "upsert", Kind: "file",
		Content: newData, SizeBytes: int64(len(newData)), BaseRevision: 12, Mode: 0o600, EntrySeq: 2,
	}
	ts := newLayerRestoreAtomicityServer(t, entry, nil)
	defer ts.Close()

	shadowDir, pendingDir := t.TempDir(), t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	oldMeta := seedCommittedLayerCache(t, shadows, pending, path, oldData, 11, 0o640)
	failNextLayerRestoreMetaAfterReplacement(t, pending)

	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}, shadows, pending, nil)
	if !errors.Is(err, errInjectedLayerRestoreMeta) {
		t.Fatalf("restore error = %v, want injected metadata failure", err)
	}
	assertLayerRestoreCacheState(t, shadows, pending, path, oldData, oldMeta)
	shadows.Close()
	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	assertLayerRestoreCacheState(t, restartedShadows, restartedPending, path, oldData, oldMeta)
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestRestoreLayerS3MetadataFailureRollsBackShadowAndRestart(t *testing.T) {
	const path = "/shared.bin"
	oldData := []byte("old-object")
	newData := bytes.Repeat([]byte("s3"), 70*1024)
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/shared.bin", Op: "upsert", Kind: "file",
		StorageType: "s3", StorageRef: "layers/layer-1/shared", SizeBytes: int64(len(newData)), BaseRevision: 22, Mode: 0o600, EntrySeq: 2,
	}
	ts := newLayerRestoreAtomicityServer(t, entry, newData)
	defer ts.Close()

	shadowDir, pendingDir := t.TempDir(), t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	oldMeta := seedCommittedLayerCache(t, shadows, pending, path, oldData, 21, 0o640)
	failNextLayerRestoreMetaAfterReplacement(t, pending)

	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}, shadows, pending, nil)
	if !errors.Is(err, errInjectedLayerRestoreMeta) {
		t.Fatalf("restore error = %v, want injected metadata failure", err)
	}
	assertLayerRestoreCacheState(t, shadows, pending, path, oldData, oldMeta)
	shadows.Close()
	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	assertLayerRestoreCacheState(t, restartedShadows, restartedPending, path, oldData, oldMeta)
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestRestoreLayerRenameMetadataFailureRestoresSourceTargetAndRestart(t *testing.T) {
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/from.txt", Op: "rename", Kind: "file",
		ContentText: "/repo/to.txt", Mode: 0o600, EntrySeq: 2,
	}
	ts := newLayerRestoreAtomicityServer(t, entry, nil)
	defer ts.Close()

	shadowDir, pendingDir := t.TempDir(), t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	fromData, toData := []byte("source-old"), []byte("target-old")
	fromMeta := seedCommittedLayerCache(t, shadows, pending, "/from.txt", fromData, 31, 0o640)
	toMeta := seedCommittedLayerCache(t, shadows, pending, "/to.txt", toData, 41, 0o644)
	failNextLayerRestoreMetaAfterReplacement(t, pending)
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"})

	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}, shadows, pending, fs)
	if !errors.Is(err, errInjectedLayerRestoreMeta) {
		t.Fatalf("restore error = %v, want injected metadata failure", err)
	}
	assertLayerRestoreCacheState(t, shadows, pending, "/from.txt", fromData, fromMeta)
	assertLayerRestoreCacheState(t, shadows, pending, "/to.txt", toData, toMeta)
	if fs.isLayerWhiteout("/from.txt") {
		t.Fatal("failed rename exposed source whiteout")
	}
	shadows.Close()
	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	assertLayerRestoreCacheState(t, restartedShadows, restartedPending, "/from.txt", fromData, fromMeta)
	assertLayerRestoreCacheState(t, restartedShadows, restartedPending, "/to.txt", toData, toMeta)
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestRestoreLayerDirectoryRenameGroupFailureRestoresInlineObjectAndExistingTarget(t *testing.T) {
	inlineData := []byte("inline-source")
	objectData := bytes.Repeat([]byte("object-source-"), 8*1024)
	targetData := []byte("existing-target")
	inlineEntry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/old/inline.txt", Op: "upsert", Kind: "file",
		Content: inlineData, SizeBytes: int64(len(inlineData)), BaseRevision: 11, Mode: 0o640, EntrySeq: 1,
	}
	objectEntry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/old/object.bin", Op: "upsert", Kind: "file",
		StorageType: "s3", StorageRef: "objects/source", SizeBytes: int64(len(objectData)), BaseRevision: 12, Mode: 0o600, EntrySeq: 2,
	}
	renameEntry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/old/", Op: "rename", Kind: "dir",
		ContentText: "/repo/new/", Mode: 0o750, EntrySeq: 3,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{inlineEntry, objectEntry, renameEntry}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			switch r.URL.Query().Get("path") {
			case inlineEntry.Path:
				_ = json.NewEncoder(w).Encode(inlineEntry)
			case objectEntry.Path:
				_ = json.NewEncoder(w).Encode(objectEntry)
			default:
				t.Errorf("unexpected layer entry path: %s", r.URL.RawQuery)
				w.WriteHeader(http.StatusNotFound)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/objects" && r.URL.Query().Get("path") == objectEntry.Path:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(objectData)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadowDir, pendingDir := t.TempDir(), t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	targetMeta := seedCommittedLayerCache(t, shadows, pending, "/new/object.bin", targetData, 7, 0o644)

	// The two upserts first become committed retained entries. Fail only after
	// the directory group has already moved/written the inline descendant and
	// is installing the object descendant over an existing target.
	failingMetaPath := filepath.Join(pendingDir, hashPath("/new/object.bin")+".meta")
	pending.restoreMetaWrite = func(path string, data []byte) error {
		if path == failingMetaPath {
			if err := atomicWrite(path, data); err != nil {
				return err
			}
			return errInjectedLayerRestoreMeta
		}
		return atomicWrite(path, data)
	}
	t.Cleanup(func() { pending.restoreMetaWrite = nil })

	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}, shadows, pending, nil)
	if !errors.Is(err, errInjectedLayerRestoreMeta) {
		t.Fatalf("directory restore error = %v, want injected second-descendant failure", err)
	}
	for path, want := range map[string][]byte{
		"/old/inline.txt": inlineData,
		"/old/object.bin": objectData,
		"/new/object.bin": targetData,
	} {
		got, readErr := shadows.ReadAll(path)
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("shadow %s after group rollback = %q, %v; want %q", path, got, readErr, want)
		}
		if meta, ok := pending.GetMeta(path); !ok || !meta.LayerCommitted {
			t.Fatalf("pending %s after group rollback = %+v, want committed", path, meta)
		}
	}
	if shadows.Has("/new/inline.txt") || pending.HasPending("/new/inline.txt") {
		t.Fatal("partially migrated inline descendant remained at target")
	}
	if meta, ok := pending.GetMeta("/new/object.bin"); !ok || meta.Generation != targetMeta.Generation || meta.BaseRev != targetMeta.BaseRev {
		t.Fatalf("existing target meta after group rollback = %+v, want %+v", meta, targetMeta)
	}

	shadows.Close()
	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	for path, want := range map[string][]byte{
		"/old/inline.txt": inlineData,
		"/old/object.bin": objectData,
		"/new/object.bin": targetData,
	} {
		got, readErr := restartedShadows.ReadAll(path)
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("restarted shadow %s = %q, %v; want %q", path, got, readErr, want)
		}
		if _, ok := restartedPending.GetMeta(path); !ok {
			t.Fatalf("restarted pending %s missing", path)
		}
	}
	if restartedShadows.Has("/new/inline.txt") || restartedPending.HasPending("/new/inline.txt") {
		t.Fatal("restart exposed partially migrated inline descendant")
	}
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestRestoreLayerDirectoryRenameGroupReplacePreservesPinnedTargetAndAccounting(t *testing.T) {
	shadows, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadows.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const sourcePath = "/old/file.txt"
	const targetPath = "/new/file.txt"
	sourceData := []byte("source replacement")
	targetData := []byte("pinned target snapshot")
	sourceMeta := seedCommittedLayerCache(t, shadows, pending, sourcePath, sourceData, 11, 0o640)
	targetMeta := seedCommittedLayerCache(t, shadows, pending, targetPath, targetData, 12, 0o600)
	targetPin := shadows.Pin(targetPath)
	defer shadows.Unpin(targetPin)
	if !shadows.canReadGeneration(targetPin, targetMeta.BaseRev, targetMeta.Generation) {
		t.Fatal("target pin was not readable before group replace")
	}

	oldDiskPath := shadows.shadowPath(sourcePath)
	newDiskPath := shadows.shadowPath(targetPath)
	shadows.mu.Lock()
	shadows.recoveredSizes = make(map[string]int64)
	shadows.recoveredSizes[oldDiskPath] = int64(len(sourceData))
	shadows.recoveredSizes[newDiskPath] = int64(len(targetData))
	shadows.mu.Unlock()
	beforePending := shadows.PendingBytes()
	beforeQuota := shadows.quotaBytes.Load()

	moved, err := restoreLayerRenameGroupIfGenerations(shadows, pending, []layerRestoreRenameMove{{
		oldPath:               sourcePath,
		newPath:               targetPath,
		expectedOldPendingGen: sourceMeta.Generation,
		expectedNewPendingGen: targetMeta.Generation,
		expectedOldShadowGen:  shadows.ActiveGeneration(sourcePath),
		expectedNewShadowGen:  shadows.ActiveGeneration(targetPath),
	}})
	if err != nil || !moved {
		t.Fatalf("group replace = (%t, %v), want success", moved, err)
	}
	if got, readErr := shadows.ReadAll(targetPath); readErr != nil || !bytes.Equal(got, sourceData) {
		t.Fatalf("active target after group replace = %q, %v; want %q", got, readErr, sourceData)
	}
	if !shadows.canReadGeneration(targetPin, targetMeta.BaseRev, targetMeta.Generation) {
		t.Fatal("group replace invalidated the already-open target snapshot")
	}
	buf := make([]byte, len(targetData))
	if n, readErr := shadows.ReadAtGen(targetPin, 0, buf); readErr != nil || n != len(buf) || !bytes.Equal(buf, targetData) {
		t.Fatalf("pinned target after group replace = %q, n=%d, err=%v; want %q", buf[:max(0, n)], n, readErr, targetData)
	}
	if got, want := shadows.PendingBytes(), beforePending-int64(len(targetData)); got != want {
		t.Fatalf("pending bytes after group replace = %d, want %d", got, want)
	}
	if got, want := shadows.quotaBytes.Load(), beforeQuota-int64(len(targetData)); got != want {
		t.Fatalf("quota bytes after group replace = %d, want %d", got, want)
	}
	shadows.mu.RLock()
	_, oldRecovered := shadows.recoveredSizes[oldDiskPath]
	_, newRecovered := shadows.recoveredSizes[newDiskPath]
	shadows.mu.RUnlock()
	if oldRecovered || newRecovered {
		t.Fatalf("group replace retained recovery accounting: old=%t new=%t", oldRecovered, newRecovered)
	}
}

func TestRestoreLayerDirectoryRenameGroupRollbackFailureFreezesMount(t *testing.T) {
	entries := []client.FSLayerEntry{
		{LayerID: "layer-1", Path: "/repo/old/a.txt", Op: "upsert", Kind: "file", Content: []byte("a"), SizeBytes: 1, EntrySeq: 1},
		{LayerID: "layer-1", Path: "/repo/old/b.txt", Op: "upsert", Kind: "file", Content: []byte("b"), SizeBytes: 1, EntrySeq: 2},
		{LayerID: "layer-1", Path: "/repo/old/", Op: "rename", Kind: "dir", ContentText: "/repo/new/", EntrySeq: 3},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
		case "/v1/layers/layer-1/entries":
			for _, entry := range entries[:2] {
				if entry.Path == r.URL.Query().Get("path") {
					_ = json.NewEncoder(w).Encode(entry)
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadowDir := t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pendingDir := t.TempDir()
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	failingMetaPath := filepath.Join(pendingDir, hashPath("/new/b.txt")+".meta")
	pending.restoreMetaWrite = func(path string, data []byte) error {
		if path == failingMetaPath {
			return errInjectedLayerRestoreMeta
		}
		return atomicWrite(path, data)
	}
	pending.restoreTxnMarkerRemove = func(marker string) error {
		raw, readErr := os.ReadFile(marker)
		if readErr != nil {
			return readErr
		}
		var record layerRestoreTxnRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if len(record.Files) > 4 {
			return errInjectedLayerRestoreCommit
		}
		return removeLayerRestoreMarker(marker)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"})
	err = restoreLayerEntries(context.Background(), fs.client, &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}, shadows, pending, fs)
	if !errors.Is(err, errLayerRestoreStateUncertain) {
		t.Fatalf("directory restore error = %v, want uncertain rollback failure", err)
	}
	if !fs.promotionBlocked.Load() {
		t.Fatal("rollback failure did not freeze the live mount")
	}
	// Model the process stopping in the fail-closed state. The retained marker
	// must make startup finish the whole-group rollback before either index is
	// exposed; it must not preserve only the first descendant move.
	shadows.Close()
	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	for path, want := range map[string][]byte{"/old/a.txt": []byte("a"), "/old/b.txt": []byte("b")} {
		got, readErr := restartedShadows.ReadAll(path)
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("restarted shadow %s = %q, %v; want %q", path, got, readErr, want)
		}
		if _, ok := restartedPending.GetMeta(path); !ok {
			t.Fatalf("restarted pending %s missing", path)
		}
	}
	for _, path := range []string{"/new/a.txt", "/new/b.txt"} {
		if restartedShadows.Has(path) || restartedPending.HasPending(path) {
			t.Fatalf("restart retained partial group target %s", path)
		}
	}
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestLayerRestoreTransactionCrashRecoveryRestoresOldPair(t *testing.T) {
	const path = "/crash.txt"
	shadowDir, pendingDir := t.TempDir(), t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	oldData := []byte("old-crash")
	oldMeta := seedCommittedLayerCache(t, shadows, pending, path, oldData, 51, 0o640)
	tx, err := beginLayerRestoreTxn(pendingDir, shadows.shadowPath(path), filepath.Join(pendingDir, hashPath(path)+".meta"))
	if err != nil {
		t.Fatal(err)
	}
	_ = tx // Intentionally left unfinished to model SIGKILL after both swaps.
	if err := atomicWrite(shadows.shadowPath(path), []byte("new-crash")); err != nil {
		t.Fatal(err)
	}
	newMeta := oldMeta
	newMeta.Generation++
	newMeta.BaseRev = 52
	newMeta.Size = int64(len("new-crash"))
	raw, err := json.Marshal(newMeta)
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(filepath.Join(pendingDir, hashPath(path)+".meta"), raw); err != nil {
		t.Fatal(err)
	}
	shadows.Close()

	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	assertLayerRestoreCacheState(t, restartedShadows, restartedPending, path, oldData, oldMeta)
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestLayerRestoreTransactionCorruptionAndRollbackFailureFailClosed(t *testing.T) {
	t.Run("corrupt marker", func(t *testing.T) {
		pendingDir := t.TempDir()
		marker := filepath.Join(pendingDir, layerRestoreTxnPrefix+"corrupt"+layerRestoreTxnSuffix)
		if err := os.WriteFile(marker, []byte("{not-json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewPendingIndex(pendingDir); err == nil || !strings.Contains(err.Error(), "decode layer restore transaction") {
			t.Fatalf("NewPendingIndex error = %v, want corrupt transaction rejection", err)
		}
	})

	t.Run("missing rollback snapshot", func(t *testing.T) {
		root := t.TempDir()
		shadowDir := filepath.Join(root, "shadow")
		pendingDir := filepath.Join(root, "pending")
		shadows, err := NewShadowStore(shadowDir)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := NewPendingIndex(pendingDir)
		if err != nil {
			t.Fatal(err)
		}
		seedCommittedLayerCache(t, shadows, pending, "/failure.txt", []byte("old"), 61, 0o640)
		tx, err := beginLayerRestoreTxn(pendingDir, shadows.shadowPath("/failure.txt"), filepath.Join(pendingDir, hashPath("/failure.txt")+".meta"))
		if err != nil {
			t.Fatal(err)
		}
		for _, snap := range tx.record.Files {
			if snap.Existed && strings.HasSuffix(snap.Path, ".shadow") {
				if err := os.Remove(snap.BackupPath); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
		if err := atomicWrite(shadows.shadowPath("/failure.txt"), []byte("mixed")); err != nil {
			t.Fatal(err)
		}
		if err := tx.rollback(); err == nil {
			t.Fatal("rollback error = nil, want missing snapshot failure")
		}
		if _, err := os.Stat(tx.markerPath); err != nil {
			t.Fatalf("rollback removed recovery marker after failure: %v", err)
		}
		shadows.Close()
		if _, err := NewPendingIndex(pendingDir); err == nil || !strings.Contains(err.Error(), "recover") {
			t.Fatalf("restart error = %v, want fail-closed rollback recovery failure", err)
		}
	})

	t.Run("overlapping markers", func(t *testing.T) {
		root := t.TempDir()
		shadowDir := filepath.Join(root, "shadow")
		pendingDir := filepath.Join(root, "pending")
		shadows, err := NewShadowStore(shadowDir)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := NewPendingIndex(pendingDir)
		if err != nil {
			t.Fatal(err)
		}
		seedCommittedLayerCache(t, shadows, pending, "/overlap.txt", []byte("old"), 62, 0o640)
		tx1, err := beginLayerRestoreTxn(pendingDir, shadows.shadowPath("/overlap.txt"), filepath.Join(pendingDir, hashPath("/overlap.txt")+".meta"))
		if err != nil {
			t.Fatal(err)
		}
		tx2, err := beginLayerRestoreTxn(pendingDir, shadows.shadowPath("/overlap.txt"), filepath.Join(pendingDir, hashPath("/overlap.txt")+".meta"))
		if err != nil {
			t.Fatal(err)
		}
		_ = tx1
		_ = tx2
		shadows.Close()
		if _, err := NewPendingIndex(pendingDir); err == nil || !strings.Contains(err.Error(), "overlapping layer restore transactions") {
			t.Fatalf("restart error = %v, want overlapping-marker fail closed", err)
		}
		if _, err := os.Stat(tx1.markerPath); err != nil {
			t.Fatalf("overlap detection modified first marker: %v", err)
		}
		if _, err := os.Stat(tx2.markerPath); err != nil {
			t.Fatalf("overlap detection modified second marker: %v", err)
		}
	})
}

func TestRestoreLayerCommitPointFailureRollsBackBeforeSuccessorWriter(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(string) error
	}{
		{
			name: "marker remove",
			fail: func(string) error { return errInjectedLayerRestoreCommit },
		},
		{
			name: "marker dir fsync",
			fail: func(marker string) error {
				if err := os.Remove(marker); err != nil {
					return err
				}
				return errInjectedLayerRestoreCommit
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const path = "/commit-point.txt"
			entry := client.FSLayerEntry{
				LayerID: "layer-1", Path: "/repo/commit-point.txt", Op: "upsert", Kind: "file",
				Content: []byte("remote-new"), SizeBytes: int64(len("remote-new")), BaseRevision: 72, Mode: 0o600, EntrySeq: 2,
			}
			ts := newLayerRestoreAtomicityServer(t, entry, nil)
			defer ts.Close()

			shadowDir, pendingDir := t.TempDir(), t.TempDir()
			shadows, err := NewShadowStore(shadowDir)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := NewPendingIndex(pendingDir)
			if err != nil {
				t.Fatal(err)
			}
			oldMeta := seedCommittedLayerCache(t, shadows, pending, path, []byte("old"), 71, 0o640)
			failed := false
			pending.restoreTxnMarkerRemove = func(marker string) error {
				if !failed {
					failed = true
					return tc.fail(marker)
				}
				return removeLayerRestoreMarker(marker)
			}

			err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}, shadows, pending, nil)
			if !errors.Is(err, errInjectedLayerRestoreCommit) {
				t.Fatalf("restore error = %v, want commit-point failure", err)
			}
			assertLayerRestoreCacheState(t, shadows, pending, path, []byte("old"), oldMeta)
			assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)

			// A successor writer starts only after rollback and must survive restart;
			// the failed restore must not leave a marker that later rewinds it.
			successor := []byte("successor-local")
			if err := shadows.WriteFull(path, successor, oldMeta.BaseRev); err != nil {
				t.Fatal(err)
			}
			if _, err := pending.PutWithBaseRevAndMode(path, int64(len(successor)), PendingOverwrite, oldMeta.BaseRev, 0o600, true); err != nil {
				t.Fatal(err)
			}
			successorMeta, ok := pending.GetMeta(path)
			if !ok {
				t.Fatal("successor metadata missing")
			}
			shadows.Close()
			restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
			assertLayerRestoreCacheState(t, restartedShadows, restartedPending, path, successor, *successorMeta)
			assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
		})
	}
}

func TestRestoreLayerCommitPointRollbackFailureBlocksSuccessorWriters(t *testing.T) {
	const path = "/blocked.txt"
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/blocked.txt", Op: "upsert", Kind: "file",
		Content: []byte("remote-new"), SizeBytes: int64(len("remote-new")), BaseRevision: 82, Mode: 0o600, EntrySeq: 2,
	}
	ts := newLayerRestoreAtomicityServer(t, entry, nil)
	defer ts.Close()

	shadows, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadows.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedCommittedLayerCache(t, shadows, pending, path, []byte("old"), 81, 0o640)
	pending.restoreTxnMarkerRemove = func(string) error { return errInjectedLayerRestoreCommit }
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadows, pending, fs)
	if !errors.Is(err, errLayerRestoreStateUncertain) {
		t.Fatalf("restore error = %v, want uncertain local state", err)
	}
	if !fs.promotionBlocked.Load() {
		t.Fatal("rollback failure did not freeze the live mount")
	}
	if unlock, ok := fs.lockPromotionMutation(); ok {
		unlock()
		t.Fatal("successor writer allowed after rollback failure")
	}
}

func TestRestoreLayerBeginAmbiguityFreezesQueuedWriterBeforePathPublication(t *testing.T) {
	const path = "/begin-ambiguous.txt"
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/begin-ambiguous.txt", Op: "upsert", Kind: "file",
		Content: []byte("remote-new"), SizeBytes: int64(len("remote-new")), BaseRevision: 92, Mode: 0o600, EntrySeq: 2,
	}
	ts := newLayerRestoreAtomicityServer(t, entry, nil)
	defer ts.Close()

	shadowDir, pendingDir := t.TempDir(), t.TempDir()
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	oldMeta := seedCommittedLayerCache(t, shadows, pending, path, []byte("old"), 91, 0o640)
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	markerInstalled := make(chan struct{})
	allowBeginFailure := make(chan struct{})
	testHookBeforeAtomicWriteDirSync = func(writtenPath string) error {
		if !strings.HasPrefix(filepath.Base(writtenPath), layerRestoreTxnPrefix) {
			return nil
		}
		select {
		case <-markerInstalled:
		default:
			close(markerInstalled)
		}
		<-allowBeginFailure
		return errInjectedLayerRestoreCommit
	}
	t.Cleanup(func() { testHookBeforeAtomicWriteDirSync = nil })

	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadows, pending, fs)
	}()
	<-markerInstalled

	writerAttempted := make(chan struct{})
	writerDone := make(chan bool, 1)
	go func() {
		close(writerAttempted)
		unlock, ok := fs.lockPromotionMutation()
		if !ok {
			writerDone <- false
			return
		}
		defer unlock()
		if err := shadows.WriteFull(path, []byte("queued-writer"), oldMeta.BaseRev); err != nil {
			writerDone <- true
			return
		}
		_, putErr := pending.PutWithBaseRevAndMode(path, int64(len("queued-writer")), PendingOverwrite, oldMeta.BaseRev, 0o600, true)
		writerDone <- putErr == nil
	}()
	<-writerAttempted
	select {
	case <-writerDone:
		t.Fatal("writer passed lifecycle barrier while restore begin held it")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowBeginFailure)

	if err := <-restoreDone; !errors.Is(err, errLayerRestoreStateUncertain) {
		t.Fatalf("restore error = %v, want ambiguous-begin fail closed", err)
	}
	if wrote := <-writerDone; wrote {
		t.Fatal("queued writer published after ambiguous transaction begin")
	}
	if !fs.promotionBlocked.Load() {
		t.Fatal("ambiguous transaction begin did not freeze live mount")
	}
	assertLayerRestoreCacheState(t, shadows, pending, path, []byte("old"), oldMeta)

	// Startup owns the surviving marker and converges it before exposing any
	// state. The writer rejected above was never authorized, so restart remains
	// at the begin snapshot with no successor generation to rewind.
	testHookBeforeAtomicWriteDirSync = nil
	shadows.Close()
	restartedShadows, restartedPending := reopenLayerRestoreCache(t, shadowDir, pendingDir)
	assertLayerRestoreCacheState(t, restartedShadows, restartedPending, path, []byte("old"), oldMeta)
	assertNoLayerRestoreArtifacts(t, shadowDir, pendingDir)
}

func TestMountRejectsCorruptLayerRestoreMarkerBeforeFuseServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo":
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Revision", "1")
		case r.Method == http.MethodHead && r.URL.Path == "/":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1":
			_ = json.NewEncoder(w).Encode(client.FSLayer{LayerID: "layer-1", BaseRootPath: "/repo", State: "active"})
		default:
			t.Errorf("unexpected request before pending recovery: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	cacheDir := t.TempDir()
	mountPoint := t.TempDir()
	pendingDir := filepath.Join(cacheDir, MountLayerHash(ts.URL, mountPoint, "/repo", "layer-1", ""), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(pendingDir, layerRestoreTxnPrefix+"corrupt"+layerRestoreTxnSuffix)
	if err := os.WriteFile(marker, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := Mount(&MountOptions{
		Server: ts.URL, APIKey: "sk-test", MountPoint: mountPoint, RemoteRoot: "/repo",
		CacheDir: cacheDir, LayerRef: "layer-1",
	})
	assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
	if !strings.Contains(err.Error(), "layer pending index recovery") {
		t.Fatalf("Mount error = %v, want pending rollback diagnosis", err)
	}
}

func TestMountRejectsCorruptLayerRestoreMarkerForOrdinaryAndReadOnlyLibraryPaths(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo":
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Revision", "1")
		case r.Method == http.MethodHead && r.URL.Path == "/":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1":
			_ = json.NewEncoder(w).Encode(client.FSLayer{LayerID: "layer-1", BaseRootPath: "/repo", State: "active"})
		default:
			t.Errorf("unexpected request before pending recovery: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	for _, tc := range []struct {
		name       string
		layerRef   string
		readOnly   bool
		pendingSub string
		hash       func(server, mountPoint string) string
	}{
		{
			name: "ordinary writable library mount", pendingSub: "pending",
			hash: func(server, mountPoint string) string { return MountHash(server, mountPoint, "/repo") },
		},
		{
			name: "read-only layer library mount", layerRef: "layer-1", readOnly: true, pendingSub: "pending-ro",
			hash: func(server, mountPoint string) string {
				return MountLayerHash(server, mountPoint, "/repo", "layer-1", "")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cacheDir := t.TempDir()
			mountPoint := t.TempDir()
			pendingDir := filepath.Join(cacheDir, tc.hash(ts.URL, mountPoint), tc.pendingSub)
			if err := os.MkdirAll(pendingDir, 0o755); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(pendingDir, layerRestoreTxnPrefix+"corrupt"+layerRestoreTxnSuffix)
			if err := os.WriteFile(marker, []byte("{not-json"), 0o600); err != nil {
				t.Fatal(err)
			}

			err := Mount(&MountOptions{
				Server: ts.URL, APIKey: "sk-test", MountPoint: mountPoint, RemoteRoot: "/repo",
				CacheDir: cacheDir, LayerRef: tc.layerRef, ReadOnly: tc.readOnly,
			})
			assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
			if !strings.Contains(err.Error(), "pending index recovery") {
				t.Fatalf("Mount error = %v, want pending rollback diagnosis", err)
			}
		})
	}
}

func TestLayerRestoreRollbackFailureStopsEventRetryBeforeRemoteRead(t *testing.T) {
	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Errorf("blocked event retry reached server: %s %s", r.Method, r.URL.String())
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	shadows, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadows.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.promotionBlocked.Store(true)

	if _, err := refreshLayerEvents(context.Background(), client.New(ts.URL, ""), opts, shadows, pending, fs, 7); !errors.Is(err, errLayerRestoreStateUncertain) {
		t.Fatalf("refresh error = %v, want fail-closed sentinel", err)
	}
	if requests != 0 {
		t.Fatalf("blocked refresh made %d remote requests", requests)
	}
}

func newLayerRestoreAtomicityServer(t *testing.T, entry client.FSLayerEntry, object []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			_ = json.NewEncoder(w).Encode(entry)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/objects" && object != nil:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(object)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
}
