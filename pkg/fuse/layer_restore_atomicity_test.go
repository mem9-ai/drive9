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

	"github.com/mem9-ai/drive9/pkg/client"
)

var errInjectedLayerRestoreMeta = errors.New("injected layer restore metadata durability failure")

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
