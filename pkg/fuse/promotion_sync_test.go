package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/xattr"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

func TestSynchronousPromotionGateOffPreservesEXDEV(t *testing.T) {
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: t.TempDir()}
	opts.setDefaults()
	fsys := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	repo := fsys.inodes.Lookup("/repo", true, 0, time.Now())
	source := fsys.inodes.Lookup("/repo/.git", true, 0, time.Now())
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("Rename status = %v, want EXDEV (source ino=%d)", status, source)
	}
	if _, err := os.Stat(filepath.Join(opts.LocalRoot, "overlay", promotionStateDirName)); !os.IsNotExist(err) {
		t.Fatalf("gate-off rename created promotion state: %v", err)
	}
}

func TestSynchronousPromotionUnsupportedTreeHasNoDurableSideEffects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.Error(w, "promotion API must not run for unsupported tree", http.StatusInternalServerError)
	}))
	defer server.Close()
	localRoot := t.TempDir()
	sourceAbs := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(sourceAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(sourceAbs, "object")
	if err := os.WriteFile(file, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(file, filepath.Join(sourceAbs, "alias")); err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: localRoot, EnableSynchronousPromotion: true}
	opts.setDefaults()
	fsys := NewDat9FS(newTestClient(server.URL), opts)
	repo := fsys.inodes.Lookup("/repo", true, 0, time.Now())
	fsys.inodes.Lookup("/repo/.git", true, 0, time.Now())
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("Rename status = %v, want EXDEV", status)
	}
	if _, err := xattr.LGet(sourceAbs, promotionSourceIdentityXAttr); !errors.Is(err, xattr.ENOATTR) {
		t.Fatalf("unsupported source identity xattr = %v, want absent", err)
	}
	if _, err := os.Stat(filepath.Join(localRoot, "overlay", promotionStateDirName)); !os.IsNotExist(err) {
		t.Fatalf("unsupported rename created promotion state: %v", err)
	}
}

func TestSynchronousPromotionRejectsMountXAttrBeforeDurableSideEffects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.Error(w, "promotion API must not run for xattr-bearing tree", http.StatusInternalServerError)
	}))
	defer server.Close()
	localRoot := t.TempDir()
	sourceAbs := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(sourceAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceAbs, "object"), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: localRoot, EnableSynchronousPromotion: true}
	opts.setDefaults()
	fsys := NewDat9FS(newTestClient(server.URL), opts)
	fsys.xattrs.Set("/repo/.git/object", "user.keep", []byte("value"))
	repo := fsys.inodes.Lookup("/repo", true, 0, time.Now())
	fsys.inodes.Lookup("/repo/.git", true, 0, time.Now())
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("Rename status = %v, want EXDEV", status)
	}
	if _, err := xattr.LGet(sourceAbs, promotionSourceIdentityXAttr); !errors.Is(err, xattr.ENOATTR) {
		t.Fatalf("unsupported source identity xattr = %v, want absent", err)
	}
	if _, err := os.Stat(filepath.Join(localRoot, "overlay", promotionStateDirName)); !os.IsNotExist(err) {
		t.Fatalf("unsupported rename created promotion state: %v", err)
	}
}

func TestSynchronousPromotionSourceIdentityMustBeUnique(t *testing.T) {
	localRoot := t.TempDir()
	overlayRoot := filepath.Join(localRoot, "overlay")
	sourceAbs := filepath.Join(overlayRoot, "repo", ".git")
	duplicateAbs := filepath.Join(overlayRoot, "copy", ".git")
	if err := os.MkdirAll(sourceAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(duplicateAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	identity := strings.Repeat("a", 32)
	for _, name := range []string{sourceAbs, duplicateAbs} {
		if err := xattr.LSet(name, promotionSourceIdentityXAttr, []byte(identity)); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyUniquePromotionSourceIdentity(overlayRoot, sourceAbs, identity); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate source identity error = %v", err)
	}
	if err := xattr.LRemove(duplicateAbs, promotionSourceIdentityXAttr); err != nil {
		t.Fatal(err)
	}
	if err := verifyUniquePromotionSourceIdentity(overlayRoot, sourceAbs, identity); err != nil {
		t.Fatalf("unique source identity: %v", err)
	}
}

func TestCommitQueueHasPrefixIncludesQueuedDescendants(t *testing.T) {
	entry := &CommitEntry{Path: "/repo/.git/object"}
	queue := &CommitQueue{
		queuedByPath: map[string]map[*CommitEntry]struct{}{entry.Path: {entry: {}}},
		inFlight:     make(map[string]*CommitEntry),
		immediate:    make(map[*CommitEntry]struct{}),
	}
	if !queue.HasPrefix("/repo/.git/") {
		t.Fatal("HasPrefix missed queued descendant")
	}
	if queue.HasPrefix("/repo/.github/") {
		t.Fatal("HasPrefix matched adjacent path")
	}
}

func TestSynchronousPromotionPublishesWholeTreeThenQuarantinesSource(t *testing.T) {
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{AllocationEpoch: 1, AllocationSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var calls []string
	var manifestHash string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		mu.Unlock()
		write := func(value any) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(value); err != nil {
				t.Errorf("encode response: %v", err)
			}
		}
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/site":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports:plan":
			var request promotion.PlanImportRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode plan: %v", err)
			}
			manifest, err := promotion.CanonicalizeManifest(request.Manifest, promotionTestLimits())
			if err != nil {
				t.Errorf("canonical plan manifest: %v", err)
			}
			manifestHash = manifest.ManifestHash
			write(promotion.ImportPlan{
				Target: request.Target, ExpectedTargetAbsent: true, ManifestHash: manifest.ManifestHash,
				EntryTotal: manifest.EntryTotal, ByteTotal: manifest.ByteTotal, MaxContentSize: manifest.MaxContentSize,
				StorageMode: promotion.StorageModeDB9Inline, StoragePlanDigest: "plan", Limits: promotionTestLimits(),
				BackendCapabilityGeneration: 1, ConfigGeneration: 1, PlanExpiresAt: time.Now().Add(time.Hour),
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports:allocate":
			write(promotion.Allocation{MigrationID: migrationID, AllocationEpoch: 1, AllocationSequence: 1, AllocationProof: "proof", CreateBefore: time.Now().Add(time.Hour)})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports":
			status := promotionTestStatus(migrationID, "CREATED")
			status.ManifestHash = manifestHash
			write(status)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":renew":
			status := promotionTestStatus(migrationID, "STAGING")
			status.ManifestHash = manifestHash
			write(status)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/promotions/imports/"+migrationID+":put-inline":
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "hello" {
				t.Errorf("inline body = %q, err=%v", body, err)
			}
			write(promotion.InlineContent{ContentID: "content-1", RelativePath: "file.txt", SizeBytes: 5, ChecksumSHA256: r.Header.Get("X-Drive9-Promotion-Checksum-SHA256"), State: "STAGED", StateVersion: 2})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":verify":
			status := promotionTestStatus(migrationID, "VERIFIED")
			status.ManifestHash = manifestHash
			write(status)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":commit":
			status := promotionTestStatus(migrationID, "COMMITTED")
			status.ManifestHash = manifestHash
			write(status)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":ack-result":
			status := promotionTestStatus(migrationID, "COMMITTED")
			status.ManifestHash = manifestHash
			write(status)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	localRoot := t.TempDir()
	sourceAbs := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(sourceAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceAbs, "file.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{
		Profile: MountProfileCodingAgent, LocalRoot: localRoot,
		EnableSynchronousPromotion: true,
	}
	opts.setDefaults()
	fsys := NewDat9FS(newTestClient(server.URL), opts)
	if got := fsys.observeDirPathPolicyWithContext(t.Context(), "/repo/.git"); got != PathLayerLocalOnly {
		t.Fatalf("source root policy = %v, want local-only", got)
	}
	if got := fsys.observePathPolicyWithContext(t.Context(), "/repo/.git/file.txt"); got != PathLayerLocalOnly {
		t.Fatalf("source file policy = %v, want local-only", got)
	}
	if got := fsys.observePathPolicyWithContext(t.Context(), "/repo/site/file.txt"); got != PathLayerRemotePersistent {
		t.Fatalf("target file policy = %v, want remote", got)
	}
	repo := fsys.inodes.Lookup("/repo", true, 0, time.Now())
	fsys.inodes.Lookup("/repo/.git", true, 0, time.Now())
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status != gofuse.OK {
		t.Fatalf("Rename status = %v, calls=%v", status, calls)
	}
	if _, err := os.Lstat(sourceAbs); !os.IsNotExist(err) {
		t.Fatalf("source still visible after promotion: %v", err)
	}
	records, err := filepath.Glob(filepath.Join(localRoot, "overlay", promotionStateDirName, "operations", "*", "record.json"))
	if err != nil || len(records) != 1 {
		t.Fatalf("record files = %v, err=%v", records, err)
	}
	raw, err := os.ReadFile(records[0])
	if err != nil {
		t.Fatal(err)
	}
	var record localPromotionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if record.Phase != promotionPhasePublishedQuarantined || record.FenceState != "RELEASED" || record.TerminalResultDigest == "" {
		t.Fatalf("final record = phase=%s fence=%s digest=%q", record.Phase, record.FenceState, record.TerminalResultDigest)
	}
	quarantined, err := os.ReadFile(filepath.Join(filepath.Dir(records[0]), "source", "file.txt"))
	if err != nil || string(quarantined) != "hello" {
		t.Fatalf("quarantine = %q, err=%v", quarantined, err)
	}
	wantOrder := []string{
		"POST /v1/promotions/imports:plan",
		"POST /v1/promotions/imports:allocate",
		"POST /v1/promotions/imports",
		"PUT /v1/promotions/imports/" + migrationID + ":put-inline",
		"POST /v1/promotions/imports/" + migrationID + ":verify",
		"POST /v1/promotions/imports/" + migrationID + ":commit",
	}
	assertPromotionCallSubsequence(t, calls, wantOrder)
}

func TestSynchronousPromotionRecoveryCompletesCommittedQuarantine(t *testing.T) {
	server, fsys, _, opDir, sourceAbs := newPromotionRecoveryFixture(t, promotionPhaseRemoteCommitted, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/promotions/imports/"+record.MigrationID+":get" {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(promotionTestRecoveryStatus(record, "COMMITTED"))
	})
	defer server.Close()

	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("recoverSynchronousPromotions: %v", err)
	}
	if _, err := os.Lstat(sourceAbs); !os.IsNotExist(err) {
		t.Fatalf("source still visible after recovery: %v", err)
	}
	if _, err := os.Stat(filepath.Join(opDir, "source", "child")); err != nil {
		t.Fatalf("quarantine missing after recovery: %v", err)
	}
	recovered, _, err := loadPromotionRecord(opDir)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Phase != promotionPhasePublishedQuarantined || recovered.FenceState != "RELEASED" {
		t.Fatalf("recovered record = (%s,%s)", recovered.Phase, recovered.FenceState)
	}
}

func TestSynchronousPromotionRecoveryCompletesRenameAfterSourceMoveCrash(t *testing.T) {
	server, fsys, _, opDir, sourceAbs := newPromotionRecoveryFixture(t, promotionPhaseRemoteCommitted, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/promotions/imports/"+record.MigrationID+":get" {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(promotionTestRecoveryStatus(record, "COMMITTED"))
	})
	defer server.Close()
	quarantine := filepath.Join(opDir, "source")
	if err := os.Rename(sourceAbs, quarantine); err != nil {
		t.Fatal(err)
	}
	if err := syncPromotionDirectories(filepath.Dir(sourceAbs), opDir); err != nil {
		t.Fatal(err)
	}

	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("recoverSynchronousPromotions: %v", err)
	}
	recovered, _, err := loadPromotionRecord(opDir)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Phase != promotionPhasePublishedQuarantined || recovered.FenceState != "RELEASED" {
		t.Fatalf("recovered record = (%s,%s)", recovered.Phase, recovered.FenceState)
	}
	if got, err := os.ReadFile(filepath.Join(quarantine, "child")); err != nil || string(got) != "payload" {
		t.Fatalf("quarantine = %q, err=%v", got, err)
	}
}

func TestSynchronousPromotionRecoveryResolvesCommitResponseLoss(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	server, fsys, record, opDir, sourceAbs := newPromotionRecoveryFixture(t, promotionPhaseCommitRequested, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/promotions/imports/" + record.MigrationID + ":get":
			_ = json.NewEncoder(w).Encode(promotionTestRecoveryStatus(record, "COMMITTING"))
		case "/v1/promotions/imports/" + record.MigrationID + ":commit":
			_ = json.NewEncoder(w).Encode(promotionTestRecoveryStatus(record, "COMMITTED"))
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	})
	defer server.Close()

	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("recoverSynchronousPromotions: %v", err)
	}
	if _, err := os.Lstat(sourceAbs); !os.IsNotExist(err) {
		t.Fatalf("source still visible after commit recovery: %v", err)
	}
	recovered, _, err := loadPromotionRecord(opDir)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Phase != promotionPhasePublishedQuarantined || recovered.FenceState != "RELEASED" {
		t.Fatalf("recovered record = (%s,%s)", recovered.Phase, recovered.FenceState)
	}
	assertPromotionCallSubsequence(t, calls, []string{
		"POST /v1/promotions/imports/" + record.MigrationID + ":get",
		"POST /v1/promotions/imports/" + record.MigrationID + ":commit",
	})
}

func TestSynchronousPromotionRecoveryTakesOverExpiredOwnerBeforeAbort(t *testing.T) {
	var abortCalls int
	server, fsys, record, opDir, sourceAbs := newPromotionRecoveryFixture(t, promotionPhaseStaging, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/promotions/imports/" + record.MigrationID + ":abort":
			abortCalls++
			if abortCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "promotion request conflict", "code": "promotion_conflict"})
				return
			}
			status := promotionTestRecoveryStatus(record, "ABORTED")
			status.OwnerEpoch = 2
			_ = json.NewEncoder(w).Encode(status)
		case "/v1/promotions/imports/" + record.MigrationID + ":get":
			status := promotionTestRecoveryStatus(record, "STAGING")
			status.LeaseExpiresAt = time.Now().Add(-time.Second)
			_ = json.NewEncoder(w).Encode(status)
		case "/v1/promotions/imports/" + record.MigrationID + ":take-over":
			status := promotionTestRecoveryStatus(record, "STAGING")
			status.OwnerEpoch = 2
			_ = json.NewEncoder(w).Encode(status)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	})
	defer server.Close()
	record.ServerState = "STAGING"
	record.TerminalResultDigest = ""
	if err := persistPromotionRecord(opDir, record); err != nil {
		t.Fatal(err)
	}

	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("recoverSynchronousPromotions: %v", err)
	}
	if _, err := os.Stat(sourceAbs); err != nil {
		t.Fatalf("source changed during abort recovery: %v", err)
	}
	recovered, _, err := loadPromotionRecord(opDir)
	if err != nil {
		t.Fatal(err)
	}
	if abortCalls != 2 || recovered.OwnerEpoch != 2 || recovered.PendingOwnerToken != "" ||
		recovered.Phase != promotionPhaseLocalAborted || recovered.FenceState != "RELEASED" {
		t.Fatalf("takeover recovery = abort_calls=%d owner=%d pending=%q phase=%s fence=%s ack=%v",
			abortCalls, recovered.OwnerEpoch, recovered.PendingOwnerToken, recovered.Phase, recovered.FenceState, recovered.ResultAcknowledged)
	}
}

func TestSynchronousPromotionRejectsMismatchedServerObservation(t *testing.T) {
	record := &localPromotionRecord{
		MigrationID: "p1_aaaaaaaaaaaac_aaaaaaaaaaaac", AllocationEpoch: 1, AllocationSequence: 1,
		RemoteTarget: "/repo/site", ManifestHash: "manifest", OwnerEpoch: 1,
	}
	status := promotionTestStatus(record.MigrationID, "STAGING")
	status.ManifestHash = "different"
	if err := observeLocalPromotionStatus(record, &status); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("mismatched observation error = %v", err)
	}
	status.ManifestHash = record.ManifestHash
	status.State = "COMMITTED"
	status.TerminalResultBlob = `{"state":"COMMITTED"}`
	status.TerminalResultDigest = strings.Repeat("0", 64)
	if err := observeLocalPromotionStatus(record, &status); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("corrupt terminal observation error = %v", err)
	}
}

func TestSynchronousPromotionRetentionBudgetFailsClosed(t *testing.T) {
	server, fsys, record, opDir, sourceAbs := newPromotionRecoveryFixture(t, promotionPhasePublishedQuarantined, "RELEASED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		t.Fatal("retention budget check must not call the server")
	})
	defer server.Close()
	if err := os.Rename(sourceAbs, filepath.Join(opDir, "source")); err != nil {
		t.Fatal(err)
	}
	record.Plan.ByteTotal = promotionMaxRetainedBytes
	if err := persistPromotionRecord(opDir, record); err != nil {
		t.Fatal(err)
	}
	if err := fsys.checkPromotionRetentionBudget(1); !errors.Is(err, promotion.ErrManifestLimitExceeded) {
		t.Fatalf("retention budget error = %v, want manifest limit", err)
	}
}

func TestSynchronousPromotionRecoveryRestoresSourceAfterNewerRestoreRollback(t *testing.T) {
	server, fsys, _, opDir, sourceAbs := newPromotionRecoveryFixture(t, promotionPhasePublishedQuarantined, "RELEASED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/promotions/imports/"+record.MigrationID+":get" {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
			return
		}
		status := promotionTestRecoveryStatus(record, "ABORTED")
		status.RestoreGeneration = record.RestoreGeneration + 1
		status.DatabaseIncarnation = "db-2"
		status.TerminalResultBlob, status.TerminalResultDigest = promotionTestTerminal("ABORTED")
		_ = json.NewEncoder(w).Encode(status)
	})
	defer server.Close()
	quarantine := filepath.Join(opDir, "source")
	if err := os.Rename(sourceAbs, quarantine); err != nil {
		t.Fatal(err)
	}
	if err := syncPromotionDirectories(filepath.Dir(sourceAbs), opDir); err != nil {
		t.Fatal(err)
	}

	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("recoverSynchronousPromotions: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(sourceAbs, "child")); err != nil || string(got) != "payload" {
		t.Fatalf("restored source = %q, err=%v", got, err)
	}
	if _, err := os.Stat(quarantine); !os.IsNotExist(err) {
		t.Fatalf("quarantine still exists after rollback reconciliation: %v", err)
	}
	recovered, _, err := loadPromotionRecord(opDir)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Phase != promotionPhaseLocalAborted || recovered.FenceState != "RELEASED" ||
		recovered.RestoreGeneration != 2 || recovered.DatabaseIncarnation != "db-2" ||
		recovered.AcceptedRestoreGeneration != 1 || recovered.AcceptedDatabaseIncarnation != "db-1" {
		t.Fatalf("rollback record = phase=%s fence=%s tuple=(%d,%s)", recovered.Phase, recovered.FenceState, recovered.RestoreGeneration, recovered.DatabaseIncarnation)
	}
}

func TestSynchronousPromotionRecoveryRejectsCorruptRecord(t *testing.T) {
	server, fsys, _, opDir, _ := newPromotionRecoveryFixture(t, promotionPhaseRemoteCommitted, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		t.Fatal("corrupt record must fail before network recovery")
	})
	defer server.Close()
	recordPath := filepath.Join(opDir, "record.json")
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var damaged localPromotionRecord
	if err := json.Unmarshal(raw, &damaged); err != nil {
		t.Fatal(err)
	}
	damaged.SourcePath = "/tampered"
	raw, err = json.Marshal(&damaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recordPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fsys.recoverSynchronousPromotions(context.Background()); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("corrupt recovery error = %v, want checksum failure", err)
	}
}

func TestSynchronousPromotionRecoveryRejectsValidChecksumWithMissingTerminalContract(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*localPromotionRecord)
	}{
		{name: "accepted tuple", mutate: func(record *localPromotionRecord) { record.AcceptedWriterGeneration = 0 }},
		{name: "terminal digest", mutate: func(record *localPromotionRecord) { record.TerminalResultDigest = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, fsys, record, opDir, _ := newPromotionRecoveryFixture(t, promotionPhaseRemoteCommitted, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
				t.Fatal("invalid durable contract must fail before network recovery")
			})
			defer server.Close()
			tc.mutate(record)
			if err := persistPromotionRecord(opDir, record); err != nil {
				t.Fatal(err)
			}
			if err := fsys.recoverSynchronousPromotions(context.Background()); err == nil {
				t.Fatal("recovery accepted a record with missing terminal contract")
			}
		})
	}
}

func TestSynchronousPromotionGateOffCannotBypassPendingJournal(t *testing.T) {
	server, fsys, _, _, _ := newPromotionRecoveryFixture(t, promotionPhaseCommitRequested, "INSTALLED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		t.Fatal("gate-off pending-journal check must not call the server")
	})
	defer server.Close()
	fsys.opts.EnableSynchronousPromotion = false
	if err := fsys.recoverSynchronousPromotions(context.Background()); err == nil || !strings.Contains(err.Error(), "requires recovery") {
		t.Fatalf("gate-off pending journal error = %v", err)
	}
}

func TestSynchronousPromotionGateOffAllowsReleasedJournal(t *testing.T) {
	server, fsys, _, _, _ := newPromotionRecoveryFixture(t, promotionPhasePublishedQuarantined, "RELEASED", func(w http.ResponseWriter, r *http.Request, record *localPromotionRecord) {
		t.Fatal("gate-off released-journal check must not call the server")
	})
	defer server.Close()
	fsys.opts.EnableSynchronousPromotion = false
	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("gate-off released journal: %v", err)
	}
}

func TestSynchronousPromotionOutcomeUnknownFailsMountClosed(t *testing.T) {
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: t.TempDir(), EnableSynchronousPromotion: true}
	opts.setDefaults()
	fsys := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	fhID := fsys.fileHandles.Allocate(&FileHandle{Ino: 2, Path: "/unrelated-dirty", DirtySeq: 1})
	if got := fsys.failPromotionClosed(&localPromotionRecord{MigrationID: "test", Phase: promotionPhaseCommitRequested}, io.ErrUnexpectedEOF); got != gofuse.EIO {
		t.Fatalf("failPromotionClosed = %v, want EIO", got)
	}
	if got := fsys.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: gofuse.FUSE_ROOT_ID}}, &gofuse.AttrOut{}); got != gofuse.EIO {
		t.Fatalf("GetAttr after unknown outcome = %v, want EIO", got)
	}
	fsys.Release(nil, &gofuse.ReleaseIn{Fh: fhID})
	if _, ok := fsys.fileHandles.Get(fhID); !ok {
		t.Fatal("Release discarded a handle after outcome-unknown fail-closed")
	}
}

func TestSynchronousPromotionVerifyFailureAbortsWithoutMovingSource(t *testing.T) {
	flow := &promotionFlowControl{verifyFailure: true}
	server, fsys, repo, sourceAbs := newPromotionFlowFixture(t, flow)
	defer server.Close()
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status == gofuse.OK {
		t.Fatal("Rename succeeded after Verify failure")
	}
	if got, err := os.ReadFile(filepath.Join(sourceAbs, "file.txt")); err != nil || string(got) != "hello" {
		t.Fatalf("source after abort = %q, err=%v", got, err)
	}
	if !flow.called(":abort") {
		t.Fatalf("calls = %v, missing abort", flow.snapshotCalls())
	}
	records, err := filepath.Glob(filepath.Join(filepath.Dir(filepath.Dir(sourceAbs)), promotionStateDirName, "operations", "*", "record.json"))
	if err != nil || len(records) != 1 {
		t.Fatalf("record files = %v, err=%v", records, err)
	}
	record, _, err := loadPromotionRecord(filepath.Dir(records[0]))
	if err != nil {
		t.Fatal(err)
	}
	if record.Phase != promotionPhaseLocalAborted || record.FenceState != "RELEASED" {
		t.Fatalf("abort record = (%s,%s)", record.Phase, record.FenceState)
	}
}

func TestSynchronousPromotionCommitUnknownFailsWholeMountClosed(t *testing.T) {
	flow := &promotionFlowControl{commitUnknown: true}
	server, fsys, repo, sourceAbs := newPromotionFlowFixture(t, flow)
	defer server.Close()
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status != gofuse.EIO {
		t.Fatalf("Rename status = %v, want EIO", status)
	}
	if _, err := os.Stat(sourceAbs); err != nil {
		t.Fatalf("source moved after unknown commit result: %v", err)
	}
	if got := fsys.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: gofuse.FUSE_ROOT_ID}}, &gofuse.AttrOut{}); got != gofuse.EIO {
		t.Fatalf("GetAttr after unknown commit = %v, want EIO", got)
	}
}

func TestSynchronousPromotionKnownAllocationFailureLeavesRestartableJournal(t *testing.T) {
	flow := &promotionFlowControl{allocateFailure: true}
	server, fsys, repo, sourceAbs := newPromotionFlowFixture(t, flow)
	defer server.Close()
	status := fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("Rename status = %v, want EXDEV", status)
	}
	if got, err := os.ReadFile(filepath.Join(sourceAbs, "file.txt")); err != nil || string(got) != "hello" {
		t.Fatalf("source after allocation rejection = %q, err=%v", got, err)
	}
	if err := fsys.recoverSynchronousPromotions(context.Background()); err != nil {
		t.Fatalf("recover known allocation rejection: %v", err)
	}
}

func TestSynchronousPromotionRemoteVisibilityChangesOnlyAtCommit(t *testing.T) {
	flow := &promotionFlowControl{
		commitStarted:         make(chan struct{}),
		releaseCommit:         make(chan struct{}),
		commitPublished:       make(chan struct{}),
		releaseCommitResponse: make(chan struct{}),
	}
	server, fsys, repo, _ := newPromotionFlowFixture(t, flow)
	defer server.Close()
	renameDone := make(chan gofuse.Status, 1)
	go func() {
		renameDone <- fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	}()
	select {
	case <-flow.commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Rename did not reach Commit")
	}
	otherMount := newTestClient(server.URL)
	if _, err := otherMount.Stat("/repo/site"); err == nil {
		t.Fatal("second mount observed target before Commit")
	}
	close(flow.releaseCommit)
	select {
	case <-flow.commitPublished:
	case <-time.After(5 * time.Second):
		t.Fatal("remote Commit did not publish")
	}
	if stat, err := otherMount.Stat("/repo/site"); err != nil || !stat.IsDir {
		t.Fatalf("second mount target after Commit = %+v, err=%v", stat, err)
	}
	select {
	case status := <-renameDone:
		t.Fatalf("Rename returned before durable local switch: %v", status)
	default:
	}
	close(flow.releaseCommitResponse)
	if status := <-renameDone; status != gofuse.OK {
		t.Fatalf("Rename status = %v", status)
	}
}

func TestSynchronousPromotionBlocksConcurrentMutationUntilCommitFinishes(t *testing.T) {
	flow := &promotionFlowControl{commitStarted: make(chan struct{}), releaseCommit: make(chan struct{})}
	server, fsys, repo, _ := newPromotionFlowFixture(t, flow)
	defer server.Close()
	renameDone := make(chan gofuse.Status, 1)
	go func() {
		renameDone <- fsys.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: repo}, Newdir: repo}, ".git", "site")
	}()
	select {
	case <-flow.commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Rename did not reach Commit")
	}
	mutationDone := make(chan gofuse.Status, 1)
	go func() {
		mutationDone <- fsys.Mkdir(nil, &gofuse.MkdirIn{InHeader: gofuse.InHeader{NodeId: repo}, Mode: 0o755}, "blocked", &gofuse.EntryOut{})
	}()
	readDone := make(chan gofuse.Status, 1)
	go func() {
		readDone <- fsys.StatFs(nil, &gofuse.InHeader{}, &gofuse.StatfsOut{})
	}()
	select {
	case status := <-mutationDone:
		t.Fatalf("concurrent mutation returned before Commit completed: %v", status)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case status := <-readDone:
		t.Fatalf("concurrent read returned inside promotion visibility window: %v", status)
	case <-time.After(100 * time.Millisecond):
	}
	close(flow.releaseCommit)
	if status := <-renameDone; status != gofuse.OK {
		t.Fatalf("Rename status = %v", status)
	}
	select {
	case <-mutationDone:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent mutation remained blocked after promotion")
	}
	select {
	case status := <-readDone:
		if status != gofuse.OK {
			t.Fatalf("concurrent read status = %v", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent read remained blocked after promotion")
	}
}

type promotionFlowControl struct {
	mu                    sync.Mutex
	calls                 []string
	verifyFailure         bool
	allocateFailure       bool
	commitUnknown         bool
	commitStarted         chan struct{}
	releaseCommit         chan struct{}
	commitPublished       chan struct{}
	releaseCommitResponse chan struct{}
	remotePublished       atomic.Bool
}

func (f *promotionFlowControl) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *promotionFlowControl) snapshotCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *promotionFlowControl) called(suffix string) bool {
	for _, call := range f.snapshotCalls() {
		if strings.HasSuffix(call, suffix) {
			return true
		}
	}
	return false
}

func newPromotionFlowFixture(t *testing.T, flow *promotionFlowControl) (*httptest.Server, *Dat9FS, uint64, string) {
	t.Helper()
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{AllocationEpoch: 9, AllocationSequence: 17})
	if err != nil {
		t.Fatal(err)
	}
	var manifestHash string
	write := func(w http.ResponseWriter, value any) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(value); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flow.record(r.Method + " " + r.URL.Path)
		status := func(state string) promotion.ImportStatus {
			value := promotionTestStatus(migrationID, state)
			value.AllocationEpoch = 9
			value.AllocationSequence = 17
			value.ManifestHash = manifestHash
			return value
		}
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/site":
			if !flow.remotePublished.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports:plan":
			var request promotion.PlanImportRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode plan: %v", err)
			}
			manifest, err := promotion.CanonicalizeManifest(request.Manifest, promotionTestLimits())
			if err != nil {
				t.Errorf("canonical plan manifest: %v", err)
			}
			manifestHash = manifest.ManifestHash
			write(w, promotion.ImportPlan{Target: request.Target, ExpectedTargetAbsent: true, ManifestHash: manifestHash,
				EntryTotal: manifest.EntryTotal, ByteTotal: manifest.ByteTotal, MaxContentSize: manifest.MaxContentSize,
				StorageMode: promotion.StorageModeDB9Inline, StoragePlanDigest: "plan", Limits: promotionTestLimits(),
				BackendCapabilityGeneration: 1, ConfigGeneration: 1, PlanExpiresAt: time.Now().Add(time.Hour)})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports:allocate":
			if flow.allocateFailure {
				w.WriteHeader(http.StatusUnprocessableEntity)
				write(w, map[string]string{"error": "promotion is not enabled", "code": "promotion_not_enabled"})
				return
			}
			write(w, promotion.Allocation{MigrationID: migrationID, AllocationEpoch: 9, AllocationSequence: 17, AllocationProof: "proof", CreateBefore: time.Now().Add(time.Hour)})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports":
			write(w, status("CREATED"))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":renew":
			write(w, status("STAGING"))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/promotions/imports/"+migrationID+":put-inline":
			write(w, promotion.InlineContent{ContentID: "content-1", RelativePath: "file.txt", SizeBytes: 5,
				ChecksumSHA256: r.Header.Get("X-Drive9-Promotion-Checksum-SHA256"), State: "STAGED", StateVersion: 2})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":verify" && flow.verifyFailure:
			w.WriteHeader(http.StatusConflict)
			write(w, map[string]string{"error": "promotion request conflict", "code": "promotion_conflict"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":verify":
			write(w, status("VERIFIED"))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":abort":
			write(w, status("ABORTED"))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":commit":
			if flow.commitStarted != nil {
				close(flow.commitStarted)
				<-flow.releaseCommit
			}
			if flow.commitUnknown {
				http.Error(w, "commit response unavailable", http.StatusServiceUnavailable)
				return
			}
			flow.remotePublished.Store(true)
			if flow.commitPublished != nil {
				close(flow.commitPublished)
			}
			if flow.releaseCommitResponse != nil {
				<-flow.releaseCommitResponse
			}
			write(w, status("COMMITTED"))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":get" && flow.commitUnknown:
			http.Error(w, "status unavailable", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+migrationID+":ack-result":
			var request promotion.AcknowledgeImportResultRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode acknowledgment: %v", err)
			}
			write(w, status(request.TerminalState))
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	localRoot := t.TempDir()
	sourceAbs := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(sourceAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceAbs, "file.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: localRoot, EnableSynchronousPromotion: true}
	opts.setDefaults()
	fsys := NewDat9FS(newTestClient(server.URL), opts)
	repo := fsys.inodes.Lookup("/repo", true, 0, time.Now())
	fsys.inodes.Lookup("/repo/.git", true, 0, time.Now())
	return server, fsys, repo, sourceAbs
}

func newPromotionRecoveryFixture(t *testing.T, phase localPromotionPhase, fence string, handler func(http.ResponseWriter, *http.Request, *localPromotionRecord)) (*httptest.Server, *Dat9FS, *localPromotionRecord, string, string) {
	t.Helper()
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{AllocationEpoch: 7, AllocationSequence: 11})
	if err != nil {
		t.Fatal(err)
	}
	localRoot := t.TempDir()
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: localRoot, EnableSynchronousPromotion: true}
	opts.setDefaults()
	sourceAbs := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(sourceAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceAbs, "child"), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	sourceID, err := ensurePromotionSourceIdentity(sourceAbs)
	if err != nil {
		t.Fatal(err)
	}
	parentDev, parentIno, err := promotionDirectoryIdentity(filepath.Dir(sourceAbs))
	if err != nil {
		t.Fatal(err)
	}
	entries := []promotion.ManifestEntry{{RelativePath: "child", Type: promotion.EntryTypeFile, Mode: uint32(0o600), MtimeNS: 1, ExpectedSizeBytes: 7, ExpectedChecksumSHA256: strings.Repeat("b", 64)}}
	manifest, err := promotion.CanonicalizeManifest(entries, promotionTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	var record *localPromotionRecord
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if record != nil && r.Method == http.MethodPost && r.URL.Path == "/v1/promotions/imports/"+record.MigrationID+":ack-result" {
			http.Error(w, "acknowledgment temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		handler(w, r, record)
	}))
	fsys := NewDat9FS(newTestClient(server.URL), opts)
	stateRoot, localRootUUID, err := fsys.ensurePromotionStateRoot()
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	plan := promotion.ImportPlan{
		Target: "/repo/site", ExpectedTargetAbsent: true, ManifestHash: manifest.ManifestHash,
		EntryTotal: manifest.EntryTotal, ByteTotal: manifest.ByteTotal, MaxContentSize: manifest.MaxContentSize,
		StorageMode: promotion.StorageModeDB9Inline, StoragePlanDigest: "plan", Limits: promotionTestLimits(),
		BackendCapabilityGeneration: 1, ConfigGeneration: 1, PlanExpiresAt: time.Now().Add(time.Hour),
	}
	serverState := "COMMITTED"
	if phase == promotionPhaseStaging {
		serverState = "STAGING"
	} else if phase == promotionPhaseCommitRequested {
		serverState = "VERIFIED"
	}
	record = &localPromotionRecord{
		Version: promotionRecordVersion, OperationID: "op-recovery", MigrationID: migrationID,
		AllocationEpoch: 7, AllocationSequence: 11, AllocationIdempotencyKey: "allocate-key", AllocationProof: "proof",
		CreateBefore: time.Now().Add(time.Hour),
		SourcePath:   "/repo/.git", SourceParentDev: parentDev, SourceParentIno: parentIno,
		TargetPath: "/repo/site", RemoteTarget: "/repo/site",
		LocalRootUUID: localRootUUID, SourceIncarnationUUID: sourceID, ManifestHash: manifest.ManifestHash,
		Plan: plan, OwnerEpoch: 1, OwnerToken: "owner", RecoveryToken: "recovery",
		ActivityDeadline:          time.Now().Add(time.Hour),
		AcceptedRestoreGeneration: 1, AcceptedDatabaseIncarnation: "db-1", AcceptedWriterGeneration: 1,
		RestoreGeneration: 1, DatabaseIncarnation: "db-1", WriterGeneration: 1,
		ServerState: serverState, FenceState: fence, Phase: phase,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if serverState == "COMMITTED" {
		_, record.TerminalResultDigest = promotionTestTerminal("COMMITTED")
	}
	opDir := filepath.Join(stateRoot, "operations", record.OperationID)
	if err := persistPromotionManifest(opDir, manifest.Entries); err != nil {
		server.Close()
		t.Fatal(err)
	}
	if err := persistPromotionRecord(opDir, record); err != nil {
		server.Close()
		t.Fatal(err)
	}
	return server, fsys, record, opDir, sourceAbs
}

func promotionTestRecoveryStatus(record *localPromotionRecord, state string) promotion.ImportStatus {
	status := promotionTestStatus(record.MigrationID, state)
	status.AllocationEpoch = record.AllocationEpoch
	status.AllocationSequence = record.AllocationSequence
	status.Target = record.RemoteTarget
	status.ManifestHash = record.ManifestHash
	status.OwnerEpoch = record.OwnerEpoch
	status.RestoreGeneration = record.RestoreGeneration
	status.DatabaseIncarnation = record.DatabaseIncarnation
	status.WriterGeneration = record.WriterGeneration
	return status
}

func promotionTestLimits() promotion.ManifestLimits {
	return promotion.ManifestLimits{
		MaxEntries: promotionMaxEntries, MaxMetadataBytes: promotionMaxMetadataBytes,
		MaxTotalInlineBytes: promotionMaxInlineBytes, MaxPathBytes: promotionMaxPathBytes,
		MaxSymlinkBytes: promotionMaxSymlinkBytes, MaxDepth: promotionMaxDepth,
		InlineThreshold: 50_000,
	}
}

func promotionTestStatus(migrationID, state string) promotion.ImportStatus {
	status := promotion.ImportStatus{
		MigrationID: migrationID, AllocationEpoch: 1, AllocationSequence: 1,
		Target: "/repo/site", ManifestHash: "manifest", QuotaReservationID: "quota",
		State: state, StateVersion: 1, OwnerEpoch: 1,
		ActivityDeadline: time.Now().Add(time.Hour), LeaseExpiresAt: time.Now().Add(time.Minute),
		RestoreGeneration: 1, DatabaseIncarnation: "db-1", WriterGeneration: 1,
	}
	if state == "COMMITTED" || state == "ABORTED" {
		status.TerminalResultBlob, status.TerminalResultDigest = promotionTestTerminal(state)
	}
	return status
}

func promotionTestTerminal(state string) (string, string) {
	blob := `{"state":"` + state + `"}`
	sum := sha256.Sum256([]byte(blob))
	return blob, hex.EncodeToString(sum[:])
}

func assertPromotionCallSubsequence(t *testing.T, got, want []string) {
	t.Helper()
	index := 0
	for _, call := range got {
		if index < len(want) && call == want[index] {
			index++
		}
	}
	if index != len(want) {
		t.Fatalf("calls = %v, missing ordered suffix from %v", got, want[index:])
	}
}
