//go:build failpoint

package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
)

const migrationTestFaultFailpoint = "github.com/mem9-ai/drive9/internal/migration/migrationTestFault"

func enableMigrationTestFault(t *testing.T, boundary string) func() {
	t.Helper()
	if err := failpoint.Enable(migrationTestFaultFailpoint, `return("`+boundary+`")`); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	disable := func() {
		once.Do(func() {
			if err := failpoint.Disable(migrationTestFaultFailpoint); err != nil {
				t.Errorf("disable failpoint: %v", err)
			}
		})
	}
	t.Cleanup(disable)
	return disable
}

func injectedMigrationTestFault(boundary string) error {
	var injected error
	failpoint.Inject("migrationTestFault", func(value failpoint.Value) {
		if selected, ok := value.(string); ok && selected == boundary {
			injected = fmt.Errorf("injected migration %s failure", boundary)
		}
	})
	return injected
}

func TestMigrationScanFailpointCannotProduceDelete(t *testing.T) {
	root := t.TempDir()
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"target-only": {data: []byte("keep"), revision: 1, mode: 0o100644, resourceID: "target-only"},
	}}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	original := worker.scanner.identity
	worker.scanner.identity = func(name string, info os.FileInfo) (fileIdentity, error) {
		if err := injectedMigrationTestFault("scan"); err != nil {
			return fileIdentity{}, err
		}
		return original(name, info)
	}
	enableMigrationTestFault(t, "scan")
	if err := worker.Round(context.Background(), RoundModeFull); err == nil {
		t.Fatal("injected incomplete scan succeeded")
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.writes != 0 || target.nodes["target-only"].revision != 1 {
		t.Fatalf("incomplete scan mutated target: writes=%d nodes=%+v", target.writes, target.nodes)
	}
	if snapshot := worker.State(); snapshot.Current.ScanComplete || snapshot.LastComplete != nil {
		t.Fatalf("incomplete scan was published: %+v", snapshot)
	}
}

func TestMigrationChecksumFailpointDiscardsUnstableRead(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "a")
	if err := os.WriteFile(name, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scanner.afterRead = func(string) {
		if injectedMigrationTestFault("checksum") == nil {
			return
		}
		replacement := filepath.Join(root, "replacement")
		if err := os.WriteFile(replacement, []byte("after!"), 0o600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(replacement, name); err != nil {
			t.Error(err)
		}
	}
	enableMigrationTestFault(t, "checksum")
	if _, err := scanner.ReadStable(context.Background(), "/a", scan.Entries["/a"].Version); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("stable read error=%v, want ErrSourceChanged", err)
	}
}

type failpointMultipartRemote struct {
	mu               sync.Mutex
	baseURL          string
	body             []byte
	checksum         string
	initiates        int
	partUploads      map[string]map[int]int
	completeAttempts map[string]int
	aborts           map[string]int
	committed        bool
	revision         int64
}

func newFailpointMultipartRemote(t *testing.T, body []byte) (*failpointMultipartRemote, *httptest.Server) {
	t.Helper()
	sum := sha256.Sum256(body)
	remote := &failpointMultipartRemote{
		body: body, checksum: hex.EncodeToString(sum[:]), partUploads: make(map[string]map[int]int),
		completeAttempts: make(map[string]int), aborts: make(map[string]int),
	}
	server := httptest.NewServer(http.HandlerFunc(remote.serveHTTP))
	remote.baseURL = server.URL
	return remote, server
}

func (r *failpointMultipartRemote) serveHTTP(w http.ResponseWriter, request *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	switch {
	case request.Method == http.MethodHead && request.URL.Path == "/v1/fs/data/file":
		if !r.committed {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(r.body)))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(r.revision, 10))
		w.Header().Set("X-Dat9-Resource-ID", "resource-file")
		w.Header().Set("X-Dat9-Mode", strconv.FormatUint(0o100600, 10))
		w.Header().Set("X-Dat9-Checksum-SHA256", r.checksum)
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/initiate":
		var input struct {
			Path             string `json:"path"`
			TotalSize        int64  `json:"total_size"`
			ExpectedRevision *int64 `json:"expected_revision"`
		}
		if json.NewDecoder(request.Body).Decode(&input) != nil || input.Path != "/data/file" || input.TotalSize != int64(len(r.body)) || input.ExpectedRevision == nil || *input.ExpectedRevision != 0 {
			http.Error(w, "invalid initiate", http.StatusBadRequest)
			return
		}
		r.initiates++
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": fmt.Sprintf("upload-%d", r.initiates), "part_size": 4, "total_parts": 2,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}},
		})
	case request.Method == http.MethodPost && len(segments) == 4 && segments[0] == "v2" && segments[1] == "uploads" && segments[3] == "presign-batch":
		var input struct {
			Parts []struct {
				PartNumber int `json:"part_number"`
			} `json:"parts"`
		}
		if json.NewDecoder(request.Body).Decode(&input) != nil {
			http.Error(w, "invalid presign", http.StatusBadRequest)
			return
		}
		parts := make([]map[string]any, 0, len(input.Parts))
		for _, part := range input.Parts {
			parts = append(parts, map[string]any{
				"number": part.PartNumber, "url": fmt.Sprintf("%s/parts/%s/%d", r.baseURL, segments[2], part.PartNumber),
				"size": 4, "expires_at": time.Now().Add(time.Minute),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"parts": parts})
	case request.Method == http.MethodPut && len(segments) == 3 && segments[0] == "parts":
		if err := injectedMigrationTestFault("part_upload"); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		part, _ := strconv.Atoi(segments[2])
		if r.partUploads[segments[1]] == nil {
			r.partUploads[segments[1]] = make(map[int]int)
		}
		r.partUploads[segments[1]][part]++
		_, _ = io.Copy(io.Discard, request.Body)
		w.Header().Set("ETag", fmt.Sprintf("etag-%s-%d", segments[1], part))
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && len(segments) == 4 && segments[0] == "v2" && segments[1] == "uploads" && segments[3] == "complete":
		r.completeAttempts[segments[2]]++
		if err := injectedMigrationTestFault("complete"); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		r.committed = true
		r.revision++
		w.WriteHeader(http.StatusNoContent)
	case request.Method == http.MethodPost && len(segments) == 4 && segments[0] == "v2" && segments[1] == "uploads" && segments[3] == "abort":
		r.aborts[segments[2]]++
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, request)
	}
}

func exerciseMultipartFailpoint(t *testing.T, boundary string) {
	t.Helper()
	body := []byte("abcdefgh")
	remote, server := newFailpointMultipartRemote(t, body)
	defer server.Close()
	api := client.New(server.URL, "owner-key")
	api.SetSmallFileThresholdForTests(1)
	disable := enableMigrationTestFault(t, boundary)
	if _, err := api.WriteStreamConditionalWithChecksum(context.Background(), "/data/file", bytes.NewReader(body), int64(len(body)), nil, 0, remote.checksum); err == nil {
		t.Fatalf("injected %s failure succeeded", boundary)
	}
	disable()
	committed, err := api.WriteStreamConditionalWithChecksum(context.Background(), "/data/file", bytes.NewReader(body), int64(len(body)), nil, 0, remote.checksum)
	if err != nil || committed.Revision != 1 || committed.ChecksumSHA256 != remote.checksum {
		t.Fatalf("fresh retry committed=%+v err=%v", committed, err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if remote.initiates != 2 || remote.aborts["upload-1"] == 0 {
		t.Fatalf("fresh sessions=%d aborts=%+v", remote.initiates, remote.aborts)
	}
	for part := 1; part <= 2; part++ {
		if remote.partUploads["upload-2"][part] != 1 {
			t.Fatalf("fresh upload part %d count=%d", part, remote.partUploads["upload-2"][part])
		}
		if boundary == "complete" && remote.partUploads["upload-1"][part] != 1 {
			t.Fatalf("complete retry did not abandon fully uploaded session: part %d count=%d", part, remote.partUploads["upload-1"][part])
		}
	}
}

func TestMigrationPartUploadFailpointStartsFreshSession(t *testing.T) {
	exerciseMultipartFailpoint(t, "part_upload")
}

func TestMigrationCompleteFailpointStartsFreshSession(t *testing.T) {
	exerciseMultipartFailpoint(t, "complete")
}

func TestMigrationCheckpointCASFailpointFailsClosed(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	enableMigrationTestFault(t, "checkpoint_cas")
	fake.failCAS = injectedMigrationTestFault("checkpoint_cas") != nil
	if _, err := store.Recover(context.Background(), startup); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("checkpoint CAS error=%v", err)
	}
	if fake.writes != 0 {
		t.Fatalf("failed checkpoint CAS published %d writes", fake.writes)
	}
}

func TestMigrationFullVerificationFailpointResetsOperationalAttempt(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "a"}}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	original := worker.scanner.identity
	worker.scanner.identity = func(name string, info os.FileInfo) (fileIdentity, error) {
		if err := injectedMigrationTestFault("full_verification"); err != nil {
			return fileIdentity{}, err
		}
		return original(name, info)
	}
	disable := enableMigrationTestFault(t, "full_verification")
	result, err := worker.VerifyFull(context.Background())
	if err == nil || result.Status != "" || worker.State().Verification.Status != "" || worker.State().Phase != PhaseDualWriteRepairing {
		t.Fatalf("verification=%+v state=%+v err=%v", result, worker.State().Verification, err)
	}
	disable()
	result, err = worker.VerifyFull(context.Background())
	if err != nil || result.Status != "passed" || worker.State().Verification.Status != "passed" {
		t.Fatalf("verification retry=%+v state=%+v err=%v", result, worker.State().Verification, err)
	}
}

func TestMigrationFenceIntentFailpointResumesBeforeIntent(t *testing.T) {
	worker, _, _, server := newFenceWorker(t)
	defer server.Close()
	worker.checkpoint.beforeWrite = func(checkpoint Checkpoint) error {
		if checkpoint.FenceIntent && !checkpoint.FenceComplete {
			return injectedMigrationTestFault("fence_intent")
		}
		return nil
	}
	enableMigrationTestFault(t, "fence_intent")
	if _, err := worker.PrepareCutover(context.Background()); err == nil || worker.fenceIntent.Load() || worker.writesFenced.Load() {
		t.Fatalf("pre-intent failure err=%v status=%+v", err, worker.statusOutput())
	}
}

func TestMigrationFenceCompleteFailpointRecoversOnlyForward(t *testing.T) {
	worker, _, _, server := newFenceWorker(t)
	defer server.Close()
	worker.checkpoint.beforeWrite = func(checkpoint Checkpoint) error {
		if checkpoint.FenceComplete {
			return injectedMigrationTestFault("fence_complete")
		}
		return nil
	}
	disable := enableMigrationTestFault(t, "fence_complete")
	if _, err := worker.PrepareCutover(context.Background()); err == nil || !worker.fenceIntent.Load() || !worker.writesFenced.Load() || worker.fenceComplete.Load() {
		t.Fatalf("post-intent failure err=%v status=%+v", err, worker.statusOutput())
	}
	if err := worker.Round(context.Background(), RoundModeFast); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("post-intent write path error=%v", err)
	}
	disable()
	checkpoint, err := worker.PrepareCutover(context.Background())
	if err != nil || !checkpoint.FenceComplete || checkpoint.HighestPhase != PhaseCutoverReady {
		t.Fatalf("forward recovery checkpoint=%+v err=%v", checkpoint, err)
	}
}
