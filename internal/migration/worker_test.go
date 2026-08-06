package migration

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type memoryTargetNode struct {
	data       []byte
	revision   int64
	mode       uint32
	resourceID string
}

type memoryTarget struct {
	mu          sync.Mutex
	nodes       map[string]memoryTargetNode
	writes      int
	listCalls   int
	failList    bool
	failListAt  int
	failPut     bool
	conflictPut bool
	listHit     chan struct{}
	eventStatus int
	events      []client.MigrationEvent
}

type workerServer struct {
	target     *memoryTarget
	checkpoint *checkpointFake
	mu         sync.Mutex
	auth       []string
	caps       client.MigrationCapabilities
}

func (s *workerServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.auth = append(s.auth, r.Header.Get("Authorization"))
	s.mu.Unlock()
	switch {
	case r.URL.Path == "/v1/status":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"max_upload_bytes": 1 << 20, "inline_threshold": 1 << 10,
			"migration_capabilities": s.caps,
		})
	case strings.HasPrefix(r.URL.Path, "/v1/fs/.drive9-migration"):
		s.checkpoint.serveHTTP(w, r)
	default:
		s.target.handler(w, r)
	}
}

func (m *memoryTarget) handler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.URL.Path == "/v1/migration/events" {
		var event client.MigrationEvent
		_ = json.NewDecoder(r.Body).Decode(&event)
		m.events = append(m.events, event)
		if m.eventStatus != 0 {
			http.Error(w, "injected event failure", m.eventStatus)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list") == "1" {
		m.listCalls++
		if m.listHit != nil {
			select {
			case m.listHit <- struct{}{}:
			default:
			}
		}
		if m.failList || m.failListAt == m.listCalls {
			http.Error(w, "injected list failure", http.StatusServiceUnavailable)
			return
		}
		names := make([]string, 0, len(m.nodes))
		for name := range m.nodes {
			names = append(names, name)
		}
		sort.Strings(names)
		entries := make([]client.FileInfo, 0, len(names))
		for _, name := range names {
			node := m.nodes[name]
			entries = append(entries, client.FileInfo{Name: name, Size: int64(len(node.data)), Revision: node.revision, Mode: node.mode, HasMode: true, ResourceID: node.resourceID, Nlink: 1})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat" {
		var request struct {
			Paths           []string `json:"paths"`
			IncludeChecksum bool     `json:"include_checksum"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		results := make([]client.BatchStatResult, len(request.Paths))
		for i, remote := range request.Paths {
			name := strings.TrimPrefix(remote, "/data/")
			node, ok := m.nodes[name]
			if !ok {
				results[i] = client.BatchStatResult{Path: remote, Status: http.StatusNotFound, Error: "missing"}
				continue
			}
			results[i] = client.BatchStatResult{Path: remote, Status: http.StatusOK, Size: int64(len(node.data)), Revision: node.revision, Mode: node.mode, HasMode: true, ResourceID: node.resourceID, Nlink: 1}
			if request.IncludeChecksum {
				sum := sha256.Sum256(node.data)
				results[i].ChecksumSHA256 = hex.EncodeToString(sum[:])
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/v1/fs/data/")
	node, exists := m.nodes[name]
	switch r.Method {
	case http.MethodHead:
		if !exists {
			http.NotFound(w, r)
			return
		}
		sum := sha256.Sum256(node.data)
		w.Header().Set("Content-Length", strconv.Itoa(len(node.data)))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(node.revision, 10))
		w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(node.mode), 10))
		w.Header().Set("X-Dat9-Resource-ID", node.resourceID)
		w.Header().Set("X-Dat9-Nlink", "1")
		w.Header().Set("X-Dat9-Checksum-SHA256", hex.EncodeToString(sum[:]))
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		if m.failPut {
			http.Error(w, "injected write failure", http.StatusServiceUnavailable)
			return
		}
		if m.conflictPut {
			m.conflictPut = false
			http.Error(w, "injected revision conflict", http.StatusConflict)
			return
		}
		expected, _ := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
		if (!exists && expected != 0) || (exists && expected != node.revision) {
			http.Error(w, "revision conflict", http.StatusConflict)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !exists {
			node = memoryTargetNode{mode: 0o100644, resourceID: "id-" + name}
		}
		node.data, node.revision = body, node.revision+1
		m.nodes[name] = node
		m.writes++
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": node.revision})
	case http.MethodPost:
		if !r.URL.Query().Has("chmod") || !exists {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Mode uint32 `json:"mode"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		node.mode = body.Mode
		m.nodes[name] = node
	case http.MethodDelete:
		if !exists {
			http.NotFound(w, r)
			return
		}
		delete(m.nodes, name)
		m.writes++
	default:
		http.NotFound(w, r)
	}
}

func newRoundWorker(t *testing.T, root string, target *memoryTarget) (*Worker, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(target.handler))
	api := client.New(server.URL, "key")
	api.SetSmallFileThresholdForTests(1 << 20)
	scanner, _ := NewScanner(root)
	inventory, _ := NewTargetScanner(api, "/data")
	apply, err := NewApplyEngine(api, scanner, ApplyConfig{Prefix: "/data", Phase: PhaseSyncing, SmallFileThreshold: 1 << 20, SmallWorkers: 2, LargeWorkers: 1, MaxBytesPerSecond: 1 << 30})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return &Worker{api: api, scanner: scanner, inventory: inventory, apply: apply, state: NewState(PhaseSyncing)}, server
}

func newDualWorker(t *testing.T, root string, target *memoryTarget, grace time.Duration, now *time.Time) (*Worker, *httptest.Server) {
	t.Helper()
	worker, server := newRoundWorker(t, root, target)
	worker.state = NewState(PhaseDualWriteRepairing)
	worker.gracePeriod = grace
	worker.now = func() time.Time { return *now }
	worker.apply.config.Phase = PhaseDualWriteRepairing
	return worker, server
}

func newWorkerStartup(t *testing.T, root string, server *httptest.Server) *Startup {
	t.Helper()
	secretRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(secretRoot, "owner-key"), []byte("first-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	credential, err := NewCredentialSource(secretRoot, "owner-key")
	if err != nil {
		t.Fatal(err)
	}
	config := &Config{
		Version: ConfigVersion, Drive9: Drive9Config{Endpoint: server.URL},
		JobDefaults: JobDefaults{
			Sync:        SyncDefaults{GracePeriod: Duration(time.Minute)},
			Performance: PerformanceDefaults{MaxBytesPerSecond: 1 << 20, SmallFileWorkers: 1, LargeFileWorkers: 1},
		},
		Spaces: map[string]SpaceConfig{"space": {CredentialRef: "owner-key"}},
	}
	job := Job{VolumeID: "vol-001", NodeName: "node", Source: SourceConfig{Type: "ebs", Root: root}, Target: TargetConfig{SpaceRef: "space", Prefix: "/data"}}
	config.Jobs = []Job{job}
	hash, err := ConfigHash(config, job)
	if err != nil {
		t.Fatal(err)
	}
	return &Startup{Config: config, Job: job, Space: config.Spaces["space"], Phase: PhaseSyncing, ConfigHash: hash, Credential: credential}
}

func allWorkerCapabilities() client.MigrationCapabilities {
	return client.MigrationCapabilities{ChecksumRead: true, ChecksumComplete: true, ConditionalCreate: true, ConditionalUpdate: true, EventIngest: true}
}

func TestWorkerSyncingInitialIncrementalRenameAndRestart(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	if err := worker.Round(context.Background(), RoundModeDeep); err != nil {
		t.Fatal(err)
	}
	if worker.State().Conditions.ReadyForRollout {
		t.Fatal("complete round bypassed startup recovery")
	}
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !worker.State().Conditions.ReadyForRollout {
		t.Fatal("initial copy did not become ready")
	}
	target.mu.Lock()
	writesAfterInitial := target.writes
	target.mu.Unlock()
	if err := worker.Round(context.Background(), RoundModeFull); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	if target.writes != writesAfterInitial {
		t.Fatalf("idempotent round wrote again: %d -> %d", writesAfterInitial, target.writes)
	}
	target.mu.Unlock()

	if err := os.WriteFile(filepath.Join(root, "a"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := worker.Round(context.Background(), RoundModeFull); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "b"), filepath.Join(root, "c")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "a")); err != nil {
		t.Fatal(err)
	}
	if err := worker.Round(context.Background(), RoundModeFull); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	if len(target.nodes) != 1 || string(target.nodes["c"].data) != "new" {
		t.Fatalf("target after rename/delete=%+v", target.nodes)
	}
	target.mu.Unlock()

	restarted, restartedServer := newRoundWorker(t, root, target)
	defer restartedServer.Close()
	if restarted.State().Conditions.ReadyForRollout || restarted.State().LastComplete != nil {
		t.Fatal("restart inherited readiness or Round")
	}
	if err := restarted.DeepRecovery(context.Background()); err != nil || !restarted.State().Conditions.ReadyForRollout {
		t.Fatalf("restart recovery error=%v state=%+v", err, restarted.State())
	}
}

func TestIndependentJobsDoNotShareTargetOrProcessState(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(rootA, "a"), []byte("job-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootB, "b"), []byte("job-b"), 0o644); err != nil {
		t.Fatal(err)
	}
	targetA := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	targetB := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	workerA, serverA := newRoundWorker(t, rootA, targetA)
	defer serverA.Close()
	workerB, serverB := newRoundWorker(t, rootB, targetB)
	defer serverB.Close()
	if err := workerA.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := workerB.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	workerA.state.SetAttention(true)
	if workerB.State().Conditions.Attention {
		t.Fatal("Job B inherited Job A process-local Attention")
	}
	if err := os.WriteFile(filepath.Join(rootA, "a"), []byte("job-a-updated"), 0o644); err != nil {
		t.Fatal(err)
	}
	workerA.state.SetAttention(false)
	if err := workerA.Round(context.Background(), RoundModeFull); err != nil {
		t.Fatal(err)
	}
	targetA.mu.Lock()
	dataA := append([]byte(nil), targetA.nodes["a"].data...)
	_, leakedB := targetA.nodes["b"]
	targetA.mu.Unlock()
	targetB.mu.Lock()
	dataB := append([]byte(nil), targetB.nodes["b"].data...)
	_, leakedA := targetB.nodes["a"]
	targetB.mu.Unlock()
	if string(dataA) != "job-a-updated" || string(dataB) != "job-b" || leakedA || leakedB {
		t.Fatalf("independent targets crossed: A=%q B=%q leakedA=%v leakedB=%v", dataA, dataB, leakedA, leakedB)
	}
}

func TestWorkerEmptyRecoveryConvergesOnlyAfterRecoveryGate(t *testing.T) {
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, server := newRoundWorker(t, t.TempDir(), target)
	defer server.Close()
	if err := worker.Round(context.Background(), RoundModeDeep); err != nil {
		t.Fatal(err)
	}
	if worker.State().Conditions.ReadyForRollout {
		t.Fatal("empty complete scan bypassed recovery gate")
	}
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if !snapshot.Current.ScanComplete || !snapshot.Current.Converged || !snapshot.Conditions.ReadyForRollout {
		t.Fatalf("empty recovery state=%+v", snapshot)
	}
}

func TestNewWorkerBuildsRuntimeAndReloadsCredential(t *testing.T) {
	root := t.TempDir()
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	backend := &workerServer{target: target, checkpoint: &checkpointFake{}, caps: allWorkerCapabilities()}
	server := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer server.Close()
	startup := newWorkerStartup(t, root, server)

	worker, err := NewWorker(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	if worker.api == nil || worker.inventory == nil || worker.apply == nil || worker.checkpoint == nil || worker.recovery == nil || worker.State().RecoveryComplete {
		t.Fatalf("incomplete Worker runtime=%+v", worker)
	}
	if err := os.WriteFile(startup.Credential.path, []byte("rotated-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := worker.refreshClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	lastAuth := backend.auth[len(backend.auth)-1]
	backend.mu.Unlock()
	if lastAuth != "Bearer rotated-key" {
		t.Fatalf("rotated authorization=%q", lastAuth)
	}

	if _, err := NewWorker(context.Background(), nil); err == nil {
		t.Fatal("nil startup accepted")
	}
	backend.caps.ConditionalUpdate = false
	if _, err := NewWorker(context.Background(), startup); err == nil {
		t.Fatal("missing capability accepted")
	}
}

func TestRunWorkerCancelsCleanly(t *testing.T) {
	root := t.TempDir()
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode), listHit: make(chan struct{}, 1)}
	backend := &workerServer{target: target, checkpoint: &checkpointFake{}, caps: allWorkerCapabilities()}
	server := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer server.Close()
	startup := newWorkerStartup(t, root, server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	socket := testControlSocket(t)
	go func() { done <- RunWorkerAt(ctx, startup, socket) }()
	select {
	case <-target.listHit:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not begin recovery Round")
	}
	if err := <-done; err != nil {
		t.Fatalf("canceled Worker error=%v", err)
	}
}

func TestWorkerIncompleteRoundRetainsLastCompleteAndRunCancels(t *testing.T) {
	root := t.TempDir()
	_ = os.WriteFile(filepath.Join(root, "a"), []byte("one"), 0o644)
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode), listHit: make(chan struct{}, 1)}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	lastID := worker.State().LastComplete.ID
	target.mu.Lock()
	target.failList = true
	target.mu.Unlock()
	if err := worker.Round(context.Background(), RoundModeFull); err == nil {
		t.Fatal("injected incomplete Round succeeded")
	}
	snapshot := worker.State()
	if snapshot.LastComplete.ID != lastID || snapshot.Current.ScanComplete || snapshot.Current.Converged || snapshot.Conditions.ReadyForRollout {
		t.Fatalf("failed Round state=%+v", snapshot)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-target.listHit:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("Worker did not retry failed Round")
	}
	if err := <-done; err != nil {
		t.Fatalf("canceled Worker error=%v", err)
	}
}

func TestWorkerRoundBoundaryFailuresRetainLastComplete(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	lastID := worker.State().LastComplete.ID

	if err := os.WriteFile(filepath.Join(root, "a"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	target.failPut = true
	target.mu.Unlock()
	if err := worker.Round(context.Background(), RoundModeFull); err == nil {
		t.Fatal("injected apply failure succeeded")
	}
	assertFailedRound(t, worker.State(), lastID, "apply")

	target.mu.Lock()
	target.failPut = false
	target.failListAt = target.listCalls + 2
	target.mu.Unlock()
	if err := worker.Round(context.Background(), RoundModeFull); err == nil {
		t.Fatal("injected reread failure succeeded")
	}
	assertFailedRound(t, worker.State(), lastID, "reread")

	if err := os.Rename(root, root+".missing"); err != nil {
		t.Fatal(err)
	}
	if err := worker.Round(context.Background(), RoundModeFull); err == nil {
		t.Fatal("injected source scan failure succeeded")
	}
	assertFailedRound(t, worker.State(), lastID, "scan")
}

func assertFailedRound(t *testing.T, snapshot StateSnapshot, lastID, failureClass string) {
	t.Helper()
	if snapshot.LastComplete == nil || snapshot.LastComplete.ID != lastID || snapshot.Current.ScanComplete || snapshot.Current.Converged || snapshot.Current.FailureClass != failureClass || snapshot.Conditions.ReadyForRollout {
		t.Fatalf("failed %s Round state=%+v", failureClass, snapshot)
	}
}

func TestDeepRecoveryPublishesUnsafeCompleteObservation(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "special"), 0o755); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	scanner.identity = func(name string, info os.FileInfo) (fileIdentity, error) {
		identity, identityErr := defaultFileIdentity(name, info)
		if filepath.Base(name) == "special" {
			identity.version.Kind = EntrySpecial
		}
		return identity, identityErr
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	worker.scanner = scanner
	worker.apply.scanner = scanner
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if !snapshot.RecoveryComplete || !snapshot.Current.ScanComplete || snapshot.Current.Converged || snapshot.Conditions.ReadyForRollout || !snapshot.Conditions.Attention {
		t.Fatalf("unsafe recovery state=%+v", snapshot)
	}
}

func TestWorkerErrorClassification(t *testing.T) {
	if !isAuthError(&client.StatusError{StatusCode: http.StatusUnauthorized}) || !isAuthError(&client.StatusError{StatusCode: http.StatusForbidden}) || isAuthError(&client.StatusError{StatusCode: http.StatusTooManyRequests}) {
		t.Fatal("authentication status classification is incorrect")
	}
	for _, err := range []error{
		ErrSourceChanged,
		ErrApplyRescan,
		ErrCheckpointConflict,
		&client.StatusError{StatusCode: http.StatusTooManyRequests},
		&client.StatusError{StatusCode: http.StatusServiceUnavailable},
	} {
		if !retryableWorkerError(err) {
			t.Fatalf("error should be retryable: %v", err)
		}
	}
	if retryableWorkerError(&client.StatusError{StatusCode: http.StatusBadRequest}) || retryableWorkerError(errors.New("permanent")) {
		t.Fatal("permanent error classified as retryable")
	}
}

func TestDualRecoveryGraceRepairAndFastSkip(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	now := time.Now().Add(time.Hour)
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if len(snapshot.Grace) != 1 || snapshot.RepairMtimeFloor == nil || !snapshot.RepairMtimeFloor.Equal(now.Add(-time.Minute)) || snapshot.Conditions.CurrentConverged {
		t.Fatalf("dual recovery state=%+v", snapshot)
	}
	target.mu.Lock()
	if target.writes != 0 {
		t.Fatalf("recovery bypassed grace with %d writes", target.writes)
	}
	target.mu.Unlock()

	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	if target.writes != 0 {
		t.Fatalf("unexpired grace wrote %d times", target.writes)
	}
	target.mu.Unlock()
	now = now.Add(time.Minute)
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	snapshot = worker.State()
	if len(snapshot.Grace) != 0 || len(snapshot.Retry) != 0 || !snapshot.Conditions.CurrentConverged {
		t.Fatalf("post-grace state=%+v", snapshot)
	}

	reads := 0
	worker.scanner.afterRead = func(string) { reads++ }
	worker.state.setRepairMtimeFloor(now.Add(time.Hour))
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	if reads != 0 {
		t.Fatalf("unchanged old path was deeply read %d times", reads)
	}
}

func TestDualTargetOnlyResidueWarnsWithoutDelete(t *testing.T) {
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"residue": {data: []byte("old"), revision: 1, mode: 0o100644, resourceID: "residue-id"},
	}}
	now := time.Now()
	worker, server := newDualWorker(t, t.TempDir(), target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if !snapshot.Conditions.CurrentConverged || snapshot.LastComplete == nil || len(snapshot.LastComplete.Findings) != 1 || snapshot.LastComplete.Findings[0].Kind != FindingTargetOnly || snapshot.LastComplete.Findings[0].Severity != SeverityWarning {
		t.Fatalf("target-only recovery=%+v", snapshot)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if _, exists := target.nodes["residue"]; !exists || target.writes != 0 {
		t.Fatalf("target-only residue was mutated: %+v", target.nodes)
	}
}

func TestDualTokenChangeRestartsGraceAndCASConflictRetries(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "a")
	if err := os.WriteFile(name, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := onlyGrace(t, worker.State()).FirstSeen
	now = now.Add(10 * time.Second)
	target.mu.Lock()
	target.nodes["a"] = memoryTargetNode{data: []byte("wrong"), revision: 7, mode: 0o100644, resourceID: "new-target"}
	target.mu.Unlock()
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	if changed := onlyGrace(t, worker.State()).FirstSeen; !changed.Equal(first) {
		t.Fatalf("target Revision change restarted grace: first=%s changed=%s", first, changed)
	}
	now = now.Add(20 * time.Second)
	if err := os.WriteFile(name, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(name, original.ModTime(), original.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	second := onlyGrace(t, worker.State()).FirstSeen
	if !second.Equal(now) || second.Equal(first) {
		t.Fatalf("token change did not restart grace: first=%s second=%s", first, second)
	}
	now = now.Add(time.Minute)
	target.mu.Lock()
	target.conflictPut = true
	target.mu.Unlock()
	if err := worker.Round(context.Background(), RoundModeFast); !errors.Is(err, ErrApplyRescan) {
		t.Fatalf("CAS conflict error=%v", err)
	}
	if len(worker.State().Retry) != 1 || len(worker.State().Grace) != 1 {
		t.Fatalf("CAS conflict was not queued: %+v", worker.State())
	}
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	if !worker.State().Conditions.CurrentConverged {
		t.Fatalf("CAS retry did not converge: %+v", worker.State())
	}
}

func onlyGrace(t *testing.T, snapshot StateSnapshot) GraceCandidate {
	t.Helper()
	if len(snapshot.Grace) != 1 {
		t.Fatalf("grace candidates=%+v", snapshot.Grace)
	}
	for _, candidate := range snapshot.Grace {
		return candidate
	}
	return GraceCandidate{}
}

func TestDualRenameCreatesNewPathAndRetainsResidue(t *testing.T) {
	root := t.TempDir()
	oldName := filepath.Join(root, "old")
	if err := os.WriteFile(oldName, []byte("value"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"old": {data: []byte("value"), revision: 1, mode: 0o100644, resourceID: "old-id"},
	}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldName, filepath.Join(root, "new")); err != nil {
		t.Fatal(err)
	}
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if _, oldExists := target.nodes["old"]; !oldExists || string(target.nodes["new"].data) != "value" {
		t.Fatalf("post-T0 rename target=%+v", target.nodes)
	}
}

func TestDualUnsafeRevisionBlocksWithoutWrite(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"a": {data: []byte("wrong"), revision: 0, mode: 0o100644, resourceID: "unsafe"},
	}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !worker.State().Conditions.Attention || len(worker.State().Grace) != 0 {
		t.Fatalf("unsafe Revision state=%+v", worker.State())
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.writes != 0 {
		t.Fatalf("unsafe Revision wrote %d times", target.writes)
	}
}

func TestDualChangedTokenRemainsPendingAfterUnstableRead(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "a")
	if err := os.WriteFile(name, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("one"))
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"a": {data: []byte("one"), revision: 1, mode: 0o100644, resourceID: hex.EncodeToString(sum[:4])},
	}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := worker.State().Reconciled["/a"]
	if err := os.WriteFile(name, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	worker.scanner.afterRead = func(string) { _ = os.WriteFile(name, []byte("tri"), 0o644) }
	if err := worker.Round(context.Background(), RoundModeFast); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("unstable read error=%v", err)
	}
	if worker.State().Reconciled["/a"] != old {
		t.Fatal("failed deep read advanced reconciled token")
	}
	worker.scanner.afterRead = nil
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	if len(worker.State().Grace) != 1 {
		t.Fatalf("changed token was lost: %+v", worker.State())
	}
}

func TestDualRevisionReuseResidualABARemainsAcceptedRisk(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"a": {data: []byte("old"), revision: 1, mode: 0o100644, resourceID: "original"},
	}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	target.nodes["a"] = memoryTargetNode{data: []byte("replacement"), revision: 1, mode: 0o100644, resourceID: "replacement"}
	target.mu.Unlock()
	now = now.Add(time.Minute)
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if string(target.nodes["a"].data) != "source" {
		t.Fatal("test no longer demonstrates Revision-only ABA limitation")
	}
}

func TestRetryDelayIsBounded(t *testing.T) {
	for attempt, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second} {
		if got := retryDelay(attempt, time.Second); got != want {
			t.Fatalf("attempt %d delay=%s want=%s", attempt, got, want)
		}
	}
}
