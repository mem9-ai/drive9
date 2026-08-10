package migration

import (
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
	mu              sync.Mutex
	nodes           map[string]memoryTargetNode
	writes          int
	listCalls       int
	failList        bool
	failListAt      int
	failListCount   int
	failListStatus  int
	failPut         bool
	failPutStatus   int
	conflictPut     bool
	failChmodCount  int
	failChmodStatus int
	changeAfterList bool
	afterList       func()
	afterHead       func()
	listHit         chan struct{}
	putStarted      chan struct{}
	putRelease      chan struct{}
	eventStatus     int
	events          []client.MigrationEvent
}

type workerServer struct {
	target           *memoryTarget
	checkpoint       *checkpointFake
	checkpointByAuth map[string]*checkpointFake
	rejectAuth       map[string]bool
	rejectStatus     int
	onReject         func()
	mu               sync.Mutex
	auth             []string
	caps             client.MigrationCapabilities
}

func TestMemoryTargetPausedPutAllowsConcurrentHead(t *testing.T) {
	target := &memoryTarget{
		nodes:      map[string]memoryTargetNode{"file": {data: []byte("old"), revision: 1, mode: 0o100644, resourceID: "id-file"}},
		putStarted: make(chan struct{}, 1),
		putRelease: make(chan struct{}),
	}
	server := httptest.NewServer(http.HandlerFunc(target.handler))
	defer server.Close()
	var release sync.Once
	releasePut := func() { release.Do(func() { close(target.putRelease) }) }
	t.Cleanup(releasePut)

	putDone := make(chan error, 1)
	go func() {
		request, err := http.NewRequest(http.MethodPut, server.URL+"/v1/fs/data/file", strings.NewReader("new"))
		if err == nil {
			request.Header.Set("X-Dat9-Expected-Revision", "1")
			response, doErr := http.DefaultClient.Do(request)
			if doErr != nil {
				err = doErr
			} else {
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK {
					err = fmt.Errorf("PUT status=%d", response.StatusCode)
				}
			}
		}
		putDone <- err
	}()
	select {
	case <-target.putStarted:
	case <-time.After(time.Second):
		t.Fatal("PUT did not reach the pause gate")
	}

	headCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	headRequest, err := http.NewRequestWithContext(headCtx, http.MethodHead, server.URL+"/v1/fs/data/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	headResponse, err := http.DefaultClient.Do(headRequest)
	if err != nil {
		releasePut()
		<-putDone
		t.Fatalf("HEAD was blocked by paused PUT: %v", err)
	}
	_ = headResponse.Body.Close()
	if headResponse.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status=%d", headResponse.StatusCode)
	}

	releasePut()
	if err := <-putDone; err != nil {
		t.Fatal(err)
	}
}

func (m *memoryTarget) resourceNlink(resourceID string) uint32 {
	var count uint32
	for _, node := range m.nodes {
		if node.resourceID == resourceID {
			count++
		}
	}
	return count
}

func (s *workerServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	authorization := r.Header.Get("Authorization")
	s.auth = append(s.auth, authorization)
	checkpoint := s.checkpoint
	if candidate, exists := s.checkpointByAuth[authorization]; exists {
		checkpoint = candidate
	}
	rejected := s.rejectAuth[authorization]
	rejectStatus := s.rejectStatus
	onReject := s.onReject
	s.mu.Unlock()
	if rejected {
		if onReject != nil {
			onReject()
		}
		if rejectStatus == 0 {
			rejectStatus = http.StatusUnauthorized
		}
		http.Error(w, "unauthorized", rejectStatus)
		return
	}
	switch {
	case r.URL.Path == "/v1/status":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"max_upload_bytes": 1 << 20, "inline_threshold": 1 << 10,
			"migration_capabilities": s.caps,
		})
	case strings.HasPrefix(r.URL.Path, "/v1/fs/.drive9-migration"):
		if checkpoint == nil {
			http.NotFound(w, r)
			return
		}
		checkpoint.serveHTTP(w, r)
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
		if m.failList || m.failListAt == m.listCalls || m.failListCount > 0 {
			if m.failListCount > 0 {
				m.failListCount--
			}
			status := m.failListStatus
			if status == 0 {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, "injected list failure", status)
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
			entries = append(entries, client.FileInfo{Name: name, Size: int64(len(node.data)), Revision: node.revision, Mode: node.mode, HasMode: true, ResourceID: node.resourceID, Nlink: m.resourceNlink(node.resourceID)})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
		if m.afterList != nil {
			afterList := m.afterList
			m.afterList = nil
			afterList()
		}
		if m.changeAfterList && len(names) > 0 {
			node := m.nodes[names[0]]
			node.revision++
			m.nodes[names[0]] = node
			m.changeAfterList = false
		}
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
			results[i] = client.BatchStatResult{Path: remote, Status: http.StatusOK, Size: int64(len(node.data)), Revision: node.revision, Mode: node.mode, HasMode: true, ResourceID: node.resourceID, Nlink: m.resourceNlink(node.resourceID)}
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
		w.Header().Set("X-Dat9-Nlink", strconv.FormatUint(uint64(m.resourceNlink(node.resourceID)), 10))
		w.Header().Set("X-Dat9-Checksum-SHA256", hex.EncodeToString(sum[:]))
		w.WriteHeader(http.StatusOK)
		if m.afterHead != nil {
			afterHead := m.afterHead
			m.afterHead = nil
			afterHead()
		}
	case http.MethodPut:
		if m.putStarted != nil {
			select {
			case m.putStarted <- struct{}{}:
			default:
			}
		}
		if m.putRelease != nil {
			putRelease := m.putRelease
			m.mu.Unlock()
			<-putRelease
			m.mu.Lock()
			node, exists = m.nodes[name]
		}
		if m.failPut {
			status := m.failPutStatus
			if status == 0 {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, "injected write failure", status)
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
		if r.URL.Query().Has("hardlink") {
			sourceName := strings.TrimPrefix(r.Header.Get("X-Dat9-Hardlink-Source"), "/data/")
			source, sourceExists := m.nodes[sourceName]
			if exists || !sourceExists {
				http.Error(w, "hardlink conflict", http.StatusConflict)
				return
			}
			m.nodes[name] = source
			m.writes++
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !r.URL.Query().Has("chmod") || !exists {
			http.NotFound(w, r)
			return
		}
		if m.failChmodCount > 0 {
			m.failChmodCount--
			status := m.failChmodStatus
			if status == 0 {
				status = http.StatusNotFound
			}
			http.Error(w, "injected chmod failure", status)
			return
		}
		var body struct {
			Mode uint32 `json:"mode"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		node.mode = node.mode&0o170000 | body.Mode&0o777
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

func TestSyncingDeletesStaleHardlinkAliasAndConverges(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"a": {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "shared"},
		"b": {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "shared"},
	}}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()

	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	_, primaryExists := target.nodes["a"]
	_, staleExists := target.nodes["b"]
	writes := target.writes
	target.mu.Unlock()
	if !primaryExists || staleExists || writes != 1 {
		t.Fatalf("target primary=%v stale=%v writes=%d", primaryExists, staleExists, writes)
	}
	snapshot := worker.State()
	if !snapshot.RecoveryComplete || !snapshot.Current.Converged || !snapshot.Conditions.ReadyForRollout || snapshot.Conditions.Attention {
		t.Fatalf("stale hardlink alias recovery=%+v", snapshot)
	}
}

func TestWorkerCarvesOutReservedControlPrefixEveryRound(t *testing.T) {
	root := t.TempDir()
	control := filepath.Join(root, strings.TrimPrefix(ControlPrefix, "/"))
	if err := os.Mkdir(control, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(control, "payload"), []byte("must-not-upload"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	inventory, err := NewTargetScanner(worker.api, "/")
	if err != nil {
		t.Fatal(err)
	}
	worker.inventory = inventory
	worker.apply.config.Prefix = "/"

	if err := worker.Round(context.Background(), RoundModeFull); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if snapshot.LastComplete == nil || !hasFindingAt(snapshot.LastComplete.Findings, ControlPrefix, FindingControlPrefix) {
		t.Fatalf("round=%+v", snapshot.LastComplete)
	}
	if !snapshot.Conditions.Attention {
		t.Fatalf("reserved control collision did not set Attention: %+v", snapshot.Conditions)
	}
	if _, exists := snapshot.LastComplete.Source[ControlPrefix]; exists {
		t.Fatalf("reserved source leaked into business manifest: %+v", snapshot.LastComplete.Source)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.writes != 0 {
		t.Fatalf("reserved control collision caused %d target writes", target.writes)
	}
}

func TestWorkerRunRedactsSourcePathFromTerminalError(t *testing.T) {
	root := t.TempDir()
	const sensitivePath = "customer-secret-name"
	if err := os.WriteFile(filepath.Join(root, sensitivePath), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode), failPut: true, failPutStatus: http.StatusBadRequest}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()

	err := worker.Run(context.Background())
	if err == nil {
		t.Fatal("worker returned nil for permanent upload failure")
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		if strings.Contains(current.Error(), sensitivePath) {
			t.Fatalf("terminal error chain leaked source path: %v", current)
		}
	}
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("terminal error lost typed cause: %T %v", err, err)
	}
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
	return &Startup{
		Config: config, Job: job, Space: config.Spaces["space"], Phase: PhaseSyncing,
		ConfigHash: hash, Credential: credential, mountProbe: testMountedSourceProbe,
	}
}

func replaceSourceRootWithEmptyDirectory(t *testing.T, root string) {
	t.Helper()
	mounted := root + "-detached"
	if err := os.Rename(root, mounted); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
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

func TestWorkerSyncingRepairsSymlinkMode(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "link")
	if err := os.Symlink("target", name); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	wantMode := uint32(info.Mode().Perm())
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"link": {data: []byte("target"), revision: 1, mode: 0o120000 | (wantMode ^ 0o100), resourceID: "link-id"},
	}}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()

	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	got := target.nodes["link"].mode
	target.mu.Unlock()
	if got&0o170000 != 0o120000 || got&0o777 != wantMode {
		t.Fatalf("symlink mode=%#o, want type symlink and permissions %#o", got, wantMode)
	}
	if !worker.State().Conditions.ReadyForRollout {
		t.Fatalf("symlink mode repair did not converge: %+v", worker.State())
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

func TestRoundProactivelyReloadsChangedCredential(t *testing.T) {
	for _, revokedOldKey := range []bool{false, true} {
		t.Run(fmt.Sprintf("revoked=%v", revokedOldKey), func(t *testing.T) {
			root := t.TempDir()
			backend := &workerServer{
				target: &memoryTarget{nodes: make(map[string]memoryTargetNode)}, checkpoint: &checkpointFake{},
				caps: allWorkerCapabilities(), rejectAuth: make(map[string]bool),
			}
			server := httptest.NewServer(http.HandlerFunc(backend.handler))
			defer server.Close()
			startup := newWorkerStartup(t, root, server)
			worker, err := NewWorker(context.Background(), startup)
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.DeepRecovery(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(startup.Credential.path, []byte("rotated-key\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			backend.rejectAuth["Bearer first-key"] = revokedOldKey
			backend.mu.Unlock()

			if err := worker.Round(context.Background(), RoundModeFull); err != nil {
				t.Fatalf("round after credential rotation: %v", err)
			}
			backend.mu.Lock()
			lastAuth := backend.auth[len(backend.auth)-1]
			backend.mu.Unlock()
			if lastAuth != "Bearer rotated-key" {
				t.Fatalf("round retained old credential: %q", lastAuth)
			}
		})
	}
}

func TestSourceRootReplacementFailsClosedInEveryWritableMode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase Phase
		mode  RoundMode
	}{
		{name: "syncing", phase: PhaseSyncing, mode: RoundModeFull},
		{name: "dual-write", phase: PhaseDualWriteRepairing, mode: RoundModeFast},
		{name: "verification", phase: PhaseDualWriteRepairing, mode: RoundModeVerification},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "source")
			if err := os.Mkdir(root, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
				t.Fatal(err)
			}
			target := &memoryTarget{nodes: map[string]memoryTargetNode{
				"file": {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "file"},
			}}
			worker, server := newRoundWorker(t, root, target)
			defer server.Close()
			worker.state = NewState(tc.phase)
			worker.apply.config.Phase = tc.phase
			if err := worker.DeepRecovery(context.Background()); err != nil {
				t.Fatal(err)
			}
			replaceSourceRootWithEmptyDirectory(t, root)

			var err error
			if tc.mode == RoundModeVerification {
				_, err = worker.VerifyFull(context.Background())
			} else {
				err = worker.Round(context.Background(), tc.mode)
			}
			if err == nil {
				t.Fatal("replacement source root was accepted")
			}
			target.mu.Lock()
			_, retained := target.nodes["file"]
			target.mu.Unlock()
			if !retained {
				t.Fatal("source replacement deleted the Drive9 copy")
			}
			if !worker.State().Conditions.Attention {
				t.Fatal("source identity failure did not raise Attention")
			}
		})
	}
}

func TestRoundRejectsAdvancedOrRevisedRemoteCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Checkpoint)
	}{
		{name: "higher phase", mutate: func(checkpoint *Checkpoint) { checkpoint.HighestPhase = PhaseDualWriteRepairing }},
		{name: "fence intent", mutate: func(checkpoint *Checkpoint) {
			checkpoint.HighestPhase = PhaseDualWriteRepairing
			checkpoint.FenceIntent = true
		}},
		{name: "stale revision", mutate: func(*Checkpoint) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
				t.Fatal(err)
			}
			target := &memoryTarget{nodes: map[string]memoryTargetNode{
				"file":  {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "file"},
				"extra": {data: []byte("keep"), revision: 1, mode: 0o100644, resourceID: "extra"},
			}}
			checkpoint := &checkpointFake{}
			backend := &workerServer{target: target, checkpoint: checkpoint, caps: allWorkerCapabilities()}
			server := httptest.NewServer(http.HandlerFunc(backend.handler))
			defer server.Close()
			worker, err := NewWorker(context.Background(), newWorkerStartup(t, root, server))
			if err != nil {
				t.Fatal(err)
			}
			checkpoint.mu.Lock()
			var remote Checkpoint
			if err := json.Unmarshal(checkpoint.body, &remote); err != nil {
				checkpoint.mu.Unlock()
				t.Fatal(err)
			}
			tc.mutate(&remote)
			checkpoint.body, err = json.Marshal(remote)
			checkpoint.revision++
			checkpoint.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}

			if err := worker.Round(context.Background(), RoundModeFull); err == nil {
				t.Fatal("stale Worker accepted changed Checkpoint")
			}
			target.mu.Lock()
			_, retained := target.nodes["extra"]
			target.mu.Unlock()
			if !retained {
				t.Fatal("stale Worker deleted target data")
			}
			if !worker.State().Conditions.Attention {
				t.Fatal("Checkpoint conflict did not raise Attention")
			}
		})
	}
}

func TestRoundRevalidatesSourceAndCheckpointImmediatelyBeforeApply(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, string, *checkpointFake)
		want   error
	}{
		{
			name: "source mount",
			change: func(t *testing.T, root string, _ *checkpointFake) {
				replaceSourceRootWithEmptyDirectory(t, root)
			},
			want: ErrSourceMountChanged,
		},
		{
			name: "remote checkpoint",
			change: func(t *testing.T, _ string, checkpoint *checkpointFake) {
				checkpoint.mu.Lock()
				defer checkpoint.mu.Unlock()
				var remote Checkpoint
				if err := json.Unmarshal(checkpoint.body, &remote); err != nil {
					t.Fatal(err)
				}
				remote.HighestPhase = PhaseDualWriteRepairing
				body, err := json.Marshal(remote)
				if err != nil {
					t.Fatal(err)
				}
				checkpoint.body = body
				checkpoint.revision++
			},
			want: ErrCheckpointMismatch,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
				t.Fatal(err)
			}
			target := &memoryTarget{nodes: map[string]memoryTargetNode{
				"file":  {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "file"},
				"extra": {data: []byte("keep"), revision: 1, mode: 0o100644, resourceID: "extra"},
			}}
			checkpoint := &checkpointFake{}
			backend := &workerServer{target: target, checkpoint: checkpoint, caps: allWorkerCapabilities()}
			server := httptest.NewServer(http.HandlerFunc(backend.handler))
			defer server.Close()
			worker, err := NewWorker(context.Background(), newWorkerStartup(t, root, server))
			if err != nil {
				t.Fatal(err)
			}
			target.afterList = func() { tc.change(t, root, checkpoint) }

			err = worker.Round(context.Background(), RoundModeFull)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Round error=%v, want %v", err, tc.want)
			}
			target.mu.Lock()
			_, retained := target.nodes["extra"]
			writes := target.writes
			target.mu.Unlock()
			if !retained || writes != 0 {
				t.Fatalf("pre-Apply identity change retained=%v writes=%d", retained, writes)
			}
			if !worker.State().Conditions.Attention {
				t.Fatal("pre-Apply identity change did not raise Attention")
			}
		})
	}
}

func TestRoundRejectsSourceMutationDuringTargetInventory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, string)
	}{
		{
			name: "existing file changes",
			change: func(t *testing.T, root string) {
				if err := os.WriteFile(filepath.Join(root, "a"), []byte("changed-content"), 0o644); err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "new path appears",
			change: func(t *testing.T, root string) {
				if err := os.WriteFile(filepath.Join(root, "new"), []byte("new-content"), 0o644); err != nil {
					t.Error(err)
					return
				}
				future := time.Now().Add(2 * time.Second)
				if err := os.Chtimes(root, future, future); err != nil {
					t.Error(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0o644); err != nil {
				t.Fatal(err)
			}
			target := &memoryTarget{nodes: map[string]memoryTargetNode{
				"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "a"},
			}}
			worker, server := newRoundWorker(t, root, target)
			defer server.Close()
			target.afterList = func() { tc.change(t, root) }

			err := worker.Round(context.Background(), RoundModeFull)
			if !errors.Is(err, ErrSourceChanged) {
				t.Fatalf("Round error=%v, want ErrSourceChanged", err)
			}
			if current := worker.State().Current; current.ScanComplete || current.Converged {
				t.Fatalf("source mutation published round: %+v", current)
			}
		})
	}
}

func TestDualFastRoundRejectsSourceMutationDuringTargetReads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, string)
	}{
		{
			name: "existing file changes",
			change: func(t *testing.T, root string) {
				if err := os.WriteFile(filepath.Join(root, "a"), []byte("changed-content"), 0o644); err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "new path appears",
			change: func(t *testing.T, root string) {
				if err := os.WriteFile(filepath.Join(root, "new"), []byte("new-content"), 0o644); err != nil {
					t.Error(err)
					return
				}
				future := time.Now().Add(2 * time.Second)
				if err := os.Chtimes(root, future, future); err != nil {
					t.Error(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0o644); err != nil {
				t.Fatal(err)
			}
			target := &memoryTarget{nodes: map[string]memoryTargetNode{
				"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "a"},
			}}
			now := time.Now()
			worker, server := newDualWorker(t, root, target, time.Minute, &now)
			defer server.Close()
			if err := worker.DeepRecovery(context.Background()); err != nil {
				t.Fatal(err)
			}
			target.afterHead = func() { tc.change(t, root) }

			err := worker.Round(context.Background(), RoundModeFast)
			if !errors.Is(err, ErrSourceChanged) {
				t.Fatalf("Fast Round error=%v, want ErrSourceChanged", err)
			}
			if current := worker.State().Current; current.ScanComplete || current.Converged {
				t.Fatalf("source mutation published Fast Round: %+v", current)
			}
		})
	}
}

func TestCredentialRefreshValidatesCheckpointBeforeAtomicSwap(t *testing.T) {
	root := t.TempDir()
	activeCheckpoint := &checkpointFake{}
	backend := &workerServer{
		target: &memoryTarget{nodes: make(map[string]memoryTargetNode)}, checkpoint: activeCheckpoint,
		checkpointByAuth: map[string]*checkpointFake{"Bearer wrong-tenant-key": nil}, caps: allWorkerCapabilities(),
	}
	server := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer server.Close()
	startup := newWorkerStartup(t, root, server)
	worker, err := NewWorker(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	oldAPI, oldInventory, oldCheckpoint, oldApply := worker.api, worker.inventory, worker.checkpoint, worker.apply
	if err := os.WriteFile(startup.Credential.path, []byte("wrong-tenant-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := worker.refreshClient(context.Background()); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("wrong-tenant refresh error=%v", err)
	}
	if worker.api != oldAPI || worker.inventory != oldInventory || worker.checkpoint != oldCheckpoint || worker.apply != oldApply {
		t.Fatal("failed credential refresh partially replaced the active runtime")
	}

	activeCheckpoint.mu.Lock()
	other := checkpointFromStartup(startup)
	other.SpaceRef = "other-space"
	body, marshalErr := json.Marshal(other)
	activeCheckpoint.mu.Unlock()
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	wrongSpace := &checkpointFake{revision: 1, body: body}
	backend.mu.Lock()
	backend.checkpointByAuth["Bearer wrong-space-key"] = wrongSpace
	backend.mu.Unlock()
	if err := os.WriteFile(startup.Credential.path, []byte("wrong-space-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := worker.refreshClient(context.Background()); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("wrong-space refresh error=%v", err)
	}
	if worker.api != oldAPI || worker.inventory != oldInventory || worker.checkpoint != oldCheckpoint || worker.apply != oldApply {
		t.Fatal("identity mismatch partially replaced the active runtime")
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
		ErrTargetChanged,
		ErrApplyRescan,
		ErrCheckpointConflict,
		os.ErrNotExist,
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

func TestWorkerRunRetriesTargetChangeInsteadOfExiting(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "id"}}}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	previousRound := worker.State().Current.ID
	target.mu.Lock()
	target.changeAfterList = true
	target.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	waitFor(t, func() bool {
		current := worker.State().Current
		return current.ID != previousRound && current.ScanComplete
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Worker exited on target churn: %v", err)
	}
}

func TestWorkerRunRetriesChmodNotFoundInsteadOfExiting(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{
		nodes:           make(map[string]memoryTargetNode),
		failChmodCount:  1,
		failChmodStatus: http.StatusNotFound,
	}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	waitFor(t, func() bool {
		select {
		case err := <-done:
			t.Fatalf("Worker exited on chmod 404: %v", err)
		default:
		}
		snapshot := worker.State()
		return snapshot.RecoveryComplete && snapshot.Conditions.ReadyForRollout
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("canceled Worker error=%v", err)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.writes != 1 {
		t.Fatalf("committed file was uploaded %d times, want once", target.writes)
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

func TestDualGraceStartsWhenStableMismatchIsObserved(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "a")
	if err := os.WriteFile(filePath, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("one"), revision: 1, mode: 0o100644, resourceID: "id"}}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	roundStarted := now
	advanced := false
	worker.scanner.beforeEntry = func(string) {
		if !advanced {
			advanced = true
			now = now.Add(2 * time.Minute)
		}
	}
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	candidate := onlyGrace(t, worker.State())
	if !candidate.FirstSeen.Equal(now) || candidate.FirstSeen.Equal(roundStarted) {
		t.Fatalf("first_seen=%s round_started=%s observation=%s", candidate.FirstSeen, roundStarted, now)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.writes != 0 {
		t.Fatalf("long scan consumed grace and repaired immediately: writes=%d", target.writes)
	}
}

func TestDualCandidateCountsDescribeSelectionReasons(t *testing.T) {
	floor := time.Unix(0, 100)
	stable := SourceVersion{Device: 1, Inode: 1, Kind: EntryRegular, Size: 1, MtimeNS: 1, CtimeNS: 1, Mode: 0o644}
	changed := stable
	changed.CtimeNS++
	source := ScanResult{Entries: map[string]SourceEntry{
		"/mtime":    {Path: "/mtime", Version: SourceVersion{Device: 1, Inode: 2, Kind: EntryRegular, Size: 1, MtimeNS: floor.UnixNano(), CtimeNS: 1, Mode: 0o644}},
		"/token":    {Path: "/token", Version: changed},
		"/new":      {Path: "/new", Version: stable},
		"/grace":    {Path: "/grace", Version: stable},
		"/retry":    {Path: "/retry", Version: stable},
		"/filtered": {Path: "/filtered", Version: stable},
	}}
	snapshot := StateSnapshot{
		RepairMtimeFloor: &floor,
		Reconciled: map[string]SourceVersion{
			"/mtime": source.Entries["/mtime"].Version,
			"/token": stable, "/grace": stable, "/retry": stable, "/filtered": stable,
		},
		Grace: map[string]GraceCandidate{"grace": {Path: "/grace"}},
		Retry: map[string]RetryItem{"retry": {Path: "/retry"}},
	}
	candidates, counts := dualCandidates(source, snapshot)
	if counts.Mtime != 1 || counts.SourceTokenChanged != 1 || counts.NewPath != 1 || counts.Filtered != 1 {
		t.Fatalf("candidate counts=%+v", counts)
	}
	for _, path := range []string{"/mtime", "/token", "/new", "/grace", "/retry"} {
		if _, exists := candidates[path]; !exists {
			t.Fatalf("candidate %s missing: %v", path, candidates)
		}
	}
	if _, exists := candidates["/filtered"]; exists {
		t.Fatalf("filtered path selected: %v", candidates)
	}
}

func TestDualRepairMissingHardlinkAliasUsesMatchingPrimary(t *testing.T) {
	root := t.TempDir()
	primaryPath := filepath.Join(root, "a")
	if err := os.WriteFile(primaryPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(primaryPath, filepath.Join(root, "b")); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"a": {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "hardlink-resource"},
	}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if candidate := onlyGrace(t, worker.State()); candidate.Path != "/b" {
		t.Fatalf("grace candidate=%+v", candidate)
	}
	now = now.Add(time.Minute)
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	alias, exists := target.nodes["b"]
	target.mu.Unlock()
	if !exists || alias.resourceID != "hardlink-resource" {
		t.Fatalf("hardlink alias=%+v exists=%v", alias, exists)
	}
	if !worker.State().Conditions.CurrentConverged {
		t.Fatalf("hardlink repair did not converge: %+v", worker.State())
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

func TestDualPermanentApplyFailureStopsRound(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{
		nodes:         map[string]memoryTargetNode{"a": {data: []byte("old"), revision: 1, mode: 0o100644, resourceID: "target-a"}},
		failPut:       true,
		failPutStatus: http.StatusBadRequest,
	}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := worker.Run(ctx)
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("permanent Apply error=%T %v", err, err)
	}
	if !worker.State().Conditions.Attention {
		t.Fatal("permanent Apply failure did not set Attention")
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
