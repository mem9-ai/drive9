package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type fakeLargeManifest struct {
	files *fakeFilePipelineAPI
	err   error
}

func (f fakeLargeManifest) ManifestPageCtx(context.Context, string, string, int) (client.ManifestPage, error) {
	if f.err != nil {
		return client.ManifestPage{}, f.err
	}
	f.files.mu.Lock()
	defer f.files.mu.Unlock()
	paths := make([]string, 0, len(f.files.nodes))
	for path := range f.files.nodes {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	entries := make([]client.ManifestEntry, 0, len(paths))
	for _, path := range paths {
		node := f.files.nodes[path]
		mode, revision, checksum := node.mode, node.revision, checksumBytes(node.data)
		entryType := client.ManifestEntryRegular
		if node.kind == EntrySymlink {
			entryType = client.ManifestEntrySymlink
		}
		entries = append(entries, client.ManifestEntry{
			Path: path, Type: entryType, MetadataComplete: true, IdentityKind: client.ManifestIdentityInode,
			Mode: &mode, Size: int64(len(node.data)), ChecksumSHA256: &checksum, Revision: &revision,
			ResourceID: node.resourceID, Nlink: node.nlink,
		})
	}
	return client.ManifestPage{Entries: entries, Done: true}, nil
}

func TestWorkerLargeScaleSyncingUsesGenerationPipelineAndFreshGate(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	worker, files := newLargeRoundWorker(t, root)
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if snapshot.LastGeneration == nil || snapshot.LastComplete != nil || !snapshot.Current.Converged || !snapshot.Conditions.ReadyForRollout {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	files.mu.Lock()
	_, copied := files.nodes["/file"]
	writes := len(files.batchWriteCalls) + len(files.multipartCalls)
	files.mu.Unlock()
	if !copied || writes != 1 {
		t.Fatalf("copied=%t writes=%d", copied, writes)
	}
	if snapshot.LastGeneration.SourceGenerationID == "" || snapshot.LastGeneration.TargetGenerationID == "" || snapshot.LastGeneration.DiffGenerationID == "" {
		t.Fatalf("generation IDs = %+v", snapshot.LastGeneration)
	}
}

func TestWorkerLargeScaleIncompleteManifestCannotPublishCondition(t *testing.T) {
	root := t.TempDir()
	worker, files := newLargeRoundWorker(t, root)
	worker.manifestAPI = fakeLargeManifest{files: files, err: context.DeadlineExceeded}
	if err := worker.Round(context.Background(), RoundModeFull); err == nil {
		t.Fatal("incomplete Manifest round succeeded")
	}
	snapshot := worker.State()
	if snapshot.Current.ScanComplete || snapshot.Conditions.ReadyForRollout || snapshot.Conditions.CurrentConverged || snapshot.LastGeneration != nil {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestWorkerLargeScaleStatusUsesBoundedGenerationSummary(t *testing.T) {
	root := t.TempDir()
	worker, _ := newLargeRoundWorker(t, root)
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := worker.statusOutput()
	if !status.LargeScale || status.Generation == nil || status.SourceCount != 0 || status.Generation.DiffGenerationID == "" ||
		!status.Generation.SourceComplete || !status.Generation.TargetComplete || !status.Generation.DiffComplete ||
		status.Generation.RebuildReason != "no_complete_source_generation" || status.Generation.MemoryUsedBytes != 0 ||
		status.Generation.SourceQueueCapacity == 0 || status.Generation.MemoryPeakBytes == 0 || status.Generation.MemoryLimitBytes != 3<<30 {
		t.Fatalf("status = %+v", status)
	}
	if len(status.Findings) != 0 || !slices.Equal(status.Generation.Stages, []string{"source", "target", "diff"}) {
		t.Fatalf("generation status = %+v", status.Generation)
	}
}

func TestWorkerLargeScaleDiffStreamsCompleteGeneration(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	worker, _ := newLargeRoundWorker(t, root)
	worker.state.BeginRound("diagnostic", RoundModeFull, testNow)
	observation, err := worker.buildLargeObservation(context.Background(), RoundModeFull, "diagnostic", "control", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.state.PublishGeneration(observation.diff.Summary); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := worker.handleControl(context.Background(), &output, ControlRequest{Command: "diff", Output: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	var finding Finding
	if err := json.NewDecoder(&output).Decode(&finding); err != nil {
		t.Fatalf("decode streamed finding: %v; output=%q", err, output.String())
	}
	if finding.Path != "/file" || finding.Kind != FindingSourceOnly || finding.Severity != SeverityBlocker {
		t.Fatalf("finding = %+v", finding)
	}
}

func TestWorkerLargeScaleRecentErrorsAreBoundedAndClassified(t *testing.T) {
	worker, _ := newLargeRoundWorker(t, t.TempDir())
	worker.setLargeProgress(GenerationStatus{Stage: "target"})
	for range 10 {
		worker.recordLargeError(ErrApplyRescan)
	}
	status := worker.statusOutput()
	if status.Generation == nil || len(status.Generation.RecentErrors) != maxGenerationRecentErrors {
		t.Fatalf("generation status=%+v", status.Generation)
	}
	for _, recent := range status.Generation.RecentErrors {
		if recent.Stage != "target" || recent.Class != "apply_rescan" || recent.At.IsZero() {
			t.Fatalf("recent error=%+v", recent)
		}
	}
}

func newLargeRoundWorker(t *testing.T, root string) (*Worker, *fakeFilePipelineAPI) {
	t.Helper()
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	files := newFakeFilePipelineAPI()
	batch := newFakeBatchApplyAPI()
	batchEngine, err := newBatchApplyEngine(batch, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	fileEngine := testFilePipeline(t, files, scanner, 1024)
	budget, err := newMemoryBudget(3 << 30)
	if err != nil {
		t.Fatal(err)
	}
	config := &Config{
		Version: ConfigVersion, Drive9: Drive9Config{Endpoint: "https://drive9.example.com"},
		JobDefaults: JobDefaults{
			Sync:        SyncDefaults{GracePeriod: Duration(time.Minute)},
			Performance: PerformanceDefaults{MaxBytesPerSecond: 1 << 30, SmallFileWorkers: 2, LargeFileWorkers: 1},
		},
		Spaces: map[string]SpaceConfig{"space-a": {CredentialRef: "owner-key"}},
	}
	job := Job{
		JobID: "job-a", VolumeID: "vol-a", NodeName: "node-a", EBSRoot: root, Subpath: "/",
		Source: SourceConfig{Type: "ebs", Root: root}, Target: TargetConfig{SpaceRef: "space-a", Prefix: "/"},
	}
	startup := &Startup{
		Config: config, Job: job, Space: config.Spaces["space-a"], Phase: PhaseSyncing,
		LargeScale: true, ConfigHash: "config-a", mountProbe: testMountedSourceProbe,
	}
	identity, err := observeJobSource(startup, testMountedSourceProbe)
	if err != nil {
		t.Fatal(err)
	}
	startup.acceptedSource = identity
	worker := &Worker{
		startup: startup, scanner: scanner, state: NewState(PhaseSyncing), sourceIdentity: identity,
		generation: store, manifestAPI: fakeLargeManifest{files: files}, batchApply: batchEngine, fileApply: fileEngine,
		gracePeriod: time.Minute, generationNonce: "test", memoryBudget: budget, now: func() time.Time { return testNow },
	}
	return worker, files
}
