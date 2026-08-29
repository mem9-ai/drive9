package migration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type syntheticGenerationReader struct {
	remaining    int
	total        int
	kind         generationRecordKind
	changeEvery  int
	modeEvery    int
	hardlinkSize int
}

type syntheticManifestClient struct {
	total   int
	emitted int
	page    int
}

func (c *syntheticManifestClient) ManifestPageCtx(_ context.Context, _, cursor string, limit int) (client.ManifestPage, error) {
	expected := ""
	if c.page > 0 {
		expected = fmt.Sprintf("cursor-%09d", c.emitted)
	}
	if cursor != expected {
		return client.ManifestPage{}, fmt.Errorf("synthetic cursor = %q, want %q", cursor, expected)
	}
	count := min(limit, c.total-c.emitted)
	entries := make([]client.ManifestEntry, count)
	checksum := strings.Repeat("a", 64)
	for offset := range entries {
		index := c.total - c.emitted - offset
		mode, revision := uint32(0o644), int64(1)
		entries[offset] = client.ManifestEntry{
			Path: fmt.Sprintf("/file-%09d", index), Type: client.ManifestEntryRegular,
			MetadataComplete: true, IdentityKind: client.ManifestIdentityInode, Mode: &mode,
			Size: 12941, ChecksumSHA256: &checksum, Revision: &revision,
			ResourceID: fmt.Sprintf("resource-%09d", index), Nlink: 1,
		}
		if index%7 == 0 {
			mode = 0o755
			entries[offset].Path = fmt.Sprintf("/dir-%09d/", index)
			entries[offset].Type = client.ManifestEntryDirectory
			entries[offset].Mode = &mode
			entries[offset].Size = 0
			entries[offset].ChecksumSHA256 = nil
			entries[offset].Nlink = 2
		}
	}
	c.emitted += count
	c.page++
	done := c.emitted == c.total
	next := ""
	if !done {
		next = fmt.Sprintf("cursor-%09d", c.emitted)
	}
	return client.ManifestPage{
		Entries: entries, NextCursor: next, Done: done, ResponseBytes: int64(count) * 256,
	}, nil
}

func (r *syntheticGenerationReader) Next() (generationRecord, bool, error) {
	if r.remaining == 0 {
		return generationRecord{}, false, nil
	}
	if r.total == 0 {
		r.total = r.remaining
	}
	path := fmt.Sprintf("/file-%09d", r.remaining)
	index := r.remaining
	r.remaining--
	checksum := strings.Repeat("a", 64)
	if r.changeEvery > 0 && index%r.changeEvery == 0 && r.kind == recordTarget {
		checksum = strings.Repeat("b", 64)
	}
	if r.kind == recordTarget {
		mode, revision := uint32(0o644), int64(1)
		if r.modeEvery > 0 && index%r.modeEvery == 0 {
			mode = 0o600
		}
		resourceID, nlink := "resource-"+path, uint32(1)
		if r.hardlinkSize > 0 {
			group := (index - 1) / r.hardlinkSize
			resourceID = fmt.Sprintf("hardlink-%09d", group)
			groupStart := group * r.hardlinkSize
			nlink = uint32(min(r.hardlinkSize, r.total-groupStart))
		}
		return generationRecord{Key: path, Target: &targetGenerationRecord{
			Path: path, Kind: EntryRegular, Size: 12941, Mode: &mode, MetadataComplete: true,
			IdentityKind: "inode", Revision: &revision, ResourceID: resourceID, Nlink: nlink,
			ChecksumSHA256: &checksum,
		}}, true, nil
	}
	inode, hardlink := uint64(r.remaining+1), ""
	if r.hardlinkSize > 0 {
		group := (index - 1) / r.hardlinkSize
		inode = uint64(group + 1)
		hardlink = fmt.Sprintf("1:%d", inode)
	}
	return generationRecord{Key: path, Source: &sourceGenerationRecord{
		Path: path, LocalPath: path, Kind: EntryRegular, Device: 1, Inode: inode,
		Size: 12941, MtimeNS: 1, CtimeNS: 1, VersionMode: 0o100644, Mode: 0o644,
		ChecksumSHA256: checksum, HardlinkKey: hardlink,
	}}, true, nil
}

type diskGenerationObjects struct {
	root      string
	mu        sync.Mutex
	revisions map[string]int64
}

func newDiskGenerationObjects(root string) *diskGenerationObjects {
	return &diskGenerationObjects{root: root, revisions: make(map[string]int64)}
}

func (s *diskGenerationObjects) local(path string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(path, "/")))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe benchmark object path")
	}
	return filepath.Join(s.root, clean), nil
}

func (s *diskGenerationObjects) EnsureDirectory(_ context.Context, path string) error {
	local, err := s.local(path)
	if err != nil {
		return err
	}
	return os.MkdirAll(local, 0o700)
}

func (s *diskGenerationObjects) Put(_ context.Context, path string, body []byte, expectedRevision int64) (int64, error) {
	local, err := s.local(path)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(local), 0o700); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.revisions[path]
	if expectedRevision == 0 && current != 0 || expectedRevision > 0 && current != expectedRevision {
		return 0, fmt.Errorf("revision conflict")
	}
	if err := os.WriteFile(local, body, 0o600); err != nil {
		return 0, err
	}
	current++
	s.revisions[path] = current
	return current, nil
}

func (s *diskGenerationObjects) Get(_ context.Context, path string, maxBytes int64) ([]byte, int64, error) {
	local, err := s.local(path)
	if err != nil {
		return nil, 0, err
	}
	body, err := os.ReadFile(local)
	if err != nil {
		return nil, 0, err
	}
	if int64(len(body)) > maxBytes {
		return nil, 0, fmt.Errorf("benchmark object too large")
	}
	s.mu.Lock()
	revision := s.revisions[path]
	s.mu.Unlock()
	return body, revision, nil
}

func (s *diskGenerationObjects) List(_ context.Context, path string) ([]generationObjectInfo, error) {
	local, err := s.local(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(local)
	if err != nil {
		return nil, err
	}
	result := make([]generationObjectInfo, len(entries))
	for index, entry := range entries {
		result[index] = generationObjectInfo{Name: entry.Name(), Directory: entry.IsDir()}
	}
	return result, nil
}

func (s *diskGenerationObjects) DeleteFile(_ context.Context, path string) error {
	local, err := s.local(path)
	if err != nil {
		return err
	}
	return os.Remove(local)
}

func (s *diskGenerationObjects) DeleteDirectory(_ context.Context, path string) error {
	local, err := s.local(path)
	if err != nil {
		return err
	}
	return os.Remove(local)
}

func BenchmarkMigrationExternalSort(b *testing.B) {
	entries := benchmarkEnvInt(b, "DRIVE9_MIGRATION_SCALE_ENTRIES", 100000)
	if os.Getenv("DRIVE9_MIGRATION_SCALE") == "1" && os.Getenv("DRIVE9_MIGRATION_SCALE_ENTRIES") == "" {
		entries = 6000000
	}
	b.ReportMetric(float64(entries), "entries")
	for b.Loop() {
		started := time.Now()
		sampler := startBenchmarkMemorySampler()
		objects := newDiskGenerationObjects(b.TempDir())
		store, err := newGenerationStore(objects, "benchmark-job")
		if err != nil {
			b.Fatal(err)
		}
		sorter, err := newExternalSorter(store, externalSortConfig{
			GenerationID: "benchmark-generation", Stage: stageSource, Kind: recordSource, IDPrefix: "benchmark-sort",
			MaxBufferBytes: largeSortBufferBytes, FanIn: largeSortFanIn,
		})
		if err != nil {
			b.Fatal(err)
		}
		result, err := sorter.Sort(context.Background(), &syntheticGenerationReader{remaining: entries, kind: recordSource})
		if err != nil {
			b.Fatal(err)
		}
		if result.Stats.InputRecords != int64(entries) {
			b.Fatalf("records = %d, want %d", result.Stats.InputRecords, entries)
		}
		b.ReportMetric(float64(result.Stats.PeakBufferBytes), "peak_buffer_bytes")
		b.ReportMetric(float64(result.Stats.InitialRuns), "initial_runs")
		b.ReportMetric(float64(result.Stats.MergePasses), "merge_passes")
		b.ReportMetric(float64(result.Stats.OutputChunks), "output_chunks")
		heapAlloc, heapSys, processRSS := sampler.Stop()
		b.ReportMetric(float64(heapAlloc), "heap_peak_alloc_bytes")
		b.ReportMetric(float64(heapSys), "heap_peak_sys_bytes")
		if processRSS > 0 {
			b.ReportMetric(float64(processRSS), "process_peak_rss_bytes")
		}
		assertScaleBenchmark(b, time.Since(started), processRSS, 30*time.Minute)
	}
}

func BenchmarkMigrationTargetManifest(b *testing.B) {
	entries := benchmarkEnvInt(b, "DRIVE9_MIGRATION_SCALE_ENTRIES", 100000)
	identity := generationIdentity{
		JobID: "benchmark-job", ConfigHash: "benchmark-config", VolumeID: "vol-a",
		EBSRoot: "/benchmark", SourceSubpath: "/", SourceRoot: "/benchmark",
		Endpoint: "https://drive9.example.com", SpaceRef: "benchmark-space", Prefix: "/",
	}
	b.ReportMetric(float64(entries), "entries")
	for b.Loop() {
		started := time.Now()
		sampler := startBenchmarkMemorySampler()
		objects := newDiskGenerationObjects(b.TempDir())
		store, err := newGenerationStore(objects, identity.JobID)
		if err != nil {
			b.Fatal(err)
		}
		budget, err := newMemoryBudget(3 << 30)
		if err != nil {
			b.Fatal(err)
		}
		builder, err := newManifestBuilder(&syntheticManifestClient{total: entries}, store, manifestConfig{
			GenerationID: "benchmark-target", RoundID: "benchmark-round", Phase: PhaseSyncing,
			Identity: identity, TargetPrefix: "/", PageLimit: largeManifestLimit,
			SortBufferBytes: largeSortBufferBytes, SortFanIn: largeSortFanIn, Budget: budget,
		})
		if err != nil {
			b.Fatal(err)
		}
		result, err := builder.Build(context.Background(), nil)
		if err != nil {
			b.Fatal(err)
		}
		if result.Metadata.EntryCount != int64(entries) {
			b.Fatalf("target records = %d, want %d", result.Metadata.EntryCount, entries)
		}
		b.ReportMetric(float64(result.Metadata.ManifestPages), "manifest_pages")
		b.ReportMetric(float64(result.Metadata.ManifestResponseBytes), "manifest_response_bytes")
		b.ReportMetric(float64(generationArtifactBytes(result.Metadata)), "artifact_bytes")
		_, peak, _ := budget.Snapshot()
		b.ReportMetric(float64(peak), "budget_peak_bytes")
		heapAlloc, heapSys, processRSS := sampler.Stop()
		b.ReportMetric(float64(heapAlloc), "heap_peak_alloc_bytes")
		b.ReportMetric(float64(heapSys), "heap_peak_sys_bytes")
		if processRSS > 0 {
			b.ReportMetric(float64(processRSS), "process_peak_rss_bytes")
		}
		assertScaleBenchmark(b, time.Since(started), processRSS, 30*time.Minute)
	}
}

func BenchmarkMigrationGenerationDiff(b *testing.B) {
	entries := benchmarkEnvInt(b, "DRIVE9_MIGRATION_SCALE_ENTRIES", 100000)
	if os.Getenv("DRIVE9_MIGRATION_SCALE") == "1" && os.Getenv("DRIVE9_MIGRATION_SCALE_ENTRIES") == "" {
		entries = 6000000
	}
	identity := generationIdentity{
		JobID: "benchmark-job", ConfigHash: "benchmark-config", VolumeID: "vol-a",
		EBSRoot: "/benchmark", SourceSubpath: "/", SourceRoot: "/benchmark",
		Endpoint: "https://drive9.example.com", SpaceRef: "benchmark-space", Prefix: "/",
	}
	b.ReportMetric(float64(entries), "entries")
	for b.Loop() {
		started := time.Now()
		sampler := startBenchmarkMemorySampler()
		ctx := context.Background()
		objects := newDiskGenerationObjects(b.TempDir())
		store, err := newGenerationStore(objects, identity.JobID)
		if err != nil {
			b.Fatal(err)
		}
		budget, err := newMemoryBudget(3 << 30)
		if err != nil {
			b.Fatal(err)
		}
		source := writeBenchmarkGeneration(b, ctx, store, budget, identity, "benchmark-source", stageSource, recordSource, entries)
		target := writeBenchmarkGeneration(b, ctx, store, budget, identity, "benchmark-target", stageTarget, recordTarget, entries)
		diff, err := newStreamDiffBuilder(store, streamDiffConfig{
			GenerationID: "benchmark-diff", RoundID: "benchmark-round", Mode: RoundModeFull,
			Phase: PhaseSyncing, Identity: identity, SortBufferBytes: largeSortBufferBytes,
			SortFanIn: largeSortFanIn, Budget: budget,
		})
		if err != nil {
			b.Fatal(err)
		}
		result, err := diff.Build(ctx, source.GenerationID, target.GenerationID)
		if err != nil {
			b.Fatal(err)
		}
		if !result.Summary.Converged || result.Summary.BlockerCount != 0 {
			b.Fatalf("diff summary = %+v", result.Summary)
		}
		used, peak, _ := budget.Snapshot()
		if used != 0 {
			b.Fatalf("memory budget leaked %d bytes", used)
		}
		b.ReportMetric(float64(peak), "budget_peak_bytes")
		b.ReportMetric(float64(generationArtifactBytes(source)+generationArtifactBytes(target)+generationArtifactBytes(result.Metadata)), "artifact_bytes")
		heapAlloc, heapSys, processRSS := sampler.Stop()
		b.ReportMetric(float64(heapAlloc), "heap_peak_alloc_bytes")
		b.ReportMetric(float64(heapSys), "heap_peak_sys_bytes")
		if processRSS > 0 {
			b.ReportMetric(float64(processRSS), "process_peak_rss_bytes")
		}
		assertScaleBenchmark(b, time.Since(started), processRSS, 5*time.Hour)
	}
}

func BenchmarkMigrationDiffScenarios(b *testing.B) {
	entries := benchmarkEnvInt(b, "DRIVE9_MIGRATION_SCENARIO_ENTRIES", 10000)
	identity := generationIdentity{
		JobID: "benchmark-job", ConfigHash: "benchmark-config", VolumeID: "vol-a",
		EBSRoot: "/benchmark", SourceSubpath: "/", SourceRoot: "/benchmark",
		Endpoint: "https://drive9.example.com", SpaceRef: "benchmark-space", Prefix: "/",
	}
	for _, scenario := range []struct {
		name         string
		changeEvery  int
		modeEvery    int
		hardlinkSize int
		converged    bool
	}{
		{name: "low_change", changeEvery: 1000},
		{name: "medium_change", changeEvery: 10},
		{name: "checksum_cache_miss", changeEvery: 1},
		{name: "high_non_default_mode", modeEvery: 2},
		{name: "hardlink_heavy", hardlinkSize: 8, converged: true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportMetric(float64(entries), "entries")
			for b.Loop() {
				ctx := context.Background()
				store, err := newGenerationStore(newDiskGenerationObjects(b.TempDir()), identity.JobID)
				if err != nil {
					b.Fatal(err)
				}
				budget, err := newMemoryBudget(3 << 30)
				if err != nil {
					b.Fatal(err)
				}
				source := writeBenchmarkGenerationFromReader(b, ctx, store, budget, identity, "scenario-source", stageSource, recordSource,
					&syntheticGenerationReader{remaining: entries, kind: recordSource, hardlinkSize: scenario.hardlinkSize}, entries)
				target := writeBenchmarkGenerationFromReader(b, ctx, store, budget, identity, "scenario-target", stageTarget, recordTarget,
					&syntheticGenerationReader{remaining: entries, kind: recordTarget, changeEvery: scenario.changeEvery, modeEvery: scenario.modeEvery, hardlinkSize: scenario.hardlinkSize}, entries)
				diff, err := newStreamDiffBuilder(store, streamDiffConfig{
					GenerationID: "scenario-diff", RoundID: "scenario-round", Mode: RoundModeFull,
					Phase: PhaseSyncing, Identity: identity, SortBufferBytes: largeSortBufferBytes,
					SortFanIn: largeSortFanIn, Budget: budget,
				})
				if err != nil {
					b.Fatal(err)
				}
				result, err := diff.Build(ctx, source.GenerationID, target.GenerationID)
				if err != nil {
					b.Fatal(err)
				}
				if result.Summary.Converged != scenario.converged {
					b.Fatalf("scenario converged=%t, want %t", result.Summary.Converged, scenario.converged)
				}
				b.ReportMetric(float64(result.Summary.BlockerCount), "blockers")
				b.ReportMetric(float64(generationArtifactBytes(result.Metadata)), "diff_artifact_bytes")
			}
		})
	}
}

func BenchmarkMigrationBatchApply(b *testing.B) {
	root := b.TempDir()
	const directories = 128
	for index := range directories {
		path := filepath.Join(root, fmt.Sprintf("dir-%06d", index))
		if err := os.Mkdir(path, 0o755); err != nil {
			b.Fatal(err)
		}
		if err := os.Chmod(path, 0o700+os.FileMode(index%2)*0o050); err != nil {
			b.Fatal(err)
		}
	}
	scanner, err := NewScanner(root)
	if err != nil {
		b.Fatal(err)
	}
	scan, err := scanner.Scan(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	directoryRecords := directoryCreateRecords(scan.Entries)
	modeRecords := modeDiffRecords(scan.Entries)
	b.ReportMetric(directories, "directories")
	b.Run("mkdir", func(b *testing.B) {
		api := newFakeBatchApplyAPI()
		engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 4})
		if err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			result, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: directoryRecords})
			if err != nil || result.Verified != directories {
				b.Fatalf("mkdir result=%+v err=%v", result, err)
			}
		}
	})
	b.Run("chmod", func(b *testing.B) {
		api := newFakeBatchApplyAPI()
		engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 4})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: directoryRecords}); err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			result, err := engine.ApplyModes(context.Background(), &recordSliceReader{records: modeRecords})
			if err != nil || result.Verified != directories {
				b.Fatalf("chmod result=%+v err=%v", result, err)
			}
		}
	})
}

func BenchmarkMigrationFileApply(b *testing.B) {
	for _, scenario := range []struct {
		name       string
		files      int
		size       int
		largeFiles int
		largeSize  int
		threshold  int64
	}{
		{name: "inline", files: 128, size: 1024, threshold: 1 << 20},
		{name: "multipart", files: 8, size: 64 << 10, threshold: 1},
		{name: "mixed_inline_multipart", files: 136, size: 1024, largeFiles: 8, largeSize: 64 << 10, threshold: 32 << 10},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			fixture := make(map[string][]byte, scenario.files)
			logicalBytes := 0
			for index := range scenario.files {
				size := scenario.size
				if index >= scenario.files-scenario.largeFiles {
					size = scenario.largeSize
				}
				fixture[fmt.Sprintf("file-%06d", index)] = bytes.Repeat([]byte("x"), size)
				logicalBytes += size
			}
			scanner, source := filePipelineSource(b, fixture)
			paths := make([]string, 0, len(source))
			for path := range source {
				paths = append(paths, path)
			}
			sort.Strings(paths)
			records := make([]generationRecord, len(paths))
			for index, path := range paths {
				records[index] = fileDiffRecord(path, source[path], nil, "write")
			}
			api := newFakeFilePipelineAPI()
			engine := testFilePipeline(b, api, scanner, scenario.threshold)
			b.ReportMetric(float64(scenario.files), "files")
			b.ReportMetric(float64(logicalBytes), "logical_bytes")
			for b.Loop() {
				result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records})
				if err != nil || result.Verified != int64(scenario.files) {
					b.Fatalf("file result=%+v err=%v", result, err)
				}
			}
		})
	}
}

func BenchmarkMigrationBackpressure(b *testing.B) {
	now := time.Unix(100, 0)
	for b.Loop() {
		limit, err := newAdaptiveLimit(8, now)
		if err != nil {
			b.Fatal(err)
		}
		limit.OnFailure(&client.StatusError{StatusCode: http.StatusTooManyRequests}, now)
		limit.OnSuccess(now.Add(adaptiveHealthyWindow))
		if limit.Current() != 3 {
			b.Fatalf("adaptive limit = %d", limit.Current())
		}
	}
}

func assertScaleBenchmark(b *testing.B, elapsed time.Duration, processRSS uint64, maxDuration time.Duration) {
	b.Helper()
	if os.Getenv("DRIVE9_MIGRATION_SCALE_ASSERT") != "1" {
		return
	}
	if elapsed > maxDuration {
		b.Fatalf("scale duration %s exceeds %s", elapsed, maxDuration)
	}
	if processRSS > 3<<30 {
		b.Fatalf("scale process peak RSS %d exceeds %d", processRSS, int64(3<<30))
	}
}

type benchmarkMemorySampler struct {
	done      chan struct{}
	wait      sync.WaitGroup
	mu        sync.Mutex
	peakAlloc uint64
	peakSys   uint64
}

func startBenchmarkMemorySampler() *benchmarkMemorySampler {
	sampler := &benchmarkMemorySampler{done: make(chan struct{})}
	sampler.wait.Add(1)
	go func() {
		defer sampler.wait.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			sampler.sample()
			select {
			case <-sampler.done:
				return
			case <-ticker.C:
			}
		}
	}()
	return sampler
}

func (s *benchmarkMemorySampler) sample() {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	s.mu.Lock()
	s.peakAlloc = max(s.peakAlloc, memory.HeapAlloc)
	s.peakSys = max(s.peakSys, memory.HeapSys)
	s.mu.Unlock()
}

func (s *benchmarkMemorySampler) Stop() (heapAlloc, heapSys, processRSS uint64) {
	close(s.done)
	s.wait.Wait()
	s.sample()
	s.mu.Lock()
	heapAlloc, heapSys = s.peakAlloc, s.peakSys
	s.mu.Unlock()
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil && usage.Maxrss > 0 {
		processRSS = uint64(usage.Maxrss)
		if runtime.GOOS != "darwin" {
			processRSS *= 1024
		}
	}
	return heapAlloc, heapSys, processRSS
}

func writeBenchmarkGeneration(b *testing.B, ctx context.Context, store *generationStore, budget *memoryBudget, identity generationIdentity, generationID string, stage generationStage, kind generationRecordKind, entries int) generationMetadata {
	b.Helper()
	return writeBenchmarkGenerationFromReader(b, ctx, store, budget, identity, generationID, stage, kind,
		&syntheticGenerationReader{remaining: entries, kind: kind}, entries)
}

func writeBenchmarkGenerationFromReader(b *testing.B, ctx context.Context, store *generationStore, budget *memoryBudget, identity generationIdentity, generationID string, stage generationStage, kind generationRecordKind, reader generationRecordReader, entries int) generationMetadata {
	b.Helper()
	sorter, err := newExternalSorter(store, externalSortConfig{
		GenerationID: generationID, Stage: stage, Kind: kind, IDPrefix: "benchmark-sort",
		MaxBufferBytes: largeSortBufferBytes, FanIn: largeSortFanIn, Budget: budget,
	})
	if err != nil {
		b.Fatal(err)
	}
	result, err := sorter.Sort(ctx, reader)
	if err != nil {
		b.Fatal(err)
	}
	metadata := generationMetadata{
		FormatVersion: generationFormatVersion, GenerationID: generationID, RoundID: "round-" + generationID,
		Phase: PhaseSyncing, Identity: identity, EntryCount: int64(entries), CreatedAt: time.Unix(1, 0).UTC(),
		Stages: map[generationStage]generationStageMetadata{stage: completedStage(result.Chunks)},
	}
	if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
		b.Fatal(err)
	}
	if err := store.PublishComplete(ctx, metadata); err != nil {
		b.Fatal(err)
	}
	return metadata
}

func benchmarkEnvInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		b.Fatalf("%s must be a positive integer", name)
	}
	return value
}
