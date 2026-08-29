package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

type fakeBatchApplyAPI struct {
	mu               sync.Mutex
	nodes            map[string]client.BatchStatResult
	mkdirCalls       [][]client.BatchMkdirItem
	chmodCalls       [][]client.BatchChmodItem
	mkdirFailures    map[string]bool
	mkdirCommitError bool
	chmodCommitError bool
	mkdirReverse     bool
	chmodReverse     bool
	deleteCalls      []string
}

func newFakeBatchApplyAPI() *fakeBatchApplyAPI {
	return &fakeBatchApplyAPI{nodes: make(map[string]client.BatchStatResult), mkdirFailures: make(map[string]bool)}
}

func (f *fakeBatchApplyAPI) BatchMkdirCtx(_ context.Context, items []client.BatchMkdirItem) ([]client.BatchMkdirResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mkdirCalls = append(f.mkdirCalls, append([]client.BatchMkdirItem(nil), items...))
	results := make([]client.BatchMkdirResult, len(items))
	for index, item := range items {
		if f.mkdirFailures[item.Path] {
			message := "parent conflict"
			results[index] = client.BatchMkdirResult{Path: item.Path, Status: 409, Error: &message}
			continue
		}
		created := true
		resource := "resource:" + item.Path
		f.nodes[item.Path] = client.BatchStatResult{
			Path: item.Path, Status: 200, IsDir: true, Mode: item.Mode, HasMode: true,
			Revision: 1, ResourceID: resource, Nlink: 2,
		}
		results[index] = client.BatchMkdirResult{Path: item.Path, Status: 201, Created: &created, ResourceID: &resource}
	}
	if f.mkdirReverse {
		slices.Reverse(results)
	}
	if f.mkdirCommitError {
		f.mkdirCommitError = false
		return nil, errors.New("mkdir response lost")
	}
	return results, nil
}

func (f *fakeBatchApplyAPI) BatchChmodCtx(_ context.Context, items []client.BatchChmodItem) ([]client.BatchChmodResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chmodCalls = append(f.chmodCalls, append([]client.BatchChmodItem(nil), items...))
	results := make([]client.BatchChmodResult, len(items))
	for index, item := range items {
		node, exists := f.nodes[item.Path]
		if !exists || node.ResourceID != item.ExpectedResourceID || !node.IsDir && (item.ExpectedRevision == nil || *item.ExpectedRevision != node.Revision) {
			message := "identity conflict"
			results[index] = client.BatchChmodResult{Path: item.Path, Status: 409, Error: &message}
			continue
		}
		node.Mode, node.HasMode = item.Mode, true
		f.nodes[item.Path] = node
		resource, revision, mode := node.ResourceID, node.Revision, node.Mode
		results[index] = client.BatchChmodResult{
			Path: item.Path, Status: 200, ResourceID: &resource, Revision: &revision, Mode: &mode,
		}
	}
	if f.chmodReverse {
		slices.Reverse(results)
	}
	if f.chmodCommitError {
		f.chmodCommitError = false
		return nil, errors.New("chmod response lost")
	}
	return results, nil
}

func (f *fakeBatchApplyAPI) BatchStatWithOptionsCtx(_ context.Context, paths []string, _ client.BatchStatOptions) ([]client.BatchStatResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	results := make([]client.BatchStatResult, len(paths))
	for index, path := range paths {
		result, exists := f.nodes[path]
		if !exists {
			result = client.BatchStatResult{Path: path, Status: 404, Error: "not found"}
		}
		results[index] = result
	}
	return results, nil
}

func (f *fakeBatchApplyAPI) DeleteFileCtx(_ context.Context, path string) error {
	return f.delete(path)
}

func (f *fakeBatchApplyAPI) DeleteDirCtx(_ context.Context, path string) error {
	return f.delete(path)
}

func (f *fakeBatchApplyAPI) delete(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, path)
	delete(f.nodes, path)
	return nil
}

func TestBatchApplyDirectoriesUsesDepthBarriersAndTemporaryMode(t *testing.T) {
	scanner, entries := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	records := directoryCreateRecords(entries)
	result, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: records})
	if err != nil {
		t.Fatal(err)
	}
	if result.Verified != int64(len(records)) || result.Pending != 0 || len(api.mkdirCalls) != 3 {
		t.Fatalf("result=%+v calls=%v", result, api.mkdirCalls)
	}
	for _, call := range api.mkdirCalls {
		for _, item := range call {
			if item.Mode != 0o755 {
				t.Fatalf("mkdir item = %+v", item)
			}
		}
	}
	if len(api.mkdirCalls[0]) != 2 || len(api.mkdirCalls[1]) != 1 || len(api.mkdirCalls[2]) != 1 {
		t.Fatalf("depth calls = %v", api.mkdirCalls)
	}
}

func TestBatchApplyDirectoryPartialFailureBlocksDeeperDepth(t *testing.T) {
	scanner, entries := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	api.mkdirFailures["/z/"] = true
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: directoryCreateRecords(entries)})
	if !errors.Is(err, ErrApplyRescan) || result.Verified != 1 || result.Pending != 1 || len(api.mkdirCalls) != 1 {
		t.Fatalf("result=%+v calls=%v err=%v", result, api.mkdirCalls, err)
	}
}

func TestBatchApplyDirectoryLostResponseAdoptsExactStat(t *testing.T) {
	scanner, entries := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	api.mkdirCommitError = true
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	records := directoryCreateRecords(entries)[:2]
	result, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: records})
	if err != nil || result.Verified != 2 || result.Unknown != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestBatchApplyModesUsesLatestIdentityAndRestrictiveDirectoriesLast(t *testing.T) {
	scanner, entries := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	for path, entry := range entries {
		remote := targetRemotePath("/", path, entry.Kind == EntryDirectory)
		mode := uint32(0o644)
		isDir, nlink := false, uint32(1)
		if entry.Kind == EntryDirectory {
			mode, isDir, nlink = 0o755, true, 2
		}
		api.nodes[remote] = client.BatchStatResult{
			Path: remote, Status: 200, IsDir: isDir, Mode: mode, HasMode: true,
			Revision: 3, ResourceID: "resource:" + remote, Nlink: nlink,
		}
	}
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	records := modeDiffRecords(entries)
	result, err := engine.ApplyModes(context.Background(), &recordSliceReader{records: records})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pending != 0 || len(api.chmodCalls) < 3 {
		t.Fatalf("result=%+v calls=%v", result, api.chmodCalls)
	}
	var order []string
	for _, call := range api.chmodCalls {
		for _, item := range call {
			order = append(order, item.Path)
			if item.ExpectedResourceID == "" || item.Path[len(item.Path)-1] != '/' && (item.ExpectedRevision == nil || *item.ExpectedRevision != 3) {
				t.Fatalf("chmod item = %+v", item)
			}
		}
	}
	want := []string{"/file", "/a/b/c/", "/a/b/", "/a/", "/z/"}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestBatchApplyModeLostResponseAdoptsExactStat(t *testing.T) {
	scanner, entries := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	api.nodes["/file"] = client.BatchStatResult{
		Path: "/file", Status: 200, Mode: 0o644, HasMode: true,
		Revision: 3, ResourceID: "resource:/file", Nlink: 1,
	}
	api.chmodCommitError = true
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	records := modeDiffRecords(entries)
	result, err := engine.ApplyModes(context.Background(), &recordSliceReader{records: records[:1]})
	if err != nil || result.Verified != 1 || result.Unknown != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestBatchApplyFastPathRejectsLivePhase(t *testing.T) {
	scanner, entries := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseDualWriteRepairing, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: directoryCreateRecords(entries)}); !errors.Is(err, ErrUnsafeApply) {
		t.Fatalf("error = %v, want ErrUnsafeApply", err)
	}
	if len(api.mkdirCalls) != 0 || len(api.chmodCalls) != 0 {
		t.Fatalf("live phase calls mkdir=%v chmod=%v", api.mkdirCalls, api.chmodCalls)
	}
}

func TestBatchApplyDeletesChildrenBeforeParentsWithIdentityCheck(t *testing.T) {
	scanner, _ := batchApplySource(t)
	api := newFakeBatchApplyAPI()
	revision := int64(2)
	for _, path := range []string{"/parent/child", "/parent/"} {
		isDir, nlink, mode := false, uint32(1), uint32(0o644)
		if path[len(path)-1] == '/' {
			isDir, nlink, mode = true, 2, 0o755
		}
		api.nodes[path] = client.BatchStatResult{
			Path: path, Status: 200, IsDir: isDir, HasMode: true, Mode: mode,
			Revision: revision, ResourceID: "resource:" + path, Nlink: nlink,
		}
	}
	records := []generationRecord{
		{Key: "delete\x00a", Diff: &diffGenerationRecord{Path: "/parent/child", Operation: "delete", Finding: FindingTargetOnly, Target: &targetGenerationRecord{Path: "/parent/child", Kind: EntryRegular, Revision: &revision, ResourceID: "resource:/parent/child", Nlink: 1}}},
		{Key: "delete\x00b", Diff: &diffGenerationRecord{Path: "/parent", Operation: "delete", Finding: FindingTargetOnly, Target: &targetGenerationRecord{Path: "/parent", Kind: EntryDirectory, Revision: &revision, ResourceID: "resource:/parent/", Nlink: 2}}},
	}
	engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.ApplyDeletes(context.Background(), &recordSliceReader{records: records})
	if err != nil || result.Verified != 2 || !slices.Equal(api.deleteCalls, []string{"/parent/child", "/parent/"}) {
		t.Fatalf("result=%+v calls=%v err=%v", result, api.deleteCalls, err)
	}
}

func TestBatchApplyExact128Boundaries(t *testing.T) {
	for _, count := range []int{127, 128, 129} {
		t.Run(fmt.Sprintf("mkdir-%d", count), func(t *testing.T) {
			scanner, entries := batchApplyManyDirectories(t, count, 0o700)
			api := newFakeBatchApplyAPI()
			engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 2})
			if err != nil {
				t.Fatal(err)
			}
			result, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: directoryCreateRecords(entries)})
			if err != nil || result.Verified != int64(count) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			wantCalls := (count + client.MaxBatchMkdirItems - 1) / client.MaxBatchMkdirItems
			if len(api.mkdirCalls) != wantCalls {
				t.Fatalf("mkdir calls=%d, want %d", len(api.mkdirCalls), wantCalls)
			}
			for _, call := range api.mkdirCalls {
				if len(call) == 0 || len(call) > client.MaxBatchMkdirItems {
					t.Fatalf("mkdir batch size=%d", len(call))
				}
			}
		})

		t.Run(fmt.Sprintf("chmod-%d", count), func(t *testing.T) {
			scanner, entries := batchApplyManyDirectories(t, count, 0o700)
			api := newFakeBatchApplyAPI()
			engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 2})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: directoryCreateRecords(entries)}); err != nil {
				t.Fatal(err)
			}
			result, err := engine.ApplyModes(context.Background(), &recordSliceReader{records: modeDiffRecords(entries)})
			if err != nil || result.Verified != int64(count) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			wantCalls := (count + client.MaxBatchChmodItems - 1) / client.MaxBatchChmodItems
			if len(api.chmodCalls) != wantCalls {
				t.Fatalf("chmod calls=%d, want %d", len(api.chmodCalls), wantCalls)
			}
			for _, call := range api.chmodCalls {
				if len(call) == 0 || len(call) > client.MaxBatchChmodItems {
					t.Fatalf("chmod batch size=%d", len(call))
				}
			}
		})
	}
}

func TestBatchApplyMalformedOrderingCancellationAndMemoryCap(t *testing.T) {
	scanner, entries := batchApplyManyDirectories(t, 2, 0o700)
	records := directoryCreateRecords(entries)

	t.Run("mkdir response order", func(t *testing.T) {
		api := newFakeBatchApplyAPI()
		api.mkdirReverse = true
		engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		result, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: records})
		if !errors.Is(err, ErrApplyRescan) || result.Unknown == 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})

	t.Run("chmod response order", func(t *testing.T) {
		api := newFakeBatchApplyAPI()
		engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: records}); err != nil {
			t.Fatal(err)
		}
		api.chmodReverse = true
		result, err := engine.ApplyModes(context.Background(), &recordSliceReader{records: modeDiffRecords(entries)})
		if !errors.Is(err, ErrApplyRescan) || result.Pending == 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})

	t.Run("pre-canceled", func(t *testing.T) {
		api := newFakeBatchApplyAPI()
		engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := engine.ApplyDirectories(ctx, &recordSliceReader{records: records}); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context.Canceled", err)
		}
		if len(api.mkdirCalls) != 0 {
			t.Fatalf("canceled scheduler made %d calls", len(api.mkdirCalls))
		}
	})

	t.Run("memory cap", func(t *testing.T) {
		api := newFakeBatchApplyAPI()
		budget, err := newMemoryBudget(1)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := newBatchApplyEngine(api, scanner, batchApplyConfig{Prefix: "/", Phase: PhaseSyncing, Workers: 1, Budget: budget})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.ApplyDirectories(context.Background(), &recordSliceReader{records: records}); !errors.Is(err, ErrMemoryBudgetExceeded) {
			t.Fatalf("error=%v, want ErrMemoryBudgetExceeded", err)
		}
		if len(api.mkdirCalls) != 0 {
			t.Fatalf("over-budget scheduler made %d calls", len(api.mkdirCalls))
		}
	})
}

func batchApplySource(t testing.TB) (*Scanner, map[string]SourceEntry) {
	t.Helper()
	root := t.TempDir()
	for path, mode := range map[string]os.FileMode{"a": 0o700, "z": 0o711, "a/b": 0o750, "a/b/c": 0o740} {
		if err := os.MkdirAll(filepath.Join(root, path), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filepath.Join(root, path), mode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0o600); err != nil {
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
	return scanner, scan.Entries
}

func batchApplyManyDirectories(t testing.TB, count int, mode os.FileMode) (*Scanner, map[string]SourceEntry) {
	t.Helper()
	root := t.TempDir()
	for index := range count {
		path := filepath.Join(root, fmt.Sprintf("dir-%06d", index))
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return scanner, scan.Entries
}

func directoryCreateRecords(entries map[string]SourceEntry) []generationRecord {
	var records []generationRecord
	for _, entry := range entries {
		if entry.Kind != EntryDirectory {
			continue
		}
		source := sourceGenerationRecordFromEntry(entry)
		dependency := diffDependency("mkdir", "basic", entry.Path, source, nil)
		records = append(records, generationRecord{
			Key: fmt.Sprintf("mkdir\x00%s\x00%s", dependency, entry.Path),
			Diff: &diffGenerationRecord{
				Path: entry.Path, Operation: "mkdir", DependencyKey: dependency,
				Finding: FindingSourceOnly, Severity: SeverityBlocker, Source: source,
			},
		})
	}
	slices.SortFunc(records, func(left, right generationRecord) int { return compareString(left.Key, right.Key) })
	return records
}

func modeDiffRecords(entries map[string]SourceEntry) []generationRecord {
	var records []generationRecord
	for _, entry := range entries {
		source := sourceGenerationRecordFromEntry(entry)
		dependency := diffDependency("chmod", "mode", entry.Path, source, nil)
		records = append(records, generationRecord{
			Key: fmt.Sprintf("chmod\x00%s\x00%s", dependency, entry.Path),
			Diff: &diffGenerationRecord{
				Path: entry.Path, Operation: "chmod", DependencyKey: dependency,
				Finding: FindingMetadata, Severity: SeverityBlocker, Source: source,
			},
		})
	}
	slices.SortFunc(records, func(left, right generationRecord) int { return compareString(left.Key, right.Key) })
	return records
}

func compareString(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
