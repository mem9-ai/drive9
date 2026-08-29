package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type fakeFileNode struct {
	data       []byte
	mode       uint32
	revision   int64
	resourceID string
	nlink      uint32
	kind       EntryKind
	linkTarget string
}

type fakeFilePipelineAPI struct {
	mu                    sync.Mutex
	nodes                 map[string]fakeFileNode
	batchWriteCalls       [][]client.BatchWriteItem
	multipartCalls        []string
	hardlinkCalls         [][2]string
	symlinkCalls          [][2]string
	batchWriteCommitError bool
	nextResource          int
	beforeBatchWrite      func(*fakeFilePipelineAPI)
	afterBatchWrite       func(*fakeFilePipelineAPI)
}

func newFakeFilePipelineAPI() *fakeFilePipelineAPI {
	return &fakeFilePipelineAPI{nodes: make(map[string]fakeFileNode)}
}

func (f *fakeFilePipelineAPI) BatchWriteCtx(_ context.Context, items []client.BatchWriteItem) ([]client.BatchWriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.beforeBatchWrite != nil {
		f.beforeBatchWrite(f)
	}
	f.batchWriteCalls = append(f.batchWriteCalls, append([]client.BatchWriteItem(nil), items...))
	results := make([]client.BatchWriteResult, len(items))
	for index, item := range items {
		node, exists := f.nodes[item.Path]
		if item.ExpectedRevision == 0 && exists || item.ExpectedRevision > 0 && (!exists || node.revision != item.ExpectedRevision) {
			results[index] = client.BatchWriteResult{Path: item.Path, Status: 409, Error: "revision conflict"}
			continue
		}
		if !exists {
			f.nextResource++
			node = fakeFileNode{resourceID: fmt.Sprintf("resource-%d", f.nextResource), nlink: 1, kind: EntryRegular}
		}
		node.data = append([]byte(nil), item.Data...)
		node.revision++
		if item.HasMode {
			node.mode = item.Mode
		}
		f.nodes[item.Path] = node
		results[index] = client.BatchWriteResult{Path: item.Path, Status: 200, Revision: node.revision}
	}
	if f.afterBatchWrite != nil {
		f.afterBatchWrite(f)
	}
	if f.batchWriteCommitError {
		f.batchWriteCommitError = false
		return nil, errors.New("batch write response lost")
	}
	return results, nil
}

func (f *fakeFilePipelineAPI) BatchStatWithOptionsCtx(_ context.Context, paths []string, _ client.BatchStatOptions) ([]client.BatchStatResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	results := make([]client.BatchStatResult, len(paths))
	for index, path := range paths {
		node, exists := f.nodes[path]
		if !exists {
			results[index] = client.BatchStatResult{Path: path, Status: 404, Error: "not found"}
			continue
		}
		results[index] = batchStatFromFileNode(path, node)
	}
	return results, nil
}

func (f *fakeFilePipelineAPI) WriteStreamConditionalWithChecksumAndPreCompleteCheck(_ context.Context, path string, reader io.Reader, size int64, _ client.ProgressFunc, expectedRevision int64, checksum string, preComplete func() error) (*client.StatResult, error) {
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != size || checksumBytes(body) != checksum {
		return nil, errors.New("multipart checksum mismatch")
	}
	if err := preComplete(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	node, exists := f.nodes[path]
	if expectedRevision == 0 && exists || expectedRevision > 0 && (!exists || node.revision != expectedRevision) {
		return nil, client.ErrConflict
	}
	if !exists {
		f.nextResource++
		node = fakeFileNode{resourceID: fmt.Sprintf("resource-%d", f.nextResource), nlink: 1, kind: EntryRegular, mode: 0o644}
	}
	node.data, node.revision = append([]byte(nil), body...), node.revision+1
	f.nodes[path] = node
	f.multipartCalls = append(f.multipartCalls, path)
	return statFromFileNode(node), nil
}

func (f *fakeFilePipelineAPI) StatCtx(_ context.Context, path string) (*client.StatResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node, exists := f.nodes[path]
	if !exists {
		return nil, &client.StatusError{StatusCode: 404, Message: "not found"}
	}
	return statFromFileNode(node), nil
}

func (f *fakeFilePipelineAPI) DeleteFileCtx(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, path)
	return nil
}

func (f *fakeFilePipelineAPI) HardlinkCtx(_ context.Context, source, destination string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	node, exists := f.nodes[source]
	if !exists {
		return &client.StatusError{StatusCode: 404, Message: "primary missing"}
	}
	node.nlink++
	for path, existing := range f.nodes {
		if existing.resourceID == node.resourceID {
			existing.nlink = node.nlink
			f.nodes[path] = existing
		}
	}
	f.nodes[destination] = node
	f.hardlinkCalls = append(f.hardlinkCalls, [2]string{source, destination})
	return nil
}

func (f *fakeFilePipelineAPI) SymlinkCtx(_ context.Context, target, linkPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextResource++
	f.nodes[linkPath] = fakeFileNode{
		kind: EntrySymlink, linkTarget: target, data: []byte(target), mode: 0o777,
		revision: 1, resourceID: fmt.Sprintf("resource-%d", f.nextResource), nlink: 1,
	}
	f.symlinkCalls = append(f.symlinkCalls, [2]string{target, linkPath})
	return nil
}

func TestFilePipelineBatchesInlineCreateAndUpdateAndAdoptsLostResponse(t *testing.T) {
	scanner, records := filePipelineSource(t, map[string][]byte{"create": []byte("new"), "update": []byte("updated")})
	api := newFakeFilePipelineAPI()
	api.nodes["/update"] = fakeFileNode{data: []byte("old"), mode: 0o644, revision: 4, resourceID: "existing", nlink: 1, kind: EntryRegular}
	updateTarget := targetRecordFromFake("/update", api.nodes["/update"])
	diff := []generationRecord{
		fileDiffRecord("/create", records["/create"], nil, "write"),
		fileDiffRecord("/update", records["/update"], updateTarget, "write"),
	}
	api.batchWriteCommitError = true
	engine := testFilePipeline(t, api, scanner, 1024)
	var progress filePipelineProgress
	engine.onProgress = func(current filePipelineProgress) { progress = current }
	result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: diff})
	if err != nil || result.Verified != 2 || result.Unknown != 0 || len(api.batchWriteCalls) != 1 {
		t.Fatalf("result=%+v calls=%d err=%v", result, len(api.batchWriteCalls), err)
	}
	call := api.batchWriteCalls[0]
	if len(call) != 2 || call[0].ExpectedRevision != 0 || call[1].ExpectedRevision != 4 {
		t.Fatalf("batch = %+v", call)
	}
	if progress.BatchCount != 1 || progress.InlineFiles != 2 || progress.InlineBytes != 10 || progress.MultipartFiles != 0 {
		t.Fatalf("progress = %+v", progress)
	}
}

func TestFilePipelineRoutesNonInlineThroughSingleReadMultipart(t *testing.T) {
	scanner, records := filePipelineSource(t, map[string][]byte{"large": bytes.Repeat([]byte("x"), 4096)})
	api := newFakeFilePipelineAPI()
	engine := testFilePipeline(t, api, scanner, 1024)
	result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: []generationRecord{
		fileDiffRecord("/large", records["/large"], nil, "write"),
	}})
	if err != nil || result.Verified != 1 || len(api.multipartCalls) != 1 || len(api.batchWriteCalls) != 0 {
		t.Fatalf("result=%+v multipart=%v batches=%d err=%v", result, api.multipartCalls, len(api.batchWriteCalls), err)
	}
}

func TestFilePipelineWritesOneHardlinkPrimaryThenAliases(t *testing.T) {
	root := t.TempDir()
	primaryPath := filepath.Join(root, "primary")
	if err := os.WriteFile(primaryPath, []byte("shared"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(primaryPath, filepath.Join(root, "alias")); err != nil {
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
	primary := sourceRecordWithChecksum(t, scanner, scan.Entries["/alias"])
	alias := sourceRecordWithChecksum(t, scanner, scan.Entries["/primary"])
	primary.HardlinkKey, alias.HardlinkKey = "1:2", "1:2"
	records := []generationRecord{
		fileDiffRecord(primary.Path, primary, nil, "link-0-primary"),
		fileDiffRecord(alias.Path, alias, nil, "link-1-alias"),
	}
	records[0].Diff.PrimaryPath = primary.Path
	records[1].Diff.PrimaryPath = primary.Path
	api := newFakeFilePipelineAPI()
	engine := testFilePipeline(t, api, scanner, 1024)
	files, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records})
	if err != nil || files.Verified != 1 {
		t.Fatalf("files=%+v err=%v", files, err)
	}
	links, err := engine.ApplyLinks(context.Background(), &recordSliceReader{records: records})
	if err != nil || links.Verified != 1 || len(api.batchWriteCalls) != 1 || len(api.batchWriteCalls[0]) != 1 || len(api.hardlinkCalls) != 1 {
		t.Fatalf("links=%+v batches=%v hardlinks=%v err=%v", links, api.batchWriteCalls, api.hardlinkCalls, err)
	}
	if api.nodes["/alias"].resourceID != api.nodes["/primary"].resourceID {
		t.Fatalf("resources alias=%s primary=%s", api.nodes["/alias"].resourceID, api.nodes["/primary"].resourceID)
	}
}

func TestFilePipelinePreservesSymlinkTextThroughLinkStage(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("../target", filepath.Join(root, "link")); err != nil {
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
	source := sourceGenerationRecordFromEntry(scan.Entries["/link"])
	record := fileDiffRecord("/link", source, nil, "write")
	api := newFakeFilePipelineAPI()
	engine := testFilePipeline(t, api, scanner, 1024)
	result, err := engine.ApplyLinks(context.Background(), &recordSliceReader{records: []generationRecord{record}})
	if err != nil || result.Verified != 1 || len(api.symlinkCalls) != 1 || api.symlinkCalls[0] != [2]string{"../target", "/link"} {
		t.Fatalf("result=%+v symlinks=%v err=%v", result, api.symlinkCalls, err)
	}
}

func TestFilePipelineRejectsLivePhaseAndSourceMutation(t *testing.T) {
	scanner, records := filePipelineSource(t, map[string][]byte{"file": []byte("data")})
	api := newFakeFilePipelineAPI()
	engine, err := newFilePipeline(api, scanner, filePipelineConfig{
		Prefix: "/", Phase: PhaseDualWriteRepairing, InlineThreshold: 1024,
		InlineWorkers: 1, MultipartWorkers: 1, MaxBytesPerSecond: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	diff := &recordSliceReader{records: []generationRecord{fileDiffRecord("/file", records["/file"], nil, "write")}}
	if _, err := engine.ApplyFiles(context.Background(), diff); !errors.Is(err, ErrUnsafeApply) {
		t.Fatalf("live error = %v", err)
	}

	engine = testFilePipeline(t, api, scanner, 1024)
	scanner.afterRead = func(string) { _ = os.WriteFile(filepath.Join(scanner.root, "file"), []byte("changed"), 0o644) }
	diff = &recordSliceReader{records: []generationRecord{fileDiffRecord("/file", records["/file"], nil, "write")}}
	if _, err := engine.ApplyFiles(context.Background(), diff); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("mutation error = %v", err)
	}
}

func TestFilePipelineExactBatchWriteBoundaries(t *testing.T) {
	for _, count := range []int{127, 128, 129} {
		t.Run(fmt.Sprintf("items-%d", count), func(t *testing.T) {
			files := make(map[string][]byte, count)
			for index := range count {
				files[fmt.Sprintf("file-%06d", index)] = []byte{'x'}
			}
			scanner, source := filePipelineSource(t, files)
			paths := make([]string, 0, len(source))
			for path := range source {
				paths = append(paths, path)
			}
			slices.Sort(paths)
			records := make([]generationRecord, len(paths))
			for index, path := range paths {
				records[index] = fileDiffRecord(path, source[path], nil, "write")
			}
			api := newFakeFilePipelineAPI()
			engine := testFilePipeline(t, api, scanner, 1024)
			result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records})
			if err != nil || result.Verified != int64(count) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			sizes := make([]int, len(api.batchWriteCalls))
			for index, call := range api.batchWriteCalls {
				sizes[index] = len(call)
				if len(call) == 0 || len(call) > client.MaxBatchWriteItems {
					t.Fatalf("batch size=%d", len(call))
				}
			}
			slices.Sort(sizes)
			want := []int{count}
			if count == 129 {
				want = []int{1, 128}
			}
			if !slices.Equal(sizes, want) {
				t.Fatalf("batch sizes=%v, want %v", sizes, want)
			}
		})
	}

	for _, tc := range []struct {
		name      string
		sizes     []int
		wantCalls []int
	}{
		{name: "exact 4 MiB", sizes: []int{2 << 20, 2 << 20}, wantCalls: []int{2}},
		{name: "over 4 MiB", sizes: []int{2 << 20, 2 << 20, 1}, wantCalls: []int{1, 2}},
		{name: "single 4 MiB", sizes: []int{4 << 20}, wantCalls: []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := make(map[string][]byte, len(tc.sizes))
			for index, size := range tc.sizes {
				files[fmt.Sprintf("file-%06d", index)] = bytes.Repeat([]byte{'x'}, size)
			}
			scanner, source := filePipelineSource(t, files)
			paths := make([]string, 0, len(source))
			for path := range source {
				paths = append(paths, path)
			}
			slices.Sort(paths)
			records := make([]generationRecord, len(paths))
			for index, path := range paths {
				records[index] = fileDiffRecord(path, source[path], nil, "write")
			}
			api := newFakeFilePipelineAPI()
			engine := testFilePipeline(t, api, scanner, 4<<20+1)
			result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records})
			if err != nil || result.Verified != int64(len(records)) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			calls := make([]int, len(api.batchWriteCalls))
			for index, call := range api.batchWriteCalls {
				calls[index] = len(call)
				var payload int64
				for _, item := range call {
					payload += int64(len(item.Data))
				}
				if payload > client.MaxBatchWriteBytes {
					t.Fatalf("payload=%d exceeds %d", payload, client.MaxBatchWriteBytes)
				}
			}
			slices.Sort(calls)
			if !slices.Equal(calls, tc.wantCalls) {
				t.Fatalf("calls=%v, want %v", calls, tc.wantCalls)
			}
		})
	}
}

func TestFilePipelineCancellationAndMemoryCapBeforeMutation(t *testing.T) {
	scanner, source := filePipelineSource(t, map[string][]byte{"file": []byte("data")})
	records := []generationRecord{fileDiffRecord("/file", source["/file"], nil, "write")}

	t.Run("pre-canceled", func(t *testing.T) {
		api := newFakeFilePipelineAPI()
		engine := testFilePipeline(t, api, scanner, 1024)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := engine.ApplyFiles(ctx, &recordSliceReader{records: records}); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context.Canceled", err)
		}
		if len(api.batchWriteCalls)+len(api.multipartCalls) != 0 {
			t.Fatal("canceled pipeline mutated target")
		}
	})

	t.Run("memory cap", func(t *testing.T) {
		api := newFakeFilePipelineAPI()
		budget, err := newMemoryBudget(1)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := newFilePipeline(api, scanner, filePipelineConfig{
			Prefix: "/", Phase: PhaseSyncing, InlineThreshold: 1024,
			InlineWorkers: 1, MultipartWorkers: 1, MaxBytesPerSecond: 1 << 20, Budget: budget,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records}); !errors.Is(err, ErrMemoryBudgetExceeded) {
			t.Fatalf("error=%v, want ErrMemoryBudgetExceeded", err)
		}
		if len(api.batchWriteCalls)+len(api.multipartCalls) != 0 {
			t.Fatal("over-budget pipeline mutated target")
		}
	})
}

func TestFilePipelineTargetCASAndPostVerificationRacesStayPending(t *testing.T) {
	scanner, source := filePipelineSource(t, map[string][]byte{"file": []byte("new")})
	original := fakeFileNode{data: []byte("old"), mode: 0o644, revision: 4, resourceID: "original", nlink: 1, kind: EntryRegular}
	target := targetRecordFromFake("/file", original)
	records := []generationRecord{fileDiffRecord("/file", source["/file"], target, "write")}

	t.Run("revision changes before batch", func(t *testing.T) {
		api := newFakeFilePipelineAPI()
		api.nodes["/file"] = original
		api.beforeBatchWrite = func(api *fakeFilePipelineAPI) {
			replaced := api.nodes["/file"]
			replaced.data = []byte("concurrent")
			replaced.revision++
			api.nodes["/file"] = replaced
		}
		engine := testFilePipeline(t, api, scanner, 1024)
		result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records})
		if !errors.Is(err, ErrApplyRescan) || result.Pending != 1 || result.Verified != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})

	t.Run("resource changes before post stat", func(t *testing.T) {
		api := newFakeFilePipelineAPI()
		api.nodes["/file"] = original
		api.afterBatchWrite = func(api *fakeFilePipelineAPI) {
			replaced := api.nodes["/file"]
			replaced.resourceID = "replacement"
			api.nodes["/file"] = replaced
		}
		engine := testFilePipeline(t, api, scanner, 1024)
		result, err := engine.ApplyFiles(context.Background(), &recordSliceReader{records: records})
		if !errors.Is(err, ErrApplyRescan) || result.Pending != 1 || result.Verified != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})
}

func testFilePipeline(t testing.TB, api filePipelineClient, scanner *Scanner, threshold int64) *filePipeline {
	t.Helper()
	engine, err := newFilePipeline(api, scanner, filePipelineConfig{
		Prefix: "/", Phase: PhaseSyncing, InlineThreshold: threshold,
		InlineWorkers: 4, MultipartWorkers: 2, MaxBytesPerSecond: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine.clock = func() time.Time { return testNow }
	return engine
}

func filePipelineSource(t testing.TB, files map[string][]byte) (*Scanner, map[string]*sourceGenerationRecord) {
	t.Helper()
	root := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(root, name), body, 0o644); err != nil {
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
	result := make(map[string]*sourceGenerationRecord, len(files))
	for path, entry := range scan.Entries {
		record := sourceRecordWithChecksum(t, scanner, entry)
		result[path] = record
	}
	return scanner, result
}

func sourceRecordWithChecksum(t testing.TB, scanner *Scanner, entry SourceEntry) *sourceGenerationRecord {
	t.Helper()
	deep, err := scanner.ReadStableEntry(context.Background(), entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.ChecksumSHA256 = deep.ChecksumSHA256
	return sourceGenerationRecordFromEntry(entry)
}

func fileDiffRecord(path string, source *sourceGenerationRecord, target *targetGenerationRecord, operation string) generationRecord {
	return generationRecord{
		Key: operation + "\x00" + path,
		Diff: &diffGenerationRecord{
			Path: path, Operation: operation, Finding: FindingContent, Severity: SeverityBlocker,
			Source: source, Target: target,
		},
	}
}

func targetRecordFromFake(path string, node fakeFileNode) *targetGenerationRecord {
	mode, revision, checksum := node.mode, node.revision, checksumBytes(node.data)
	return &targetGenerationRecord{
		Path: path, Kind: node.kind, Size: int64(len(node.data)), Mode: &mode, MetadataComplete: true,
		IdentityKind: "inode", Revision: &revision, ResourceID: node.resourceID, Nlink: node.nlink, ChecksumSHA256: &checksum,
	}
}

func batchStatFromFileNode(path string, node fakeFileNode) client.BatchStatResult {
	mode := node.mode
	switch node.kind {
	case EntryRegular:
		mode |= 0o100000
	case EntrySymlink:
		mode |= 0o120000
	}
	return client.BatchStatResult{
		Path: path, Status: 200, Size: int64(len(node.data)), Revision: node.revision,
		Mode: mode, HasMode: true, ResourceID: node.resourceID, Nlink: node.nlink,
		ChecksumSHA256: checksumBytes(node.data),
	}
}

func statFromFileNode(node fakeFileNode) *client.StatResult {
	stat := batchStatFromFileNode("", node)
	return &client.StatResult{
		Size: stat.Size, Revision: stat.Revision, Mode: stat.Mode, HasMode: true,
		ResourceID: stat.ResourceID, Nlink: stat.Nlink, ChecksumSHA256: stat.ChecksumSHA256,
	}
}

func checksumBytes(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
