package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type inventoryFixture struct {
	mu            sync.Mutex
	directories   map[string][]client.FileInfo
	stats         map[string]client.BatchStatResult
	batchSizes    []int
	checksumBatch [][]string
	requests      []string
	listFailure   string
}

func (f *inventoryFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	if r.Method == http.MethodGet && r.URL.Query().Get("list") == "1" {
		remote := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		if remote != "/" {
			remote = strings.TrimSuffix(remote, "/")
		}
		if remote == f.listFailure {
			http.Error(w, "injected list failure", http.StatusInternalServerError)
			return
		}
		entries, ok := f.directories[remote]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat" {
		var request struct {
			Paths           []string `json:"paths"`
			IncludeChecksum bool     `json:"include_checksum"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.batchSizes = append(f.batchSizes, len(request.Paths))
		if request.IncludeChecksum {
			f.checksumBatch = append(f.checksumBatch, append([]string(nil), request.Paths...))
		}
		results := make([]client.BatchStatResult, len(request.Paths))
		for i, path := range request.Paths {
			result, ok := f.stats[path]
			if !ok {
				result = client.BatchStatResult{Path: path, Status: http.StatusNotFound, Error: "missing"}
			}
			results[i] = result
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
		return
	}
	http.NotFound(w, r)
}

func newTargetScannerFixture(t *testing.T, fixture *inventoryFixture, prefix string) *TargetScanner {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(server.Close)
	scanner, err := NewTargetScanner(client.New(server.URL, "test-key"), prefix)
	if err != nil {
		t.Fatal(err)
	}
	return scanner
}

func targetStat(path string, kind EntryKind, revision int64, checksum string) client.BatchStatResult {
	mode := uint32(0o100644)
	isDir := false
	size := int64(4)
	switch kind {
	case EntryDirectory:
		mode, isDir = 0o755, true
		size = 0
	case EntrySymlink:
		mode = 0o120777
	}
	return client.BatchStatResult{
		Path: path, Status: http.StatusOK, Size: size, IsDir: isDir, Revision: revision,
		Mode: mode, HasMode: true, ResourceID: "id-" + path, Nlink: 1, ChecksumSHA256: checksum,
	}
}

func TestTargetScannerConfinesPrefixExcludesControlAndBatches(t *testing.T) {
	const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fixture := &inventoryFixture{
		directories: map[string][]client.FileInfo{
			"/": {
				{Name: ".drive9-migration", IsDir: true, HasMode: true, Mode: 0o755},
				{Name: "dir", IsDir: true, HasMode: true, Mode: 0o755, ResourceID: "id-/dir/"},
			},
			"/dir": make([]client.FileInfo, 300),
		},
		stats: make(map[string]client.BatchStatResult),
	}
	fixture.stats["/dir/"] = targetStat("/dir/", EntryDirectory, 0, "")
	for i := range 300 {
		name := fmt.Sprintf("f%03d", i)
		path := "/dir/" + name
		fixture.directories["/dir"][i] = client.FileInfo{
			Name: name, Size: 4, Revision: 1, HasMode: true, Mode: 0o100644, ResourceID: "id-" + path,
		}
		fixture.stats[path] = targetStat(path, EntryRegular, 1, checksum)
	}

	scanner := newTargetScannerFixture(t, fixture, "/")
	deep := make(map[string]struct{}, 300)
	for i := range 300 {
		deep[fmt.Sprintf("/dir/f%03d", i)] = struct{}{}
	}
	result, err := scanner.Scan(context.Background(), deep)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || len(result.Entries) != 301 {
		t.Fatalf("target scan=%+v", result)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	for _, request := range fixture.requests {
		if strings.Contains(request, ControlPrefix+"/") {
			t.Fatalf("control subtree was read: %s", request)
		}
	}
	for _, size := range fixture.batchSizes {
		if size > client.MaxBatchStatPaths {
			t.Fatalf("batch size=%d", size)
		}
	}
	if len(fixture.checksumBatch) != 2 || len(fixture.checksumBatch[0])+len(fixture.checksumBatch[1]) != 300 {
		t.Fatalf("checksum batches=%v", fixture.checksumBatch)
	}
}

func TestTargetScannerFailureAndConcurrentChangeRemainIncomplete(t *testing.T) {
	t.Run("interrupted child listing", func(t *testing.T) {
		fixture := &inventoryFixture{
			directories: map[string][]client.FileInfo{"/data": {{Name: "child", IsDir: true, HasMode: true, Mode: 0o755, ResourceID: "dir"}}},
			stats:       map[string]client.BatchStatResult{"/data/child/": targetStat("/data/child/", EntryDirectory, 0, "")},
			listFailure: "/data/child",
		}
		result, err := newTargetScannerFixture(t, fixture, "/data").Scan(context.Background(), nil)
		if err == nil || result.Complete || len(result.Entries) != 0 {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	})

	t.Run("revision changed after list", func(t *testing.T) {
		fixture := &inventoryFixture{
			directories: map[string][]client.FileInfo{"/data": {{Name: "file", Revision: 1, HasMode: true, Mode: 0o100644, ResourceID: "file"}}},
			stats:       map[string]client.BatchStatResult{"/data/file": targetStat("/data/file", EntryRegular, 2, "")},
		}
		result, err := newTargetScannerFixture(t, fixture, "/data").Scan(context.Background(), nil)
		if !errors.Is(err, ErrTargetChanged) || result.Complete {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	})

	t.Run("per-path status remains typed", func(t *testing.T) {
		fixture := &inventoryFixture{
			directories: map[string][]client.FileInfo{"/data": {{Name: "file", HasMode: true, Mode: 0o100644}}},
			stats: map[string]client.BatchStatResult{
				"/data/file": {Path: "/data/file", Status: http.StatusTooManyRequests, Error: "retry"},
			},
		}
		result, err := newTargetScannerFixture(t, fixture, "/data").Scan(context.Background(), nil)
		var statusErr *client.StatusError
		if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTooManyRequests || result.Complete {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	})
}

func TestBuildRoundClassifiesDiffsAndMissingChecksum(t *testing.T) {
	checksumA := strings.Repeat("a", 64)
	checksumB := strings.Repeat("b", 64)
	source := ScanResult{Complete: true, Entries: map[string]SourceEntry{
		"/source-only": {Path: "/source-only", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 1}, Mode: 0o644},
		"/content":     {Path: "/content", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 4}, Mode: 0o644, ChecksumSHA256: checksumA},
		"/metadata":    {Path: "/metadata", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 4}, Mode: 0o600, ChecksumSHA256: checksumA},
		"/type":        {Path: "/type", Kind: EntryDirectory, Version: SourceVersion{Kind: EntryDirectory}, Mode: 0o755},
		"/identity":    {Path: "/identity", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 4}, Mode: 0o644, ChecksumSHA256: checksumA},
		"/revision":    {Path: "/revision", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 4}, Mode: 0o644, ChecksumSHA256: checksumA},
	}}
	target := TargetScan{Complete: true, Entries: map[string]TargetEntry{
		"/target-only": {Path: "/target-only", Kind: EntryRegular, Revision: 1, ResourceID: "extra", HasMode: true, Mode: 0o644},
		"/content":     {Path: "/content", Kind: EntryRegular, Size: 4, Revision: 1, ResourceID: "content", Nlink: 1, HasMode: true, Mode: 0o644, ChecksumSHA256: checksumB},
		"/metadata":    {Path: "/metadata", Kind: EntryRegular, Size: 4, Revision: 1, ResourceID: "metadata", Nlink: 1, HasMode: true, Mode: 0o644, ChecksumSHA256: checksumA},
		"/type":        {Path: "/type", Kind: EntryRegular, Revision: 1, ResourceID: "type", HasMode: true, Mode: 0o644},
		"/identity":    {Path: "/identity", Kind: EntryRegular, Size: 4, Revision: 1, HasMode: true, Mode: 0o644},
		"/revision":    {Path: "/revision", Kind: EntryRegular, Size: 4, ResourceID: "revision", Nlink: 1, HasMode: true, Mode: 0o644},
	}}
	round, err := BuildRound("round", RoundModeFull, time.Now(), source, target)
	if err != nil {
		t.Fatal(err)
	}
	want := map[FindingKind]bool{
		FindingSourceOnly: true, FindingTargetOnly: true, FindingContent: true,
		FindingMetadata: true, FindingType: true, FindingIdentity: true, FindingRevision: true,
	}
	for _, finding := range round.Findings {
		delete(want, finding.Kind)
	}
	if len(want) != 0 || round.Converged || !round.ScanComplete {
		t.Fatalf("missing findings=%v round=%+v", want, round)
	}
}

func TestIncompleteScanNeverBuildsDeleteAndObservedDoesNotReconcile(t *testing.T) {
	sourceEntry := SourceEntry{Path: "/file", Kind: EntryRegular, Version: SourceVersion{Device: 1, Inode: 2, Kind: EntryRegular, Size: 4}, Mode: 0o644}
	round, err := BuildRound("bad", RoundModeFull, time.Now(), ScanResult{Complete: true, Entries: map[string]SourceEntry{"/file": sourceEntry}}, TargetScan{
		Complete: false,
		Entries:  map[string]TargetEntry{"/extra": {Path: "/extra", Kind: EntryRegular}},
	})
	if err == nil || round.ScanComplete || len(round.Findings) != 0 {
		t.Fatalf("incomplete round=%+v error=%v", round, err)
	}

	state := NewState(PhaseDualWriteRepairing)
	state.SetRecoveryComplete(true)
	state.MarkReconciled("/file", SourceVersion{Device: 1, Inode: 1, Kind: EntryRegular, Size: 4})
	goodTarget := TargetScan{Complete: true, Entries: map[string]TargetEntry{
		"/file": {Path: "/file", Kind: EntryRegular, Size: 4, Mode: 0o644, HasMode: true, Revision: 1, ResourceID: "file", Nlink: 1},
	}}
	round, err = BuildRound("one", RoundModeFull, time.Now(), ScanResult{Complete: true, Entries: map[string]SourceEntry{"/file": sourceEntry}}, goodTarget)
	if err != nil {
		t.Fatal(err)
	}
	round.Converged = true
	state.BeginRound(round.ID, round.Mode, round.StartedAt)
	if err := state.PublishRound(round); err != nil {
		t.Fatal(err)
	}
	snapshot := state.Snapshot()
	if snapshot.Conditions.CurrentConverged || snapshot.Observed["/file"] != sourceEntry.Version || snapshot.Reconciled["/file"] == sourceEntry.Version {
		t.Fatalf("ordinary observation advanced reconciliation: %+v", snapshot)
	}
	state.MarkReconciled("/file", sourceEntry.Version)
	round.ID = "two"
	state.BeginRound(round.ID, round.Mode, round.StartedAt)
	if err := state.PublishRound(round); err != nil {
		t.Fatal(err)
	}
	if !state.Snapshot().Conditions.CurrentConverged {
		t.Fatal("reconciled complete round did not converge")
	}
}

func TestDiffHardlinkIdentityIsDeterministic(t *testing.T) {
	source := map[string]SourceEntry{
		"/a": {Path: "/a", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 1}, Mode: 0o644, HardlinkKey: "1:2"},
		"/b": {Path: "/b", Kind: EntryRegular, Version: SourceVersion{Kind: EntryRegular, Size: 1}, Mode: 0o644, HardlinkKey: "1:2"},
	}
	target := map[string]TargetEntry{
		"/a": {Path: "/a", Kind: EntryRegular, Size: 1, Mode: 0o644, HasMode: true, Revision: 1, ResourceID: "one", Nlink: 1},
		"/b": {Path: "/b", Kind: EntryRegular, Size: 1, Mode: 0o644, HasMode: true, Revision: 1, ResourceID: "two", Nlink: 1},
	}
	for range 20 {
		findings := diffSnapshots(source, target)
		if len(findings) != 1 || findings[0].Path != "/b" || findings[0].Kind != FindingIdentity {
			t.Fatalf("identity findings=%+v", findings)
		}
	}
}

func TestTargetScannerRejectsEscapingServerName(t *testing.T) {
	fixture := &inventoryFixture{directories: map[string][]client.FileInfo{"/data": {{Name: "../escape"}}}, stats: map[string]client.BatchStatResult{}}
	result, err := newTargetScannerFixture(t, fixture, "/data").Scan(context.Background(), nil)
	if err == nil || result.Complete {
		t.Fatalf("unsafe server path result=%+v error=%v", result, err)
	}
}
