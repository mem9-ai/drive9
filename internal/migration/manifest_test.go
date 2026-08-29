package migration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestManifestBuildsCompleteSortedGenerationAndFiltersControlPrefix(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	var mu sync.Mutex
	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		mu.Lock()
		cursors = append(cursors, cursor)
		mu.Unlock()
		switch cursor {
		case "":
			_, _ = io.WriteString(w, `{"entries":[
				{"path":"/z.txt","type":"regular","metadata_complete":true,"identity_kind":"inode","mode":420,"size":1,"checksum_sha256":"`+checksum+`","revision":1,"resource_id":"inode-z","nlink":1},
				{"path":"/.drive9-migration/","type":"directory","metadata_complete":true,"identity_kind":"inode","mode":448,"size":0,"checksum_sha256":null,"revision":1,"resource_id":"control","nlink":2}
			],"next_cursor":"cursor-1","done":false}`)
		case "cursor-1":
			_, _ = io.WriteString(w, `{"entries":[],"next_cursor":"cursor-2","done":false}`)
		case "cursor-2":
			_, _ = io.WriteString(w, `{"entries":[
				{"path":"/a/","type":"directory","metadata_complete":false,"identity_kind":"legacy_dentry","mode":null,"size":0,"checksum_sha256":null,"revision":null,"resource_id":"node-a","nlink":2}
			],"next_cursor":"","done":true}`)
		default:
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	builder, err := newManifestBuilder(client.New(server.URL, "owner-key"), store, manifestConfig{
		GenerationID: "target-a", RoundID: "round-a", Phase: PhaseSyncing,
		Identity: testManifestIdentity(), TargetPrefix: "/", PageLimit: 2, SortBufferBytes: 1024, SortFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := builder.Build(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Metadata.ManifestPages != 3 || result.Metadata.EntryCount != 2 || result.FilteredControlEntries != 1 ||
		result.Metadata.ManifestRawEntries != 3 || result.Metadata.ManifestResponseBytes == 0 ||
		result.Metadata.ManifestEmptyPages != 1 || result.Metadata.ManifestCursorAdvances != 2 ||
		result.Metadata.ManifestSortRuns == 0 || result.Metadata.ManifestLastPageAt.IsZero() {
		t.Fatalf("result = %+v", result)
	}
	paths := readTargetPaths(t, context.Background(), store, result.Metadata)
	if !slices.Equal(paths, []string{"/a", "/z.txt"}) {
		t.Fatalf("paths = %v", paths)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(cursors, []string{"", "cursor-1", "cursor-2"}) {
		t.Fatalf("cursors = %v", cursors)
	}
}

func TestManifestSyncingResumesVerifiedRawPages(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	checksum := strings.Repeat("a", 64)
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			_, _ = io.WriteString(w, manifestRegularPage("/b", "inode-b", checksum, "cursor-1", false))
			return
		}
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	firstBuilder := testManifestBuilder(t, client.New(firstServer.URL, ""), store, "target-a", PhaseSyncing)
	if _, err := firstBuilder.Build(ctx, nil); err == nil {
		t.Fatal("interrupted Manifest unexpectedly completed")
	}
	firstServer.Close()
	partial, _, err := store.LoadMetadata(ctx, "target-a", testManifestIdentity())
	if err != nil || partial.ManifestCursor != "cursor-1" || partial.ManifestPages != 1 {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}

	var resumedCursor string
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resumedCursor = r.URL.Query().Get("cursor")
		_, _ = io.WriteString(w, manifestRegularPage("/a", "inode-a", checksum, "", true))
	}))
	defer secondServer.Close()
	secondBuilder := testManifestBuilder(t, client.New(secondServer.URL, ""), store, "target-a", PhaseSyncing)
	result, err := secondBuilder.Build(ctx, &partial)
	if err != nil {
		t.Fatal(err)
	}
	if resumedCursor != "cursor-1" {
		t.Fatalf("resumed cursor = %q", resumedCursor)
	}
	if paths := readTargetPaths(t, ctx, store, result.Metadata); !slices.Equal(paths, []string{"/a", "/b"}) {
		t.Fatalf("paths = %v", paths)
	}
}

func TestManifestLivePhaseRestartsFromFirstPage(t *testing.T) {
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	partial := generationMetadata{
		FormatVersion: generationFormatVersion, GenerationID: "old-target", RoundID: "old-round",
		Phase: PhaseSyncing, Identity: testManifestIdentity(), ManifestCursor: "stale-cursor", CreatedAt: testNow,
		Stages: map[generationStage]generationStageMetadata{stageTargetRaw: {Complete: false}},
	}
	var cursor string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor = r.URL.Query().Get("cursor")
		_, _ = io.WriteString(w, `{"entries":[],"next_cursor":"","done":true}`)
	}))
	defer server.Close()
	builder := testManifestBuilder(t, client.New(server.URL, ""), store, "target-live", PhaseDualWriteRepairing)
	if _, err := builder.Build(context.Background(), &partial); err != nil {
		t.Fatal(err)
	}
	if cursor != "" {
		t.Fatalf("live phase resumed stale cursor %q", cursor)
	}
}

func TestManifestDuplicateAcrossPagesFailsClosed(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			_, _ = io.WriteString(w, manifestRegularPage("/same", "inode-a", checksum, "next", false))
			return
		}
		_, _ = io.WriteString(w, manifestRegularPage("/same", "inode-b", checksum, "", true))
	}))
	defer server.Close()
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	builder := testManifestBuilder(t, client.New(server.URL, ""), store, "target-a", PhaseSyncing)
	if _, err := builder.Build(context.Background(), nil); !errors.Is(err, ErrDuplicateGenerationKey) {
		t.Fatalf("error = %v, want ErrDuplicateGenerationKey", err)
	}
	if _, err := store.LoadComplete(context.Background(), "target-a", testManifestIdentity()); !errors.Is(err, ErrGenerationIncomplete) {
		t.Fatalf("load error = %v", err)
	}
}

var testNow = func() (value time.Time) { return time.Unix(100, 0).UTC() }()

func testManifestBuilder(t *testing.T, api manifestPageClient, store *generationStore, generationID string, phase Phase) *manifestBuilder {
	t.Helper()
	builder, err := newManifestBuilder(api, store, manifestConfig{
		GenerationID: generationID, RoundID: "round-" + generationID, Phase: phase,
		Identity: testManifestIdentity(), TargetPrefix: "/", PageLimit: 1, SortBufferBytes: 1024, SortFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder.clock = func() time.Time { return testNow }
	return builder
}

func testManifestIdentity() generationIdentity {
	return generationIdentity{
		JobID: "job-a", ConfigHash: "config-a", VolumeID: "vol-a", EBSRoot: "/ebs",
		SourceSubpath: "/", SourceRoot: "/ebs", Endpoint: "https://drive9.example.com", SpaceRef: "space-a", Prefix: "/",
	}
}

func manifestRegularPage(path, resourceID, checksum, next string, done bool) string {
	return fmt.Sprintf(`{"entries":[{"path":%q,"type":"regular","metadata_complete":true,"identity_kind":"inode","mode":420,"size":1,"checksum_sha256":%q,"revision":1,"resource_id":%q,"nlink":1}],"next_cursor":%q,"done":%t}`,
		path, checksum, resourceID, next, done)
}

func readTargetPaths(t *testing.T, ctx context.Context, store *generationStore, metadata generationMetadata) []string {
	t.Helper()
	stage := metadata.Stages[stageTarget]
	return readSortedChunks(t, ctx, store, metadata.GenerationID, stage.Chunks)
}
