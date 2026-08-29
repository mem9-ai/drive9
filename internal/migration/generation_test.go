package migration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryGenerationObject struct {
	body     []byte
	revision int64
}

type memoryGenerationObjects struct {
	mu            sync.Mutex
	objects       map[string]memoryGenerationObject
	directories   map[string]struct{}
	putErrorAfter map[string]error
}

func newMemoryGenerationObjects() *memoryGenerationObjects {
	return &memoryGenerationObjects{objects: make(map[string]memoryGenerationObject), directories: make(map[string]struct{}), putErrorAfter: make(map[string]error)}
}

func (s *memoryGenerationObjects) EnsureDirectory(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.directories[path] = struct{}{}
	return nil
}

func (s *memoryGenerationObjects) Put(_ context.Context, path string, body []byte, expectedRevision int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.objects[path]
	if expectedRevision == 0 && exists || expectedRevision > 0 && (!exists || current.revision != expectedRevision) {
		return 0, fmt.Errorf("revision conflict")
	}
	revision := int64(1)
	if exists {
		revision = current.revision + 1
	}
	s.objects[path] = memoryGenerationObject{body: append([]byte(nil), body...), revision: revision}
	if err := s.putErrorAfter[path]; err != nil {
		delete(s.putErrorAfter, path)
		return 0, err
	}
	return revision, nil
}

func (s *memoryGenerationObjects) Get(_ context.Context, path string, maxBytes int64) ([]byte, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, exists := s.objects[path]
	if !exists {
		return nil, 0, fmt.Errorf("not found")
	}
	if int64(len(object.body)) > maxBytes {
		return nil, 0, fmt.Errorf("too large")
	}
	return append([]byte(nil), object.body...), object.revision, nil
}

func (s *memoryGenerationObjects) List(_ context.Context, directory string) ([]generationObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	children := make(map[string]bool)
	for path := range s.directories {
		if path == directory || !strings.HasPrefix(path, directory) {
			continue
		}
		rest := strings.TrimSuffix(strings.TrimPrefix(path, directory), "/")
		if rest != "" && !strings.Contains(rest, "/") {
			children[rest] = true
		}
	}
	for path := range s.objects {
		if !strings.HasPrefix(path, directory) {
			continue
		}
		rest := strings.TrimPrefix(path, directory)
		if rest != "" && !strings.Contains(rest, "/") {
			children[rest] = false
		}
	}
	names := make([]string, 0, len(children))
	for name := range children {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]generationObjectInfo, len(names))
	for index, name := range names {
		result[index] = generationObjectInfo{Name: name, Directory: children[name]}
	}
	return result, nil
}

func (s *memoryGenerationObjects) DeleteFile(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, path)
	return nil
}

func (s *memoryGenerationObjects) DeleteDirectory(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.directories, path)
	return nil
}

func TestGenerationPublishLoadAndValidate(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	body, descriptor := testChunk(t)
	if err := store.SaveChunk(ctx, "generation-a", stageSource, "source-000001", body, descriptor); err != nil {
		t.Fatal(err)
	}
	metadata := testGenerationMetadata(descriptor)
	if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishComplete(ctx, metadata); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadComplete(ctx, metadata.GenerationID, metadata.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.GenerationID != metadata.GenerationID || len(loaded.Stages[stageSource].Chunks) != 1 {
		t.Fatalf("loaded = %+v", loaded)
	}
}

func TestGenerationPublishAdoptsExactCommitUnknown(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	body, descriptor := testChunk(t)
	if err := store.SaveChunk(ctx, "generation-a", stageSource, "source-000001", body, descriptor); err != nil {
		t.Fatal(err)
	}
	metadata := testGenerationMetadata(descriptor)
	if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
		t.Fatal(err)
	}
	objects.putErrorAfter[store.completePath(metadata.GenerationID)] = errors.New("response lost")
	if err := store.PublishComplete(ctx, metadata); err != nil {
		t.Fatalf("publish exact commit unknown: %v", err)
	}
}

func TestGenerationCannotPublishIncompleteStage(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	_, descriptor := testChunk(t)
	metadata := testGenerationMetadata(descriptor)
	stage := metadata.Stages[stageSource]
	stage.Complete = false
	metadata.Stages[stageSource] = stage
	if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishComplete(ctx, metadata); !errors.Is(err, ErrGenerationInvalid) {
		t.Fatalf("publish error = %v, want ErrGenerationInvalid", err)
	}
}

func TestGenerationCleanupIsScopedToJobVerification(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	body, descriptor := testChunk(t)
	if err := store.SaveChunk(ctx, "generation-a", stageSource, descriptor.ID, body, descriptor); err != nil {
		t.Fatal(err)
	}
	metadata := testGenerationMetadata(descriptor)
	if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishComplete(ctx, metadata); err != nil {
		t.Fatal(err)
	}
	checkpointPath := ControlPrefix + "/jobs/job-a/checkpoint.json"
	objects.objects[checkpointPath] = memoryGenerationObject{body: []byte("checkpoint"), revision: 1}
	if err := store.CleanupVerification(ctx); err != nil {
		t.Fatal(err)
	}
	for path := range objects.objects {
		if strings.HasPrefix(path, ControlPrefix+"/jobs/job-a/verification/") {
			t.Fatalf("verification artifact survived cleanup: %s", path)
		}
	}
	if _, exists := objects.objects[checkpointPath]; !exists {
		t.Fatal("cleanup removed checkpoint")
	}
}

func TestGenerationPruneRequiresDurableReplacementCompleteMarker(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	body, descriptor := testChunk(t)
	old := testGenerationMetadata(descriptor)
	old.GenerationID = "generation-old"
	old.RoundID = "round-old"
	if err := store.SaveChunk(ctx, old.GenerationID, stageSource, descriptor.ID, body, descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveMetadata(ctx, old, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishComplete(ctx, old); err != nil {
		t.Fatal(err)
	}

	replacement := testGenerationMetadata(descriptor)
	replacement.GenerationID = "generation-new"
	replacement.RoundID = "round-new"
	if err := store.SaveChunk(ctx, replacement.GenerationID, stageSource, descriptor.ID, body, descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveMetadata(ctx, replacement, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.PruneReplaced(ctx, replacement); !errors.Is(err, ErrGenerationIncomplete) {
		t.Fatalf("prune without complete marker = %v, want ErrGenerationIncomplete", err)
	}
	if _, exists := objects.objects[store.completePath(old.GenerationID)]; !exists {
		t.Fatal("old generation was pruned before replacement became complete")
	}

	if err := store.PublishComplete(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := store.PruneReplaced(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.objects[store.completePath(old.GenerationID)]; exists {
		t.Fatal("old generation survived replacement prune")
	}
	if _, exists := objects.objects[store.completePath(replacement.GenerationID)]; !exists {
		t.Fatal("replacement generation was pruned")
	}
}

func TestGenerationDiscoveryFindsLatestCompleteSourceAndResumableTarget(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	body, descriptor := testChunk(t)
	for index, generationID := range []string{"source-old", "source-new"} {
		if err := store.SaveChunk(ctx, generationID, stageSource, descriptor.ID, body, descriptor); err != nil {
			t.Fatal(err)
		}
		metadata := testGenerationMetadata(descriptor)
		metadata.GenerationID = generationID
		metadata.RoundID = "round-" + generationID
		metadata.CreatedAt = time.Unix(int64(index+1), 0).UTC()
		if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
			t.Fatal(err)
		}
		if err := store.PublishComplete(ctx, metadata); err != nil {
			t.Fatal(err)
		}
	}
	partial := generationMetadata{
		FormatVersion: generationFormatVersion, GenerationID: "target-partial", RoundID: "round-partial",
		Phase: PhaseSyncing, Identity: testGenerationMetadata(descriptor).Identity,
		ManifestCursor: "cursor-1", ManifestPages: 1, CreatedAt: time.Unix(3, 0).UTC(),
		Stages: map[generationStage]generationStageMetadata{stageTargetRaw: {Complete: false}},
	}
	if _, err := store.SaveMetadata(ctx, partial, 0); err != nil {
		t.Fatal(err)
	}
	latest, err := store.FindLatestCompleteSource(ctx, partial.Identity)
	if err != nil || latest == nil || latest.GenerationID != "source-new" {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	resume, err := store.FindResumableTarget(ctx, partial.Identity)
	if err != nil || resume == nil || resume.GenerationID != "target-partial" || resume.ManifestCursor != "cursor-1" {
		t.Fatalf("resume=%+v err=%v", resume, err)
	}
}

func TestGenerationFailsClosedForIncompleteCorruptAndMismatchedIdentity(t *testing.T) {
	ctx := context.Background()
	body, descriptor := testChunk(t)
	for _, tc := range []struct {
		name   string
		mutate func(*memoryGenerationObjects, *generationStore, *generationIdentity)
		want   error
	}{
		{name: "missing complete", mutate: func(objects *memoryGenerationObjects, store *generationStore, _ *generationIdentity) {
			delete(objects.objects, store.completePath("generation-a"))
		}, want: ErrGenerationIncomplete},
		{name: "corrupt chunk", mutate: func(objects *memoryGenerationObjects, store *generationStore, _ *generationIdentity) {
			path := store.chunkPath("generation-a", stageSource, "source-000001")
			object := objects.objects[path]
			object.body[0] ^= 0xff
			objects.objects[path] = object
		}, want: ErrGenerationInvalid},
		{name: "missing chunk", mutate: func(objects *memoryGenerationObjects, store *generationStore, _ *generationIdentity) {
			delete(objects.objects, store.chunkPath("generation-a", stageSource, "source-000001"))
		}, want: ErrGenerationInvalid},
		{name: "mismatched identity", mutate: func(_ *memoryGenerationObjects, _ *generationStore, expected *generationIdentity) {
			expected.ConfigHash = "other"
		}, want: ErrGenerationMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := newMemoryGenerationObjects()
			store, err := newGenerationStore(objects, "job-a")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.SaveChunk(ctx, "generation-a", stageSource, "source-000001", body, descriptor); err != nil {
				t.Fatal(err)
			}
			metadata := testGenerationMetadata(descriptor)
			if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
				t.Fatal(err)
			}
			if err := store.PublishComplete(ctx, metadata); err != nil {
				t.Fatal(err)
			}
			expected := metadata.Identity
			tc.mutate(objects, store, &expected)
			_, err = store.LoadComplete(ctx, metadata.GenerationID, expected)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func testGenerationMetadata(descriptor chunkDescriptor) generationMetadata {
	identity := generationIdentity{
		JobID: "job-a", ConfigHash: "config-a", VolumeID: "vol-a", EBSRoot: "/ebs", SourceSubpath: "/",
		SourceRoot: "/ebs", Endpoint: "https://drive9.example.com", SpaceRef: "space-a", Prefix: "/",
	}
	descriptor.Stage = stageSource
	return generationMetadata{
		FormatVersion: generationFormatVersion,
		GenerationID:  "generation-a",
		RoundID:       "round-a",
		Phase:         PhaseSyncing,
		Identity:      identity,
		CreatedAt:     time.Unix(100, 0).UTC(),
		Stages: map[generationStage]generationStageMetadata{
			stageSource: {Complete: true, RecordCount: descriptor.RecordCount, Chunks: []chunkDescriptor{descriptor}},
		},
	}
}
