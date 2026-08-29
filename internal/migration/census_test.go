package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCensusBuildsCompleteGenerationAndReusesFullSourceVersion(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	bPath := filepath.Join(root, "dir", "b")
	if err := os.WriteFile(bPath, []byte("bbb"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	build := func(id string, previous *generationMetadata) censusResult {
		builder, err := newCensusBuilder(scanner, store, censusConfig{
			GenerationID: id, RoundID: "round-" + id, Phase: PhaseSyncing,
			Identity: testCensusIdentity(root), HashWorkers: 2, SortBufferBytes: 1024, SortFanIn: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := builder.Build(context.Background(), previous)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	first := build("generation-1", nil)
	if first.Metadata.EntryCount != 3 || first.Metadata.DirectoryCount != 1 || first.Metadata.FileCount != 2 ||
		first.Metadata.HashNewCount != 2 || first.Metadata.HashReuseCount != 0 || first.MaxHashInFlight > 2 {
		t.Fatalf("first = %+v", first)
	}
	second := build("generation-2", &first.Metadata)
	if second.Metadata.HashNewCount != 0 || second.Metadata.HashReuseCount != 2 {
		t.Fatalf("second hashes new/reused = %d/%d", second.Metadata.HashNewCount, second.Metadata.HashReuseCount)
	}

	before, err := os.Stat(bPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, []byte("ccc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(bPath, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	third := build("generation-3", &second.Metadata)
	if third.Metadata.HashNewCount != 1 || third.Metadata.HashReuseCount != 1 {
		t.Fatalf("third hashes new/reused = %d/%d", third.Metadata.HashNewCount, third.Metadata.HashReuseCount)
	}
	loaded, err := store.LoadComplete(context.Background(), third.Metadata.GenerationID, third.Metadata.Identity)
	if err != nil || loaded.GenerationID != third.Metadata.GenerationID {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
}

func TestCensusChecksumReuseRequiresEverySourceVersionField(t *testing.T) {
	base := sourceGenerationRecord{
		Path: "/a", Kind: EntryRegular, Device: 1, Inode: 2, Size: 3,
		MtimeNS: 4, CtimeNS: 5, VersionMode: 6, ChecksumSHA256: checksumHex([]byte("a")),
	}
	if !canReuseSourceChecksum(base, base) {
		t.Fatal("identical SourceVersion did not reuse checksum")
	}
	mutations := []func(*sourceGenerationRecord){
		func(record *sourceGenerationRecord) { record.Device++ },
		func(record *sourceGenerationRecord) { record.Inode++ },
		func(record *sourceGenerationRecord) { record.Kind = EntrySymlink },
		func(record *sourceGenerationRecord) { record.Size++ },
		func(record *sourceGenerationRecord) { record.MtimeNS++ },
		func(record *sourceGenerationRecord) { record.CtimeNS++ },
		func(record *sourceGenerationRecord) { record.VersionMode++ },
	}
	for index, mutate := range mutations {
		changed := base
		mutate(&changed)
		if canReuseSourceChecksum(changed, base) {
			t.Fatalf("mutation %d reused checksum", index)
		}
	}
}

func TestCensusSourceOrDirectoryMutationCannotPublishComplete(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Scanner, *censusBuilder, string)
	}{
		{name: "file changes during hash", mutate: func(scanner *Scanner, _ *censusBuilder, root string) {
			scanner.afterRead = func(string) { _ = os.WriteFile(filepath.Join(root, "file"), []byte("changed"), 0o600) }
		}},
		{name: "root changes before validation", mutate: func(_ *Scanner, builder *censusBuilder, root string) {
			builder.beforeDirectoryValidation = func() { _ = os.Mkdir(filepath.Join(root, "late"), 0o755) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "file"), []byte("initial"), 0o600); err != nil {
				t.Fatal(err)
			}
			scanner, err := NewScanner(root)
			if err != nil {
				t.Fatal(err)
			}
			store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
			if err != nil {
				t.Fatal(err)
			}
			builder, err := newCensusBuilder(scanner, store, censusConfig{
				GenerationID: "generation-a", RoundID: "round-a", Phase: PhaseSyncing,
				Identity: testCensusIdentity(root), HashWorkers: 1, SortBufferBytes: 1024, SortFanIn: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(scanner, builder, root)
			_, err = builder.Build(context.Background(), nil)
			if !errors.Is(err, ErrSourceChanged) {
				t.Fatalf("error = %v, want ErrSourceChanged", err)
			}
			if _, err := store.LoadComplete(context.Background(), "generation-a", testCensusIdentity(root)); !errors.Is(err, ErrGenerationIncomplete) {
				t.Fatalf("load error = %v, want ErrGenerationIncomplete", err)
			}
		})
	}
}

func testCensusIdentity(root string) generationIdentity {
	return generationIdentity{
		JobID: "job-a", ConfigHash: "config-a", VolumeID: "vol-a", EBSRoot: root,
		SourceSubpath: "/", SourceRoot: root, Endpoint: "https://drive9.example.com", SpaceRef: "space-a", Prefix: "/",
	}
}

func TestCensusCancellationLeavesGenerationIncomplete(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	builder, err := newCensusBuilder(scanner, store, censusConfig{
		GenerationID: "generation-a", RoundID: "round-a", Phase: PhaseSyncing,
		Identity: testCensusIdentity(root), HashWorkers: 1, SortBufferBytes: 1024, SortFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := builder.Build(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}
