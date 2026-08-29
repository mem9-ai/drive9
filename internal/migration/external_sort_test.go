package migration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

type sliceGenerationReader struct {
	records []generationRecord
	index   int
	errAt   int
}

func (r *sliceGenerationReader) Next() (generationRecord, bool, error) {
	if r.errAt > 0 && r.index == r.errAt {
		return generationRecord{}, false, errors.New("input failed")
	}
	if r.index >= len(r.records) {
		return generationRecord{}, false, nil
	}
	record := r.records[r.index]
	r.index++
	return record, true, nil
}

func TestExternalSortMultiPassProducesGlobalOrderWithinBudget(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	var records []generationRecord
	for index := 29; index >= 0; index-- {
		path := fmt.Sprintf("/file-%02d", index)
		records = append(records, generationRecord{Key: path, Source: &sourceGenerationRecord{Path: path, Kind: EntryRegular}})
	}
	sorter, err := newExternalSorter(store, externalSortConfig{
		GenerationID: "generation-a", Stage: stageSource, Kind: recordSource,
		MaxBufferBytes: 512, FanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := sorter.Sort(ctx, &sliceGenerationReader{records: records})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.InputRecords != int64(len(records)) || result.Stats.InitialRuns < 3 || result.Stats.MergePasses < 2 ||
		result.Stats.PeakBufferBytes > 512 || len(result.Chunks) == 0 {
		t.Fatalf("result = %+v", result)
	}
	got := readSortedChunks(t, ctx, store, "generation-a", result.Chunks)
	want := make([]string, len(records))
	for index := range want {
		want[index] = fmt.Sprintf("/file-%02d", index)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paths = %v, want %v", got, want)
	}
}

func TestExternalSortRejectsDuplicatesAcrossRuns(t *testing.T) {
	objects := newMemoryGenerationObjects()
	store, err := newGenerationStore(objects, "job-a")
	if err != nil {
		t.Fatal(err)
	}
	records := []generationRecord{
		{Key: "/same", Source: &sourceGenerationRecord{Path: "/same", Kind: EntryRegular, LocalPath: "/first"}},
		{Key: "/other", Source: &sourceGenerationRecord{Path: "/other", Kind: EntryRegular}},
		{Key: "/same", Source: &sourceGenerationRecord{Path: "/same", Kind: EntryRegular, LocalPath: "/second"}},
	}
	sorter, err := newExternalSorter(store, externalSortConfig{
		GenerationID: "generation-a", Stage: stageSource, Kind: recordSource,
		MaxBufferBytes: 180, FanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sorter.Sort(context.Background(), &sliceGenerationReader{records: records}); !errors.Is(err, ErrDuplicateGenerationKey) {
		t.Fatalf("error = %v, want ErrDuplicateGenerationKey", err)
	}
}

func TestExternalSortFailsClosedOnInputErrorCancellationAndOversizedRecord(t *testing.T) {
	newSorter := func(t *testing.T, maxBytes int64) *externalSorter {
		t.Helper()
		store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
		if err != nil {
			t.Fatal(err)
		}
		sorter, err := newExternalSorter(store, externalSortConfig{
			GenerationID: "generation-a", Stage: stageSource, Kind: recordSource,
			MaxBufferBytes: maxBytes, FanIn: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		return sorter
	}
	records := []generationRecord{
		{Key: "/a", Source: &sourceGenerationRecord{Path: "/a", Kind: EntryRegular}},
		{Key: "/b", Source: &sourceGenerationRecord{Path: "/b", Kind: EntryRegular}},
	}
	if _, err := newSorter(t, 512).Sort(context.Background(), &sliceGenerationReader{records: records, errAt: 1}); err == nil {
		t.Fatal("input failure was ignored")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newSorter(t, 512).Sort(canceled, &sliceGenerationReader{records: records}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	if _, err := newSorter(t, 16).Sort(context.Background(), &sliceGenerationReader{records: records}); err == nil {
		t.Fatal("record larger than sort budget was accepted")
	}
}

func readSortedChunks(t *testing.T, ctx context.Context, store *generationStore, generationID string, descriptors []chunkDescriptor) []string {
	t.Helper()
	var paths []string
	for _, descriptor := range descriptors {
		reader, err := store.OpenChunk(ctx, generationID, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		for {
			record, ok, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			paths = append(paths, record.Key)
		}
	}
	return paths
}
