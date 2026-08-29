package migration

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

var ErrDuplicateGenerationKey = errors.New("duplicate generation key")

type generationRecordReader interface {
	Next() (generationRecord, bool, error)
}

type externalSortConfig struct {
	GenerationID   string
	Stage          generationStage
	Kind           generationRecordKind
	IDPrefix       string
	MaxBufferBytes int64
	FanIn          int
	Budget         *memoryBudget
}

type externalSortStats struct {
	InputRecords    int64
	InputBytes      int64
	InitialRuns     int
	MergePasses     int
	OutputChunks    int
	PeakBufferBytes int64
}

type externalSortResult struct {
	Chunks []chunkDescriptor
	Stats  externalSortStats
}

type externalSorter struct {
	store  *generationStore
	config externalSortConfig
}

type sortRun struct {
	chunks []chunkDescriptor
}

func newExternalSorter(store *generationStore, config externalSortConfig) (*externalSorter, error) {
	if store == nil {
		return nil, fmt.Errorf("external sort requires generation store")
	}
	if err := validateGenerationIdentifier(config.GenerationID); err != nil {
		return nil, fmt.Errorf("external sort generation ID: %w", err)
	}
	if !validGenerationStage(config.Stage) || recordKindForStage(config.Stage) != config.Kind {
		return nil, fmt.Errorf("external sort stage and record kind mismatch")
	}
	if config.MaxBufferBytes <= 0 || config.MaxBufferBytes > maxChunkPayloadBytes/2 {
		return nil, fmt.Errorf("external sort buffer must be within 1..%d bytes", maxChunkPayloadBytes/2)
	}
	if config.FanIn < 2 {
		return nil, fmt.Errorf("external sort fan-in must be at least 2")
	}
	if config.IDPrefix == "" {
		config.IDPrefix = "sort"
	}
	if err := validateGenerationIdentifier(config.IDPrefix); err != nil {
		return nil, fmt.Errorf("external sort ID prefix: %w", err)
	}
	return &externalSorter{store: store, config: config}, nil
}

func (s *externalSorter) Sort(ctx context.Context, input generationRecordReader) (externalSortResult, error) {
	if input == nil {
		return externalSortResult{}, fmt.Errorf("external sort requires input")
	}
	if s.config.Budget != nil {
		reserved := s.config.MaxBufferBytes * int64(s.config.FanIn+1)
		release, err := s.config.Budget.Acquire(ctx, reserved)
		if err != nil {
			return externalSortResult{}, err
		}
		defer release()
	}
	var result externalSortResult
	var runs []sortRun
	var buffered []generationRecord
	var bufferedBytes int64
	flush := func() error {
		if len(buffered) == 0 {
			return nil
		}
		run, err := s.writeSortedRecords(ctx, buffered, 0, len(runs))
		if err != nil {
			return err
		}
		runs = append(runs, run)
		buffered = nil
		bufferedBytes = 0
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return externalSortResult{}, err
		}
		record, ok, err := input.Next()
		if err != nil {
			return externalSortResult{}, fmt.Errorf("external sort input: %w", err)
		}
		if !ok {
			break
		}
		if err := validateGenerationRecord(s.config.Kind, record); err != nil {
			return externalSortResult{}, err
		}
		recordBytes, err := generationRecordBytes(record)
		if err != nil {
			return externalSortResult{}, err
		}
		if recordBytes > s.config.MaxBufferBytes {
			return externalSortResult{}, fmt.Errorf("external sort record %q exceeds buffer budget", record.Key)
		}
		if len(buffered) > 0 && bufferedBytes+recordBytes > s.config.MaxBufferBytes {
			if err := flush(); err != nil {
				return externalSortResult{}, err
			}
		}
		buffered = append(buffered, record)
		bufferedBytes += recordBytes
		result.Stats.InputRecords++
		result.Stats.InputBytes += recordBytes
		result.Stats.PeakBufferBytes = max(result.Stats.PeakBufferBytes, bufferedBytes)
	}
	if err := flush(); err != nil {
		return externalSortResult{}, err
	}
	result.Stats.InitialRuns = len(runs)
	if len(runs) == 0 {
		return result, nil
	}
	pass := 1
	for len(runs) > s.config.FanIn {
		var next []sortRun
		for first := 0; first < len(runs); first += s.config.FanIn {
			last := min(first+s.config.FanIn, len(runs))
			if last-first == 1 {
				next = append(next, runs[first])
				continue
			}
			merged, peak, err := s.mergeRuns(ctx, runs[first:last], pass, len(next))
			if err != nil {
				return externalSortResult{}, err
			}
			result.Stats.PeakBufferBytes = max(result.Stats.PeakBufferBytes, peak)
			next = append(next, merged)
		}
		result.Stats.MergePasses++
		runs = next
		pass++
	}
	if len(runs) > 1 {
		merged, peak, err := s.mergeRuns(ctx, runs, pass, 0)
		if err != nil {
			return externalSortResult{}, err
		}
		result.Stats.PeakBufferBytes = max(result.Stats.PeakBufferBytes, peak)
		result.Stats.MergePasses++
		runs = []sortRun{merged}
	}
	result.Chunks = append([]chunkDescriptor(nil), runs[0].chunks...)
	result.Stats.OutputChunks = len(result.Chunks)
	return result, nil
}

func (s *externalSorter) writeSortedRecords(ctx context.Context, records []generationRecord, pass, runIndex int) (sortRun, error) {
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	for i := 1; i < len(records); i++ {
		if records[i-1].Key == records[i].Key {
			return sortRun{}, fmt.Errorf("%w: %s", ErrDuplicateGenerationKey, records[i].Key)
		}
	}
	return s.saveRecordChunk(ctx, records, pass, runIndex, 0)
}

func (s *externalSorter) saveRecordChunk(ctx context.Context, records []generationRecord, pass, runIndex, chunkIndex int) (sortRun, error) {
	writer, err := newChunkWriter(s.config.Kind)
	if err != nil {
		return sortRun{}, err
	}
	for i := range records {
		if err := writer.Write(records[i]); err != nil {
			return sortRun{}, err
		}
	}
	id := fmt.Sprintf("%s-p%02d-r%06d-c%06d", s.config.IDPrefix, pass, runIndex, chunkIndex)
	body, descriptor, err := writer.Close(id)
	if err != nil {
		return sortRun{}, err
	}
	descriptor.Stage = s.config.Stage
	if err := s.store.SaveChunk(ctx, s.config.GenerationID, s.config.Stage, id, body, descriptor); err != nil {
		return sortRun{}, err
	}
	return sortRun{chunks: []chunkDescriptor{descriptor}}, nil
}

func (s *externalSorter) mergeRuns(ctx context.Context, runs []sortRun, pass, runIndex int) (sortRun, int64, error) {
	readers := make([]*sortRunReader, len(runs))
	queue := mergeRecordHeap{}
	for i := range runs {
		readers[i] = &sortRunReader{ctx: ctx, store: s.store, generationID: s.config.GenerationID, chunks: runs[i].chunks}
		record, ok, err := readers[i].Next()
		if err != nil {
			return sortRun{}, 0, err
		}
		if ok {
			heap.Push(&queue, mergeRecord{record: record, run: i})
		}
	}
	var output sortRun
	var buffered []generationRecord
	var bufferedBytes, peak int64
	var lastKey string
	chunkIndex := 0
	flush := func() error {
		if len(buffered) == 0 {
			return nil
		}
		chunk, err := s.saveRecordChunk(ctx, buffered, pass, runIndex, chunkIndex)
		if err != nil {
			return err
		}
		output.chunks = append(output.chunks, chunk.chunks...)
		buffered = nil
		bufferedBytes = 0
		chunkIndex++
		return nil
	}
	for queue.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return sortRun{}, peak, err
		}
		item := heap.Pop(&queue).(mergeRecord)
		if lastKey == item.record.Key {
			return sortRun{}, peak, fmt.Errorf("%w: %s", ErrDuplicateGenerationKey, item.record.Key)
		}
		recordBytes, err := generationRecordBytes(item.record)
		if err != nil {
			return sortRun{}, peak, err
		}
		if recordBytes > s.config.MaxBufferBytes {
			return sortRun{}, peak, fmt.Errorf("external sort record %q exceeds buffer budget", item.record.Key)
		}
		if len(buffered) > 0 && bufferedBytes+recordBytes > s.config.MaxBufferBytes {
			if err := flush(); err != nil {
				return sortRun{}, peak, err
			}
		}
		buffered = append(buffered, item.record)
		bufferedBytes += recordBytes
		peak = max(peak, bufferedBytes)
		lastKey = item.record.Key
		next, ok, err := readers[item.run].Next()
		if err != nil {
			return sortRun{}, peak, err
		}
		if ok {
			heap.Push(&queue, mergeRecord{record: next, run: item.run})
		}
	}
	if err := flush(); err != nil {
		return sortRun{}, peak, err
	}
	return output, peak, nil
}

type sortRunReader struct {
	ctx          context.Context
	store        *generationStore
	generationID string
	chunks       []chunkDescriptor
	index        int
	current      *chunkReader
}

func (r *sortRunReader) Next() (generationRecord, bool, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return generationRecord{}, false, err
		}
		if r.current == nil {
			if r.index >= len(r.chunks) {
				return generationRecord{}, false, nil
			}
			reader, err := r.store.OpenChunk(r.ctx, r.generationID, r.chunks[r.index])
			if err != nil {
				return generationRecord{}, false, err
			}
			r.current = reader
			r.index++
		}
		record, ok, err := r.current.Next()
		if err != nil {
			return generationRecord{}, false, err
		}
		if ok {
			return record, true, nil
		}
		r.current = nil
	}
}

type mergeRecord struct {
	record generationRecord
	run    int
}

type mergeRecordHeap []mergeRecord

func (h mergeRecordHeap) Len() int { return len(h) }
func (h mergeRecordHeap) Less(i, j int) bool {
	if h[i].record.Key == h[j].record.Key {
		return h[i].run < h[j].run
	}
	return h[i].record.Key < h[j].record.Key
}
func (h mergeRecordHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *mergeRecordHeap) Push(value any) { *h = append(*h, value.(mergeRecord)) }
func (h *mergeRecordHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func generationRecordBytes(record generationRecord) (int64, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("encode generation record: %w", err)
	}
	if len(body) > maxGenerationRecordBytes {
		return 0, fmt.Errorf("generation record exceeds %d bytes", maxGenerationRecordBytes)
	}
	return int64(len(body) + 1), nil
}
