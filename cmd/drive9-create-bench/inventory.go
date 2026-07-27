package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	inventorySchema          = "drive9-space-inventory/v1"
	selectedSpaceStateSchema = "drive9-space-bench-spaces/v1"
	inventoryRecordsPerSync  = 100
)

type inventoryRecord struct {
	SchemaVersion    string    `json:"schema_version"`
	Sequence         int       `json:"sequence"`
	Server           string    `json:"server"`
	TenantID         string    `json:"tenant_id"`
	APIKey           string    `json:"api_key"`
	CreatedAt        time.Time `json:"created_at"`
	SpendingLimit    *int64    `json:"tidbcloud_spending_limit,omitempty"`
	InitialStatus    string    `json:"initial_status"`
	FinalStatus      string    `json:"final_status"`
	ProvisionSeconds float64   `json:"provision_seconds"`
	ReadySeconds     float64   `json:"ready_seconds,omitempty"`
	StatusRequests   int       `json:"status_requests"`
	Active           bool      `json:"active"`
	Error            string    `json:"error,omitempty"`
}

type inventoryWriter struct {
	file    *os.File
	buffer  *bufio.Writer
	encoder *json.Encoder
	pending int
	closed  bool
}

func openInventoryWriter(path string) (*inventoryWriter, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("inventory path is required")
	}
	dir := filepath.Dir(path)
	if err := ensurePrivateDirectory(dir, "inventory"); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("inventory already exists: %s", path)
		}
		return nil, fmt.Errorf("create inventory: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("secure inventory: %w", err)
	}
	buffer := bufio.NewWriterSize(file, 256*1024)
	return &inventoryWriter{
		file:    file,
		buffer:  buffer,
		encoder: json.NewEncoder(buffer),
	}, nil
}

func (w *inventoryWriter) Append(record inventoryRecord) error {
	if w == nil || w.closed {
		return fmt.Errorf("inventory writer is closed")
	}
	if err := validateInventoryRecord(record); err != nil {
		return err
	}
	if err := w.encoder.Encode(record); err != nil {
		return fmt.Errorf("append inventory record: %w", err)
	}
	if err := w.flush(); err != nil {
		return err
	}
	w.pending++
	if w.pending < inventoryRecordsPerSync {
		return nil
	}
	return w.sync()
}

func (w *inventoryWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	flushErr := w.flushAndSync()
	closeErr := w.file.Close()
	return errors.Join(flushErr, closeErr)
}

func (w *inventoryWriter) flushAndSync() error {
	flushErr := w.flush()
	syncErr := w.sync()
	return errors.Join(flushErr, syncErr)
}

func (w *inventoryWriter) flush() error {
	if err := w.buffer.Flush(); err != nil {
		return fmt.Errorf("flush inventory: %w", err)
	}
	return nil
}

func (w *inventoryWriter) sync() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync inventory: %w", err)
	}
	w.pending = 0
	return nil
}

func validateInventoryRecord(record inventoryRecord) error {
	if record.SchemaVersion != inventorySchema {
		return fmt.Errorf("inventory record schema is %q; require %q", record.SchemaVersion, inventorySchema)
	}
	if record.Sequence < 0 {
		return fmt.Errorf("inventory record sequence must not be negative")
	}
	if strings.TrimSpace(record.Server) == "" {
		return fmt.Errorf("inventory record server is required")
	}
	if strings.TrimSpace(record.TenantID) == "" {
		return fmt.Errorf("inventory record tenant_id is required")
	}
	if strings.TrimSpace(record.APIKey) == "" {
		return fmt.Errorf("inventory record api_key is required")
	}
	if record.CreatedAt.IsZero() {
		return fmt.Errorf("inventory record created_at is required")
	}
	return nil
}

type selectedSpaceCredential struct {
	TenantID      string    `json:"tenant_id"`
	APIKey        string    `json:"api_key"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	SpendingLimit int64     `json:"tidbcloud_spending_limit"`
}

type selectedSpaceState struct {
	SchemaVersion string                    `json:"schema_version"`
	Server        string                    `json:"server"`
	Spaces        []selectedSpaceCredential `json:"spaces"`
}

type sampleCandidate struct {
	rank  [sha256.Size]byte
	space selectedSpaceCredential
}

type candidateMaxHeap []sampleCandidate

func (h candidateMaxHeap) Len() int { return len(h) }

func (h candidateMaxHeap) Less(left, right int) bool {
	return compareCandidate(h[left], h[right]) > 0
}

func (h candidateMaxHeap) Swap(left, right int) {
	h[left], h[right] = h[right], h[left]
}

func (h *candidateMaxHeap) Push(value any) {
	*h = append(*h, value.(sampleCandidate))
}

func (h *candidateMaxHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

type spaceSampler struct {
	size       int
	seedPrefix []byte
	candidates candidateMaxHeap
}

func newSpaceSampler(size int, seed string) *spaceSampler {
	return &spaceSampler{
		size:       max(0, size),
		seedPrefix: []byte(seed + "\x00"),
	}
}

func (s *spaceSampler) Offer(record inventoryRecord) {
	if s == nil || s.size == 0 || !record.Active || record.FinalStatus != "active" {
		return
	}
	if strings.TrimSpace(record.TenantID) == "" || strings.TrimSpace(record.APIKey) == "" {
		return
	}
	limit := int64(0)
	if record.SpendingLimit != nil {
		limit = *record.SpendingLimit
	}
	rankInput := make([]byte, 0, len(s.seedPrefix)+len(record.TenantID))
	rankInput = append(rankInput, s.seedPrefix...)
	rankInput = append(rankInput, record.TenantID...)
	candidate := sampleCandidate{
		rank: sha256.Sum256(rankInput),
		space: selectedSpaceCredential{
			TenantID:      record.TenantID,
			APIKey:        record.APIKey,
			CreatedAt:     record.CreatedAt,
			SpendingLimit: limit,
		},
	}
	if len(s.candidates) < s.size {
		heap.Push(&s.candidates, candidate)
		return
	}
	if compareCandidate(candidate, s.candidates[0]) >= 0 {
		return
	}
	s.candidates[0] = candidate
	heap.Fix(&s.candidates, 0)
}

func (s *spaceSampler) Spaces() []selectedSpaceCredential {
	if s == nil {
		return nil
	}
	ordered := append(candidateMaxHeap(nil), s.candidates...)
	sort.Slice(ordered, func(left, right int) bool {
		return compareCandidate(ordered[left], ordered[right]) < 0
	})
	spaces := make([]selectedSpaceCredential, len(ordered))
	for index := range ordered {
		spaces[index] = ordered[index].space
	}
	return spaces
}

func compareCandidate(left, right sampleCandidate) int {
	if compared := bytes.Compare(left.rank[:], right.rank[:]); compared != 0 {
		return compared
	}
	return strings.Compare(left.space.TenantID, right.space.TenantID)
}

func writeSelectedSnapshot(path, server string, spaces []selectedSpaceCredential) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("selected snapshot path is required")
	}
	dir := filepath.Dir(path)
	if err := ensurePrivateDirectory(dir, "selected snapshot"); err != nil {
		return err
	}
	state := selectedSpaceState{
		SchemaVersion: selectedSpaceStateSchema,
		Server:        strings.TrimRight(strings.TrimSpace(server), "/"),
		Spaces:        append([]selectedSpaceCredential(nil), spaces...),
	}
	tmp, err := os.CreateTemp(dir, ".spaces-selected-*.tmp")
	if err != nil {
		return fmt.Errorf("create selected snapshot temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure selected snapshot temporary file: %w", err)
	}
	encoder := json.NewEncoder(tmp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("encode selected snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync selected snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close selected snapshot: %w", err)
	}
	closed = true
	if err := os.Link(tmpPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("selected snapshot already exists: %s", path)
		}
		return fmt.Errorf("publish selected snapshot: %w", err)
	}
	return nil
}

func ensurePrivateDirectory(path, outputName string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create %s directory: %w", outputName, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect %s directory: %w", outputName, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s parent is not a directory: %s", outputName, path)
	}
	if permissions := info.Mode().Perm(); permissions != 0o700 {
		return fmt.Errorf(
			"%s directory permissions are %04o; require 0700: %s",
			outputName,
			permissions,
			path,
		)
	}
	return nil
}
