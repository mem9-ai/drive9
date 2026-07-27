package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInventoryWriterAppendsSecureJSONLines(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bench", "spaces.jsonl")
	writer, err := openInventoryWriter(path)
	if err != nil {
		t.Fatalf("openInventoryWriter: %v", err)
	}
	records := []inventoryRecord{
		testInventoryRecord("tenant-1", "key-1", "active"),
		testInventoryRecord("tenant-2", "key-2", "failed"),
	}
	for _, record := range records {
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %04o, want 0600", got)
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat parent: %v", err)
	}
	if got := parent.Mode().Perm(); got&0o077 != 0 {
		t.Fatalf("parent mode = %04o, want no group/other permissions", got)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	var got []inventoryRecord
	for scanner.Scan() {
		var record inventoryRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode line: %v", err)
		}
		got = append(got, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("records = %#v, want %#v", got, records)
	}
}

func TestInventoryWriterRefusesExistingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(privateTempDir(t), "spaces.jsonl")
	if err := os.WriteFile(path, []byte("existing\n"), 0o600); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	_, err := openInventoryWriter(path)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want already exists", err)
	}
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read inventory: %v", readErr)
	}
	if string(raw) != "existing\n" {
		t.Fatalf("existing inventory changed: %q", raw)
	}
}

func TestInventoryWriterRejectsNonPrivateDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod parent: %v", err)
	}
	_, err := openInventoryWriter(filepath.Join(dir, "spaces.jsonl"))
	if err == nil || !strings.Contains(err.Error(), "require 0700") {
		t.Fatalf("error = %v, want require 0700", err)
	}
}

func TestInventoryWriterFlushesAndSyncsPeriodically(t *testing.T) {
	t.Parallel()

	path := filepath.Join(privateTempDir(t), "spaces.jsonl")
	writer, err := openInventoryWriter(path)
	if err != nil {
		t.Fatalf("openInventoryWriter: %v", err)
	}
	for index := 0; index < inventoryRecordsPerSync; index++ {
		record := testInventoryRecord(
			"tenant-"+itoa(index),
			"key-"+itoa(index),
			"active",
		)
		record.Sequence = index
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile before close: %v", err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != inventoryRecordsPerSync {
		t.Fatalf("flushed lines = %d, want %d", lines, inventoryRecordsPerSync)
	}
	if writer.pending != 0 {
		t.Fatalf("pending records after sync = %d, want 0", writer.pending)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestInventoryWriterMakesEachRecordVisibleBeforeSync(t *testing.T) {
	t.Parallel()

	path := filepath.Join(privateTempDir(t), "spaces.jsonl")
	writer, err := openInventoryWriter(path)
	if err != nil {
		t.Fatalf("openInventoryWriter: %v", err)
	}
	if err := writer.Append(testInventoryRecord("tenant-1", "key-1", "active")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile before sync: %v", err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("visible lines = %d, want 1", lines)
	}
	if writer.pending != 1 {
		t.Fatalf("pending records before sync = %d, want 1", writer.pending)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSpaceSamplerIsDeterministicAndActiveOnly(t *testing.T) {
	t.Parallel()

	forward := newSpaceSampler(10, "fixed-seed")
	reverse := newSpaceSampler(10, "fixed-seed")
	records := make([]inventoryRecord, 0, 101)
	for index := 0; index < 100; index++ {
		records = append(records, testInventoryRecord(
			"tenant-"+itoa(index),
			"key-"+itoa(index),
			"active",
		))
	}
	records = append(records, testInventoryRecord("tenant-inactive", "key-inactive", "failed"))
	for _, record := range records {
		forward.Offer(record)
	}
	for index := len(records) - 1; index >= 0; index-- {
		reverse.Offer(records[index])
	}

	gotForward := forward.Spaces()
	gotReverse := reverse.Spaces()
	if len(gotForward) != 10 {
		t.Fatalf("sample size = %d, want 10", len(gotForward))
	}
	if !reflect.DeepEqual(gotForward, gotReverse) {
		t.Fatalf("sample depends on input order:\nforward=%#v\nreverse=%#v", gotForward, gotReverse)
	}
	for _, space := range gotForward {
		if space.TenantID == "tenant-inactive" {
			t.Fatal("inactive space entered sample")
		}
	}
}

func TestWriteSelectedSnapshotUsesSpaceBenchSchemaAndDoesNotOverwrite(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bench", "spaces-15k.json")
	spaces := []selectedSpaceCredential{{
		TenantID:      "tenant-1",
		APIKey:        "key-1",
		CreatedAt:     time.Date(2026, 7, 24, 1, 2, 3, 0, time.UTC),
		SpendingLimit: 10000,
	}}
	if err := writeSelectedSnapshot(path, "https://drive9.example.com/", spaces); err != nil {
		t.Fatalf("writeSelectedSnapshot: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var snapshot selectedSpaceState
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.SchemaVersion != selectedSpaceStateSchema {
		t.Fatalf("schema = %q", snapshot.SchemaVersion)
	}
	if snapshot.Server != "https://drive9.example.com" {
		t.Fatalf("server = %q", snapshot.Server)
	}
	if !reflect.DeepEqual(snapshot.Spaces, spaces) {
		t.Fatalf("spaces = %#v, want %#v", snapshot.Spaces, spaces)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %04o, want 0600", info.Mode().Perm())
	}

	err = writeSelectedSnapshot(path, "https://drive9.example.com", spaces)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second write error = %v, want already exists", err)
	}
}

func testInventoryRecord(tenantID, apiKey, status string) inventoryRecord {
	limit := int64(10000)
	return inventoryRecord{
		SchemaVersion:    inventorySchema,
		Sequence:         1,
		Server:           "https://drive9.example.com",
		TenantID:         tenantID,
		APIKey:           apiKey,
		CreatedAt:        time.Date(2026, 7, 24, 1, 2, 3, 0, time.UTC),
		SpendingLimit:    &limit,
		InitialStatus:    "provisioning",
		FinalStatus:      status,
		ProvisionSeconds: 1,
		ReadySeconds:     2,
		StatusRequests:   3,
		Active:           status == "active",
	}
}

func itoa(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	index := len(buf)
	for value > 0 {
		index--
		buf[index] = digits[value%10]
		value /= 10
	}
	return string(buf[index:])
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("secure temporary directory: %v", err)
	}
	return dir
}
