package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestRunMainSampleCombinesInventories(t *testing.T) {
	t.Parallel()

	dir := privateTempDir(t)
	firstPath := filepath.Join(dir, "first.jsonl")
	secondPath := filepath.Join(dir, "second.jsonl")
	outPath := filepath.Join(dir, "spaces-4.json")

	shared := testInventoryRecord("tenant-shared", "key-shared", "active")
	writeSampleInventory(t, firstPath, []inventoryRecord{
		testInventoryRecord("tenant-a", "key-a", "active"),
		testInventoryRecord("tenant-inactive", "key-inactive", "failed"),
		shared,
	})
	writeSampleInventory(t, secondPath, []inventoryRecord{
		testInventoryRecord("tenant-b", "key-b", "active"),
		testInventoryRecord("tenant-c", "key-c", "active"),
		shared,
	})

	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"sample",
			"--inventory", firstPath,
			"--inventory", secondPath,
			"--sample-size", "4",
			"--sample-seed", "fixed-seed",
			"--out", outPath,
		},
		emptyEnv,
		func() (string, error) {
			return "", errors.New("sample mode must not resolve the home directory")
		},
		&stdout,
		&stderr,
	)
	if exitCode != 0 {
		t.Fatalf(
			"exit code = %d, stdout=%q stderr=%q",
			exitCode,
			stdout.String(),
			stderr.String(),
		)
	}
	for _, want := range []string{
		"inventories=2",
		"records=6",
		"active=5",
		"duplicates=1",
		"selected=4",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q: %q", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String()+stderr.String(), "key-") {
		t.Fatalf("command output contains an API key: stdout=%q stderr=%q",
			stdout.String(), stderr.String())
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read sample output: %v", err)
	}
	var snapshot selectedSpaceState
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode sample output: %v", err)
	}
	if snapshot.SchemaVersion != selectedSpaceStateSchema {
		t.Fatalf("schema version = %q", snapshot.SchemaVersion)
	}
	if snapshot.Server != "https://drive9.example.com" {
		t.Fatalf("server = %q", snapshot.Server)
	}
	gotTenantIDs := make([]string, 0, len(snapshot.Spaces))
	for _, space := range snapshot.Spaces {
		gotTenantIDs = append(gotTenantIDs, space.TenantID)
	}
	sort.Strings(gotTenantIDs)
	wantTenantIDs := []string{
		"tenant-a",
		"tenant-b",
		"tenant-c",
		"tenant-shared",
	}
	if strings.Join(gotTenantIDs, ",") != strings.Join(wantTenantIDs, ",") {
		t.Fatalf("tenant IDs = %v, want %v", gotTenantIDs, wantTenantIDs)
	}
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat sample output: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("sample output mode = %04o, want 0600", got)
	}

	reversedOutPath := filepath.Join(dir, "spaces-4-reversed.json")
	stdout.Reset()
	stderr.Reset()
	exitCode = runMain(
		[]string{
			"sample",
			"--inventory", secondPath,
			"--inventory", firstPath,
			"--sample-size", "4",
			"--sample-seed", "fixed-seed",
			"--out", reversedOutPath,
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 0 {
		t.Fatalf(
			"reversed exit code = %d, stdout=%q stderr=%q",
			exitCode,
			stdout.String(),
			stderr.String(),
		)
	}
	reversedRaw, err := os.ReadFile(reversedOutPath)
	if err != nil {
		t.Fatalf("read reversed sample output: %v", err)
	}
	if string(reversedRaw) != string(raw) {
		t.Fatalf(
			"sample depends on inventory order:\nforward=%s\nreversed=%s",
			raw,
			reversedRaw,
		)
	}
}

func TestRunMainSampleRejectsMismatchedServers(t *testing.T) {
	t.Parallel()

	dir := privateTempDir(t)
	firstPath := filepath.Join(dir, "first.jsonl")
	secondPath := filepath.Join(dir, "second.jsonl")
	outPath := filepath.Join(dir, "spaces.json")
	writeSampleInventory(t, firstPath, []inventoryRecord{
		testInventoryRecord("tenant-a", "key-a", "active"),
	})
	second := testInventoryRecord("tenant-b", "key-b", "active")
	second.Server = "https://other.example.com"
	writeSampleInventory(t, secondPath, []inventoryRecord{second})

	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"sample",
			"--inventory", firstPath,
			"--inventory", secondPath,
			"--sample-size", "1",
			"--out", outPath,
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 1 {
		t.Fatalf(
			"exit code = %d, want 1; stdout=%q stderr=%q",
			exitCode,
			stdout.String(),
			stderr.String(),
		)
	}
	if !strings.Contains(stderr.String(), "server mismatch") {
		t.Fatalf("stderr = %q, want server mismatch", stderr.String())
	}
	if _, err := os.Stat(outPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sample output stat error = %v, want not exist", err)
	}
}

func TestRunMainSampleRejectsConflictingTenantCredentials(t *testing.T) {
	t.Parallel()

	dir := privateTempDir(t)
	firstPath := filepath.Join(dir, "first.jsonl")
	secondPath := filepath.Join(dir, "second.jsonl")
	outPath := filepath.Join(dir, "spaces.json")
	writeSampleInventory(t, firstPath, []inventoryRecord{
		testInventoryRecord("tenant-shared", "key-first", "active"),
	})
	writeSampleInventory(t, secondPath, []inventoryRecord{
		testInventoryRecord("tenant-shared", "key-second", "active"),
	})

	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"sample",
			"--inventory", firstPath,
			"--inventory", secondPath,
			"--sample-size", "1",
			"--out", outPath,
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1; stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "conflicting credentials for tenant") {
		t.Fatalf("stderr = %q, want credential conflict", stderr.String())
	}
	if strings.Contains(stderr.String(), "key-first") ||
		strings.Contains(stderr.String(), "key-second") {
		t.Fatalf("stderr contains an API key: %q", stderr.String())
	}
}

func TestRunMainSampleRejectsInsufficientActiveSpaces(t *testing.T) {
	t.Parallel()

	dir := privateTempDir(t)
	inventoryPath := filepath.Join(dir, "spaces.jsonl")
	outPath := filepath.Join(dir, "spaces.json")
	writeSampleInventory(t, inventoryPath, []inventoryRecord{
		testInventoryRecord("tenant-active", "key-active", "active"),
		testInventoryRecord("tenant-failed", "key-failed", "failed"),
	})

	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"sample",
			"--inventory", inventoryPath,
			"--sample-size", "2",
			"--out", outPath,
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1; stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "only 1 unique active spaces available") {
		t.Fatalf("stderr = %q, want insufficient active spaces", stderr.String())
	}
	if _, err := os.Stat(outPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sample output stat error = %v, want not exist", err)
	}
}

func TestRunMainSampleRejectsMalformedInventory(t *testing.T) {
	t.Parallel()

	dir := privateTempDir(t)
	inventoryPath := filepath.Join(dir, "spaces.jsonl")
	outPath := filepath.Join(dir, "spaces.json")
	if err := os.WriteFile(inventoryPath, []byte("{not-json}\n"), 0o600); err != nil {
		t.Fatalf("write malformed inventory: %v", err)
	}

	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"sample",
			"--inventory", inventoryPath,
			"--sample-size", "1",
			"--out", outPath,
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1; stderr=%q", exitCode, stderr.String())
	}
	for _, want := range []string{"line 1", "decode record"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want %q", stderr.String(), want)
		}
	}
	if _, err := os.Stat(outPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sample output stat error = %v, want not exist", err)
	}
}

func TestRunMainSampleValidatesFlags(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "inventory required",
			args: []string{"sample", "--sample-size", "1", "--out", "spaces.json"},
			want: "inventory is required",
		},
		{
			name: "positive sample size required",
			args: []string{"sample", "--inventory", "spaces.jsonl", "--out", "spaces.json"},
			want: "sample-size must be positive",
		},
		{
			name: "output required",
			args: []string{"sample", "--inventory", "spaces.jsonl", "--sample-size", "1"},
			want: "out is required",
		},
		{
			name: "sample seed required",
			args: []string{
				"sample",
				"--inventory", "spaces.jsonl",
				"--sample-size", "1",
				"--sample-seed", "",
				"--out", "spaces.json",
			},
			want: "sample-seed must not be empty",
		},
		{
			name: "output differs from input",
			args: []string{
				"sample",
				"--inventory", "spaces.jsonl",
				"--sample-size", "1",
				"--out", "spaces.jsonl",
			},
			want: "out and inventory must use different paths",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			exitCode := runMain(
				tc.args,
				emptyEnv,
				func() (string, error) { return t.TempDir(), nil },
				&stdout,
				&stderr,
			)
			if exitCode != 2 {
				t.Fatalf(
					"exit code = %d, want 2; stdout=%q stderr=%q",
					exitCode,
					stdout.String(),
					stderr.String(),
				)
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
		})
	}
}

func writeSampleInventory(t *testing.T, path string, records []inventoryRecord) {
	t.Helper()
	writer, err := openInventoryWriter(path)
	if err != nil {
		t.Fatalf("open inventory writer: %v", err)
	}
	for _, record := range records {
		if err := writer.Append(record); err != nil {
			t.Fatalf("append inventory record: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close inventory writer: %v", err)
	}
}
