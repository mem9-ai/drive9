package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadSpaceStateMissingCreatesEmptyState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bench", "spaces.json")
	state, exists, err := loadSpaceState(path, "https://drive9.example.com")
	if err != nil {
		t.Fatalf("loadSpaceState: %v", err)
	}
	if exists {
		t.Fatal("missing state reported as existing")
	}
	if state.SchemaVersion != spaceStateSchema || state.Server != "https://drive9.example.com" {
		t.Fatalf("state = %#v", state)
	}
	if len(state.Spaces) != 0 {
		t.Fatalf("spaces = %d", len(state.Spaces))
	}
}

func TestSaveAndLoadSpaceState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bench", "spaces.json")
	want := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        "https://drive9.example.com",
		Spaces: []spaceCredential{{
			TenantID:      "tenant-1",
			APIKey:        "drive9-secret",
			CreatedAt:     time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC),
			SpendingLimit: 10000,
		}},
	}
	if err := saveSpaceState(path, want); err != nil {
		t.Fatalf("saveSpaceState: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat state: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("state mode = %o, want 600", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("state dir mode = %o, want 700", got)
	}

	got, exists, err := loadSpaceState(path, want.Server)
	if err != nil {
		t.Fatalf("loadSpaceState: %v", err)
	}
	if !exists {
		t.Fatal("saved state reported as missing")
	}
	if len(got.Spaces) != 1 || got.Spaces[0] != want.Spaces[0] {
		t.Fatalf("spaces = %#v", got.Spaces)
	}
}

func TestLoadSpaceStateRejectsServerMismatch(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "spaces.json")
	raw, err := json.Marshal(spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        "https://one.example.com",
		Spaces:        []spaceCredential{},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	_, _, err = loadSpaceState(path, "https://two.example.com")
	if err == nil || !strings.Contains(err.Error(), "server") {
		t.Fatalf("error = %v, want server mismatch", err)
	}
}

func TestLoadSpaceStateRejectsDuplicateTenant(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "spaces.json")
	state := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        "https://drive9.example.com",
		Spaces: []spaceCredential{
			{TenantID: "tenant-1", APIKey: "key-1", SpendingLimit: 10000},
			{TenantID: "tenant-1", APIKey: "key-2", SpendingLimit: 10000},
		},
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	_, _, err = loadSpaceState(path, state.Server)
	if err == nil || !strings.Contains(err.Error(), "duplicate tenant_id") {
		t.Fatalf("error = %v, want duplicate tenant error", err)
	}
}

func TestLoadSpaceStateDoesNotExposeAPIKeyInValidationError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "spaces.json")
	const secret = "drive9-super-secret"
	raw := `{"schema_version":"drive9-space-bench-spaces/v1","server":"https://drive9.example.com","spaces":[{"tenant_id":"","api_key":"` + secret + `"}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	_, _, err := loadSpaceState(path, "https://drive9.example.com")
	if err == nil {
		t.Fatal("expected validation error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error exposed API key: %v", err)
	}
}
