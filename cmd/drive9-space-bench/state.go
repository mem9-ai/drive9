package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const spaceStateSchema = "drive9-space-bench-spaces/v1"

type spaceCredential struct {
	TenantID      string    `json:"tenant_id"`
	APIKey        string    `json:"api_key"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	SpendingLimit int64     `json:"tidbcloud_spending_limit"`
}

type spaceState struct {
	SchemaVersion string            `json:"schema_version"`
	Server        string            `json:"server"`
	Spaces        []spaceCredential `json:"spaces"`
}

func loadSpaceState(path, server string) (spaceState, bool, error) {
	empty := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        strings.TrimRight(strings.TrimSpace(server), "/"),
		Spaces:        []spaceCredential{},
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return empty, false, nil
		}
		return spaceState{}, false, fmt.Errorf("stat space credential file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return spaceState{}, true, fmt.Errorf(
			"space credential file permissions are %o; require 0600",
			info.Mode().Perm(),
		)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return spaceState{}, true, fmt.Errorf("read space credential file: %w", err)
	}
	var state spaceState
	if err := json.Unmarshal(raw, &state); err != nil {
		return spaceState{}, true, fmt.Errorf("decode space credential file: %w", err)
	}
	if err := validateSpaceState(state, server); err != nil {
		return spaceState{}, true, err
	}
	state.Server = strings.TrimRight(strings.TrimSpace(state.Server), "/")
	if state.Spaces == nil {
		state.Spaces = []spaceCredential{}
	}
	return state, true, nil
}

func validateSpaceState(state spaceState, requestedServer string) error {
	if state.SchemaVersion != spaceStateSchema {
		return fmt.Errorf(
			"space credential file schema_version is %q; require %q",
			state.SchemaVersion,
			spaceStateSchema,
		)
	}
	stateServer := strings.TrimRight(strings.TrimSpace(state.Server), "/")
	if stateServer == "" {
		return fmt.Errorf("space credential file server is empty")
	}
	requestedServer = strings.TrimRight(strings.TrimSpace(requestedServer), "/")
	if requestedServer != "" && stateServer != requestedServer {
		return fmt.Errorf(
			"space credential file server %q does not match requested server %q",
			stateServer,
			requestedServer,
		)
	}
	tenantIDs := make(map[string]struct{}, len(state.Spaces))
	apiKeys := make(map[string]struct{}, len(state.Spaces))
	for index, space := range state.Spaces {
		tenantID := strings.TrimSpace(space.TenantID)
		apiKey := strings.TrimSpace(space.APIKey)
		if tenantID == "" {
			return fmt.Errorf("space credential entry %d has an empty tenant_id", index)
		}
		if apiKey == "" {
			return fmt.Errorf("space credential entry %d has an empty api_key", index)
		}
		if _, ok := tenantIDs[tenantID]; ok {
			return fmt.Errorf("space credential entry %d has duplicate tenant_id %q", index, tenantID)
		}
		if _, ok := apiKeys[apiKey]; ok {
			return fmt.Errorf("space credential entry %d has a duplicate api_key", index)
		}
		if space.SpendingLimit < 0 {
			return fmt.Errorf("space credential entry %d has a negative spending limit", index)
		}
		tenantIDs[tenantID] = struct{}{}
		apiKeys[apiKey] = struct{}{}
	}
	return nil
}

func saveSpaceState(path string, state spaceState) error {
	state.Server = strings.TrimRight(strings.TrimSpace(state.Server), "/")
	if state.Spaces == nil {
		state.Spaces = []spaceCredential{}
	}
	if err := validateSpaceState(state, state.Server); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create space credential directory: %w", err)
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode space credential file: %w", err)
	}
	raw = append(raw, '\n')

	tmp, err := os.CreateTemp(dir, ".spaces-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary space credential file: %w", err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure temporary space credential file: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		return fmt.Errorf("write temporary space credential file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary space credential file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary space credential file: %w", err)
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace space credential file: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure space credential file: %w", err)
	}
	return nil
}
