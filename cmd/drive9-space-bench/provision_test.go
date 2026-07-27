package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProvisionSpaceSendsCredentialsAndDefaultSpendingLimit(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/provision" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body["public_key"] != "public-ak" || body["private_key"] != "private-sk" {
			t.Errorf("credentials = %#v", body)
		}
		if body["tidbcloud_spending_limit"] != float64(10000) {
			t.Errorf("spending limit = %#v", body["tidbcloud_spending_limit"])
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"tenant_id":"tenant-1","api_key":"space-api-key","status":"provisioning"}`)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	createdAt := time.Date(2026, 7, 23, 2, 3, 4, 0, time.UTC)
	space, err := provisionSpace(context.Background(), cfg, server.Client(), func() time.Time {
		return createdAt
	})
	if err != nil {
		t.Fatalf("provisionSpace: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", requests.Load())
	}
	if space.TenantID != "tenant-1" || space.APIKey != "space-api-key" {
		t.Fatalf("space = %#v", space)
	}
	if !space.CreatedAt.Equal(createdAt) || space.SpendingLimit != 10000 {
		t.Fatalf("space metadata = %#v", space)
	}
}

func TestProvisionSpaceDoesNotRetry(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	_, err := provisionSpace(context.Background(), cfg, server.Client(), time.Now)
	if err == nil {
		t.Fatal("expected provision error")
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want one non-retried POST", requests.Load())
	}
}

func TestEnsureSpaceCountReusesExistingAndPersistsShortfall(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		index := requests.Add(1) + 1
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w,
			`{"tenant_id":"tenant-%d","api_key":"space-key-%d","status":"provisioning"}`,
			index, index)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.SpaceCount = 3
	cfg.SpacesFile = filepath.Join(t.TempDir(), "bench", "spaces.json")
	state := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        server.URL,
		Spaces: []spaceCredential{{
			TenantID:      "tenant-1",
			APIKey:        "space-key-1",
			SpendingLimit: 10000,
		}},
	}
	got, err := ensureSpaceCount(
		context.Background(),
		cfg,
		state,
		server.Client(),
		time.Now,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("ensureSpaceCount: %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("provision requests = %d, want 2", requests.Load())
	}
	if len(got.Spaces) != 3 {
		t.Fatalf("spaces = %d, want 3", len(got.Spaces))
	}

	persisted, exists, err := loadSpaceState(cfg.SpacesFile, server.URL)
	if err != nil {
		t.Fatalf("load persisted state: %v", err)
	}
	if !exists || len(persisted.Spaces) != 3 {
		t.Fatalf("persisted spaces = %d, exists = %t", len(persisted.Spaces), exists)
	}
}

func TestEnsureSpaceCountRequiresCredentialsOnlyForShortfall(t *testing.T) {
	t.Parallel()

	cfg := testBenchConfig(t, "https://drive9.example.com")
	cfg.PublicKey = ""
	cfg.PrivateKey = ""
	cfg.SpaceCount = 1
	cfg.SpacesFile = filepath.Join(t.TempDir(), "spaces.json")

	full := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        cfg.Server,
		Spaces: []spaceCredential{{
			TenantID: "tenant-1", APIKey: "key-1", SpendingLimit: 10000,
		}},
	}
	if _, err := ensureSpaceCount(context.Background(), cfg, full, nil, time.Now, io.Discard); err != nil {
		t.Fatalf("full state unexpectedly required credentials: %v", err)
	}

	empty := spaceState{SchemaVersion: spaceStateSchema, Server: cfg.Server, Spaces: []spaceCredential{}}
	_, err := ensureSpaceCount(context.Background(), cfg, empty, nil, time.Now, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("error = %v, want credentials required", err)
	}
}

func TestEnsureSpaceCountPersistsAcceptedSpacesBeforeReturningFailure(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"tenant_id":"tenant-1","api_key":"key-1","status":"provisioning"}`)
			return
		}
		http.Error(w, "provision failed", http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.SpaceCount = 2
	cfg.ProvisionConcurrency = 1
	cfg.SpacesFile = filepath.Join(t.TempDir(), "bench", "spaces.json")
	state := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        server.URL,
		Spaces:        []spaceCredential{},
	}
	got, err := ensureSpaceCount(
		context.Background(),
		cfg,
		state,
		server.Client(),
		time.Now,
		io.Discard,
	)
	if err == nil {
		t.Fatal("expected partial provisioning failure")
	}
	if len(got.Spaces) != 1 {
		t.Fatalf("returned spaces = %d, want 1", len(got.Spaces))
	}
	persisted, exists, loadErr := loadSpaceState(cfg.SpacesFile, server.URL)
	if loadErr != nil {
		t.Fatalf("load persisted state: %v", loadErr)
	}
	if !exists || len(persisted.Spaces) != 1 || persisted.Spaces[0].TenantID != "tenant-1" {
		t.Fatalf("persisted state = %#v, exists = %t", persisted, exists)
	}
}

func TestWaitForAllSpacesReadyUsesEachAPIKey(t *testing.T) {
	t.Parallel()

	seen := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		_, _ = io.WriteString(w, `{"status":"active"}`)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	spaces := []spaceCredential{
		{TenantID: "tenant-1", APIKey: "key-1"},
		{TenantID: "tenant-2", APIKey: "key-2"},
	}
	if err := waitForAllSpacesReady(context.Background(), cfg, spaces, server.Client(), io.Discard); err != nil {
		t.Fatalf("waitForAllSpacesReady: %v", err)
	}
	close(seen)
	got := map[string]bool{}
	for key := range seen {
		got[key] = true
	}
	if !got["key-1"] || !got["key-2"] {
		t.Fatalf("seen keys = %#v", got)
	}
}

func TestWaitForAllSpacesReadyDoesNotBatchWholePollingLifetimes(t *testing.T) {
	t.Parallel()

	var secondSpacePolled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		status := "active"
		switch key {
		case "key-1":
			if !secondSpacePolled.Load() {
				status = "provisioning"
			}
		case "key-2":
			secondSpacePolled.Store(true)
		default:
			http.Error(w, "bad key", http.StatusUnauthorized)
			return
		}
		_, _ = fmt.Fprintf(w, `{"status":%q}`, status)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.ProvisionConcurrency = 1
	cfg.ProvisionTimeout = 250 * time.Millisecond
	cfg.PollInterval = time.Millisecond
	spaces := []spaceCredential{
		{TenantID: "tenant-1", APIKey: "key-1"},
		{TenantID: "tenant-2", APIKey: "key-2"},
	}
	if err := waitForAllSpacesReady(context.Background(), cfg, spaces, server.Client(), io.Discard); err != nil {
		t.Fatalf("waitForAllSpacesReady: %v", err)
	}
	if !secondSpacePolled.Load() {
		t.Fatal("second space was never polled")
	}
}

func TestWaitForAllSpacesReadyIsStrictAndRedactsAPIKey(t *testing.T) {
	t.Parallel()

	const secret = "space-super-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	spaces := []spaceCredential{{TenantID: "tenant-1", APIKey: secret}}
	err := waitForAllSpacesReady(context.Background(), cfg, spaces, server.Client(), io.Discard)
	if err == nil {
		t.Fatal("expected readiness error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error exposed API key: %v", err)
	}
}

func testBenchConfig(t *testing.T, server string) benchConfig {
	t.Helper()
	cfg, err := parseConfig([]string{
		"--server", server,
		"--tidbcloud-public-key", "public-ak",
		"--tidbcloud-private-key", "private-sk",
		"--provision-rps", "1000000",
		"--poll-interval", "1ms",
		"--provision-timeout", "1s",
	}, func(string) string { return "" }, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	return cfg
}
