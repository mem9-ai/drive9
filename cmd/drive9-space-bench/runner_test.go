package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunBenchmarkReusesStoredSpaceWithoutCloudCredentials(t *testing.T) {
	t.Parallel()

	var (
		provisionRequests atomic.Int64
		fsRequests        atomic.Int64
		mu                sync.Mutex
		files             = map[string][]byte{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer stored-key" {
			http.Error(w, "bad auth", http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/v1/provision":
			provisionRequests.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		case r.URL.Path == "/v1/status":
			_, _ = io.WriteString(w, `{"status":"active"}`)
		case strings.HasPrefix(r.URL.Path, "/v1/fs/") && r.Method == http.MethodPut:
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				return
			}
			mu.Lock()
			files[r.URL.Path] = raw
			mu.Unlock()
			fsRequests.Add(1)
		case strings.HasPrefix(r.URL.Path, "/v1/fs/") && r.Method == http.MethodGet:
			mu.Lock()
			raw := append([]byte(nil), files[r.URL.Path]...)
			mu.Unlock()
			_, _ = w.Write(raw)
			fsRequests.Add(1)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.PublicKey = ""
	cfg.PrivateKey = ""
	cfg.SpaceCount = 1
	cfg.Duration = 50 * time.Millisecond
	cfg.ReportInterval = time.Hour
	cfg.SpacesFile = filepath.Join(t.TempDir(), "bench", "spaces.json")
	state := spaceState{
		SchemaVersion: spaceStateSchema,
		Server:        server.URL,
		Spaces: []spaceCredential{{
			TenantID:      "stored-tenant",
			APIKey:        "stored-key",
			SpendingLimit: 10000,
		}},
	}
	if err := saveSpaceState(cfg.SpacesFile, state); err != nil {
		t.Fatalf("saveSpaceState: %v", err)
	}

	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		io.Discard,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if provisionRequests.Load() != 0 {
		t.Fatalf("provision requests = %d, want 0", provisionRequests.Load())
	}
	if fsRequests.Load() == 0 {
		t.Fatal("workload sent no filesystem requests")
	}
	if report.Spaces.Reused != 1 || report.Spaces.Provisioned != 0 || report.Spaces.Ready != 1 {
		t.Fatalf("space summary = %#v", report.Spaces)
	}
	if report.StopReason != "duration" {
		t.Fatalf("stop reason = %q, want duration", report.StopReason)
	}
}
