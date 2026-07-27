package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunWorkloadTouchesEverySpaceAndVerifiesContent(t *testing.T) {
	t.Parallel()

	type tenantData struct {
		files   map[string][]byte
		puts    int
		gets    int
		deletes int
	}
	var (
		mu      sync.Mutex
		tenants = map[string]*tenantData{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		data := tenants[key]
		if data == nil {
			data = &tenantData{files: map[string][]byte{}}
			tenants[key] = data
		}
		switch r.Method {
		case http.MethodPut:
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				mu.Unlock()
				return
			}
			data.files[r.URL.Path] = raw
			data.puts++
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			raw, ok := data.files[r.URL.Path]
			if !ok {
				mu.Unlock()
				http.NotFound(w, r)
				return
			}
			data.gets++
			_, _ = w.Write(raw)
		case http.MethodDelete:
			if _, ok := data.files[r.URL.Path]; !ok {
				mu.Unlock()
				http.NotFound(w, r)
				return
			}
			delete(data.files, r.URL.Path)
			data.deletes++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		mu.Unlock()
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.WorkersPerSpace = 1
	cfg.FilesPerWorker = 2
	cfg.FileSize = 128
	cfg.DeleteEvery = 1
	cfg.ReportInterval = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	spaces := []spaceCredential{
		{TenantID: "tenant-1", APIKey: "key-1"},
		{TenantID: "tenant-2", APIKey: "key-2"},
	}
	run, err := runWorkload(ctx, cfg, spaces, server.Client(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("runWorkload: %v", err)
	}
	if run.Stats.WriteSuccess < 2 || run.Stats.ReadSuccess < 2 || run.Stats.DeleteSuccess < 2 {
		t.Fatalf("stats = %#v", run.Stats)
	}
	if run.Stats.WriteErrors != 0 ||
		run.Stats.ReadErrors != 0 ||
		run.Stats.DeleteErrors != 0 ||
		run.Stats.VerificationErrors != 0 {
		t.Fatalf("unexpected errors: %#v", run.Stats)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, key := range []string{"key-1", "key-2"} {
		if tenants[key] == nil ||
			tenants[key].puts == 0 ||
			tenants[key].gets == 0 ||
			tenants[key].deletes == 0 {
			t.Fatalf("tenant %q traffic = %#v", key, tenants[key])
		}
	}
}

func TestRunWorkloadRecordsVerificationMismatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	var deletes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
		case http.MethodGet:
			_, _ = w.Write(make([]byte, 64))
		case http.MethodDelete:
			deletes.Add(1)
		}
	}))
	defer server.Close()
	defer cancel()

	cfg := testBenchConfig(t, server.URL)
	cfg.FileSize = 64
	cfg.DeleteEvery = 1
	cfg.ReportInterval = time.Hour
	spaces := []spaceCredential{{TenantID: "tenant-1", APIKey: "key-1"}}
	run, err := runWorkload(ctx, cfg, spaces, server.Client(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("runWorkload: %v", err)
	}
	if run.Stats.VerificationErrors == 0 {
		t.Fatalf("verification errors = 0, stats = %#v", run.Stats)
	}
	if deletes.Load() != 0 || run.Stats.DeleteRequests != 0 {
		t.Fatalf("delete requests = server:%d stats:%d, want 0",
			deletes.Load(), run.Stats.DeleteRequests)
	}
}

func TestRunWorkloadRejectsEmptySpaces(t *testing.T) {
	t.Parallel()

	cfg := testBenchConfig(t, "https://drive9.example.com")
	_, err := runWorkload(
		context.Background(),
		cfg,
		nil,
		http.DefaultClient,
		io.Discard,
		io.Discard,
	)
	if err == nil || !strings.Contains(err.Error(), "no spaces") {
		t.Fatalf("error = %v, want no spaces", err)
	}
}

func TestRunWorkloadSpaceStartRPSDefersLaterSpacesAndCancelsPromptly(t *testing.T) {
	var (
		mu      sync.Mutex
		touched = map[string]int{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		touched[key]++
		mu.Unlock()
		http.Error(w, "test failure", http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.SpaceStartRPS = 0.5
	cfg.ReportInterval = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	spaces := []spaceCredential{
		{TenantID: "tenant-1", APIKey: "key-1"},
		{TenantID: "tenant-2", APIKey: "key-2"},
		{TenantID: "tenant-3", APIKey: "key-3"},
	}
	started := time.Now()
	if _, err := runWorkload(
		ctx,
		cfg,
		spaces,
		server.Client(),
		io.Discard,
		io.Discard,
	); err != nil {
		t.Fatalf("runWorkload: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("runWorkload took %s after cancellation, want less than 1s", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if touched["key-1"] == 0 {
		t.Fatalf("first space was not activated: %#v", touched)
	}
	if touched["key-2"] != 0 || touched["key-3"] != 0 {
		t.Fatalf("later spaces activated before their slots: %#v", touched)
	}
}

func TestPrintWorkloadErrorDeltas(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	stats := workloadStatsSnapshot{
		WriteErrors:  3,
		ReadErrors:   2,
		DeleteErrors: 4,
		lastWriteError: &workloadErrorSample{
			TenantID:    "tenant-write",
			WorkerIndex: 2,
			RemotePath:  "/write.bin",
			Message:     "write returned HTTP 500: backend unavailable",
		},
		lastReadError: &workloadErrorSample{
			TenantID:    "tenant-read",
			WorkerIndex: 3,
			RemotePath:  "/read.bin",
			Message:     "read request: timeout",
		},
		lastDeleteError: &workloadErrorSample{
			TenantID:    "tenant-delete",
			WorkerIndex: 4,
			RemotePath:  "/delete.bin",
			Message:     "delete returned HTTP 500: backend unavailable",
		},
	}
	var (
		output   bytes.Buffer
		reported workloadErrorReportState
	)
	printWorkloadErrorDeltas(&output, now, 10*time.Second, stats, &reported)
	got := output.String()
	for _, want := range []string{
		"time=2026-07-24T00:00:00Z",
		"operation=write",
		"new_errors=3",
		"total_errors=3",
		`tenant="tenant-write"`,
		"worker=2",
		`path="/write.bin"`,
		`error="write returned HTTP 500: backend unavailable"`,
		"operation=read",
		"new_errors=2",
		"total_errors=2",
		"operation=delete",
		"new_errors=4",
		"total_errors=4",
		`tenant="tenant-delete"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("error output missing %q:\n%s", want, got)
		}
	}

	lengthAfterFirstReport := output.Len()
	printWorkloadErrorDeltas(&output, now.Add(time.Second), 11*time.Second, stats, &reported)
	if output.Len() != lengthAfterFirstReport {
		t.Fatalf("unchanged counters produced duplicate output:\n%s", output.String())
	}

	stats.WriteErrors = 5
	stats.lastWriteError = &workloadErrorSample{
		TenantID:    "tenant-write-2",
		WorkerIndex: 4,
		RemotePath:  "/write-2.bin",
		Message:     "write returned HTTP 429: rate limited",
	}
	printWorkloadErrorDeltas(&output, now.Add(2*time.Second), 12*time.Second, stats, &reported)
	delta := output.String()[lengthAfterFirstReport:]
	for _, want := range []string{
		"operation=write",
		"new_errors=2",
		"total_errors=5",
		`tenant="tenant-write-2"`,
		"HTTP 429",
	} {
		if !strings.Contains(delta, want) {
			t.Fatalf("delta output missing %q:\n%s", want, delta)
		}
	}
	if strings.Contains(delta, "operation=read") {
		t.Fatalf("unchanged read errors were logged again:\n%s", delta)
	}
}

func TestPrintWorkloadProgressIncludesDeleteAndCountsItsIOPS(t *testing.T) {
	t.Parallel()

	stats := workloadStatsSnapshot{
		WriteRequests:  2,
		WriteSuccess:   2,
		ReadRequests:   2,
		ReadSuccess:    2,
		DeleteRequests: 2,
		DeleteSuccess:  1,
		DeleteErrors:   1,
	}
	var output bytes.Buffer
	printWorkloadProgress(&output, 2*time.Second, 1, 1, 1, stats)
	got := output.String()
	for _, want := range []string{
		"active_spaces=1/1",
		"write=2/0",
		"read=2/0",
		"delete=1/1",
		"ops_per_second=3.00",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("progress missing %q:\n%s", want, got)
		}
	}
}

func TestRunWorkloadFlushesSanitizedErrorOnShutdown(t *testing.T) {
	t.Parallel()

	const (
		publicSecret  = "public-super-secret"
		privateSecret = "private-super-secret"
		spaceSecret   = "space-super-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(
			w,
			"backend failed "+publicSecret+" "+privateSecret+" "+spaceSecret,
			http.StatusInternalServerError,
		)
	}))
	defer server.Close()

	cfg := testBenchConfig(t, server.URL)
	cfg.PublicKey = publicSecret
	cfg.PrivateKey = privateSecret
	cfg.ReportInterval = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var errorOutput bytes.Buffer
	run, err := runWorkload(
		ctx,
		cfg,
		[]spaceCredential{{TenantID: "tenant-1", APIKey: spaceSecret}},
		server.Client(),
		io.Discard,
		&errorOutput,
	)
	if err != nil {
		t.Fatalf("runWorkload: %v", err)
	}
	if run.Stats.WriteErrors == 0 {
		t.Fatalf("write errors = 0, stats = %#v", run.Stats)
	}
	got := errorOutput.String()
	for _, want := range []string{
		"operation=write",
		`tenant="tenant-1"`,
		"worker=0",
		`path="/bench/drive9-space-bench/worker-0/file-`,
		"write returned HTTP 500",
		"[REDACTED]",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("error output missing %q:\n%s", want, got)
		}
	}
	for _, secret := range []string{publicSecret, privateSecret, spaceSecret} {
		if strings.Contains(got, secret) {
			t.Fatalf("error output exposed secret %q:\n%s", secret, got)
		}
	}
}
