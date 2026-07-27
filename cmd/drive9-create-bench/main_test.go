package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	t.Run("explicit server", func(t *testing.T) {
		cfg, err := parseConfig([]string{
			"--server", "https://drive9.example.com/",
			"--out", "report.json",
			"--total", "3",
			"--concurrency", "2",
			"--rps", "4.5",
		}, emptyEnv, t.TempDir(), io.Discard)
		if err != nil {
			t.Fatalf("parseConfig: %v", err)
		}
		if cfg.Server != "https://drive9.example.com" {
			t.Fatalf("Server = %q", cfg.Server)
		}
		if cfg.Total != 3 || cfg.Concurrency != 2 || cfg.RPS != 4.5 {
			t.Fatalf("unexpected workload config: %+v", cfg)
		}
		if !cfg.WaitReady {
			t.Fatal("WaitReady = false, want true")
		}
	})

	t.Run("inventory and sampling", func(t *testing.T) {
		cfg, err := parseConfig([]string{
			"--server", "https://drive9.example.com",
			"--out", "report.json",
			"--total", "500000",
			"--inventory", "spaces.jsonl",
			"--sample-size", "15000",
			"--sample-seed", "drive9-500k-v1",
			"--sample-out", "spaces-15k.json",
			"--report-interval", "30s",
		}, emptyEnv, t.TempDir(), io.Discard)
		if err != nil {
			t.Fatalf("parseConfig: %v", err)
		}
		if cfg.Inventory != "spaces.jsonl" || cfg.SampleOut != "spaces-15k.json" {
			t.Fatalf("inventory outputs = %+v", cfg)
		}
		if cfg.SampleSize != 15000 || cfg.SampleSeed != "drive9-500k-v1" {
			t.Fatalf("sample config = %+v", cfg)
		}
		if cfg.ReportInterval != 30*time.Second {
			t.Fatalf("ReportInterval = %s", cfg.ReportInterval)
		}
	})

	t.Run("environment", func(t *testing.T) {
		env := mapEnv(map[string]string{
			envServer:     "https://drive9.example.com",
			envPublicKey:  "public",
			envPrivateKey: "private",
		})
		cfg, err := parseConfig([]string{
			"--out", "report.json",
			"--tidbcloud-spending-limit", "10",
		}, env, t.TempDir(), io.Discard)
		if err != nil {
			t.Fatalf("parseConfig: %v", err)
		}
		if cfg.PublicKey != "public" || cfg.PrivateKey != "private" {
			t.Fatalf("credentials were not loaded from the environment")
		}
		if cfg.SpendingLimit == nil || *cfg.SpendingLimit != 10 {
			t.Fatalf("SpendingLimit = %v", cfg.SpendingLimit)
		}
	})
}

func TestParseConfigLoadsDefaultConfigFile(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, ".drive9", "bench", "config.json")
	writeCreateBenchConfigFile(t, configPath, `{
  "server": "https://config.drive9.example.com/",
  "tidbcloud_public_key": "config-public",
  "tidbcloud_private_key": "config-private",
  "spaces": 500,
  "tidbcloud_spending_limit": 10000
}`, 0o600)

	cfg, err := parseConfig(
		[]string{"--out", "report.json"},
		emptyEnv,
		home,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Server != "https://config.drive9.example.com" {
		t.Fatalf("Server = %q", cfg.Server)
	}
	if cfg.PublicKey != "config-public" || cfg.PrivateKey != "config-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
	if cfg.Total != 1 || cfg.SpendingLimit != nil {
		t.Fatalf("space-bench-only config fields changed create workload: %+v", cfg)
	}
}

func TestParseConfigEnvironmentAndFlagsOverrideConfigFile(t *testing.T) {
	home := t.TempDir()
	configPath := filepath.Join(home, ".drive9", "bench", "config.json")
	writeCreateBenchConfigFile(t, configPath, `{
  "server": "https://config.drive9.example.com",
  "tidbcloud_public_key": "config-public",
  "tidbcloud_private_key": "config-private"
}`, 0o600)
	env := mapEnv(map[string]string{
		envServer:     "https://env.drive9.example.com",
		envPublicKey:  "env-public",
		envPrivateKey: "env-private",
	})

	cfg, err := parseConfig(
		[]string{
			"--server", "https://flag.drive9.example.com/",
			"--out", "report.json",
		},
		env,
		home,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Server != "https://flag.drive9.example.com" {
		t.Fatalf("Server = %q", cfg.Server)
	}
	if cfg.PublicKey != "env-public" || cfg.PrivateKey != "env-private" {
		t.Fatalf("credentials = %q/%q", cfg.PublicKey, cfg.PrivateKey)
	}
}

func TestParseConfigRejectsInvalidExplicitConfig(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")
		_, err := parseConfig(
			[]string{
				"--config", path,
				"--server", "https://drive9.example.com",
				"--out", "report.json",
			},
			emptyEnv,
			t.TempDir(),
			io.Discard,
		)
		if err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("error = %v, want missing config error", err)
		}
	})

	t.Run("insecure mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.json")
		writeCreateBenchConfigFile(
			t,
			path,
			`{"server":"https://drive9.example.com"}`,
			0o644,
		)
		_, err := parseConfig(
			[]string{"--config", path, "--out", "report.json"},
			emptyEnv,
			t.TempDir(),
			io.Discard,
		)
		if err == nil || !strings.Contains(err.Error(), "mode 0600") {
			t.Fatalf("error = %v, want mode 0600 error", err)
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.json")
		writeCreateBenchConfigFile(
			t,
			path,
			`{"server":"https://drive9.example.com","unknown":true}`,
			0o600,
		)
		_, err := parseConfig(
			[]string{"--config", path, "--out", "report.json"},
			emptyEnv,
			t.TempDir(),
			io.Discard,
		)
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("error = %v, want unknown field error", err)
		}
	})
}

func TestParseConfigRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{
			name: "missing server",
			args: []string{"--out", "report.json"},
			want: "server is required",
		},
		{
			name: "missing output",
			args: []string{"--server", "https://drive9.example.com"},
			want: "out is required",
		},
		{
			name: "partial credentials",
			args: []string{"--server", "https://drive9.example.com", "--out", "report.json"},
			env:  map[string]string{envPublicKey: "public"},
			want: "must be set together",
		},
		{
			name: "spending limit without credentials",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "report.json",
				"--tidbcloud-spending-limit", "10",
			},
			want: "requires",
		},
		{
			name: "invalid server",
			args: []string{"--server", "drive9.example.com", "--out", "report.json"},
			want: "http or https",
		},
		{
			name: "invalid total",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "report.json",
				"--total", "0",
			},
			want: "total must be positive",
		},
		{
			name: "rps interval overflow",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "report.json",
				"--rps", "1e-20",
			},
			want: "rps must",
		},
		{
			name: "server query",
			args: []string{
				"--server", "https://drive9.example.com?token=secret",
				"--out", "report.json",
			},
			want: "query or fragment",
		},
		{
			name: "sample requires inventory",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "report.json",
				"--sample-size", "1",
				"--sample-out", "spaces.json",
			},
			want: "inventory",
		},
		{
			name: "sample requires output",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "report.json",
				"--inventory", "spaces.jsonl",
				"--sample-size", "1",
			},
			want: "sample-out",
		},
		{
			name: "sample cannot exceed total",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "report.json",
				"--inventory", "spaces.jsonl",
				"--sample-size", "2",
				"--sample-out", "spaces.json",
				"--total", "1",
			},
			want: "sample-size",
		},
		{
			name: "inventory and report must differ",
			args: []string{
				"--server", "https://drive9.example.com",
				"--out", "spaces.jsonl",
				"--inventory", "spaces.jsonl",
			},
			want: "different paths",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(tt.args, mapEnv(tt.env), t.TempDir(), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestNewBenchHTTPClientSizesConnectionPoolForConcurrency(t *testing.T) {
	client := newBenchHTTPClient(benchConfig{Concurrency: 500})
	t.Cleanup(client.CloseIdleConnections)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", client.Transport)
	}
	if transport.MaxConnsPerHost != 1000 ||
		transport.MaxIdleConnsPerHost != 1000 ||
		transport.MaxIdleConns != 1000 {
		t.Fatalf(
			"connection limits = total:%d idle_host:%d host:%d",
			transport.MaxIdleConns,
			transport.MaxIdleConnsPerHost,
			transport.MaxConnsPerHost,
		)
	}
}

func TestRunBenchmarkProvisionsAndPollsUntilActive(t *testing.T) {
	var statusRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/provision":
			if r.Method != http.MethodPost {
				t.Errorf("provision method = %s", r.Method)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q", got)
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode provision body: %v", err)
			}
			if body["public_key"] != "public" || body["private_key"] != "private" {
				t.Errorf("provision body credentials = %#v", body)
			}
			if body["tidbcloud_spending_limit"] != float64(10) {
				t.Errorf("spending limit = %#v", body["tidbcloud_spending_limit"])
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"tenant_id":"tenant-1","api_key":"drive9_secret","status":"provisioning"}`)
		case "/v1/status":
			if got := r.Header.Get("Authorization"); got != "Bearer drive9_secret" {
				t.Errorf("Authorization = %q", got)
			}
			count := statusRequests.Add(1)
			status := "provisioning"
			if count >= 2 {
				status = "active"
			}
			_, _ = fmt.Fprintf(w, `{"status":%q}`, status)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	limit := int64(10)
	cfg := testConfig(server.URL)
	cfg.PublicKey = "public"
	cfg.PrivateKey = "private"
	cfg.SpendingLimit = &limit

	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		discardLogger(),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if report.Summary.Success != 1 || report.Summary.Failed != 0 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if report.Summary.ProvisionRequests != 1 || report.Summary.StatusRequests != 2 {
		t.Fatalf("request counts = %+v", report.Summary)
	}
	if len(report.Failures) != 0 {
		t.Fatalf("failure samples = %+v", report.Failures)
	}
	if report.Summary.ProvisionLatency.SampleCount != 1 {
		t.Fatalf("provision latency = %+v", report.Summary.ProvisionLatency)
	}
	if report.Summary.ReadyLatency.SampleCount != 1 {
		t.Fatalf("ready latency = %+v", report.Summary.ReadyLatency)
	}

	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	for _, secret := range []string{"public", "private", "drive9_secret"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("report contains secret %q: %s", secret, raw)
		}
	}
}

func TestRunBenchmarkNoWaitSkipsStatus(t *testing.T) {
	var statusRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/provision":
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"tenant_id":"tenant-1","api_key":"drive9_secret","status":"provisioning"}`)
		case "/v1/status":
			statusRequests.Add(1)
			_, _ = io.WriteString(w, `{"status":"active"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.WaitReady = false
	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		discardLogger(),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if report.Summary.Success != 1 || report.Summary.Failed != 0 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if got := statusRequests.Load(); got != 0 {
		t.Fatalf("status requests = %d, want 0", got)
	}
	if report.Summary.ReadyLatency.SampleCount != 0 {
		t.Fatalf("ready latency = %+v", report.Summary.ReadyLatency)
	}
}

func TestRunBenchmarkDoesNotRetryProvisionAndRedactsSecrets(t *testing.T) {
	var provisionRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provisionRequests.Add(1)
		http.Error(w, "upstream rejected private-secret", http.StatusBadGateway)
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.PublicKey = "public-secret"
	cfg.PrivateKey = "private-secret"
	var logs strings.Builder
	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		log.New(&logs, "", log.LstdFlags),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	if got := provisionRequests.Load(); got != 1 {
		t.Fatalf("provision requests = %d, want 1", got)
	}
	if report.Summary.Success != 0 || report.Summary.Failed != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	for _, secret := range []string{"public-secret", "private-secret"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("report contains secret %q: %s", secret, raw)
		}
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("stderr contains secret %q: %s", secret, logs.String())
		}
	}
	for _, want := range []string{
		"request error: operation=provision",
		"index=0",
		`tenant=""`,
		"attempt=1",
		"http_status=502",
		"latency_seconds=",
		"upstream rejected [REDACTED]",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, logs.String())
		}
	}
	assertTimestampedRequestErrors(t, logs.String(), 1)
}

func TestRunBenchmarkLogsEveryStatusRequestFailure(t *testing.T) {
	var statusRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/provision":
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(
				w,
				`{"tenant_id":"tenant-1","api_key":"drive9-secret","status":"provisioning"}`,
			)
		case "/v1/status":
			attempt := statusRequests.Add(1)
			if attempt <= 2 {
				http.Error(
					w,
					"transient failure for drive9-secret",
					http.StatusInternalServerError,
				)
				return
			}
			_, _ = io.WriteString(w, `{"status":"active"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	var logs strings.Builder
	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		log.New(&logs, "", log.LstdFlags),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if report.Summary.Success != 1 || report.Summary.StatusRequests != 3 {
		t.Fatalf("summary = %+v", report.Summary)
	}

	got := logs.String()
	if count := strings.Count(got, "request error: operation=status"); count != 2 {
		t.Fatalf("status request error logs = %d, want 2: %s", count, got)
	}
	for _, want := range []string{
		"index=0",
		`tenant="tenant-1"`,
		"attempt=1",
		"attempt=2",
		"http_status=500",
		"latency_seconds=",
		"transient failure for [REDACTED]",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stderr missing %q: %s", want, got)
		}
	}
	if strings.Contains(got, "drive9-secret") {
		t.Fatalf("stderr contains Space API key: %s", got)
	}
	assertTimestampedRequestErrors(t, got, 2)
}

func TestRunBenchmarkStopsOnTerminalStatus(t *testing.T) {
	var statusRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/provision":
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"tenant_id":"tenant-1","api_key":"drive9_secret","status":"provisioning"}`)
		case "/v1/status":
			statusRequests.Add(1)
			_, _ = io.WriteString(w, `{"status":"failed"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.PollInterval = time.Hour
	var logs strings.Builder
	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		log.New(&logs, "", log.LstdFlags),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	if got := statusRequests.Load(); got != 1 {
		t.Fatalf("status requests = %d, want 1", got)
	}
	if report.Summary.Success != 0 || report.Summary.Failed != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if len(report.Failures) != 1 {
		t.Fatalf("failure samples = %+v", report.Failures)
	}
	if got := report.Failures[0].FinalStatus; got != "failed" {
		t.Fatalf("final status = %q, want failed", got)
	}
	for _, want := range []string{
		"tenant failure: index=0",
		`tenant="tenant-1"`,
		`initial_status="provisioning"`,
		`final_status="failed"`,
		"provision_seconds=",
		"ready_seconds=",
		`error="tenant entered terminal status`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, logs.String())
		}
	}
	if countTimestampedLogKind(t, logs.String(), "tenant failure:") != 1 {
		t.Fatalf("tenant failure is not timestamped: %s", logs.String())
	}
}

func TestRunBenchmarkWritesInventoryAndSelectedSnapshot(t *testing.T) {
	var provisions atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		index := provisions.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(
			w,
			`{"tenant_id":"tenant-%d","api_key":"drive9-key-%d","status":"active"}`,
			index,
			index,
		)
	}))
	defer server.Close()

	dir := privateTempDir(t)
	limit := int64(10000)
	cfg := testConfig(server.URL)
	cfg.Total = 3
	cfg.Concurrency = 2
	cfg.Inventory = filepath.Join(dir, "spaces.jsonl")
	cfg.SampleSize = 2
	cfg.SampleSeed = "fixed-seed"
	cfg.SampleOut = filepath.Join(dir, "spaces-2.json")
	cfg.SpendingLimit = &limit

	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		discardLogger(),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if report.Inventory.Records != 3 ||
		report.Inventory.Active != 3 ||
		report.Inventory.SampleWritten != 2 {
		t.Fatalf("inventory summary = %+v", report.Inventory)
	}

	rawInventory, err := os.ReadFile(cfg.Inventory)
	if err != nil {
		t.Fatalf("read inventory: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(rawInventory)), "\n")
	if len(lines) != 3 {
		t.Fatalf("inventory records = %d, want 3", len(lines))
	}
	for _, line := range lines {
		var record inventoryRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode inventory record: %v", err)
		}
		if record.APIKey == "" || record.SpendingLimit == nil || *record.SpendingLimit != limit {
			t.Fatalf("inventory record = %+v", record)
		}
	}

	rawSample, err := os.ReadFile(cfg.SampleOut)
	if err != nil {
		t.Fatalf("read selected snapshot: %v", err)
	}
	var sample selectedSpaceState
	if err := json.Unmarshal(rawSample, &sample); err != nil {
		t.Fatalf("decode selected snapshot: %v", err)
	}
	if sample.SchemaVersion != selectedSpaceStateSchema || len(sample.Spaces) != 2 {
		t.Fatalf("selected snapshot = %+v", sample)
	}

	rawReport, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if strings.Contains(string(rawReport), "drive9-key-") {
		t.Fatalf("report contains API key: %s", rawReport)
	}
}

func TestRunBenchmarkDoesNotCreateSnapshotWhenActiveSampleIsShort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(
			w,
			`{"tenant_id":"tenant-1","api_key":"drive9-key-1","status":"failed"}`,
		)
	}))
	defer server.Close()

	dir := privateTempDir(t)
	cfg := testConfig(server.URL)
	cfg.Inventory = filepath.Join(dir, "spaces.jsonl")
	cfg.SampleSize = 1
	cfg.SampleSeed = "fixed-seed"
	cfg.SampleOut = filepath.Join(dir, "spaces-1.json")

	report, err := runBenchmark(
		context.Background(),
		cfg,
		server.Client(),
		discardLogger(),
		discardLogger(),
	)
	if err == nil || !strings.Contains(err.Error(), "active spaces") {
		t.Fatalf("error = %v, want active spaces", err)
	}
	if report.Inventory.Records != 1 || report.Inventory.Active != 0 {
		t.Fatalf("inventory summary = %+v", report.Inventory)
	}
	if _, statErr := os.Stat(cfg.SampleOut); !os.IsNotExist(statErr) {
		t.Fatalf("sample output stat error = %v, want not exist", statErr)
	}
}

func TestRunBenchmarkCancellationFlushesPartialInventory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var requests atomic.Int64
	client := httpDoerFunc(func(*http.Request) (*http.Response, error) {
		index := requests.Add(1)
		cancel()
		return acceptedTenantResponse(
			fmt.Sprintf("tenant-%d", index),
			fmt.Sprintf("drive9-key-%d", index),
		), nil
	})

	cfg := testConfig("https://drive9.example.com")
	cfg.Total = 10
	cfg.Inventory = filepath.Join(privateTempDir(t), "spaces.jsonl")

	report, err := runBenchmark(
		ctx,
		cfg,
		client,
		discardLogger(),
		discardLogger(),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if report.Summary.Completed != 1 || report.Inventory.Records != 1 {
		t.Fatalf("partial report = %+v, inventory = %+v", report.Summary, report.Inventory)
	}
	raw, readErr := os.ReadFile(cfg.Inventory)
	if readErr != nil {
		t.Fatalf("read inventory: %v", readErr)
	}
	if lines := strings.Count(strings.TrimSpace(string(raw)), "\n") + 1; lines != 1 {
		t.Fatalf("inventory records = %d, want 1", lines)
	}
}

func TestRunBenchmarkLogsAggregateProgress(t *testing.T) {
	var requests atomic.Int64
	client := httpDoerFunc(func(*http.Request) (*http.Response, error) {
		time.Sleep(3 * time.Millisecond)
		index := requests.Add(1)
		return acceptedTenantResponse(
			fmt.Sprintf("tenant-%d", index),
			fmt.Sprintf("drive9-key-%d", index),
		), nil
	})
	cfg := testConfig("https://drive9.example.com")
	cfg.Total = 5
	cfg.RPS = 1_000_000
	cfg.ReportInterval = time.Millisecond
	var logs strings.Builder

	_, err := runBenchmark(
		context.Background(),
		cfg,
		client,
		log.New(&logs, "", 0),
		discardLogger(),
	)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	for _, want := range []string{
		"progress: window_seconds=",
		"window_success=",
		"total_success=",
		"provision_latency_window:",
		"p50_seconds=",
		"p90_seconds=",
		"p95_seconds=",
		"p99_seconds=",
		"ready_latency_window:",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("progress log missing %q: %q", want, logs.String())
		}
	}
	if strings.Contains(logs.String(), "drive9-key-") {
		t.Fatalf("progress log contains API key: %q", logs.String())
	}
}

func TestRunMainWritesReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"tenant_id":"tenant-1","api_key":"drive9_secret","status":"active"}`)
	}))
	defer server.Close()

	reportPath := t.TempDir() + "/report.json"
	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"--server", server.URL,
			"--out", reportPath,
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %s", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "success=1 failed=0") {
		t.Fatalf("stdout = %q", stdout.String())
	}

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report benchmarkReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.SchemaVersion != reportSchema || report.Summary.Success != 1 {
		t.Fatalf("report = %+v", report)
	}
	if strings.Contains(string(raw), "drive9_secret") {
		t.Fatal("report contains tenant API key")
	}
}

func TestRunMainSeparatesProgressAndRequestErrors(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(3 * time.Millisecond)
		index := requests.Add(1)
		if index == 2 {
			http.Error(w, "temporary provision failure", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(
			w,
			`{"tenant_id":"tenant-%d","api_key":"drive9-key-%d","status":"active"}`,
			index,
			index,
		)
	}))
	defer server.Close()

	reportPath := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr strings.Builder
	exitCode := runMain(
		[]string{
			"--server", server.URL,
			"--out", reportPath,
			"--total", "4",
			"--concurrency", "1",
			"--rps", "1000000",
			"--report-interval", "1ms",
		},
		emptyEnv,
		func() (string, error) { return t.TempDir(), nil },
		&stdout,
		&stderr,
	)
	if exitCode != 1 {
		t.Fatalf(
			"exit code = %d, want 1; stdout=%s stderr=%s",
			exitCode,
			stdout.String(),
			stderr.String(),
		)
	}
	for _, want := range []string{
		"progress: window_seconds=",
		"provision_latency_window:",
		"p50_seconds=",
		"p90_seconds=",
		"p95_seconds=",
		"p99_seconds=",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q: %s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "request error:") {
		t.Fatalf("stdout contains request error: %s", stdout.String())
	}
	for _, want := range []string{
		"request error: operation=provision",
		"http_status=502",
		"latency_seconds=",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), "progress:") {
		t.Fatalf("stderr contains progress: %s", stderr.String())
	}
	if found := countTimestampedLogKind(t, stdout.String(), "progress:"); found < 1 {
		t.Fatalf("timestamped progress logs = %d, want at least 1", found)
	}
	assertTimestampedRequestErrors(t, stderr.String(), 1)
}

func TestPrintHistogram(t *testing.T) {
	histogram := newLatencyHistogram(defaultProvisionLatencyBounds)
	for _, sample := range []time.Duration{
		time.Second,
		time.Second,
		21 * time.Second,
		41 * time.Second,
	} {
		histogram.Record(sample)
	}
	var output strings.Builder
	printLatencyHistogram(&output, "Provision Latency Histogram", histogram.Snapshot())

	got := output.String()
	for _, want := range []string{
		"Provision Latency Histogram:",
		"1s",
		"30s",
		"1m0s",
		"       2 |" + strings.Repeat("█", 50),
		"       1 |" + strings.Repeat("█", 25),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("histogram output missing %q:\n%s", want, got)
		}
	}
}

func TestPrintHistogramHandlesEqualSamples(t *testing.T) {
	histogram := newLatencyHistogram(defaultProvisionLatencyBounds)
	histogram.Record(2 * time.Second)
	histogram.Record(2 * time.Second)
	var output strings.Builder
	printLatencyHistogram(&output, "Ready Latency Histogram", histogram.Snapshot())

	got := output.String()
	for _, want := range []string{
		"Ready Latency Histogram:",
		"2s",
		"       2 |" + strings.Repeat("█", 50),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("histogram output missing %q:\n%s", want, got)
		}
	}
}

func TestPrintSummaryIncludesAvailableHistograms(t *testing.T) {
	provision := newLatencyHistogram(defaultProvisionLatencyBounds)
	provision.Record(time.Second)
	provision.Record(2 * time.Second)
	ready := newLatencyHistogram(defaultReadyLatencyBounds)
	ready.Record(10 * time.Second)
	ready.Record(20 * time.Second)
	report := benchmarkReport{
		Summary: benchmarkSummary{
			ProvisionLatency: provision.Snapshot(),
			ReadyLatency:     ready.Snapshot(),
		},
	}
	var output strings.Builder
	printSummary(&output, report)

	got := output.String()
	for _, want := range []string{
		"Provision Latency Histogram:",
		"Ready Latency Histogram:",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary output missing %q:\n%s", want, got)
		}
	}
}

func TestPrintSummarySkipsEmptyHistograms(t *testing.T) {
	var output strings.Builder
	printSummary(&output, benchmarkReport{})
	if strings.Contains(output.String(), "Histogram:") {
		t.Fatalf("unexpected empty histogram:\n%s", output.String())
	}
}

func testConfig(server string) benchConfig {
	return benchConfig{
		Server:         server,
		Out:            "unused.json",
		Total:          1,
		Concurrency:    1,
		RPS:            1000,
		WaitReady:      true,
		PollInterval:   time.Millisecond,
		Timeout:        time.Second,
		ReportInterval: time.Hour,
	}
}

func emptyEnv(string) string {
	return ""
}

func mapEnv(values map[string]string) func(string) string {
	return func(name string) string {
		return values[name]
	}
}

func discardLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

func assertTimestampedRequestErrors(t *testing.T, logs string, want int) {
	t.Helper()
	found := countTimestampedLogKind(t, logs, "request error:")
	if found != want {
		t.Fatalf("timestamped request errors = %d, want %d: %s", found, want, logs)
	}
}

func countTimestampedLogKind(t *testing.T, logs, kind string) int {
	t.Helper()
	found := 0
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if !strings.Contains(line, kind) {
			continue
		}
		found++
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("%s log has no timestamp: %q", kind, line)
		}
		if _, err := time.Parse("2006/01/02 15:04:05", fields[0]+" "+fields[1]); err != nil {
			t.Fatalf("%s log timestamp: %v: %q", kind, err, line)
		}
	}
	return found
}

type httpDoerFunc func(*http.Request) (*http.Response, error)

func (function httpDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return function(request)
}

func acceptedTenantResponse(tenantID, apiKey string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusAccepted,
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
			`{"tenant_id":%q,"api_key":%q,"status":"active"}`,
			tenantID,
			apiKey,
		))),
	}
}

func writeCreateBenchConfigFile(t *testing.T, path, raw string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create config directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(raw), mode); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("set config mode: %v", err)
	}
}
