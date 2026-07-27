package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSafeReportConfigOmitsCredentials(t *testing.T) {
	t.Parallel()

	cfg := testBenchConfig(t, "https://drive9.example.com")
	cfg.PublicKey = "public-super-secret"
	cfg.PrivateKey = "private-super-secret"
	cfg.DeleteEvery = 10
	raw, err := json.Marshal(safeReportConfig(cfg))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	for _, secret := range []string{cfg.PublicKey, cfg.PrivateKey} {
		if strings.Contains(got, secret) {
			t.Fatalf("report config exposed secret %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, `"credentials_supplied":true`) {
		t.Fatalf("report config missing credential presence: %s", got)
	}
	if !strings.Contains(got, `"delete_every":10`) {
		t.Fatalf("report config missing delete cadence: %s", got)
	}
}

func TestWriteReportIsSecureAndContainsNoSpaceAPIKeys(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "reports", "result.json")
	report := benchmarkReport{
		SchemaVersion: benchmarkReportSchema,
		StartedAt:     time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC),
		FinishedAt:    time.Date(2026, 7, 23, 1, 1, 0, 0, time.UTC),
		Spaces: spaceSummary{
			Requested:   500,
			Configured:  500,
			Reused:      400,
			Provisioned: 100,
			Ready:       500,
		},
	}
	if err := writeReport(path, report); err != nil {
		t.Fatalf("writeReport: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat report: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("report mode = %o, want 600", got)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if bytes.Contains(raw, []byte("api_key")) {
		t.Fatalf("report unexpectedly contains API-key field: %s", raw)
	}
}

func TestWorkloadErrorSampleIsNotSerialized(t *testing.T) {
	t.Parallel()

	stats := newWorkloadStats()
	stats.setLastWriteError(workloadErrorSample{
		TenantID:    "tenant-secret-context",
		WorkerIndex: 1,
		RemotePath:  "/secret-context.bin",
		Message:     "sensitive retained error",
	})
	stats.recordWrite(time.Millisecond, 0, errors.New("write failed"))

	raw, err := json.Marshal(stats.Snapshot())
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}
	for _, excluded := range []string{
		"tenant-secret-context",
		"secret-context.bin",
		"sensitive retained error",
		"lastWriteError",
	} {
		if bytes.Contains(raw, []byte(excluded)) {
			t.Fatalf("serialized stats exposed %q: %s", excluded, raw)
		}
	}
	if !bytes.Contains(raw, []byte(`"write_errors":1`)) {
		t.Fatalf("serialized stats omitted aggregate error count: %s", raw)
	}
}

func TestPrintFinalSummaryIncludesReadAndWriteHistograms(t *testing.T) {
	t.Parallel()

	stats := newWorkloadStats()
	stats.recordWrite(time.Millisecond, 10, nil)
	stats.recordRead(2*time.Millisecond, 10, nil)
	stats.recordDelete(3*time.Millisecond, nil)
	report := benchmarkReport{
		Spaces: spaceSummary{Requested: 1, Configured: 1, Ready: 1},
		Workload: workloadRun{
			StartedAt:  time.Now().Add(-time.Second),
			FinishedAt: time.Now(),
			Stats:      stats.Snapshot(),
		},
	}

	var output bytes.Buffer
	printFinalSummary(&output, report)
	got := output.String()
	for _, want := range []string{
		"spaces=1/1",
		"delete=1/0",
		"Write Latency Histogram:",
		"Read Latency Histogram:",
		"Delete Latency Histogram:",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q:\n%s", want, got)
		}
	}
}
