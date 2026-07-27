package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const benchmarkReportSchema = "drive9-space-bench/v1"

type reportConfig struct {
	Server               string  `json:"server"`
	SpacesFile           string  `json:"spaces_file"`
	SpaceCount           int     `json:"spaces"`
	ProvisionConcurrency int     `json:"provision_concurrency"`
	ProvisionRPS         float64 `json:"provision_rps"`
	PollInterval         string  `json:"poll_interval"`
	ProvisionTimeout     string  `json:"provision_timeout"`
	SpendingLimit        int64   `json:"tidbcloud_spending_limit"`
	CredentialsSupplied  bool    `json:"credentials_supplied"`
	WorkersPerSpace      int     `json:"workers_per_space"`
	FilesPerWorker       int     `json:"files_per_worker"`
	DeleteEvery          int     `json:"delete_every"`
	FileSize             int     `json:"file_size"`
	Duration             string  `json:"duration"`
	RequestTimeout       string  `json:"request_timeout"`
	ReportInterval       string  `json:"report_interval"`
	IORPS                float64 `json:"io_rps"`
}

type spaceSummary struct {
	Requested   int `json:"requested"`
	Configured  int `json:"configured"`
	Reused      int `json:"reused"`
	Provisioned int `json:"provisioned"`
	Ready       int `json:"ready"`
}

type benchmarkReport struct {
	SchemaVersion string       `json:"schema_version"`
	StartedAt     time.Time    `json:"started_at"`
	FinishedAt    time.Time    `json:"finished_at"`
	Config        reportConfig `json:"config"`
	Spaces        spaceSummary `json:"spaces"`
	StopReason    string       `json:"stop_reason,omitempty"`
	Error         string       `json:"error,omitempty"`
	Workload      workloadRun  `json:"workload"`
}

func safeReportConfig(cfg benchConfig) reportConfig {
	return reportConfig{
		Server:               cfg.Server,
		SpacesFile:           cfg.SpacesFile,
		SpaceCount:           cfg.SpaceCount,
		ProvisionConcurrency: cfg.ProvisionConcurrency,
		ProvisionRPS:         cfg.ProvisionRPS,
		PollInterval:         cfg.PollInterval.String(),
		ProvisionTimeout:     cfg.ProvisionTimeout.String(),
		SpendingLimit:        cfg.SpendingLimit,
		CredentialsSupplied:  cfg.PublicKey != "" && cfg.PrivateKey != "",
		WorkersPerSpace:      cfg.WorkersPerSpace,
		FilesPerWorker:       cfg.FilesPerWorker,
		DeleteEvery:          cfg.DeleteEvery,
		FileSize:             cfg.FileSize,
		Duration:             cfg.Duration.String(),
		RequestTimeout:       cfg.RequestTimeout.String(),
		ReportInterval:       cfg.ReportInterval.String(),
		IORPS:                cfg.IORPS,
	}
}

func writeReport(path string, report benchmarkReport) error {
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode benchmark report: %w", err)
	}
	raw = append(raw, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".drive9-space-bench-report-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary benchmark report: %w", err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		_ = os.Remove(tmpPath)
		if !closed {
			_ = tmp.Close()
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure temporary benchmark report: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		return fmt.Errorf("write temporary benchmark report: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary benchmark report: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary benchmark report: %w", err)
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace benchmark report: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure benchmark report: %w", err)
	}
	return nil
}

func printFinalSummary(w io.Writer, report benchmarkReport) {
	elapsed := report.Workload.FinishedAt.Sub(report.Workload.StartedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	stats := report.Workload.Stats
	requests := stats.WriteRequests + stats.ReadRequests + stats.DeleteRequests
	opsPerSecond := 0.0
	if elapsed > 0 {
		opsPerSecond = float64(requests) / elapsed.Seconds()
	}
	_, _ = fmt.Fprintf(
		w,
		"drive9 space bench done: spaces=%d/%d reused=%d provisioned=%d "+
			"duration=%s write=%d/%d read=%d/%d delete=%d/%d "+
			"verify_errors=%d "+
			"ops_per_second=%.2f stop=%s\n",
		report.Spaces.Ready,
		report.Spaces.Requested,
		report.Spaces.Reused,
		report.Spaces.Provisioned,
		elapsed.Round(time.Millisecond),
		stats.WriteSuccess,
		stats.WriteErrors,
		stats.ReadSuccess,
		stats.ReadErrors,
		stats.DeleteSuccess,
		stats.DeleteErrors,
		stats.VerificationErrors,
		opsPerSecond,
		report.StopReason,
	)
	printLatencySummary(w, "write latency", stats.WriteLatency)
	printLatencySummary(w, "read latency", stats.ReadLatency)
	printLatencySummary(w, "delete latency", stats.DeleteLatency)
	printLatencyHistogram(w, "Write Latency Histogram", stats.WriteLatency)
	printLatencyHistogram(w, "Read Latency Histogram", stats.ReadLatency)
	printLatencyHistogram(w, "Delete Latency Histogram", stats.DeleteLatency)
}

func printLatencySummary(w io.Writer, name string, snapshot histogramSnapshot) {
	if snapshot.Count == 0 {
		return
	}
	_, _ = fmt.Fprintf(
		w,
		"%s: avg=%.6fs p50=%.6fs p95=%.6fs p99=%.6fs max=%.6fs samples=%d\n",
		name,
		snapshot.AverageSeconds,
		snapshot.P50Seconds,
		snapshot.P95Seconds,
		snapshot.P99Seconds,
		snapshot.MaxSeconds,
		snapshot.Count,
	)
}
