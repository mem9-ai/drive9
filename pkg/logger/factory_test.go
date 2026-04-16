package logger

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewCLILoggerCreatesLogDirAndFile(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	logPath, err := CLILogPath()
	if err != nil {
		t.Fatalf("CLILogPath: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(logPath)); !os.IsNotExist(err) {
		t.Fatalf("expected log dir to be absent before init, got err=%v", err)
	}

	l, err := NewCLILogger()
	if err != nil {
		t.Fatalf("NewCLILogger: %v", err)
	}
	t.Cleanup(func() { _ = l.Sync() })

	info, err := os.Stat(filepath.Dir(logPath))
	if err != nil {
		t.Fatalf("Stat(log dir): %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected log dir, got file: %s", filepath.Dir(logPath))
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("Stat(log file): %v", err)
	}
}

func TestBenchTimingLogEnabledCachesUntilReset(t *testing.T) {
	resetBenchTimingLogEnabledForTest()
	t.Cleanup(resetBenchTimingLogEnabledForTest)

	t.Setenv(envBenchTimingLogEnabled, "true")
	if !BenchTimingLogEnabled() {
		t.Fatal("expected bench timing log to be enabled")
	}

	t.Setenv(envBenchTimingLogEnabled, "false")
	if !BenchTimingLogEnabled() {
		t.Fatal("expected cached enabled value to remain true before reset")
	}

	resetBenchTimingLogEnabledForTest()
	if BenchTimingLogEnabled() {
		t.Fatal("expected bench timing log to be disabled after reset")
	}
}

func TestInfoBenchTimingHonorsEnabledFlag(t *testing.T) {
	resetBenchTimingLogEnabledForTest()
	t.Cleanup(resetBenchTimingLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	ctx := WithContext(context.Background(), zap.New(core))

	t.Setenv(envBenchTimingLogEnabled, "false")
	InfoBenchTiming(ctx, "timing_disabled")
	if entries := recorded.All(); len(entries) != 0 {
		t.Fatalf("recorded %d entries with timing disabled, want 0", len(entries))
	}

	resetBenchTimingLogEnabledForTest()
	t.Setenv(envBenchTimingLogEnabled, "true")
	InfoBenchTiming(ctx, "timing_enabled")
	entries := recorded.All()
	if len(entries) != 1 {
		t.Fatalf("recorded %d entries with timing enabled, want 1", len(entries))
	}
	if entries[0].Message != "timing_enabled" {
		t.Fatalf("message = %q, want timing_enabled", entries[0].Message)
	}
}
