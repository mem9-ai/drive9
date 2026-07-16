package logger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestNewServerLoggerHonorsLogLevel(t *testing.T) {
	tests := []struct {
		name         string
		level        string
		debugEnabled bool
		infoEnabled  bool
	}{
		{name: "default_info", infoEnabled: true},
		{name: "debug", level: "debug", debugEnabled: true, infoEnabled: true},
		{name: "uppercase_warn", level: "WARN"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.level != "" {
				t.Setenv(envLogLevel, tc.level)
			} else {
				t.Setenv(envLogLevel, "")
			}
			l, err := NewServerLogger()
			if err != nil {
				t.Fatalf("NewServerLogger: %v", err)
			}
			t.Cleanup(func() { _ = l.Sync() })
			if got := l.Core().Enabled(zap.DebugLevel); got != tc.debugEnabled {
				t.Fatalf("debug enabled=%v, want %v", got, tc.debugEnabled)
			}
			if got := l.Core().Enabled(zap.InfoLevel); got != tc.infoEnabled {
				t.Fatalf("info enabled=%v, want %v", got, tc.infoEnabled)
			}
		})
	}
}

func TestNewServerLoggerRejectsInvalidLogLevel(t *testing.T) {
	tests := []string{"verbose", "dpanic", "panic", "fatal"}
	for _, level := range tests {
		t.Run(level, func(t *testing.T) {
			t.Setenv(envLogLevel, level)
			if _, err := NewServerLogger(); err == nil {
				t.Fatal("expected invalid log level error")
			}
		})
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

func TestDBTraceLogEnabledCachesUntilReset(t *testing.T) {
	resetDBTraceLogEnabledForTest()
	t.Cleanup(resetDBTraceLogEnabledForTest)

	if !DBTraceLogEnabled() {
		t.Fatal("expected DB trace log to be enabled by default")
	}

	t.Setenv(envDBTraceLogEnabled, "false")
	if !DBTraceLogEnabled() {
		t.Fatal("expected cached enabled value to remain true before reset")
	}

	resetDBTraceLogEnabledForTest()
	if DBTraceLogEnabled() {
		t.Fatal("expected DB trace log to be disabled after reset")
	}

	t.Setenv(envDBTraceLogEnabled, "true")
	resetDBTraceLogEnabledForTest()
	if !DBTraceLogEnabled() {
		t.Fatal("expected DB trace log to be enabled")
	}
}

func TestDBSlowTraceThresholdDefaultsTo300MS(t *testing.T) {
	resetDBSlowTraceThresholdForTest()
	t.Cleanup(resetDBSlowTraceThresholdForTest)

	if got := DBSlowTraceThreshold(); got != 300*time.Millisecond {
		t.Fatalf("DBSlowTraceThreshold() = %s, want 300ms", got)
	}
}

func TestDBSlowTraceThresholdAcceptsZeroForAllOperations(t *testing.T) {
	resetDBSlowTraceThresholdForTest()
	t.Cleanup(resetDBSlowTraceThresholdForTest)

	t.Setenv(envDBSlowTraceMS, "0")
	if got := DBSlowTraceThreshold(); got != 0 {
		t.Fatalf("DBSlowTraceThreshold() = %s, want 0", got)
	}
}

func TestDBSlowTraceThresholdCachesUntilReset(t *testing.T) {
	resetDBSlowTraceThresholdForTest()
	t.Cleanup(resetDBSlowTraceThresholdForTest)

	t.Setenv(envDBSlowTraceMS, "250")
	if got := DBSlowTraceThreshold(); got != 250*time.Millisecond {
		t.Fatalf("DBSlowTraceThreshold() = %s, want 250ms", got)
	}

	t.Setenv(envDBSlowTraceMS, "500")
	if got := DBSlowTraceThreshold(); got != 250*time.Millisecond {
		t.Fatalf("DBSlowTraceThreshold() = %s, want cached 250ms before reset", got)
	}

	resetDBSlowTraceThresholdForTest()
	if got := DBSlowTraceThreshold(); got != 500*time.Millisecond {
		t.Fatalf("DBSlowTraceThreshold() after reset = %s, want 500ms", got)
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
