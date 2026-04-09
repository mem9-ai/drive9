package logger

import (
	"os"
	"path/filepath"
	"testing"
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
