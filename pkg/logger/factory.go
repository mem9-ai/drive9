package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	envBenchTimingLogEnabled = "DRIVE9_BENCH_TIMING_LOG_ENABLED"
	envLogLevel              = "DRIVE9_LOG_LEVEL"
)

const (
	benchTimingLogUnknown uint32 = iota
	benchTimingLogDisabled
	benchTimingLogEnabled
)

var benchTimingLogState atomic.Uint32

func NewServerLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	rawLevel := strings.TrimSpace(os.Getenv(envLogLevel))
	if rawLevel != "" {
		var level zapcore.Level
		if err := level.UnmarshalText([]byte(strings.ToLower(rawLevel))); err != nil {
			return nil, fmt.Errorf("%s must be one of debug, info, warn, or error: %w", envLogLevel, err)
		}
		if level < zapcore.DebugLevel || level > zapcore.ErrorLevel {
			return nil, fmt.Errorf("%s must be one of debug, info, warn, or error", envLogLevel)
		}
		cfg.Level.SetLevel(level)
	}
	return cfg.Build()
}

func BenchTimingLogEnabled() bool {
	switch benchTimingLogState.Load() {
	case benchTimingLogDisabled:
		return false
	case benchTimingLogEnabled:
		return true
	}

	enabled := envBool(envBenchTimingLogEnabled, false)
	if enabled {
		benchTimingLogState.Store(benchTimingLogEnabled)
		return true
	}
	benchTimingLogState.Store(benchTimingLogDisabled)
	return false
}

func CLIEnabled() bool {
	return envBool("DRIVE9_CLI_LOG_ENABLED", false)
}

func CLILogDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home: %w", err)
	}
	return filepath.Join(home, ".drive9", "cli"), nil
}

func CLILogPath() (string, error) {
	logDir, err := CLILogDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(logDir, "drive9-cli.log"), nil
}

func NewCLILogger() (*zap.Logger, error) {
	logDir, err := CLILogDir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	logPath, err := CLILogPath()
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	_ = f.Close()

	rotate := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    envInt("DRIVE9_CLI_LOG_MAX_SIZE_MB", 10),
		MaxBackups: envInt("DRIVE9_CLI_LOG_MAX_BACKUPS", 5),
		MaxAge:     envInt("DRIVE9_CLI_LOG_MAX_AGE_DAYS", 14),
		Compress:   true,
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(rotate),
		zap.InfoLevel,
	)
	return zap.New(core), nil
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func envBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func resetBenchTimingLogEnabledForTest() {
	benchTimingLogState.Store(benchTimingLogUnknown)
}
