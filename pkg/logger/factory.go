package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func NewServerLogger() (*zap.Logger, error) {
	return zap.NewProduction()
}

func NewCLILogger() (*zap.Logger, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home: %w", err)
	}
	logDir := filepath.Join(home, ".dat", "log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	logPath := filepath.Join(logDir, "dat9-cli.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	_ = f.Close()

	rotate := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    envInt("DAT9_CLI_LOG_MAX_SIZE_MB", 10),
		MaxBackups: envInt("DAT9_CLI_LOG_MAX_BACKUPS", 5),
		MaxAge:     envInt("DAT9_CLI_LOG_MAX_AGE_DAYS", 14),
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
