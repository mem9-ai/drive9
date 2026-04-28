package logger

import (
	"context"
	"sync/atomic"

	"github.com/mem9-ai/dat9/pkg/traceid"
	"go.uber.org/zap"
)

type contextKeyType int

const contextKey contextKeyType = iota

var global atomic.Pointer[zap.Logger]

func init() {
	l, _ := zap.NewProduction()
	global.Store(l)
	zap.ReplaceGlobals(l)
}

func Set(l *zap.Logger) {
	if l == nil {
		return
	}
	global.Store(l)
	zap.ReplaceGlobals(l)
}

func L() *zap.Logger {
	if l := global.Load(); l != nil {
		return l
	}
	return zap.L()
}

func WithContext(ctx context.Context, l *zap.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey, l)
}

func FromContext(ctx context.Context) *zap.Logger {
	if l, _ := ctx.Value(contextKey).(*zap.Logger); l != nil {
		return l
	}
	if traceID := traceid.FromContext(ctx); traceID != "" {
		return L().With(zap.String("trace_id", traceID))
	}
	return L()
}

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func InfoBenchTiming(ctx context.Context, msg string, fields ...zap.Field) {
	if !BenchTimingLogEnabled() {
		return
	}
	FromContext(ctx).WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}
