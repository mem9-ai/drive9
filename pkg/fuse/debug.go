package fuse

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const (
	fuseDebugSlowReadThreshold = 100 * time.Millisecond
	fuseDebugSlowOpThreshold   = 250 * time.Millisecond
)

func (fs *Dat9FS) debugEnabled() bool {
	return fs != nil && fs.opts != nil && fs.opts.Debug
}

func (fs *Dat9FS) debugf(format string, args ...any) {
	if !fs.debugEnabled() {
		return
	}
	safeLogPrintf("dat9 debug: "+format, args...)
}

func (fs *Dat9FS) debugDurationf(start time.Time, threshold time.Duration, format string, args ...any) {
	if !fs.debugEnabled() {
		return
	}
	d := time.Since(start)
	if d < threshold {
		return
	}
	args = append(args, d)
	safeLogPrintf("dat9 debug: "+format+" dur=%s", args...)
}

func escapeLogControlWhitespace(value string) string {
	return strings.NewReplacer("\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(value)
}

func safeLogPrintf(format string, args ...any) {
	escapeLogArgs(args)
	log.Printf(format, args...)
}

func safeStderrPrintf(format string, args ...any) {
	escapeLogArgs(args)
	_, _ = fmt.Fprintf(os.Stderr, format, args...)
}

func escapeLogArgs(args []any) {
	for i, arg := range args {
		switch value := arg.(type) {
		case string:
			args[i] = escapeLogControlWhitespace(value)
		case error:
			args[i] = escapeLogControlWhitespace(value.Error())
		case fmt.Stringer:
			args[i] = escapeLogControlWhitespace(value.String())
		}
	}
}
