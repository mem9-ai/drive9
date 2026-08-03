package fuse

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
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
		switch arg.(type) {
		case string:
			args[i] = safeLogValue{value: arg}
		case error:
			args[i] = safeLogValue{value: arg}
		case fmt.Stringer:
			args[i] = safeLogValue{value: arg}
		}
	}
}

type safeLogValue struct {
	value any
}

func (v safeLogValue) Format(state fmt.State, verb rune) {
	var directive strings.Builder
	directive.WriteByte('%')
	for _, flag := range "#0+- " {
		if state.Flag(int(flag)) {
			directive.WriteRune(flag)
		}
	}
	if width, ok := state.Width(); ok {
		directive.WriteString(strconv.Itoa(width))
	}
	if precision, ok := state.Precision(); ok {
		directive.WriteByte('.')
		directive.WriteString(strconv.Itoa(precision))
	}
	directive.WriteRune(verb)

	rendered := fmt.Sprintf(directive.String(), v.value)
	_, _ = io.WriteString(state, escapeLogControlWhitespace(rendered))
}
