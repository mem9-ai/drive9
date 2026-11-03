package staging

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

var _ = os.Stderr

var errLayerRolledBack = errors.New("fs layer has been rolled back; remount required")

var errCommitPayloadStale = errors.New("commit payload generation is stale")

type safeLogValue struct{ value any }

// String renders the wrapped value with control whitespace escaped.
func (v safeLogValue) String() string {
	switch t := v.value.(type) {
	case string:
		return strings.NewReplacer("\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(t)
	case error:
		return strings.NewReplacer("\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(t.Error())
	}
	return fmt.Sprint(v.value)
}

// safeLogPrintf escapes control whitespace in strings and errors
// before logging, so log injection stays impossible.
func safeLogPrintf(format string, args ...any) {
	escapeLogArgs(args)
	log.Printf(format, args...)
}

func escapeLogArgs(args []any) {
	for i, arg := range args {
		switch arg.(type) {
		case string:
			args[i] = safeLogValue{value: arg}
		case error:
			args[i] = safeLogValue{value: arg}
		}
	}
}
