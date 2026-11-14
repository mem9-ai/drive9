package extent

// commitTrace logs the write pipeline's phase timings to stderr when
// DRIVE9_EXTENT_COMMIT_TRACE is set (drain diagnosis: staging upload
// cadence, commit assemble/publish wall). No-op otherwise.

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	commitTraceOnce sync.Once
	commitTraceOn   bool
)

func commitTrace(format string, args ...any) {
	commitTraceOnce.Do(func() { commitTraceOn = os.Getenv("DRIVE9_EXTENT_COMMIT_TRACE") != "" })
	if !commitTraceOn {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "extent-trace %s %s\n", time.Now().UTC().Format("15:04:05.000"), msg)
}

func sizeOfPut(key string) int64 {
	_, _, size, err := parseKey(key)
	if err != nil {
		return -1
	}
	return size
}
