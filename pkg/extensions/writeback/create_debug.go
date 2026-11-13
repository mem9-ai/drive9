package writeback

// Debug-only create tracing: when DRIVE9_DEBUG_CLAIM is set, the writeback
// plane logs every OpCreate it sees (it should see none for creates the
// extent plane claims) and the lazy-create decision it makes. Mirrors
// extent.claimDebug; kept separate because the helper is package-scoped.

import (
	"fmt"
	"os"
	"sync"
)

var (
	wbCreateDebugOnce sync.Once
	wbCreateDebug     bool
)

func wbCreateDebugf(format string, args ...any) {
	wbCreateDebugOnce.Do(func() {
		wbCreateDebug = os.Getenv("DRIVE9_DEBUG_CLAIM") != ""
	})
	if !wbCreateDebug {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "wb-create: %s\n", msg)
}
