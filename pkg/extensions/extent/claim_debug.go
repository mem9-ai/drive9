package extent

// Debug-only claim tracing: when DRIVE9_DEBUG_CLAIM is set, every fresh-create
// claim decision logs its outcome and decline reason to stderr (captured by
// the mount supervisor's worker log). Used to diagnose why a production
// kernel O_CREAT that matches the profile's extent patterns is not claimed.

import (
	"fmt"
	"os"
	"sync"
)

var (
	claimDebugEnabledOnce sync.Once
	claimDebugEnabled     bool
)

func claimDebug(reason string, args ...any) {
	claimDebugEnabledOnce.Do(func() {
		claimDebugEnabled = os.Getenv("DRIVE9_DEBUG_CLAIM") != ""
	})
	if !claimDebugEnabled {
		return
	}
	msg := fmt.Sprintf(reason, args...)
	fmt.Fprintf(os.Stderr, "extent-claim: %s\n", msg)
}
