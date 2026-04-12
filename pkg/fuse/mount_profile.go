package fuse

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// SyncMode controls when Fsync is considered durable.
type SyncMode int

const (
	// SyncAuto measures RTT at mount time. RTT > 10ms → interactive, else strict.
	SyncAuto SyncMode = iota
	// SyncInteractive: Fsync = local journal durable only. Remote commit is async.
	SyncInteractive
	// SyncStrict: Fsync = remote durable. Blocks until remote PUT succeeds.
	SyncStrict
)

// String returns the sync mode name for CLI/logging.
func (m SyncMode) String() string {
	switch m {
	case SyncInteractive:
		return "interactive"
	case SyncStrict:
		return "strict"
	case SyncAuto:
		return "auto"
	default:
		return "unknown"
	}
}

// ParseSyncMode converts a string to SyncMode. Returns an error for unknown values
// so that typos like "strcit" fail fast instead of silently falling back to auto.
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "interactive":
		return SyncInteractive, nil
	case "strict":
		return SyncStrict, nil
	case "auto", "":
		return SyncAuto, nil
	default:
		return SyncAuto, fmt.Errorf("unknown sync mode %q (valid: auto, interactive, strict)", s)
	}
}

// rttThreshold is the threshold for auto-detecting sync mode.
const rttThreshold = 10 * time.Millisecond

// maxCommitQueuePending is the backpressure limit for the commit queue.
const maxCommitQueuePending = 100

// MeasureRTT measures the round-trip time to the server by issuing a HEAD
// request to the root path. Returns the measured duration.
func MeasureRTT(ctx context.Context, serverURL string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, serverURL+"/", nil)
	if err != nil {
		return 0, fmt.Errorf("build RTT probe request: %w", err)
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	rtt := time.Since(start)
	if err != nil {
		return rtt, fmt.Errorf("execute RTT probe: %w", err)
	}
	_ = resp.Body.Close()
	return rtt, nil
}

// ResolveMode resolves SyncAuto to either SyncInteractive or SyncStrict
// based on measured RTT to the server.
func ResolveMode(ctx context.Context, mode SyncMode, serverURL string) SyncMode {
	if mode != SyncAuto {
		return mode
	}
	rtt, err := MeasureRTT(ctx, serverURL)
	if err != nil {
		// Cannot measure — assume WAN, use interactive.
		return SyncInteractive
	}
	if rtt > rttThreshold {
		return SyncInteractive
	}
	return SyncStrict
}

// ApplyInteractiveProfile configures MountOptions for interactive editing
// workloads (vim, VSCode, JetBrains).
func ApplyInteractiveProfile(opts *MountOptions) {
	// SyncMode == SyncAuto is resolved at mount time; no override needed here.
	if opts.AttrTTL <= 0 {
		opts.AttrTTL = 1 * time.Second
	}
	if opts.EntryTTL <= 0 {
		opts.EntryTTL = 1 * time.Second
	}
	if opts.DirTTL <= 0 {
		opts.DirTTL = 2 * time.Second
	}
	// Disable debounce — replaced by shadow writes.
	opts.FlushDebounce = 0
	opts.UploadConcurrency = 4
}
