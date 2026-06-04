package fuse

import (
	"context"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

const (
	defaultRegularFileMode     uint32 = 0o644
	posixPermissionModeMask    uint32 = 0o7777
	postUploadModeAttempts            = 5
	postUploadModeInitialDelay        = 10 * time.Millisecond
)

func shouldApplyRemoteMode(kind PendingKind, hasMode bool, mode uint32) bool {
	if !hasMode {
		return false
	}
	if kind == PendingNew && mode&posixPermissionModeMask == defaultRegularFileMode {
		return false
	}
	return true
}

func retryPostUploadMode(ctx context.Context, apply func() error) error {
	var lastErr error
	for attempt := 0; attempt < postUploadModeAttempts; attempt++ {
		lastErr = apply()
		if lastErr == nil {
			return nil
		}
		if !client.IsNotFound(lastErr) || attempt == postUploadModeAttempts-1 {
			return lastErr
		}

		delay := postUploadModeInitialDelay << attempt
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}
